package ingest

import (
	"io"
	"sync"
	"time"

	"github.com/qkd/bb84-sifter/frame"
)

type BufferStatus int

const (
	BufferEmpty BufferStatus = iota
	BufferFilling
	BufferFull
	BufferProcessing
)

type DoubleBuffer struct {
	bufA []frame.Event
	bufB []frame.Event

	aStatus BufferStatus
	bStatus BufferStatus

	writePtr *[]frame.Event
	readPtr  *[]frame.Event

	mu       sync.RWMutex
	writeIdx int
	capacity int
}

func NewDoubleBuffer(capacity int) *DoubleBuffer {
	if capacity <= 0 {
		capacity = 1 << 20
	}
	db := &DoubleBuffer{
		bufA:     make([]frame.Event, 0, capacity),
		bufB:     make([]frame.Event, 0, capacity),
		capacity: capacity,
		aStatus:  BufferEmpty,
		bStatus:  BufferEmpty,
	}
	db.writePtr = &db.bufA
	db.readPtr = &db.bufB
	return db
}

func (db *DoubleBuffer) Write(events []frame.Event) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	available := db.capacity - len(*db.writePtr)
	if available <= 0 {
		return 0
	}

	toWrite := len(events)
	if toWrite > available {
		toWrite = available
	}

	*db.writePtr = append(*db.writePtr, events[:toWrite]...)
	db.writeIdx += toWrite

	if len(*db.writePtr) >= db.capacity {
		db.swapBuffers()
	}

	return toWrite
}

func (db *DoubleBuffer) swapBuffers() {
	if db.writePtr == &db.bufA {
		db.aStatus = BufferFull
		db.writePtr = &db.bufB
		*db.writePtr = (*db.writePtr)[:0]
		db.bStatus = BufferFilling
	} else {
		db.bStatus = BufferFull
		db.writePtr = &db.bufA
		*db.writePtr = (*db.writePtr)[:0]
		db.aStatus = BufferFilling
	}
	db.readPtr = db.fullBuffer()
}

func (db *DoubleBuffer) fullBuffer() *[]frame.Event {
	if db.aStatus == BufferFull {
		return &db.bufA
	}
	if db.bStatus == BufferFull {
		return &db.bufB
	}
	return nil
}

func (db *DoubleBuffer) ReadReady() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.aStatus == BufferFull || db.bStatus == BufferFull
}

func (db *DoubleBuffer) Read() []frame.Event {
	db.mu.Lock()
	defer db.mu.Unlock()

	var result []frame.Event

	if db.aStatus == BufferFull {
		result = db.bufA
		db.bufA = db.bufA[:0]
		db.aStatus = BufferEmpty
	} else if db.bStatus == BufferFull {
		result = db.bufB
		db.bufB = db.bufB[:0]
		db.bStatus = BufferEmpty
	}

	return result
}

func (db *DoubleBuffer) Flush() []frame.Event {
	db.mu.Lock()
	defer db.mu.Unlock()

	var result []frame.Event
	if len(*db.writePtr) > 0 {
		result = *db.writePtr
		*db.writePtr = (*db.writePtr)[:0]
	}
	return result
}

func (db *DoubleBuffer) Capacity() int {
	return db.capacity
}

func (db *DoubleBuffer) WriteLen() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(*db.writePtr)
}

type Ingestor struct {
	mapper   *frame.Mapper
	buffer   *DoubleBuffer
	source   io.Reader
	running  bool
	mu       sync.Mutex
	done     chan struct{}
	FrameOut chan *frame.Frame
	EventOut chan []frame.Event
}

func NewIngestor(src io.Reader, bufCapacity int) *Ingestor {
	return &Ingestor{
		mapper:   frame.NewMapper(),
		buffer:   NewDoubleBuffer(bufCapacity),
		source:   src,
		done:     make(chan struct{}),
		FrameOut: make(chan *frame.Frame, 256),
		EventOut: make(chan []frame.Event, 64),
	}
}

func (ing *Ingestor) Start() error {
	ing.mu.Lock()
	if ing.running {
		ing.mu.Unlock()
		return nil
	}
	ing.running = true
	ing.done = make(chan struct{})
	ing.mu.Unlock()

	go ing.readLoop()
	go ing.flushLoop()
	return nil
}

func (ing *Ingestor) Stop() {
	ing.mu.Lock()
	defer ing.mu.Unlock()
	if !ing.running {
		return
	}
	ing.running = false
	close(ing.done)

	remaining := ing.buffer.Flush()
	if len(remaining) > 0 {
		select {
		case ing.EventOut <- remaining:
		default:
		}
	}
}

func (ing *Ingestor) readLoop() {
	for {
		select {
		case <-ing.done:
			return
		default:
		}

		f, err := ing.mapper.ReadNext(ing.source)
		if err != nil {
			time.Sleep(100 * time.Microsecond)
			continue
		}

		select {
		case ing.FrameOut <- f:
		default:
		}

		if len(f.Events) > 0 {
			written := ing.buffer.Write(f.Events)
			if written < len(f.Events) {
				if ing.buffer.ReadReady() {
					events := ing.buffer.Read()
					if len(events) > 0 {
						ing.EventOut <- events
					}
				}
				ing.buffer.Write(f.Events[written:])
			}
		}
	}
}

func (ing *Ingestor) flushLoop() {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ing.done:
			return
		case <-ticker.C:
			if ing.buffer.ReadReady() {
				events := ing.buffer.Read()
				if len(events) > 0 {
					select {
					case ing.EventOut <- events:
					default:
					}
				}
			}
		}
	}
}

func (ing *Ingestor) BufferStats() (writeLen, capacity int) {
	return ing.buffer.WriteLen(), ing.buffer.Capacity()
}
