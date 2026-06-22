package ingest

import (
	"io"
	"sync"
	"sync/atomic"
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
	return db
}

func (db *DoubleBuffer) Write(events []frame.Event) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(events) == 0 {
		return 0
	}

	var writeBuf *[]frame.Event
	var writeStatus *BufferStatus

	if db.aStatus != BufferFull {
		writeBuf = &db.bufA
		writeStatus = &db.aStatus
	} else if db.bStatus != BufferFull {
		writeBuf = &db.bufB
		writeStatus = &db.bStatus
	} else {
		return 0
	}

	if *writeStatus == BufferEmpty {
		*writeStatus = BufferFilling
	}

	available := db.capacity - len(*writeBuf)
	if available <= 0 {
		*writeStatus = BufferFull
		return 0
	}

	toWrite := len(events)
	if toWrite > available {
		toWrite = available
	}

	if toWrite > 0 {
		*writeBuf = append(*writeBuf, events[:toWrite]...)
		db.writeIdx += toWrite
	}

	if len(*writeBuf) >= db.capacity {
		*writeStatus = BufferFull
	}

	return toWrite
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
		db.bufA = make([]frame.Event, 0, db.capacity)
		db.aStatus = BufferEmpty
	} else if db.bStatus == BufferFull {
		result = db.bufB
		db.bufB = make([]frame.Event, 0, db.capacity)
		db.bStatus = BufferEmpty
	}

	return result
}

func (db *DoubleBuffer) Flush() []frame.Event {
	db.mu.Lock()
	defer db.mu.Unlock()

	var result []frame.Event

	if len(db.bufA) > 0 {
		result = db.bufA
		db.bufA = make([]frame.Event, 0, db.capacity)
		db.aStatus = BufferEmpty
	}
	if len(db.bufB) > 0 {
		result = append(result, db.bufB...)
		db.bufB = make([]frame.Event, 0, db.capacity)
		db.bStatus = BufferEmpty
	}

	return result
}

func (db *DoubleBuffer) Capacity() int {
	return db.capacity
}

func (db *DoubleBuffer) WriteLen() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.bufA) + len(db.bufB)
}

type Ingestor struct {
	scanner    *frame.RingScanner
	buffer     *DoubleBuffer
	source     io.Reader
	running    int32
	started    int32
	stopOnce   sync.Once
	startOnce  sync.Once
	done       chan struct{}
	doneMu     sync.Mutex
	FrameOut   chan *frame.Frame
	EventOut   chan []frame.Event
	wg         sync.WaitGroup
	scanErrs   uint64
}

func NewIngestor(src io.Reader, bufCapacity int) *Ingestor {
	return &Ingestor{
		source:   src,
		scanner:  frame.NewRingScanner(src, frame.DefaultRingCap),
		buffer:   NewDoubleBuffer(bufCapacity),
		done:     make(chan struct{}),
		FrameOut: make(chan *frame.Frame, 256),
		EventOut: make(chan []frame.Event, 64),
	}
}

func (ing *Ingestor) Start() error {
	if !atomic.CompareAndSwapInt32(&ing.started, 0, 1) {
		return nil
	}

	ing.startOnce.Do(func() {
		ing.doneMu.Lock()
		ing.done = make(chan struct{})
		ing.doneMu.Unlock()

		atomic.StoreInt32(&ing.running, 1)

		if err := ing.scanner.Start(); err != nil {
			atomic.StoreInt32(&ing.started, 0)
			atomic.StoreInt32(&ing.running, 0)
			return
		}

		ing.wg.Add(2)
		go ing.frameDispatchLoop()
		go ing.flushLoop()
	})

	return nil
}

func (ing *Ingestor) Stop() {
	ing.stopOnce.Do(func() {
		atomic.StoreInt32(&ing.running, 0)

		ing.doneMu.Lock()
		if ing.done != nil {
			select {
			case <-ing.done:
			default:
				close(ing.done)
			}
		}
		ing.doneMu.Unlock()

		ing.scanner.Stop()

		ing.wg.Wait()

		remaining := ing.buffer.Flush()
		if len(remaining) > 0 {
			ing.trySendEvents(remaining)
		}

		close(ing.FrameOut)
		close(ing.EventOut)
	})
}

func (ing *Ingestor) isRunning() bool {
	return atomic.LoadInt32(&ing.running) == 1
}

func (ing *Ingestor) frameDispatchLoop() {
	defer ing.wg.Done()

	for ing.isRunning() {
		select {
		case <-ing.done:
			return
		case f, ok := <-ing.scanner.FrameChan():
			if !ok {
				return
			}
			if f == nil {
				continue
			}

			ing.trySendFrame(f)

			if f.Len() > 0 {
				ing.processFrameEvents(f.Events)
			}
		}
	}
}

func (ing *Ingestor) trySendFrame(f *frame.Frame) {
	select {
	case <-ing.done:
		return
	case ing.FrameOut <- f:
	default:
	}
}

func (ing *Ingestor) trySendEvents(events []frame.Event) {
	select {
	case <-ing.done:
		return
	case ing.EventOut <- events:
	default:
	}
}

func (ing *Ingestor) processFrameEvents(events []frame.Event) {
	if len(events) == 0 {
		return
	}

	written := ing.buffer.Write(events)
	for written < len(events) && ing.isRunning() {
		if ing.buffer.ReadReady() {
			batch := ing.buffer.Read()
			if len(batch) > 0 {
				ing.trySendEvents(batch)
			}
		}
		remaining := events[written:]
		w := ing.buffer.Write(remaining)
		if w == 0 {
			time.Sleep(50 * time.Microsecond)
		}
		written += w
	}
}

func (ing *Ingestor) flushLoop() {
	defer ing.wg.Done()

	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for ing.isRunning() {
		select {
		case <-ing.done:
			return
		case <-ticker.C:
			if ing.buffer.ReadReady() {
				batch := ing.buffer.Read()
				if len(batch) > 0 {
					ing.trySendEvents(batch)
				}
			}
		}
	}
}

func (ing *Ingestor) BufferStats() (writeLen, capacity int) {
	return ing.buffer.WriteLen(), ing.buffer.Capacity()
}

func (ing *Ingestor) ScannerStats() (readable, writable, errCount, resets int) {
	return ing.scanner.Stats()
}
