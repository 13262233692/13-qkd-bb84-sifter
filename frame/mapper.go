package frame

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	ErrInvalidMagic   = errors.New("frame: invalid magic number")
	ErrShortHeader    = errors.New("frame: header too short")
	ErrShortPayload   = errors.New("frame: payload too short")
	ErrEventCountOOB  = errors.New("frame: event count out of bounds")
	ErrOffsetOverflow = errors.New("frame: offset overflow during event parse")
	ErrBufferFull     = errors.New("frame: ring scanner buffer full")
	ErrClosed         = errors.New("frame: ring scanner closed")
)

const (
	eventByteSize  = 12
	headerByteSize = 14
	MaxEventCount  = 1 << 15
	DefaultRingCap = 1 << 22
)

type Mapper struct {
	byteOrder binary.ByteOrder
}

func NewMapper() *Mapper {
	return &Mapper{
		byteOrder: binary.LittleEndian,
	}
}

func (m *Mapper) ParseHeader(data []byte) (seqNum uint64, eventCount uint16, err error) {
	if len(data) < headerByteSize {
		return 0, 0, ErrShortHeader
	}
	magic := m.byteOrder.Uint32(data[0:4])
	if magic != FrameMagic {
		return 0, 0, ErrInvalidMagic
	}
	seqNum = m.byteOrder.Uint64(data[4:12])
	eventCount = m.byteOrder.Uint16(data[12:14])
	if eventCount > MaxEventCount {
		return 0, 0, ErrEventCountOOB
	}
	return seqNum, eventCount, nil
}

func (m *Mapper) ParseEvents(data []byte, eventCount uint16) ([]Event, error) {
	if eventCount > MaxEventCount {
		return nil, ErrEventCountOOB
	}

	expectedLen := int(eventCount) * eventByteSize
	if len(data) < expectedLen {
		return nil, ErrShortPayload
	}

	events := make([]Event, 0, eventCount)
	offset := 0
	dataLen := len(data)

	for i := uint16(0); i < eventCount; i++ {
		if offset+eventByteSize > dataLen {
			return nil, ErrOffsetOverflow
		}

		evt := Event{}
		evt.Timestamp = m.byteOrder.Uint64(data[offset : offset+8])
		offset += 8

		if offset >= dataLen {
			return nil, ErrOffsetOverflow
		}
		basisByte := data[offset]
		offset++
		evt.Basis = (basisByte >> 6) & 0x01
		evt.Bit = (basisByte >> 5) & 0x01
		evt.Intensity = (basisByte >> 3) & 0x03

		offset += 3
		events = append(events, evt)
	}

	return events, nil
}

func (m *Mapper) ParseFrame(data []byte) (*Frame, error) {
	if len(data) < headerByteSize {
		return nil, ErrShortHeader
	}

	seqNum, eventCount, err := m.ParseHeader(data)
	if err != nil {
		return nil, err
	}

	expectedTotal := headerByteSize + int(eventCount)*eventByteSize
	if len(data) < expectedTotal {
		return nil, ErrShortPayload
	}

	payload := data[headerByteSize:expectedTotal]
	events, err := m.ParseEvents(payload, eventCount)
	if err != nil {
		return nil, err
	}

	return &Frame{
		Magic:      FrameMagic,
		SeqNum:     seqNum,
		EventCount: eventCount,
		Events:     events,
	}, nil
}

func (m *Mapper) FrameSize(eventCount uint16) int {
	return headerByteSize + int(eventCount)*eventByteSize
}

func (m *Mapper) ReadNext(r io.Reader) (*Frame, error) {
	headerBuf := make([]byte, headerByteSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	seqNum, eventCount, err := m.ParseHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	if eventCount > MaxEventCount {
		return nil, ErrEventCountOOB
	}

	payloadSize := int(eventCount) * eventByteSize
	if payloadSize <= 0 {
		return &Frame{
			Magic:      FrameMagic,
			SeqNum:     seqNum,
			EventCount: 0,
			Events:     []Event{},
		}, nil
	}

	payloadBuf := make([]byte, payloadSize)
	if _, err := io.ReadFull(r, payloadBuf); err != nil {
		return nil, err
	}

	events, err := m.ParseEvents(payloadBuf, eventCount)
	if err != nil {
		return nil, err
	}

	return &Frame{
		Magic:      FrameMagic,
		SeqNum:     seqNum,
		EventCount: eventCount,
		Events:     events,
	}, nil
}

func (m *Mapper) SerializeFrame(f *Frame) []byte {
	if f.EventCount > MaxEventCount {
		f.EventCount = MaxEventCount
	}
	totalSize := m.FrameSize(f.EventCount)
	buf := make([]byte, totalSize)

	m.byteOrder.PutUint32(buf[0:4], f.Magic)
	m.byteOrder.PutUint64(buf[4:12], f.SeqNum)
	m.byteOrder.PutUint16(buf[12:14], f.EventCount)

	offset := headerByteSize
	bufLen := len(buf)
	for _, e := range f.Events {
		if offset+8 > bufLen {
			break
		}
		m.byteOrder.PutUint64(buf[offset:offset+8], e.Timestamp)
		offset += 8

		if offset >= bufLen {
			break
		}
		var basisByte uint8
		basisByte |= (e.Basis & 0x01) << 6
		basisByte |= (e.Bit & 0x01) << 5
		basisByte |= (e.Intensity & 0x03) << 3
		buf[offset] = basisByte
		offset++

		offset += 3
	}

	return buf
}

type RingScanner struct {
	mu        sync.Mutex
	mapper    *Mapper
	buf       []byte
	r         int
	w         int
	capacity  int
	closed    bool
	source    io.Reader
	wg        sync.WaitGroup
	done      chan struct{}
	frameOut  chan *Frame
	errCount  uint64
	resets    uint64
}

func NewRingScanner(source io.Reader, capacity int) *RingScanner {
	if capacity <= 0 {
		capacity = DefaultRingCap
	}
	if capacity < headerByteSize*2 {
		capacity = headerByteSize * 2
	}

	return &RingScanner{
		mapper:   NewMapper(),
		buf:      make([]byte, capacity),
		r:        0,
		w:        0,
		capacity: capacity,
		source:   source,
		done:     make(chan struct{}),
		frameOut: make(chan *Frame, 256),
	}
}

func (rs *RingScanner) readable() int {
	if rs.w >= rs.r {
		return rs.w - rs.r
	}
	return rs.capacity - rs.r + rs.w
}

func (rs *RingScanner) writable() int {
	return rs.capacity - rs.readable() - 1
}

func (rs *RingScanner) readAt(dst []byte, offset int) int {
	n := len(dst)
	avail := rs.readable()
	if n > avail {
		n = avail
	}
	if n <= 0 {
		return 0
	}

	start := (rs.r + offset) % rs.capacity
	first := rs.capacity - start
	if first >= n {
		copy(dst, rs.buf[start:start+n])
	} else {
		copy(dst, rs.buf[start:])
		copy(dst[first:], rs.buf[:n-first])
	}
	return n
}

func (rs *RingScanner) consume(n int) {
	rs.r = (rs.r + n) % rs.capacity
}

func (rs *RingScanner) writeFromSource() (int, error) {
	rs.mu.Unlock()

	buf := make([]byte, rs.writable())
	n, err := rs.source.Read(buf)

	rs.mu.Lock()
	if n <= 0 {
		return 0, err
	}

	first := rs.capacity - rs.w
	if first >= n {
		copy(rs.buf[rs.w:], buf[:n])
	} else {
		copy(rs.buf[rs.w:], buf[:first])
		copy(rs.buf, buf[first:n])
	}
	rs.w = (rs.w + n) % rs.capacity

	return n, err
}

func (rs *RingScanner) tryParseFrame() *Frame {
	if rs.readable() < headerByteSize {
		return nil
	}

	headerBuf := make([]byte, headerByteSize)
	if rs.readAt(headerBuf, 0) < headerByteSize {
		return nil
	}

	_, eventCount, err := rs.mapper.ParseHeader(headerBuf)
	if err != nil {
		rs.errCount++
		rs.consume(1)
		return nil
	}

	frameSize := rs.mapper.FrameSize(eventCount)
	if rs.readable() < frameSize {
		return nil
	}

	frameBuf := make([]byte, frameSize)
	if rs.readAt(frameBuf, 0) < frameSize {
		return nil
	}

	f, err := rs.mapper.ParseFrame(frameBuf)
	if err != nil {
		rs.errCount++
		rs.consume(1)
		return nil
	}

	rs.consume(frameSize)
	return f
}

func (rs *RingScanner) Start() error {
	rs.mu.Lock()
	if rs.closed {
		rs.mu.Unlock()
		return ErrClosed
	}
	rs.mu.Unlock()

	rs.wg.Add(1)
	go rs.scanLoop()
	return nil
}

func (rs *RingScanner) Stop() {
	rs.mu.Lock()
	if rs.closed {
		rs.mu.Unlock()
		return
	}
	rs.closed = true
	close(rs.done)
	rs.mu.Unlock()

	rs.wg.Wait()
	close(rs.frameOut)
}

func (rs *RingScanner) scanLoop() {
	defer rs.wg.Done()

	for {
		select {
		case <-rs.done:
			return
		default:
		}

		rs.mu.Lock()
		if rs.writable() > 0 {
			rs.mu.Unlock()
			readBuf := make([]byte, 65536)
			n, err := rs.source.Read(readBuf)
			rs.mu.Lock()
			if n > 0 {
				if rs.writable() < n {
					rs.r = (rs.r + n - rs.writable()) % rs.capacity
					rs.resets++
				}
				first := rs.capacity - rs.w
				if first >= n {
					copy(rs.buf[rs.w:], readBuf[:n])
				} else {
					copy(rs.buf[rs.w:], readBuf[:first])
					copy(rs.buf, readBuf[first:n])
				}
				rs.w = (rs.w + n) % rs.capacity
			}
			if err != nil {
				rs.errCount++
				rs.mu.Unlock()
				select {
				case <-rs.done:
					return
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
		} else {
			rs.r = (rs.r + 1) % rs.capacity
			rs.resets++
		}

		for {
			f := rs.tryParseFrame()
			if f == nil {
				break
			}
			rs.mu.Unlock()
			select {
			case rs.frameOut <- f:
			case <-rs.done:
				rs.mu.Lock()
				return
			default:
			}
			rs.mu.Lock()
		}
		rs.mu.Unlock()
	}
}

func (rs *RingScanner) FrameChan() <-chan *Frame {
	return rs.frameOut
}

func (rs *RingScanner) Stats() (readable, writable, errCount, resets int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.readable(), rs.writable(), int(rs.errCount), int(rs.resets)
}
