package frame

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	ErrInvalidMagic  = errors.New("frame: invalid magic number")
	ErrShortHeader   = errors.New("frame: header too short")
	ErrShortPayload  = errors.New("frame: payload too short")
)

const (
	eventByteSize = 12
	headerByteSize = 14
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
	return seqNum, eventCount, nil
}

func (m *Mapper) ParseEvents(data []byte, eventCount uint16) ([]Event, error) {
	expectedLen := int(eventCount) * eventByteSize
	if len(data) < expectedLen {
		return nil, ErrShortPayload
	}

	events := make([]Event, eventCount)
	offset := 0

	for i := uint16(0); i < eventCount; i++ {
		events[i].Timestamp = m.byteOrder.Uint64(data[offset : offset+8])
		offset += 8

		basisByte := data[offset]
		offset++
		events[i].Basis = (basisByte >> 6) & 0x01
		events[i].Bit = (basisByte >> 5) & 0x01
		events[i].Intensity = (basisByte >> 3) & 0x03

		offset += 3
	}

	return events, nil
}

func (m *Mapper) ParseFrame(data []byte) (*Frame, error) {
	seqNum, eventCount, err := m.ParseHeader(data)
	if err != nil {
		return nil, err
	}

	payload := data[headerByteSize:]
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

	payloadSize := int(eventCount) * eventByteSize
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
	totalSize := m.FrameSize(f.EventCount)
	buf := make([]byte, totalSize)

	m.byteOrder.PutUint32(buf[0:4], f.Magic)
	m.byteOrder.PutUint64(buf[4:12], f.SeqNum)
	m.byteOrder.PutUint16(buf[12:14], f.EventCount)

	offset := headerByteSize
	for _, e := range f.Events {
		m.byteOrder.PutUint64(buf[offset:offset+8], e.Timestamp)
		offset += 8

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
