package frame

const (
	FrameHeaderSize = 16
	FrameMagic      = uint32(0x514B4431)
	MaxEventsPerFrame = 4096
)

const (
	BasisRectilinear uint8 = 0
	BasisDiagonal    uint8 = 1
)

const (
	IntensitySignal uint8 = 0
	IntensityDecoy  uint8 = 1
	IntensityVacuum uint8 = 2
)

type Event struct {
	Timestamp uint64
	Basis     uint8
	Bit       uint8
	Intensity uint8
}

type Frame struct {
	Magic      uint32
	SeqNum     uint64
	EventCount uint16
	Events     []Event
}

func NewFrame(seqNum uint64, capacity int) *Frame {
	if capacity <= 0 {
		capacity = MaxEventsPerFrame
	}
	return &Frame{
		Magic:  FrameMagic,
		SeqNum: seqNum,
		Events: make([]Event, 0, capacity),
	}
}

func (f *Frame) Append(e Event) {
	f.Events = append(f.Events, e)
	f.EventCount = uint16(len(f.Events))
}

func (f *Frame) Len() int {
	return len(f.Events)
}
