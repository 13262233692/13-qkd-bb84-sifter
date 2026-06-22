package sim

import (
	"io"
	"sync"
	"time"

	"github.com/qkd/bb84-sifter/frame"
)

type StreamMode int

const (
	ModeAlice StreamMode = iota
	ModeBob
)

type frameBatch struct {
	aliceData []byte
	bobData   []byte
}

type PairedSource struct {
	mu        sync.Mutex
	gen       *Generator
	mapper    *frame.Mapper
	batches   []frameBatch
	batchSize int
	seqNum    uint64
}

func NewPairedSource(gen *Generator, batchSize int) *PairedSource {
	if batchSize <= 0 {
		batchSize = 256
	}
	return &PairedSource{
		gen:       gen,
		mapper:    frame.NewMapper(),
		batches:   make([]frameBatch, 0, 16),
		batchSize: batchSize,
	}
}

func (ps *PairedSource) generateBatch() frameBatch {
	aliceEvents := ps.gen.GenerateAliceEvents(ps.batchSize)
	bobEvents := ps.gen.GenerateBobEvents(aliceEvents)

	aliceFrame := frame.NewFrame(ps.seqNum, len(aliceEvents))
	bobFrame := frame.NewFrame(ps.seqNum, len(bobEvents))
	ps.seqNum++

	for _, e := range aliceEvents {
		aliceFrame.Append(e)
	}
	for _, e := range bobEvents {
		bobFrame.Append(e)
	}

	return frameBatch{
		aliceData: ps.mapper.SerializeFrame(aliceFrame),
		bobData:   ps.mapper.SerializeFrame(bobFrame),
	}
}

func (ps *PairedSource) ensureBatch(idx int) {
	for len(ps.batches) <= idx {
		ps.batches = append(ps.batches, ps.generateBatch())
	}
}

func (ps *PairedSource) AliceReader() io.Reader {
	return &pairedReader{
		source: ps,
		mode:   ModeAlice,
	}
}

func (ps *PairedSource) BobReader() io.Reader {
	return &pairedReader{
		source: ps,
		mode:   ModeBob,
	}
}

type pairedReader struct {
	source    *PairedSource
	mode      StreamMode
	batchIdx  int
	posInBuf  int
}

func (pr *pairedReader) Read(p []byte) (n int, err error) {
	pr.source.mu.Lock()
	defer pr.source.mu.Unlock()

	pr.source.ensureBatch(pr.batchIdx)

	var batchData []byte
	switch pr.mode {
	case ModeAlice:
		batchData = pr.source.batches[pr.batchIdx].aliceData
	case ModeBob:
		batchData = pr.source.batches[pr.batchIdx].bobData
	}

	if pr.posInBuf >= len(batchData) {
		pr.batchIdx++
		pr.posInBuf = 0
		pr.source.ensureBatch(pr.batchIdx)
		switch pr.mode {
		case ModeAlice:
			batchData = pr.source.batches[pr.batchIdx].aliceData
		case ModeBob:
			batchData = pr.source.batches[pr.batchIdx].bobData
		}
	}

	n = copy(p, batchData[pr.posInBuf:])
	pr.posInBuf += n
	return n, nil
}

type ByteReader struct {
	data []byte
	idx  int
}

func NewByteReader(data []byte) *ByteReader {
	return &ByteReader{data: data}
}

func (br *ByteReader) Read(p []byte) (n int, err error) {
	if br.idx >= len(br.data) {
		return 0, io.EOF
	}
	n = copy(p, br.data[br.idx:])
	br.idx += n
	return n, nil
}

func (br *ByteReader) Reset() {
	br.idx = 0
}

type FrameStream struct {
	mu       sync.Mutex
	mapper   *frame.Mapper
	gen      *Generator
	mode     StreamMode
	rate     float64
	buf      []byte
	idx      int
	running  bool
	stopChan chan struct{}
}

func NewFrameStream(gen *Generator, mode StreamMode, rate float64) *FrameStream {
	return &FrameStream{
		mapper:   frame.NewMapper(),
		gen:      gen,
		mode:     mode,
		rate:     rate,
		stopChan: make(chan struct{}),
	}
}

func (fs *FrameStream) Read(p []byte) (n int, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if len(fs.buf) == 0 || fs.idx >= len(fs.buf) {
		aliceEvents := fs.gen.GenerateAliceEvents(256)
		var events []frame.Event

		switch fs.mode {
		case ModeAlice:
			events = aliceEvents
		case ModeBob:
			events = fs.gen.GenerateBobEvents(aliceEvents)
		}

		f := frame.NewFrame(0, len(events))
		for _, e := range events {
			f.Append(e)
		}

		fs.buf = fs.mapper.SerializeFrame(f)
		fs.idx = 0

		if fs.rate > 0 {
			frameTime := time.Duration(float64(len(events)) / fs.rate * float64(time.Second))
			fs.mu.Unlock()
			time.Sleep(frameTime)
			fs.mu.Lock()
		}
	}

	n = copy(p, fs.buf[fs.idx:])
	fs.idx += n

	return n, nil
}

func (fs *FrameStream) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.running {
		return nil
	}
	fs.running = false
	close(fs.stopChan)
	return nil
}

type DualStream struct {
	Alice *FrameStream
	Bob   *FrameStream
}

func NewDualStream(gen *Generator, rate float64) *DualStream {
	return &DualStream{
		Alice: NewFrameStream(gen, ModeAlice, rate),
		Bob:   NewFrameStream(gen, ModeBob, rate),
	}
}

type InfiniteStream struct {
	gen  *Generator
	mode StreamMode
	buf  []byte
	pos  int
	mu   sync.Mutex
}

func NewInfiniteStream(gen *Generator, mode StreamMode) *InfiniteStream {
	return &InfiniteStream{
		gen:  gen,
		mode: mode,
	}
}

func (s *InfiniteStream) Read(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pos >= len(s.buf) {
		aliceEvents := s.gen.GenerateAliceEvents(1024)
		var events []frame.Event
		switch s.mode {
		case ModeAlice:
			events = aliceEvents
		case ModeBob:
			events = s.gen.GenerateBobEvents(aliceEvents)
		}

		mapper := frame.NewMapper()
		f := frame.NewFrame(0, len(events))
		for _, e := range events {
			f.Append(e)
		}
		s.buf = mapper.SerializeFrame(f)
		s.pos = 0
	}

	n = copy(p, s.buf[s.pos:])
	s.pos += n
	return n, nil
}
