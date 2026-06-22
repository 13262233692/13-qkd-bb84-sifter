package sim

import (
	"math/rand"
	"time"

	"github.com/qkd/bb84-sifter/frame"
)

type Generator struct {
	rng             *rand.Rand
	qber            float64
	birefringence   float64
	darkCountRate   float64
	clockRate       float64
	seqNum          uint64
}

type Config struct {
	Seed          int64
	QBER          float64
	Birefringence float64
	DarkCountRate float64
	ClockRate     float64
}

func NewGenerator(cfg Config) *Generator {
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	qber := cfg.QBER
	if qber == 0 {
		qber = 0.02
	}
	clockRate := cfg.ClockRate
	if clockRate == 0 {
		clockRate = 1e9
	}
	return &Generator{
		rng:           rand.New(rand.NewSource(seed)),
		qber:          qber,
		birefringence: cfg.Birefringence,
		darkCountRate: cfg.DarkCountRate,
		clockRate:     clockRate,
	}
}

func (g *Generator) GenerateAliceEvents(count int) []frame.Event {
	events := make([]frame.Event, count)
	ts := uint64(g.rng.Int63n(1000000))
	period := uint64(1e9 / g.clockRate)
	jitter := period / 10
	if jitter < 1 {
		jitter = 1
	}

	for i := 0; i < count; i++ {
		events[i].Timestamp = ts
		events[i].Basis = uint8(g.rng.Intn(2))
		events[i].Bit = uint8(g.rng.Intn(2))

		r := g.rng.Float64()
		switch {
		case r < 0.8:
			events[i].Intensity = frame.IntensitySignal
		case r < 0.95:
			events[i].Intensity = frame.IntensityDecoy
		default:
			events[i].Intensity = frame.IntensityVacuum
		}

		ts += period + uint64(g.rng.Int63n(int64(jitter)))
	}

	return events
}

func (g *Generator) GenerateBobEvents(aliceEvents []frame.Event) []frame.Event {
	bobEvents := make([]frame.Event, len(aliceEvents))

	for i, ae := range aliceEvents {
		be := frame.Event{
			Timestamp: ae.Timestamp + uint64(g.rng.Int63n(100)),
			Intensity: ae.Intensity,
		}

		be.Basis = uint8(g.rng.Intn(2))

		if g.rng.Float64() < g.birefringence {
			be.Basis = 1 - be.Basis
		}

		if be.Basis == ae.Basis {
			if g.rng.Float64() < g.qber {
				be.Bit = 1 - ae.Bit
			} else {
				be.Bit = ae.Bit
			}
		} else {
			be.Bit = uint8(g.rng.Intn(2))
		}

		bobEvents[i] = be
	}

	return bobEvents
}

func (g *Generator) GenerateFrame(eventCount int) *frame.Frame {
	f := frame.NewFrame(g.seqNum, eventCount)
	g.seqNum++

	events := g.GenerateAliceEvents(eventCount)
	for _, e := range events {
		f.Append(e)
	}

	return f
}

func (g *Generator) GeneratePairedFrames(eventCount int) (*frame.Frame, *frame.Frame) {
	alice := g.GenerateFrame(eventCount)
	bob := frame.NewFrame(g.seqNum, eventCount)
	g.seqNum++

	bobEvents := g.GenerateBobEvents(alice.Events)
	for _, e := range bobEvents {
		bob.Append(e)
	}

	return alice, bob
}

type StreamSource struct {
	gen    *Generator
	events []frame.Event
	idx    int
}

func NewStreamSource(gen *Generator, pregenerateCount int) *StreamSource {
	return &StreamSource{
		gen:    gen,
		events: gen.GenerateAliceEvents(pregenerateCount),
	}
}

func (s *StreamSource) Read(p []byte) (n int, err error) {
	return 0, nil
}
