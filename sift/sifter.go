package sift

import (
	"sync"
	"sync/atomic"

	"github.com/qkd/bb84-sifter/frame"
	"github.com/qkd/bb84-sifter/qber"
)

type SiftedKey struct {
	Bits        []uint8
	Intensities []uint8
	Length      int
}

type Stats struct {
	TotalEvents   uint64
	SiftedBits    uint64
	RectSifted    uint64
	DiagSifted    uint64
	SignalBits    uint64
	DecoyBits     uint64
	VacuumBits    uint64
}

type Sifter struct {
	mu       sync.Mutex
	stats    Stats
	estimator *qber.Estimator
	keyBuf   []uint8
	intBuf   []uint8
	keyLen   int
	qberWindow int
	lastQBER  float64
}

func NewSifter(qberWindow int, useFFT bool) *Sifter {
	return &Sifter{
		estimator:  qber.NewEstimator(qberWindow, useFFT),
		keyBuf:     make([]uint8, 0, 65536),
		intBuf:     make([]uint8, 0, 65536),
		qberWindow: qberWindow,
	}
}

func (s *Sifter) Sift(alice, bob []frame.Event) SiftedKey {
	n := len(alice)
	if n > len(bob) {
		n = len(bob)
	}

	siftedBits := make([]uint8, 0, n/2)
	siftedInts := make([]uint8, 0, n/2)

	var rectCount, diagCount uint64
	var sigCount, decCount, vacCount uint64

	for i := 0; i < n; i++ {
		if alice[i].Basis == bob[i].Basis {
			siftedBits = append(siftedBits, alice[i].Bit)
			siftedInts = append(siftedInts, alice[i].Intensity)

			if alice[i].Basis == frame.BasisRectilinear {
				rectCount++
			} else {
				diagCount++
			}

			switch alice[i].Intensity {
			case frame.IntensitySignal:
				sigCount++
			case frame.IntensityDecoy:
				decCount++
			case frame.IntensityVacuum:
				vacCount++
			}
		}
	}

	s.mu.Lock()
	atomic.AddUint64(&s.stats.TotalEvents, uint64(n))
	atomic.AddUint64(&s.stats.SiftedBits, uint64(len(siftedBits)))
	atomic.AddUint64(&s.stats.RectSifted, rectCount)
	atomic.AddUint64(&s.stats.DiagSifted, diagCount)
	atomic.AddUint64(&s.stats.SignalBits, sigCount)
	atomic.AddUint64(&s.stats.DecoyBits, decCount)
	atomic.AddUint64(&s.stats.VacuumBits, vacCount)

	s.keyBuf = append(s.keyBuf, siftedBits...)
	s.intBuf = append(s.intBuf, siftedInts...)
	s.keyLen = len(s.keyBuf)
	s.mu.Unlock()

	if len(siftedBits) > 0 {
		bobSifted := make([]uint8, len(siftedBits))
		idx := 0
		for i := 0; i < n; i++ {
			if alice[i].Basis == bob[i].Basis {
				bobSifted[idx] = bob[i].Bit
				idx++
			}
		}
		result := s.estimator.Estimate(siftedBits, bobSifted, siftedInts)
		s.lastQBER = result.QBER
	}

	return SiftedKey{
		Bits:        siftedBits,
		Intensities: siftedInts,
		Length:      len(siftedBits),
	}
}

func (s *Sifter) SiftBases(aliceBases, bobBases []uint8, aliceBits, bobBits []uint8) ([]uint8, []uint8) {
	n := len(aliceBases)
	if n > len(bobBases) {
		n = len(bobBases)
	}

	siftedAlice := make([]uint8, 0, n/2)
	siftedBob := make([]uint8, 0, n/2)

	for i := 0; i < n; i++ {
		if aliceBases[i] == bobBases[i] {
			siftedAlice = append(siftedAlice, aliceBits[i])
			siftedBob = append(siftedBob, bobBits[i])
		}
	}

	return siftedAlice, siftedBob
}

func (s *Sifter) DrainKey(targetBits int) ([]uint8, []uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.keyLen < targetBits {
		return nil, nil
	}

	keyBits := make([]uint8, targetBits)
	intensities := make([]uint8, targetBits)
	copy(keyBits, s.keyBuf[:targetBits])
	copy(intensities, s.intBuf[:targetBits])

	s.keyBuf = s.keyBuf[targetBits:]
	s.intBuf = s.intBuf[targetBits:]
	s.keyLen = len(s.keyBuf)

	return keyBits, intensities
}

func (s *Sifter) PeekKey(bits int) ([]uint8, []uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.keyLen < bits {
		bits = s.keyLen
	}
	if bits == 0 {
		return nil, nil
	}

	keyBits := make([]uint8, bits)
	intensities := make([]uint8, bits)
	copy(keyBits, s.keyBuf[:bits])
	copy(intensities, s.intBuf[:bits])

	return keyBits, intensities
}

func (s *Sifter) AvailableBits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keyLen
}

func (s *Sifter) Stats() Stats {
	return Stats{
		TotalEvents: atomic.LoadUint64(&s.stats.TotalEvents),
		SiftedBits:  atomic.LoadUint64(&s.stats.SiftedBits),
		RectSifted:  atomic.LoadUint64(&s.stats.RectSifted),
		DiagSifted:  atomic.LoadUint64(&s.stats.DiagSifted),
		SignalBits:  atomic.LoadUint64(&s.stats.SignalBits),
		DecoyBits:   atomic.LoadUint64(&s.stats.DecoyBits),
		VacuumBits:  atomic.LoadUint64(&s.stats.VacuumBits),
	}
}

func (s *Sifter) QBER() float64 {
	return s.estimator.TotalQBER()
}

func (s *Sifter) LastQBER() float64 {
	return s.lastQBER
}

func (s *Sifter) QBERStats() qber.EstimateResult {
	return s.estimator.Stats()
}

func (s *Sifter) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = Stats{}
	s.keyBuf = s.keyBuf[:0]
	s.intBuf = s.intBuf[:0]
	s.keyLen = 0
	s.estimator.Reset()
	s.lastQBER = 0
}

type BasisReconciler struct {
	mu       sync.Mutex
	aliceBuf []frame.Event
	bobBuf   []frame.Event
	sifter   *Sifter
}

func NewBasisReconciler(qberWindow int, useFFT bool) *BasisReconciler {
	return &BasisReconciler{
		aliceBuf: make([]frame.Event, 0, 4096),
		bobBuf:   make([]frame.Event, 0, 4096),
		sifter:   NewSifter(qberWindow, useFFT),
	}
}

func (br *BasisReconciler) AddAlice(events []frame.Event) {
	br.mu.Lock()
	br.aliceBuf = append(br.aliceBuf, events...)
	br.mu.Unlock()
}

func (br *BasisReconciler) AddBob(events []frame.Event) {
	br.mu.Lock()
	br.bobBuf = append(br.bobBuf, events...)
	br.mu.Unlock()
}

func (br *BasisReconciler) Process() SiftedKey {
	br.mu.Lock()
	defer br.mu.Unlock()

	n := len(br.aliceBuf)
	if n > len(br.bobBuf) {
		n = len(br.bobBuf)
	}
	if n == 0 {
		return SiftedKey{}
	}

	result := br.sifter.Sift(br.aliceBuf[:n], br.bobBuf[:n])

	br.aliceBuf = br.aliceBuf[n:]
	br.bobBuf = br.bobBuf[n:]

	return result
}

func (br *BasisReconciler) Sifter() *Sifter {
	return br.sifter
}
