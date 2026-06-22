package qber

import (
	"math"
	"sync"

	"github.com/qkd/bb84-sifter/frame"
)

type EstimateResult struct {
	QBER            float64
	TotalBits       int
	ErrorBits       int
	SignalQBER      float64
	DecoyQBER       float64
	VacuumQBER      float64
}

type Estimator struct {
	mu             sync.Mutex
	totalBits      int
	errorBits      int
	windowSize     int
	useFFT         bool
	sampleRate     float64

	signalTotal   int
	signalErrors  int
	decoyTotal    int
	decoyErrors   int
	vacuumTotal   int
	vacuumErrors  int

	history        []float64
	maxHistory     int
}

func NewEstimator(windowSize int, useFFT bool) *Estimator {
	if windowSize <= 0 {
		windowSize = 10000
	}
	return &Estimator{
		windowSize: windowSize,
		useFFT:     useFFT,
		maxHistory: 100,
		history:    make([]float64, 0, 100),
	}
}

func (e *Estimator) Estimate(aliceBits, bobBits []uint8, intensities []uint8) EstimateResult {
	if len(aliceBits) != len(bobBits) || len(aliceBits) == 0 {
		return EstimateResult{}
	}

	n := len(aliceBits)
	errors := 0

	if e.useFFT && n >= 64 {
		errors = e.fftErrorCount(aliceBits, bobBits)
	} else {
		for i := 0; i < n; i++ {
			if aliceBits[i] != bobBits[i] {
				errors++
			}
		}
	}

	sigTotal, sigErr := 0, 0
	decTotal, decErr := 0, 0
	vacTotal, vacErr := 0, 0

	if len(intensities) == n {
		for i := 0; i < n; i++ {
			isErr := aliceBits[i] != bobBits[i]
			switch intensities[i] {
			case frame.IntensitySignal:
				sigTotal++
				if isErr {
					sigErr++
				}
			case frame.IntensityDecoy:
				decTotal++
				if isErr {
					decErr++
				}
			case frame.IntensityVacuum:
				vacTotal++
				if isErr {
					vacErr++
				}
			}
		}
	}

	e.mu.Lock()
	e.totalBits += n
	e.errorBits += errors
	e.signalTotal += sigTotal
	e.signalErrors += sigErr
	e.decoyTotal += decTotal
	e.decoyErrors += decErr
	e.vacuumTotal += vacTotal
	e.vacuumErrors += vacErr

	qber := float64(errors) / float64(n)
	e.history = append(e.history, qber)
	if len(e.history) > e.maxHistory {
		e.history = e.history[1:]
	}
	e.mu.Unlock()

	result := EstimateResult{
		QBER:       qber,
		TotalBits:  n,
		ErrorBits:  errors,
		SignalQBER: safeDiv(sigErr, sigTotal),
		DecoyQBER:  safeDiv(decErr, decTotal),
		VacuumQBER: safeDiv(vacErr, vacTotal),
	}

	return result
}

func (e *Estimator) fftErrorCount(alice, bob []uint8) int {
	n := len(alice)
	aliceF := BitsToFloat(alice)
	bobF := BitsToFloat(bob)

	corr := CrossCorrelation(aliceF, bobF)

	peakIdx := 0
	peakVal := math.Abs(corr[0])
	for i := 1; i < len(corr); i++ {
		val := math.Abs(corr[i])
		if val > peakVal {
			peakVal = val
			peakIdx = i
		}
	}

	agreement := corr[peakIdx]
	total := float64(n)
	expectedMatch := (agreement + total) / 2
	errorCount := int(total - expectedMatch)

	if errorCount < 0 {
		errorCount = 0
	}
	if errorCount > n {
		errorCount = n
	}

	return errorCount
}

func (e *Estimator) EstimateFromEvents(aliceEvents, bobEvents []frame.Event) EstimateResult {
	n := len(aliceEvents)
	if n > len(bobEvents) {
		n = len(bobEvents)
	}

	aliceBits := make([]uint8, n)
	bobBits := make([]uint8, n)
	intensities := make([]uint8, n)

	for i := 0; i < n; i++ {
		aliceBits[i] = aliceEvents[i].Bit
		bobBits[i] = bobEvents[i].Bit
		intensities[i] = aliceEvents[i].Intensity
	}

	return e.Estimate(aliceBits, bobBits, intensities)
}

func (e *Estimator) TotalQBER() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return safeDiv(e.errorBits, e.totalBits)
}

func (e *Estimator) Stats() EstimateResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return EstimateResult{
		QBER:       safeDiv(e.errorBits, e.totalBits),
		TotalBits:  e.totalBits,
		ErrorBits:  e.errorBits,
		SignalQBER: safeDiv(e.signalErrors, e.signalTotal),
		DecoyQBER:  safeDiv(e.decoyErrors, e.decoyTotal),
		VacuumQBER: safeDiv(e.vacuumErrors, e.vacuumTotal),
	}
}

func (e *Estimator) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.totalBits = 0
	e.errorBits = 0
	e.signalTotal = 0
	e.signalErrors = 0
	e.decoyTotal = 0
	e.decoyErrors = 0
	e.vacuumTotal = 0
	e.vacuumErrors = 0
	e.history = e.history[:0]
}

func safeDiv(num, den int) float64 {
	if den == 0 {
		return 0.0
	}
	return float64(num) / float64(den)
}

type DarkCountEstimator struct {
	mu          sync.Mutex
	darkRate    float64
	windowTime  float64
	detectorEff float64
}

func NewDarkCountEstimator(darkRate, windowTime, detEff float64) *DarkCountEstimator {
	return &DarkCountEstimator{
		darkRate:    darkRate,
		windowTime:  windowTime,
		detectorEff: detEff,
	}
}

func (d *DarkCountEstimator) ExpectedDarkCounts(rate float64, duration float64) float64 {
	return d.darkRate * duration
}

func (d *DarkCountEstimator) BirefringenceError(drift float64) float64 {
	return 0.5 * (1 - math.Cos(drift*math.Pi))
}
