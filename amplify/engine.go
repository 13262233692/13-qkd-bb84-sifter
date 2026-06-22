package amplify

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qkd/bb84-sifter/output"
)

const (
	QBERCriticalThreshold = 0.075
	DefaultPrivacyBudget  = 0.2
)

type CircuitBreakerState int

const (
	BreakerClosed   CircuitBreakerState = iota
	BreakerOpen
)

type AmplifyResult struct {
	InputBits  int
	OutputBits int
	QBER       float64
	Discarded  bool
	BlockID    int
	Duration   time.Duration
}

type EngineConfig struct {
	InputBits     int
	OutputBits    int
	PrivacyBudget float64
	QBERThreshold float64
	MatrixSeed    []byte
	Output        io.Writer
	Verbose       bool
}

type Engine struct {
	cfg      EngineConfig
	matrix   *ToeplitzMatrix
	writer   *output.KeyWriter
	breaker  CircuitBreakerState
	qber     float64
	mu       sync.Mutex
	stats    EngineStats
	sealed   int32
}

type EngineStats struct {
	TotalInputBits  uint64
	TotalOutputBits uint64
	BlocksAmplified uint64
	BlocksDiscarded uint64
	TotalDuration   time.Duration
}

func NewEngine(cfg EngineConfig) *Engine {
	if cfg.QBERThreshold <= 0 {
		cfg.QBERThreshold = QBERCriticalThreshold
	}
	if cfg.PrivacyBudget <= 0 || cfg.PrivacyBudget >= 1 {
		cfg.PrivacyBudget = DefaultPrivacyBudget
	}
	if cfg.OutputBits <= 0 {
		cfg.OutputBits = int(float64(cfg.InputBits) * cfg.PrivacyBudget)
	}
	if cfg.OutputBits >= cfg.InputBits {
		cfg.OutputBits = int(float64(cfg.InputBits) * cfg.PrivacyBudget)
	}
	if cfg.OutputBits < 64 {
		cfg.OutputBits = 64
	}

	outputBits := cfg.OutputBits
	inputBits := cfg.InputBits
	if inputBits < 64 {
		inputBits = 64
	}

	diagSeed := GenerateSeedFromBytes(cfg.MatrixSeed, inputBits+outputBits-1)
	mat := NewToeplitzMatrix(outputBits, inputBits, diagSeed)

	var w *output.KeyWriter
	if cfg.Output != nil {
		w = output.NewKeyWriter(output.Config{
			Output:       cfg.Output,
			BitsPerBlock: outputBits,
			Format:       "hex",
			WithHeader:   true,
		})
	}

	return &Engine{
		cfg:     cfg,
		matrix:  mat,
		writer:  w,
		breaker: BreakerClosed,
	}
}

func (e *Engine) Amplify(rawBits []uint8, qber float64) AmplifyResult {
	start := time.Now()

	e.mu.Lock()
	e.qber = qber
	if e.breaker == BreakerOpen {
		e.mu.Unlock()
		return AmplifyResult{
			InputBits: len(rawBits),
			QBER:      qber,
			Discarded: true,
			Duration:  time.Since(start),
		}
	}
	e.mu.Unlock()

	if qber >= e.cfg.QBERThreshold {
		e.triggerBreaker(qber)
		return AmplifyResult{
			InputBits: len(rawBits),
			QBER:      qber,
			Discarded: true,
			Duration:  time.Since(start),
		}
	}

	inputLen := len(rawBits)
	if inputLen > e.matrix.Cols() {
		inputLen = e.matrix.Cols()
	}

	amplified := e.matrix.Multiply(rawBits[:inputLen])
	outputLen := len(amplified)

	atomic.AddUint64(&e.stats.TotalInputBits, uint64(inputLen))
	atomic.AddUint64(&e.stats.TotalOutputBits, uint64(outputLen))
	atomic.AddUint64(&e.stats.BlocksAmplified, 1)

	if e.writer != nil && len(amplified) > 0 {
		e.writer.WriteBits(amplified)
	}

	return AmplifyResult{
		InputBits:  inputLen,
		OutputBits: outputLen,
		QBER:       qber,
		Discarded:  false,
		Duration:   time.Since(start),
	}
}

func (e *Engine) AmplifyBatch(batches [][]uint8, qbers []float64) []AmplifyResult {
	results := make([]AmplifyResult, 0, len(batches))
	for i, batch := range batches {
		qber := float64(0)
		if i < len(qbers) {
			qber = qbers[i]
		}
		results = append(results, e.Amplify(batch, qber))
	}
	return results
}

func (e *Engine) triggerBreaker(qber float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.breaker == BreakerOpen {
		return
	}
	e.breaker = BreakerOpen
	atomic.StoreInt32(&e.sealed, 1)
	atomic.AddUint64(&e.stats.BlocksDiscarded, 1)

	alertMsg := fmt.Sprintf(
		"\n\x1b[41m\x1b[30m ╔══════════════════════════════════════════════════════════════╗ \x1b[0m\n"+
			"\x1b[41m\x1b[30m ║  ⚠  安全红线熔断触发 — 量子信道遭受相干窃听  ⚠           ║ \x1b[0m\n"+
			"\x1b[41m\x1b[30m ║  QBER=%.4f%% 超过安全阈值 %.2f%%                            ║ \x1b[0m\n"+
			"\x1b[41m\x1b[30m ║  所有待放大原始数据块已静默废弃                          ║ \x1b[0m\n"+
			"\x1b[41m\x1b[30m ║  干线已封锁 — 输入 override-unseal 解封                    ║ \x1b[0m\n"+
			"\x1b[41m\x1b[30m ╚══════════════════════════════════════════════════════════════╝ \x1b[0m\n",
		qber*100, e.cfg.QBERThreshold*100,
	)
	fmt.Fprint(e.cfg.Output, alertMsg)
}

func (e *Engine) IsSealed() bool {
	return atomic.LoadInt32(&e.sealed) == 1
}

func (e *Engine) Unseal() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.breaker != BreakerOpen {
		return false
	}
	e.breaker = BreakerClosed
	atomic.StoreInt32(&e.sealed, 0)
	return true
}

func (e *Engine) BreakerState() CircuitBreakerState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.breaker
}

func (e *Engine) Stats() EngineStats {
	return EngineStats{
		TotalInputBits:  atomic.LoadUint64(&e.stats.TotalInputBits),
		TotalOutputBits: atomic.LoadUint64(&e.stats.TotalOutputBits),
		BlocksAmplified: atomic.LoadUint64(&e.stats.BlocksAmplified),
		BlocksDiscarded: atomic.LoadUint64(&e.stats.BlocksDiscarded),
	}
}

func (e *Engine) Matrix() *ToeplitzMatrix {
	return e.matrix
}

func (e *Engine) Flush() {
	if e.writer != nil {
		e.writer.Flush()
	}
}

func LoadRawBitsFromFile(path string) ([]uint8, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("amplify: open raw file: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("amplify: read raw file: %w", err)
	}

	str := string(data)
	bits := output.HexToBits(str)
	if len(bits) == 0 {
		bits = output.BytesToBits(data)
	}
	return bits, nil
}
