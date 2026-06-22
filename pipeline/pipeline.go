package pipeline

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qkd/bb84-sifter/frame"
	"github.com/qkd/bb84-sifter/ingest"
	"github.com/qkd/bb84-sifter/output"
	"github.com/qkd/bb84-sifter/sift"
)

type Config struct {
	AliceSource  io.Reader
	BobSource    io.Reader
	BufferSize   int
	QBERWindow   int
	UseFFT       bool
	KeyBlockBits int
	OutputFormat string
	WithHeader   bool
	OutputWriter io.Writer
}

type Pipeline struct {
	cfg      Config
	aliceIng *ingest.Ingestor
	bobIng   *ingest.Ingestor
	sifter   *sift.Sifter
	writer   *output.KeyWriter

	running int32
	done    chan struct{}
	wg      sync.WaitGroup

	aliceBuf []frame.Event
	bobBuf   []frame.Event
	mu       sync.Mutex

	processedEvents uint64
	lastReportTime  time.Time
	lastProcessed   uint64
}

func NewPipeline(cfg Config) *Pipeline {
	if cfg.OutputWriter == nil {
		cfg.OutputWriter = os.Stdout
	}
	if cfg.KeyBlockBits <= 0 {
		cfg.KeyBlockBits = 1 << 20
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1 << 20
	}

	return &Pipeline{
		cfg:      cfg,
		aliceIng: ingest.NewIngestor(cfg.AliceSource, cfg.BufferSize),
		bobIng:   ingest.NewIngestor(cfg.BobSource, cfg.BufferSize),
		sifter:   sift.NewSifter(cfg.QBERWindow, cfg.UseFFT),
		writer: output.NewKeyWriter(output.Config{
			Output:       cfg.OutputWriter,
			BitsPerBlock: cfg.KeyBlockBits,
			Format:       cfg.OutputFormat,
			WithHeader:   cfg.WithHeader,
		}),
		done:           make(chan struct{}),
		lastReportTime: time.Now(),
	}
}

func (p *Pipeline) Start() error {
	if !atomic.CompareAndSwapInt32(&p.running, 0, 1) {
		return nil
	}

	if err := p.aliceIng.Start(); err != nil {
		return fmt.Errorf("alice ingestor: %w", err)
	}
	if err := p.bobIng.Start(); err != nil {
		p.aliceIng.Stop()
		return fmt.Errorf("bob ingestor: %w", err)
	}

	p.wg.Add(1)
	go p.processLoop()

	return nil
}

func (p *Pipeline) Stop() {
	if !atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		return
	}

	close(p.done)
	p.aliceIng.Stop()
	p.bobIng.Stop()
	p.wg.Wait()

	p.writer.Flush()
}

func (p *Pipeline) processLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(500 * time.Microsecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		default:
		}

		select {
		case events := <-p.aliceIng.EventOut:
			p.mu.Lock()
			p.aliceBuf = append(p.aliceBuf, events...)
			p.mu.Unlock()
			p.trySift()

		case events := <-p.bobIng.EventOut:
			p.mu.Lock()
			p.bobBuf = append(p.bobBuf, events...)
			p.mu.Unlock()
			p.trySift()

		case <-ticker.C:
			p.trySift()
		}
	}
}

func (p *Pipeline) trySift() {
	p.mu.Lock()
	defer p.mu.Unlock()

	aliceLen := len(p.aliceBuf)
	bobLen := len(p.bobBuf)

	if aliceLen == 0 || bobLen == 0 {
		return
	}

	n := aliceLen
	if bobLen < n {
		n = bobLen
	}

	batchSize := n
	if batchSize > 10000 {
		batchSize = 10000
	}

	sifted := p.sifter.Sift(p.aliceBuf[:batchSize], p.bobBuf[:batchSize])

	p.aliceBuf = p.aliceBuf[batchSize:]
	p.bobBuf = p.bobBuf[batchSize:]

	atomic.AddUint64(&p.processedEvents, uint64(batchSize))

	if sifted.Length > 0 {
		p.writer.WriteBits(sifted.Bits)
	}
}

func (p *Pipeline) ProcessedEvents() uint64 {
	return atomic.LoadUint64(&p.processedEvents)
}

func (p *Pipeline) SiftedBits() uint64 {
	return p.sifter.Stats().SiftedBits
}

func (p *Pipeline) QBER() float64 {
	return p.sifter.QBER()
}

func (p *Pipeline) BlockCount() uint64 {
	return p.writer.BlockCount()
}

func (p *Pipeline) StatsText() string {
	stats := p.sifter.Stats()
	qberStats := p.sifter.QBERStats()
	return fmt.Sprintf("events=%d sifted=%d qber=%.4f%% blocks=%d",
		stats.TotalEvents, stats.SiftedBits, qberStats.QBER*100, p.writer.BlockCount())
}

func (p *Pipeline) EventRate() float64 {
	now := time.Now()
	elapsed := now.Sub(p.lastReportTime).Seconds()
	processed := atomic.LoadUint64(&p.processedEvents)
	rate := float64(processed-p.lastProcessed) / elapsed
	p.lastProcessed = processed
	p.lastReportTime = now
	return rate
}

func (p *Pipeline) Sifter() *sift.Sifter {
	return p.sifter
}

func (p *Pipeline) Writer() *output.KeyWriter {
	return p.writer
}
