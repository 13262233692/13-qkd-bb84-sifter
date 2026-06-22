package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qkd/bb84-sifter/pipeline"
	"github.com/qkd/bb84-sifter/sim"

	"github.com/spf13/cobra"
)

var (
	simQBER  float64
	simRate  float64
	simSeed  int64
	duration time.Duration
	useSim   bool
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "运行 BB84 基矢筛选后处理",
	Long:  `从 PCIe 采集卡或模拟源读取光子事件码流，执行实时基矢比对与 QBER 估算，输出筛选密钥。`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSifter()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolVar(&useSim, "sim", true, "使用模拟数据（默认启用）")
	runCmd.Flags().Float64Var(&simQBER, "sim-qber", 0.02, "模拟 QBER (0.0 - 1.0)")
	runCmd.Flags().Float64Var(&simRate, "sim-rate", 1e6, "模拟事件速率 (Hz)")
	runCmd.Flags().Int64Var(&simSeed, "sim-seed", 0, "模拟随机种子 (0 = 自动)")
	runCmd.Flags().DurationVarP(&duration, "duration", "d", 0, "运行持续时间 (0 = 无限)")
}

func runSifter() error {
	gen := sim.NewGenerator(sim.Config{
		Seed: simSeed,
		QBER: simQBER,
	})

	pairedSource := sim.NewPairedSource(gen, 1024)
	aliceStream := pairedSource.AliceReader()
	bobStream := pairedSource.BobReader()

	pipe := pipeline.NewPipeline(pipeline.Config{
		AliceSource:  aliceStream,
		BobSource:    bobStream,
		BufferSize:   bufferSize,
		QBERWindow:   qberWindow,
		UseFFT:       useFFT,
		KeyBlockBits: keyBlockBits,
		OutputFormat: outputFormat,
		WithHeader:   withHeader,
		OutputWriter: os.Stdout,
	})

	if err := pipe.Start(); err != nil {
		return fmt.Errorf("pipeline start failed: %w", err)
	}
	defer pipe.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var timerChan <-chan time.Time
	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		timerChan = timer.C
	}

	statTicker := time.NewTicker(1 * time.Second)
	defer statTicker.Stop()

	if verbose {
		fmt.Fprintf(os.Stderr, "[INFO] BB84 诱骗态后处理启动\n")
		fmt.Fprintf(os.Stderr, "[INFO] 模式: 模拟 | QBER: %.2f%% | 速率: %.0f Hz\n", simQBER*100, simRate)
		fmt.Fprintf(os.Stderr, "[INFO] 密钥块大小: %d bits (%.2f KB)\n", keyBlockBits, float64(keyBlockBits)/8192)
		fmt.Fprintf(os.Stderr, "[INFO] FFT 加速: %v | 缓冲大小: %d 事件\n", useFFT, bufferSize)
	}

	for {
		select {
		case <-sigChan:
			if verbose {
				fmt.Fprintf(os.Stderr, "\n[INFO] 收到停止信号，正在退出...\n")
			}
			printPipelineStats(pipe)
			return nil

		case <-timerChan:
			if verbose {
				fmt.Fprintf(os.Stderr, "\n[INFO] 运行时间结束\n")
			}
			printPipelineStats(pipe)
			return nil

		case <-statTicker.C:
			if verbose {
				rate := pipe.EventRate()
				fmt.Fprintf(os.Stderr, "\r[STATS] %s | 速率: %.0f evt/s", pipe.StatsText(), rate)
			}
		}
	}
}

func printPipelineStats(p *pipeline.Pipeline) {
	sifter := p.Sifter()
	stats := sifter.Stats()
	qberStats := sifter.QBERStats()
	writer := p.Writer()

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "========== 运行统计 ==========\n")
	fmt.Fprintf(os.Stderr, "总事件数:     %d\n", stats.TotalEvents)
	fmt.Fprintf(os.Stderr, "筛选密钥比特: %d (%.2f KB)\n", stats.SiftedBits, float64(stats.SiftedBits)/8192)
	fmt.Fprintf(os.Stderr, "  直线基: %d\n", stats.RectSifted)
	fmt.Fprintf(os.Stderr, "  对角基: %d\n", stats.DiagSifted)
	fmt.Fprintf(os.Stderr, "  信号态: %d\n", stats.SignalBits)
	fmt.Fprintf(os.Stderr, "  诱骗态: %d\n", stats.DecoyBits)
	fmt.Fprintf(os.Stderr, "  真空态: %d\n", stats.VacuumBits)
	fmt.Fprintf(os.Stderr, "总 QBER:      %.4f%% (%d/%d)\n", qberStats.QBER*100, qberStats.ErrorBits, qberStats.TotalBits)
	fmt.Fprintf(os.Stderr, "  信号态 QBER: %.4f%%\n", qberStats.SignalQBER*100)
	fmt.Fprintf(os.Stderr, "  诱骗态 QBER: %.4f%%\n", qberStats.DecoyQBER*100)
	fmt.Fprintf(os.Stderr, "  真空态 QBER: %.4f%%\n", qberStats.VacuumQBER*100)
	fmt.Fprintf(os.Stderr, "输出密钥块:   %d\n", writer.BlockCount())
	fmt.Fprintf(os.Stderr, "==============================\n")
}
