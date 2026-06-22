package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/qkd/bb84-sifter/frame"
	"github.com/qkd/bb84-sifter/qber"
	"github.com/qkd/bb84-sifter/sift"
	"github.com/qkd/bb84-sifter/sim"

	"github.com/spf13/cobra"
)

var (
	benchEvents int
	benchIters  int
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "性能基准测试",
	Long:  `对 FFT 加速 QBER 估算与基矢筛选进行性能基准测试。`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runBenchmark()
	},
}

func init() {
	rootCmd.AddCommand(benchCmd)

	benchCmd.Flags().IntVar(&benchEvents, "events", 100000, "基准测试事件数")
	benchCmd.Flags().IntVar(&benchIters, "iterations", 5, "基准测试迭代次数")
}

func runBenchmark() error {
	gen := sim.NewGenerator(sim.Config{
		Seed: 42,
		QBER: 0.02,
	})

	fmt.Fprintf(os.Stderr, "========== BB84 后处理基准测试 ==========\n")
	fmt.Fprintf(os.Stderr, "事件数: %d | 迭代: %d\n", benchEvents, benchIters)
	fmt.Fprintf(os.Stderr, "----------------------------------------\n\n")

	aliceEvents := gen.GenerateAliceEvents(benchEvents)
	bobEvents := gen.GenerateBobEvents(aliceEvents)

	fmt.Fprintf(os.Stderr, "[1/3] 基矢筛选 (Sifting) 基准测试...\n")
	var siftTime time.Duration
	var siftedCount int
	sifter := sift.NewSifter(qberWindow, false)

	for i := 0; i < benchIters; i++ {
		sifter.Reset()
		start := time.Now()
		result := sifter.Sift(aliceEvents, bobEvents)
		siftTime += time.Since(start)
		siftedCount = result.Length
	}

	avgSift := siftTime / time.Duration(benchIters)
	siftRate := float64(benchEvents) / avgSift.Seconds()
	fmt.Fprintf(os.Stderr, "  平均耗时: %v\n", avgSift)
	fmt.Fprintf(os.Stderr, "  吞吐量:   %.2f 事件/秒\n", siftRate)
	fmt.Fprintf(os.Stderr, "  筛选率:   %.2f%% (%d/%d)\n\n",
		float64(siftedCount)/float64(benchEvents)*100, siftedCount, benchEvents)

	fmt.Fprintf(os.Stderr, "[2/3] 标量 QBER 估算基准测试...\n")
	var scalarTime time.Duration
	var scalarQBER float64

	scalarEst := qber.NewEstimator(qberWindow, false)
	siftedAlice := make([]uint8, 0, benchEvents/2)
	siftedBob := make([]uint8, 0, benchEvents/2)
	intensities := make([]uint8, 0, benchEvents/2)

	for i := 0; i < benchEvents; i++ {
		if aliceEvents[i].Basis == bobEvents[i].Basis {
			siftedAlice = append(siftedAlice, aliceEvents[i].Bit)
			siftedBob = append(siftedBob, bobEvents[i].Bit)
			intensities = append(intensities, aliceEvents[i].Intensity)
		}
	}

	for i := 0; i < benchIters; i++ {
		scalarEst.Reset()
		start := time.Now()
		result := scalarEst.Estimate(siftedAlice, siftedBob, intensities)
		scalarTime += time.Since(start)
		scalarQBER = result.QBER
	}

	avgScalar := scalarTime / time.Duration(benchIters)
	scalarRate := float64(len(siftedAlice)) / avgScalar.Seconds()
	fmt.Fprintf(os.Stderr, "  平均耗时: %v\n", avgScalar)
	fmt.Fprintf(os.Stderr, "  吞吐量:   %.2f 比特/秒\n", scalarRate)
	fmt.Fprintf(os.Stderr, "  QBER:     %.4f%%\n\n", scalarQBER*100)

	fmt.Fprintf(os.Stderr, "[3/3] FFT 加速 QBER 估算基准测试...\n")
	var fftTime time.Duration
	var fftQBER float64

	fftEst := qber.NewEstimator(qberWindow, true)

	for i := 0; i < benchIters; i++ {
		fftEst.Reset()
		start := time.Now()
		result := fftEst.Estimate(siftedAlice, siftedBob, intensities)
		fftTime += time.Since(start)
		fftQBER = result.QBER
	}

	avgFFT := fftTime / time.Duration(benchIters)
	fftRate := float64(len(siftedAlice)) / avgFFT.Seconds()
	fmt.Fprintf(os.Stderr, "  平均耗时: %v\n", avgFFT)
	fmt.Fprintf(os.Stderr, "  吞吐量:   %.2f 比特/秒\n", fftRate)
	fmt.Fprintf(os.Stderr, "  QBER:     %.4f%%\n\n", fftQBER*100)

	speedup := float64(scalarTime) / float64(fftTime)
	fmt.Fprintf(os.Stderr, "============ 结果汇总 ============\n")
	fmt.Fprintf(os.Stderr, "FFT 加速比:  %.2fx\n", speedup)
	fmt.Fprintf(os.Stderr, "标量 QBER:   %.4f%%\n", scalarQBER*100)
	fmt.Fprintf(os.Stderr, "FFT QBER:    %.4f%%\n", fftQBER*100)
	fmt.Fprintf(os.Stderr, "误差:        %.4f%%\n", (scalarQBER-fftQBER)*100)
	fmt.Fprintf(os.Stderr, "==================================\n")

	return nil
}

func benchmarkFrameParse(events []frame.Event, mapper interface{}) {
}
