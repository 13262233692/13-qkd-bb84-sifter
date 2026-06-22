package cmd

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/qkd/bb84-sifter/amplify"
	"github.com/qkd/bb84-sifter/output"
	"github.com/spf13/cobra"
)

var (
	amplifyRawFile      string
	amplifyPrivacyBudget float64
	amplifyMatrixSeed   string
	amplifyQBEROverride float64
	amplifyInputBits    int
	amplifyOutputBits   int
	amplifySimMode      bool
	amplifySimQBER      float64
	amplifySimBits      int
	amplifySimSeed      int64
	amplifyQBERThresh   float64
	amplifyVerbose      bool
)

var amplifyCmd = &cobra.Command{
	Use:   "amplify",
	Short: "托普利茨矩阵隐私放大 — 抗 PNS 攻击物理熵增提纯",
	Long: `基于托普利茨压缩矩阵的隐私放大模块，面向窃听者光子分离攻击（PNS）。
从筛选后的亚健康密钥流中提取物理熵增提纯的安全密钥。
当 QBER 突破 7.5% 安全红线时自动熔断，封锁终端直到管理员输入 override-unseal。`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAmplify()
	},
}

func init() {
	rootCmd.AddCommand(amplifyCmd)

	amplifyCmd.Flags().StringVar(&amplifyRawFile, "raw-file", "", "原始筛选密钥文件路径 (Hex 格式)")
	amplifyCmd.Flags().Float64Var(&amplifyPrivacyBudget, "privacy-budget", 0.2, "隐私预算 (压缩比, 0.0-1.0)")
	amplifyCmd.Flags().StringVar(&amplifyMatrixSeed, "matrix-seed", "", "托普利茨矩阵真随机种子 (Hex)")
	amplifyCmd.Flags().Float64Var(&amplifyQBEROverride, "qber", -1, "覆盖 QBER 值 (0.0-1.0, -1 自动)")
	amplifyCmd.Flags().IntVar(&amplifyInputBits, "input-bits", 0, "输入密钥比特数 (0=自动)")
	amplifyCmd.Flags().IntVar(&amplifyOutputBits, "output-bits", 0, "输出密钥比特数 (0=按隐私预算计算)")
	amplifyCmd.Flags().BoolVar(&amplifySimMode, "sim", false, "使用模拟数据")
	amplifyCmd.Flags().Float64Var(&amplifySimQBER, "sim-qber", 0.025, "模拟 QBER (0.0-1.0)")
	amplifyCmd.Flags().IntVar(&amplifySimBits, "sim-bits", 1<<20, "模拟密钥比特数")
	amplifyCmd.Flags().Int64Var(&amplifySimSeed, "sim-seed", 0, "模拟随机种子 (0=自动)")
	amplifyCmd.Flags().Float64Var(&amplifyQBERThresh, "qber-threshold", 0.075, "QBER 安全红线阈值 (默认 7.5%%)")
	amplifyCmd.Flags().BoolVarP(&amplifyVerbose, "verbose", "v", false, "详细输出模式")
}

func runAmplify() error {
	var rawBits []uint8
	var qber float64
	var err error

	if amplifySimMode {
		rawBits, qber, err = generateSimAmplifyInput()
	} else {
		rawBits, qber, err = loadAmplifyInput()
	}
	if err != nil {
		return err
	}

	if amplifyQBEROverride >= 0 {
		qber = amplifyQBEROverride
	}

	inputBits := len(rawBits)
	if amplifyInputBits > 0 && amplifyInputBits < inputBits {
		inputBits = amplifyInputBits
		rawBits = rawBits[:inputBits]
	}

	outputBits := amplifyOutputBits
	if outputBits <= 0 {
		outputBits = int(float64(inputBits) * amplifyPrivacyBudget)
	}
	if outputBits >= inputBits {
		outputBits = int(float64(inputBits) * amplifyPrivacyBudget)
	}

	var seedBytes []byte
	if amplifyMatrixSeed != "" {
		seedBits := output.HexToBits(amplifyMatrixSeed)
		seedBytes = output.BitsToBytes(seedBits)
	} else {
		seedBytes = []byte(fmt.Sprintf("qkd-toeplitz-%d-%d", inputBits, time.Now().UnixNano()))
	}

	if amplifyVerbose {
		fmt.Fprintf(os.Stderr, "[INFO] 隐私放大引擎启动\n")
		fmt.Fprintf(os.Stderr, "[INFO] 输入比特: %d | 输出比特: %d | 压缩比: %.4f\n",
			inputBits, outputBits, float64(outputBits)/float64(inputBits))
		fmt.Fprintf(os.Stderr, "[INFO] QBER: %.4f%% | 安全红线: %.2f%%\n", qber*100, amplifyQBERThresh*100)
		fmt.Fprintf(os.Stderr, "[INFO] 托普利茨矩阵: %d x %d (对角线 %d 元素)\n",
			outputBits, inputBits, outputBits+inputBits-1)
		fmt.Fprintf(os.Stderr, "[INFO] 隐私预算: %.4f\n", amplifyPrivacyBudget)
	}

	engine := amplify.NewEngine(amplify.EngineConfig{
		InputBits:     inputBits,
		OutputBits:    outputBits,
		PrivacyBudget: amplifyPrivacyBudget,
		QBERThreshold: amplifyQBERThresh,
		MatrixSeed:    seedBytes,
		Output:        os.Stdout,
		Verbose:       amplifyVerbose,
	})

	blockSize := inputBits
	numBlocks := 1
	if inputBits > 1<<20 {
		blockSize = 1 << 20
		numBlocks = (inputBits + blockSize - 1) / blockSize
	}

	totalStart := time.Now()

	for b := 0; b < numBlocks; b++ {
		start := b * blockSize
		end := start + blockSize
		if end > inputBits {
			end = inputBits
		}

		chunk := rawBits[start:end]
		result := engine.Amplify(chunk, qber)

		if amplifyVerbose {
			status := "✓"
			if result.Discarded {
				status = "✗ 废弃"
			}
			fmt.Fprintf(os.Stderr, "[BLOCK %d/%d] %s | 输入=%d 输出=%d QBER=%.4f%% 耗时=%v\n",
				b+1, numBlocks, status, result.InputBits, result.OutputBits,
				result.QBER*100, result.Duration)
		}

		if engine.IsSealed() {
			break
		}
	}

	engine.Flush()
	totalDuration := time.Since(totalStart)

	stats := engine.Stats()
	fmt.Fprintf(os.Stderr, "\n========== 隐私放大统计 ==========\n")
	fmt.Fprintf(os.Stderr, "输入比特:     %d\n", stats.TotalInputBits)
	fmt.Fprintf(os.Stderr, "输出比特:     %d\n", stats.TotalOutputBits)
	fmt.Fprintf(os.Stderr, "压缩比:       %.4f\n", float64(stats.TotalOutputBits)/float64(maxVal(stats.TotalInputBits, 1)))
	fmt.Fprintf(os.Stderr, "放大块数:     %d\n", stats.BlocksAmplified)
	fmt.Fprintf(os.Stderr, "废弃块数:     %d\n", stats.BlocksDiscarded)
	fmt.Fprintf(os.Stderr, "熔断状态:     %s\n", breakerStateText(engine.BreakerState()))
	fmt.Fprintf(os.Stderr, "总耗时:       %v\n", totalDuration)
	fmt.Fprintf(os.Stderr, "===================================\n")

	if engine.IsSealed() {
		return terminalSealLoop(engine)
	}

	return nil
}

func generateSimAmplifyInput() ([]uint8, float64, error) {
	seed := amplifySimSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	bits := make([]uint8, amplifySimBits)
	for i := range bits {
		bits[i] = uint8(rng.Intn(2))
	}

	return bits, amplifySimQBER, nil
}

func loadAmplifyInput() ([]uint8, float64, error) {
	if amplifyRawFile == "" {
		return nil, 0, fmt.Errorf("请指定 --raw-file 或启用 --sim 模式")
	}

	bits, err := amplify.LoadRawBitsFromFile(amplifyRawFile)
	if err != nil {
		return nil, 0, fmt.Errorf("加载密钥文件失败: %w", err)
	}

	qber := 0.02
	if amplifyQBEROverride >= 0 {
		qber = amplifyQBEROverride
	}

	if amplifyVerbose {
		fmt.Fprintf(os.Stderr, "[INFO] 从 %s 加载 %d 比特\n", amplifyRawFile, len(bits))
	}

	return bits, qber, nil
}

func terminalSealLoop(engine *amplify.Engine) error {
	fmt.Fprintf(os.Stderr, "\n\x1b[31m╔══════════════════════════════════════════════════════╗\x1b[0m\n")
	fmt.Fprintf(os.Stderr, "\x1b[31m║  干线已封锁 — 等待管理员输入 override-unseal 解封  ║\x1b[0m\n")
	fmt.Fprintf(os.Stderr, "\x1b[31m╚══════════════════════════════════════════════════════╝\x1b[0m\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprintf(os.Stderr, "\x1b[31m[SEALED] > \x1b[0m")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "override-unseal" {
			if engine.Unseal() {
				fmt.Fprintf(os.Stderr, "\x1b[32m[UNSEALED] 干线已解封，隐私放大引擎已重置\x1b[0m\n")
				return nil
			}
		} else {
			fmt.Fprintf(os.Stderr, "\x1b[31m[SEALED] 无效指令，干线仍处于封锁状态\x1b[0m\n")
		}
	}
	return nil
}

func breakerStateText(state amplify.CircuitBreakerState) string {
	switch state {
	case amplify.BreakerClosed:
		return "正常 (CLOSED)"
	case amplify.BreakerOpen:
		return "熔断 (OPEN)"
	default:
		return "未知"
	}
}

func maxVal(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
