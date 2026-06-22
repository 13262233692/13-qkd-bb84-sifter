package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "bb84-sifter",
	Short: "BB84 诱骗态量子密钥分发后处理微端",
	Long: `面向商用量子保密通信干线的极低延迟诱骗态 BB84 协议后处理命令行工具。
支持高速 PCIe 码流摄入、FFT 加速 QBER 估算、实时基矢比对筛选。`,
	Version: "0.1.0",
}

var (
	qberWindow   int
	useFFT       bool
	bufferSize   int
	keyBlockBits int
	outputFormat string
	withHeader   bool
	verbose      bool
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().IntVar(&qberWindow, "qber-window", 10000, "QBER 估算窗口大小（比特数）")
	rootCmd.PersistentFlags().BoolVar(&useFFT, "fft", true, "启用 FFT 加速 QBER 估算")
	rootCmd.PersistentFlags().IntVar(&bufferSize, "buffer-size", 1<<20, "双缓冲容量（事件数）")
	rootCmd.PersistentFlags().IntVar(&keyBlockBits, "block-bits", 1<<20, "每块密钥比特数（默认 1 Mbit）")
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "format", "f", "hex", "输出格式: hex, binary, base64")
	rootCmd.PersistentFlags().BoolVar(&withHeader, "header", false, "输出块头信息")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "详细输出模式")
}
