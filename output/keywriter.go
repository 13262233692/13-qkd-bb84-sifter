package output

import (
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	HexCharPerByte = 2
)

type KeyWriter struct {
	mu          sync.Mutex
	out         io.Writer
	bitsPerBlock int
	bitBuf      []uint8
	bitCount    int
	blockCount  uint64
	totalBits   uint64
	lastOutput  time.Time
	format      string
	withHeader  bool
	withChecksum bool
}

type Config struct {
	Output        io.Writer
	BitsPerBlock  int
	Format        string
	WithHeader    bool
	WithChecksum  bool
}

func NewKeyWriter(cfg Config) *KeyWriter {
	if cfg.Output == nil {
		panic("output: nil writer")
	}
	if cfg.BitsPerBlock <= 0 {
		cfg.BitsPerBlock = 1 << 20
	}
	if cfg.Format == "" {
		cfg.Format = "hex"
	}
	return &KeyWriter{
		out:         cfg.Output,
		bitsPerBlock: cfg.BitsPerBlock,
		bitBuf:      make([]uint8, 0, cfg.BitsPerBlock*2),
		format:      cfg.Format,
		withHeader:  cfg.WithHeader,
		withChecksum: cfg.WithChecksum,
		lastOutput:  time.Now(),
	}
}

func (kw *KeyWriter) WriteBits(bits []uint8) (int, error) {
	kw.mu.Lock()
	defer kw.mu.Unlock()

	kw.bitBuf = append(kw.bitBuf, bits...)
	kw.bitCount = len(kw.bitBuf)
	kw.totalBits += uint64(len(bits))

	written := 0
	for kw.bitCount >= kw.bitsPerBlock {
		block := kw.bitBuf[:kw.bitsPerBlock]
		kw.bitBuf = kw.bitBuf[kw.bitsPerBlock:]
		kw.bitCount = len(kw.bitBuf)

		if err := kw.emitBlock(block); err != nil {
			return written, err
		}
		written += kw.bitsPerBlock
		kw.blockCount++
	}

	return written, nil
}

func (kw *KeyWriter) emitBlock(bits []uint8) error {
	switch kw.format {
	case "hex":
		return kw.emitHex(bits)
	case "binary":
		return kw.emitBinary(bits)
	case "base64":
		return kw.emitBase64(bits)
	default:
		return kw.emitHex(bits)
	}
}

func (kw *KeyWriter) emitHex(bits []uint8) error {
	hexStr := BitsToHex(bits)

	if kw.withHeader {
		ts := time.Now().Format(time.RFC3339Nano)
		header := fmt.Sprintf("[BLOCK #%d] %s  bits=%d  qber=%.4f\n",
			kw.blockCount+1, ts, len(bits), 0.0)
		if _, err := kw.out.Write([]byte(header)); err != nil {
			return err
		}
	}

	if _, err := kw.out.Write([]byte(hexStr)); err != nil {
		return err
	}

	if kw.withChecksum {
		cs := checksum(bits)
		if _, err := kw.out.Write([]byte(fmt.Sprintf("  // cs=%04x\n", cs))); err != nil {
			return err
		}
	} else {
		if _, err := kw.out.Write([]byte("\n")); err != nil {
			return err
		}
	}

	kw.lastOutput = time.Now()
	return nil
}

func (kw *KeyWriter) emitBinary(bits []uint8) error {
	bytes := BitsToBytes(bits)
	_, err := kw.out.Write(bytes)
	return err
}

func (kw *KeyWriter) emitBase64(bits []uint8) error {
	bytes := BitsToBytes(bits)
	b64 := fmt.Sprintf("%x", bytes)
	_, err := kw.out.Write([]byte(b64 + "\n"))
	return err
}

func (kw *KeyWriter) Flush() error {
	kw.mu.Lock()
	defer kw.mu.Unlock()

	if kw.bitCount == 0 {
		return nil
	}

	block := make([]uint8, kw.bitCount)
	copy(block, kw.bitBuf)
	kw.bitBuf = kw.bitBuf[:0]
	kw.bitCount = 0

	return kw.emitBlock(block)
}

func (kw *KeyWriter) BlockCount() uint64 {
	kw.mu.Lock()
	defer kw.mu.Unlock()
	return kw.blockCount
}

func (kw *KeyWriter) TotalBits() uint64 {
	kw.mu.Lock()
	defer kw.mu.Unlock()
	return kw.totalBits
}

func (kw *KeyWriter) BufferedBits() int {
	kw.mu.Lock()
	defer kw.mu.Unlock()
	return kw.bitCount
}

func BitsToHex(bits []uint8) string {
	n := len(bits)
	hexLen := (n + 3) / 4
	if hexLen == 0 {
		return ""
	}

	hex := make([]byte, hexLen)
	for i := 0; i < hexLen; i++ {
		var val byte
		for j := 0; j < 4 && i*4+j < n; j++ {
			val = (val << 1) | bits[i*4+j]
		}
		remainder := n - i*4
		if remainder < 4 {
			val <<= uint(4 - remainder)
		}
		if val < 10 {
			hex[i] = '0' + val
		} else {
			hex[i] = 'a' + val - 10
		}
	}

	return string(hex)
}

func HexToBits(hexStr string) []uint8 {
	bits := make([]uint8, 0, len(hexStr)*4)
	for _, c := range hexStr {
		var val byte
		switch {
		case c >= '0' && c <= '9':
			val = byte(c - '0')
		case c >= 'a' && c <= 'f':
			val = byte(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			val = byte(c - 'A' + 10)
		default:
			continue
		}
		for i := 3; i >= 0; i-- {
			bits = append(bits, (val>>uint(i))&1)
		}
	}
	return bits
}

func BitsToBytes(bits []uint8) []byte {
	n := len(bits)
	byteLen := (n + 7) / 8
	if byteLen == 0 {
		return nil
	}

	bytes := make([]byte, byteLen)
	for i := 0; i < byteLen; i++ {
		var b byte
		for j := 0; j < 8 && i*8+j < n; j++ {
			b = (b << 1) | bits[i*8+j]
		}
		remainder := n - i*8
		if remainder < 8 {
			b <<= uint(8 - remainder)
		}
		bytes[i] = b
	}
	return bytes
}

func BytesToBits(b []byte) []uint8 {
	bits := make([]uint8, 0, len(b)*8)
	for _, byteVal := range b {
		for i := 7; i >= 0; i-- {
			bits = append(bits, (byteVal>>uint(i))&1)
		}
	}
	return bits
}

func checksum(bits []uint8) uint16 {
	var sum uint16
	for _, b := range bits {
		sum += uint16(b)
	}
	return sum
}
