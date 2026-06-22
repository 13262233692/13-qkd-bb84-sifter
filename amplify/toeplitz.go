package amplify

import (
	"sync"
)

const (
	MinToeplitzSize = 256
)

type ToeplitzMatrix struct {
	rows    int
	cols    int
	diag    []uint8
	once    sync.Once
	colBuf  []uint8
	rowBuf  []uint8
}

func NewToeplitzMatrix(rows, cols int, seed []uint8) *ToeplitzMatrix {
	if rows < MinToeplitzSize {
		rows = MinToeplitzSize
	}
	if cols < MinToeplitzSize {
		cols = MinToeplitzSize
	}

	diagLen := rows + cols - 1
	diag := make([]uint8, diagLen)

	if len(seed) > 0 {
		for i := range diag {
			diag[i] = seed[i%len(seed)]
		}
	} else {
		for i := range diag {
			diag[i] = uint8(i & 1)
		}
	}

	return &ToeplitzMatrix{
		rows:   rows,
		cols:   cols,
		diag:   diag,
		colBuf: make([]uint8, cols),
		rowBuf: make([]uint8, rows),
	}
}

func (t *ToeplitzMatrix) Rows() int { return t.rows }
func (t *ToeplitzMatrix) Cols() int { return t.cols }

func (t *ToeplitzMatrix) Get(i, j int) uint8 {
	if i < 0 || i >= t.rows || j < 0 || j >= t.cols {
		return 0
	}
	return t.diag[i-j+t.cols-1]
}

func (t *ToeplitzMatrix) GetRow(i int) []uint8 {
	if i < 0 || i >= t.rows {
		return nil
	}
	for j := 0; j < t.cols; j++ {
		t.rowBuf[j] = t.diag[i-j+t.cols-1]
	}
	result := make([]uint8, t.cols)
	copy(result, t.rowBuf)
	return result
}

func (t *ToeplitzMatrix) GetCol(j int) []uint8 {
	if j < 0 || j >= t.cols {
		return nil
	}
	for i := 0; i < t.rows; i++ {
		t.colBuf[i] = t.diag[i-j+t.cols-1]
	}
	result := make([]uint8, t.rows)
	copy(result, t.colBuf)
	return result
}

func (t *ToeplitzMatrix) Multiply(input []uint8) []uint8 {
	inputLen := len(input)
	if inputLen > t.cols {
		inputLen = t.cols
	}
	outputLen := t.rows
	if outputLen <= 0 {
		return nil
	}

	output := make([]uint8, outputLen)

	chunkSize := 64
	if outputLen < chunkSize {
		chunkSize = outputLen
	}

	numWorkers := outputLen / chunkSize
	if numWorkers > 16 {
		numWorkers = 16
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	var wg sync.WaitGroup
	rowsPerWorker := outputLen / numWorkers

	for w := 0; w < numWorkers; w++ {
		startRow := w * rowsPerWorker
		endRow := startRow + rowsPerWorker
		if w == numWorkers-1 {
			endRow = outputLen
		}

		wg.Add(1)
		go func(sr, er int) {
			defer wg.Done()
			for i := sr; i < er; i++ {
				var bit uint8
				diagOff := i + t.cols - 1
				for j := 0; j < inputLen; j++ {
					idx := diagOff - j
					if idx >= 0 && idx < len(t.diag) {
						bit ^= t.diag[idx] & input[j]
					}
				}
				output[i] = bit
			}
		}(startRow, endRow)
	}

	wg.Wait()
	return output
}

func (t *ToeplitzMatrix) MultiplyBlock(input []uint8, blockSize int) [][]uint8 {
	inputLen := len(input)
	if inputLen == 0 || blockSize <= 0 {
		return nil
	}

	var blocks [][]uint8
	for off := 0; off < inputLen; off += blockSize {
		end := off + blockSize
		if end > inputLen {
			end = inputLen
		}
		if end-off > t.cols {
			end = off + t.cols
		}

		chunk := input[off:end]
		result := t.Multiply(chunk)
		blocks = append(blocks, result)
	}
	return blocks
}

func GenerateSeedFromBytes(rawSeed []byte, targetLen int) []uint8 {
	if targetLen <= 0 {
		return nil
	}
	result := make([]uint8, targetLen)
	if len(rawSeed) == 0 {
		return result
	}

	state := make([]byte, 32)
	for i := range state {
		state[i] = rawSeed[i%len(rawSeed)]
	}

	for i := 0; i < targetLen; i++ {
		a := state[i%32]
		b := state[(i+7)%32]
		c := state[(i+17)%32]
		d := state[(i+23)%32]
		result[i] = ((a ^ b) + c) ^ d
		state[i%32] = result[i]
	}

	return result
}
