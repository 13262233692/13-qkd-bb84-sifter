package qber

import (
	"math"
	"math/cmplx"
)

func FFT(x []complex128) []complex128 {
	n := len(x)
	if n == 0 {
		return x
	}
	if n&(n-1) != 0 {
		nextPow2 := 1
		for nextPow2 < n {
			nextPow2 <<= 1
		}
		padded := make([]complex128, nextPow2)
		copy(padded, x)
		x = padded
		n = nextPow2
	}
	return fftCore(x)
}

func fftCore(x []complex128) []complex128 {
	n := len(x)
	if n <= 1 {
		return x
	}

	even := make([]complex128, n/2)
	odd := make([]complex128, n/2)
	for i := 0; i < n/2; i++ {
		even[i] = x[2*i]
		odd[i] = x[2*i+1]
	}

	even = fftCore(even)
	odd = fftCore(odd)

	result := make([]complex128, n)
	for k := 0; k < n/2; k++ {
		angle := -2 * math.Pi * float64(k) / float64(n)
		t := cmplx.Exp(complex(0, angle)) * odd[k]
		result[k] = even[k] + t
		result[k+n/2] = even[k] - t
	}
	return result
}

func IFFT(x []complex128) []complex128 {
	n := len(x)
	if n == 0 {
		return x
	}

	conj := make([]complex128, n)
	for i := range x {
		conj[i] = cmplx.Conj(x[i])
	}

	conj = FFT(conj)

	result := make([]complex128, n)
	for i := range conj {
		result[i] = cmplx.Conj(conj[i]) / complex(float64(n), 0)
	}
	return result
}

func CrossCorrelation(a, b []float64) []float64 {
	n := len(a)
	m := len(b)
	resultLen := n + m - 1

	fftLen := 1
	for fftLen < resultLen {
		fftLen <<= 1
	}

	aComplex := make([]complex128, fftLen)
	bComplex := make([]complex128, fftLen)

	for i, v := range a {
		aComplex[i] = complex(v, 0)
	}
	for i, v := range b {
		bComplex[i] = complex(v, 0)
	}

	aFFT := FFT(aComplex)
	bFFT := FFT(bComplex)

	corrFFT := make([]complex128, fftLen)
	for i := range aFFT {
		corrFFT[i] = aFFT[i] * cmplx.Conj(bFFT[i])
	}

	corr := IFFT(corrFFT)

	result := make([]float64, resultLen)
	for i := 0; i < resultLen; i++ {
		result[i] = real(corr[i])
	}
	return result
}

func FastConvolution(a, b []float64) []float64 {
	n := len(a)
	m := len(b)
	resultLen := n + m - 1

	fftLen := 1
	for fftLen < resultLen {
		fftLen <<= 1
	}

	aComplex := make([]complex128, fftLen)
	bComplex := make([]complex128, fftLen)

	for i, v := range a {
		aComplex[i] = complex(v, 0)
	}
	for i, v := range b {
		bComplex[i] = complex(v, 0)
	}

	aFFT := FFT(aComplex)
	bFFT := FFT(bComplex)

	convFFT := make([]complex128, fftLen)
	for i := range aFFT {
		convFFT[i] = aFFT[i] * bFFT[i]
	}

	conv := IFFT(convFFT)

	result := make([]float64, resultLen)
	for i := 0; i < resultLen; i++ {
		result[i] = real(conv[i])
	}
	return result
}

func BitsToFloat(bits []uint8) []float64 {
	result := make([]float64, len(bits))
	for i, b := range bits {
		if b == 1 {
			result[i] = 1.0
		} else {
			result[i] = -1.0
		}
	}
	return result
}
