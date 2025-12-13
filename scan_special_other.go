//go:build !amd64 && !arm64

package gomap

var useSIMDScan = false

func scanSpecial8Mask(ptr *Hash, target Hash) uint64 {
	return scanSpecial8Scalar(ptr, target)
}
