//go:build !amd64

package gomap

var useAVX512Scan = false

func scanSpecial8Mask(ptr *Hash, target Hash) uint64 {
	return scanSpecial8Scalar(ptr, target)
}
