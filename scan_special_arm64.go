//go:build arm64

package gomap

import (
	"os"
	"unsafe"
)

var useSIMDScan = os.Getenv("GOMAP_DISABLE_NEON_SCAN") != "1" && os.Getenv("GOMAP_DISABLE_SIMD_SCAN") != "1"

//go:noescape
func scanSpecial8NEON(ptr unsafe.Pointer, target uint64) uint64

func scanSpecial8Mask(ptr *Hash, target Hash) uint64 {
	if useSIMDScan {
		return scanSpecial8NEON(unsafe.Pointer(ptr), uint64(target))
	}
	return scanSpecial8Scalar(ptr, target)
}
