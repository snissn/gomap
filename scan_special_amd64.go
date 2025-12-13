//go:build amd64

package gomap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/cpu"
)

var useSIMDScan = cpu.X86.HasAVX512F && os.Getenv("GOMAP_DISABLE_AVX512_SCAN") != "1" && os.Getenv("GOMAP_DISABLE_SIMD_SCAN") != "1"

//go:noescape
func scanSpecial8AVX512(ptr unsafe.Pointer, target uint64) uint64

func scanSpecial8Mask(ptr *Hash, target Hash) uint64 {
	if useSIMDScan {
		return scanSpecial8AVX512(unsafe.Pointer(ptr), uint64(target))
	}
	return scanSpecial8Scalar(ptr, target)
}
