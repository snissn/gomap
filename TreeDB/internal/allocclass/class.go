// Package allocclass provides the pinned Go1.26.3 64-bit backing-class rules
// shared by TreeDB's finite storage owners. It measures backing, not process RSS.
package allocclass

import (
	"errors"
	"math"
	"unsafe"
)

var ErrUnsupported = errors.New("allocclass: unsupported backing shape")

func ClassBytes(n uint64, scan bool) (uint64, error) {
	if unsafe.Sizeof(uintptr(0)) != 8 || n > math.MaxUint64-8192 {
		return 0, ErrUnsupported
	}
	if n == 0 {
		return 0, nil
	}
	if !scan && n < 16 {
		return 16, nil
	}
	if n <= 32760 {
		if scan && n > 512 {
			n += 8
		}
		for _, c := range classes {
			if n <= uint64(c) {
				return uint64(c), nil
			}
		}
	}
	return (n + 8191) &^ uint64(8191), nil
}

var classes = [...]uint16{8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768}
