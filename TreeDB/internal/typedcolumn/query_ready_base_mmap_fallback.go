//go:build !(darwin || linux || freebsd || netbsd || openbsd)

package typedcolumn

import (
	"errors"
	"os"
)

func queryReadyBaseMmapSupported() bool { return false }

func mmapQueryReadyBaseFile(_ *os.File) ([]byte, error) {
	return nil, errors.New("typedcolumn: query-ready base mmap is unsupported on this platform")
}

func mmapQueryReadyBaseFileRange(_ *os.File, _ int64, _ int) ([]byte, []byte, error) {
	return nil, nil, errors.New("typedcolumn: query-ready base mmap range is unsupported on this platform")
}

func munmapQueryReadyBaseFile(_ []byte) error { return nil }
