//go:build windows
// +build windows

package pager

import "errors"

var errMmapUnsupported = errors.New("pager: mmap not supported on windows")

func mmapFile(_ uintptr, _ int64, _ int) ([]byte, error) {
	return nil, errMmapUnsupported
}

func mmapAvailable() error {
	return errMmapUnsupported
}

func munmapFile(_ []byte) error {
	return nil
}

func msyncFile(_ []byte) error {
	return nil
}
