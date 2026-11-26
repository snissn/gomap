//go:build !linux

package pager

import "os"

func syncPageRangesData(_ *os.File, _ [][]byte, _ []syncPageRange, _ int64) (bool, error) {
	return false, nil
}
