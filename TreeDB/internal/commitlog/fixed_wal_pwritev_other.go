//go:build !linux

package commitlog

import "os"

const fixedWALSupportsPwritev = false

func writeFixedAtParts(_ *os.File, _ int64, _, _, _ []byte) (uint64, error) {
	return 0, os.ErrInvalid
}

func hintFixedWALWriteback(_ *os.File, _, _ int64) {}
