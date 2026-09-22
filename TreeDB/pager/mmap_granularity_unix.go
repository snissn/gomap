//go:build !windows
// +build !windows

package pager

import "os"

func mmapOffsetGranularity() int64 {
	return int64(os.Getpagesize())
}
