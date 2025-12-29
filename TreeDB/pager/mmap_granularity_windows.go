//go:build windows
// +build windows

package pager

import "os"

func mmapOffsetGranularity() int64 {
	if allocationGranularity > 0 {
		return allocationGranularity
	}
	return int64(os.Getpagesize())
}
