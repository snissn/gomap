//go:build darwin || linux

package main

import (
	"fmt"
	"runtime"
	"syscall"
	"time"
)

func comparisonProcessUsageSnapshot() (comparisonProcessUsage, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return comparisonProcessUsage{}, fmt.Errorf("getrusage: %w", err)
	}
	maxRSSBytes := usage.Maxrss
	if runtime.GOOS == "linux" {
		maxRSSBytes *= 1024
	}
	return comparisonProcessUsage{
		Available:         true,
		CPUSeconds:        timevalSeconds(usage.Utime) + timevalSeconds(usage.Stime),
		PeakRSSBytes:      maxRSSBytes,
		CapturedUnixNanos: time.Now().UnixNano(),
	}, nil
}

func timevalSeconds(value syscall.Timeval) float64 {
	return float64(value.Sec) + float64(value.Usec)/1e6
}
