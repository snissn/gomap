//go:build !darwin && !linux

package main

import "time"

func comparisonProcessUsageSnapshot() (comparisonProcessUsage, error) {
	return comparisonProcessUsage{Available: false, CapturedUnixNanos: time.Now().UnixNano()}, nil
}
