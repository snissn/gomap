//go:build !darwin && !linux

package main

func comparisonProcessUsageSnapshot() (comparisonProcessUsage, error) {
	return comparisonProcessUsage{Available: false}, nil
}
