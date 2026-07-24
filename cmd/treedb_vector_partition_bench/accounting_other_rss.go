//go:build !linux

package main

func vectorPartitionBenchmarkPeakRSS() (int64, bool) { return 0, false }
