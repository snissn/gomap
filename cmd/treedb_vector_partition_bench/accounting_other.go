//go:build !unix

package main

func vectorPartitionBenchmarkCPUNanos() (int64, bool) { return 0, false }
