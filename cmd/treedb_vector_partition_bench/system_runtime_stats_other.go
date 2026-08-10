//go:build !linux

package main

func vectorPartitionSystemKernelRuntimeStatsV1() (cpu, runQueue, timeslices, voluntary, nonvoluntary uint64) {
	return 0, 0, 0, 0, 0
}
