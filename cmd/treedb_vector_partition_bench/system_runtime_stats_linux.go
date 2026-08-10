//go:build linux

package main

import (
	"math"
	"syscall"
)

func vectorPartitionSystemKernelRuntimeStatsV1() (cpu, runQueue, timeslices, voluntary, nonvoluntary uint64) {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		user, system := syscall.TimevalToNsec(usage.Utime), syscall.TimevalToNsec(usage.Stime)
		if user >= 0 && system >= 0 && math.MaxInt64-user >= system {
			cpu = uint64(user + system)
		}
		if usage.Nvcsw >= 0 {
			voluntary = uint64(usage.Nvcsw)
		}
		if usage.Nivcsw >= 0 {
			nonvoluntary = uint64(usage.Nivcsw)
		}
	}
	// /proc/self/task/*/schedstat is not process accounting: a thread that
	// exits between samples disappears with its counters. Leave those two
	// compatibility fields unavailable rather than publish a false delta.
	return
}
