//go:build linux

package main

import (
	"fmt"
	"math"
	"os"
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
	entries, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return
	}
	for _, entry := range entries {
		var threadCPU, threadRunQueue, threadTimeslices uint64
		raw, readErr := os.ReadFile("/proc/self/task/" + entry.Name() + "/schedstat")
		if readErr != nil {
			continue
		}
		if _, scanErr := fmt.Sscan(string(raw), &threadCPU, &threadRunQueue, &threadTimeslices); scanErr != nil || math.MaxUint64-runQueue < threadRunQueue || math.MaxUint64-timeslices < threadTimeslices {
			continue
		}
		runQueue += threadRunQueue
		timeslices += threadTimeslices
	}
	return
}
