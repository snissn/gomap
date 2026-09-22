//go:build linux

package main

import "syscall"

func processUsage() (cpuSeconds, maxRSSBytes float64, reason string) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0, err.Error()
	}
	cpuSeconds = float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	// Linux ru_maxrss is KiB.
	return cpuSeconds, float64(usage.Maxrss) * 1024, ""
}
