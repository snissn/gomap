//go:build darwin

package main

import "syscall"

func processUsage() (cpuSeconds float64, peakRSSBytes uint64, err error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0, err
	}
	cpuSeconds = float64(syscall.TimevalToNsec(usage.Utime)+syscall.TimevalToNsec(usage.Stime)) / 1e9
	return cpuSeconds, uint64(usage.Maxrss), nil
}
