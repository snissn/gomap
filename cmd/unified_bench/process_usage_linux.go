//go:build linux

package main

import (
	"errors"
	"math"
	"syscall"
)

const processUsageSupported = true

func processUsageTimevalNS(sec, usec int64) (int64, error) {
	if sec < 0 || usec < 0 || usec >= 1000000 || sec > (math.MaxInt64-usec*1000)/1000000000 {
		return 0, errors.New("invalid or overflowing rusage timeval")
	}
	return sec*1000000000 + usec*1000, nil
}

func readProcessUsage() (processUsageCounters, error) {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return processUsageCounters{}, err
	}
	user, err := processUsageTimevalNS(int64(r.Utime.Sec), int64(r.Utime.Usec))
	if err != nil {
		return processUsageCounters{}, err
	}
	system, err := processUsageTimevalNS(int64(r.Stime.Sec), int64(r.Stime.Usec))
	if err != nil {
		return processUsageCounters{}, err
	}
	counters := processUsageCounters{
		UserCPUNS:                  user,
		SystemCPUNS:                system,
		MinorFaults:                int64(r.Minflt),
		MajorFaults:                int64(r.Majflt),
		VoluntaryContextSwitches:   int64(r.Nvcsw),
		InvoluntaryContextSwitches: int64(r.Nivcsw),
	}
	if !counters.valid() {
		return processUsageCounters{}, errors.New("negative rusage counter")
	}
	return counters, nil
}
