//go:build unix

package main

import "syscall"

func vectorPartitionBenchmarkCPUNanos() (int64, bool) {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0, false
	}
	seconds := int64(usage.Utime.Sec) + int64(usage.Stime.Sec)
	microseconds := int64(usage.Utime.Usec) + int64(usage.Stime.Usec)
	return seconds*1e9 + microseconds*1e3, true
}
