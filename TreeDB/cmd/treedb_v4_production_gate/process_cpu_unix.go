//go:build unix

package main

import "syscall"

func processCPUNanos() (int64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return int64(usage.Utime.Sec)*1e9 + int64(usage.Utime.Usec)*1e3 +
		int64(usage.Stime.Sec)*1e9 + int64(usage.Stime.Usec)*1e3, nil
}
