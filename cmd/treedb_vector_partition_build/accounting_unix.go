//go:build unix

package main

import "syscall"

// cpuNanos reports process user+system CPU time only where getrusage is
// available and succeeds; callers must retain the availability bit in reports.
func cpuNanos() (int64, bool) {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0, false
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + (r.Utime.Usec+r.Stime.Usec)*1e3, true
}
