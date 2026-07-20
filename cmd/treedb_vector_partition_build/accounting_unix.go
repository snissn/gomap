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
	// Timeval fields do not have a uniform integer width across Unix targets.
	// Convert each field before arithmetic so this also compiles on Darwin,
	// NetBSD, and AIX.
	seconds := int64(r.Utime.Sec) + int64(r.Stime.Sec)
	micros := int64(r.Utime.Usec) + int64(r.Stime.Usec)
	return seconds*1e9 + micros*1e3, true
}
