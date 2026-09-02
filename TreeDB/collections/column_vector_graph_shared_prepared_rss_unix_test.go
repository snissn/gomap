//go:build !windows

package collections

import (
	"runtime"
	"syscall"
)

func processMaxRSSBytes1735() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil || usage.Maxrss <= 0 {
		return 0
	}
	maxRSS := uint64(usage.Maxrss)
	switch runtime.GOOS {
	case "linux", "freebsd", "openbsd", "netbsd":
		return maxRSS * 1024
	default:
		return maxRSS
	}
}
