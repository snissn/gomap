//go:build linux

package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

// peakRSS reads Linux VmHWM, whose unit is kB. Other platforms explicitly
// report this measurement as unavailable instead of fabricating a zero.
func peakRSS() (int64, bool) {
	runtime.GC()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			var kb int64
			if _, err := fmt.Sscanf(line, "VmHWM: %d kB", &kb); err == nil {
				return kb * 1024, true
			}
		}
	}
	return 0, false
}
