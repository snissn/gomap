//go:build linux

package main

import (
	"fmt"
	"os"
	"strings"
)

func vectorPartitionBenchmarkPeakRSS() (int64, bool) {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			var kibibytes int64
			if _, err := fmt.Sscanf(line, "VmHWM: %d kB", &kibibytes); err == nil {
				return kibibytes * 1024, true
			}
		}
	}
	return 0, false
}
