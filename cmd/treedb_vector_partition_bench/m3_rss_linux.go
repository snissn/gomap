//go:build linux

package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

func m3PeakRSS() (int64, bool) {
	runtime.GC()
	return m3ProcStatusBytes("VmHWM:")
}

func m3ResidentBytes() (int64, bool) {
	return m3ProcStatusBytes("VmRSS:")
}

func m3ProcStatusBytes(field string) (int64, bool) {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, field) {
			var kb int64
			if _, err := fmt.Sscanf(line, field+" %d kB", &kb); err == nil && kb > 0 {
				return kb * 1024, true
			}
		}
	}
	return 0, false
}
