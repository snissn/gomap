//go:build darwin

package main

import (
	"strings"

	"golang.org/x/sys/unix"
)

func hostCPUModel() string {
	s, err := unix.Sysctl("machdep.cpu.brand_string")
	if err == nil {
		s = strings.TrimSpace(s)
		if s != "" {
			return s
		}
	}
	return ""
}

func hostMachineModel() string {
	s, err := unix.Sysctl("hw.model")
	if err == nil {
		s = strings.TrimSpace(s)
		if s != "" {
			return s
		}
	}
	return ""
}

func hostMemBytes() uint64 {
	v, err := unix.SysctlUint64("hw.memsize")
	if err == nil {
		return v
	}
	return 0
}
