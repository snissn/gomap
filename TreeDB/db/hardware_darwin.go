//go:build darwin

package db

import "golang.org/x/sys/unix"

func detectPhysicalCoreCount() int {
	v, err := unix.SysctlUint32("hw.physicalcpu")
	if err != nil || v == 0 {
		return 0
	}
	return int(v)
}
