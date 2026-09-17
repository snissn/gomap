//go:build windows

package main

import "syscall"

func processCPUNanos() (int64, error) {
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, err
	}
	var creation, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return 0, err
	}
	// Windows FILETIME is measured in 100-nanosecond intervals.
	return (filetimeTicks(kernel) + filetimeTicks(user)) * 100, nil
}

func filetimeTicks(value syscall.Filetime) int64 {
	return int64(uint64(value.HighDateTime)<<32 | uint64(value.LowDateTime))
}
