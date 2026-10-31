//go:build linux

package ycsbevidence

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func processIdentity(dir string) (Process, uint64, uint64, error) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return Process{}, 0, 0, err
	}
	fields := strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+1:]))
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return Process{}, 0, 0, err
	}
	hash, err := hashExe()
	if err != nil {
		return Process{}, 0, 0, err
	}
	var stat syscall.Stat_t
	if err = syscall.Stat(dir, &stat); err != nil {
		return Process{}, 0, 0, err
	}
	_, set := os.LookupEnv("GOMAXPROCS")
	return Process{os.Getpid(), ticks, hash, !set}, uint64(stat.Dev), stat.Ino, nil
}
