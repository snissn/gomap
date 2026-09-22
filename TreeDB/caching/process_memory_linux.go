//go:build linux

package caching

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
)

func currentProcessRSSBytes() (rssBytes uint64, rssHWMBytes uint64, ok bool) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		switch {
		case bytes.HasPrefix(line, []byte("VmRSS:")):
			if v, ok := parseProcStatusKBLine(line); ok {
				rssBytes = v
			}
		case bytes.HasPrefix(line, []byte("VmHWM:")):
			if v, ok := parseProcStatusKBLine(line); ok {
				rssHWMBytes = v
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, false
	}
	return rssBytes, rssHWMBytes, true
}

func parseProcStatusKBLine(line []byte) (uint64, bool) {
	fields := bytes.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	v, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0, false
	}
	return v * 1024, true
}
