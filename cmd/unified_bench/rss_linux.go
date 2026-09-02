//go:build linux

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
)

// currentRSSBytes returns the current process resident set size (RSS) in bytes.
// On Linux this reads /proc/self/status (VmRSS).
func currentRSSBytes() (uint64, bool, error) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false, err
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		line := sc.Bytes()
		// Example: "VmRSS:\t  12345 kB"
		if !bytes.HasPrefix(line, []byte("VmRSS:")) {
			continue
		}
		fields := bytes.Fields(line)
		if len(fields) < 2 {
			return 0, false, fmt.Errorf("parse VmRSS line: %q", string(line))
		}
		kb, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("parse VmRSS value: %w", err)
		}
		return kb * 1024, true, nil
	}
	if err := sc.Err(); err != nil {
		return 0, false, err
	}
	return 0, false, fmt.Errorf("VmRSS not found in /proc/self/status")
}
