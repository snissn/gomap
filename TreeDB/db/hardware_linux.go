//go:build linux

package db

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

func detectPhysicalCoreCount() int {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	type cpuBlock struct {
		physicalID string
		coreID     string
	}
	flushBlock := func(block cpuBlock, cores map[string]struct{}) {
		if block.physicalID == "" || block.coreID == "" {
			return
		}
		cores[block.physicalID+":"+block.coreID] = struct{}{}
	}

	cores := make(map[string]struct{})
	block := cpuBlock{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			flushBlock(block, cores)
			block = cpuBlock{}
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		switch key {
		case "physical id":
			block.physicalID = value
		case "core id":
			block.coreID = value
		}
	}
	flushBlock(block, cores)
	if len(cores) > 0 {
		return len(cores)
	}

	// Some virtualized /proc/cpuinfo variants omit topology IDs but report the
	// package-local physical core count. Treat that as a best-effort fallback only
	// when it parses cleanly.
	if _, err := f.Seek(0, 0); err != nil {
		return 0
	}
	scanner = bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) != "cpu cores" {
			continue
		}
		cores, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err == nil && cores > 0 {
			return cores
		}
	}
	return 0
}
