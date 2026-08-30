package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var (
	reBatchWriteSteady = regexp.MustCompile(`(?m)^\s*Batch Write \(Steady\) / .* = ([0-9][0-9,]*(?:\.[0-9]+)?)\s*$`)
	rePersistentVLog   = regexp.MustCompile(`(?m)^\s*maindb/(value_vlog|leaf_vlog):[^\n]*\btotal=([0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+)\b`)
	reUserWriteMode    = regexp.MustCompile(`(?m)^\s*vlog_write_mode\.([A-Za-z0-9_-]+):[^\n]*\braw_bytes=([0-9]+)\s+stored_bytes=([0-9]+)\b`)
	reLeafWriteMode    = regexp.MustCompile(`(?m)^\s*vlog_leaf_scan\.write_mode\.([A-Za-z0-9_-]+):[^\n]*\braw_bytes=([0-9]+)\s+stored_bytes=([0-9]+)\b`)
	reCPUTotalSamples  = regexp.MustCompile(`(?m)^Duration:\s+[^,\n]+,\s+Total samples =\s+([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s)\s+\(`)
)

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type runMetrics struct {
	OpsPerSec       float64
	PersistentVLog  float64
	UserWriteMode   string
	UserRawBytes    uint64
	UserStoredBytes uint64
	LeafWriteMode   string
	LeafRawBytes    uint64
	LeafStoredBytes uint64
}

func main() {
	var (
		offLogPaths          repeatedStringFlag
		autoLogPaths         repeatedStringFlag
		offCPUProfilePaths   repeatedStringFlag
		autoCPUProfilePaths  repeatedStringFlag
		minCPUEfficiencyFrac float64
		minCPUSeconds        float64
		maxSizeFrac          float64
		minSamples           int
	)
	flag.Var(&offLogPaths, "off-log", "path to unified_bench output for treedb_vlog_off; repeat once per paired sample")
	flag.Var(&autoLogPaths, "auto-log", "path to unified_bench output for treedb_vlog_auto; repeat once per paired sample")
	flag.Var(&offCPUProfilePaths, "off-cpu-profile", "path to exact-test CPU profile for treedb_vlog_off; repeat once per paired sample")
	flag.Var(&autoCPUProfilePaths, "auto-cpu-profile", "path to exact-test CPU profile for treedb_vlog_auto; repeat once per paired sample")
	flag.Float64Var(&minCPUEfficiencyFrac, "min-cpu-efficiency-frac", 0.95, "minimum settled auto/off CPU-efficiency fraction; values at or below this fail")
	flag.Float64Var(&minCPUSeconds, "min-cpu-seconds", 0.25, "minimum total sampled CPU seconds required from every exact-test profile")
	flag.Float64Var(&maxSizeFrac, "max-size-frac", 1.02, "maximum allowed auto/off combined persistent value-log bytes fraction")
	flag.IntVar(&minSamples, "min-samples", 2, "minimum number of complete paired samples required")
	flag.Parse()

	if len(offLogPaths) != len(autoLogPaths) {
		fmt.Fprintf(os.Stderr, "paired sample count mismatch: off=%d auto=%d\n", len(offLogPaths), len(autoLogPaths))
		os.Exit(2)
	}
	if len(offCPUProfilePaths) != len(autoCPUProfilePaths) {
		fmt.Fprintf(os.Stderr, "CPU profile pair count mismatch: off=%d auto=%d\n", len(offCPUProfilePaths), len(autoCPUProfilePaths))
		os.Exit(2)
	}
	if len(offCPUProfilePaths) != len(offLogPaths) {
		fmt.Fprintf(os.Stderr, "CPU profile/log sample count mismatch: profiles=%d logs=%d\n", len(offCPUProfilePaths), len(offLogPaths))
		os.Exit(2)
	}
	if minSamples < 2 {
		fmt.Fprintf(os.Stderr, "invalid -min-samples %d: must be at least 2\n", minSamples)
		os.Exit(2)
	}
	if minCPUEfficiencyFrac <= 0 || math.IsNaN(minCPUEfficiencyFrac) || math.IsInf(minCPUEfficiencyFrac, 0) {
		fmt.Fprintf(os.Stderr, "invalid -min-cpu-efficiency-frac %.4f: must be finite and positive\n", minCPUEfficiencyFrac)
		os.Exit(2)
	}
	if minCPUSeconds <= 0 || math.IsNaN(minCPUSeconds) || math.IsInf(minCPUSeconds, 0) {
		fmt.Fprintf(os.Stderr, "invalid -min-cpu-seconds %.4f: must be finite and positive\n", minCPUSeconds)
		os.Exit(2)
	}
	if maxSizeFrac <= 0 || math.IsNaN(maxSizeFrac) || math.IsInf(maxSizeFrac, 0) {
		fmt.Fprintf(os.Stderr, "invalid -max-size-frac %.4f: must be finite and positive\n", maxSizeFrac)
		os.Exit(2)
	}
	if len(offLogPaths) < minSamples {
		fmt.Fprintf(os.Stderr, "insufficient paired samples: got=%d want>=%d\n", len(offLogPaths), minSamples)
		os.Exit(2)
	}

	fail := false
	wallThroughputLogSum := 0.0
	cpuEfficiencyLogSum := 0.0
	maxObservedSizeFrac := 0.0
	for idx := range offLogPaths {
		off, err := parseMetrics(offLogPaths[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse off log sample %d: %v\n", idx+1, err)
			os.Exit(2)
		}
		auto, err := parseMetrics(autoLogPaths[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse auto log sample %d: %v\n", idx+1, err)
			os.Exit(2)
		}
		if off.OpsPerSec <= 0 || off.PersistentVLog <= 0 {
			fmt.Fprintf(os.Stderr, "invalid off metrics sample %d: ops=%.3f persistent_vlog=%.0f\n", idx+1, off.OpsPerSec, off.PersistentVLog)
			os.Exit(2)
		}
		if auto.OpsPerSec <= 0 || auto.PersistentVLog <= 0 {
			fmt.Fprintf(os.Stderr, "invalid auto metrics sample %d: ops=%.3f persistent_vlog=%.0f\n", idx+1, auto.OpsPerSec, auto.PersistentVLog)
			os.Exit(2)
		}
		if err := validateModeMetrics(off, auto); err != nil {
			fmt.Fprintf(os.Stderr, "invalid mode metrics sample %d: %v\n", idx+1, err)
			os.Exit(2)
		}
		offCPUSeconds, err := parseCPUProfile(offCPUProfilePaths[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse off CPU profile sample %d: %v\n", idx+1, err)
			os.Exit(2)
		}
		autoCPUSeconds, err := parseCPUProfile(autoCPUProfilePaths[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse auto CPU profile sample %d: %v\n", idx+1, err)
			os.Exit(2)
		}
		if offCPUSeconds < minCPUSeconds {
			fmt.Fprintf(os.Stderr, "off CPU profile too short sample %d: total_samples=%.6fs want>=%.6fs\n", idx+1, offCPUSeconds, minCPUSeconds)
			os.Exit(2)
		}
		if autoCPUSeconds < minCPUSeconds {
			fmt.Fprintf(os.Stderr, "auto CPU profile too short sample %d: total_samples=%.6fs want>=%.6fs\n", idx+1, autoCPUSeconds, minCPUSeconds)
			os.Exit(2)
		}

		wallThroughputFrac := auto.OpsPerSec / off.OpsPerSec
		cpuEfficiencyFrac := offCPUSeconds / autoCPUSeconds
		sizeFrac := auto.PersistentVLog / off.PersistentVLog
		wallThroughputLogSum += math.Log(wallThroughputFrac)
		cpuEfficiencyLogSum += math.Log(cpuEfficiencyFrac)
		if sizeFrac > maxObservedSizeFrac {
			maxObservedSizeFrac = sizeFrac
		}

		fmt.Printf("incompressible gate sample %d: off_ops=%.0f auto_ops=%.0f wall_throughput_frac=%.4f\n", idx+1, off.OpsPerSec, auto.OpsPerSec, wallThroughputFrac)
		fmt.Printf("incompressible gate sample %d: off_cpu_seconds=%.6f auto_cpu_seconds=%.6f cpu_efficiency_frac=%.4f\n", idx+1, offCPUSeconds, autoCPUSeconds, cpuEfficiencyFrac)
		fmt.Printf("incompressible gate sample %d: off_persistent_vlog=%.0f auto_persistent_vlog=%.0f size_frac=%.4f\n", idx+1, off.PersistentVLog, auto.PersistentVLog, sizeFrac)
		fmt.Printf("incompressible gate sample %d: user_mode=off raw_bytes=%d leaf_modes=off:%s/auto:%s leaf_raw_bytes=%d auto_leaf_stored_bytes=%d\n", idx+1, auto.UserRawBytes, off.LeafWriteMode, auto.LeafWriteMode, auto.LeafRawBytes, auto.LeafStoredBytes)
		if sizeFrac > maxSizeFrac {
			fmt.Fprintf(os.Stderr, "FAIL size gate sample %d: auto/off=%.4f > %.4f\n", idx+1, sizeFrac, maxSizeFrac)
			fail = true
		}
	}

	wallThroughputGeomean := math.Exp(wallThroughputLogSum / float64(len(offLogPaths)))
	cpuEfficiencyGeomean := math.Exp(cpuEfficiencyLogSum / float64(len(offLogPaths)))
	fmt.Printf("incompressible gate aggregate: samples=%d wall_throughput_geomean=%.4f cpu_efficiency_geomean=%.4f (min=%.4f) max_size_frac=%.4f (max=%.4f)\n", len(offLogPaths), wallThroughputGeomean, cpuEfficiencyGeomean, minCPUEfficiencyFrac, maxObservedSizeFrac, maxSizeFrac)
	if cpuEfficiencyGeomean <= minCPUEfficiencyFrac {
		fmt.Fprintf(os.Stderr, "FAIL aggregate CPU-efficiency gate: off/auto CPU geomean=%.4f <= %.4f\n", cpuEfficiencyGeomean, minCPUEfficiencyFrac)
		fail = true
	}
	if fail {
		os.Exit(1)
	}
	fmt.Println("PASS incompressible auto-vs-off aggregate CPU-efficiency gate")
}

func parseCPUProfile(path string) (float64, error) {
	output, err := exec.Command("go", "tool", "pprof", "-top", "-unit=seconds", path).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("go tool pprof: %w: %s", err, strings.TrimSpace(string(output)))
	}
	matches := reCPUTotalSamples.FindAllStringSubmatch(string(output), -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("missing Total samples metric")
	}
	if len(matches) != 1 {
		return 0, fmt.Errorf("ambiguous Total samples metrics: got %d", len(matches))
	}
	value, err := strconv.ParseFloat(matches[0][1], 64)
	if err != nil {
		return 0, fmt.Errorf("parse Total samples: %w", err)
	}
	seconds, ok := cpuSeconds(value, matches[0][2])
	if !ok {
		return 0, fmt.Errorf("unsupported Total samples unit %q", matches[0][2])
	}
	if seconds <= 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, fmt.Errorf("invalid Total samples %.6fs", seconds)
	}
	return seconds, nil
}

func cpuSeconds(value float64, unit string) (float64, bool) {
	switch unit {
	case "ns":
		return value / 1e9, true
	case "us":
		return value / 1e6, true
	case "ms":
		return value / 1e3, true
	case "s":
		return value, true
	default:
		return 0, false
	}
}

func parseMetrics(path string) (runMetrics, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return runMetrics{}, err
	}
	s := string(b)

	opsMatches := reBatchWriteSteady.FindAllStringSubmatch(s, -1)
	if len(opsMatches) == 0 {
		return runMetrics{}, fmt.Errorf("missing Batch Write (Steady) metric")
	}
	if len(opsMatches) != 1 {
		return runMetrics{}, fmt.Errorf("ambiguous Batch Write (Steady) metrics: got %d", len(opsMatches))
	}
	ops, err := parseNumeric(opsMatches[0][1])
	if err != nil {
		return runMetrics{}, fmt.Errorf("parse ops: %w", err)
	}

	persistentBytes := 0.0
	seenPersistent := make(map[string]bool, 2)
	for _, match := range rePersistentVLog.FindAllStringSubmatch(s, -1) {
		name := match[1]
		if seenPersistent[name] {
			return runMetrics{}, fmt.Errorf("duplicate maindb/%s total bytes", name)
		}
		valueBytes, err := parseBytesToken(match[2])
		if err != nil {
			return runMetrics{}, fmt.Errorf("parse maindb/%s total bytes: %w", name, err)
		}
		seenPersistent[name] = true
		persistentBytes += valueBytes
	}
	for _, name := range []string{"value_vlog", "leaf_vlog"} {
		if !seenPersistent[name] {
			return runMetrics{}, fmt.Errorf("missing maindb/%s total bytes", name)
		}
	}

	userMode, userRaw, userStored, err := parseModeMetrics(reUserWriteMode, s, "user")
	if err != nil {
		return runMetrics{}, err
	}
	leafMode, leafRaw, leafStored, err := parseModeMetrics(reLeafWriteMode, s, "leaf")
	if err != nil {
		return runMetrics{}, err
	}

	return runMetrics{
		OpsPerSec:       ops,
		PersistentVLog:  persistentBytes,
		UserWriteMode:   userMode,
		UserRawBytes:    userRaw,
		UserStoredBytes: userStored,
		LeafWriteMode:   leafMode,
		LeafRawBytes:    leafRaw,
		LeafStoredBytes: leafStored,
	}, nil
}

func parseModeMetrics(re *regexp.Regexp, s, label string) (string, uint64, uint64, error) {
	matches := re.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return "", 0, 0, fmt.Errorf("missing %s write-mode metrics", label)
	}
	if len(matches) != 1 {
		return "", 0, 0, fmt.Errorf("ambiguous %s write-mode metrics: got %d", label, len(matches))
	}
	raw, err := strconv.ParseUint(matches[0][2], 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("parse %s raw bytes: %w", label, err)
	}
	stored, err := strconv.ParseUint(matches[0][3], 10, 64)
	if err != nil {
		return "", 0, 0, fmt.Errorf("parse %s stored bytes: %w", label, err)
	}
	return matches[0][1], raw, stored, nil
}

func validateModeMetrics(off, auto runMetrics) error {
	for _, item := range []struct {
		label   string
		metrics runMetrics
	}{{"off", off}, {"auto", auto}} {
		if item.metrics.UserWriteMode != "off" {
			return fmt.Errorf("%s user write mode is %q, want %q", item.label, item.metrics.UserWriteMode, "off")
		}
		if item.metrics.UserRawBytes == 0 || item.metrics.UserStoredBytes != item.metrics.UserRawBytes {
			return fmt.Errorf("%s user values are not raw: raw=%d stored=%d", item.label, item.metrics.UserRawBytes, item.metrics.UserStoredBytes)
		}
	}
	if off.UserRawBytes != auto.UserRawBytes {
		return fmt.Errorf("user raw-byte mismatch: off=%d auto=%d", off.UserRawBytes, auto.UserRawBytes)
	}
	if off.LeafWriteMode != "off" {
		return fmt.Errorf("off leaf write mode is %q, want %q", off.LeafWriteMode, "off")
	}
	if off.LeafRawBytes == 0 || off.LeafStoredBytes != off.LeafRawBytes {
		return fmt.Errorf("off leaf values are not raw: raw=%d stored=%d", off.LeafRawBytes, off.LeafStoredBytes)
	}
	if auto.LeafWriteMode != "block" {
		return fmt.Errorf("auto leaf write mode is %q, want %q", auto.LeafWriteMode, "block")
	}
	if auto.LeafRawBytes == 0 || auto.LeafStoredBytes >= auto.LeafRawBytes {
		return fmt.Errorf("auto block leaf values are not smaller: raw=%d stored=%d", auto.LeafRawBytes, auto.LeafStoredBytes)
	}
	leafRawDelta := off.LeafRawBytes
	leafRawMax := auto.LeafRawBytes
	if leafRawDelta > auto.LeafRawBytes {
		leafRawDelta -= auto.LeafRawBytes
		leafRawMax = off.LeafRawBytes
	} else {
		leafRawDelta = auto.LeafRawBytes - leafRawDelta
	}
	if leafRawDelta > leafRawMax/1000 {
		return fmt.Errorf("leaf raw-byte mismatch exceeds 0.1%%: off=%d auto=%d", off.LeafRawBytes, auto.LeafRawBytes)
	}

	return nil
}

func parseNumeric(v string) (float64, error) {
	v = strings.TrimSpace(strings.ReplaceAll(v, ",", ""))
	return strconv.ParseFloat(v, 64)
}

func parseBytesToken(token string) (float64, error) {
	token = strings.TrimSpace(token)
	parts := strings.Fields(token)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid bytes token %q", token)
	}
	n, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, err
	}
	if n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("invalid bytes value %q", parts[0])
	}
	mult, ok := unitMultiplier(parts[1])
	if !ok {
		return 0, fmt.Errorf("unsupported unit %q", parts[1])
	}
	return n * mult, nil
}

func unitMultiplier(u string) (float64, bool) {
	switch u {
	case "B":
		return 1, true
	case "KB":
		return 1000, true
	case "MB":
		return 1000 * 1000, true
	case "GB":
		return 1000 * 1000 * 1000, true
	case "TB":
		return 1000 * 1000 * 1000 * 1000, true
	case "KiB":
		return 1024, true
	case "MiB":
		return 1024 * 1024, true
	case "GiB":
		return 1024 * 1024 * 1024, true
	case "TiB":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}
