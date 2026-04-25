package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	reBatchWrite = regexp.MustCompile(`Batch Write / .* = ([0-9][0-9,]*(?:\.[0-9]+)?)`)
	reValueLog   = regexp.MustCompile(`maindb/(?:value_vlog|wal):[^\n]*\bvalue=([0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+)`)
)

type runMetrics struct {
	OpsPerSec float64
	WALValue  float64
}

func main() {
	var (
		offLogPath        string
		autoLogPath       string
		minThroughputFrac float64
		maxSizeFrac       float64
	)
	flag.StringVar(&offLogPath, "off-log", "", "path to unified_bench output for treedb_vlog_off")
	flag.StringVar(&autoLogPath, "auto-log", "", "path to unified_bench output for treedb_vlog_auto")
	flag.Float64Var(&minThroughputFrac, "min-throughput-frac", 0.95, "minimum allowed auto/off batch-write throughput fraction")
	flag.Float64Var(&maxSizeFrac, "max-size-frac", 1.02, "maximum allowed auto/off value-log bytes fraction")
	flag.Parse()

	if offLogPath == "" || autoLogPath == "" {
		fmt.Fprintln(os.Stderr, "missing -off-log or -auto-log")
		os.Exit(2)
	}

	off, err := parseMetrics(offLogPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse off log: %v\n", err)
		os.Exit(2)
	}
	auto, err := parseMetrics(autoLogPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse auto log: %v\n", err)
		os.Exit(2)
	}
	if off.OpsPerSec <= 0 || off.WALValue <= 0 {
		fmt.Fprintf(os.Stderr, "invalid off metrics: ops=%.3f wal_value=%.0f\n", off.OpsPerSec, off.WALValue)
		os.Exit(2)
	}
	if auto.OpsPerSec <= 0 || auto.WALValue <= 0 {
		fmt.Fprintf(os.Stderr, "invalid auto metrics: ops=%.3f wal_value=%.0f\n", auto.OpsPerSec, auto.WALValue)
		os.Exit(2)
	}

	throughputFrac := auto.OpsPerSec / off.OpsPerSec
	sizeFrac := auto.WALValue / off.WALValue

	fmt.Printf("incompressible gate: off_ops=%.0f auto_ops=%.0f throughput_frac=%.4f (min=%.4f)\n", off.OpsPerSec, auto.OpsPerSec, throughputFrac, minThroughputFrac)
	fmt.Printf("incompressible gate: off_value_log=%.0f auto_value_log=%.0f size_frac=%.4f (max=%.4f)\n", off.WALValue, auto.WALValue, sizeFrac, maxSizeFrac)

	fail := false
	if throughputFrac < minThroughputFrac {
		fmt.Fprintf(os.Stderr, "FAIL throughput gate: auto/off=%.4f < %.4f\n", throughputFrac, minThroughputFrac)
		fail = true
	}
	if sizeFrac > maxSizeFrac {
		fmt.Fprintf(os.Stderr, "FAIL size gate: auto/off=%.4f > %.4f\n", sizeFrac, maxSizeFrac)
		fail = true
	}
	if fail {
		os.Exit(1)
	}
	fmt.Println("PASS incompressible auto-vs-off gate")
}

func parseMetrics(path string) (runMetrics, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return runMetrics{}, err
	}
	s := string(b)

	opsMatches := reBatchWrite.FindAllStringSubmatch(s, -1)
	if len(opsMatches) == 0 {
		return runMetrics{}, fmt.Errorf("missing Batch Write metric")
	}
	ops, err := parseNumeric(opsMatches[len(opsMatches)-1][1])
	if err != nil {
		return runMetrics{}, fmt.Errorf("parse ops: %w", err)
	}

	walMatch := reValueLog.FindStringSubmatch(s)
	if len(walMatch) != 2 {
		return runMetrics{}, fmt.Errorf("missing TreeDB value-log bytes")
	}
	walBytes, err := parseBytesToken(walMatch[1])
	if err != nil {
		return runMetrics{}, fmt.Errorf("parse value-log bytes: %w", err)
	}

	return runMetrics{OpsPerSec: ops, WALValue: walBytes}, nil
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
