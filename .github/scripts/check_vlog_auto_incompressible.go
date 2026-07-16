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

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type runMetrics struct {
	OpsPerSec float64
	WALValue  float64
}

func main() {
	var (
		offLogPaths       repeatedStringFlag
		autoLogPaths      repeatedStringFlag
		minThroughputFrac float64
		maxSizeFrac       float64
		minSamples        int
	)
	flag.Var(&offLogPaths, "off-log", "path to unified_bench output for treedb_vlog_off; repeat once per paired sample")
	flag.Var(&autoLogPaths, "auto-log", "path to unified_bench output for treedb_vlog_auto; repeat once per paired sample")
	flag.Float64Var(&minThroughputFrac, "min-throughput-frac", 1.01, "strict #1529 parity-plus auto/off batch-write throughput fraction; values at or below this fail")
	flag.Float64Var(&maxSizeFrac, "max-size-frac", 1.02, "maximum allowed auto/off value-log bytes fraction")
	flag.IntVar(&minSamples, "min-samples", 2, "minimum number of complete paired samples required")
	flag.Parse()

	if len(offLogPaths) != len(autoLogPaths) {
		fmt.Fprintf(os.Stderr, "paired sample count mismatch: off=%d auto=%d\n", len(offLogPaths), len(autoLogPaths))
		os.Exit(2)
	}
	if minSamples < 2 {
		fmt.Fprintf(os.Stderr, "invalid -min-samples %d: must be at least 2\n", minSamples)
		os.Exit(2)
	}
	if minThroughputFrac <= 0 || math.IsNaN(minThroughputFrac) || math.IsInf(minThroughputFrac, 0) {
		fmt.Fprintf(os.Stderr, "invalid -min-throughput-frac %.4f: must be finite and positive\n", minThroughputFrac)
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
	throughputLogSum := 0.0
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
		if off.OpsPerSec <= 0 || off.WALValue <= 0 {
			fmt.Fprintf(os.Stderr, "invalid off metrics sample %d: ops=%.3f wal_value=%.0f\n", idx+1, off.OpsPerSec, off.WALValue)
			os.Exit(2)
		}
		if auto.OpsPerSec <= 0 || auto.WALValue <= 0 {
			fmt.Fprintf(os.Stderr, "invalid auto metrics sample %d: ops=%.3f wal_value=%.0f\n", idx+1, auto.OpsPerSec, auto.WALValue)
			os.Exit(2)
		}

		throughputFrac := auto.OpsPerSec / off.OpsPerSec
		sizeFrac := auto.WALValue / off.WALValue
		throughputLogSum += math.Log(throughputFrac)
		if sizeFrac > maxObservedSizeFrac {
			maxObservedSizeFrac = sizeFrac
		}

		fmt.Printf("incompressible gate sample %d: off_ops=%.0f auto_ops=%.0f throughput_frac=%.4f\n", idx+1, off.OpsPerSec, auto.OpsPerSec, throughputFrac)
		fmt.Printf("incompressible gate sample %d: off_value_log=%.0f auto_value_log=%.0f size_frac=%.4f\n", idx+1, off.WALValue, auto.WALValue, sizeFrac)
		if sizeFrac > maxSizeFrac {
			fmt.Fprintf(os.Stderr, "FAIL size gate sample %d: auto/off=%.4f > %.4f\n", idx+1, sizeFrac, maxSizeFrac)
			fail = true
		}
	}

	aggregateThroughputFrac := math.Exp(throughputLogSum / float64(len(offLogPaths)))
	fmt.Printf("incompressible gate aggregate: samples=%d throughput_geomean=%.4f (min=%.4f) max_size_frac=%.4f (max=%.4f)\n", len(offLogPaths), aggregateThroughputFrac, minThroughputFrac, maxObservedSizeFrac, maxSizeFrac)
	if aggregateThroughputFrac <= minThroughputFrac {
		fmt.Fprintf(os.Stderr, "FAIL aggregate throughput gate: auto/off geomean=%.4f <= %.4f\n", aggregateThroughputFrac, minThroughputFrac)
		fail = true
	}
	if fail {
		os.Exit(1)
	}
	fmt.Println("PASS incompressible auto-vs-off aggregate gate")
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
