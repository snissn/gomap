package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type baseline struct {
	Defaults map[string]threshold `json:"defaults"`
}

type threshold struct {
	MinMBps  float64 `json:"min_mbps"`
	RatioMin float64 `json:"ratio_min"`
	RatioMax float64 `json:"ratio_max"`
}

type stats struct {
	MBps   []float64
	Ratios []float64
}

var benchSuffix = regexp.MustCompile(`-\d+$`)

func main() {
	var benchPath string
	var baselinePath string
	flag.StringVar(&benchPath, "bench-output", "", "path to benchmark output")
	flag.StringVar(&baselinePath, "baseline", "", "path to baseline json")
	flag.Parse()

	if benchPath == "" || baselinePath == "" {
		fmt.Fprintln(os.Stderr, "usage: check_slab_adaptive_bench --bench-output <file> --baseline <file>")
		os.Exit(2)
	}

	benchFile, err := os.Open(benchPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open bench output: %v\n", err)
		os.Exit(1)
	}
	defer benchFile.Close()

	baseFile, err := os.Open(baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open baseline: %v\n", err)
		os.Exit(1)
	}
	defer baseFile.Close()

	var base baseline
	if err := json.NewDecoder(baseFile).Decode(&base); err != nil {
		fmt.Fprintf(os.Stderr, "decode baseline: %v\n", err)
		os.Exit(1)
	}

	results := map[string]*stats{}
	scanner := bufio.NewScanner(benchFile)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "BenchmarkCompressionAdaptiveSweep/") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := benchSuffix.ReplaceAllString(fields[0], "")
		mbps := findFloatBefore(fields, "MB/s")
		ratio := findFloatBefore(fields, "observed_ratio")
		if mbps == 0 && ratio == 0 {
			continue
		}
		entry := results[name]
		if entry == nil {
			entry = &stats{}
			results[name] = entry
		}
		if mbps != 0 {
			entry.MBps = append(entry.MBps, mbps)
		}
		if ratio != 0 {
			entry.Ratios = append(entry.Ratios, ratio)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan bench output: %v\n", err)
		os.Exit(1)
	}

	defaultKey := "ratio=0.995/window=1048576/pause=16777216"
	for workload, limits := range base.Defaults {
		name := fmt.Sprintf("BenchmarkCompressionAdaptiveSweep/%s/%s", workload, defaultKey)
		entry, ok := results[name]
		if !ok {
			warnf("missing benchmark result for %s", name)
			continue
		}
		mbps := median(entry.MBps)
		ratio := median(entry.Ratios)

		if mbps > 0 && mbps < limits.MinMBps {
			warnf("%s MB/s %.2f below min %.2f", workload, mbps, limits.MinMBps)
		}
		if ratio > 0 && (ratio < limits.RatioMin || ratio > limits.RatioMax) {
			warnf("%s observed_ratio %.6f outside [%.3f, %.3f]", workload, ratio, limits.RatioMin, limits.RatioMax)
		}
		fmt.Printf("bench %s mbps=%.2f observed_ratio=%.6f\n", workload, mbps, ratio)
	}
}

func findFloatBefore(fields []string, token string) float64 {
	for i := 1; i < len(fields); i++ {
		if fields[i] == token && i > 0 {
			v, err := strconv.ParseFloat(fields[i-1], 64)
			if err == nil {
				return v
			}
			return 0
		}
	}
	return 0
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	mid := len(vals) / 2
	if len(vals)%2 == 1 {
		return vals[mid]
	}
	return (vals[mid-1] + vals[mid]) / 2
}

func warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("::warning::%s\n", msg)
}
