package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type baselineFile struct {
	Benchmarks []baselineEntry `json:"benchmarks"`
}

type baselineEntry struct {
	Name             string  `json:"name"`
	MinMBps          float64 `json:"min_mbps"`
	MinObservedRatio float64 `json:"min_observed_ratio"`
	MaxObservedRatio float64 `json:"max_observed_ratio"`
	MaxDictFallback  float64 `json:"max_dict_fallback"`
}

type parsedBench struct {
	MBps          float64
	ObservedRatio float64
	DictFallback  float64
}

func main() {
	var benchOutputPath string
	var baselinePath string
	var strict bool

	flag.StringVar(&benchOutputPath, "bench-output", "", "path to go test benchmark output")
	flag.StringVar(&baselinePath, "baseline", "", "path to baseline json")
	flag.BoolVar(&strict, "strict", false, "exit non-zero on baseline violations")
	flag.Parse()

	if benchOutputPath == "" || baselinePath == "" {
		fmt.Fprintln(os.Stderr, "missing -bench-output or -baseline")
		os.Exit(2)
	}

	benchBytes, err := os.ReadFile(benchOutputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read bench output: %v\n", err)
		os.Exit(2)
	}

	baseBytes, err := os.ReadFile(baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read baseline: %v\n", err)
		os.Exit(2)
	}

	var baseline baselineFile
	if err := json.Unmarshal(baseBytes, &baseline); err != nil {
		fmt.Fprintf(os.Stderr, "parse baseline json: %v\n", err)
		os.Exit(2)
	}

	byName, err := parseGoBenchOutput(string(benchBytes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse bench output: %v\n", err)
		os.Exit(2)
	}

	violations := 0
	for _, entry := range baseline.Benchmarks {
		samples := byName[entry.Name]
		if len(samples) == 0 {
			msg := fmt.Sprintf("missing benchmark in output: %s", entry.Name)
			emitViolation(strict, msg)
			violations++
			continue
		}

		mbps := trimmedMeanFloat(samples, func(s parsedBench) float64 { return s.MBps })
		ratio := trimmedMeanFloat(samples, func(s parsedBench) float64 { return s.ObservedRatio })
		fallbackMax := maxFloat(samples, func(s parsedBench) float64 { return s.DictFallback })

		fmt.Printf("perf: %s runs=%d mbps=%.2f ratio=%.5f fallback_max=%.0f\n", entry.Name, len(samples), mbps, ratio, fallbackMax)

		if mbps < entry.MinMBps {
			msg := fmt.Sprintf("%s MB/s %.2f < min %.2f", entry.Name, mbps, entry.MinMBps)
			emitViolation(strict, msg)
			violations++
		}
		if !math.IsNaN(ratio) && (ratio < entry.MinObservedRatio || ratio > entry.MaxObservedRatio) {
			msg := fmt.Sprintf("%s observed_ratio %.5f not in [%.5f, %.5f]", entry.Name, ratio, entry.MinObservedRatio, entry.MaxObservedRatio)
			emitViolation(strict, msg)
			violations++
		}
		if !math.IsNaN(fallbackMax) && fallbackMax > entry.MaxDictFallback {
			msg := fmt.Sprintf("%s dict_fallback max %.0f > max %.0f", entry.Name, fallbackMax, entry.MaxDictFallback)
			emitViolation(strict, msg)
			violations++
		}
	}

	if strict && violations > 0 {
		os.Exit(1)
	}
}

func emitViolation(strict bool, msg string) {
	if strict {
		fmt.Printf("::error::%s\n", msg)
	} else {
		fmt.Printf("::warning::%s\n", msg)
	}
}

var cpuSuffix = regexp.MustCompile(`-\d+$`)

func parseGoBenchOutput(out string) (map[string][]parsedBench, error) {
	result := make(map[string][]parsedBench)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		name := cpuSuffix.ReplaceAllString(fields[0], "")

		mbps := parseMetric(fields, "MB/s")
		ratio := parseMetric(fields, "observed_ratio")
		fallback := parseMetric(fields, "dict_fallback")

		result[name] = append(result[name], parsedBench{
			MBps:          mbps,
			ObservedRatio: ratio,
			DictFallback:  fallback,
		})
	}
	return result, nil
}

func parseMetric(fields []string, unit string) float64 {
	for i := 1; i < len(fields); i++ {
		if fields[i] != unit {
			continue
		}
		if i == 0 {
			break
		}
		v, err := strconv.ParseFloat(fields[i-1], 64)
		if err != nil {
			return math.NaN()
		}
		return v
	}
	return math.NaN()
}

func trimmedMeanFloat(samples []parsedBench, pick func(parsedBench) float64) float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		v := pick(sample)
		if math.IsNaN(v) {
			continue
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		return math.NaN()
	}
	sort.Float64s(values)

	keep := values
	if len(values) >= 5 {
		keep = values[1 : len(values)-1] // keep middle 3 for len=5
	}
	sum := 0.0
	for _, v := range keep {
		sum += v
	}
	return sum / float64(len(keep))
}

func maxFloat(samples []parsedBench, pick func(parsedBench) float64) float64 {
	maxV := math.NaN()
	for _, sample := range samples {
		v := pick(sample)
		if math.IsNaN(v) {
			continue
		}
		if math.IsNaN(maxV) || v > maxV {
			maxV = v
		}
	}
	return maxV
}
