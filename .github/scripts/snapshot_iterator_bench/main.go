package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const benchmarkPrefix = "BenchmarkSnapshotIteratorSeekNext/"

var cpuSuffix = regexp.MustCompile(`-\d+$`)

type rowKey struct {
	Keys      int
	Mode      string
	Operation string
}

type sample struct {
	NSPerOp     float64
	BytesPerOp  float64
	AllocsPerOp float64
}

type metadata struct {
	Head        string `json:"head"`
	RunnerImage string `json:"runner_image"`
	CPU         string `json:"cpu"`
	GoVersion   string `json:"go_version"`
}

type caseResult struct {
	Keys                 int     `json:"keys"`
	Operation            string  `json:"operation"`
	Samples              int     `json:"samples"`
	SnapshotMedianNS     float64 `json:"snapshot_median_ns_per_op"`
	PublicMedianNS       float64 `json:"public_median_ns_per_op"`
	DeltaPercent         float64 `json:"delta_percent"`
	SnapshotMedianBytes  float64 `json:"snapshot_median_bytes_per_op"`
	PublicMedianBytes    float64 `json:"public_median_bytes_per_op"`
	SnapshotMedianAllocs float64 `json:"snapshot_median_allocs_per_op"`
	PublicMedianAllocs   float64 `json:"public_median_allocs_per_op"`
	TimingPassed         bool    `json:"timing_passed"`
	AllocationGatePassed bool    `json:"allocation_gate_passed"`
	Passed               bool    `json:"passed"`
}

type report struct {
	Metadata             metadata     `json:"metadata"`
	RequiredSamples      int          `json:"required_samples"`
	MaxRegressionPercent float64      `json:"max_regression_percent"`
	Cases                []caseResult `json:"cases"`
	Violations           []string     `json:"violations,omitempty"`
	Passed               bool         `json:"passed"`
}

func main() {
	var inputPath, jsonPath, markdownPath string
	var meta metadata
	var requiredSamples int
	var maxRegression float64
	flag.StringVar(&inputPath, "bench-output", "", "path to raw go benchmark output")
	flag.StringVar(&jsonPath, "json-output", "", "path for the machine-readable report")
	flag.StringVar(&markdownPath, "markdown-output", "", "path for the Markdown report")
	flag.StringVar(&meta.Head, "head", "", "exact git commit under test")
	flag.StringVar(&meta.RunnerImage, "runner-image", "", "hosted runner image")
	flag.StringVar(&meta.CPU, "cpu", "", "runner CPU description")
	flag.StringVar(&meta.GoVersion, "go-version", "", "Go toolchain description")
	flag.IntVar(&requiredSamples, "samples", 7, "required samples per benchmark row")
	flag.Float64Var(&maxRegression, "max-regression", 0.05, "maximum snapshot/public median regression fraction")
	flag.Parse()

	if inputPath == "" || jsonPath == "" || markdownPath == "" {
		fatal(errors.New("-bench-output, -json-output, and -markdown-output are required"))
	}
	raw, err := os.ReadFile(inputPath)
	if err != nil {
		fatal(fmt.Errorf("read benchmark output: %w", err))
	}
	rows, err := parseBenchmarkOutput(string(raw))
	if err != nil {
		fatal(err)
	}
	rep := evaluate(rows, meta, requiredSamples, maxRegression)
	if err := writeReports(rep, jsonPath, markdownPath); err != nil {
		fatal(err)
	}
	for _, violation := range rep.Violations {
		fmt.Printf("::error::%s\n", violation)
	}
	if !rep.Passed {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

func parseBenchmarkOutput(raw string) (map[rowKey][]sample, error) {
	rows := make(map[rowKey][]sample)
	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, benchmarkPrefix) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			return nil, fmt.Errorf("malformed snapshot iterator benchmark row: %q", line)
		}
		key, err := parseRowKey(cpuSuffix.ReplaceAllString(fields[0], ""))
		if err != nil {
			return nil, err
		}
		ns, err := metric(fields, "ns/op")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fields[0], err)
		}
		bytesPerOp, err := metric(fields, "B/op")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fields[0], err)
		}
		allocs, err := metric(fields, "allocs/op")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fields[0], err)
		}
		rows[key] = append(rows[key], sample{NSPerOp: ns, BytesPerOp: bytesPerOp, AllocsPerOp: allocs})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan benchmark output: %w", err)
	}
	return rows, nil
}

func parseRowKey(name string) (rowKey, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 || parts[0] != strings.TrimSuffix(benchmarkPrefix, "/") {
		return rowKey{}, fmt.Errorf("unexpected snapshot iterator benchmark name %q", name)
	}
	keysText, ok := strings.CutPrefix(parts[1], "keys=")
	if !ok {
		return rowKey{}, fmt.Errorf("missing key count in benchmark name %q", name)
	}
	keys, err := strconv.Atoi(keysText)
	if err != nil {
		return rowKey{}, fmt.Errorf("invalid key count in benchmark name %q", name)
	}
	modeOperation := parts[2]
	if strings.HasPrefix(modeOperation, "public_") {
		var ok bool
		modeOperation, ok = strings.CutSuffix(modeOperation, "_baseline")
		if !ok {
			return rowKey{}, fmt.Errorf("public benchmark is not labeled as a baseline in %q", name)
		}
	}
	mode, operation, ok := strings.Cut(modeOperation, "_")
	if !ok || (mode != "snapshot" && mode != "public") || (operation != "seek" && operation != "next") {
		return rowKey{}, fmt.Errorf("unexpected mode/operation in benchmark name %q", name)
	}
	return rowKey{Keys: keys, Mode: mode, Operation: operation}, nil
}

func metric(fields []string, unit string) (float64, error) {
	for i := 1; i < len(fields); i++ {
		if fields[i] != unit {
			continue
		}
		value, err := strconv.ParseFloat(fields[i-1], 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s value %q: %w", unit, fields[i-1], err)
		}
		return value, nil
	}
	return 0, fmt.Errorf("missing %s", unit)
}

func evaluate(rows map[rowKey][]sample, meta metadata, requiredSamples int, maxRegression float64) report {
	rep := report{
		Metadata:             meta,
		RequiredSamples:      requiredSamples,
		MaxRegressionPercent: maxRegression * 100,
	}
	for _, keys := range []int{1024, 16384} {
		for _, operation := range []string{"seek", "next"} {
			snapshot := rows[rowKey{Keys: keys, Mode: "snapshot", Operation: operation}]
			public := rows[rowKey{Keys: keys, Mode: "public", Operation: operation}]
			if len(snapshot) != requiredSamples || len(public) != requiredSamples {
				rep.Violations = append(rep.Violations, fmt.Sprintf(
					"keys=%d operation=%s requires %d samples per mode; snapshot=%d public=%d",
					keys, operation, requiredSamples, len(snapshot), len(public)))
				continue
			}
			snapshotNS := median(samplesMetric(snapshot, func(s sample) float64 { return s.NSPerOp }))
			publicNS := median(samplesMetric(public, func(s sample) float64 { return s.NSPerOp }))
			snapshotBytes := median(samplesMetric(snapshot, func(s sample) float64 { return s.BytesPerOp }))
			publicBytes := median(samplesMetric(public, func(s sample) float64 { return s.BytesPerOp }))
			snapshotAllocs := median(samplesMetric(snapshot, func(s sample) float64 { return s.AllocsPerOp }))
			publicAllocs := median(samplesMetric(public, func(s sample) float64 { return s.AllocsPerOp }))
			delta := 0.0
			if publicNS > 0 {
				delta = ((snapshotNS / publicNS) - 1) * 100
			}
			timingPassed := publicNS > 0 && snapshotNS <= publicNS*(1+maxRegression)
			allocationPassed := snapshotBytes <= publicBytes && snapshotAllocs <= publicAllocs
			result := caseResult{
				Keys: keys, Operation: operation, Samples: requiredSamples,
				SnapshotMedianNS: snapshotNS, PublicMedianNS: publicNS, DeltaPercent: delta,
				SnapshotMedianBytes: snapshotBytes, PublicMedianBytes: publicBytes,
				SnapshotMedianAllocs: snapshotAllocs, PublicMedianAllocs: publicAllocs,
				TimingPassed: timingPassed, AllocationGatePassed: allocationPassed,
				Passed: timingPassed && allocationPassed,
			}
			rep.Cases = append(rep.Cases, result)
			if !timingPassed {
				rep.Violations = append(rep.Violations, fmt.Sprintf(
					"keys=%d operation=%s snapshot median %.3f ns/op exceeds public %.3f ns/op by %.2f%% (max %.2f%%)",
					keys, operation, snapshotNS, publicNS, delta, maxRegression*100))
			}
			if !allocationPassed {
				rep.Violations = append(rep.Violations, fmt.Sprintf(
					"keys=%d operation=%s allocation increase: snapshot %.0f B/op %.0f allocs/op; public %.0f B/op %.0f allocs/op",
					keys, operation, snapshotBytes, snapshotAllocs, publicBytes, publicAllocs))
			}
		}
	}
	rep.Passed = len(rep.Violations) == 0 && len(rep.Cases) == 4
	return rep
}

func samplesMetric(samples []sample, pick func(sample) float64) []float64 {
	values := make([]float64, len(samples))
	for i := range samples {
		values[i] = pick(samples[i])
	}
	return values
}

func median(values []float64) float64 {
	values = append([]float64(nil), values...)
	sort.Float64s(values)
	mid := len(values) / 2
	if len(values)%2 == 1 {
		return values[mid]
	}
	return (values[mid-1] + values[mid]) / 2
}

func writeReports(rep report, jsonPath, markdownPath string) error {
	encoded, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(jsonPath, append(encoded, '\n'), 0o644); err != nil {
		return fmt.Errorf("write JSON report: %w", err)
	}
	if err := os.WriteFile(markdownPath, []byte(markdown(rep)), 0o644); err != nil {
		return fmt.Errorf("write Markdown report: %w", err)
	}
	return nil
}

func markdown(rep report) string {
	status := "PASS"
	if !rep.Passed {
		status = "FAIL"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "## Snapshot iterator performance gate: %s\n\n", status)
	fmt.Fprintf(&b, "- Exact head: `%s`\n- Runner: `%s`\n- CPU: `%s`\n- Go: `%s`\n", rep.Metadata.Head, rep.Metadata.RunnerImage, rep.Metadata.CPU, rep.Metadata.GoVersion)
	fmt.Fprintf(&b, "- Gate: snapshot median <= public median + %.2f%%; no B/op or allocs/op increase; %d samples per row.\n\n", rep.MaxRegressionPercent, rep.RequiredSamples)
	b.WriteString("| keys | operation | snapshot median | public median | delta | snapshot/public B/op | snapshot/public allocs/op | result |\n")
	b.WriteString("|---:|---|---:|---:|---:|---:|---:|---|\n")
	for _, result := range rep.Cases {
		rowStatus := "PASS"
		if !result.Passed {
			rowStatus = "FAIL"
		}
		fmt.Fprintf(&b, "| %d | %s | %.3f ns/op | %.3f ns/op | %+.2f%% | %.0f / %.0f | %.0f / %.0f | %s |\n",
			result.Keys, result.Operation, result.SnapshotMedianNS, result.PublicMedianNS, result.DeltaPercent,
			result.SnapshotMedianBytes, result.PublicMedianBytes, result.SnapshotMedianAllocs, result.PublicMedianAllocs, rowStatus)
	}
	if len(rep.Violations) > 0 {
		b.WriteString("\nViolations:\n\n")
		for _, violation := range rep.Violations {
			fmt.Fprintf(&b, "- %s\n", violation)
		}
	}
	return b.String()
}
