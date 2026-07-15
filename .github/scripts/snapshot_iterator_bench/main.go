package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const benchmarkPrefix = "BenchmarkSnapshotIteratorSeekNext/"

var cpuSuffix = regexp.MustCompile(`-\d+$`)

type rowKey struct {
	Keys            int
	Mode, Operation string
}
type sample struct {
	NSPerOp     float64 `json:"ns_per_op"`
	BytesPerOp  float64 `json:"bytes_per_op"`
	AllocsPerOp float64 `json:"allocs_per_op"`
}
type capturedSample struct {
	Pair     int    `json:"pair"`
	Order    string `json:"order"`
	Mode     string `json:"mode"`
	Sequence int    `json:"sequence"`
	Sample   sample `json:"sample"`
}
type metadata struct {
	Head         string `json:"head"`
	BinarySHA256 string `json:"binary_sha256"`
	RunnerImage  string `json:"runner_image"`
	CPU          string `json:"cpu"`
	Affinity     string `json:"affinity"`
	GoVersion    string `json:"go_version"`
}
type pairResult struct {
	Pair         int     `json:"pair"`
	Order        string  `json:"order"`
	Snapshot     sample  `json:"snapshot"`
	Public       sample  `json:"public"`
	DeltaPercent float64 `json:"delta_percent"`
}
type caseResult struct {
	Keys                     int          `json:"keys"`
	Operation                string       `json:"operation"`
	Samples                  int          `json:"samples"`
	Pairs                    []pairResult `json:"pairs"`
	SnapshotMedianNS         float64      `json:"snapshot_median_ns_per_op"`
	PublicMedianNS           float64      `json:"public_median_ns_per_op"`
	IndependentDeltaPercent  float64      `json:"independent_delta_percent"`
	PairedMedianDeltaPercent float64      `json:"paired_median_delta_percent"`
	SnapshotMedianBytes      float64      `json:"snapshot_median_bytes_per_op"`
	PublicMedianBytes        float64      `json:"public_median_bytes_per_op"`
	SnapshotMedianAllocs     float64      `json:"snapshot_median_allocs_per_op"`
	PublicMedianAllocs       float64      `json:"public_median_allocs_per_op"`
	TimingPassed             bool         `json:"timing_passed"`
	AllocationGatePassed     bool         `json:"allocation_gate_passed"`
	Passed                   bool         `json:"passed"`
}
type report struct {
	Metadata             metadata     `json:"metadata"`
	RequiredSamples      int          `json:"required_samples"`
	MaxRegressionPercent float64      `json:"max_regression_percent"`
	Cases                []caseResult `json:"cases"`
	Violations           []string     `json:"violations,omitempty"`
	Passed               bool         `json:"passed"`
}
type annotation struct {
	Pair  int
	Order string
}

func main() {
	var inputPath, jsonPath, markdownPath string
	var meta metadata
	var requiredSamples int
	var maxRegression float64
	flag.StringVar(&inputPath, "bench-output", "", "path to annotated raw go benchmark output")
	flag.StringVar(&jsonPath, "json-output", "", "path for JSON report")
	flag.StringVar(&markdownPath, "markdown-output", "", "path for Markdown report")
	flag.StringVar(&meta.Head, "head", "", "exact git commit under test")
	flag.StringVar(&meta.BinarySHA256, "binary-sha256", "", "SHA-256 of the single benchmark binary")
	flag.StringVar(&meta.RunnerImage, "runner-image", "", "hosted runner image")
	flag.StringVar(&meta.CPU, "cpu", "", "runner CPU")
	flag.StringVar(&meta.Affinity, "affinity", "", "recorded CPU affinity")
	flag.StringVar(&meta.GoVersion, "go-version", "", "Go toolchain")
	flag.IntVar(&requiredSamples, "samples", 8, "required even balanced samples per benchmark row")
	flag.Float64Var(&maxRegression, "max-regression", .05, "maximum paired snapshot/public regression fraction")
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
	for _, v := range rep.Violations {
		fmt.Printf("::error::%s\n", v)
	}
	if !rep.Passed {
		os.Exit(1)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }

// Each measured row must be immediately preceded by "# pair=N order=AB|BA".
func parseBenchmarkOutput(raw string) (map[rowKey][]capturedSample, error) {
	rows := make(map[rowKey][]capturedSample)
	scanner := bufio.NewScanner(strings.NewReader(raw))
	var pending *annotation
	sequence := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "# pair=") {
			if pending != nil {
				return nil, fmt.Errorf("pair annotation without benchmark row before %q", line)
			}
			a, err := parseAnnotation(line)
			if err != nil {
				return nil, err
			}
			pending = &a
			continue
		}
		if !strings.HasPrefix(line, benchmarkPrefix) {
			continue
		}
		if pending == nil {
			return nil, fmt.Errorf("benchmark row missing pair annotation: %q", line)
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
		bytes, err := metric(fields, "B/op")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fields[0], err)
		}
		allocs, err := metric(fields, "allocs/op")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fields[0], err)
		}
		sequence++
		rows[key] = append(rows[key], capturedSample{Pair: pending.Pair, Order: pending.Order, Mode: key.Mode, Sequence: sequence, Sample: sample{ns, bytes, allocs}})
		pending = nil
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan benchmark output: %w", err)
	}
	if pending != nil {
		return nil, errors.New("pair annotation without benchmark row")
	}
	return rows, nil
}
func parseAnnotation(line string) (annotation, error) {
	var a annotation
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != "#" || !strings.HasPrefix(fields[1], "pair=") || !strings.HasPrefix(fields[2], "order=") {
		return a, fmt.Errorf("malformed pair annotation %q", line)
	}
	pair, err := strconv.Atoi(strings.TrimPrefix(fields[1], "pair="))
	a.Pair, a.Order = pair, strings.TrimPrefix(fields[2], "order=")
	if err != nil || a.Pair < 1 || (a.Order != "AB" && a.Order != "BA") {
		return a, fmt.Errorf("malformed pair annotation %q", line)
	}
	return a, nil
}
func parseRowKey(name string) (rowKey, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 || parts[0] != strings.TrimSuffix(benchmarkPrefix, "/") {
		return rowKey{}, fmt.Errorf("unexpected snapshot iterator benchmark name %q", name)
	}
	text, ok := strings.CutPrefix(parts[1], "keys=")
	if !ok {
		return rowKey{}, fmt.Errorf("missing key count in benchmark name %q", name)
	}
	keys, err := strconv.Atoi(text)
	if err != nil {
		return rowKey{}, fmt.Errorf("invalid key count in benchmark name %q", name)
	}
	mo := parts[2]
	if strings.HasPrefix(mo, "public_") {
		var ok bool
		mo, ok = strings.CutSuffix(mo, "_baseline")
		if !ok {
			return rowKey{}, fmt.Errorf("public benchmark is not labeled as a baseline in %q", name)
		}
	}
	mode, op, ok := strings.Cut(mo, "_")
	if !ok || (mode != "snapshot" && mode != "public") || (op != "seek" && op != "next") {
		return rowKey{}, fmt.Errorf("unexpected mode/operation in benchmark name %q", name)
	}
	return rowKey{keys, mode, op}, nil
}
func metric(fields []string, unit string) (float64, error) {
	for i := 1; i < len(fields); i++ {
		if fields[i] == unit {
			v, err := strconv.ParseFloat(fields[i-1], 64)
			if err != nil {
				return 0, fmt.Errorf("parse %s value %q: %w", unit, fields[i-1], err)
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("missing %s", unit)
}

func evaluate(rows map[rowKey][]capturedSample, meta metadata, required int, max float64) report {
	rep := report{Metadata: meta, RequiredSamples: required, MaxRegressionPercent: max * 100}
	if required < 2 || required%2 != 0 {
		rep.Violations = append(rep.Violations, "required samples must be a positive even number")
	}
	if math.IsNaN(max) || math.IsInf(max, 0) || max < 0 {
		rep.Violations = append(rep.Violations, "maximum regression must be a finite non-negative number")
	}
	for label, value := range map[string]string{"head": meta.Head, "binary_sha256": meta.BinarySHA256, "runner_image": meta.RunnerImage, "cpu": meta.CPU, "affinity": meta.Affinity, "go_version": meta.GoVersion} {
		if strings.TrimSpace(value) == "" {
			rep.Violations = append(rep.Violations, "missing metadata "+label)
		}
	}
	if required < 2 || required%2 != 0 || math.IsNaN(max) || math.IsInf(max, 0) || max < 0 {
		return rep
	}
	for _, keys := range []int{1024, 16384} {
		for _, op := range []string{"seek", "next"} {
			s := rows[rowKey{keys, "snapshot", op}]
			p := rows[rowKey{keys, "public", op}]
			result, violations := evaluateCase(keys, op, s, p, required, max)
			rep.Violations = append(rep.Violations, violations...)
			if result != nil {
				rep.Cases = append(rep.Cases, *result)
			}
		}
	}
	rep.Passed = len(rep.Violations) == 0 && len(rep.Cases) == 4
	return rep
}
func evaluateCase(keys int, op string, snap, pub []capturedSample, required int, max float64) (*caseResult, []string) {
	prefix := fmt.Sprintf("keys=%d operation=%s", keys, op)
	if len(snap) != required || len(pub) != required {
		return nil, []string{fmt.Sprintf("%s requires %d samples per mode; snapshot=%d public=%d", prefix, required, len(snap), len(pub))}
	}
	pairs := map[int]map[string]capturedSample{}
	ab, ba := 0, 0
	for _, x := range append(append([]capturedSample{}, snap...), pub...) {
		if !validSample(x.Sample) {
			return nil, []string{fmt.Sprintf("%s pair=%d mode=%s has non-finite, negative, or non-positive timing metric", prefix, x.Pair, x.Mode)}
		}
		if pairs[x.Pair] == nil {
			pairs[x.Pair] = map[string]capturedSample{}
		}
		if _, ok := pairs[x.Pair][x.Mode]; ok {
			return nil, []string{fmt.Sprintf("%s duplicate pair=%d mode=%s", prefix, x.Pair, x.Mode)}
		}
		pairs[x.Pair][x.Mode] = x
	}
	if len(pairs) != required {
		return nil, []string{fmt.Sprintf("%s has %d distinct pairs for %d samples", prefix, len(pairs), required)}
	}
	results := make([]pairResult, 0, len(pairs))
	for id := 1; id <= required; id++ {
		m, ok := pairs[id]
		if !ok {
			return nil, []string{fmt.Sprintf("%s missing pair=%d", prefix, id)}
		}
		a, b := m["snapshot"], m["public"]
		if a.Pair == 0 || b.Pair == 0 || a.Order != b.Order || abs(a.Sequence-b.Sequence) != 1 || (a.Order == "AB" && a.Sequence > b.Sequence) || (a.Order == "BA" && b.Sequence > a.Sequence) {
			return nil, []string{fmt.Sprintf("%s missing or unbalanced pair=%d", prefix, id)}
		}
		if a.Order == "AB" {
			ab++
		} else {
			ba++
		}
		results = append(results, pairResult{id, a.Order, a.Sample, b.Sample, (a.Sample.NSPerOp/b.Sample.NSPerOp - 1) * 100})
	}
	if ab != ba {
		return nil, []string{fmt.Sprintf("%s requires balanced AB/BA pairs; AB=%d BA=%d", prefix, ab, ba)}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Pair < results[j].Pair })
	sns, pns, sb, pb, sa, pa, deltas := []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}
	for _, x := range results {
		sns = append(sns, x.Snapshot.NSPerOp)
		pns = append(pns, x.Public.NSPerOp)
		sb = append(sb, x.Snapshot.BytesPerOp)
		pb = append(pb, x.Public.BytesPerOp)
		sa = append(sa, x.Snapshot.AllocsPerOp)
		pa = append(pa, x.Public.AllocsPerOp)
		deltas = append(deltas, x.DeltaPercent)
	}
	sn, pn := median(sns), median(pns)
	paired := median(deltas)
	independent := (sn/pn - 1) * 100
	timing := paired <= max*100
	alloc := median(sb) <= median(pb) && median(sa) <= median(pa)
	r := &caseResult{keys, op, required, results, sn, pn, independent, paired, median(sb), median(pb), median(sa), median(pa), timing, alloc, timing && alloc}
	v := []string{}
	if !timing {
		v = append(v, fmt.Sprintf("%s paired median snapshot/public delta %.2f%% exceeds max %.2f%%", prefix, paired, max*100))
	}
	if !alloc {
		v = append(v, fmt.Sprintf("%s allocation increase: snapshot %.0f B/op %.0f allocs/op; public %.0f B/op %.0f allocs/op", prefix, r.SnapshotMedianBytes, r.SnapshotMedianAllocs, r.PublicMedianBytes, r.PublicMedianAllocs))
	}
	return r, v
}
func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
func validSample(s sample) bool {
	return s.NSPerOp > 0 && s.BytesPerOp >= 0 && s.AllocsPerOp >= 0 && !math.IsNaN(s.NSPerOp) && !math.IsInf(s.NSPerOp, 0) && !math.IsNaN(s.BytesPerOp) && !math.IsInf(s.BytesPerOp, 0) && !math.IsNaN(s.AllocsPerOp) && !math.IsInf(s.AllocsPerOp, 0)
}
func median(v []float64) float64 {
	v = append([]float64(nil), v...)
	sort.Float64s(v)
	m := len(v) / 2
	if len(v)%2 == 1 {
		return v[m]
	}
	return (v[m-1] + v[m]) / 2
}
func writeReports(rep report, j, m string) error {
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err = os.WriteFile(j, append(data, '\n'), 0644); err != nil {
		return fmt.Errorf("write JSON report: %w", err)
	}
	return os.WriteFile(m, []byte(markdown(rep)), 0644)
}
func markdown(rep report) string {
	status := "PASS"
	if !rep.Passed {
		status = "FAIL"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "## Snapshot iterator performance gate: %s\n\n", status)
	fmt.Fprintf(&b, "- Exact head: `%s`\n- Binary SHA-256: `%s`\n- Runner: `%s`\n- CPU: `%s`\n- Affinity: `%s`\n- Go: `%s`\n", rep.Metadata.Head, rep.Metadata.BinarySHA256, rep.Metadata.RunnerImage, rep.Metadata.CPU, rep.Metadata.Affinity, rep.Metadata.GoVersion)
	fmt.Fprintf(&b, "- Gate: paired median snapshot/public delta <= %.2f%%; no B/op or allocs/op increase; %d balanced samples per row. Independent medians are diagnostic only.\n\n", rep.MaxRegressionPercent, rep.RequiredSamples)
	b.WriteString("| keys | operation | paired delta | snapshot/public independent medians | snapshot/public B/op | snapshot/public allocs/op | result |\n|---:|---|---:|---:|---:|---:|---|\n")
	for _, r := range rep.Cases {
		state := "PASS"
		if !r.Passed {
			state = "FAIL"
		}
		fmt.Fprintf(&b, "| %d | %s | %+.2f%% | %.3f / %.3f ns/op (%+.2f%%) | %.0f / %.0f | %.0f / %.0f | %s |\n", r.Keys, r.Operation, r.PairedMedianDeltaPercent, r.SnapshotMedianNS, r.PublicMedianNS, r.IndependentDeltaPercent, r.SnapshotMedianBytes, r.PublicMedianBytes, r.SnapshotMedianAllocs, r.PublicMedianAllocs, state)
	}
	if len(rep.Violations) > 0 {
		b.WriteString("\nViolations:\n\n")
		for _, v := range rep.Violations {
			fmt.Fprintf(&b, "- %s\n", v)
		}
	}
	return b.String()
}
