package docs_test

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const strictCommandWALThroughputMinRatio = 1.01

func TestPR9AcceptancePerformanceGateIsStrictParityPlus(t *testing.T) {
	var artifact struct {
		PerformanceGate struct {
			Required               bool                   `json:"required"`
			Status                 string                 `json:"status"`
			ThroughputGateOperator string                 `json:"throughput_gate_operator"`
			Thresholds             map[string]interface{} `json:"thresholds"`
		} `json:"performance_gate"`
	}
	raw := readRepoText(t, "artifacts/command-wal/pr9/acceptance.json")
	if err := json.Unmarshal([]byte(raw), &artifact); err != nil {
		t.Fatalf("decode PR9 acceptance artifact: %v", err)
	}

	gate := artifact.PerformanceGate
	if !gate.Required {
		t.Fatal("PR9 performance gate must remain required")
	}
	if gate.ThroughputGateOperator != ">" {
		t.Fatalf("PR9 throughput gate operator=%q, want >", gate.ThroughputGateOperator)
	}
	for _, key := range []string{
		"focused_batch_write_min_ratio",
		"unified_batch_write_min_ratio",
		"point_set_longer_min_ratio",
		"vlog_auto_incompressible_min_ratio",
		"all_throughput_gates_must_exceed_ratio",
	} {
		got := requiredFloatThreshold(t, gate.Thresholds, key)
		if got != strictCommandWALThroughputMinRatio {
			t.Fatalf("PR9 threshold %s=%g, want %g", key, got, strictCommandWALThroughputMinRatio)
		}
	}
	for key, value := range gate.Thresholds {
		if !strings.Contains(key, "ratio") {
			continue
		}
		got, ok := value.(float64)
		if !ok {
			t.Fatalf("PR9 ratio threshold %s has non-numeric value %T", key, value)
		}
		if got != strictCommandWALThroughputMinRatio {
			t.Fatalf("PR9 ratio threshold %s=%g, want %g", key, got, strictCommandWALThroughputMinRatio)
		}
	}
	if allowed, ok := gate.Thresholds["allocation_regression_allowed"].(bool); !ok || allowed {
		t.Fatalf("PR9 allocation_regression_allowed=%v (%T), want false", gate.Thresholds["allocation_regression_allowed"], gate.Thresholds["allocation_regression_allowed"])
	}

	var root map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &root); err != nil {
		t.Fatalf("decode PR9 acceptance artifact as object: %v", err)
	}
	performanceGate, ok := root["performance_gate"].(map[string]interface{})
	if !ok {
		t.Fatal("PR9 acceptance artifact missing performance_gate object")
	}
	thresholds := collectRequiredThroughputThresholds(performanceGate)
	if len(thresholds) == 0 {
		t.Fatal("PR9 performance gate has no required throughput threshold ratios")
	}
	var relaxedThresholds []string
	for _, threshold := range thresholds {
		if threshold.value != strictCommandWALThroughputMinRatio {
			relaxedThresholds = append(relaxedThresholds, fmt.Sprintf("%s=%.3fx", threshold.path, threshold.value))
		}
	}
	if len(relaxedThresholds) > 0 {
		t.Fatalf("PR9 throughput gate thresholds must all be %.2fx: %s", strictCommandWALThroughputMinRatio, strings.Join(relaxedThresholds, ", "))
	}

	ratios := collectRequiredThroughputRatios(performanceGate)
	if len(ratios) == 0 {
		t.Fatal("PR9 performance gate has no required throughput ratios")
	}
	var below []string
	for _, ratio := range ratios {
		if ratio.value <= strictCommandWALThroughputMinRatio {
			below = append(below, fmt.Sprintf("%s=%.3fx", ratio.path, ratio.value))
		}
	}
	statusIsFailing := strings.Contains(strings.ToLower(gate.Status), "fail")
	if len(below) > 0 && !statusIsFailing {
		t.Fatalf("PR9 artifact accepts throughput ratios at or below %.2fx: %s", strictCommandWALThroughputMinRatio, strings.Join(below, ", "))
	}
	if len(below) == 0 && statusIsFailing {
		t.Fatalf("PR9 artifact ratios exceed %.2fx but status is still failing: %q", strictCommandWALThroughputMinRatio, gate.Status)
	}

	spec := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/verification.md"))
	assertContainsAll(t, spec, "historical strict PR9 performance gate",
		"historical strict PR9 performance gate",
		"parity-plus",
		"strictly greater than `1.01x`",
		"at or below `1.01x` is a failing gate",
		"sub-parity results such as `0.80x`",
		"every historical command-WAL acceptance artifact",
		"incompressible value-log auto/off acceptance lanes",
	)
	assertContainsAll(t, spec, "current hosted incompressible value-log gate",
		"current hosted incompressible value-log gate",
		"`batch_write_steady`",
		"AMD EPYC 7763",
		"strictly greater than `0.94x`",
		"AMD EPYC 9V74",
		"`0.93x`",
		"Every other or unknown CPU model",
		"strictly greater than `0.95x`",
		"records the CPU model and selected threshold",
		"publishes every raw wall-throughput ratio for diagnosis",
		"off CPU sample seconds divided by auto CPU sample seconds",
		"shorter-than-`0.25s` CPU profiles fail closed",
		"never retried, selected, or discarded",
		"sum of the `total=` fields",
		"`maindb/value_vlog` and `maindb/leaf_vlog`",
		"less than or equal to `1.02x`",
		"raw user values in both rows",
		"block-compressed leaves in auto",
		"geometric mean of every fixed, order-balanced CPU-efficiency pair",
		"One favorable sample cannot override a mostly failing sample set",
	)
	userCommandWAL := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/user-command-wal.md"))
	assertContainsAll(t, userCommandWAL, "historical PR9 user command WAL acceptance",
		"all historical required command-WAL acceptance performance gates",
		"historical PR9 point `Set`",
		"strictly greater than `1.01x`",
		"sub-parity evidence such as `0.80x`",
	)
	assertContainsAll(t, userCommandWAL, "current hosted incompressible value-log acceptance",
		"current hosted incompressible value-log gate",
		"`batch_write_steady`",
		"AMD EPYC 7763",
		"strict `>0.94x` threshold",
		"AMD EPYC 9V74",
		"strict `>0.93x` threshold",
		"all other or unknown CPU models",
		"strict `>0.95x` threshold",
		"records the CPU model and selected threshold",
		"retains every raw wall-throughput ratio as diagnostic evidence",
		"off CPU sample seconds divided by auto CPU sample seconds",
		"shorter-than-`0.25s` CPU profiles fail closed",
		"no row is retried, selected, or discarded",
		"sum of each pair's `total=` fields",
		"`maindb/value_vlog` plus `maindb/leaf_vlog`",
		"at or below `1.02x`",
		"raw user values in both rows",
		"block-compressed leaves in auto",
		"geometric mean of every fixed, order-balanced CPU-efficiency pair",
		"One favorable sample cannot override a mostly failing sample set",
	)
	checker := readRepoText(t, ".github/scripts/check_vlog_auto_incompressible.go")
	assertContainsAll(t, checker, "current incompressible checker",
		`flag.Float64Var(&minCPUEfficiencyFrac, "min-cpu-efficiency-frac", 0.95`,
		`flag.Float64Var(&minCPUSeconds, "min-cpu-seconds", 0.25`,
		`[]string{"value_vlog", "leaf_vlog"}`,
		"validateModeMetrics(off, auto)",
		"parseCPUProfile(offCPUProfilePaths[idx])",
		"cpuEfficiencyGeomean <= minCPUEfficiencyFrac",
	)
	workflow := collapseWhitespace(readRepoText(t, ".github/workflows/treedb-tests.yml"))
	assertContainsAll(t, workflow, "current perf smoke workflow",
		"sample_count=6",
		"record_vlog_row treedb_vlog_off",
		"record_vlog_row treedb_vlog_auto",
		`benchmark_failures+=("${db}:${output}")`,
		`incompressible gate benchmark_failures=${#benchmark_failures[@]}`,
		`if ((${#benchmark_failures[@]} > 0)); then exit 1`,
		"-test batch_write_steady",
		`-cpuprofile "${cpu_profile_prefix}"`,
		"-cpuprofile-tests batch_write_steady",
		`min_cpu_efficiency_frac="0.95"`,
		`[[ "${cpu_model}" == "AMD EPYC 7763 64-Core Processor" ]]`,
		`min_cpu_efficiency_frac="0.94"`,
		`[[ "${cpu_model}" == "AMD EPYC 9V74 80-Core Processor" ]]`,
		`min_cpu_efficiency_frac="0.93"`,
		`-min-cpu-efficiency-frac "${min_cpu_efficiency_frac}"`,
		"-min-cpu-seconds 0.25",
		"EPYC 7763 uses 0.94x, EPYC 9V74 uses 0.93x, and every other or unknown CPU uses 0.95x",
		"incompressible gate cpu_model=${cpu_model}",
		"incompressible gate min_cpu_efficiency_frac=${min_cpu_efficiency_frac}",
		"vlog_auto_incompressible_*_sample*.pprof",
		"total= fields from value_vlog plus leaf_vlog",
	)
	gateStart := strings.Index(workflow, "name: Incompressible auto-vs-off gate")
	gateEnd := strings.Index(workflow, "snapshot-iterator-perf:")
	if gateStart < 0 || gateEnd <= gateStart {
		t.Fatal("PR9 incompressible workflow gate boundaries not found")
	}
	if got := strings.Count(workflow[gateStart:gateEnd], "-profile fast"); got != 1 {
		t.Fatalf("PR9 incompressible workflow shared bench_unsafe selection count=%d, want 1", got)
	}
}

func TestCommandWALAcceptancePerformanceGatesAreStrictParityPlus(t *testing.T) {
	_, repoRoot := repoRoots(t)
	paths, err := filepath.Glob(filepath.Join(repoRoot, "artifacts", "command-wal", "pr*", "acceptance.json"))
	if err != nil {
		t.Fatalf("glob command-WAL acceptance artifacts: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no command-WAL acceptance artifacts found")
	}

	for _, path := range paths {
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			t.Fatalf("relative path for %s: %v", path, err)
		}
		rel = filepath.ToSlash(rel)

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		var root map[string]interface{}
		if err := json.Unmarshal(raw, &root); err != nil {
			t.Fatalf("decode %s: %v", rel, err)
		}

		gate, ok := root["performance_gate"].(map[string]interface{})
		if !ok {
			continue
		}
		required, _ := gate["required"].(bool)
		if !required {
			continue
		}

		status, _ := gate["status"].(string)
		statusLower := strings.ToLower(status)
		statusIsPassing := strings.Contains(statusLower, "pass") && !strings.Contains(statusLower, "fail")
		statusIsFailing := strings.Contains(statusLower, "fail")
		if !statusIsPassing && !statusIsFailing {
			t.Fatalf("%s required performance gate status=%q must explicitly pass or fail strict parity-plus", rel, status)
		}

		if op, _ := gate["throughput_gate_operator"].(string); op != ">" {
			t.Fatalf("%s required performance gate operator=%q, want >", rel, op)
		}

		thresholds := collectRequiredThroughputThresholds(gate)
		if len(thresholds) == 0 {
			t.Fatalf("%s required performance gate has no throughput threshold ratios", rel)
		}
		for _, threshold := range thresholds {
			if threshold.value != strictCommandWALThroughputMinRatio {
				t.Fatalf("%s throughput gate threshold %s=%.3fx, want %.2fx", rel, threshold.path, threshold.value, strictCommandWALThroughputMinRatio)
			}
		}

		ratios := collectRequiredThroughputRatios(gate)
		if statusIsPassing && len(ratios) == 0 {
			t.Fatalf("%s passing performance gate has no required throughput ratios", rel)
		}
		var below []string
		for _, ratio := range ratios {
			if ratio.value <= strictCommandWALThroughputMinRatio {
				below = append(below, fmt.Sprintf("%s=%.3fx", ratio.path, ratio.value))
			}
		}
		if len(below) > 0 && !statusIsFailing {
			t.Fatalf("%s accepts throughput ratios at or below %.2fx: %s", rel, strictCommandWALThroughputMinRatio, strings.Join(below, ", "))
		}
		if statusIsPassing && len(below) > 0 {
			t.Fatalf("%s passing performance gate has failing throughput ratios: %s", rel, strings.Join(below, ", "))
		}
	}
}

func requiredFloatThreshold(t *testing.T, thresholds map[string]interface{}, key string) float64 {
	t.Helper()
	value, ok := thresholds[key]
	if !ok {
		t.Fatalf("PR9 thresholds missing %s", key)
	}
	got, ok := value.(float64)
	if !ok {
		t.Fatalf("PR9 threshold %s has non-numeric value %T", key, value)
	}
	return got
}

type commandWALThroughputRatio struct {
	path  string
	value float64
}

func collectRequiredThroughputRatios(root map[string]interface{}) []commandWALThroughputRatio {
	var ratios []commandWALThroughputRatio
	var walk func(path string, value interface{}, informational bool, metadata bool)
	walk = func(path string, value interface{}, informational bool, metadata bool) {
		switch v := value.(type) {
		case map[string]interface{}:
			if status, ok := v["status"].(string); ok && strings.Contains(strings.ToLower(status), "informational") {
				informational = true
			}
			for key, child := range v {
				childPath := key
				if path != "" {
					childPath = path + "." + key
				}
				childMetadata := metadata || key == "thresholds"
				if !informational && !childMetadata && isRequiredThroughputRatioKey(key) {
					if f, ok := child.(float64); ok && !math.IsNaN(f) {
						ratios = append(ratios, commandWALThroughputRatio{path: childPath, value: f})
					}
					continue
				}
				walk(childPath, child, informational, childMetadata)
			}
		case []interface{}:
			for i, child := range v {
				walk(fmt.Sprintf("%s[%d]", path, i), child, informational, metadata)
			}
		}
	}
	walk("", root, false, false)
	return ratios
}

func collectRequiredThroughputThresholds(root map[string]interface{}) []commandWALThroughputRatio {
	var thresholds []commandWALThroughputRatio
	var walk func(path string, value interface{}, informational bool)
	walk = func(path string, value interface{}, informational bool) {
		switch v := value.(type) {
		case map[string]interface{}:
			if status, ok := v["status"].(string); ok && strings.Contains(strings.ToLower(status), "informational") {
				informational = true
			}
			for key, child := range v {
				childPath := key
				if path != "" {
					childPath = path + "." + key
				}
				if !informational && isCommandWALThroughputThresholdKey(key) {
					if f, ok := child.(float64); ok && !math.IsNaN(f) {
						thresholds = append(thresholds, commandWALThroughputRatio{path: childPath, value: f})
					}
					continue
				}
				walk(childPath, child, informational)
			}
		case []interface{}:
			for i, child := range v {
				walk(fmt.Sprintf("%s[%d]", path, i), child, informational)
			}
		}
	}
	walk("", root, false)
	return thresholds
}

func isRequiredThroughputRatioKey(key string) bool {
	normalized := strings.ToLower(key)
	if strings.Contains(normalized, "size") || strings.Contains(normalized, "alloc") || strings.Contains(normalized, "byte") {
		return false
	}
	if strings.HasPrefix(normalized, "min_") || strings.Contains(normalized, "_min_") || strings.HasSuffix(normalized, "_min_ratio") || strings.HasSuffix(normalized, "_ratios") || normalized == "min_ratio" {
		return false
	}
	if !strings.HasSuffix(normalized, "_ratio") {
		return false
	}
	return strings.Contains(normalized, "throughput") ||
		strings.Contains(normalized, "ops") ||
		strings.Contains(normalized, "sets_per_sec") ||
		strings.Contains(normalized, "docs_per_sec") ||
		strings.Contains(normalized, "collections_per_sec") ||
		strings.Contains(normalized, "batch_write") ||
		strings.Contains(normalized, "point_set") ||
		strings.Contains(normalized, "to_legacy") ||
		strings.Contains(normalized, "to_wal_off") ||
		strings.Contains(normalized, "to_off")
}

func isCommandWALThroughputThresholdKey(key string) bool {
	normalized := strings.ToLower(key)
	if strings.Contains(normalized, "size") || strings.Contains(normalized, "alloc") {
		return false
	}
	if strings.Contains(normalized, "throughput") && strings.Contains(normalized, "ratio") {
		return true
	}
	return strings.HasSuffix(normalized, "_min_ratio") || normalized == "min_ratio"
}
