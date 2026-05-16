package docs_test

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

const pr9StrictThroughputMinRatio = 1.01

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
		"all_throughput_gates_must_exceed_ratio",
	} {
		got := requiredFloatThreshold(t, gate.Thresholds, key)
		if got != pr9StrictThroughputMinRatio {
			t.Fatalf("PR9 threshold %s=%g, want %g", key, got, pr9StrictThroughputMinRatio)
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
		if got != pr9StrictThroughputMinRatio {
			t.Fatalf("PR9 ratio threshold %s=%g, want %g", key, got, pr9StrictThroughputMinRatio)
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
	ratios := collectPR9RequiredThroughputRatios(performanceGate)
	if len(ratios) == 0 {
		t.Fatal("PR9 performance gate has no required throughput ratios")
	}
	var below []string
	for _, ratio := range ratios {
		if ratio.value <= pr9StrictThroughputMinRatio {
			below = append(below, fmt.Sprintf("%s=%.3fx", ratio.path, ratio.value))
		}
	}
	statusIsFailing := strings.Contains(strings.ToLower(gate.Status), "fail")
	if len(below) > 0 && !statusIsFailing {
		t.Fatalf("PR9 artifact accepts throughput ratios at or below %.2fx: %s", pr9StrictThroughputMinRatio, strings.Join(below, ", "))
	}
	if len(below) == 0 && statusIsFailing {
		t.Fatalf("PR9 artifact ratios exceed %.2fx but status is still failing: %q", pr9StrictThroughputMinRatio, gate.Status)
	}

	spec := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/verification.md"))
	assertContainsAll(t, spec, "strict PR9 performance gate",
		"strict parity-plus",
		"strictly greater than `1.01x`",
		"at or below `1.01x` is a failing gate",
		"sub-parity results such as `0.80x`",
	)
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

type pr9ThroughputRatio struct {
	path  string
	value float64
}

func collectPR9RequiredThroughputRatios(root map[string]interface{}) []pr9ThroughputRatio {
	var ratios []pr9ThroughputRatio
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
				if key == "public_command_wal_cached_to_legacy_ratio" && !informational {
					if f, ok := child.(float64); ok && !math.IsNaN(f) {
						ratios = append(ratios, pr9ThroughputRatio{path: childPath, value: f})
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
	return ratios
}
