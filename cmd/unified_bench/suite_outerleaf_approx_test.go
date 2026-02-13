package main

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParseOuterLeafApproxValueSizes(t *testing.T) {
	sizes, err := parseOuterLeafApproxValueSizes("128,1024,128")
	if err != nil {
		t.Fatalf("parse sizes: %v", err)
	}
	if len(sizes) != 2 || sizes[0] != 128 || sizes[1] != 1024 {
		t.Fatalf("unexpected sizes: %v", sizes)
	}
	if _, err := parseOuterLeafApproxValueSizes("128,0"); err == nil {
		t.Fatalf("expected error for zero size")
	}
}

func TestValidateOuterLeafApproxSuiteConfig_InvalidFenceFPR(t *testing.T) {
	cfg := outerLeafApproxSuiteConfig{
		Queries:             10,
		MaxKeys:             100,
		ValueSizes:          []int{128},
		BlockTargetBytes:    4096,
		BlockCacheBytes:     1 << 20,
		FenceFPR:            1.1,
		WALBytesPerRecord:   24,
		VlogRecordOverhead:  20,
		GateBytesReduction:  0.2,
		GateLookupSlowdown:  0.15,
		GateWriteRegression: 0.1,
		GateWAIncrease:      0.1,
	}
	if err := validateOuterLeafApproxSuiteConfig(cfg); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestSimulateOuterLeafLookupPattern_Deterministic(t *testing.T) {
	keys, values := makeOuterLeafApproxTestKVs(256, 128)
	blocks, _, err := buildOuterLeafApproxBlocks(keys, values, 1024, treedb.ValueLogBlockSnappy)
	if err != nil {
		t.Fatalf("build blocks: %v", err)
	}
	queries := generateOuterLeafApproxQueries(keys, 500, 12345)
	a := simulateOuterLeafLookupPattern(keys, blocks, queries, 2<<20, 0.05, 777)
	b := simulateOuterLeafLookupPattern(keys, blocks, queries, 2<<20, 0.05, 777)
	if a != b {
		t.Fatalf("expected deterministic pattern stats: a=%+v b=%+v", a, b)
	}
}

func TestModelOuterLeafWriteAmp(t *testing.T) {
	keys, values := makeOuterLeafApproxTestKVs(32, 64)
	wa := modelOuterLeafWriteAmp(keys, values, 8_000, len(values), 6_000, 8, 24, 9_000)
	if wa.LogicalBytes == 0 {
		t.Fatalf("expected non-zero logical bytes")
	}
	if wa.BaselineWA <= 0 || wa.OuterWA <= 0 {
		t.Fatalf("expected positive WA ratios: baseline=%f outer=%f", wa.BaselineWA, wa.OuterWA)
	}
	expected := (wa.OuterWA - wa.BaselineWA) / wa.BaselineWA
	if math.Abs(expected-wa.WAIncreaseFraction) > 1e-12 {
		t.Fatalf("unexpected WA increase: got=%f want=%f", wa.WAIncreaseFraction, expected)
	}
}

func TestMaybeWriteOuterLeafApproxReportJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reports", "outerleaf_approx_results.json")
	report := outerLeafApproxReport{
		GeneratedAt: "2026-02-12T00:00:00Z",
		Overall:     "PASS",
		Cases: []outerLeafApproxCaseResult{
			{Workload: "random_read", ValueSize: 128, Codec: "snappy"},
		},
	}
	if err := maybeWriteOuterLeafApproxReportJSON(path, report); err != nil {
		t.Fatalf("write json report: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read json report: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal json report: %v", err)
	}
	if parsed["overall"] != "PASS" {
		t.Fatalf("unexpected overall: %v", parsed["overall"])
	}
	if _, ok := parsed["cases"]; !ok {
		t.Fatalf("expected cases field")
	}
}

func TestRunOuterLeafApproxSuite_OverridesAndJSON_Smoke(t *testing.T) {
	oldValueSizes := *outerLeafApproxValueSizes
	oldQueries := *outerLeafApproxQueries
	oldCacheMB := *outerLeafApproxBlockCacheMB
	oldFenceFPR := *outerLeafApproxFenceFPR
	oldReportJSON := *outerLeafApproxReportJSON
	defer func() {
		*outerLeafApproxValueSizes = oldValueSizes
		*outerLeafApproxQueries = oldQueries
		*outerLeafApproxBlockCacheMB = oldCacheMB
		*outerLeafApproxFenceFPR = oldFenceFPR
		*outerLeafApproxReportJSON = oldReportJSON
	}()

	*outerLeafApproxValueSizes = "64"
	*outerLeafApproxQueries = 200
	*outerLeafApproxBlockCacheMB = 1
	*outerLeafApproxFenceFPR = 0.02
	jsonPath := filepath.Join(t.TempDir(), "outerleaf_approx_results.json")
	*outerLeafApproxReportJSON = jsonPath

	out, err := runOuterLeafApproxSuite(BenchConfig{
		Keys:         1_000,
		ValueSize:    64,
		BatchSize:    128,
		WriteWorkers: 1,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runOuterLeafApproxSuite: %v", err)
	}
	if !strings.Contains(out, "json_report:") {
		t.Fatalf("expected json_report line in output")
	}
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("expected JSON report output: %v", err)
	}
}

func makeOuterLeafApproxTestKVs(n, valueSize int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		keys[i] = k
		v := make([]byte, valueSize)
		for j := range v {
			v[j] = byte((i + j) % 251)
		}
		values[i] = v
	}
	return keys, values
}
