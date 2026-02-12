package main

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestRunBenchmark_PreloadsForReadAndScanOnly(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "read_rand,full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	full := run.Results["full_scan"]["TreeDB"]
	prefix := run.Results["prefix_scan"]["TreeDB"]
	if math.IsNaN(full) || full <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", full)
	}
	if math.IsNaN(prefix) || prefix <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", prefix)
	}
}

func TestRunBenchmark_RandomReadBatch_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    128,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "sequential_write,random_read_batch",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	for _, dbName := range []string{"TreeDB", "LevelDB"} {
		got := run.Results["random_read_batch"][dbName]
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected random_read_batch > 0 for %s, got %v", dbName, got)
		}
	}
}

func TestRunBenchmark_RandomReadParallel_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    128,
		ReadWorkers:  4,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "sequential_write,random_read_parallel",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	for _, dbName := range []string{"TreeDB", "LevelDB"} {
		got := run.Results["random_read_parallel"][dbName]
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected random_read_parallel > 0 for %s, got %v", dbName, got)
		}
	}
}

func TestNormalizeTests_ReadRandomBatchAliases(t *testing.T) {
	got := normalizeTests(parseList("read_rand_batch,read_random_batch,random_read_batch"))
	want := []string{"random_read_batch"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected normalize result: got=%v want=%v", got, want)
		}
	}
}

func TestNormalizeTests_ReadRandomParallelAlias(t *testing.T) {
	got := normalizeTests(parseList("read_rand_parallel,random_read_parallel"))
	want := []string{"random_read_parallel"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected normalize result: got=%v want=%v", got, want)
		}
	}
}

func TestRunBenchmark_AllIncludesRandomReadParallel(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		ReadWorkers:  2,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	for _, dbName := range []string{"TreeDB", "LevelDB"} {
		got, ok := run.Results["random_read_parallel"][dbName]
		if !ok {
			t.Fatalf("expected random_read_parallel result for %s", dbName)
		}
		if math.IsNaN(got) || got <= 0 {
			t.Fatalf("expected random_read_parallel > 0 for %s, got %v", dbName, got)
		}
	}
}

func TestRunBenchmark_PrefixScanMatchesBatchWriteKeyRange(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "batch_write,full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	full := run.Results["full_scan"]["TreeDB"]
	prefix := run.Results["prefix_scan"]["TreeDB"]
	if math.IsNaN(full) || full <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", full)
	}
	if math.IsNaN(prefix) || prefix <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", prefix)
	}
}

func TestRunChurnSuite_Smoke(t *testing.T) {
	out, err := runChurnSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runChurnSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunChurnVacuumSuite_Smoke(t *testing.T) {
	out, err := runChurnVacuumSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runChurnVacuumSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunFlushThrashSuite_Smoke(t *testing.T) {
	out, err := runFlushThrashSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runFlushThrashSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunLongMixSuite_Smoke(t *testing.T) {
	out, err := runLongMixSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runLongMixSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunBigKeysGuardSuite_Smoke(t *testing.T) {
	out, err := runBigKeysGuardSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		MaxWall: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("runBigKeysGuardSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunBenchmark_CheckpointBetweenTests_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write,random_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		CheckpointBetweenTests: true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	seq := run.Results["sequential_write"]["TreeDB"]
	randWrite := run.Results["random_write"]["TreeDB"]
	if math.IsNaN(seq) || seq <= 0 {
		t.Fatalf("expected sequential_write > 0, got %v", seq)
	}
	if math.IsNaN(randWrite) || randWrite <= 0 {
		t.Fatalf("expected random_write > 0, got %v", randWrite)
	}
}

func TestRunBenchmark_VacuumBetweenTests_Smoke(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write,random_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		CheckpointBetweenTests: true,
		VacuumBetweenTests:     true,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}

	if len(run.VacuumDurations) == 0 {
		t.Fatalf("expected non-empty vacuum durations")
	}
}

func TestRunBenchmark_CompressionVariantsMatrix_Smoke(t *testing.T) {
	prevTreeDB := *treedbVlogDictMode
	prevTreeDBCompressionVariant := *treedbVlogCompressionVariant
	prevLevelDB := *leveldbBlockCompressionMode
	defer func() {
		*treedbVlogDictMode = prevTreeDB
		*treedbVlogCompressionVariant = prevTreeDBCompressionVariant
		*leveldbBlockCompressionMode = prevLevelDB
	}()
	*treedbVlogDictMode = "both"
	*treedbVlogCompressionVariant = "default"
	*leveldbBlockCompressionMode = "both"

	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    128,
		BatchSize:    100,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "batch_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if len(run.Instances) != 4 {
		t.Fatalf("expected 4 instances, got %d", len(run.Instances))
	}

	got := run.Results["batch_write"]
	wantCols := []string{
		"TreeDB (vlog_dict=off)",
		"TreeDB (vlog_dict=on)",
		"LevelDB (block=off)",
		"LevelDB (block=on)",
	}
	for _, col := range wantCols {
		if _, ok := got[col]; !ok {
			t.Fatalf("missing result column %q (have: %v)", col, mapsKeysSorted(got))
		}
	}
}

func TestRunBenchmark_CompressionVariantsAutoMatrix_Smoke(t *testing.T) {
	prevTreeDB := *treedbVlogDictMode
	prevTreeDBCompressionVariant := *treedbVlogCompressionVariant
	prevLevelDB := *leveldbBlockCompressionMode
	defer func() {
		*treedbVlogDictMode = prevTreeDB
		*treedbVlogCompressionVariant = prevTreeDBCompressionVariant
		*leveldbBlockCompressionMode = prevLevelDB
	}()
	*treedbVlogDictMode = "default"
	*treedbVlogCompressionVariant = "all"
	*leveldbBlockCompressionMode = "both"

	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		ValueSize:    128,
		BatchSize:    100,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       "treedb,leveldb",
		TestsArg:     "batch_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if len(run.Instances) != 7 {
		t.Fatalf("expected 7 instances, got %d", len(run.Instances))
	}

	got := run.Results["batch_write"]
	wantCols := []string{
		"TreeDB (vlog=off)",
		"TreeDB (vlog=dict)",
		"TreeDB (vlog=block/snappy)",
		"TreeDB (vlog=block/lz4)",
		"TreeDB (vlog=auto)",
		"LevelDB (block=off)",
		"LevelDB (block=on)",
	}
	for _, col := range wantCols {
		if _, ok := got[col]; !ok {
			t.Fatalf("missing result column %q (have: %v)", col, mapsKeysSorted(got))
		}
	}
}

func TestRunBenchmark_KeyShapePrefix4(t *testing.T) {
	run, err := runBenchmark(BenchConfig{
		Keys:         2_000,
		KeyShape:     "be8_prefix4",
		ValueSize:    16,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "batch_write,full_scan,prefix_scan",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	full := run.Results["full_scan"]["TreeDB"]
	prefix := run.Results["prefix_scan"]["TreeDB"]
	if math.IsNaN(full) || full <= 0 {
		t.Fatalf("expected full_scan > 0, got %v", full)
	}
	if math.IsNaN(prefix) || prefix <= 0 {
		t.Fatalf("expected prefix_scan > 0, got %v", prefix)
	}
}

func TestRunBenchmark_InvalidKeyShape(t *testing.T) {
	_, err := runBenchmark(BenchConfig{
		Keys:         100,
		KeyShape:     "nope",
		ValueSize:    16,
		BatchSize:    10,
		RangeQueries: 5,
		RangeSpan:    2,
		DBsArg:       "treedb",
		TestsArg:     "sequential_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported -key-shape") {
		t.Fatalf("expected unsupported key-shape error, got %v", err)
	}
}

func TestRunBenchmark_KeyShapePrefix4RejectsOverflow(t *testing.T) {
	_, err := runBenchmark(BenchConfig{
		Keys:         (math.MaxUint32 / 10) + 1,
		KeyShape:     "be8_prefix4",
		ValueSize:    16,
		BatchSize:    10,
		RangeQueries: 5,
		RangeSpan:    2,
		DBsArg:       "treedb",
		TestsArg:     "random_write",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds uint32 range") {
		t.Fatalf("expected be8_prefix4 overflow error, got %v", err)
	}
}

func TestMakeWriteValuePool_RepeatNotAllIdentical(t *testing.T) {
	values, err := makeWriteValuePool(1, "repeat", 128, 32)
	if err != nil {
		t.Fatalf("makeWriteValuePool: %v", err)
	}
	if len(values) < 2 {
		t.Fatalf("expected pool size >= 2, got %d", len(values))
	}
	allSame := true
	for i := 1; i < len(values); i++ {
		if !bytes.Equal(values[0], values[i]) {
			allSame = false
			break
		}
	}
	if allSame {
		t.Fatalf("expected repeat value pool to contain non-identical values")
	}
}

func TestMakeWriteValuePool_CelestiaHeightPrefixFill(t *testing.T) {
	values, err := makeWriteValuePool(1, "celestia_height_prefix_fill", 128, 8)
	if err != nil {
		t.Fatalf("makeWriteValuePool: %v", err)
	}
	if len(values) != 8 {
		t.Fatalf("expected 8 values, got %d", len(values))
	}
	wantPrefix := []byte("celestia/height/")
	if !bytes.HasPrefix(values[0], wantPrefix) {
		t.Fatalf("expected prefix %q", wantPrefix)
	}
	if bytes.Equal(values[0], values[1]) {
		t.Fatalf("expected distinct values across indices")
	}
}

func mapsKeysSorted(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestMakeWriteValuePool_UnknownPatternErrors(t *testing.T) {
	if _, err := makeWriteValuePool(1, "not_a_real_pattern", 16, 0); err == nil {
		t.Fatalf("expected error")
	}
}

func TestParseTreeDBVlogCompressionVariant(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{in: "default"},
		{in: "off"},
		{in: "dict"},
		{in: "block_snappy"},
		{in: "block_lz4"},
		{in: "auto"},
		{in: "all"},
		{in: "nope", wantErr: true},
	}
	for _, tc := range cases {
		_, err := parseTreeDBVlogCompressionVariant("treedb-vlog-compression-variant", tc.in)
		if tc.wantErr && err == nil {
			t.Fatalf("expected error for %q", tc.in)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.in, err)
		}
	}
}

func TestRunFlushDrainSuite_ShortKeys(t *testing.T) {
	cfg := BenchConfig{
		Keys:                   1,
		ValueSize:              128,
		BatchSize:              1000,
		DBsArg:                 "treedb",
		TestsArg:               "all",
		KeepDir:                false,
		Progress:               false,
		SeedUsed:               1,
		CheckpointBetweenTests: true,
		MaxWall:                10 * time.Second,
	}
	if _, err := runFlushDrainSuite(cfg); err != nil {
		t.Fatalf("runFlushDrainSuite failed: %v", err)
	}
}

func TestWriteBenchprofArtifacts_WritesJSONAndMarkdown(t *testing.T) {
	dir := t.TempDir()
	runs := []BenchRun{
		{
			Config: BenchConfig{
				Keys:    123,
				Profile: "fast",
			},
			Results: map[string]map[string]float64{
				"full_scan": {
					"TreeDB": 1000,
				},
				"prefix_scan": {
					"TreeDB": 1200,
				},
			},
		},
	}

	if err := writeBenchprofArtifacts(dir, runs); err != nil {
		t.Fatalf("writeBenchprofArtifacts: %v", err)
	}

	jsonPath := filepath.Join(dir, "benchprof_results.json")
	mdPath := filepath.Join(dir, "benchprof_results.md")
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("expected json output: %v", err)
	}
	if _, err := os.Stat(mdPath); err != nil {
		t.Fatalf("expected markdown output: %v", err)
	}

	var parsed benchprofExport
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}
	if len(parsed.Runs) != 1 {
		t.Fatalf("expected 1 run in json, got %d", len(parsed.Runs))
	}
	if got := parsed.Runs[0].Results["full_scan"]["TreeDB"]; got != 1000 {
		t.Fatalf("unexpected full_scan value: %v", got)
	}
}
