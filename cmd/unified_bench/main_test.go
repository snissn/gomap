package main

import (
	"math"
	"testing"
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

func TestRunChurnSuite_CompactionBeforeScans_Smoke(t *testing.T) {
	out, err := runChurnSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    2_048,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		TreeDBCompactBeforeScans:       true,
		TreeDBCompactDeadRatio:         0.10,
		TreeDBCompactMinBytes:          1,
		TreeDBCompactMaxSlabs:          1,
		TreeDBCompactMicroBatch:        64,
		TreeDBCompactRotateBeforeWrite: true,
	})
	if err != nil {
		t.Fatalf("runChurnSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestRunChurnSuite_VacuumAndCompactionBeforeScans_Smoke(t *testing.T) {
	out, err := runChurnSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    2_048,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,

		TreeDBVacuumBeforeScans:        true,
		TreeDBCompactBeforeScans:       true,
		TreeDBCompactDeadRatio:         0.10,
		TreeDBCompactMinBytes:          1,
		TreeDBCompactMaxSlabs:          1,
		TreeDBCompactMicroBatch:        64,
		TreeDBCompactRotateBeforeWrite: true,
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

func TestRunChurnMaintSuite_Smoke(t *testing.T) {
	out, err := runChurnMaintSuite(BenchConfig{
		Keys:         2_000,
		ValueSize:    2_048,
		BatchSize:    100,
		RangeQueries: 50,
		RangeSpan:    20,
		DBsArg:       "treedb",
		TestsArg:     "all",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
		TreeDBCompactBeforeScans:       true,
		TreeDBCompactDeadRatio:         0.10,
		TreeDBCompactMinBytes:          1,
		TreeDBCompactMaxSlabs:          1,
		TreeDBCompactMicroBatch:        64,
		TreeDBCompactRotateBeforeWrite: true,
	})
	if err != nil {
		t.Fatalf("runChurnMaintSuite: %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}
