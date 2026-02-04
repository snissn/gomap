package main

import (
	"math"
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
