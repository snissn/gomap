package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSummarizeLatencyNearestRank(t *testing.T) {
	summary := summarizeLatency([]time.Duration{
		10 * time.Microsecond,
		50 * time.Microsecond,
		20 * time.Microsecond,
		40 * time.Microsecond,
		30 * time.Microsecond,
	})
	if summary.P50 != 30 {
		t.Fatalf("p50=%v want 30", summary.P50)
	}
	if summary.P95 != 50 {
		t.Fatalf("p95=%v want 50", summary.P95)
	}
	if summary.P99 != 50 {
		t.Fatalf("p99=%v want 50", summary.P99)
	}
}

func TestCollectDiskSnapshotBreakdown(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.db"), []byte("1234"), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "leaf_vlog"), 0o700); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "leaf_vlog", "value.log"), []byte("123456"), 0o600); err != nil {
		t.Fatalf("write value log: %v", err)
	}

	snapshot, err := collectDiskSnapshot(dir)
	if err != nil {
		t.Fatalf("collect disk snapshot: %v", err)
	}
	if snapshot.TotalBytes != 10 {
		t.Fatalf("total=%d want 10", snapshot.TotalBytes)
	}
	if snapshot.Paths["index.db"] != 4 {
		t.Fatalf("index.db=%d want 4", snapshot.Paths["index.db"])
	}
	if snapshot.Paths["leaf_vlog"] != 6 {
		t.Fatalf("leaf_vlog=%d want 6", snapshot.Paths["leaf_vlog"])
	}
}

func TestParseConfigValidation(t *testing.T) {
	if _, err := parseConfig([]string{"-target", "bad"}); err == nil {
		t.Fatal("bad target accepted")
	}
	cfg, err := parseConfig([]string{"-target", "mongo", "-documents", "10", "-secondary-indexes", "1", "-format", "json"})
	if err != nil {
		t.Fatalf("parse valid config: %v", err)
	}
	if cfg.Target != "mongo" || cfg.Documents != 10 || cfg.SecondaryIndexes != 1 || cfg.Format != "json" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestWriteResultSupportsGenericWriter(t *testing.T) {
	result := &benchmarkResult{
		Target:           "treedb",
		Database:         "bench",
		Collection:       "docs",
		Documents:        1,
		SecondaryIndexes: 1,
		Phases: []phaseResult{{
			Name:           "load_insert_many",
			Operations:     1,
			DriverCalls:    1,
			DurationMillis: 1,
			OpsPerSecond:   1000,
		}},
	}
	var out bytes.Buffer
	if err := writeResult(&out, "text", result); err != nil {
		t.Fatalf("writeResult: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("target=treedb")) {
		t.Fatalf("text output missing target: %q", out.String())
	}
}
