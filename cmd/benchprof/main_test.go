package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestParseScanOpsMarkdown(t *testing.T) {
	input := `
Full Scan / TreeDB (vlog=off) = 2,432,548
Prefix Scan / TreeDB (vlog=off) = 5,457,443
`
	rows := parseScanOpsMarkdown(input)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	if row.Label != "TreeDB (vlog=off)" {
		t.Fatalf("unexpected label: %q", row.Label)
	}
	if row.FullScan != 2432548 {
		t.Fatalf("unexpected full_scan: %.0f", row.FullScan)
	}
	if row.Prefix != 5457443 {
		t.Fatalf("unexpected prefix_scan: %.0f", row.Prefix)
	}
	wantRatio := 5457443.0 / 2432548.0
	if math.Abs(row.PrefixDiv-wantRatio) > 0.000001 {
		t.Fatalf("unexpected ratio: got %.8f want %.8f", row.PrefixDiv, wantRatio)
	}
}

func TestParsePprofTopOutput(t *testing.T) {
	input := `
Showing nodes accounting for 14.35s, 92.82% of 15.46s total
Dropped 233 nodes (cum <= 0.08s)
      flat  flat%   sum%        cum   cum%
     8.15s 52.72% 52.72%      8.15s 52.72%  runtime.memmove
     1.11s  7.18% 59.90%      1.11s  7.18%  bytes.(*Buffer).Write
`
	got := parsePprofTopOutput(input)
	if got.total != "15.46s" {
		t.Fatalf("unexpected total: %q", got.total)
	}
	if len(got.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got.entries))
	}
	if got.entries[0].Function != "runtime.memmove" {
		t.Fatalf("unexpected first fn: %q", got.entries[0].Function)
	}
	if got.entries[0].FlatPct != 52.72 {
		t.Fatalf("unexpected first flat pct: %.2f", got.entries[0].FlatPct)
	}
}

func TestDiscoverProfileFiles(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile := func(rel string) {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	mustWriteFile("cpu_full_scan_treedb_vlog_off.pprof")
	mustWriteFile("nested/cpu_prefix_scan_treedb_vlog_off.pprof")
	mustWriteFile("block.pprof")
	mustWriteFile("mutex.pprof")
	mustWriteFile("trace.out")

	files, err := discoverProfileFiles(dir)
	if err != nil {
		t.Fatalf("discoverProfileFiles: %v", err)
	}

	if got := files.fullScanCPU["treedb_vlog_off"]; got == "" {
		t.Fatalf("missing full_scan profile")
	}
	if got := files.prefixScanCPU["treedb_vlog_off"]; got == "" {
		t.Fatalf("missing prefix_scan profile")
	}
	if files.blockPath == "" {
		t.Fatalf("missing block profile")
	}
	if files.mutexPath == "" {
		t.Fatalf("missing mutex profile")
	}
	if files.tracePath == "" {
		t.Fatalf("missing trace profile")
	}
}
