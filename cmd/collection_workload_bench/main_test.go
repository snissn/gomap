package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

func TestParseFormatsAcceptsCollectionsV1Alias(t *testing.T) {
	formats, err := parseFormats("json,collections-v1,bson")
	if err != nil {
		t.Fatalf("parseFormats: %v", err)
	}
	want := []collections.DocumentFormat{
		collections.DocumentFormatJSON,
		collections.DocumentFormatTemplateV1,
		collections.DocumentFormatBSON,
	}
	if len(formats) != len(want) {
		t.Fatalf("formats len=%d want %d", len(formats), len(want))
	}
	for i := range want {
		if formats[i] != want[i] {
			t.Fatalf("formats[%d]=%q want %q", i, formats[i], want[i])
		}
	}
}

func TestRunSmallNativeCollectionMatrix(t *testing.T) {
	cfg := config{
		Documents:             32,
		BatchSize:             16,
		Reads:                 8,
		RangeReads:            4,
		Updates:               4,
		Deletes:               2,
		Formats:               []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1, collections.DocumentFormatBSON},
		IndexCounts:           []int{2},
		ReadStates:            []readState{readStateBuffered},
		ReaderSweep:           []int{1, 2},
		OutputFormat:          "json",
		TreeDBProfile:         treedb.ProfileBench,
		DataRootStorage:       collections.RootStorageDefault,
		IndexStateRootStorage: collections.RootStorageDefault,
		IndexRootStorage:      collections.RootStorageDefault,
	}
	res, err := run(cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := len(res.Rows), len(cfg.Formats); got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	for _, row := range res.Rows {
		if row.Load.Operations != int64(cfg.Documents) {
			t.Fatalf("%s load ops=%d want %d", row.Format, row.Load.Operations, cfg.Documents)
		}
		requirePhase(t, row, "id_find_one", false)
		requirePhase(t, row, "email_find_one", false)
		requirePhase(t, row, "age_range_indexed_limit_10", false)
		requirePhase(t, row, "age_range_scan_limit_10", false)
		requirePhase(t, row, "concurrent_id_find_one_r2", false)
		requirePhase(t, row, "concurrent_email_find_one_r2", false)
		requirePhase(t, row, "concurrent_age_range_indexed_limit_10_r2", false)
		requirePhase(t, row, "id_update_set", false)
		requirePhase(t, row, "concurrent_id_update_set_w2", false)
		requirePhase(t, row, "id_delete_one", false)
	}
}

func TestIndexesZeroSkipsSecondaryNativePhases(t *testing.T) {
	cfg := config{
		Documents:             16,
		BatchSize:             16,
		Reads:                 4,
		RangeReads:            2,
		Updates:               0,
		Deletes:               0,
		Formats:               []collections.DocumentFormat{collections.DocumentFormatJSON},
		IndexCounts:           []int{0},
		ReadStates:            []readState{readStateBuffered},
		ReaderSweep:           []int{1},
		OutputFormat:          "json",
		TreeDBProfile:         treedb.ProfileBench,
		DataRootStorage:       collections.RootStorageDefault,
		IndexStateRootStorage: collections.RootStorageDefault,
		IndexRootStorage:      collections.RootStorageDefault,
	}
	res, err := run(cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := len(res.Rows), 1; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	row := res.Rows[0]
	requirePhase(t, row, "id_find_one", false)
	requirePhase(t, row, "email_find_one", true)
	requirePhase(t, row, "age_range_indexed_limit_10", true)
	requirePhase(t, row, "age_range_scan_limit_10", false)
}

func TestResetTreeDBDirRefusesNonEmptyUnmarkedDir(t *testing.T) {
	dir := t.TempDir()
	important := filepath.Join(dir, "important.txt")
	if err := os.WriteFile(important, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write important file: %v", err)
	}
	err := resetTreeDBDir(dir)
	if err == nil {
		t.Fatal("resetTreeDBDir succeeded for non-empty unmarked directory")
	}
	if !strings.Contains(err.Error(), "refusing to delete non-empty treedb-dir") {
		t.Fatalf("error=%v want refusal", err)
	}
	if got, statErr := os.ReadFile(important); statErr != nil || string(got) != "keep" {
		t.Fatalf("important file changed, got=%q err=%v", got, statErr)
	}
}

func TestResetTreeDBDirAllowsMarkedBenchmarkDir(t *testing.T) {
	dir := t.TempDir()
	if err := writeTreeDBBenchDirMarker(dir); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	oldFile := filepath.Join(dir, "old.db")
	if err := os.WriteFile(oldFile, []byte("old"), 0o600); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	if err := resetTreeDBDir(dir); err != nil {
		t.Fatalf("resetTreeDBDir: %v", err)
	}
	if _, err := os.Stat(oldFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old file still exists or unexpected stat err: %v", err)
	}
	if !hasTreeDBBenchDirMarker(dir) {
		t.Fatal("benchmark marker missing after reset")
	}
}

func requirePhase(t *testing.T, row rowResult, name string, skipped bool) {
	t.Helper()
	for _, phase := range row.Phases {
		if phase.Name != name {
			continue
		}
		if phase.Skipped != skipped {
			t.Fatalf("%s/%s skipped=%v want %v", row.Format, name, phase.Skipped, skipped)
		}
		if !skipped {
			if phase.Operations <= 0 {
				t.Fatalf("%s/%s operations=%d want >0", row.Format, name, phase.Operations)
			}
			if phase.OpsPerSec <= 0 || phase.NSPerOp <= 0 {
				t.Fatalf("%s/%s ops/sec=%f ns/op=%f want positive", row.Format, name, phase.OpsPerSec, phase.NSPerOp)
			}
		}
		return
	}
	t.Fatalf("%s phase %q not found", row.Format, name)
}
