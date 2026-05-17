package colgranule

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestColumnWorkspacePublishesReopensAndRunsJSONBenchQueries(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	part, err := BuildJSONBenchColumnPart(ds, 32)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry, err := workspace.PublishPart(part, ds.Dictionaries)
	if err != nil {
		t.Fatalf("PublishPart: %v", err)
	}
	if entry.PartID != part.Descriptor.PartID || entry.Rows != ds.Rows || entry.AssetBytes == 0 {
		t.Fatalf("bad manifest entry=%+v part rows=%d", entry, ds.Rows)
	}
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("reopen workspace: %v", err)
	}
	defer reopened.Close()
	manifest := reopened.Manifest()
	if manifest.Collection != "jsonbench" || len(manifest.Parts) != 1 || manifest.Parts[0].PartID != part.Descriptor.PartID {
		t.Fatalf("bad reopened manifest=%+v", manifest)
	}

	first, err := reopened.LoadPartWithOptions(part.Descriptor.PartID, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("LoadPart cold: %v", err)
	}
	if first.CacheState != "cold" || first.CacheStats.MarkCache.Misses == 0 || first.CacheStats.DecodedCache.Misses == 0 {
		t.Fatalf("cold cache stats=%+v state=%s", first.CacheStats, first.CacheState)
	}
	second, err := reopened.LoadPartWithOptions(part.Descriptor.PartID, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("LoadPart warm: %v", err)
	}
	if second.CacheState != "warm" || second.CacheStats.MarkCache.Hits == 0 || second.CacheStats.DecodedCache.Hits == 0 {
		t.Fatalf("warm cache stats=%+v state=%s", second.CacheStats, second.CacheState)
	}

	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	queries := []struct {
		name string
		raw  func(JSONBenchDataset, queryCodeSet) (int, uint64)
		part jsonBenchPartQueryRunner
	}{
		{"Q1", runJSONBenchQ1, runJSONBenchPartQ1},
		{"Q2", runJSONBenchQ2, runJSONBenchPartQ2},
		{"Q3", runJSONBenchQ3, runJSONBenchPartQ3},
		{"Q4", runJSONBenchQ4, runJSONBenchPartQ4},
		{"Q5", runJSONBenchQ5, runJSONBenchPartQ5},
	}
	scratch := &jsonBenchPartQueryScratch{
		scanner:   first.Part.NewScanner(),
		projected: make(map[string][]int64, 6),
	}
	for _, query := range queries {
		rawRows, rawDigest := query.raw(ds, codes)
		partRows, partDigest, diagnostics, err := query.part(first.Part, codes, scratch)
		if err != nil {
			t.Fatalf("%s workspace query: %v", query.name, err)
		}
		if partRows != rawRows || partDigest != rawDigest {
			t.Fatalf("%s rows/digest=(%d,%d) raw=(%d,%d)", query.name, partRows, partDigest, rawRows, rawDigest)
		}
		if diagnostics.RowsScanned == 0 || diagnostics.AggregateKernel == "" {
			t.Fatalf("%s bad diagnostics=%+v", query.name, diagnostics)
		}
	}
}

func TestColumnWorkspaceRejectsUnknownManifestVersion(t *testing.T) {
	dir, _ := testColumnWorkspaceWithPart(t)
	path := filepath.Join(dir, columnWorkspaceManifestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var env columnWorkspaceManifestEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	env.Version = columnWorkspaceManifestVersion + 1
	corrupt, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal corrupt manifest: %v", err)
	}
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"}); err == nil || !strings.Contains(err.Error(), "unsupported workspace manifest version") {
		t.Fatalf("OpenColumnWorkspace err=%v want unsupported manifest version", err)
	}
}

func TestColumnWorkspaceRejectsManifestChecksumMismatch(t *testing.T) {
	dir, _ := testColumnWorkspaceWithPart(t)
	path := filepath.Join(dir, columnWorkspaceManifestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var env columnWorkspaceManifestEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	env.Manifest.Generation++
	corrupt, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal corrupt manifest: %v", err)
	}
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"}); err == nil || !strings.Contains(err.Error(), "workspace manifest checksum") {
		t.Fatalf("OpenColumnWorkspace err=%v want checksum mismatch", err)
	}
}

func TestColumnWorkspaceRejectsCorruptTCS1AssetOnReopen(t *testing.T) {
	dir, entry := testColumnWorkspaceWithPart(t)
	path := filepath.Join(dir, "assets", "column-assets-000001.seg")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteAt([]byte{0}, entry.AssetRef.Offset); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt file: %v", err)
	}
	if _, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"}); err == nil || !strings.Contains(err.Error(), "validate workspace part") {
		t.Fatalf("OpenColumnWorkspace err=%v want corrupt asset validation", err)
	}
}

func testColumnWorkspaceWithPart(t *testing.T) (string, ColumnWorkspacePartManifest) {
	t.Helper()
	ds := syntheticJSONBenchDataset(64)
	part, err := BuildJSONBenchColumnPart(ds, 16)
	if err != nil {
		t.Fatalf("BuildJSONBenchColumnPart: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry, err := workspace.PublishPart(part, ds.Dictionaries)
	if err != nil {
		t.Fatalf("PublishPart: %v", err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir, entry
}
