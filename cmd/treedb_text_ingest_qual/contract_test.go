package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateCompleteMatrix(t *testing.T) {
	m := validManifest()
	r := validReport(t, m)
	if err := validate(m, r, manifestHash(t, m)); err != nil {
		t.Fatal(err)
	}
}
func TestValidateRejectsContractFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*manifest, *report)
		want string
	}{
		{"vectors", func(m *manifest, _ *report) { m.Observed.VectorsEnabled = true }, "vectors disabled"},
		{"missing matrix", func(_ *manifest, r *report) { r.Rows = r.Rows[1:] }, "missing required mode/scale"},
		{"duplicate retained repetition", func(_ *manifest, r *report) { r.Rows = append(r.Rows, r.Rows[12]) }, "duplicate repetition"},
		{"copied summary", func(_ *manifest, r *report) { r.Summaries[0].MedianWallSeconds = 2 }, "summary does not recompute"},
		{"zero stage placeholder", func(_ *manifest, r *report) { r.Rows[0].Stages["value_log"] = metric{State: "observed"} }, "zero placeholder"},
		{"storage overlap", func(_ *manifest, r *report) { r.Rows[0].Storage.PhysicalTotalBytes++ }, "physical total"},
		{"source parent text index accounting", func(_ *manifest, r *report) {
			r.Rows[0].Mode = "source_chunk"
			r.Rows[0].GeneratedChunks = 1
			r.Rows[0].ParentsTextIndexed = false
		}, "returned parent"},
		{"source parent child live count", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "source_chunk" {
					r.Rows[i].IndexedLiveRows++
					break
				}
			}
		}, "returned parent"},
		{"source count drift", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "source_chunk" {
					r.Rows[i].SourceDocuments--
					break
				}
			}
		}, "document accounting"},
		{"maintenance count semantics", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "maintenance" {
					r.Rows[i].IndexedLiveRows = 0
					break
				}
			}
		}, "document accounting"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			r := validReport(t, m)
			tc.edit(&m, &r)
			if err := validate(m, r, manifestHash(t, m)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
	}
}
func TestObserveStorageClassifiesSyntheticTree(t *testing.T) {
	dir := t.TempDir()
	for path, contents := range map[string]string{
		"index.db": "i", "value_vlog/0001": "vv", "wal/0001": "www", "metadata": "oooo",
	} {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s, err := observeStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.PhysicalIndexPageBytes != 1 || s.PhysicalValueLogBytes != 2 || s.PhysicalWALBytes != 3 || s.PhysicalOtherBytes != 4 || s.PhysicalTotalBytes != 10 || s.PhysicalTotalWALExcludedBytes != 7 {
		t.Fatalf("unexpected storage: %+v", s)
	}
	if len(s.OtherPaths) != 1 || s.OtherPaths[0] != "metadata" {
		t.Fatalf("other paths: %v", s.OtherPaths)
	}
}

func validManifest() manifest {
	return manifest{SchemaVersion: contractVersion, FixtureSHA256: "fixture", Analyzer: "simple", FieldWeights: "title=3,body=1", IDsSHA256: "ids", Command: "go run", Commit: "abcdef", Host: "test", CacheState: "cold", Durability: "wal_on", TimedBoundary: "insert through checkpoint", Observed: observedIdentity{VCSClean: true, Commit: "abcdef", Durability: "wal_on", VectorIndexes: 0}}
}
func manifestHash(t *testing.T, m manifest) string {
	t.Helper()
	b, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	x := sha256.Sum256(b)
	return hex.EncodeToString(x[:])
}
func validReport(t *testing.T, m manifest) report {
	t.Helper()
	r := report{SchemaVersion: contractVersion, ManifestSHA256: manifestHash(t, m)}
	for _, mode := range requiredModes {
		for _, scale := range requiredScales {
			n := 1
			if scale > 10000 {
				n = 3
			}
			for rep := 1; rep <= n; rep++ {
				r.Rows = append(r.Rows, validRow(mode, scale, rep))
			}
			r.Summaries = append(r.Summaries, summaryFor(mode, scale, n))
		}
	}
	return r
}
func validRow(mode string, scale, rep int) row {
	source, chunks, live := scale, 0, scale
	parentsIndexed := false
	if mode == "source_chunk" {
		parentsIndexed = true
		chunks = scale * 2
		live = source + chunks
	}
	if mode == "maintenance" {
		live = scale / 2
	}
	return row{Mode: mode, Scale: scale, Repetition: rep, SourceDocuments: source, GeneratedChunks: chunks, IndexedLiveRows: live, ParentsTextIndexed: parentsIndexed, IndexedParentRows: func() int {
		if mode == "source_chunk" {
			return source
		}
		return 0
	}(), Postings: 1, Terms: 1, Blocks: 1, Generations: 1, SourceDocsPerSec: 1, ChunksPerSec: 1, IndexedRowsPerSec: 1, WallSeconds: 1, CPUSeconds: metric{State: "unavailable", Reason: "platform"}, BytesPerOp: metric{State: "unavailable", Reason: "not a Go benchmark"}, AllocsPerOp: metric{State: "unavailable", Reason: "not a Go benchmark"}, CumulativeAllocs: metric{State: "observed", Value: 1}, PeakRSSBytes: metric{State: "observed", Value: 1}, Stages: map[string]metric{"analyzer": {State: "observed", Value: 1}, "posting_builder": {State: "observed", Value: 1}, "root_mutation": {State: "observed", Value: 1}, "value_log": {State: "unavailable", Reason: "not separately instrumented"}, "checkpoint": {State: "observed", Value: 1}, "reopen": {State: "observed", Value: 1}}, Storage: storage{PhysicalIndexPageBytes: 1, PhysicalValueLogBytes: 1, PhysicalWALBytes: 1, PhysicalOtherBytes: 1, PhysicalTotalBytes: 4, PhysicalTotalWALExcludedBytes: 3, LogicalPrimaryPayloadBytes: 1, LogicalTextV2Overlap: "logical_text_v2_components_overlap_physical_storage_non_additive"}, TextV2: textV2{DocIDBytes: 1, DocMapBytes: 1, PostingBytes: 1, NormBytes: 1, TermBytes: 1, StatusBytes: 1}, CheckpointOK: true, CloseOK: true, ReopenOK: true, Probe: scoreOnlyProbe{Results: 1}}
}
func summaryFor(mode string, scale, n int) modeScaleSummary {
	return modeScaleSummary{Mode: mode, Scale: scale, MedianWallSeconds: 1, P95WallSeconds: 1, MedianIndexedRowsPerSec: 1, P95IndexedRowsPerSec: 1}
}
