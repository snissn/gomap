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
func TestValidateAllowsReusedNumericPID(t *testing.T) {
	m := validManifest()
	r := validReport(t, m)
	r.Rows[1].PeakRSSPID = r.Rows[0].PeakRSSPID
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
		{"stale fixture manifest", func(m *manifest, _ *report) { m.FixtureSHA256 = "stale" }, "qualification generator"},
		{"row fixture drift", func(_ *manifest, r *report) { r.Rows[0].IDsSHA256 = "stale" }, "fixture identity"},
		{"unverifiable measured revision", func(m *manifest, _ *report) { m.CommitURL = "https://example.invalid" }, "verifiable commit URL"},
		{"wrong analyzer", func(m *manifest, _ *report) { m.Analyzer = "keyword" }, "match the producer"},
		{"wrong field weights", func(m *manifest, _ *report) { m.FieldWeights = "title=1,body=1" }, "match the producer"},
		{"missing matrix", func(_ *manifest, r *report) { r.Rows = r.Rows[1:] }, "missing required mode/scale"},
		{"duplicate retained repetition", func(_ *manifest, r *report) { r.Rows = append(r.Rows, r.Rows[12]) }, "duplicate repetition"},
		{"copied summary", func(_ *manifest, r *report) { r.Summaries[0].MedianWallSeconds = 2 }, "summary does not recompute"},
		{"zero stage placeholder", func(_ *manifest, r *report) { r.Rows[0].Stages["value_log"] = metric{State: "observed"} }, "zero placeholder"},
		{"zero resource placeholder", func(_ *manifest, r *report) { r.Rows[0].PeakRSSBytes.Value = 0 }, "resource metric must be positive"},
		{"missing fresh RSS process scope", func(_ *manifest, r *report) { r.Rows[0].PeakRSSScope = "" }, "fresh process"},
		{"partial indexed live rows", func(_ *manifest, r *report) { r.Rows[0].IndexedLiveRows-- }, "every source document"},
		{"stale throughput", func(_ *manifest, r *report) { r.Rows[0].IndexedRowsPerSec++ }, "throughput does not recompute"},
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
		{"source batch accounting", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "source_chunk" {
					r.Rows[i].ChunkBatchCount++
					break
				}
			}
		}, "batch accounting"},
		{"source generation accounting", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "source_chunk" {
					r.Rows[i].Generations++
					break
				}
			}
		}, "batch accounting"},
		{"maintenance count semantics", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "maintenance" {
					r.Rows[i].IndexedLiveRows = 0
					break
				}
			}
		}, "document accounting"},
		{"maintenance tombstone debt", func(_ *manifest, r *report) {
			for i := range r.Rows {
				if r.Rows[i].Mode == "maintenance" {
					r.Rows[i].TombstoneDebt = 0
					break
				}
			}
		}, "tombstone debt"},
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
func TestProduceSmokeUsesFreshChildPerModeAndPreservesRepetition(t *testing.T) {
	var calls []struct {
		dir, mode         string
		scale, repetition int
	}
	err := produceSmokeWith(t.TempDir(), 2, 3, func(dir, mode string, scale, repetition int) error {
		calls = append(calls, struct {
			dir, mode         string
			scale, repetition int
		}{dir, mode, scale, repetition})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != len(requiredModes) {
		t.Fatalf("fresh child calls=%d want %d", len(calls), len(requiredModes))
	}
	for i, call := range calls {
		if call.mode != requiredModes[i] || call.scale != 2 || call.repetition != 3 {
			t.Fatalf("call %d = %+v", i, call)
		}
	}
}

func TestProduceModeRecordsRepetitionAndMaintenanceDebt(t *testing.T) {
	r, err := produceMode(t.TempDir(), "maintenance", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if r.Repetition != 3 || r.TombstoneDebt != 1 {
		t.Fatalf("repetition=%d tombstone_debt=%d", r.Repetition, r.TombstoneDebt)
	}
	if r.PeakRSSScope != "fresh_process_per_mode" || r.PeakRSSPID < 1 {
		t.Fatalf("RSS provenance = %q/%d", r.PeakRSSScope, r.PeakRSSPID)
	}
}

func TestObserveStorageClassifiesSyntheticTree(t *testing.T) {
	dir := t.TempDir()
	for path, contents := range map[string]string{
		"index.db": "i", "value_vlog/0001": "vv", "maindb/leaf_vlog/0001": "lll", "wal/0001": "www", "metadata": "oooo",
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
	if s.PhysicalIndexPageBytes != 1 || s.PhysicalValueLogBytes != 5 || s.PhysicalWALBytes != 3 || s.PhysicalOtherBytes != 4 || s.PhysicalTotalBytes != 13 || s.PhysicalTotalWALExcludedBytes != 10 {
		t.Fatalf("unexpected storage: %+v", s)
	}
	if len(s.OtherPaths) != 1 || s.OtherPaths[0] != "metadata" {
		t.Fatalf("other paths: %v", s.OtherPaths)
	}
}

func validManifest() manifest {
	fixtureSHA, idsSHA := qualificationManifestIdentity()
	commit := strings.Repeat("a", 40)
	return manifest{SchemaVersion: contractVersion, FixtureSHA256: fixtureSHA, Analyzer: qualificationAnalyzer, FieldWeights: qualificationFieldWeights, IDsSHA256: idsSHA, Command: "go run", Commit: commit, CommitURL: "https://github.com/snissn/gomap/commit/" + commit, TreeOID: strings.Repeat("b", 40), ImplementationPath: qualificationImplementationPath, ImplementationBlobOID: strings.Repeat("c", 40), Host: "test", CacheState: "cold", Durability: "wal_on", TimedBoundary: "insert through checkpoint", Observed: observedIdentity{VCSClean: true, Commit: commit, Durability: "wal_on", VectorIndexes: 0}}
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
	modeOffset := map[string]int{"indexed_insert": 1, "post_load_backfill": 2, "source_chunk": 3, "maintenance": 4}[mode]
	parentsIndexed := false
	if mode == "source_chunk" {
		parentsIndexed = true
		chunks = scale * 2
		live = source + chunks
	}
	if mode == "maintenance" {
		live = scale / 2
	}
	r := row{Mode: mode, Scale: scale, Repetition: rep, SourceDocuments: source, GeneratedChunks: chunks, IndexedLiveRows: live, ParentsTextIndexed: parentsIndexed, IndexedParentRows: func() int {
		if mode == "source_chunk" {
			return source
		}
		return 0
	}(), ChunkBatchSize: func() int {
		if mode == "source_chunk" {
			return min(sourceChunkBatchLimit, scale)
		}
		return 0
	}(), ChunkBatchCount: func() int {
		if mode == "source_chunk" {
			return (scale + sourceChunkBatchLimit - 1) / sourceChunkBatchLimit
		}
		return 0
	}(), Postings: 1, Terms: 1, Blocks: 1, Generations: 1, TombstoneDebt: func() uint64 {
		if mode == "maintenance" {
			return uint64(source - live)
		}
		return 0
	}(), SourceDocsPerSec: float64(source), ChunksPerSec: float64(chunks), IndexedRowsPerSec: float64(live), WallSeconds: 1, CPUSeconds: metric{State: "unavailable", Reason: "platform"}, BytesPerOp: metric{State: "unavailable", Reason: "not a Go benchmark"}, AllocsPerOp: metric{State: "unavailable", Reason: "not a Go benchmark"}, CumulativeAllocs: metric{State: "observed", Value: 1}, PeakRSSBytes: metric{State: "observed", Value: 1}, PeakRSSScope: "fresh_process_per_mode", PeakRSSPID: scale*100 + rep*10 + modeOffset, Stages: map[string]metric{"analyzer": {State: "observed", Value: 1}, "posting_builder": {State: "observed", Value: 1}, "root_mutation": {State: "observed", Value: 1}, "value_log": {State: "unavailable", Reason: "not separately instrumented"}, "checkpoint": {State: "observed", Value: 1}, "reopen": {State: "observed", Value: 1}}, Storage: storage{PhysicalIndexPageBytes: 1, PhysicalValueLogBytes: 1, PhysicalWALBytes: 1, PhysicalOtherBytes: 1, PhysicalTotalBytes: 4, PhysicalTotalWALExcludedBytes: 3, LogicalPrimaryPayloadBytes: 1, LogicalTextV2Overlap: "logical_text_v2_components_overlap_physical_storage_non_additive"}, TextV2: textV2{DocIDBytes: 1, DocMapBytes: 1, PostingBytes: 1, NormBytes: 1, TermBytes: 1, StatusBytes: 1}, CheckpointOK: true, CloseOK: true, ReopenOK: true, Probe: scoreOnlyProbe{Results: 1}}
	if mode == "source_chunk" {
		r.Generations = uint64(r.ChunkBatchCount + 1)
	}
	r.FixtureSHA256, r.IDsSHA256 = qualificationIdentity(scale)
	return r
}
func summaryFor(mode string, scale, n int) modeScaleSummary {
	rate := validRow(mode, scale, 1).IndexedRowsPerSec
	return modeScaleSummary{Mode: mode, Scale: scale, MedianWallSeconds: 1, P95WallSeconds: 1, MedianIndexedRowsPerSec: rate, P95IndexedRowsPerSec: rate}
}
