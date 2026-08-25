package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	if err := verifyGitProvenance(m, func(args ...string) (string, error) {
		value, ok := validGitOutputs(m)[gitKey(args...)]
		if !ok {
			return "", errors.New("unexpected Git invocation")
		}
		return value, nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestDecodeStrictJSONRejectsUnknownAndTrailingValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "unknown field", raw: `{"vectors":true}`, want: "unknown field"},
		{name: "second value", raw: `{} {}`, want: "multiple JSON values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m manifest
			if err := decodeStrictJSON([]byte(tc.raw), &m); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decodeStrictJSON error=%v want substring %q", err, tc.want)
			}
		})
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

func TestValidateRejectsSourceChunkAcceptanceThresholds(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*row, sourceChunkAcceptance)
		want string
	}{
		{"physical ceiling", func(r *row, a sourceChunkAcceptance) {
			setWALExcludedPhysicalBytes(r, a.MaximumPhysicalTotalWALExcludedBytes+1)
		}, "physical storage ceiling"},
		{"throughput", func(r *row, a sourceChunkAcceptance) {
			r.WallSeconds = a.BaselineWallSeconds/a.MinimumSourceDocsPerSecondMultiple + 1
			r.SourceDocsPerSec = float64(r.SourceDocuments) / r.WallSeconds
			r.ChunksPerSec = float64(r.GeneratedChunks) / r.WallSeconds
			r.IndexedRowsPerSec = float64(r.IndexedLiveRows) / r.WallSeconds
		}, "throughput is below"},
		{"peak RSS", func(r *row, a sourceChunkAcceptance) {
			r.PeakRSSBytes.Value = float64(a.BaselinePeakRSSBytes + 1)
		}, "peak RSS regresses"},
		{"allocations", func(r *row, a sourceChunkAcceptance) {
			r.CumulativeAllocs.Value = float64(a.BaselineCumulativeAllocations + 1)
		}, "allocations regress"},
		{"storage regression", func(r *row, a sourceChunkAcceptance) {
			setWALExcludedPhysicalBytes(r, a.BaselinePhysicalTotalWALExcludedBytes+1)
		}, "storage regresses"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			r := validReport(t, m)
			candidate := sourceChunk10KRow(t, &r)
			tc.edit(candidate, m.Acceptance.SourceChunk10K)
			if err := validate(m, r, manifestHash(t, m)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestVerifyGitProvenanceRejectsUnavailableAndMismatchedObjects(t *testing.T) {
	m := validManifest()
	commitType := gitKey("cat-file", "-t", m.Commit)
	tree := gitKey("rev-parse", "--verify", m.Commit+"^{tree}")
	measuredPath := gitKey("rev-parse", "--verify", m.TreeOID+":"+m.ImplementationPath)
	blobType := gitKey("cat-file", "-t", m.ImplementationBlobOID)
	candidatePath := gitKey("rev-parse", "--verify", "HEAD:"+m.ImplementationPath)
	for _, tc := range []struct {
		name, key, value, fail, want string
	}{
		{name: "Git unavailable", key: commitType, fail: "git unavailable", want: "resolve measured commit"},
		{name: "commit type", key: commitType, value: "blob", want: "want commit"},
		{name: "commit tree", key: tree, value: strings.Repeat("d", 40), want: "measured commit tree"},
		{name: "measured path unavailable", key: measuredPath, fail: "missing path", want: "resolve measured implementation path"},
		{name: "measured path blob", key: measuredPath, value: strings.Repeat("d", 40), want: "measured implementation blob"},
		{name: "blob type", key: blobType, value: "tree", want: "want blob"},
		{name: "candidate path unavailable", key: candidatePath, fail: "missing HEAD path", want: "resolve candidate HEAD implementation path"},
		{name: "candidate blob", key: candidatePath, value: strings.Repeat("d", 40), want: "candidate HEAD implementation blob"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputs := validGitOutputs(m)
			if tc.value != "" {
				outputs[tc.key] = tc.value
			}
			resolve := func(args ...string) (string, error) {
				key := gitKey(args...)
				if key == tc.key && tc.fail != "" {
					return "", errors.New(tc.fail)
				}
				value, ok := outputs[key]
				if !ok {
					return "", errors.New("unexpected Git invocation")
				}
				return value, nil
			}
			if err := verifyGitProvenance(m, resolve); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %q", err, tc.want)
			}
		})
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
		{"unverifiable measured revision", func(m *manifest, _ *report) { m.CommitURL = "https://example.invalid" }, "pinned commit"},
		{"weakened acceptance", func(m *manifest, _ *report) { m.Acceptance.SourceChunk10K.MinimumSourceDocsPerSecondMultiple = 1 }, "pinned frozen"},
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
	commit := qualificationMeasuredCommit
	return manifest{SchemaVersion: contractVersion, FixtureSHA256: fixtureSHA, Analyzer: qualificationAnalyzer, FieldWeights: qualificationFieldWeights, IDsSHA256: idsSHA, Command: "go run", Commit: commit, CommitURL: "https://github.com/snissn/gomap/commit/" + commit, TreeOID: qualificationMeasuredTreeOID, ImplementationPath: qualificationImplementationPath, ImplementationBlobOID: qualificationImplementationBlob, Host: "test", CacheState: "cold", Durability: "wal_on", TimedBoundary: "insert through checkpoint", Observed: observedIdentity{VCSClean: true, Commit: commit, Durability: "wal_on", VectorIndexes: 0}, Acceptance: expectedQualificationAcceptance()}
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

func sourceChunk10KRow(t *testing.T, r *report) *row {
	t.Helper()
	for i := range r.Rows {
		if r.Rows[i].Mode == "source_chunk" && r.Rows[i].Scale == 10_000 {
			return &r.Rows[i]
		}
	}
	t.Fatal("source_chunk/10000 row missing")
	return nil
}

func setWALExcludedPhysicalBytes(r *row, value int64) {
	r.Storage.PhysicalIndexPageBytes = value
	r.Storage.PhysicalValueLogBytes = 0
	r.Storage.PhysicalWALBytes = 0
	r.Storage.PhysicalOtherBytes = 0
	r.Storage.PhysicalTotalBytes = value
	r.Storage.PhysicalTotalWALExcludedBytes = value
}

func gitKey(args ...string) string {
	return strings.Join(args, "\x00")
}

func validGitOutputs(m manifest) map[string]string {
	return map[string]string{
		gitKey("cat-file", "-t", m.Commit):                                  "commit",
		gitKey("rev-parse", "--verify", m.Commit+"^{tree}"):                 m.TreeOID,
		gitKey("rev-parse", "--verify", m.TreeOID+":"+m.ImplementationPath): m.ImplementationBlobOID,
		gitKey("cat-file", "-t", m.ImplementationBlobOID):                   "blob",
		gitKey("rev-parse", "--verify", "HEAD:"+m.ImplementationPath):       m.ImplementationBlobOID,
	}
}
