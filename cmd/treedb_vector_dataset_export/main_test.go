package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestExportDatasetSmoke(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dataset")
	res, err := exportDataset(config{
		out:        dir,
		docs:       8,
		dimensions: 4,
		queries:    3,
		topK:       2,
	})
	if err != nil {
		t.Fatalf("exportDataset: %v", err)
	}
	if res.Manifest.Docs != 8 || res.Manifest.Dimensions != 4 || res.Manifest.Queries != 3 || res.Manifest.ExactTruthQueries != 3 {
		t.Fatalf("unexpected manifest: %+v", res.Manifest)
	}
	for _, name := range []string{"manifest.json", "documents.f32", "queries.f32", "documents.jsonl", "queries.jsonl", "exact_truth.jsonl"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
		if name != "manifest.json" && res.Manifest.Files[name].Bytes == 0 {
			t.Fatalf("manifest missing %s file stats: %+v", name, res.Manifest.Files)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var parsed manifest
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if parsed.Generator != "treedb_vector_synthetic_v1" || parsed.Metric != "cosine" || !parsed.Normalized {
		t.Fatalf("unexpected parsed manifest: %+v", parsed)
	}
	if len(parsed.Files) != len(res.Manifest.Files) {
		t.Fatalf("on-disk manifest file count differs from result: parsed=%d result=%d", len(parsed.Files), len(res.Manifest.Files))
	}
	for name, parsedFile := range parsed.Files {
		if parsedFile != res.Manifest.Files[name] {
			t.Fatalf("on-disk manifest file stats differ for %s: parsed=%+v result=%+v", name, parsedFile, res.Manifest.Files[name])
		}
	}
	if _, ok := parsed.Files["manifest.json"]; ok {
		t.Fatalf("manifest should not include self-referential manifest.json stats: %+v", parsed.Files["manifest.json"])
	}
	if parsed.ExactTruthFile != "exact_truth.jsonl" || parsed.ExactTruthKind != "exhaustive_cosine_distance_ascending_then_id_top_k_v1" || parsed.ExactTruthQueries != 3 {
		t.Fatalf("missing declared exact truth: %+v", parsed)
	}
}

func TestExportExactTruthUsesDeclaredLeadingQueryCount(t *testing.T) {
	const (
		queries      = 5
		truthQueries = 2
	)
	dir := filepath.Join(t.TempDir(), "truth-prefix")
	res, err := exportDataset(config{out: dir, docs: 8, dimensions: 4, queries: queries, truthQueries: truthQueries, topK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if res.Manifest.Queries != queries || res.Manifest.ExactTruthQueries != truthQueries {
		t.Fatalf("manifest query coverage=%+v", res.Manifest)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != truthQueries {
		t.Fatalf("truth rows=%d want declared prefix=%d", len(lines), truthQueries)
	}
	for i, line := range lines {
		var row exactTruthJSONL
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		if row.QueryID != fmt.Sprintf("query-%06d", i) {
			t.Fatalf("truth row %d query_id=%q", i, row.QueryID)
		}
	}
	info, err := os.Stat(filepath.Join(dir, "queries.f32"))
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(queries * 4 * 4); info.Size() != want {
		t.Fatalf("query vector bytes=%d want all-query corpus bytes=%d", info.Size(), want)
	}
}

func TestExportExplicitZeroTruthWritesDeclaredEmptyCoverage(t *testing.T) {
	const queries = 5
	dir := filepath.Join(t.TempDir(), "no-truth")
	res, err := exportDataset(config{
		out:           dir,
		docs:          8,
		dimensions:    4,
		queries:       queries,
		truthQueries:  0,
		truthExplicit: true,
		topK:          3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Manifest.Queries != queries || res.Manifest.ExactTruthQueries != 0 {
		t.Fatalf("manifest query coverage=%+v", res.Manifest)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 0 {
		t.Fatalf("disabled exact truth emitted %d bytes", len(raw))
	}
	info, err := os.Stat(filepath.Join(dir, "queries.f32"))
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(queries * 4 * 4); info.Size() != want {
		t.Fatalf("query vector bytes=%d want all-query corpus bytes=%d", info.Size(), want)
	}
}

func TestExportExactTruthIncludesTopK(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "truth")
	if _, err := exportDataset(config{out: dir, docs: 8, dimensions: 4, queries: 2, topK: 3}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var row exactTruthJSONL
	if err := json.Unmarshal([]byte(strings.Split(strings.TrimSpace(string(raw)), "\n")[0]), &row); err != nil {
		t.Fatal(err)
	}
	if len(row.Neighbors) != 3 || row.Kind != "exhaustive_cosine_distance_ascending_then_id_top_k_v1" {
		t.Fatalf("truth=%+v", row)
	}
}

func TestExportExactTruthMatchesIndependentSmallCorpusRecomputation(t *testing.T) {
	const (
		docs    = 9
		queries = 3
		dims    = 5
		topK    = 4
	)
	dir := filepath.Join(t.TempDir(), "truth")
	if _, err := exportDataset(config{out: dir, docs: docs, dimensions: dims, queries: queries, topK: topK}); err != nil {
		t.Fatal(err)
	}
	documentVectors := readFloat32RowsForTest(t, filepath.Join(dir, "documents.f32"), docs, dims)
	queryVectors := readFloat32RowsForTest(t, filepath.Join(dir, "queries.f32"), queries, dims)
	raw, err := os.ReadFile(filepath.Join(dir, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != queries {
		t.Fatalf("truth rows=%d want %d", len(lines), queries)
	}
	for queryIndex := 0; queryIndex < queries; queryIndex++ {
		var got exactTruthJSONL
		if err := json.Unmarshal([]byte(lines[queryIndex]), &got); err != nil {
			t.Fatal(err)
		}
		want := make([]truthNeighbor, docs)
		for documentIndex, document := range documentVectors {
			var dot, documentNorm, queryNorm float64
			for d, value := range document {
				queryValue := queryVectors[queryIndex][d]
				dot += float64(value) * float64(queryValue)
				documentNorm += float64(value) * float64(value)
				queryNorm += float64(queryValue) * float64(queryValue)
			}
			want[documentIndex] = truthNeighbor{
				DocumentID: fmt.Sprintf("doc-%06d", documentIndex),
				Distance:   1 - dot/math.Sqrt(documentNorm*queryNorm),
			}
		}
		sort.Slice(want, func(i, j int) bool {
			if want[i].Distance == want[j].Distance {
				return want[i].DocumentID < want[j].DocumentID
			}
			return want[i].Distance < want[j].Distance
		})
		want = want[:topK]
		if len(got.Neighbors) != len(want) {
			t.Fatalf("query %d neighbors=%d want %d", queryIndex, len(got.Neighbors), len(want))
		}
		for rank := range want {
			if got.Neighbors[rank].DocumentID != want[rank].DocumentID || math.Float64bits(got.Neighbors[rank].Distance) != math.Float64bits(want[rank].Distance) {
				t.Fatalf("query %d rank %d got=%+v want=%+v", queryIndex, rank, got.Neighbors[rank], want[rank])
			}
		}
	}
}

func TestExactCosineTruthNormalizesNonUnitVectorsAndOrdersTies(t *testing.T) {
	query := []float32{3, 4}
	documents := [][]float32{
		{6, 8},
		{1, 1},
		{-4, 3},
		{1.5, 2},
		{0, 5},
	}
	got := exactTruthForVectors(query, len(documents), len(documents), func(i int) []float32 {
		return documents[i]
	})
	wantIDs := []string{"doc-000000", "doc-000003", "doc-000001", "doc-000004", "doc-000002"}
	for i, wantID := range wantIDs {
		if got[i].DocumentID != wantID {
			t.Fatalf("rank %d id=%q want %q; truth=%+v", i, got[i].DocumentID, wantID, got)
		}
	}
	if math.Abs(got[0].Distance) > 1e-15 || math.Abs(got[1].Distance) > 1e-15 {
		t.Fatalf("same-direction non-unit vectors must have distance ~0: %+v", got[:2])
	}
	wantDiagonal := 1 - 7/(5*math.Sqrt(2))
	if math.Abs(got[2].Distance-wantDiagonal) > 1e-15 {
		t.Fatalf("diagonal distance=%g want %g", got[2].Distance, wantDiagonal)
	}
	if math.Abs(got[3].Distance-0.2) > 1e-15 || math.Abs(got[4].Distance-1) > 1e-15 {
		t.Fatalf("nontrivial cosine ranking distances=%+v", got)
	}
}

func readFloat32RowsForTest(t *testing.T, path string, count, dims int) [][]float32 {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != count*dims*4 {
		t.Fatalf("%s bytes=%d want %d", path, len(raw), count*dims*4)
	}
	rows := make([][]float32, count)
	for i := range rows {
		rows[i] = make([]float32, dims)
		for d := range rows[i] {
			offset := (i*dims + d) * 4
			rows[i][d] = math.Float32frombits(binary.LittleEndian.Uint32(raw[offset:]))
		}
	}
	return rows
}

func TestTruthNeighborLessUsesExplicitDistanceThenIDTieOrder(t *testing.T) {
	a := truthNeighbor{DocumentID: "doc-000001", Distance: 0.25}
	b := truthNeighbor{DocumentID: "doc-000002", Distance: 0.25}
	if !truthNeighborLess(a, b) || truthNeighborLess(b, a) {
		t.Fatalf("tie helper does not order stable IDs: a=%+v b=%+v", a, b)
	}
	if !truthNeighborLess(truthNeighbor{DocumentID: "doc-z", Distance: 0.1}, a) {
		t.Fatal("tie helper does not prioritize smaller cosine distance")
	}
}

func TestExportDatasetIsByteStableAndTruthUsesAscendingDistance(t *testing.T) {
	a, b := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	cfg := config{docs: 8, dimensions: 4, queries: 2, topK: 3}
	cfg.out = a
	if _, err := exportDataset(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.out = b
	if _, err := exportDataset(cfg); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"manifest.json", "documents.f32", "queries.f32", "documents.jsonl", "queries.jsonl", "exact_truth.jsonl"} {
		x, _ := os.ReadFile(filepath.Join(a, name))
		y, _ := os.ReadFile(filepath.Join(b, name))
		if string(x) != string(y) {
			t.Fatalf("%s is not stable", name)
		}
	}
	raw, _ := os.ReadFile(filepath.Join(a, "exact_truth.jsonl"))
	var row exactTruthJSONL
	_ = json.Unmarshal([]byte(strings.Split(strings.TrimSpace(string(raw)), "\n")[0]), &row)
	for i := 1; i < len(row.Neighbors); i++ {
		if row.Neighbors[i-1].Distance > row.Neighbors[i].Distance {
			t.Fatal("truth distances are not ascending")
		}
	}
}

func TestExportMaterializesDocumentVectorsOnceForAllQueries(t *testing.T) {
	const (
		docs    = 11
		queries = 7
		dims    = 5
	)
	calls := 0
	_, err := exportDatasetWithDocumentGenerator(config{
		out:        filepath.Join(t.TempDir(), "dataset"),
		docs:       docs,
		dimensions: dims,
		queries:    queries,
		topK:       4,
	}, func(document, dimensions int) []float32 {
		calls++
		return embedding(document, dimensions)
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != docs {
		t.Fatalf("document generator calls=%d want exactly docs=%d, independent of queries=%d", calls, docs, queries)
	}
}

func TestMillionDocumentSingleQueryTruthPeakIsFeasible(t *testing.T) {
	peak, err := checkedTruthPeakBytes(maxDatasetVectors, 64, 10)
	if err != nil {
		t.Fatalf("1M-document single-query truth shape rejected: %v", err)
	}
	if peak >= maxDatasetBytes {
		t.Fatalf("1M-document single-query modeled peak=%d cap=%d", peak, maxDatasetBytes)
	}
	if _, err := parseConfig([]string{
		"-out", t.TempDir(),
		"-docs", strconv.Itoa(maxDatasetVectors),
		"-queries", "1",
		"-dims", "64",
		"-top-k", "10",
	}); err != nil {
		t.Fatalf("1M-document single-query config rejected: %v", err)
	}
}

func TestLargeComparisonShapeUsesBoundedTruthPrefixBeforeAllocation(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-out", filepath.Join(t.TempDir(), "large"),
		"-docs", "100000",
		"-queries", "50000",
		"-truth-queries", "64",
		"-dims", "64",
		"-top-k", "10",
	})
	if err != nil {
		t.Fatalf("bounded-truth comparison shape rejected: %v", err)
	}
	if cfg.truthQueries != 64 || cfg.queries != 50000 {
		t.Fatalf("normalized query coverage=%+v", cfg)
	}
	disabled, err := parseConfig([]string{
		"-out", filepath.Join(t.TempDir(), "disabled"),
		"-docs", "100000",
		"-queries", "50000",
		"-truth-queries", "0",
		"-dims", "64",
		"-top-k", "10",
	})
	if err != nil {
		t.Fatalf("disabled-truth comparison shape rejected: %v", err)
	}
	if disabled.truthQueries != 0 || !disabled.truthExplicit {
		t.Fatalf("disabled truth coverage=%+v", disabled)
	}
	if _, err := parseConfig([]string{
		"-out", filepath.Join(t.TempDir(), "unbounded"),
		"-docs", "100000",
		"-queries", "50000",
		"-dims", "64",
		"-top-k", "10",
	}); err == nil || !strings.Contains(err.Error(), "truth comparison cap") {
		t.Fatalf("unbounded all-query truth error=%v", err)
	}
}

func TestTruthQueryBoundsNormalizeBeforeComparisonCap(t *testing.T) {
	base := []string{"-out", t.TempDir(), "-docs", "20", "-queries", "4", "-dims", "4", "-top-k", "2"}
	cfg, err := parseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.truthQueries != cfg.queries {
		t.Fatalf("default truth_queries=%d want all queries=%d", cfg.truthQueries, cfg.queries)
	}
	explicitZero, err := parseConfig(append(append([]string{}, base...), "-truth-queries", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if explicitZero.truthQueries != 0 || !explicitZero.truthExplicit {
		t.Fatalf("explicit zero did not disable truth: %+v", explicitZero)
	}
	for _, value := range []string{"-1", "5"} {
		args := append(append([]string{}, base...), "-truth-queries", value)
		if _, err := parseConfig(args); err == nil || !strings.Contains(err.Error(), "-truth-queries") {
			t.Fatalf("accepted truth query count %s: %v", value, err)
		}
	}
}

func TestParseConfigRejectsCorpusCapsBeforeAllocation(t *testing.T) {
	if _, err := parseConfig([]string{"-out", t.TempDir(), "-docs", "1000001"}); err == nil {
		t.Fatal("accepted vector cap")
	}
	if _, err := checkedVectorBytes(maxDatasetVectors, maxDatasetDims); err == nil {
		t.Fatal("accepted byte cap")
	}
	if _, err := checkedCombinedVectorBytes(maxDatasetVectors, maxDatasetVectors, maxDatasetDims); err == nil {
		t.Fatal("accepted combined document/query byte cap")
	}
	if _, err := checkedTruthPeakBytes(maxDatasetVectors, maxDatasetDims, 1); err == nil {
		t.Fatal("accepted exact truth peak above cap")
	}
}

func TestParseConfigRejectsMissingOut(t *testing.T) {
	if _, err := parseConfig([]string{"-docs", "4"}); err == nil {
		t.Fatal("parseConfig accepted missing -out")
	}
}

func TestManifestCreatedAtUsesSourceDateEpoch(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "0")
	got, err := manifestCreatedAt()
	if err != nil {
		t.Fatalf("manifestCreatedAt: %v", err)
	}
	if got != "1970-01-01T00:00:00Z" {
		t.Fatalf("created_at=%q", got)
	}
}

func TestExportDatasetRejectsNonEmptyOut(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dataset")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("do not delete"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	_, err := exportDataset(config{
		out:        dir,
		docs:       8,
		dimensions: 4,
		queries:    3,
		topK:       2,
	})
	if err == nil {
		t.Fatal("exportDataset accepted non-empty output directory")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "keep")); statErr != nil {
		t.Fatalf("sentinel was removed: %v", statErr)
	}
}
