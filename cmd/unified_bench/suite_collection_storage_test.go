package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollectionStorageEffectiveModesAndWorkloads(t *testing.T) {
	modes, err := collectionStorageEffectiveModes("document,typed-column,vector")
	if err != nil {
		t.Fatalf("modes: %v", err)
	}
	wantModes := []string{collectionStorageModeDocumentOnly, collectionStorageModeTypedColumnPart, collectionStorageModeVectorTypedColumn}
	if strings.Join(modes, ",") != strings.Join(wantModes, ",") {
		t.Fatalf("modes=%v want %v", modes, wantModes)
	}
	if _, err := collectionStorageEffectiveModes("document_only,document_only"); err == nil {
		t.Fatalf("duplicate modes accepted")
	}
	if _, err := collectionStorageEffectiveModes("bogus"); err == nil {
		t.Fatalf("unknown mode accepted")
	}

	workloads, err := collectionStorageEffectiveWorkloads("insert,scan,agg,vector")
	if err != nil {
		t.Fatalf("workloads: %v", err)
	}
	wantWorkloads := []string{collectionStorageWorkloadInsertBatch, collectionStorageWorkloadPredicateScan, collectionStorageWorkloadAggregate, collectionStorageWorkloadVectorSearchSmoke}
	if strings.Join(workloads, ",") != strings.Join(wantWorkloads, ",") {
		t.Fatalf("workloads=%v want %v", workloads, wantWorkloads)
	}
	if _, err := collectionStorageEffectiveWorkloads("aggregate,aggregate"); err == nil {
		t.Fatalf("duplicate workloads accepted")
	}
	if _, err := collectionStorageEffectiveWorkloads("bogus"); err == nil {
		t.Fatalf("unknown workload accepted")
	}
	if err := validateCollectionStorageSupportedSelection([]string{collectionStorageModeDocumentOnly}, []string{collectionStorageWorkloadVectorSearchSmoke}); err == nil {
		t.Fatalf("unsupported explicit vector workload accepted without vector_typed_column mode")
	}
	if err := validateCollectionStorageSupportedSelection([]string{collectionStorageModeDocumentOnly, collectionStorageModeVectorTypedColumn}, []string{collectionStorageWorkloadVectorSearchSmoke}); err != nil {
		t.Fatalf("vector workload with vector mode rejected: %v", err)
	}
}

func TestBuildCollectionStorageFixtureHonorsFieldCount(t *testing.T) {
	rows, bytesTotal := buildCollectionStorageFixture(1, 4, 2, 3, 6, 1)
	if len(rows) != 1 || bytesTotal <= 0 {
		t.Fatalf("fixture rows=%d bytes=%d", len(rows), bytesTotal)
	}
	var decoded map[string]any
	if err := json.Unmarshal(rows[0].Document, &decoded); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	for _, key := range []string{"extra_0", "extra_1", "extra_2"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("fixture missing %s in %s", key, rows[0].Document)
		}
	}
	if _, ok := decoded["extra_3"]; ok {
		t.Fatalf("fixture emitted too many extra fields: %s", rows[0].Document)
	}
}

func TestCollectionStorageVectorSearchOptionsUseProjectionPreset(t *testing.T) {
	opts, err := collectionStorageVectorSearchOptions([]float32{1, 0, 0}, 2, 8, true, false)
	if err != nil {
		t.Fatalf("projected options: %v", err)
	}
	if !opts.IncludeDocuments || len(opts.DocumentFetchOptions.ExcludePaths) != 1 || opts.DocumentFetchOptions.ExcludePaths[0] != "embedding" {
		t.Fatalf("projected opts=%+v want include docs excluding embedding", opts)
	}
	if got := collectionStorageVectorFinalFetchShape(true, false); got != collectionStorageVectorFinalFetchProjectionNoEmbedding {
		t.Fatalf("final fetch shape=%q", got)
	}

	full, err := collectionStorageVectorSearchOptions([]float32{1, 0, 0}, 2, 8, true, true)
	if err != nil {
		t.Fatalf("full options: %v", err)
	}
	if !full.IncludeDocuments || len(full.DocumentFetchOptions.ExcludePaths) != 0 {
		t.Fatalf("full opts=%+v want full docs without projection", full)
	}
}

func TestCollectionStorageSuiteSmoke(t *testing.T) {
	withExplicitFlag(t, "keys")
	out, err := runCollectionStorageSuite(BenchConfig{
		Keys:       8,
		BatchSize:  4,
		Profile:    "durable",
		DBsArg:     "treedb",
		SeedUsed:   1,
		CPUProfile: filepath.Join(t.TempDir(), "cpu"),
	}, collectionStorageSuiteOptions{
		ModesArg:          "document_only,typed_column_part,vector_typed_column",
		WorkloadsArg:      "aggregate,vector_search_smoke",
		QueryCount:        1,
		PointGetCount:     2,
		VectorDims:        3,
		VectorTopK:        2,
		IncludeFinalFetch: true,
		CheckpointReopen:  true,
	})
	if err != nil {
		t.Fatalf("runCollectionStorageSuite: %v", err)
	}
	for _, needle := range []string{"collection_storage", "typed_column_part", "vector_typed_column", "vector_search_smoke", "projection_without_embedding", "unsupported workloads", "Comparison Semantics"} {
		if !strings.Contains(out, needle) {
			t.Fatalf("output missing %q:\n%s", needle, out)
		}
	}
}

func TestCollectionStorageSuiteVectorFullDocumentsShape(t *testing.T) {
	withExplicitFlag(t, "keys")
	out, err := runCollectionStorageSuite(BenchConfig{
		Keys:       8,
		BatchSize:  4,
		Profile:    "durable",
		DBsArg:     "treedb",
		SeedUsed:   1,
		CPUProfile: filepath.Join(t.TempDir(), "cpu"),
	}, collectionStorageSuiteOptions{
		ModesArg:            "vector_typed_column",
		WorkloadsArg:        "vector_search_smoke",
		QueryCount:          1,
		PointGetCount:       2,
		VectorDims:          3,
		VectorTopK:          2,
		IncludeFinalFetch:   true,
		VectorFullDocuments: true,
		CheckpointReopen:    true,
	})
	if err != nil {
		t.Fatalf("runCollectionStorageSuite full docs: %v", err)
	}
	for _, needle := range []string{"vector_final_fetch_shape: `full_document_embedding_echo`", "explicit full-document/embedding-echo comparison path"} {
		if !strings.Contains(out, needle) {
			t.Fatalf("output missing %q:\n%s", needle, out)
		}
	}
}

func TestCollectionStorageSuiteProfileDirArtifacts(t *testing.T) {
	withExplicitFlag(t, "keys")
	dir := t.TempDir()
	cfg := BenchConfig{
		Keys:                 8,
		BatchSize:            4,
		Profile:              "durable",
		DBsArg:               "treedb",
		SeedUsed:             1,
		CPUProfile:           filepath.Join(dir, "cpu"),
		AllocsProfile:        filepath.Join(dir, "allocs"),
		CheckpointCPUProfile: filepath.Join(dir, "checkpoint_cpu"),
		BlockProfile:         filepath.Join(dir, "block.pprof"),
		MutexProfile:         filepath.Join(dir, "mutex.pprof"),
		TraceProfile:         filepath.Join(dir, "trace.out"),
		AllocsProfileRate:    1,
	}
	_, err := runCollectionStorageSuite(cfg, collectionStorageSuiteOptions{
		ProfileDir:        dir,
		ExecutionPath:     "native-fastpath",
		ModesArg:          "document_only,typed_column_part",
		WorkloadsArg:      "aggregate",
		QueryCount:        1,
		PointGetCount:     2,
		VectorDims:        3,
		VectorTopK:        2,
		IncludeFinalFetch: true,
		CheckpointReopen:  true,
		RunBenchprof:      false,
	})
	if err != nil {
		t.Fatalf("runCollectionStorageSuite profile-dir: %v", err)
	}
	for _, name := range []string{
		"collection_storage_results.json",
		"collection_storage_results.md",
		"collection_storage_results.html",
		"benchprof_results.json",
		"benchprof_results.md",
		"cpu_collection_storage_treedb_collection_storage.pprof",
		"allocs_collection_storage_treedb_collection_storage.pprof",
		"checkpoint_cpu_checkpoint_collection_storage_treedb_collection_storage.pprof",
		"block.pprof",
		"mutex.pprof",
		"trace.out",
	} {
		path := filepath.Join(dir, name)
		if st, err := os.Stat(path); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		} else if st.Size() == 0 {
			t.Fatalf("artifact %s is empty", name)
		}
	}
	var parsed struct {
		Runs []struct {
			CollectionWorkloads []benchprofCollectionWorkload `json:"collection_workloads"`
		} `json:"runs"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatalf("read benchprof_results.json: %v", err)
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse benchprof_results.json: %v", err)
	}
	if len(parsed.Runs) != 1 || len(parsed.Runs[0].CollectionWorkloads) != 2 {
		t.Fatalf("collection_workloads len=%d runs=%d", len(parsed.Runs[0].CollectionWorkloads), len(parsed.Runs))
	}
	for _, w := range parsed.Runs[0].CollectionWorkloads {
		if w.Suite != collectionStorageSuiteName || w.Workload != collectionStorageWorkloadAggregate || !w.CorrectnessValidated || !w.SemanticEquivalent {
			t.Fatalf("unexpected workload metadata: %+v", w)
		}
	}
}

func withExplicitFlag(t *testing.T, name string) {
	t.Helper()
	prev := explicitFlags
	next := make(map[string]bool, len(prev)+1)
	for k, v := range prev {
		next[k] = v
	}
	next[name] = true
	explicitFlags = next
	t.Cleanup(func() { explicitFlags = prev })
}
