package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestM8WholeCollectionPopulationV1(t *testing.T) {
	truth := make([][]m8CanonicalResultV1, 4)
	cell := m8WholeCollectionCellV1{Concurrency: 1, ElapsedNanos: 1000, SetupError: "setup control", Attempts: make([]m8WholeCollectionAttemptV1, 4)}
	for i := range truth {
		truth[i] = []m8CanonicalResultV1{{ID: "doc-000000", Score: 1}}
		cell.Attempts[i] = m8WholeCollectionAttemptV1{
			Class: "returned", SearchNanos: 100, ResultCount: 1,
			Results:  []m8WholeCollectionHitV1{{ID: "doc-000000", Ordinal: 0, ScoreBits: math.Float64bits(1)}},
			Strategy: collections.VectorIndexStrategyColumnGraph, Path: collections.VectorIndexSearchPathColumnGraphNativeReader,
		}
	}
	cell.Attempts[1].Class, cell.Attempts[1].Error = "error", "search failed"
	cell.Attempts[2].Results[0].ScoreBits = math.Float64bits(math.NaN())
	cell.Attempts[3] = m8WholeCollectionAttemptV1{Class: "not_dispatched"}
	if err := m8SummarizeWholeCollectionV1(&cell, truth, 1); err != nil {
		t.Fatal(err)
	}
	want := m8MeasurementSummaryV1{Declared: 4, Dispatched: 3, Succeeded: 1, Errors: 1, Invalid: 1, NotDispatched: 1, TruthHits: 1, TruthSlots: 4, SuccessRecall: 1, ServiceRecall: .25, CompletionRate: 1.0 / 3, AttemptRate: 3e6, Goodput: 1e6}
	if cell.Summary != want || cell.P95Nanos != 100 {
		t.Fatalf("summary=%+v p95=%d", cell.Summary, cell.P95Nanos)
	}
	// JSON retention and repeated reduction cannot erase failures or inflate the
	// denominator to the successful subset.
	raw, err := json.Marshal(cell)
	if err != nil {
		t.Fatal(err)
	}
	var retained m8WholeCollectionCellV1
	if err := json.Unmarshal(raw, &retained); err != nil {
		t.Fatal(err)
	}
	if err := m8SummarizeWholeCollectionV1(&retained, truth, 1); err != nil || !reflect.DeepEqual(retained, cell) {
		t.Fatalf("roundtrip changed population: %v", err)
	}
	for name, mutate := range map[string]func(*m8WholeCollectionCellV1){
		"missing_slot":          func(c *m8WholeCollectionCellV1) { c.Attempts = c.Attempts[:3] },
		"duration":              func(c *m8WholeCollectionCellV1) { c.Attempts[0].SearchNanos = c.ElapsedNanos + 1 },
		"missing_cause":         func(c *m8WholeCollectionCellV1) { c.Attempts[1].Error = "" },
		"unknown_class":         func(c *m8WholeCollectionCellV1) { c.Attempts[0].Class = "retry" },
		"undispatched_work":     func(c *m8WholeCollectionCellV1) { c.Attempts[3].ResultCount = 1 },
		"missing_setup_failure": func(c *m8WholeCollectionCellV1) { c.SetupError = "" },
	} {
		t.Run(name, func(t *testing.T) {
			var bad m8WholeCollectionCellV1
			if err := json.Unmarshal(raw, &bad); err != nil {
				t.Fatal(err)
			}
			mutate(&bad)
			if err := m8SummarizeWholeCollectionV1(&bad, truth, 1); err == nil {
				t.Fatal("accepted malformed population")
			}
		})
	}
	for name, mutate := range map[string]func(*m8WholeCollectionAttemptV1){
		"count":     func(a *m8WholeCollectionAttemptV1) { a.ResultCount++ },
		"id":        func(a *m8WholeCollectionAttemptV1) { a.Results[0].ID = "doc-000001" },
		"ordinal":   func(a *m8WholeCollectionAttemptV1) { a.Results[0].Ordinal++ },
		"fallback":  func(a *m8WholeCollectionAttemptV1) { a.Stats.GraphRowFallbacks++ },
		"documents": func(a *m8WholeCollectionAttemptV1) { a.Stats.DocumentsFetched++ },
		"path":      func(a *m8WholeCollectionAttemptV1) { a.Path = "" },
	} {
		t.Run(name, func(t *testing.T) {
			var bad m8WholeCollectionCellV1
			if err := json.Unmarshal(raw, &bad); err != nil {
				t.Fatal(err)
			}
			mutate(&bad.Attempts[0])
			if err := m8SummarizeWholeCollectionV1(&bad, truth, 1); err != nil {
				t.Fatal(err)
			}
			if bad.Summary.Succeeded != 0 || bad.Summary.Invalid != 2 || bad.Summary.ServiceRecall != 0 || bad.P95Nanos != 0 {
				t.Fatal("invalid response kept successful credit")
			}
		})
	}
}

func TestM8WholeCollectionAdmissionV1(t *testing.T) {
	out := filepath.Join(t.TempDir(), "reference.jsonl")
	for _, args := range [][]string{
		{}, {"-out", out, "-ef-search", "9", "--", "-root", "/missing"},
		{"-out", out, "-ef-search", "96,96", "--", "-root", "/missing"},
		{"-out", out, "-ef-search", "96", "-concurrency", "2", "--", "-root", "/missing"},
		{"-out", out, "-ef-search", "96", "-repetitions", "6", "--", "-root", "/missing"},
		{"-out", out, "-ef-search", "96", "--", "-root", "/missing"},
	} {
		if err := run(append([]string{"whole-collection-reference"}, args...), io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatal("created output before admission/replay")
		}
	}
	if err := os.WriteFile(out, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runM8WholeCollectionReferenceV1([]string{"-out", out, "-ef-search", "96", "--", "-root", "/missing"}, io.Discard); err == nil {
		t.Fatal("overwrote output")
	}
	raw, err := os.ReadFile(out)
	if err != nil || string(raw) != "preserve" {
		t.Fatal("changed previous output")
	}
	accepted := m8ProductionReportV1{ExecutionID: "sentinel"}
	if err := replayM8ReportV1(nil, io.Discard, &accepted); err == nil || accepted.ExecutionID != "sentinel" {
		t.Fatal("failed replay published decoded data")
	}
}

func TestM8WholeCollectionGraphOrdinalIsNotFixtureOrdinalV1(t *testing.T) {
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000000"}, {ID: "doc-000001"}}}
	cell := m8WholeCollectionCellV1{Concurrency: 1, ElapsedNanos: 1000, Attempts: []m8WholeCollectionAttemptV1{{
		Class: "returned", SearchNanos: 100, ResultCount: 2,
		Strategy: collections.VectorIndexStrategyColumnGraph, Path: collections.VectorIndexSearchPathColumnGraphNativeReader,
		Results: []m8WholeCollectionHitV1{{ID: "doc-000000", Ordinal: 1}, {ID: "doc-000001", Ordinal: 0}},
	}}}
	if err := m8SummarizeWholeCollectionV1(&cell, truth, 2); err != nil || cell.Summary.ServiceRecall != 1 {
		t.Fatalf("graph reordering rejected: %+v %v", cell.Summary, err)
	}
	cell.Attempts[0].Results[1].Ordinal = 1
	if err := m8SummarizeWholeCollectionV1(&cell, truth, 2); err != nil || cell.Summary.Invalid != 1 {
		t.Fatalf("duplicate graph ordinal accepted: %+v %v", cell.Summary, err)
	}
}

func testM8WholeCollectionReadOnlyV1(t testing.TB) (*m8ProductionMultiGroupAssetsV1, [][]float32, [][]m8CanonicalResultV1) {
	t.Helper()
	requireM8PersistentAssetSupportV1(t)
	fixture := fixtureManifest{Vectors: 128, Queries: 64, Dimensions: 16, Seed: 42}
	vectors := deterministicVectors(fixture)
	queries64 := deterministicQueries(vectors, fixture)
	truth, err := m8ExactTruthFixtureV1(vectors, queries64, 10)
	if err != nil {
		t.Fatal(err)
	}
	queries := make([][]float32, len(queries64))
	for i, q := range queries64 {
		queries[i] = m8Query32V1(q)
	}
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	dir := assets.dir
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	assets.owned = false
	if err := assets.db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := assets.Close(); err != nil {
		t.Fatal(err)
	}
	assets, err = openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assets.Close(); err != nil {
			t.Error(err)
		}
	})
	return assets, queries, truth
}

func TestM8WholeCollectionReadOnlyPublicPathV1(t *testing.T) {
	assets, queries, truth := testM8WholeCollectionReadOnlyV1(t)
	before, err := assets.collection.VectorPartitionSourceIdentityV1(partitionHNSWIndex)
	if err != nil {
		t.Fatal(err)
	}
	for _, concurrency := range []int{1, 32} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			cell := m8MeasureWholeCollectionV1(assets.collection, queries, 128, concurrency, 32)
			if err := m8SummarizeWholeCollectionV1(&cell, truth, 128); err != nil {
				t.Fatal(err)
			}
			if cell.SetupError != "" || cell.Summary.Succeeded != len(queries) || cell.Summary.ServiceRecall < .95 || cell.AllocatedBytes == 0 || cell.Allocations == 0 {
				t.Fatalf("public path: setup=%q summary=%+v first=%+v", cell.SetupError, cell.Summary, cell.Attempts[0])
			}
			// All receipts are checked after worker buffers have been reused.
			for _, a := range cell.Attempts {
				if a.Class != "success" {
					t.Fatalf("attempt=%+v", a)
				}
			}
		})
	}
	after, err := assets.collection.VectorPartitionSourceIdentityV1(partitionHNSWIndex)
	if err != nil || before != after {
		t.Fatal("reference modified retained source")
	}
	broken := append([][]float32(nil), queries...)
	broken[1] = []float32{1}
	cell := m8MeasureWholeCollectionV1(assets.collection, broken, 128, 32, 0)
	if err := m8SummarizeWholeCollectionV1(&cell, truth, 128); err != nil {
		t.Fatal(err)
	}
	if cell.Summary.Errors != 1 || cell.Summary.Succeeded != len(queries)-1 || cell.Summary.Dispatched != len(queries) {
		t.Fatalf("error dropped population: %+v", cell.Summary)
	}
}

func BenchmarkM8WholeCollectionReferenceV1(b *testing.B) {
	assets, queries, _ := testM8WholeCollectionReadOnlyV1(b)
	b.ReportAllocs()
	b.ResetTimer()
	var bytes, allocations uint64
	for i := 0; i < b.N; i++ {
		cell := m8MeasureWholeCollectionV1(assets.collection, queries, 128, 1, 32)
		if cell.SetupError != "" {
			b.Fatal(cell.SetupError)
		}
		bytes += cell.AllocatedBytes
		allocations += cell.Allocations
	}
	b.ReportMetric(float64(bytes)/float64(b.N*len(queries)), "measured-B/query")
	b.ReportMetric(float64(allocations)/float64(b.N*len(queries)), "measured-allocs/query")
	// Standard ns/op/B/op also include per-cell setup, warmup and receipt slots.
}
