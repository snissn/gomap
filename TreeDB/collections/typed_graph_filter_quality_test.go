package collections

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"
)

// This engine diagnostic reuses the existing deterministic vector generator.
// It is deliberately separate from the frozen Minima application manifest.
func openTypedGraphQualityFixture(t testing.TB, n int) (*Collection, *VectorIndexSearcher, [][]byte, [][]byte, []TypedColumnBatch, []int) {
	t.Helper()
	started := time.Now()
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].M = 16
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	t.Cleanup(func() { _ = db.Close() })
	ids, retained := make([][]byte, n), make([][]byte, n)
	columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	ranks := make([]int, n)
	for i := range n {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
		columns[0].Float32Vectors = append(columns[0].Float32Vectors, vectorBenchmarkEmbedding(i, 8))
		columns[1].Strings = append(columns[1].Strings, "content")
		ranks[i] = (i * 7919) % n
		columns[2].Strings = append(columns[2].Strings, fmt.Sprintf("%05d", ranks[i]))
		columns[3].Strings = append(columns[3].Strings, "source")
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	inserted := time.Now()
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	built := time.Now()
	base, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	t.Logf("fixture rows=%d dims=8 M=16 generated_insert=%s rebuild=%s open=%s", n, inserted.Sub(started), built.Sub(inserted), time.Since(built))
	return col, base, ids, retained, columns, ranks
}

func TestTypedGraphPreparedFilterDispersedQuality(t *testing.T) {
	const n = 50000
	col, base, ids, retained, columns, ranks := openTypedGraphQualityFixture(t, n)
	deleted := make([]bool, n)
	queries := make([][]float32, 0, 8)
	for i := range n {
		if ranks[i] >= 512 && ranks[i] < 4097 {
			queries = append(queries, slices.Clone(columns[0].Float32Vectors[i]))
		}
		if len(queries) == cap(queries) {
			break
		}
	}
	oracle := func(query []float32, limit int) []VectorIndexSearchResult {
		var queryNorm float64
		for _, x := range query {
			queryNorm += float64(x) * float64(x)
		}
		var results []VectorIndexSearchResult
		for i, vector := range columns[0].Float32Vectors {
			if deleted[i] || ranks[i] >= limit {
				continue
			}
			var dot, norm float64
			for j, x := range vector {
				dot += float64(x) * float64(query[j])
				norm += float64(x) * float64(x)
			}
			results = append(results, VectorIndexSearchResult{ID: ids[i], Score: dot / math.Sqrt(norm*queryNorm)})
		}
		slices.SortFunc(results, func(a, b VectorIndexSearchResult) int {
			if vectorIndexSearchResultBefore(a, b) {
				return -1
			}
			if vectorIndexSearchResultBefore(b, a) {
				return 1
			}
			return 0
		})
		return results[:min(10, len(results))]
	}
	check := func(stage string) {
		current, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		defer current.Close()
		overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 32, Tombstones: 16, Bytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		for _, count := range []int{4096, 4097, 5000, n} {
			setup := time.Now()
			plan, err := prepareTypedGraphFilter(overlay, HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: fmt.Sprintf("%05d", count-1), Inclusive: true}}}, typedGraphFilterLimits{SourceIDs: n + 32, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 4 << 20, InspectedEntries: 2 * n})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s limit=%d final=%d setup=%s source_ids=%d source_bytes=%d retained=%d inspected=%d lookup_work_bound=%d", stage, count, plan.count, time.Since(setup), plan.sourceIDs, plan.sourceBytes, plan.retainedBytes, plan.inspectedEntries, plan.mappingWork)
			var buffer VectorIndexSearchBuffer
			for q, query := range queries {
				got, stats, err := overlay.searchPreparedFilter(plan, query, 10, 256, 8192, &buffer)
				if err != nil {
					t.Fatalf("%s limit=%d query=%d stats=%+v err=%v", stage, count, q, stats, err)
				}
				want := oracle(query, count)
				overlap := 0
				for _, result := range got {
					for _, expected := range want {
						if string(result.ID) == string(expected.ID) {
							overlap++
							break
						}
					}
				}
				t.Logf("%s limit=%d query=%d recall=%d/%d exact=%t exact_base=%d delta=%d candidates=%d edges=%d seed_inspected=%d ineligible_scored=%d frontier_peak=%d", stage, count, q, overlap, len(want), stats.FilteredExact, stats.ExactBaseScored, stats.DeltaScored, stats.Base.Candidates, stats.Base.Edges, stats.Base.FilteredSeedInspections, stats.Base.FilteredIneligibleScores, stats.Base.FilteredFrontierPeak)
				if plan.count <= 4096 {
					assertNativeScalarExactOracle(t, got, want)
				} else {
					if stats.Base.Candidates == 0 || stats.Base.Edges == 0 || stats.ExactBaseScored != 0 {
						t.Fatalf("ANN missing graph work: %+v", stats)
					}
					assertNativeScalarANNOracleContract(t, got, want)
				}
			}
		}
	}
	check("base")
	// Modify selected and unselected rows, then delete/reinsert one ID. Existing
	// base remains pinned; current postings and typed suffix own the new truth.
	selected := -1
	unselected := -1
	for i := range n {
		if selected < 0 && ranks[i] < 4096 {
			selected = i
		}
		if unselected < 0 && ranks[i] >= 4097 {
			unselected = i
		}
		if selected >= 0 && unselected >= 0 {
			break
		}
	}
	for _, i := range []int{selected, unselected} {
		newRank := 60000
		if i == unselected {
			newRank = 1000
		}
		newVector := vectorBenchmarkEmbedding(n+i, 8)
		replacement := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{newVector}}, {Name: "content", Strings: []string{"replacement"}}, {Name: "user", Strings: []string{fmt.Sprintf("%05d", newRank)}}, {Name: "path", Strings: []string{"updated"}}}
		if _, err := col.ReplaceTypedBatch(ids[i:i+1], retained[i:i+1], replacement); err != nil {
			t.Fatal(err)
		}
		ranks[i], columns[0].Float32Vectors[i] = newRank, newVector
	}
	if err := col.Delete(ids[unselected]); err != nil {
		t.Fatal(err)
	}
	reinsert := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{columns[0].Float32Vectors[unselected]}}, {Name: "content", Strings: []string{"reinsert"}}, {Name: "user", Strings: []string{"01000"}}, {Name: "path", Strings: []string{"updated"}}}
	if _, _, err := col.InsertTypedBatchWithStats(ids[unselected:unselected+1], retained[unselected:unselected+1], reinsert); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if i != unselected && i != selected && ranks[i] < 4096 {
			if err := col.Delete(ids[i]); err != nil {
				t.Fatal(err)
			}
			deleted[i] = true
			break
		}
	}
	// A new ID balances the deleted eligible row and exercises an append-only
	// base part in the suffix, not merely replacement/tombstone parts.
	newID := []byte("row-50000")
	newRetained := []byte(`{"id":"row-50000"}`)
	newVector := vectorBenchmarkEmbedding(2*n, 8)
	insert := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{newVector}}, {Name: "content", Strings: []string{"new append"}}, {Name: "user", Strings: []string{"01001"}}, {Name: "path", Strings: []string{"new"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{newID}, [][]byte{newRetained}, insert); err != nil {
		t.Fatal(err)
	}
	ids = append(ids, newID)
	ranks = append(ranks, 1001)
	deleted = append(deleted, false)
	columns[0].Float32Vectors = append(columns[0].Float32Vectors, newVector)
	check("mutated")
}
