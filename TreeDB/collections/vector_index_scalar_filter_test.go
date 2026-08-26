package collections

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeScalarColumnsNormalizeTypesMissingAndLiveMutations(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{
		{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString},
		{Name: "active_idx", Field: "active", ValueType: IndexValueBool},
		{Name: "sequence_idx", Field: "sequence", ValueType: IndexValueInt64},
		{Name: "weight_idx", Field: "weight", ValueType: IndexValueDouble},
	})
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("all"), []byte("missing"), []byte("other")}
	documents := [][]byte{
		[]byte(`{"embedding":[0.99,0.01],"tenant":"alpha","active":true,"sequence":9007199254740993,"weight":1.5}`),
		[]byte(`{"embedding":[1,0]}`),
		[]byte(`{"embedding":[0.98,0.02],"tenant":"beta","active":false,"sequence":7,"weight":2.5}`),
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	checks := []HybridScalarFilter{
		{IndexName: "tenant_idx", Value: "alpha"},
		{IndexName: "active_idx", Value: true},
		{IndexName: "sequence_idx", Value: int64(9007199254740993)},
		{IndexName: "weight_idx", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: 1.5, Inclusive: true}, Upper: IndexRangeBound{Value: 1.5, Inclusive: true}}},
	}
	for _, filter := range checks {
		response := searchNativeScalarTest(t, col, def, filter, 3)
		if len(response.Results) != 1 || string(response.Results[0].ID) != "all" {
			t.Fatalf("filter=%+v results=%+v", filter, response.Results)
		}
		if response.Stats.ScalarFilterPlan != NativeScalarFilterPlanCompleteExact {
			t.Fatalf("filter=%+v plan=%s", filter, response.Stats.ScalarFilterPlan)
		}
	}
	if replaced, err := col.Replace([]byte("all"), []byte(`{"embedding":[0.99,0.01],"tenant":"beta","active":true,"sequence":9007199254740993,"weight":1.5}`)); err != nil || !replaced {
		t.Fatalf("replace=%v err=%v", replaced, err)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 3); len(got.Results) != 0 {
		t.Fatalf("old tenant survived update: %+v", got.Results)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("all")}); err != nil || deleted != 1 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "active_idx", Value: true}, 3); len(got.Results) != 0 {
		t.Fatalf("deleted id survived scalar search: %+v", got.Results)
	}
}

func TestNativeScalarPlannerOverLimitMixedFiniteAndTenantIsolation(t *testing.T) {
	indexes := []IndexDefinition{
		{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString},
		{Name: "path_idx", Field: "path", ValueType: IndexValueString},
		{Name: "active_idx", Field: "active", ValueType: IndexValueBool},
	}
	d, col, def := newNativeScalarTestCollection(t, indexes)
	defer func() { _ = d.Close() }()
	const alphaRows = hybridScalarDefaultLookupLimit + 1
	const betaRows = 16
	ids := make([][]byte, 0, alphaRows+betaRows)
	documents := make([][]byte, 0, alphaRows+betaRows)
	for i := range alphaRows + betaRows {
		tenant := "alpha"
		if i >= alphaRows {
			tenant = "beta"
		}
		path := "bulk"
		switch {
		case i < 2:
			path = "small"
		case i < 2+nativeScalarExactSafetyCap+88:
			path = "finite"
		}
		ids = append(ids, []byte(fmt.Sprintf("doc-%05d", i)))
		documents = append(documents, []byte(fmt.Sprintf(`{"embedding":[1,%0.8f],"tenant":%q,"path":%q,"active":true}`, float64(i+1)/100000, tenant, path)))
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}

	all := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "active_idx", Value: true}, 8)
	if all.Stats.ScalarFilterPlan != NativeScalarFilterPlanVectorAligned || all.Stats.ScalarFilterProbeTruncated == 0 {
		t.Fatalf("all-match diagnostics=%+v", all.Diagnostics())
	}
	unfiltered := searchNativeUnfilteredTest(t, col, def, 8)
	if !sameVectorIndexResultIDs(all.Results, unfiltered.Results) {
		t.Fatalf("all-match=%+v unfiltered=%+v", all.Results, unfiltered.Results)
	}

	alpha := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 16)
	if alpha.Stats.ScalarFilterPlan != NativeScalarFilterPlanVectorAligned || alpha.Stats.ScalarFilterProbeTruncated == 0 {
		t.Fatalf("alpha diagnostics=%+v", alpha.Diagnostics())
	}
	for _, result := range alpha.Results {
		if string(result.ID) >= fmt.Sprintf("doc-%05d", alphaRows) {
			t.Fatalf("cross-tenant result=%q", result.ID)
		}
	}

	mixed := searchNativeScalarTest(t, col, def, HybridScalarFilter{And: []HybridScalarFilter{
		{IndexName: "tenant_idx", Value: "alpha"},
		{IndexName: "path_idx", Value: "small"},
	}}, 8)
	if mixed.Stats.ScalarFilterPlan != NativeScalarFilterPlanMixed || mixed.Stats.ScalarFilterProbeTruncated == 0 || len(mixed.Results) != 2 {
		t.Fatalf("mixed diagnostics=%+v results=%+v", mixed.Diagnostics(), mixed.Results)
	}

	finite := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "path_idx", Value: "finite"}, 8)
	if finite.Stats.ScalarFilterPlan != NativeScalarFilterPlanCompleteFinite || len(finite.Results) != 8 {
		t.Fatalf("finite diagnostics=%+v results=%d", finite.Diagnostics(), len(finite.Results))
	}
	exact := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "path_idx", Value: "small"}, 8)
	if exact.Stats.ScalarFilterPlan != NativeScalarFilterPlanCompleteExact || len(exact.Results) != 2 {
		t.Fatalf("exact diagnostics=%+v results=%d", exact.Diagnostics(), len(exact.Results))
	}
}

func TestNativeScalarColumnsReopenFromSecondaryIndexes(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{DocumentFormat: DocumentFormatJSON},
		Indexes:       []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := mgr.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("alpha"), []byte("beta")}, [][]byte{
		[]byte(`{"embedding":[1,0],"tenant":"alpha"}`),
		[]byte(`{"embedding":[0.99,0.01],"tenant":"beta"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	col, err = NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	response := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 2)
	if len(response.Results) != 1 || string(response.Results[0].ID) != "alpha" {
		t.Fatalf("reopened scalar results=%+v", response.Results)
	}
}

func TestNativeScalarFilterUnsupportedMultikeyFailsClosed(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tags_idx", Field: "tags", ValueType: IndexValueString, MultiKey: true}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0],"tags":["a"]}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
		IndexName: def.Name, Query: []float32{1, 0}, TopK: 1, StatsMode: VectorIndexSearchStatsModeProduction,
		DeclaredScalarFilter: &HybridScalarFilter{IndexName: "tags_idx", Value: "a"},
	}, &buffer)
	if err == nil || len(response.Results) != 0 || response.Stats.SearchRouteNativeRuntime != 0 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func BenchmarkNativeScalarFilterPlanCrossover(b *testing.B) {
	d, col, def := newNativeScalarTestCollection(b, []IndexDefinition{
		{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString},
		{Name: "path_idx", Field: "path", ValueType: IndexValueString},
		{Name: "active_idx", Field: "active", ValueType: IndexValueBool},
	})
	defer func() { _ = d.Close() }()
	rows := hybridScalarDefaultLookupLimit*5 + 257
	ids := make([][]byte, rows)
	documents := make([][]byte, rows)
	for i := range rows {
		path := "broad"
		if i < 64 {
			path = "exact"
		} else if i < 64+nativeScalarExactSafetyCap+64 {
			path = "finite"
		}
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		documents[i] = []byte(fmt.Sprintf(`{"embedding":[1,%0.8f],"tenant":"alpha","path":%q,"active":%t}`, float64(i+1)/100000, path, i%5 == 0))
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		b.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatal(err)
	}
	cases := []struct {
		name   string
		filter HybridScalarFilter
	}{
		{name: "exact", filter: HybridScalarFilter{IndexName: "path_idx", Value: "exact"}},
		{name: "finite", filter: HybridScalarFilter{IndexName: "path_idx", Value: "finite"}},
		{name: "mixed", filter: HybridScalarFilter{And: []HybridScalarFilter{{IndexName: "tenant_idx", Value: "alpha"}, {IndexName: "path_idx", Value: "exact"}}}},
		{name: "all_match_over_limit", filter: HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}},
		{name: "sparse_over_limit", filter: HybridScalarFilter{IndexName: "active_idx", Value: true}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var buffer VectorIndexSearchBuffer
			opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 10, EfSearch: 64, StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &tc.filter}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newNativeScalarTestCollection(tb testing.TB, indexes []IndexDefinition) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatal(err)
	}
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 8, EfSearch: 64, Strategy: VectorIndexStrategyNativeRuntime}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, AllowArrayValuesInIndex: true}, Indexes: indexes, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		_ = d.Close()
		tb.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatal(err)
	}
	return d, col, def
}

func searchNativeScalarTest(t *testing.T, col *Collection, def VectorIndexDefinition, filter HybridScalarFilter, topK int) VectorIndexSearchResponse {
	t.Helper()
	var buffer VectorIndexSearchBuffer
	response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: topK, EfSearch: 64, StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &filter}, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func searchNativeUnfilteredTest(t *testing.T, col *Collection, def VectorIndexDefinition, topK int) VectorIndexSearchResponse {
	t.Helper()
	var buffer VectorIndexSearchBuffer
	response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: topK, EfSearch: 64, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func sameVectorIndexResultIDs(left, right []VectorIndexSearchResult) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if string(left[i].ID) != string(right[i].ID) {
			return false
		}
	}
	return true
}
