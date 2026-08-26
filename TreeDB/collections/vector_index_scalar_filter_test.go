package collections

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
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
	rangeChecks := []struct {
		filter HybridScalarFilter
		wantID string
	}{
		{
			filter: HybridScalarFilter{IndexName: "sequence_idx", Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(7), Inclusive: true},
				Upper: IndexRangeBound{Value: int64(9007199254740993), Inclusive: false},
			}},
			wantID: "other",
		},
		{
			filter: HybridScalarFilter{IndexName: "sequence_idx", Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(7), Inclusive: false},
				Upper: IndexRangeBound{Value: int64(9007199254740993), Inclusive: true},
			}},
			wantID: "all",
		},
	}
	for _, check := range rangeChecks {
		response := searchNativeScalarTest(t, col, def, check.filter, 3)
		if len(response.Results) != 1 || string(response.Results[0].ID) != check.wantID {
			t.Fatalf("range=%+v results=%+v want=%q; missing field must not match", check.filter.Range, response.Results, check.wantID)
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

func TestNativeScalarVectorIndexRebuildSwapsAlignedColumns(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("keep"), []byte("move"), []byte("gone")},
		[][]byte{
			[]byte(`{"embedding":[0.8,0.2],"tenant":"alpha"}`),
			[]byte(`{"embedding":[1,0],"tenant":"alpha"}`),
			[]byte(`{"embedding":[0.9,0.1],"tenant":"alpha"}`),
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 3); len(got.Results) != 3 {
		t.Fatalf("initial results=%+v", got.Results)
	}
	runtime := col.registeredVectorIndex(def.Name)
	if runtime == nil {
		t.Fatal("registered native runtime is unavailable")
	}
	if replaced, err := col.Replace([]byte("move"), []byte(`{"embedding":[1,0],"tenant":"beta"}`)); err != nil || !replaced {
		t.Fatalf("replace move=%v err=%v", replaced, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("gone")}); err != nil || deleted != 1 {
		t.Fatalf("delete gone=%d err=%v", deleted, err)
	}
	if err := runtime.Rebuild(); err != nil {
		t.Fatal(err)
	}
	alpha := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 3)
	if len(alpha.Results) != 1 ||
		string(alpha.Results[0].ID) != "keep" ||
		alpha.Stats.ScalarFilterCandidateIDs != 1 ||
		alpha.Stats.ScalarFilterAdmitted != 1 ||
		alpha.Stats.ScalarFilterExactScoring != 1 {
		t.Fatalf("rebuilt alpha=%+v diagnostics=%+v", alpha.Results, alpha.Diagnostics())
	}
	beta := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "beta"}, 3)
	if len(beta.Results) != 1 ||
		string(beta.Results[0].ID) != "move" ||
		beta.Stats.ScalarFilterCandidateIDs != 1 ||
		beta.Stats.ScalarFilterAdmitted != 1 ||
		beta.Stats.ScalarFilterExactScoring != 1 {
		t.Fatalf("rebuilt beta=%+v diagnostics=%+v", beta.Results, beta.Diagnostics())
	}
	view := runtime.acquireSearchView()
	if view == nil {
		t.Fatal("rebuilt search view is unavailable")
	}
	column, ok := view.scalarColumns["tenant_idx"]
	rows, nodes, deltaNodes := len(column.offsets)-1, len(view.nodes), len(view.deltaNodes)
	aligned := ok && rows == nodes && nodes == 2 && deltaNodes == 0
	runtime.releaseSearchView(view)
	if !aligned {
		t.Fatalf("rebuilt scalar alignment ok=%v rows=%d nodes=%d delta=%d", ok, rows, nodes, deltaNodes)
	}
}

func TestSearchVectorIndexDeclaredScalarFilterFailsClosedWithoutTenantLeak(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("alpha"), []byte("beta-nearest")}, [][]byte{
		[]byte(`{"embedding":[0,1],"tenant":"alpha"}`),
		[]byte(`{"embedding":[1,0],"tenant":"beta"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	filter := HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}
	response, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name, Query: []float32{1, 0}, TopK: 1, DeclaredScalarFilter: &filter,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || len(response.Results) != 0 {
		t.Fatalf("response=%+v err=%v; convenience path must fail closed instead of returning beta-nearest", response, err)
	}
}

func TestNativeScalarExplicitPostfilterPreservesCandidateLimit(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("alpha-far"), []byte("beta-near")}, [][]byte{
		[]byte(`{"embedding":[0,1],"tenant":"alpha"}`),
		[]byte(`{"embedding":[1,0],"tenant":"beta"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	filter := &HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}
	vector := &HybridVectorQuery{
		IndexName: def.Name, Query: []float32{1, 0}, CandidateLimit: 1, EfSearch: 16, QueryMode: VectorIndexQueryModeExact,
	}
	postfiltered, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 1, Vector: vector, ScalarFilter: filter,
		ScalarFilterStrategy: HybridScalarFilterStrategyPostfilter,
		ResultMode:           HybridResultModeScoreOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(postfiltered.Results) != 0 ||
		postfiltered.Stats.ScalarPostfilterChecks != 1 ||
		postfiltered.Stats.ScalarFilterMatched != 0 ||
		postfiltered.Stats.ScalarFilterRejected != 1 {
		t.Fatalf("postfilter response=%+v", postfiltered)
	}
	prefiltered, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 1, Vector: vector, ScalarFilter: filter, ResultMode: HybridResultModeScoreOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prefiltered.Results) != 1 ||
		string(prefiltered.Results[0].ID) != "alpha-far" ||
		prefiltered.Stats.ScalarFilterPlan != NativeScalarFilterPlanCompleteExact {
		t.Fatalf("native prefilter response=%+v", prefiltered)
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
	if all.Stats.ScalarFilterPlan != NativeScalarFilterPlanVectorAligned || all.Stats.ScalarFilterProbeTruncated == 0 || all.Stats.ScalarFilterExactScoring != 0 {
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
	if mixed.Stats.ScalarFilterPlan != NativeScalarFilterPlanMixed || mixed.Stats.ScalarFilterProbeTruncated == 0 || mixed.Stats.ScalarFilterExactScoring != 1 || len(mixed.Results) != 2 {
		t.Fatalf("mixed diagnostics=%+v results=%+v", mixed.Diagnostics(), mixed.Results)
	}

	finite := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "path_idx", Value: "finite"}, 8)
	if finite.Stats.ScalarFilterPlan != NativeScalarFilterPlanCompleteFinite || finite.Stats.ScalarFilterExactScoring != 0 || len(finite.Results) != 8 {
		t.Fatalf("finite diagnostics=%+v results=%d", finite.Diagnostics(), len(finite.Results))
	}
	exact := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "path_idx", Value: "small"}, 8)
	if exact.Stats.ScalarFilterPlan != NativeScalarFilterPlanCompleteExact || exact.Stats.ScalarFilterExactScoring != 1 || len(exact.Results) != 2 {
		t.Fatalf("exact diagnostics=%+v results=%d", exact.Diagnostics(), len(exact.Results))
	}
	totalRows := alphaRows + betaRows
	smallOracle := nativeScalarFixtureExactOracle(totalRows, 8, func(i int) bool { return i < 2 })
	assertNativeScalarExactOracle(t, exact.Results, smallOracle)
	assertNativeScalarExactOracle(t, mixed.Results, smallOracle)
	finiteOracle := nativeScalarFixtureExactOracle(totalRows, 8, func(i int) bool {
		return i >= 2 && i < 2+nativeScalarExactSafetyCap+88
	})
	assertNativeScalarANNOracleContract(t, finite.Results, finiteOracle)
	allOracle := nativeScalarFixtureExactOracle(totalRows, 8, func(int) bool { return true })
	assertNativeScalarANNOracleContract(t, all.Results, allOracle)
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
	if replaced, err := col.Replace([]byte("alpha"), []byte(`{"embedding":[1,0],"tenant":"beta"}`)); err != nil || !replaced {
		t.Fatalf("replace after reopen=%v err=%v", replaced, err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("gamma")}, [][]byte{[]byte(`{"embedding":[0.98,0.02],"tenant":"alpha"}`)}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("beta")}); err != nil || deleted != 1 {
		t.Fatalf("delete after reopen=%d err=%v", deleted, err)
	}
	response = searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 3)
	if len(response.Results) != 1 || string(response.Results[0].ID) != "gamma" {
		t.Fatalf("base+live-delta update/delete results=%+v", response.Results)
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
	col, err = NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	response = searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 3)
	if len(response.Results) != 1 || string(response.Results[0].ID) != "gamma" {
		t.Fatalf("base+delta update/delete second reopen results=%+v", response.Results)
	}
}

func TestNativeScalarConcurrentMutationSearchTenantIsolation(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch([][]byte{[]byte("alpha"), []byte("beta")}, [][]byte{
		[]byte(`{"embedding":[0.9,0.1],"tenant":"alpha"}`),
		[]byte(`{"embedding":[1,0],"tenant":"beta"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 32; i++ {
			document := []byte(fmt.Sprintf(`{"embedding":[1,%0.4f],"tenant":"beta"}`, float64(i+1)/1000))
			replaced, err := col.Replace([]byte("beta"), document)
			if err != nil {
				errs <- fmt.Errorf("replace beta: %w", err)
				return
			}
			if !replaced {
				errs <- errors.New("replace beta reported missing document")
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		filter := HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}
		var buffer VectorIndexSearchBuffer
		for i := 0; i < 64; i++ {
			response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
				IndexName: def.Name, Query: []float32{1, 0}, TopK: 2, EfSearch: 64,
				StatsMode: VectorIndexSearchStatsModeProduction, DeclaredScalarFilter: &filter,
			}, &buffer)
			if err != nil {
				if errors.Is(err, ErrHybridSearchStaleIndex) || errors.Is(err, ErrVectorIndexSnapshotMismatch) {
					continue
				}
				errs <- err
				return
			}
			for _, result := range response.Results {
				if string(result.ID) != "alpha" {
					errs <- fmt.Errorf("cross-tenant result %q", result.ID)
					return
				}
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
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

func BenchmarkNativeScalarFilterExecutorCrossover(b *testing.B) {
	const graphRows = 8192
	cardinalities := []int{256, 512, 1024}
	for _, dimensions := range []int{2, 128} {
		idx, query := newNativeScalarExecutorBenchmarkIndex(b, dimensions, graphRows)
		view := idx.searchView.Load()
		if view == nil {
			b.Fatal("native scalar benchmark view is unavailable")
		}
		allowSets := make(map[int]hybridScalarAllowSet, len(cardinalities))
		for _, cardinality := range cardinalities {
			allowSet := make(hybridScalarAllowSet, cardinality)
			for i := 0; i < cardinality; i++ {
				allowSet[fmt.Sprintf("doc-%05d", i)] = struct{}{}
			}
			allowSets[cardinality] = allowSet
		}
		for _, cardinality := range cardinalities {
			allowSet := allowSets[cardinality]
			plans := []struct {
				name  string
				exact bool
			}{
				{name: "exact", exact: true},
				{name: "finite_ann", exact: false},
			}
			for _, route := range plans {
				identity := NativeScalarFilterPlanCompleteFinite
				if route.exact {
					identity = NativeScalarFilterPlanCompleteExact
				}
				plan := &nativeScalarFilterExecution{
					identity:         identity,
					finiteIDs:        allowSet,
					candidateIDs:     uint64(cardinality),
					sourceGeneration: view.sourceDocumentGeneration,
					exactScoring:     route.exact,
				}
				name := fmt.Sprintf("dim%d/card%d/serial/%s", dimensions, cardinality, route.name)
				b.Run(name, func(b *testing.B) {
					var buffer VectorIndexSearchBuffer
					b.ReportAllocs()
					for b.Loop() {
						results, _, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 64, plan, &buffer)
						if err != nil || len(results) != 10 {
							b.Fatalf("results=%d err=%v", len(results), err)
						}
					}
					b.ReportMetric(float64(graphRows), "graph_rows")
					b.ReportMetric(float64(cardinality), "candidate_ids/op")
				})
				if dimensions == 128 && cardinality == 512 {
					name = fmt.Sprintf("dim%d/card%d/parallel/%s", dimensions, cardinality, route.name)
					b.Run(name, func(b *testing.B) {
						b.ReportAllocs()
						b.ReportMetric(float64(graphRows), "graph_rows")
						b.ReportMetric(float64(cardinality), "candidate_ids/op")
						b.RunParallel(func(pb *testing.PB) {
							var buffer VectorIndexSearchBuffer
							for pb.Next() {
								results, _, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 64, plan, &buffer)
								if err != nil || len(results) != 10 {
									b.Errorf("results=%d err=%v", len(results), err)
									return
								}
							}
						})
					})
				}
			}
		}
	}
}

func newNativeScalarExecutorBenchmarkIndex(b *testing.B, dimensions, graphRows int) (*VectorIndex, []float32) {
	b.Helper()
	idx, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "native_scalar_crossover", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: dimensions, M: 8, EfConstruction: 64, EfSearch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	query := make([]float32, dimensions)
	query[0] = 1
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for i := 0; i < graphRows; i++ {
		id := []byte(fmt.Sprintf("doc-%05d", i))
		vector := make([]float32, dimensions)
		vector[0] = 1
		vector[1] = float32(i+1) / float32(graphRows*4)
		if err := idx.insertVectorLocked(id, vector); err != nil {
			b.Fatal(err)
		}
	}
	idx.sourceDocumentGeneration = 1
	idx.sourceDocumentRootsValid = true
	idx.publishSearchViewLocked(true)
	return idx, query
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

func nativeScalarFixtureExactOracle(totalRows, topK int, matches func(int) bool) []VectorIndexSearchResult {
	results := make([]VectorIndexSearchResult, 0, topK)
	for i := 0; i < totalRows; i++ {
		if !matches(i) {
			continue
		}
		y := float32(float64(i+1) / 100000)
		results = append(results, VectorIndexSearchResult{
			ID:    []byte(fmt.Sprintf("doc-%05d", i)),
			Score: 1 / math.Sqrt(1+float64(y)*float64(y)),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return string(results[i].ID) < string(results[j].ID)
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}

func assertNativeScalarExactOracle(t *testing.T, got, oracle []VectorIndexSearchResult) {
	t.Helper()
	if len(got) != len(oracle) {
		t.Fatalf("exact result count=%d want=%d results=%+v", len(got), len(oracle), got)
	}
	for i := range oracle {
		if string(got[i].ID) != string(oracle[i].ID) || math.Abs(got[i].Score-oracle[i].Score) > 1e-6 {
			t.Fatalf("exact result[%d]=%+v want=%+v", i, got[i], oracle[i])
		}
	}
}

func assertNativeScalarANNOracleContract(t *testing.T, got, oracle []VectorIndexSearchResult) {
	t.Helper()
	if len(got) != len(oracle) {
		t.Fatalf("ANN result count=%d want=%d results=%+v", len(got), len(oracle), got)
	}
	oracleIDs := make(map[string]struct{}, len(oracle))
	for _, result := range oracle {
		oracleIDs[string(result.ID)] = struct{}{}
	}
	overlap := 0
	for i, result := range got {
		if math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
			t.Fatalf("ANN result[%d] has non-finite score: %+v", i, result)
		}
		if i > 0 && (got[i-1].Score < result.Score ||
			got[i-1].Score == result.Score && string(got[i-1].ID) > string(result.ID)) {
			t.Fatalf("ANN results violate deterministic score/id order at %d: %+v", i, got)
		}
		if _, ok := oracleIDs[string(result.ID)]; ok {
			overlap++
		}
	}
	if overlap != len(oracle) {
		t.Fatalf("ANN top-k recall=%d/%d results=%+v oracle=%+v", overlap, len(oracle), got, oracle)
	}
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
