package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
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

func TestNativeScalarBSONNumericTypesPreservedAcrossBuildAndMutation(t *testing.T) {
	d, col, def := newNativeScalarTestCollectionWithFormat(t, DocumentFormatBSON, []IndexDefinition{
		{Name: "sequence_idx", Field: "sequence", ValueType: IndexValueInt64},
		{Name: "weight_idx", Field: "weight", ValueType: IndexValueDouble},
	})
	defer func() { _ = d.Close() }()
	const sequence = int64(9007199254740993)
	document := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "doc"},
		{Key: "embedding", Value: bson.A{1.0, 0.0}},
		{Key: "sequence", Value: sequence},
		{Key: "weight", Value: 1.5},
	})
	if _, err := col.InsertBatch([][]byte{[]byte("doc")}, [][]byte{document}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "sequence_idx", Value: sequence}, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "doc" {
		t.Fatalf("BSON int64 build results=%+v", got.Results)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "weight_idx", Value: 1.5}, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "doc" {
		t.Fatalf("BSON double build results=%+v", got.Results)
	}

	replacement := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "doc"},
		{Key: "embedding", Value: bson.A{0.9, 0.1}},
		{Key: "sequence", Value: sequence + 1},
		{Key: "weight", Value: 2.5},
	})
	if replaced, err := col.Replace([]byte("doc"), replacement); err != nil || !replaced {
		t.Fatalf("replace=%v err=%v", replaced, err)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "sequence_idx", Value: sequence}, 1); len(got.Results) != 0 {
		t.Fatalf("old BSON int64 survived mutation: %+v", got.Results)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "sequence_idx", Value: sequence + 1}, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "doc" {
		t.Fatalf("updated BSON int64 results=%+v", got.Results)
	}
	if got := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "weight_idx", Value: 2.5}, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "doc" {
		t.Fatalf("updated BSON double results=%+v", got.Results)
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
	rows, nodes, deltaNodes := column.rows, len(view.nodes), len(view.deltaNodes)
	aligned := ok && rows == nodes && nodes == 2 && deltaNodes == 0
	runtime.releaseSearchView(view)
	if !aligned {
		t.Fatalf("rebuilt scalar alignment ok=%v rows=%d nodes=%d delta=%d", ok, rows, nodes, deltaNodes)
	}
}

func TestNonNativeVectorIndexesSkipScalarRuntimeAndRetainBatchPath(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	columnDef := VectorIndexDefinition{
		Name: "embedding_column", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: nativeVectorFrozenPrefixBatchWidth, Strategy: VectorIndexStrategyColumnGraph,
	}
	manager := NewCollectionManager(d)
	if _, err := manager.CreateCollection(&CollectionMeta{
		Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON},
		Indexes:       []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}},
		VectorIndexes: []VectorIndexDefinition{columnDef},
	}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	column, err := newVectorIndex(col, vectorIndexOptionsFromDefinition(columnDef))
	if err != nil {
		t.Fatal(err)
	}
	adHoc, err := newVectorIndex(col, VectorIndexOptions{
		Name: "adhoc", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: nativeVectorFrozenPrefixBatchWidth,
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, idx := range map[string]*VectorIndex{"column_graph": column, "ad_hoc": adHoc} {
		if len(idx.scalarDefinitions) != 0 || len(idx.scalarRuntimes) != 0 || len(idx.scalarColumns) != 0 {
			t.Fatalf("%s scalar state defs=%d runtimes=%d columns=%d", name, len(idx.scalarDefinitions), len(idx.scalarRuntimes), len(idx.scalarColumns))
		}
	}
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = materializer.Close() }()
	ids := make([][]byte, nativeVectorFrozenPrefixBatchMinimum)
	documents := make([][]byte, len(ids))
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		documents[i] = []byte(fmt.Sprintf(`{"embedding":[1,%0.4f],"tenant":"alpha"}`, float64(i+1)/1000))
	}
	column.nativePersistent = true // Make the retained frozen-prefix batch path observable.
	if err := column.insertStoredDocumentsUnpublished(materializer, ids, documents); err != nil {
		t.Fatal(err)
	}
	if column.frozenPrefixBatches == 0 || len(column.nodes) != len(ids) {
		t.Fatalf("batch path batches=%d nodes=%d want nodes=%d", column.frozenPrefixBatches, len(column.nodes), len(ids))
	}
}

func TestCreateNativeVectorIndexAfterScalarIndexPublishesFilteredRuntime(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(d)
	if _, err := manager.CreateCollection(&CollectionMeta{
		Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON},
	}); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	def := VectorIndexDefinition{
		Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 8, EfSearch: 64, Strategy: VectorIndexStrategyNativeRuntime,
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	runtime := col.registeredVectorIndex(def.Name)
	if runtime == nil {
		_ = d.Close()
		t.Fatal("created native runtime is unavailable")
	}
	if len(runtime.scalarDefinitions) != 1 || len(runtime.scalarColumns) != 1 {
		_ = d.Close()
		t.Fatalf("created native runtime scalar state defs=%d columns=%d", len(runtime.scalarDefinitions), len(runtime.scalarColumns))
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("alpha"), []byte("beta")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"tenant":"alpha"}`),
			[]byte(`{"embedding":[0.99,0.01],"tenant":"beta"}`),
		},
	); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	filter := HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}
	if got := searchNativeScalarTest(t, col, def, filter, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "alpha" {
		_ = d.Close()
		t.Fatalf("created runtime filtered results=%+v", got.Results)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCollection, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if got := searchNativeScalarTest(t, reopenedCollection, def, filter, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "alpha" {
		t.Fatalf("reopened created runtime filtered results=%+v", got.Results)
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
	if all.Stats.ScalarFilterPlan != NativeScalarFilterPlanVectorAligned ||
		all.Stats.ScalarFilterProbeTruncated == 0 ||
		all.Stats.ScalarFilterExactScoring != 0 ||
		all.Stats.ScalarFilterCandidateIDs != nativeScalarANNSeedLimit ||
		all.Stats.ScalarFilterRetainedCandidateIDs != nativeScalarANNSeedLimit {
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

func TestNativeScalarReloadSkipsBufferedOverlayTombstones(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	def := VectorIndexDefinition{
		Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime,
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatJSON,
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes:       []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("doc")},
		[][]byte{[]byte(`{"embedding":[1,0],"tenant":"alpha"}`)},
	); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if _, err := col.CompactRootOverlays(context.Background()); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if replaced, err := col.Replace([]byte("doc"), []byte(`{"embedding":[1,0],"tenant":"beta"}`)); err != nil || !replaced {
		_ = d.Close()
		t.Fatalf("replace=%v err=%v", replaced, err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCollection, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if got := searchNativeScalarTest(t, reopenedCollection, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 1); len(got.Results) != 0 {
		t.Fatalf("reopened old buffered scalar results=%+v", got.Results)
	}
	if got := searchNativeScalarTest(t, reopenedCollection, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "beta"}, 1); len(got.Results) != 1 || string(got.Results[0].ID) != "doc" {
		t.Fatalf("reopened updated buffered scalar results=%+v", got.Results)
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

func TestNativeScalarRowSnapshotsMetadataDuringReplacement(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{
		{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString},
		{Name: "sequence_idx", Field: "sequence", ValueType: IndexValueInt64},
	})
	defer func() { _ = d.Close() }()
	idx, err := newVectorIndex(col, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = materializer.Close() }()
	definitions := append([]IndexDefinition(nil), idx.scalarDefinitions...)
	runtimes := cloneNativeScalarRuntimes(idx.scalarRuntimes)
	document := []byte(`{"embedding":[1,0],"tenant":"alpha","sequence":42}`)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 1000 {
			idx.mu.Lock()
			idx.scalarDefinitions = append([]IndexDefinition(nil), definitions...)
			idx.scalarRuntimes = cloneNativeScalarRuntimes(runtimes)
			idx.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 1000 {
			row, err := idx.nativeScalarRow(materializer, document)
			if err != nil {
				errs <- err
				return
			}
			if len(row) != 2 {
				errs <- fmt.Errorf("native scalar row=%v want two values", row)
				return
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

func TestNativeScalarAppendFailureLeavesGraphAndColumnsUnchanged(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{
		{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString},
		{Name: "sequence_idx", Field: "sequence", ValueType: IndexValueInt64},
	})
	defer func() { _ = d.Close() }()
	idx, err := newVectorIndex(col, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.insertVectorWithNativeScalarLocked(
		[]byte("doc"),
		[]float32{1, 0},
		map[string][]byte{"tenant_idx": []byte("alpha"), "sequence_idx": []byte{1}},
	); err != nil {
		t.Fatal(err)
	}
	oldNodeID := idx.currentNode["doc"]
	oldNodes := len(idx.nodes)
	tenantColumn := idx.scalarColumns["tenant_idx"]
	oldTenantOffsets := tenantColumn.rows
	delete(idx.scalarColumns, "sequence_idx")

	err = idx.insertVectorWithNativeScalarLocked(
		[]byte("doc"),
		[]float32{0, 1},
		map[string][]byte{"tenant_idx": []byte("beta"), "sequence_idx": []byte{2}},
	)
	if err == nil {
		t.Fatal("insert with unavailable later scalar column succeeded")
	}
	tenantColumn = idx.scalarColumns["tenant_idx"]
	if len(idx.nodes) != oldNodes ||
		idx.currentNode["doc"] != oldNodeID ||
		idx.nodes[oldNodeID].deleted ||
		tenantColumn.rows != oldTenantOffsets {
		t.Fatalf("failed insert mutated state: nodes=%d/%d current=%d/%d deleted=%v tenant_rows=%d/%d", len(idx.nodes), oldNodes, idx.currentNode["doc"], oldNodeID, idx.nodes[oldNodeID].deleted, tenantColumn.rows, oldTenantOffsets)
	}
}

func TestNativeScalarChunkPayloadLimitResetsAtRowBoundary(t *testing.T) {
	def := IndexDefinition{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}
	idx := &VectorIndex{
		scalarDefinitions: []IndexDefinition{def},
		scalarColumns:     map[string]vectorIndexScalarColumn{def.Name: newVectorIndexScalarColumn(def.ValueType)},
	}
	row := func(value []byte) map[string][]byte {
		return map[string][]byte{def.Name: value}
	}

	column := idx.scalarColumns[def.Name]
	column.rows = 1
	column.tailBytes = math.MaxUint32 - 1
	idx.scalarColumns[def.Name] = column
	if err := idx.validateNativeScalarRowsAppendLocked(row([]byte{1, 2})); err == nil {
		t.Fatal("same-chunk append beyond uint32 payload limit succeeded")
	}

	column.rows = nativeScalarColumnChunkRows
	column.tailBytes = math.MaxUint32
	idx.scalarColumns[def.Name] = column
	if err := idx.validateNativeScalarRowsAppendLocked(row([]byte{1, 2})); err != nil {
		t.Fatalf("next-chunk append inherited prior chunk payload: %v", err)
	}

	column.rows = nativeScalarColumnChunkRows - 1
	column.tailBytes = math.MaxUint32 - 1
	idx.scalarColumns[def.Name] = column
	if err := idx.validateNativeScalarRowsAppendLocked(row([]byte{1}), row([]byte{2, 3})); err != nil {
		t.Fatalf("batch crossing chunk boundary inherited prior chunk payload: %v", err)
	}
}

func TestNativeScalarPublicationSnapshotsOnlyMutableTail(t *testing.T) {
	column := newVectorIndexScalarColumn(IndexValueString)
	columns := map[string]vectorIndexScalarColumn{"tenant_idx": column}
	for row := 0; row < nativeScalarColumnChunkRows+44; row++ {
		column = columns["tenant_idx"]
		column.appendPrevalidated([]byte(fmt.Sprintf("tenant-%03d", row)), true)
		columns["tenant_idx"] = column
	}
	held := snapshotVectorIndexScalarColumns(columns)["tenant_idx"]
	column = columns["tenant_idx"]
	column.appendPrevalidated([]byte("tenant-new"), true)
	columns["tenant_idx"] = column
	current := snapshotVectorIndexScalarColumns(columns)["tenant_idx"]

	if held.rows != nativeScalarColumnChunkRows+44 || current.rows != held.rows+1 {
		t.Fatalf("published rows held=%d current=%d", held.rows, current.rows)
	}
	if value, ok := held.value(held.rows - 1); !ok || string(value) != fmt.Sprintf("tenant-%03d", held.rows-1) {
		t.Fatalf("held tail changed after append: value=%q present=%v", value, ok)
	}
	if _, ok := held.value(held.rows); ok {
		t.Fatal("held publication exposed a later row")
	}
	if len(held.fullChunks) != 1 || len(current.fullChunks) != 1 ||
		&held.fullChunks[0].data[0] != &current.fullChunks[0].data[0] {
		t.Fatal("immutable full scalar chunk was recopied instead of shared")
	}
	if &held.tail.data[0] == &current.tail.data[0] {
		t.Fatal("mutable scalar tail was shared across publications")
	}
}

func TestNativeScalarANNSeedsEligibleRegionBeyondGlobalFrontier(t *testing.T) {
	const (
		rows             = 5200
		eligibleStart    = 1000
		seedRegionEnd    = 1040
		bridgeEnd        = 1100
		eligibleEnd      = 5157
		topK             = 5
		efSearch         = 16
		explorationLimit = efSearch * nativeScalarANNVisitFactor
	)
	nodes := make([]vectorIndexNode, rows)
	column := newVectorIndexScalarColumn(IndexValueString)
	for row := range nodes {
		eligible := row == 0 || row >= eligibleStart && row < seedRegionEnd ||
			row >= bridgeEnd && row < eligibleEnd
		tenant := []byte("beta")
		vector := []float32{1, 0}
		if eligible {
			tenant = []byte("alpha")
			vector = []float32{0, 1}
		}
		if row >= bridgeEnd && row < eligibleEnd {
			score := 0.9 - 0.0001*float32(row-bridgeEnd)
			vector = []float32{score, float32(math.Sqrt(float64(1 - score*score)))}
		}
		normSquared := float64(vector[0]*vector[0] + vector[1]*vector[1])
		nodes[row] = vectorIndexNode{
			documentID:    []byte(fmt.Sprintf("doc-%05d", row)),
			vector:        vector,
			normSquared:   normSquared,
			cachedInvNorm: float32(1 / math.Sqrt(normSquared)),
			neighbors:     make([][]vectorIndexNeighbor, 1),
		}
		column.appendPrevalidated(tenant, true)
	}
	for row := 0; row+1 < eligibleStart; row++ {
		nodes[row].neighbors[0] = append(nodes[row].neighbors[0], vectorIndexNeighbor{nodeID: row + 1})
		nodes[row+1].neighbors[0] = append(nodes[row+1].neighbors[0], vectorIndexNeighbor{nodeID: row})
	}
	for row := eligibleStart; row+1 < eligibleEnd; row++ {
		nodes[row].neighbors[0] = append(nodes[row].neighbors[0], vectorIndexNeighbor{nodeID: row + 1})
		nodes[row+1].neighbors[0] = append(nodes[row+1].neighbors[0], vectorIndexNeighbor{nodeID: row})
	}
	columns := map[string]vectorIndexScalarColumn{"tenant_idx": column}
	plan := &nativeScalarFilterExecution{
		identity: NativeScalarFilterPlanVectorAligned,
		clauses: []nativeScalarClause{{
			indexName: "tenant_idx", lower: []byte("alpha"), upper: []byte("alpha"),
			lowerInclusive: true, upperInclusive: true,
		}},
	}
	matcher, err := bindNativeScalarMatcher(plan, columns, len(nodes), "base")
	if err != nil {
		t.Fatal(err)
	}
	query := []float32{1, 0}
	view := &vectorIndexSearchView{
		metric: VectorMetricCosine, encoding: VectorIndexEncodingFloat32,
		dimensions: 2, m: 8, efSearch: efSearch,
	}
	runtime := VectorIndex{
		metric: VectorMetricCosine, encoding: VectorIndexEncodingFloat32,
		dimensions: 2, m: 8, efSearch: efSearch,
		nodes: nodes, entry: 0,
	}

	var ordinaryScratch vectorIndexSearchScratch
	ordinary := runtime.searchCandidatesLocked(query, 1, nil, explorationLimit, &ordinaryScratch)
	for _, candidate := range ordinary {
		node := &nodes[candidate.nodeID]
		if matcher.matches(candidate.nodeID, node.documentID) {
			t.Fatalf("ordinary bounded global frontier unexpectedly reached eligible row %d", candidate.nodeID)
		}
	}

	var scratch vectorIndexSearchScratch
	var results []VectorIndexSearchResult
	var resultIDBytes []byte
	seedBudget := nativeScalarSeedBudget{
		rows: nativeScalarANNSeedProbeLimit, scores: nativeScalarANNSeedLimit, planesRemaining: 1,
	}
	got, work, err := searchVectorIndexViewPlaneNativeScalar(
		query, 1, nil, topK, efSearch,
		nodes, 0, 0, &matcher, nil, plan, view, false,
		&seedBudget, &scratch, &results, &resultIDBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != topK {
		t.Fatalf("filtered result count=%d want=%d work=%+v", len(got), topK, work)
	}
	for rank, result := range got {
		want := fmt.Sprintf("doc-%05d", bridgeEnd+rank)
		if string(result.ID) != want {
			t.Fatalf("rank %d id=%q want=%q results=%+v", rank, result.ID, want, got)
		}
		nodeID := bridgeEnd + rank
		if !matcher.matches(nodeID, nodes[nodeID].documentID) {
			t.Fatalf("rank %d leaked nonmatching tenant id=%q", rank, result.ID)
		}
	}
	if work.eligibleSeeds <= 0 || work.eligibleSeeds > nativeScalarANNSeedLimit {
		t.Fatalf("eligible seeds=%d want 1..%d", work.eligibleSeeds, nativeScalarANNSeedLimit)
	}
	if work.seedRowsVisited > nativeScalarANNSeedProbeLimit ||
		work.scored > explorationLimit ||
		work.visited != work.seedRowsVisited+work.scored ||
		work.admitted != topK || work.underfill {
		t.Fatalf("unbounded or dishonest scalar ANN work=%+v score_limit=%d", work, explorationLimit)
	}
}

func TestNativeScalarANNReducedSeedBudgetSpansWholeGraph(t *testing.T) {
	const rows = 5200
	nodes := make([]vectorIndexNode, rows)
	column := newVectorIndexScalarColumn(IndexValueString)
	for row := range nodes {
		tenant := []byte("beta")
		vector := []float32{0, 1}
		if row < 650 || row >= 3000 && row < 3300 {
			tenant = []byte("alpha")
		}
		if row >= 3000 && row < 3300 {
			vector = []float32{1, 0}
		}
		nodes[row] = vectorIndexNode{
			documentID: []byte(fmt.Sprintf("doc-%05d", row)), vector: vector,
			normSquared: 1, cachedInvNorm: 1, neighbors: make([][]vectorIndexNeighbor, 1),
		}
		column.appendPrevalidated(tenant, true)
	}
	plan := &nativeScalarFilterExecution{
		identity: NativeScalarFilterPlanVectorAligned,
		clauses: []nativeScalarClause{{
			indexName: "tenant_idx", lower: []byte("alpha"), upper: []byte("alpha"),
			lowerInclusive: true, upperInclusive: true,
		}},
	}
	view := &vectorIndexSearchView{
		metric: VectorMetricCosine, encoding: VectorIndexEncodingFloat32,
		dimensions: 2, m: 8, efSearch: 1,
	}
	budget := nativeScalarSeedBudget{
		rows: nativeScalarANNSeedProbeLimit, scores: nativeScalarANNSeedLimit, planesRemaining: 1,
	}
	var scratch vectorIndexSearchScratch
	var results []VectorIndexSearchResult
	var ids []byte
	matcher := nativeScalarBoundMatcher{plan: plan, columns: [4]vectorIndexScalarColumn{column}}
	got, work, err := searchVectorIndexViewPlaneNativeScalar(
		[]float32{1, 0}, 1, nil, 1, 1, nodes, 0, 0,
		&matcher, nil,
		plan, view, false, &budget, &scratch, &results, &ids,
	)
	if err != nil || len(got) != 1 {
		t.Fatalf("reduced-budget search results=%+v work=%+v err=%v", got, work, err)
	}
	if id := string(got[0].ID); id < "doc-03000" || id >= "doc-03300" {
		t.Fatalf("reduced seed budget stayed in early buckets: id=%q work=%+v", id, work)
	}
	if work.eligibleSeeds > nativeScalarANNVisitFactor/4 {
		t.Fatalf("tiny-budget seed scores exceeded reserved share: %+v", work)
	}
}

func TestNativeScalarANNSeedBudgetSharedAcrossViewPlanes(t *testing.T) {
	const rows = 5200
	buildPlane := func(prefix string, perfect bool) ([]vectorIndexNode, vectorIndexScalarColumn) {
		nodes := make([]vectorIndexNode, rows)
		column := newVectorIndexScalarColumn(IndexValueString)
		for row := range nodes {
			tenant := []byte("beta")
			vector := []float32{0, 1}
			if row >= 1000 {
				tenant = []byte("alpha")
				if perfect {
					vector = []float32{1, 0}
				}
			}
			nodes[row] = vectorIndexNode{
				documentID: []byte(fmt.Sprintf("%s-%05d", prefix, row)), vector: vector,
				normSquared: 1, cachedInvNorm: 1, neighbors: make([][]vectorIndexNeighbor, 1),
			}
			column.appendPrevalidated(tenant, true)
		}
		return nodes, column
	}
	baseNodes, baseColumn := buildPlane("base", false)
	deltaNodes, deltaColumn := buildPlane("delta", true)
	view := &vectorIndexSearchView{
		name: "shared-seed-budget", sourceDocumentRootsValid: true,
		metric: VectorMetricCosine, encoding: VectorIndexEncodingFloat32,
		dimensions: 2, m: 8, efSearch: 16,
		nodes: baseNodes, entry: 0,
		scalarColumns: map[string]vectorIndexScalarColumn{"tenant_idx": baseColumn},
		deltaNodes:    deltaNodes, deltaEntry: 0,
		deltaScalarColumns: map[string]vectorIndexScalarColumn{"tenant_idx": deltaColumn},
	}
	plan := &nativeScalarFilterExecution{
		identity: NativeScalarFilterPlanVectorAligned,
		clauses: []nativeScalarClause{{
			indexName: "tenant_idx", lower: []byte("alpha"), upper: []byte("alpha"),
			lowerInclusive: true, upperInclusive: true,
		}},
	}
	var buffer VectorIndexSearchBuffer
	got, work, err := view.searchWithNativeScalarFilterBuffer([]float32{1, 0}, 2, 16, plan, &buffer)
	if err != nil || len(got) != 2 {
		t.Fatalf("two-plane seeded search results=%+v work=%+v err=%v", got, work, err)
	}
	for _, result := range got {
		if !bytes.HasPrefix(result.ID, []byte("delta-")) {
			t.Fatalf("base plane exhausted shared seed rows before delta search: results=%+v work=%+v", got, work)
		}
	}
	if work.seedRowsVisited > nativeScalarANNSeedProbeLimit ||
		work.eligibleSeeds > nativeScalarANNSeedLimit {
		t.Fatalf("two-plane search exceeded query-wide seed caps: %+v", work)
	}
}

func TestNativeScalarANNReservesLayerZeroExpansion(t *testing.T) {
	const rows = 17
	nodes := make([]vectorIndexNode, rows)
	column := newVectorIndexScalarColumn(IndexValueString)
	for row := range nodes {
		vector := []float32{float32(row + 1), 1}
		if row == rows-1 {
			vector = []float32{100, 0}
		}
		normSquared := float64(vector[0]*vector[0] + vector[1]*vector[1])
		nodes[row] = vectorIndexNode{
			documentID: []byte(fmt.Sprintf("doc-%02d", row)), vector: vector,
			normSquared: normSquared, cachedInvNorm: float32(1 / math.Sqrt(normSquared)),
			neighbors: make([][]vectorIndexNeighbor, 2),
		}
		tenant := []byte("beta")
		if row == rows-1 {
			tenant = []byte("alpha")
		}
		column.appendPrevalidated(tenant, true)
	}
	for row := range 15 {
		nodes[row].neighbors[1] = []vectorIndexNeighbor{{nodeID: row + 1}}
	}
	nodes[13].neighbors[0] = []vectorIndexNeighbor{{nodeID: rows - 1}}
	nodes[14].neighbors[0] = []vectorIndexNeighbor{{nodeID: rows - 1}}
	plan := &nativeScalarFilterExecution{
		identity: NativeScalarFilterPlanVectorAligned,
		clauses: []nativeScalarClause{{
			indexName: "tenant_idx", lower: []byte("alpha"), upper: []byte("alpha"),
			lowerInclusive: true, upperInclusive: true,
		}},
	}
	view := &vectorIndexSearchView{
		metric: VectorMetricCosine, encoding: VectorIndexEncodingFloat32,
		dimensions: 2, m: 8, efSearch: 1,
	}
	budget := nativeScalarSeedBudget{rows: nativeScalarANNSeedProbeLimit, planesRemaining: 1}
	var scratch vectorIndexSearchScratch
	var results []VectorIndexSearchResult
	var ids []byte
	matcher := nativeScalarBoundMatcher{plan: plan, columns: [4]vectorIndexScalarColumn{column}}
	got, work, err := searchVectorIndexViewPlaneNativeScalar(
		[]float32{1, 0}, 1, nil, 1, 1, nodes, 0, 1,
		&matcher, nil,
		plan, view, false, &budget, &scratch, &results, &ids,
	)
	if err != nil || len(got) != 1 || string(got[0].ID) != "doc-16" {
		t.Fatalf("layer-zero navigation was not reserved: results=%+v work=%+v err=%v", got, work, err)
	}
	if work.scored > nativeScalarANNVisitFactor {
		t.Fatalf("layer-zero reservation exceeded score budget: %+v", work)
	}
}

func TestNativeScalarANNReusesSearchBufferScratch(t *testing.T) {
	idx, query := newNativeScalarExecutorBenchmarkIndex(t, 2, 8192)
	view := idx.searchView.Load()
	plan := &nativeScalarFilterExecution{
		identity:         NativeScalarFilterPlanVectorAligned,
		sourceGeneration: view.sourceDocumentGeneration,
	}
	var buffer VectorIndexSearchBuffer
	results, _, work, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 64, plan, &buffer)
	if err != nil || len(results) != 10 {
		t.Fatalf("warm filtered ANN results=%d err=%v", len(results), err)
	}
	if work.seedRowsVisited <= 0 || work.seedRowsVisited > nativeScalarANNSeedProbeLimit ||
		work.eligibleSeeds <= 0 || work.eligibleSeeds > nativeScalarANNSeedLimit ||
		work.visited != work.seedRowsVisited+work.scored {
		t.Fatalf("all-match route reported unbounded or dishonest seeded work: %+v", work)
	}
	var tinyBuffer VectorIndexSearchBuffer
	tinyResults, _, tinyWork, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 1, 1, plan, &tinyBuffer)
	if err != nil || len(tinyResults) != 1 || string(tinyResults[0].ID) != "doc-00000" {
		t.Fatalf("tiny-budget all-match results=%+v err=%v work=%+v", tinyResults, err, tinyWork)
	}
	if tinyWork.scored > nativeScalarANNVisitFactor || tinyWork.eligibleSeeds > nativeScalarANNVisitFactor/4 {
		t.Fatalf("tiny-budget seeded work exceeded reserved navigation bound: %+v", tinyWork)
	}
	visited := &buffer.nativeSearchScratch.visitedEpochs[0]
	allocs := testing.AllocsPerRun(100, func() {
		results, _, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 64, plan, &buffer)
		if err != nil || len(results) != 10 {
			panic(fmt.Sprintf("filtered ANN results=%d err=%v", len(results), err))
		}
	})
	if allocs != 0 {
		t.Fatalf("filtered ANN steady-state allocs=%v want 0", allocs)
	}
	if &buffer.nativeSearchScratch.visitedEpochs[0] != visited {
		t.Fatal("filtered ANN replaced warmed visited scratch")
	}
}

func TestNativeScalarVectorAlignedPlanServesPublishedGenerationDuringNewerProbe(t *testing.T) {
	idx, query := newNativeScalarExecutorBenchmarkIndex(t, 2, 32)
	view := idx.searchView.Load()
	plan := &nativeScalarFilterExecution{
		identity:         NativeScalarFilterPlanVectorAligned,
		sourceGeneration: view.sourceDocumentGeneration + 1,
	}
	var buffer VectorIndexSearchBuffer
	results, state, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 16, plan, &buffer)
	if err != nil {
		t.Fatalf("vector-aligned search with newer probe generation: %v", err)
	}
	if len(results) != 10 || state.sourceDocumentGeneration != view.sourceDocumentGeneration {
		t.Fatalf("results=%d state generation=%d want 10 results from generation %d", len(results), state.sourceDocumentGeneration, view.sourceDocumentGeneration)
	}

	plan.identity = NativeScalarFilterPlanCompleteFinite
	if _, _, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 16, plan, &buffer); !errors.Is(err, ErrHybridSearchStaleIndex) {
		t.Fatalf("finite plan generation mismatch err=%v want ErrHybridSearchStaleIndex", err)
	}
}

func BenchmarkNativeScalarRowCachedRuntimes(b *testing.B) {
	d, col, def := newNativeScalarTestCollection(b, []IndexDefinition{
		{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString},
		{Name: "sequence_idx", Field: "sequence", ValueType: IndexValueInt64},
	})
	defer func() { _ = d.Close() }()
	idx, err := newVectorIndex(col, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		b.Fatal(err)
	}
	if len(idx.scalarRuntimes) != 2 {
		b.Fatalf("cached scalar runtimes=%d want=2", len(idx.scalarRuntimes))
	}
	firstRuntime := &idx.scalarRuntimes[0]
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = materializer.Close() }()
	document := []byte(`{"embedding":[1,0],"tenant":"alpha","sequence":42}`)
	b.ReportAllocs()
	for b.Loop() {
		row, err := idx.nativeScalarRow(materializer, document)
		if err != nil || len(row) != 2 {
			b.Fatalf("row=%v err=%v", row, err)
		}
	}
	if &idx.scalarRuntimes[0] != firstRuntime {
		b.Fatal("native scalar runtime cache was replaced during document extraction")
	}
	b.ReportMetric(1, "runtime_builds/setup")
}

func BenchmarkNativeScalarSingleRowPublicationSnapshot(b *testing.B) {
	column := newVectorIndexScalarColumn(IndexValueString)
	columns := map[string]vectorIndexScalarColumn{"tenant_idx": column}
	value := []byte("alpha")
	for range 8192 {
		column = columns["tenant_idx"]
		column.appendPrevalidated(value, true)
		columns["tenant_idx"] = column
	}
	b.ReportAllocs()
	for b.Loop() {
		column = columns["tenant_idx"]
		column.appendPrevalidated(value, true)
		columns["tenant_idx"] = column
		published := snapshotVectorIndexScalarColumns(columns)
		if published["tenant_idx"].rows != column.rows {
			b.Fatal("published scalar row count mismatch")
		}
	}
}

func BenchmarkNativeScalarANNBufferReuse(b *testing.B) {
	idx, query := newNativeScalarExecutorBenchmarkIndex(b, 2, 8192)
	view := idx.searchView.Load()
	plan := &nativeScalarFilterExecution{
		identity:         NativeScalarFilterPlanVectorAligned,
		sourceGeneration: view.sourceDocumentGeneration,
	}
	var buffer VectorIndexSearchBuffer
	if results, _, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 64, plan, &buffer); err != nil || len(results) != 10 {
		b.Fatalf("warm filtered ANN results=%d err=%v", len(results), err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		results, _, _, err := idx.searchGraphOnlyWithNativeScalarFilterBuffer(query, 10, 64, plan, &buffer)
		if err != nil || len(results) != 10 {
			b.Fatalf("filtered ANN results=%d err=%v", len(results), err)
		}
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

func newNativeScalarExecutorBenchmarkIndex(tb testing.TB, dimensions, graphRows int) (*VectorIndex, []float32) {
	tb.Helper()
	idx, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "native_scalar_crossover", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: dimensions, M: 8, EfConstruction: 64, EfSearch: 64,
	})
	if err != nil {
		tb.Fatal(err)
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
			tb.Fatal(err)
		}
	}
	idx.sourceDocumentGeneration = 1
	idx.sourceDocumentRootsValid = true
	idx.publishSearchViewLocked(true)
	return idx, query
}

func newNativeScalarTestCollection(tb testing.TB, indexes []IndexDefinition) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	return newNativeScalarTestCollectionWithFormat(tb, DocumentFormatJSON, indexes)
}

func newNativeScalarTestCollectionWithFormat(tb testing.TB, documentFormat DocumentFormat, indexes []IndexDefinition) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatal(err)
	}
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 8, EfSearch: 64, Strategy: VectorIndexStrategyNativeRuntime}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: documentFormat, AllowArrayValuesInIndex: true}, Indexes: indexes, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
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
