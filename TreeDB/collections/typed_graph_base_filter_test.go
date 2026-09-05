package collections

import (
	"errors"
	"math"
	"slices"
	"testing"
)

func TestTypedGraphBaseFilterBindingThresholdAndReuse(t *testing.T) {
	const n = 5000
	col, base, ids, retained, columns, ranks := openTypedGraphQualityFixture(t, n)
	filter := HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "04096", Inclusive: true}}}
	cold, err := prepareTypedGraphBaseFilter(base, filter, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 10000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 20000}, Clauses: 8, PredicateBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	// The plan must not retain the caller's mutable range object.
	filter.Range.Upper.Value = "absent"
	var removed int
	for i, rank := range ranks {
		if rank == 4096 {
			removed = i
			break
		}
	}
	if err := col.Delete(ids[removed]); err != nil {
		t.Fatal(err)
	}
	current, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 32, Tombstones: 16, Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := bindTypedGraphBaseFilter(cold, overlay, typedGraphFilterBindLimits{Rows: 32, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 5000})
	if err != nil || bound.count != 4096 {
		t.Fatalf("4097 ->4096 binding: plan=%+v err=%v", bound, err)
	}
	oldRows, boundRows := cold.plan.base.SparseRows(), bound.base.SparseRows()
	if len(oldRows) != 4097 || len(boundRows) != len(oldRows) || &oldRows[0] != &boundRows[0] {
		t.Fatal("binding copied/subtracted the immutable base selection")
	}
	var buffer VectorIndexSearchBuffer
	results, stats, err := overlay.searchPreparedFilter(bound, vectorBenchmarkEmbedding(19, 8), 10, 256, 8192, &buffer)
	if err != nil || len(results) != 10 || !stats.FilteredExact || stats.ExactBaseScored != 4096 {
		t.Fatalf("bound exact route: results=%d stats=%+v err=%v", len(results), stats, err)
	}
	for _, result := range results {
		if string(result.ID) == string(ids[removed]) {
			t.Fatal("deleted base row leaked")
		}
	}
	limits := typedGraphFilterBindLimits{Rows: 32, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 5000}
	if bound.sourceIDs != 1 || bound.sourceBytes != len(ids[removed]) || bound.inspectedEntries != 0 || cold.plan.sourceIDs != 4097 || bound.exactScanRows != 4097 {
		t.Fatalf("binding repeated cold posting work: cold=%+v bound=%+v", cold.plan, bound)
	}
	for _, mutate := range []func(*typedGraphFilterBindLimits){
		func(l *typedGraphFilterBindLimits) { l.IDBytes = 1 },
		func(l *typedGraphFilterBindLimits) { l.MappingWork = 1 },
		func(l *typedGraphFilterBindLimits) { l.RetainedBytes = 1 },
		func(l *typedGraphFilterBindLimits) { l.ExactScanRows = 4096 },
	} {
		limited := limits
		mutate(&limited)
		if p, err := bindTypedGraphBaseFilter(cold, overlay, limited); !errors.Is(err, errTypedGraphSearchBudget) || p != nil {
			t.Fatalf("binding budget returned partial plan=%+v err=%v", p, err)
		}
	}
	// Reinsert the same ID with opposite vector: the old best base candidate
	// must be excluded, and current typed data returns the row for its new query.
	vector := slices.Clone(columns[0].Float32Vectors[removed])
	for i := range vector {
		vector[i] = -vector[i]
	}
	row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{vector}}, {Name: "content", Strings: []string{"current content"}}, {Name: "user", Strings: []string{columns[2].Strings[removed]}}, {Name: "path", Strings: []string{"current path"}}}
	if _, _, err := col.InsertTypedBatchWithStats(ids[removed:removed+1], retained[removed:removed+1], row); err != nil {
		t.Fatal(err)
	}
	newCurrent, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer newCurrent.Close()
	newOverlay, err := prepareTypedGraphOverlaySearch(base, newCurrent, typedGraphOverlayLimits{Rows: 32, Tombstones: 16, Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	newBound, err := bindTypedGraphBaseFilter(cold, newOverlay, limits)
	if err != nil || newBound.count != 4097 || len(newBound.excludedBase) != 1 || len(newBound.delta) != 1 || newBound.sourceIDs != 1 {
		t.Fatalf("reinsert binding=%+v err=%v", newBound, err)
	}
	results, stats, err = newOverlay.searchPreparedFilter(newBound, columns[0].Float32Vectors[removed], 10, 256, 8192, &buffer)
	if err != nil || len(results) != 10 || stats.FilteredExact || stats.Base.Candidates == 0 || stats.Base.Edges == 0 || stats.BaseResultIDs != 11 || stats.BaseShadowed != 1 {
		t.Fatalf("ANN shadow overfetch n=%d stats=%+v err=%v", len(results), stats, err)
	}
	for _, result := range results {
		if string(result.ID) == string(ids[removed]) {
			t.Fatal("old best vector leaked through replacement")
		}
	}
	results, _, err = newOverlay.searchPreparedFilter(newBound, vector, 10, 256, 8192, &buffer)
	if err != nil || len(results) != 10 || string(results[0].ID) != string(ids[removed]) {
		t.Fatalf("current vector not used: results=%+v err=%v", results, err)
	}
	fetched, err := newCurrent.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(fetched.Results) != 10 {
		t.Fatalf("current materialization: %+v", fetched)
	}
	if _, _, err := newOverlay.searchPreparedFilter(newBound, vector, 10, 256, 10, &buffer); !errors.Is(err, errTypedGraphSearchBudget) || len(buffer.results) != 0 {
		t.Fatalf("overfetch cap returned partial result: %v", err)
	}
	if _, _, err := newOverlay.searchPreparedFilter(bound, vector, 10, 256, 8192, &buffer); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || len(buffer.results) != 0 {
		t.Fatalf("wrong current binding accepted: %v", err)
	}
	// The old current pin and exact plan remain stable after the later insert.
	if _, oldStats, err := overlay.searchPreparedFilter(bound, vector, 10, 256, 8192, &buffer); err != nil || !oldStats.FilteredExact || bound.count != 4096 {
		t.Fatalf("old binding changed: %+v %v", oldStats, err)
	}
	t.Logf("cold source IDs=%d bind IDs=%d ANN IDs=%d exact enumeration=%d", cold.plan.sourceIDs, newBound.sourceIDs, stats.BaseResultIDs, bound.exactScanRows)
	all, err := prepareTypedGraphBaseFilter(base, HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "04999", Inclusive: true}}}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 10000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 20000}, Clauses: 8, PredicateBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	allBound, err := bindTypedGraphBaseFilter(all, newOverlay, limits)
	if err != nil || all.plan.sourceIDs != 5000 || allBound.sourceIDs != newBound.sourceIDs || allBound.inspectedEntries != 0 || allBound.exactScanRows != 0 || !allBound.base.IsAll() {
		t.Fatalf("fixed D binding scaled with base eligibility: plan=%+v err=%v", allBound, err)
	}
	changedCurrent := *newCurrent
	changedCatalog := *newCurrent.catalog
	changedCatalog.meta.Indexes = slices.Clone(changedCatalog.meta.Indexes)
	changedCatalog.meta.Indexes[0].Field = "different"
	changedCurrent.catalog = &changedCatalog
	changedOverlay := *newOverlay
	changedOverlay.current = &changedCurrent
	if p, err := bindTypedGraphBaseFilter(cold, &changedOverlay, limits); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || p != nil {
		t.Fatalf("changed scalar definition accepted: %+v %v", p, err)
	}
}

func TestTypedGraphBaseFilterStringBindingParity(t *testing.T) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 64)
	coldLimits := typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 1000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 2000}, Clauses: 8, PredicateBytes: 1024}
	bindLimits := typedGraphFilterBindLimits{Rows: 16, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 1000}
	cases := []struct {
		name   string
		filter HybridScalarFilter
		want   int
	}{
		{"nul_equality_delta_only", HybridScalarFilter{IndexName: "user", Value: "\x00z"}, 1},
		{"lower_unbounded", HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Value: "\x00z", Inclusive: true}}}, 1},
		{"upper_unbounded", HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "\x00", Inclusive: true}, Upper: IndexRangeBound{Unbounded: true}}}, 64},
		{"exclusive_empty", HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Value: "\x00z"}}}, 0},
		{"inverted", HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "z", Inclusive: true}, Upper: IndexRangeBound{Value: "a", Inclusive: true}}}, 0},
		{"empty_string", HybridScalarFilter{IndexName: "user", Value: ""}, 0},
		{"leaves_range", HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "00063", Inclusive: true}}}, 63},
	}
	cold := make([]*typedGraphBaseFilter, len(cases))
	for i, tc := range cases {
		var err error
		cold[i], err = prepareTypedGraphBaseFilter(base, tc.filter, coldLimits)
		if err != nil {
			t.Fatal(tc.name, err)
		}
	}
	row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"current"}}, {Name: "user", Strings: []string{"\x00z"}}, {Name: "path", Strings: []string{"path"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], row); err != nil {
		t.Fatal(err)
	}
	current, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 16, Tombstones: 8, Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// prepareRows normalizes decoder-borrowed StringBytes into owned String.
	if len(overlay.rows) != 1 || overlay.rows[0].Values[2].StringBytes != nil || overlay.rows[0].Values[2].String != "\x00z" {
		t.Fatal("overlay string normalization invariant missing")
	}
	query := vectorBenchmarkEmbedding(1, 8)
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := bindTypedGraphBaseFilter(cold[i], overlay, bindLimits)
			if err != nil || bound.count != tc.want {
				t.Fatalf("count plan=%+v err=%v", bound, err)
			}
			fresh, err := prepareTypedGraphFilter(overlay, tc.filter, coldLimits.typedGraphFilterLimits)
			if err != nil {
				t.Fatal(err)
			}
			var a, b VectorIndexSearchBuffer
			got, stats, err := overlay.searchPreparedFilter(bound, query, 4, 16, 128, &a)
			if err != nil || !stats.FilteredExact {
				t.Fatal(stats, err)
			}
			want, _, err := overlay.searchPreparedFilter(fresh, query, 4, 16, 128, &b)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Fatalf("got=%+v want=%+v", got, want)
			}
			for j := range got {
				if string(got[j].ID) != string(want[j].ID) || math.Abs(got[j].Score-want[j].Score) > 1e-6 {
					t.Fatalf("got=%+v want=%+v", got, want)
				}
			}
		})
	}
	for _, limit := range []typedGraphBaseFilterLimits{
		{typedGraphFilterLimits: coldLimits.typedGraphFilterLimits, Clauses: 1, PredicateBytes: 1},
		{typedGraphFilterLimits: coldLimits.typedGraphFilterLimits, Clauses: 0, PredicateBytes: 1024},
	} {
		if p, err := prepareTypedGraphBaseFilter(base, cases[0].filter, limit); !errors.Is(err, errTypedGraphSearchBudget) || p != nil {
			t.Fatalf("cold predicate bounds: %+v %v", p, err)
		}
	}
	for _, mutate := range []func(*typedGraphFilterBindLimits){func(l *typedGraphFilterBindLimits) { l.ValueBytes = 1 }, func(l *typedGraphFilterBindLimits) { l.PredicateWork = 0 }} {
		limited := bindLimits
		mutate(&limited)
		if p, err := bindTypedGraphBaseFilter(cold[0], overlay, limited); !errors.Is(err, errTypedGraphSearchBudget) || p != nil {
			t.Fatalf("predicate bound: %+v %v", p, err)
		}
	}
}
