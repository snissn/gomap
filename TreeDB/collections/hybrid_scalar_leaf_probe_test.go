package collections

import (
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestHybridScalarLeafProbeParity4475(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"tags":["a","b"]}`),
			[]byte(`{"tags":["a"]}`),
			[]byte(`{"tags":["b"]}`),
			[]byte(`{"tags":["z"]}`),
		},
	); err != nil {
		t.Fatalf("insert persisted documents: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := col.Insert([]byte("e"), []byte(`{"tags":["a","b"]}`)); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}

	cases := []struct {
		name   string
		filter HybridScalarFilter
		limit  int
	}{
		{name: "single", filter: HybridScalarFilter{IndexName: "tag", Value: "z"}, limit: 10},
		{name: "mixed_buffered_and_persisted", filter: HybridScalarFilter{IndexName: "tag", Value: "a"}, limit: 10},
		{name: "deduplicated_range", filter: HybridScalarFilter{IndexName: "tag", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: "a", Inclusive: true},
			Upper: IndexRangeBound{Value: "b", Inclusive: true},
		}}, limit: 10},
		{name: "truncated", filter: HybridScalarFilter{IndexName: "tag", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: "a", Inclusive: true},
			Upper: IndexRangeBound{Value: "b", Inclusive: true},
		}}, limit: 2},
		{name: "empty", filter: HybridScalarFilter{IndexName: "tag", Value: "missing"}, limit: 10},
	}
	type expectedProbe struct {
		ids       [][]byte
		truncated bool
	}
	expected := make([]expectedProbe, len(cases))
	for i, tc := range cases {
		if tc.filter.Range != nil {
			opts := *tc.filter.Range
			opts.Limit = tc.limit
			expected[i].ids, expected[i].truncated, err = col.FindByIndexRange(tc.filter.IndexName, opts)
		} else {
			expected[i].ids, expected[i].truncated, err = col.FindByIndexValueLimit(tc.filter.IndexName, tc.filter.Value, tc.limit)
		}
		if err != nil {
			t.Fatalf("%s reference lookup: %v", tc.name, err)
		}
	}

	view, err := col.openHybridScalarLookupView()
	if err != nil {
		t.Fatalf("open scalar lookup view: %v", err)
	}
	var ownedAfterClose hybridScalarAllowSet
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, inputIDs, truncated, err := view.leafProbe(tc.filter, tc.limit)
			if err != nil {
				t.Fatalf("leafProbe: %v", err)
			}
			if inputIDs != uint64(len(expected[i].ids)) || truncated != expected[i].truncated || len(set) != len(expected[i].ids) {
				t.Fatalf("set=%v input_ids=%d truncated=%v want ids=%q input_ids=%d truncated=%v", set, inputIDs, truncated, expected[i].ids, len(expected[i].ids), expected[i].truncated)
			}
			for _, id := range expected[i].ids {
				if _, ok := set[string(id)]; !ok {
					t.Fatalf("set=%v missing reference id %q", set, id)
				}
			}
			if tc.name == "mixed_buffered_and_persisted" {
				ownedAfterClose = set
			}
		})
	}
	set, inputIDs, truncated, err := view.leafProbe(HybridScalarFilter{IndexName: "missing", Value: "a"}, 10)
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || set != nil || inputIDs != 0 || truncated {
		t.Fatalf("missing index set=%v input_ids=%d truncated=%v err=%v", set, inputIDs, truncated, err)
	}
	view.close()
	if _, ok := ownedAfterClose["e"]; !ok {
		t.Fatalf("borrowed probe result did not retain its owned ID after closing the snapshot: %v", ownedAfterClose)
	}

	if _, err := col.Insert([]byte("f"), []byte(`{"tags":["a"]}`)); err != nil {
		t.Fatalf("insert next-generation document: %v", err)
	}
	nextView, err := col.openHybridScalarLookupView()
	if err != nil {
		t.Fatalf("open next scalar lookup view: %v", err)
	}
	defer nextView.close()
	next, nextInputIDs, nextTruncated, err := nextView.leafProbe(HybridScalarFilter{IndexName: "tag", Value: "a"}, 10)
	if err != nil || nextTruncated || nextInputIDs != uint64(len(ownedAfterClose)+1) || len(next) != len(ownedAfterClose)+1 {
		t.Fatalf("next-generation set=%v input_ids=%d truncated=%v err=%v", next, nextInputIDs, nextTruncated, err)
	}
	if _, ok := next["f"]; !ok {
		t.Fatalf("next-generation set=%v missing f", next)
	}
}

var hybridScalarLeafProbeBenchmarkSink4475 hybridScalarAllowSet

func BenchmarkHybridScalarLeafProbe4475(b *testing.B) {
	const documents = 1024
	rows := make([]hybridSearchExecutorFixtureRow2505, documents)
	for i := range rows {
		rows[i] = hybridSearchExecutorFixtureRow2505{
			id:    fmt.Sprintf("doc-%04d", i),
			title: "probe",
			body:  "probe",
			city:  "benchmark",
			score: int64(i),
		}
	}
	_, d, col := openHybridScalarSearchExecutorFixture2505(b, rows)
	defer func() { _ = d.Close() }()
	view, err := col.openHybridScalarLookupView()
	if err != nil {
		b.Fatalf("open scalar lookup view: %v", err)
	}
	defer view.close()
	filter := HybridScalarFilter{IndexName: "city", Value: "benchmark"}

	set, inputIDs, truncated, err := view.leafProbe(filter, documents)
	if err != nil || truncated || inputIDs != documents || len(set) != documents {
		b.Fatalf("warm leafProbe set=%d input_ids=%d truncated=%v err=%v", len(set), inputIDs, truncated, err)
	}

	b.ReportAllocs()
	for b.Loop() {
		set, inputIDs, truncated, err = view.leafProbe(filter, documents)
		if err != nil || truncated || inputIDs != documents || len(set) != documents {
			b.Fatalf("leafProbe set=%d input_ids=%d truncated=%v err=%v", len(set), inputIDs, truncated, err)
		}
	}
	hybridScalarLeafProbeBenchmarkSink4475 = set
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
	b.ReportMetric(float64(inputIDs), "input_ids/op")
}

func TestHybridScalarAllowSetProbeCacheHits4476(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"tags":["a","b"]}`),
			[]byte(`{"tags":["a"]}`),
			[]byte(`{"tags":["z"]}`),
		},
	); err != nil {
		t.Fatalf("insert documents: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	plan := hybridSearchExecutionPlan{
		scalarFilter:      &HybridScalarFilter{IndexName: "tag", Value: "a"},
		scalarLookupLimit: 10,
		topK:              10,
	}
	set1, stats1, err := col.hybridScalarAllowSet(plan)
	if err != nil {
		t.Fatalf("first allow set: %v", err)
	}
	if len(set1) != 2 {
		t.Fatalf("first set=%v want 2 ids", set1)
	}
	hits, misses := col.scalarProbeCacheStats()
	if hits != 0 || misses != 1 {
		t.Fatalf("after first probe hits=%d misses=%d want 0/1", hits, misses)
	}

	set2, stats2, err := col.hybridScalarAllowSet(plan)
	if err != nil {
		t.Fatalf("second allow set: %v", err)
	}
	if len(set2) != len(set1) {
		t.Fatalf("second set=%v first set=%v", set2, set1)
	}
	for id := range set1 {
		if _, ok := set2[id]; !ok {
			t.Fatalf("second set missing id %q", id)
		}
	}
	if stats2.ScalarFilterInputIDs != stats1.ScalarFilterInputIDs {
		t.Fatalf("replayed input ids %d != %d", stats2.ScalarFilterInputIDs, stats1.ScalarFilterInputIDs)
	}
	hits, misses = col.scalarProbeCacheStats()
	if hits != 1 || misses != 1 {
		t.Fatalf("after repeat probe hits=%d misses=%d want 1/1", hits, misses)
	}

	if _, err := col.Insert([]byte("d"), []byte(`{"tags":["a"]}`)); err != nil {
		t.Fatalf("insert new document: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush after insert: %v", err)
	}
	set3, _, err := col.hybridScalarAllowSet(plan)
	if err != nil {
		t.Fatalf("third allow set: %v", err)
	}
	if len(set3) != 3 {
		t.Fatalf("after insert set=%v want 3 ids (stale cache?)", set3)
	}
	hits, misses = col.scalarProbeCacheStats()
	if hits != 1 || misses != 2 {
		t.Fatalf("after mutation hits=%d misses=%d want 1/2", hits, misses)
	}
}
