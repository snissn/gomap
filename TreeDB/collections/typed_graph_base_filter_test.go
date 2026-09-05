package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestTypedGraphBaseFilterIndependentReadersDuringPublication(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 64)
	const readers, rounds = 2, 8
	var roundTrip, activeRead [readers][rounds]time.Duration
	var writeAck [rounds]time.Duration
	bases := make([]*VectorIndexSearcher, readers)
	cold := make([]*typedGraphBaseFilter, readers)
	for i := range readers {
		var err error
		bases[i], err = col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
		if err != nil {
			t.Fatal(err)
		}
		defer bases[i].Close()
		cold[i], err = prepareTypedGraphBaseFilter(bases[i], HybridScalarFilter{IndexName: "path", Value: "source"}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 1000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 2000}, Clauses: 8, PredicateBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Every worker owns its searcher, cold plan, current pin and reusable buffer.
	// The writer publishes between two materializations from each held pin.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	ready, failures := make(chan struct{}, readers), make(chan error, readers)
	published := make([]chan struct{}, rounds)
	for i := range published {
		published[i] = make(chan struct{})
	}
	for worker := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buffer VectorIndexSearchBuffer
			for round := range rounds {
				err := func() error {
					started := time.Now()
					current, err := col.OpenCollectionReadView()
					if err != nil {
						return err
					}
					defer current.Close()
					overlay, err := prepareTypedGraphOverlaySearch(bases[worker], current, typedGraphOverlayLimits{Rows: 32, Tombstones: 16, Bytes: 1 << 20})
					if err != nil {
						return err
					}
					plan, err := bindTypedGraphBaseFilter(cold[worker], overlay, typedGraphFilterBindLimits{Rows: 32, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 1000})
					if err != nil {
						return err
					}
					results, stats, err := overlay.searchPreparedFilter(plan, columns[0].Float32Vectors[0], 4, 16, 128, &buffer)
					if err != nil {
						return err
					}
					if len(results) != 4 || string(results[0].ID) != string(ids[0]) || !stats.FilteredExact || plan.count != 64 {
						return fmt.Errorf("worker %d round %d inconsistent results", worker, round)
					}
					before, err := current.FetchDocumentsForVectorIndexSearchResults(results[:1], DocumentFetchOptions{})
					if err != nil {
						return err
					}
					ready <- struct{}{}
					firstRead := time.Since(started)
					select {
					case <-published[round]:
					case <-ctx.Done():
						return ctx.Err()
					}
					resumed := time.Now()
					after, err := current.FetchDocumentsForVectorIndexSearchResults(results[:1], DocumentFetchOptions{})
					if err != nil {
						return err
					}
					if len(before.Results) != 1 || len(after.Results) != 1 || !before.Results[0].Found || !after.Results[0].Found || !bytes.Equal(before.Results[0].Document, after.Results[0].Document) {
						return fmt.Errorf("worker %d round %d pin changed across publication", worker, round)
					}
					activeRead[worker][round] = firstRead + time.Since(resumed)
					roundTrip[worker][round] = time.Since(started)
					return nil
				}()
				if err != nil {
					failures <- err
					cancel()
					return
				}
			}
		}()
	}
	for round := range rounds {
		for range readers {
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal(<-failures)
			}
		}
		row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{fmt.Sprintf("published-%d", round)}}, {Name: "user", Strings: columns[2].Strings[:1]}, {Name: "path", Strings: columns[3].Strings[:1]}}
		started := time.Now()
		if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], row); err != nil {
			t.Fatal(err)
		}
		writeAck[round] = time.Since(started)
		close(published[round])
	}
	wg.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	// Small forced-interleaving samples, not stable production tail estimates.
	// Round trip includes barrier scheduling and the writer's complete ack;
	// active reads exclude that wait. Both exclude current-pin Close.
	for _, sample := range []struct {
		name   string
		values []time.Duration
	}{{"pin_through_postpublication_fetch", append(slices.Clone(roundTrip[0][:]), roundTrip[1][:]...)}, {"active_read_segments", append(slices.Clone(activeRead[0][:]), activeRead[1][:]...)}, {"writer_replace_ack", slices.Clone(writeAck[:])}} {
		slices.Sort(sample.values)
		percentile := func(p int) time.Duration { return sample.values[(len(sample.values)*p+99)/100-1] }
		t.Logf("%s samples=%d p50=%s p95=%s p99=%s sorted_durations=%v", sample.name, len(sample.values), percentile(50), percentile(95), percentile(99), sample.values)
	}
}

func TestTypedGraphBaseFilterBindingNewIDAndCurrentOutput(t *testing.T) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 64)
	coldLimits := typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 1000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 2000}, Clauses: 8, PredicateBytes: 1024}
	bindLimits := typedGraphFilterBindLimits{Rows: 16, IDBytes: 10000, ValueBytes: 10000, MappingWork: 10000, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 1000}
	filter := HybridScalarFilter{And: []HybridScalarFilter{{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Value: "00000", Inclusive: true}, Upper: IndexRangeBound{Value: "00063", Inclusive: true}}}, {IndexName: "path", Value: "source"}}}
	cold, err := prepareTypedGraphBaseFilter(base, filter, coldLimits)
	if err != nil {
		t.Fatal(err)
	}
	// One existing row leaves the conjunction; a new ID with no base locator
	// enters. The surviving current result count remains 64.
	row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"left"}}, {Name: "user", Strings: []string{"00000"}}, {Name: "path", Strings: []string{"outside"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], row); err != nil {
		t.Fatal(err)
	}
	newIDs, newRetained := [][]byte{[]byte("new-id")}, [][]byte{[]byte(`{"id":"new-id","residual":"kept"}`)}
	vector := vectorBenchmarkEmbedding(9876, 8)
	row[0].Float32Vectors = [][]float32{vector}
	row[1].Strings = []string{"pinned content"}
	row[3].Strings = []string{"source"}
	if _, _, err := col.InsertTypedBatchWithStats(newIDs, newRetained, row); err != nil {
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
	bound, err := bindTypedGraphBaseFilter(cold, overlay, bindLimits)
	if err != nil || bound.count != 64 || len(bound.excludedBase) != 1 || len(bound.delta) != 1 || bound.sourceIDs != 2 {
		t.Fatalf("binding=%+v err=%v", bound, err)
	}
	fresh, err := prepareTypedGraphFilter(overlay, filter, coldLimits.typedGraphFilterLimits)
	if err != nil {
		t.Fatal(err)
	}
	var a, b VectorIndexSearchBuffer
	got, stats, err := overlay.searchPreparedFilter(bound, vector, 10, 16, 128, &a)
	if err != nil || !stats.FilteredExact || len(got) != 10 || string(got[0].ID) != "new-id" {
		t.Fatalf("results=%+v stats=%+v err=%v", got, stats, err)
	}
	want, _, err := overlay.searchPreparedFilter(fresh, vector, 10, 16, 128, &b)
	if err != nil || len(got) != len(want) {
		t.Fatal(err)
	}
	for i := range got {
		if string(got[i].ID) != string(want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			t.Fatalf("bound=%+v current postings=%+v", got, want)
		}
	}
	// Later publication must not change materialization from the query's pin.
	row[1].Strings = []string{"later content"}
	row[3].Strings = []string{"later path"}
	if _, err := col.ReplaceTypedBatch(newIDs, newRetained, row); err != nil {
		t.Fatal(err)
	}
	fetched, err := current.FetchDocumentsForVectorIndexSearchResults(got[:1], DocumentFetchOptions{})
	if err != nil || len(fetched.Results) != 1 || !fetched.Results[0].Found {
		t.Fatalf("fetch=%+v err=%v", fetched, err)
	}
	var document struct {
		Content   string    `json:"content"`
		Embedding []float32 `json:"embedding"`
		Residual  string    `json:"residual"`
		Meta      struct {
			User string `json:"user_id"`
			Path string `json:"fpath"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(fetched.Results[0].Document, &document); err != nil {
		t.Fatal(err)
	}
	if document.Content != "pinned content" || document.Meta.User != "00000" || document.Meta.Path != "source" || document.Residual != "kept" || !slices.EqualFunc(document.Embedding, vector, func(a, b float32) bool { return math.Float32bits(a) == math.Float32bits(b) }) {
		t.Fatalf("wrong pinned document: %s", fetched.Results[0].Document)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := bindTypedGraphBaseFilter(cold, overlay, bindLimits); p != nil || !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("closed current binding=%+v err=%v", p, err)
	}
	if _, _, err := overlay.searchPreparedFilter(bound, vector, 10, 16, 128, &a); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || len(a.results) != 0 {
		t.Fatalf("closed current results=%+v err=%v", a.results, err)
	}
	latest, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	latestOverlay, err := prepareTypedGraphOverlaySearch(base, latest, typedGraphOverlayLimits{Rows: 16, Tombstones: 8, Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.documentView.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := bindTypedGraphBaseFilter(cold, latestOverlay, bindLimits); p != nil || !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("closed base view binding=%+v err=%v", p, err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := prepareTypedGraphBaseFilter(base, filter, coldLimits); p != nil || !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
		t.Fatalf("closed base preparation=%+v err=%v", p, err)
	}
}

func TestTypedGraphBaseFilterBindingDeltaOnlyThreshold(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 64)
	cold, err := prepareTypedGraphBaseFilter(base, HybridScalarFilter{IndexName: "user", Value: "delta"}, typedGraphBaseFilterLimits{typedGraphFilterLimits: typedGraphFilterLimits{SourceIDs: 10000, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 20000}, Clauses: 8, PredicateBytes: 1024})
	if err != nil || cold.plan.count != 0 {
		t.Fatalf("cold=%+v err=%v", cold, err)
	}
	ids, retained := make([][]byte, 4097), make([][]byte, 4097)
	columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("delta-%05d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
		columns[0].Float32Vectors = append(columns[0].Float32Vectors, vectorBenchmarkEmbedding(i, 8))
		columns[1].Strings = append(columns[1].Strings, "delta content")
		columns[2].Strings = append(columns[2].Strings, "delta")
		columns[3].Strings = append(columns[3].Strings, "source")
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	for _, count := range []int{4097, 4096} {
		if count == 4096 {
			if err := col.Delete(ids[4096]); err != nil {
				t.Fatal(err)
			}
		}
		current, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		defer current.Close()
		overlay, err := prepareTypedGraphOverlaySearch(base, current, typedGraphOverlayLimits{Rows: 5000, Tombstones: 16, Bytes: 16 << 20})
		if err != nil {
			t.Fatal(err)
		}
		bound, err := bindTypedGraphBaseFilter(cold, overlay, typedGraphFilterBindLimits{Rows: 5000, IDBytes: 1 << 20, ValueBytes: 1 << 20, MappingWork: 1 << 20, PredicateWork: 10000, RetainedBytes: 1 << 20, ExactScanRows: 5000})
		if err != nil || bound.count != count || bound.base.Count() != 0 {
			t.Fatalf("count=%d plan=%+v err=%v", count, bound, err)
		}
		results, stats, err := overlay.searchPreparedFilter(bound, columns[0].Float32Vectors[0], 10, 256, 8192, &buffer)
		if count == 4097 {
			if !errors.Is(err, errTypedGraphSearchBudget) || len(results) != 0 || len(buffer.results) != 0 || stats.Base.Candidates != 0 {
				t.Fatalf("delta-only mislabeled ANN: results=%d stats=%+v err=%v", len(results), stats, err)
			}
		} else if err != nil || len(results) != 10 || !stats.FilteredExact || stats.DeltaScored != 4096 || stats.Base.Candidates != 0 || string(results[0].ID) != string(ids[0]) {
			t.Fatalf("delta exact results=%+v stats=%+v err=%v", results, stats, err)
		}
	}
}

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
	for i := range changedCatalog.meta.Indexes {
		if changedCatalog.meta.Indexes[i].Name == "user" {
			changedCatalog.meta.Indexes[i].Field = "different"
		}
	}
	changedCurrent.catalog = &changedCatalog
	changedOverlay := *newOverlay
	changedOverlay.current = &changedCurrent
	if p, err := bindTypedGraphBaseFilter(cold, &changedOverlay, limits); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || p != nil {
		t.Fatalf("changed scalar definition accepted: nonnil=%v err=%v", p != nil, err)
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
