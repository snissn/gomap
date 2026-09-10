package collections

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestTypedGraphFilterKeeper(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 128)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	owners := typedGraphOverlapLimits()
	owners.Owners = 8
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, owners.Cold); err != nil {
		t.Fatal(err)
	}
	keeper, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", owners)
	if err != nil {
		t.Fatal(err)
	}
	initialBacking := keeper.capturedBase.backingBytes
	limits := typedGraphFilterLimits{SourceIDs: 128, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 512}
	query := func(ctx context.Context, filter HybridScalarFilter, limits typedGraphFilterLimits) (ColumnGraphFilterWork, error) {
		owner, err := col.openTypedGraphReadOwner(owners)
		if err != nil {
			return ColumnGraphFilterWork{}, err
		}
		defer owner.Close()
		p := col.borrowTypedGraphFilterKeeper(owner, "embedding_graph")
		if p != nil {
			defer p.mu.RUnlock()
		}
		var work ColumnGraphFilterWork
		plan, err := prepareTypedGraphServingFilter(ctx, p, owner.overlay, filter, limits, &work)
		if err != nil {
			return work, err
		}
		// Fresh current-snapshot preparation remains the result oracle.
		fresh, err := prepareTypedGraphFilterUnmetered(context.Background(), owner.overlay, filter, limits, nil)
		if err != nil {
			return work, err
		}
		var a, b VectorIndexSearchBuffer
		got, _, err := owner.overlay.searchPreparedFilter(plan, columns[0].Float32Vectors[0], 4, 16, 128, &a)
		if err != nil {
			return work, err
		}
		want, _, err := owner.overlay.searchPreparedFilter(fresh, columns[0].Float32Vectors[0], 4, 16, 128, &b)
		if err != nil {
			return work, err
		}
		if !reflect.DeepEqual(got, want) {
			return work, fmt.Errorf("cached results=%v fresh=%v", got, want)
		}
		gotDocs, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(got, DocumentFetchOptions{})
		if err != nil {
			return work, err
		}
		wantDocs, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(want, DocumentFetchOptions{})
		if err == nil && !reflect.DeepEqual(gotDocs.Results, wantDocs.Results) {
			err = fmt.Errorf("cached full fetch differs")
		}
		return work, err
	}
	ctx := context.Background()
	all := HybridScalarFilter{IndexName: "path", Value: "source"}
	// Four independent current pins converge on one completed immutable plan.
	var wg sync.WaitGroup
	var work [4]ColumnGraphFilterWork
	var errs [4]error
	beforeProducer := workstats.Read().Graph
	for i := range work {
		wg.Go(func() { work[i], errs[i] = query(ctx, all, limits) })
	}
	wg.Wait()
	var sourceIDs uint64
	for i, w := range work {
		if errs[i] != nil || !w.Completed || w.EligibleRows != 128 {
			t.Fatalf("reader %d: %+v %v", i, w, errs[i])
		}
		sourceIDs += w.SourceIDs
	}
	afterProducer := workstats.Read().Graph
	var bytes, inspected, mapping uint64
	for _, w := range work {
		bytes += w.SourceBytes
		inspected += w.InspectedEntries
		mapping += w.MappingWorkCharged
	}
	if afterProducer.Filters.Attempts-beforeProducer.Filters.Attempts != 4 || afterProducer.Filters.Completed-beforeProducer.Filters.Completed != 4 || afterProducer.FilterSourceIDs-beforeProducer.FilterSourceIDs != sourceIDs || afterProducer.FilterSourceBytes-beforeProducer.FilterSourceBytes != bytes || afterProducer.FilterInspectedEntries-beforeProducer.FilterInspectedEntries != inspected || afterProducer.FilterMappingWorkCharged-beforeProducer.FilterMappingWorkCharged != mapping {
		t.Fatal("cached miss/hit work does not match producers")
	}
	if sourceIDs != 128 {
		t.Fatalf("same predicate repeated cold work: %d", sourceIDs)
	}
	if keeper.capturedBase.backingBytes <= initialBacking {
		t.Fatal("cached selection uncharged")
	}
	cached := keeper.capturedBase.filters[0].filter
	if cached == nil || cached.plan.overlay != nil || cached.holder == nil {
		t.Fatal("cache retained request pin")
	}
	// Exact limit values are part of the key, including tighter source limits.
	tighter := limits
	tighter.SourceIDs = 127
	if _, err := query(ctx, all, tighter); !errors.Is(err, errTypedGraphSearchBudget) {
		t.Fatalf("cached result bypassed tighter cap: %v", err)
	}
	if keeper.capturedBase.filters[1].filter != nil {
		t.Fatal("failed preparation installed entry")
	}
	// Seven distinct range bounds, including NUL, fill the remaining entries.
	filters := []HybridScalarFilter{all}
	for _, upper := range []string{"", "\x00", "00000", "00001", "00002", "00003", "00004"} {
		filter := HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Value: upper, Inclusive: true}}}
		if _, err := query(ctx, filter, limits); err != nil {
			t.Fatal(err)
		}
		filters = append(filters, filter)
	}
	for i, f := range filters {
		w, err := query(ctx, f, limits)
		if err != nil || w.SourceIDs != 0 || w.InspectedEntries != 0 {
			t.Fatalf("cached key %d: %+v %v", i, w, err)
		}
	}
	ninth := HybridScalarFilter{IndexName: "user", Value: "00005"}
	for range 2 {
		w, err := query(ctx, ninth, limits)
		if err != nil || w.SourceIDs != 1 {
			t.Fatalf("ninth fallback: %+v %v", w, err)
		}
	}
	// Caller range mutation creates a distinct key, without changing its old plan.
	changed := filters[7]
	changed.Range.Upper.Value = "00007"
	w, err := query(ctx, changed, limits)
	if err != nil || w.SourceIDs != 8 || w.EligibleRows != 8 {
		t.Fatalf("range alias: %+v %v", w, err)
	}
	// Updates, deletes and a new ID change current membership and full output.
	row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"updated full payload"}}, {Name: "user", Strings: []string{"\x00"}}, {Name: "path", Strings: []string{"elsewhere"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], row); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	row[3].Strings[0] = "source"
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("new-id")}, [][]byte{[]byte(`{"id":"new-id","residual":"kept"}`)}, row); err != nil {
		t.Fatal(err)
	}
	for _, f := range []HybridScalarFilter{all, filters[2], filters[5]} {
		w, err := query(ctx, f, limits)
		if err != nil {
			t.Fatalf("current suffix: %+v %v", w, err)
		}
		if f.IndexName == "path" {
			if w.SourceIDs != 3 || w.InspectedEntries != 0 {
				t.Fatalf("broad predicate lost suffix binding: %+v", w)
			}
		} else if w.InspectedEntries == 0 {
			t.Fatalf("narrow predicate bound the larger or equal suffix: %+v", w)
		}
	}
	if keeper.capturedBase.filters[2].filter == nil || keeper.capturedBase.filters[5].filter == nil {
		t.Fatal("cheaper current preparation evicted cached predicates")
	}
	// Cancellation on a warm plan preserves errors and no completed result.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if w, err := query(canceled, all, limits); !errors.Is(err, context.Canceled) || w.Completed {
		t.Fatalf("cancel=%+v %v", w, err)
	}
	owner, err := col.openTypedGraphReadOwner(owners)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := col.borrowTypedGraphFilterKeeper(owner, "embedding_graph")
	if borrowed != keeper {
		t.Fatal("lost keeper")
	}
	closed := make(chan error, 1)
	go func() { closed <- col.CloseVectorIndexPreparedSearchCache() }()
	select {
	case err := <-closed:
		t.Fatalf("Close released borrowed keeper: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	var finalWork ColumnGraphFilterWork
	plan, err := prepareTypedGraphServingFilter(ctx, borrowed, owner.overlay, all, limits, &finalWork)
	if err != nil || !plan.validFor(owner.overlay) {
		t.Fatalf("borrowed plan during Close: %v", err)
	}
	borrowed.mu.RUnlock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	a := &col.collectionSchemaCoordinator().typedGraphOwners
	a.Lock()
	if a.owners != 0 || a.baseOwners != 0 || a.stateBytes != 0 || a.baseBackingBytes != 0 {
		t.Errorf("leaked keeper/owner accounting: %+v", a)
	}
	a.Unlock()
	// A new keeper starts empty; a surviving raw filter cannot authorize it.
	keeper, err = col.acquireTypedGraphCapturedBaseCache("embedding_graph", owners)
	if err != nil {
		t.Fatal(err)
	}
	if keeper.capturedBase.filters[0].filter != nil {
		t.Fatal("retired entries survived")
	}
	w, err = query(ctx, all, limits)
	// SourceIDs=128 cold plus 3 changed IDs exceeds this request's shared cap.
	if !errors.Is(err, errTypedGraphSearchBudget) || w.Completed || w.SourceIDs != 128 {
		t.Fatalf("cold+bind budget=%+v %v", w, err)
	}
	if keeper.capturedBase.filters[0].filter != nil {
		t.Fatal("failed bind installed cache entry")
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
}

// Block at the cold builder's first context check, after installing its promise.
type typedGraphFilterBuildContext struct {
	context.Context
	calls            int
	entered, release chan struct{}
}

func (c *typedGraphFilterBuildContext) Err() error {
	c.calls++
	if c.calls == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestTypedGraphFilterKeeperWaitAndAdmission(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, _, _, _, _ := openTypedGraphQualityFixture(t, 64)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	keeper, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", limits)
	if err != nil {
		t.Fatal(err)
	}
	defer col.CloseVectorIndexPreparedSearchCache()
	first, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	p := col.borrowTypedGraphFilterKeeper(first, "embedding_graph")
	if p != keeper {
		t.Fatal("missing keeper")
	}
	defer p.mu.RUnlock()
	filter := HybridScalarFilter{IndexName: "path", Value: "source"}
	f := typedGraphFilterLimits{SourceIDs: 128, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 256}
	blocked := &typedGraphFilterBuildContext{Context: context.Background(), entered: make(chan struct{}), release: make(chan struct{})}
	finished := make(chan error, 1)
	var cold ColumnGraphFilterWork
	go func() {
		_, err := prepareTypedGraphServingFilter(blocked, p, first.overlay, filter, f, &cold)
		finished <- err
	}()
	<-blocked.entered
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	var waiting ColumnGraphFilterWork
	_, waitErr := prepareTypedGraphServingFilter(timeout, p, second.overlay, filter, f, &waiting)
	cancel()
	close(blocked.release)
	buildErr := <-finished
	if !errors.Is(waitErr, context.DeadlineExceeded) || waiting.Completed || waiting.SourceIDs != 0 || buildErr != nil || cold.SourceIDs != 64 || !cold.Completed {
		t.Fatalf("wait=%+v %v build=%+v %v", waiting, waitErr, cold, buildErr)
	}
	if first.overlay.base.documentView != nil {
		t.Fatal("zero-suffix cold preparation rebuilt the validated base view")
	}
	var warm ColumnGraphFilterWork
	if _, err := prepareTypedGraphServingFilter(context.Background(), p, second.overlay, filter, f, &warm); err != nil || warm.SourceIDs != 0 {
		t.Fatalf("canceled waiter damaged builder: %+v %v", warm, err)
	}
	// Accounted state is full: a successful request must remain usable uncached.
	a := p.capturedBase.accounting
	a.Lock()
	extra := a.limits.StateBytes - a.stateBytes - a.baseDescriptorBytes - a.baseBackingBytes
	a.stateBytes += extra
	before := a.baseBackingBytes
	a.Unlock()
	one := HybridScalarFilter{IndexName: "user", Value: "00000"}
	var uncached ColumnGraphFilterWork
	plan, err := prepareTypedGraphServingFilter(context.Background(), p, second.overlay, one, f, &uncached)
	a.Lock()
	a.stateBytes -= extra
	changedBacking := a.baseBackingBytes != before
	a.Unlock()
	if err != nil || plan == nil || !plan.validFor(second.overlay) || uncached.SourceIDs != 1 || changedBacking || p.capturedBase.filters[1].filter != nil {
		t.Fatalf("admission changed request: %+v %v", uncached, err)
	}
	// Cancel a genuine cold prefix; neither the slot nor its bytes may survive.
	interrupted := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 6}
	var prefix ColumnGraphFilterWork
	plan, err = prepareTypedGraphServingFilter(interrupted, p, second.overlay, one, f, &prefix)
	if !errors.Is(err, context.Canceled) || plan != nil || prefix.Completed || p.capturedBase.filters[1].filter != nil {
		t.Fatalf("cold cancel: %+v %v", prefix, err)
	}
	if _, err := prepareTypedGraphServingFilter(context.Background(), p, second.overlay, one, f, &warm); err != nil || warm.SourceIDs != 1 {
		t.Fatalf("cold cancel retry: %+v %v", warm, err)
	}
}

func TestTypedGraphFilterKeeperCutoffAndRebuild(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, fixture, ids, retained, columns, ranks := openTypedGraphQualityFixture(t, 4098)
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	owners := typedGraphOverlapLimits()
	pub := typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}
	if err := col.reconcileTypedGraphPublication(pub, owners.Cold); err != nil {
		t.Fatal(err)
	}
	keeper, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", owners)
	if err != nil {
		t.Fatal(err)
	}
	defer col.CloseVectorIndexPreparedSearchCache()
	limits := typedGraphFilterLimits{SourceIDs: 8192, SourceBytes: 1 << 20, RetainedBytes: 1 << 20, MappingWork: 1 << 20, InspectedEntries: 16384}
	filter := HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Value: "04096", Inclusive: true}}}
	query := func(want int, cold bool) {
		t.Helper()
		owner, err := col.openTypedGraphReadOwner(owners)
		if err != nil {
			t.Fatal(err)
		}
		defer owner.Close()
		p := col.borrowTypedGraphFilterKeeper(owner, "embedding_graph")
		if p == nil {
			t.Fatal("missing keeper")
		}
		defer p.mu.RUnlock()
		var work ColumnGraphFilterWork
		plan, err := prepareTypedGraphServingFilter(context.Background(), p, owner.overlay, filter, limits, &work)
		if err != nil || plan.count != want || (work.InspectedEntries != 0) != cold {
			t.Fatalf("cutoff preparation=%+v %v", work, err)
		}
		fresh, err := prepareTypedGraphFilter(owner.overlay, filter, limits)
		if err != nil {
			t.Fatal(err)
		}
		for row := range 4098 {
			if (plan.base.Contains(row) && !plan.excludesBaseOrdinal(row)) != fresh.base.Contains(row) {
				t.Fatalf("base membership changed at %d", row)
			}
		}
		if !reflect.DeepEqual(plan.delta, fresh.delta) {
			t.Fatal("delta membership changed")
		}
		var buffer VectorIndexSearchBuffer
		_, stats, err := owner.overlay.searchPreparedFilter(plan, columns[0].Float32Vectors[0], 10, 256, 8192, &buffer)
		if err != nil || stats.FilteredExact != (want <= 4096) {
			t.Fatalf("cutoff scoring=%+v %v", stats, err)
		}
	}
	query(4097, true)
	old := keeper.capturedBase.filters[0].filter
	removed := 0
	for i, rank := range ranks {
		if rank == 4096 {
			removed = i
			break
		}
	}
	if err := col.Delete(ids[removed]); err != nil {
		t.Fatal(err)
	}
	query(4096, false)
	row := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[removed : removed+1]}, {Name: "content", Strings: columns[1].Strings[removed : removed+1]}, {Name: "user", Strings: columns[2].Strings[removed : removed+1]}, {Name: "path", Strings: columns[3].Strings[removed : removed+1]}}
	if _, _, err := col.InsertTypedBatchWithStats(ids[removed:removed+1], retained[removed:removed+1], row); err != nil {
		t.Fatal(err)
	}
	query(4097, false)
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.reconcileTypedGraphPublication(pub, owners.Cold); err != nil {
		t.Fatal(err)
	}
	if _, err := col.acquireTypedGraphCapturedBaseCache("embedding_graph", owners); err != nil {
		t.Fatal(err)
	}
	current, err := col.openTypedGraphReadOwner(owners)
	if err != nil {
		t.Fatal(err)
	}
	if old.validFor(current.overlay) {
		t.Fatal("old filter authorized rebuilt base")
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	query(4097, true)
}
