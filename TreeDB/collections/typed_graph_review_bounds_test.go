package collections

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestColumnAssetLifecycleSharedCopyBudgetCountsRecords(t *testing.T) {
	db := openCollectionCommandWALDB(t, prepareColumnAssetReachabilityCommandWALDirM15A(t))
	defer db.Close()
	col := openColumnAssetLifecycleTestCollection1954(t, db)
	ref := writeColumnAssetReachabilityCandidateM15A(t, db, col, 3, 99)
	pin, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "shared-copy-budget", Refs: []ColumnAssetRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	registry, err := col.RegisterColumnAssetPreparedAsset(ColumnAssetPreparedAssetRegistrationOptions{Owner: "shared-copy-budget", Source: "test", Refs: []ColumnAssetRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if pins, err := col.columnAssetLifecyclePinSetSnapshotWithLimit(2); err != nil || len(pins) != 1 {
		t.Fatalf("pin fixture=%d err=%v", len(pins), err)
	}
	if records, err := col.columnAssetLifecycleRegistrySnapshotWithLimit(2); err != nil || len(records) != 1 {
		t.Fatalf("registry fixture=%d err=%v", len(records), err)
	}
	for _, limit := range []int{1, 2, 3} {
		if _, err := col.columnAssetLifecycleAugmentReachabilityOptions(ColumnAssetReachabilityOptions{MaxLifecycleEntries: limit}); !errors.Is(err, ErrColumnAssetReachabilityLifecycleLimit) {
			t.Fatalf("limit%d accepted two records plus two refs: %v", limit, err)
		}
	}
	for _, limit := range []int{0, 4} {
		opts, err := col.columnAssetLifecycleAugmentReachabilityOptions(ColumnAssetReachabilityOptions{MaxLifecycleEntries: limit})
		if err != nil || len(opts.PreparedRefs) != 1 || len(opts.PreparedQueryRefs) != 1 {
			t.Fatalf("limit%d exact budget rejected/lost refs: %+v err=%v", limit, opts, err)
		}
	}
	remaining := 2
	if pins, err := col.columnAssetLifecyclePinSetSnapshotWithBudget(&remaining); err != nil || len(pins) != 1 || remaining != 0 {
		t.Fatalf("pin shared budget remaining=%d err=%v", remaining, err)
	}
	if records, err := col.columnAssetLifecycleRegistrySnapshotWithBudget(&remaining); !errors.Is(err, ErrColumnAssetReachabilityLifecycleLimit) || len(records) != 0 {
		t.Fatalf("exhausted budget cloned registry records=%d err=%v", len(records), err)
	}
	if _, err := col.columnAssetLifecycleAugmentReachabilityOptions(ColumnAssetReachabilityOptions{MaxLifecycleEntries: 4, CandidateRefs: []ColumnAssetRef{ref}}); !errors.Is(err, ErrColumnAssetReachabilityLifecycleLimit) {
		t.Fatalf("input ref did not share copy budget: %v", err)
	}
}

func TestTypedGraphCapturedCacheCanceledWaiterPreservesBuilder(t *testing.T) {
	requireTypedGraphPreparedHolderTest(t)
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	index := base.indexName
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	collectionVectorIndexPreparedSearchBuildHookForTest.fn = func(got string) {
		if got == index {
			once.Do(func() { close(entered); <-release })
		}
	}
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	defer func() {
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
		collectionVectorIndexPreparedSearchBuildHookForTest.fn = nil
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	}()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	built := make(chan error, 1)
	go func() { _, err := col.acquireTypedGraphCapturedBaseCache(index, limits); built <- err }()
	<-entered
	col.vectorBufferedSearchMu.Lock()
	beforeWaits := col.vectorBufferedSearchWaits
	col.vectorBufferedSearchMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := make(chan error, 1)
	go func() { _, err := col.acquireTypedGraphCapturedBaseCacheWithContext(ctx, index, limits); waiter <- err }()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		col.vectorBufferedSearchMu.Lock()
		waiting := col.vectorBufferedSearchWaits > beforeWaits
		col.vectorBufferedSearchMu.Unlock()
		if waiting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("cache waiter did not join builder")
		case <-tick.C:
		}
	}
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("waiter error=%v", err)
		}
	case <-time.After(time.Second):
		t.Error("canceled cache waiter stayed blocked")
		releaseOnce.Do(func() { close(release) })
		<-waiter
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-built; err != nil {
		t.Fatal(err)
	}
	keeper, err := col.acquireTypedGraphCapturedBaseCache(index, limits)
	if err != nil || keeper == nil || keeper.capturedBase == nil {
		t.Fatalf("canceled waiter invalidated builder: %v", err)
	}
	if err := col.CloseVectorIndexPreparedSearchCache(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedGraphPublicFinalWarmBarrierCancellation(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	for _, fold := range []bool{false, true} {
		name := "ensure"
		if fold {
			name = "fold"
		}
		t.Run(name, func(t *testing.T) {
			col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
			index := base.indexName
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}
			opts := typedGraphPublicTestOptions()
			if fold {
				if err := col.EnsureColumnGraphServing(context.Background(), index, opts); err != nil {
					t.Fatal(err)
				}
			}
			root, err := canonicalVectorPartitionStorageRootV1(col.db.Dir())
			if err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			holder := make(chan error, 1)
			var once, releaseOnce sync.Once
			collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
			collectionVectorIndexPreparedSearchBuildHookForTest.fn = func(got string) {
				if got != index {
					return
				}
				once.Do(func() {
					go func() {
						holder <- WithVectorPartitionStorageBarrierV1(root, func() error { close(entered); <-release; return nil })
					}()
					<-entered
				})
			}
			collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
			defer func() {
				collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
				collectionVectorIndexPreparedSearchBuildHookForTest.fn = nil
				collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
				releaseOnce.Do(func() { close(release) })
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			beforeFold := workstats.Read().Fold
			done := make(chan error, 1)
			go func() {
				if fold {
					done <- col.FoldColumnGraphServing(ctx, index)
				} else {
					done <- col.EnsureColumnGraphServing(ctx, index, opts)
				}
			}()
			deadline := time.After(5 * time.Second)
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				vectorPartitionStorageBarriersV1.Lock()
				entry := vectorPartitionStorageBarriersV1.entries[root]
				waiting := entry != nil && entry.refs == 2
				vectorPartitionStorageBarriersV1.Unlock()
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("exited before final warming barrier: %v", err)
				case <-deadline:
					t.Fatal("final warmer did not wait")
				case <-tick.C:
				}
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("canceled final warmer: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("canceled final warmer remains blocked on storage barrier")
				releaseOnce.Do(func() { close(release) })
				<-done
			}
			if fold {
				afterFold := workstats.Read().Fold
				state, available := col.ColumnGraphServingSnapshot()
				if afterFold.Publications-beforeFold.Publications != 1 || afterFold.Public.Completed != beforeFold.Public.Completed || afterFold.Public.Errors-beforeFold.Public.Errors != 1 || !available || !state.ServingReady || state.Invalid {
					t.Errorf("postpublication cancellation before=%+v after=%+v state=%+v", beforeFold, afterFold, state)
				}
			}
			releaseOnce.Do(func() { close(release) })
			if err := <-holder; err != nil {
				t.Fatal(err)
			}
		})
	}
}
