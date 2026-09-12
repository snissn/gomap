package collections

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTypedGraphReadOwnerDoesNotWaitForImmediatePublication(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	limits := opts.Owners
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	before := col.typedGraphPublicationSnapshot()
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		once.Do(func() { close(entered); <-release })
		return nil
	})
	defer restore()
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"new"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	written := make(chan error, 1)
	go func() { _, err := col.UpsertTypedBatch(ids[:1], retained[:1], changed); written <- err }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("immediate publication did not reach asset preparation")
	}
	type opened struct {
		owner *typedGraphReadOwner
		err   error
	}
	done := make(chan opened, 1)
	go func() {
		owner, err := col.openTypedGraphReadOwner(limits)
		if err == nil {
			var buffer VectorIndexSearchBuffer
			response, view, searchErr := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
			if view != nil {
				_, fetchErr := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
				searchErr = errors.Join(searchErr, fetchErr, view.Close())
			}
			err = searchErr
		}
		done <- opened{owner, err}
	}()
	var result opened
	select {
	case result = <-done:
	case <-time.After(time.Second):
		unblock()
		<-written
		result = <-done
		if result.owner != nil {
			_ = result.owner.Close()
		}
		t.Fatal("coherent old-generation read waited for an immediate writer")
	}
	if result.owner != nil {
		defer result.owner.Close()
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.owner.state != before {
		t.Fatal("read did not retain the coherent pre-publication generation")
	}
	unblock()
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	next, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if next.state == before || len(next.state.rows) != 1 || next.state.rows[0].Values[1].String != "new" || result.owner.state != before {
		t.Fatal("new acknowledgment or retained old generation lost publication coherence")
	}
}

func TestTypedGraphReadOwnerDrainsBufferedReceiptCreatedBeforeCapture(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(col.db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	typedGraphOwnerAfterSnapshotHook.Lock()
	typedGraphOwnerAfterSnapshotHook.afterDrain = func(c *Collection) {
		if c == col {
			once.Do(func() { close(reached); <-release })
		}
	}
	typedGraphOwnerAfterSnapshotHook.Unlock()
	defer func() {
		typedGraphOwnerAfterSnapshotHook.Lock()
		typedGraphOwnerAfterSnapshotHook.afterDrain = nil
		typedGraphOwnerAfterSnapshotHook.Unlock()
	}()
	type opened struct {
		owner *typedGraphReadOwner
		err   error
	}
	done := make(chan opened, 1)
	go func() {
		owner, err := col.openTypedGraphReadOwner(opts.Owners)
		done <- opened{owner, err}
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("reader did not reach pre-capture admission")
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"buffered"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, _, err := other.InsertTypedBatchWithStats([][]byte{[]byte("buffered")}, [][]byte{[]byte(`{"id":"buffered"}`)}, changed); err != nil {
		close(release)
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	coord.typedPublicationDebtMu.Lock()
	buffered := coord.typedPublicationBuffered
	coord.typedPublicationDebtMu.Unlock()
	if buffered != 1 {
		close(release)
		t.Fatalf("buffered receipts=%d want=1", buffered)
	}
	close(release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.owner.Close()
	if result.owner.state.physicalRows != 1 {
		t.Fatal("read omitted acknowledged buffered insert created before capture")
	}
}

func TestTypedGraphReadOwnerWaitsForSchemaMaintenance(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}

	reachedAdmission := make(chan struct{})
	typedGraphOwnerAfterSnapshotHook.Lock()
	typedGraphOwnerAfterSnapshotHook.afterDrain = func(c *Collection) {
		if c == col {
			close(reachedAdmission)
		}
	}
	typedGraphOwnerAfterSnapshotHook.Unlock()
	defer func() {
		typedGraphOwnerAfterSnapshotHook.Lock()
		typedGraphOwnerAfterSnapshotHook.afterDrain = nil
		typedGraphOwnerAfterSnapshotHook.Unlock()
	}()

	unlockSchema := col.lockCollectionSchemaWrite()
	done := make(chan error, 1)
	go func() {
		owner, err := col.openTypedGraphReadOwner(limits)
		if owner != nil {
			err = errors.Join(err, owner.Close())
		}
		done <- err
	}()
	select {
	case <-reachedAdmission:
	case <-time.After(10 * time.Second):
		unlockSchema()
		t.Fatal("reader did not reach schema admission")
	}
	select {
	case err := <-done:
		unlockSchema()
		t.Fatalf("reader crossed schema maintenance: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	unlockSchema()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reader did not resume after schema maintenance")
	}
}

func waitTypedGraphReadAdmissionWaiter(t *testing.T, coord *collectionSchemaCoordinator) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		coord.typedPublicationDebtMu.Lock()
		waiting := coord.typedPublicationChanged != nil
		coord.typedPublicationDebtMu.Unlock()
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("read did not wait for the accepted-root installation gap")
}

func TestTypedGraphReadOwnerRetriesPublicationChangedDuringCapture(t *testing.T) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	captured, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	typedGraphOwnerAfterSnapshotHook.Lock()
	typedGraphOwnerAfterSnapshotHook.afterCapture = func(c *Collection) {
		if c == col {
			once.Do(func() { close(captured); <-release })
		}
	}
	typedGraphOwnerAfterSnapshotHook.Unlock()
	defer func() {
		typedGraphOwnerAfterSnapshotHook.Lock()
		typedGraphOwnerAfterSnapshotHook.afterCapture = nil
		typedGraphOwnerAfterSnapshotHook.Unlock()
	}()
	done := make(chan error, 1)
	go func() {
		owner, err := col.openTypedGraphReadOwner(limits)
		if owner != nil {
			if len(owner.state.rows) != 1 || owner.state.rows[0].Values[1].String != "new" {
				err = errors.New("retry did not capture the newly installed generation")
			}
			err = errors.Join(err, owner.Close())
		}
		done <- err
	}()
	select {
	case <-captured:
	case <-time.After(10 * time.Second):
		t.Fatal("reader did not capture snapshot")
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"new"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := col.UpsertTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reader failed to retry an old snapshot with newer installed state")
	}
}

func TestTypedGraphReadOwnerInstallationGapWaiters(t *testing.T) {
	for _, outcome := range []string{"installed", "invalidated", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
			defer base.Close()
			limits := typedGraphOverlapLimits()
			if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20}, limits.Cold); err != nil {
				t.Fatal(err)
			}
			coord := col.collectionSchemaCoordinator()
			accepted := make(chan *typedGraphPublicationCandidate, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			typedGraphPublicationAfterAcceptedHook.Lock()
			typedGraphPublicationAfterAcceptedHook.fn = func(p *typedGraphPublicationCandidate) {
				if p.coord == coord {
					accepted <- p
					<-release
				}
			}
			typedGraphPublicationAfterAcceptedHook.Unlock()
			defer func() {
				typedGraphPublicationAfterAcceptedHook.Lock()
				typedGraphPublicationAfterAcceptedHook.fn = nil
				typedGraphPublicationAfterAcceptedHook.Unlock()
			}()
			changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"new"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
			written := make(chan error, 1)
			go func() { _, err := col.UpsertTypedBatch(ids[:1], retained[:1], changed); written <- err }()
			var candidate *typedGraphPublicationCandidate
			select {
			case candidate = <-accepted:
			case <-time.After(10 * time.Second):
				t.Fatal("writer did not reach accepted-root gap")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 3)
			for range 3 {
				go func() {
					owner, err := col.openTypedGraphReadOwnerWithContext(ctx, limits)
					if owner != nil {
						if len(owner.state.rows) != 1 || owner.state.rows[0].Values[1].String != "new" {
							err = errors.New("read admitted stale publication state")
						}
						err = errors.Join(err, owner.Close())
					}
					done <- err
				}()
			}
			waitTypedGraphReadAdmissionWaiter(t, coord)
			barrierCtx, barrierCancel := context.WithTimeout(context.Background(), time.Second)
			defer barrierCancel()
			if err := WithVectorPartitionStorageBarrierWithContextV1(barrierCtx, col.db.Dir(), func() error { return nil }); err != nil {
				t.Fatalf("gap waiter retained storage barrier: %v", err)
			}
			var want error
			switch outcome {
			case "installed":
				unblock()
			case "invalidated":
				candidate.invalidate()
				want = ErrVectorIndexSnapshotMismatch
			case "cancelled":
				cancel()
				want = context.Canceled
			}
			for range 3 {
				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Fatalf("wait result=%v want=%v", err, want)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("publication waiter was not released")
				}
			}
			unblock()
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTypedGraphReadOwnerCloseWakesPublicationWaiter(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	// Model a valid-but-stale frontier with unfinished debt, without a writer
	// that would itself prevent DB close from reaching the coordinator hook.
	before := coord.typedPublication.Load()
	stale := *before
	stale.catalog = cloneCatalogWithRootUpdates(before.catalog, before.catalog.meta, []string{collectionPrimaryRootName(col.Name())}, []uint64{0})
	coord.typedPublication.Store(&stale)
	coord.typedPublicationDebtMu.Lock()
	coord.typedPublicationPending.rows = 1
	coord.typedPublicationDebtMu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := col.openTypedGraphReadOwner(limits); done <- err }()
	waitTypedGraphReadAdmissionWaiter(t, coord)
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		// AcquireSnapshot can lose the close race after the explicit closing
		// check; both existing closed-admission errors reject the read.
		if !errors.Is(err, backenddb.ErrClosed) && !errors.Is(err, ErrVectorIndexSnapshotMismatch) {
			t.Fatalf("closed waiter=%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close left publication waiter blocked")
	}
}
