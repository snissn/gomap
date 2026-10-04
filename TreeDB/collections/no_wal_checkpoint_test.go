package collections

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNoWALFastBoundaryDrainsAcknowledgedCollectionBuffers(t *testing.T) {
	for _, boundary := range []string{"checkpoint", "compact_storage", "set_sync", "delete_sync", "batch_sync", "empty_batch_sync", "update_sync", "conditional_sync", "empty_conditional_sync", "noop_update_sync", "close"} {
		t.Run(boundary, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
			opts.DisableSideStores = true
			opts.DisableBackgroundPrune = true
			opts.BackgroundCheckpointInterval = -1
			d, closeDB, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatal(err)
			}
			closeOnce := collectionMaintenanceCloseOnce(closeDB)
			t.Cleanup(func() { _ = closeOnce() })
			docs := map[string][]byte{}
			// Separate managers exercise the registry, rather than one manager's FlushAll.
			for _, name := range []string{"plain", "indexed"} {
				mgr := NewCollectionManager(d)
				meta := &CollectionMeta{Name: name, Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}
				if name == "indexed" {
					meta.Indexes = []IndexDefinition{{Name: "name", Field: "name", ValueType: IndexValueString}}
				}
				if _, err := mgr.CreateCollection(meta); err != nil {
					t.Fatal(err)
				}
				col, err := mgr.OpenCollection(name)
				if err != nil {
					t.Fatal(err)
				}
				doc := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "ada"}})
				docs[name] = doc
				if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
					t.Fatal(err)
				}
				col.writeDomain.mu.RLock()
				pending := col.writeDomain.count != 0 || hasBufferedIndexedPendingWrites(col.writeDomain)
				col.writeDomain.mu.RUnlock()
				if !pending {
					t.Fatal("fixture did not buffer", name)
				}
			}
			model, err := powerlossoracle.Capture(dir)
			if err != nil {
				t.Fatal(err)
			}
			var modelMu sync.Mutex
			restoreHook := durabilitycut.Install(func(event durabilitycut.Event) error {
				modelMu.Lock()
				defer modelMu.Unlock()
				return model.Observe(dir, event)
			})
			restore := sync.OnceFunc(restoreHook)
			defer restore()
			switch boundary {
			case "checkpoint":
				err = d.Checkpoint()
			case "compact_storage":
				_, err = d.CompactStorage(context.Background(), backenddb.CompactStorageOptions{})
			case "set_sync":
				err = d.SetSync([]byte("later"), []byte("durable"))
			case "delete_sync":
				err = d.DeleteSync([]byte("later"))
			case "batch_sync", "empty_batch_sync":
				batch := d.NewBatch()
				if boundary == "batch_sync" {
					if e := batch.Set([]byte("later"), []byte("durable")); e != nil {
						t.Fatal(e)
					}
				}
				err = batch.WriteSync()
				_ = batch.Close()
			case "update_sync":
				err = d.UpdateSync([]byte("later"), func([]byte) (backenddb.UpdateResult, error) { return backenddb.SetUpdate([]byte("durable")), nil })
			case "conditional_sync":
				tx, e := d.NewConditionalTxn()
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Close()
				if e = tx.Set([]byte("later"), []byte("durable")); e != nil {
					t.Fatal(e)
				}
				err = tx.CommitSync()
			case "noop_update_sync":
				err = d.UpdateSync([]byte("later"), func([]byte) (backenddb.UpdateResult, error) { return backenddb.NoopUpdate(), nil })
			case "empty_conditional_sync":
				tx, e := d.NewConditionalTxn()
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Close()
				err = tx.CommitSync()
			case "close":
				err = closeOnce()
			}
			if err != nil {
				t.Fatal(err)
			}
			// Reopen the modeled stable image before Close can drain anything.
			// Preserve resource identities through the existing oracle scope.
			restore()
			replayDir := t.TempDir()
			if err := model.MaterializeStable(replayDir); err != nil {
				t.Fatal(err)
			}
			release, err := model.InstallStableIdentityOverrides(replayDir)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			opts.Dir = replayDir
			opts.ReadOnly = true
			reopened, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			for name, want := range docs {
				col, err := NewCollectionManager(reopened).OpenCollection(name)
				if err != nil {
					t.Fatalf("reopen %s: %v (backend=%s original=%s)", name, err, reopened.Dir(), d.Dir())
				}
				got, err := col.Get([]byte("u1"))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s lost earlier acknowledged %s collection row: got %x want %x", boundary, name, got, want)
				}
			}
			if boundary == "set_sync" || boundary == "batch_sync" {
				got, err := reopened.Get([]byte("later"))
				if err != nil || !bytes.Equal(got, []byte("durable")) {
					t.Fatalf("later sync KV=%q err=%v", got, err)
				}
			}
		})
	}
}

func TestNoWALCheckpointWaitsForIndexedAsyncPublisher(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"hnl"}`)}); err != nil {
		t.Fatal(err)
	}
	entered, releaseWait := collectionWaitIndexedAsyncFlushGateForTest(t)
	if !col.writeDomain.beginIndexedAsyncFlush() {
		t.Fatal("async publisher already active")
	}
	finish := sync.OnceFunc(func() { col.writeDomain.finishIndexedAsyncFlush(nil) })
	defer func() { releaseWait(); finish() }()
	done := make(chan error, 1)
	go func() { done <- d.Checkpoint() }()
	select {
	case <-entered:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("checkpoint did not wait for async publisher")
	}
	select {
	case err := <-done:
		t.Fatalf("checkpoint returned before publisher finished: %v", err)
	default:
	}
	releaseWait()
	finish()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("checkpoint drain deadlocked")
	}
	if got := collectionCheckpointBoundaryPendingCountForTest(t, col); got != 0 {
		t.Fatalf("pending=%d", got)
	}
}

// Volatile collection ACKs are not a global ordinary-operation prefix. The
// autonomous publisher can seal later raw KV while a local row is unflushed;
// explicit boundaries above must close this gap before returning successfully.
func TestNoWALVolatileCollectionAckDoesNotImplyGlobalOrdinaryPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("earlier")}, [][]byte{[]byte(`{"name":"ada"}`)}); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var snapshot *powerlossoracle.Model
	sealed := make(chan struct{})
	signal := sync.OnceFunc(func() { close(sealed) })
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		mu.Lock()
		defer mu.Unlock()
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			snapshot = model.Clone()
			signal()
		}
		return nil
	})
	defer restore()
	if err := d.Set([]byte("later"), []byte("visible")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sealed:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("ordinary publication was not autonomously sealed")
	}
	mu.Lock()
	image := snapshot
	mu.Unlock()
	replay := t.TempDir()
	if err := image.MaterializeStable(replay); err != nil {
		t.Fatal(err)
	}
	release, err := image.InstallStableIdentityOverrides(replay)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts.Dir = replay
	opts.ReadOnly = true
	reopened, closeReopened, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReopened()
	got, err := reopened.Get([]byte("later"))
	if err != nil || !bytes.Equal(got, []byte("visible")) {
		t.Fatalf("later raw KV=%q err=%v", got, err)
	}
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	got, err = reopenedCol.Get([]byte("earlier"))
	if err != nil || len(got) != 0 {
		t.Fatalf("unflushed volatile earlier row=%q err=%v", got, err)
	}
}

func TestNoWALCheckpointCoversPriorCollectionWritesWithoutChasingRefill(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	d, closeDB, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closeDB() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "ada"}})
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("prior")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	var modelMu sync.Mutex
	restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
		modelMu.Lock()
		defer modelMu.Unlock()
		return model.Observe(dir, event)
	}))
	defer restore()
	observed := 0
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
		observed++
		if observed != 1 {
			return errors.New("checkpoint chased later collection refill")
		}
		return &commandwalbarrier.PendingDrain{Drain: func() error {
			_, err := col.InsertBatchValidatedBSON([][]byte{[]byte("later")}, [][]byte{doc})
			return err
		}}
	})
	defer unregister()
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	col.writeDomain.mu.RLock()
	pending := col.writeDomain.count
	col.writeDomain.mu.RUnlock()
	if pending != 1 || observed != 1 {
		t.Fatalf("pending=%d observed=%d want later refill retained, one hook invocation", pending, observed)
	}
	restore()
	replayDir := t.TempDir()
	if err := model.MaterializeStable(replayDir); err != nil {
		t.Fatal(err)
	}
	release, err := model.InstallStableIdentityOverrides(replayDir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts.Dir, opts.ReadOnly = replayDir, true
	reopened, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	stableCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := stableCol.Get([]byte("prior")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("prior acknowledged collection write=%x err=%v", got, err)
	}
	if got, err := stableCol.Get([]byte("later")); err != nil || len(got) != 0 {
		t.Fatalf("later buffered refill=%x err=%v want absent from stable image", got, err)
	}
}

func TestNoWALCheckpointBoundsActiveIndexedAsyncFrontier(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON, BufferedIndexedWrites: true, BufferedIndexedAsyncFlush: true},
		Indexes: []IndexDefinition{{Name: "name", Field: "name", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "ada"}})
	insert := func(key string) {
		t.Helper()
		if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte(key)}, [][]byte{doc}); err != nil {
			t.Fatal(err)
		}
	}
	insert("prepared")
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil || work == nil {
		t.Fatalf("prepared work=%v err=%v", work, err)
	}
	defer collectionTestCloseIndexedFlushWork(work)
	domain := col.writeDomain
	if !domain.beginIndexedAsyncFlush() {
		t.Fatal("worker already active")
	}
	finish := sync.OnceFunc(func() { domain.finishIndexedAsyncFlush(nil) })
	defer finish()
	insert("queued_before")
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	var modelMu sync.Mutex
	restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
		modelMu.Lock()
		defer modelMu.Unlock()
		return model.Observe(dir, event)
	}))
	defer restore()
	entered, releaseWait := collectionWaitIndexedAsyncFlushGateForTest(t)
	defer releaseWait()
	done := make(chan struct{})
	var boundaryErr error
	go func() {
		boundaryErr = d.Checkpoint()
		close(done)
	}()
	t.Cleanup(func() {
		releaseWait()
		finish()
		select {
		case <-done:
		case <-time.After(collectionTestTimeout(t, 5*time.Second)):
			t.Error("checkpoint did not terminate")
		}
	})
	select {
	case <-entered:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("checkpoint did not wait for prepared publication")
	}
	releaseWait()
	insert("queued_later")
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatal(err)
	}
	domain.mu.Lock()
	rotateIndexedMutableToFlushUnitForAsyncLocked(domain)
	scheduled := col.scheduleIndexedAsyncFlush(domain)
	deferred := domain.indexedAsyncFlushDeferred
	domain.mu.Unlock()
	if !scheduled || !deferred {
		t.Fatalf("scheduled=%v deferred=%v want deferred post-entry refill", scheduled, deferred)
	}
	// Continue the real worker loop after its prepared publication. It must
	// yield the queued tail rather than chase writes admitted during the wait.
	if err := flushCollectionWriteDomainAsync(d, domain); err != nil {
		t.Fatal(err)
	}
	domain.mu.RLock()
	pending := domain.count
	domain.mu.RUnlock()
	if pending != 2 {
		t.Fatalf("worker consumed queued tail: pending=%d want 2 for synchronous drain", pending)
	}
	finish()
	select {
	case <-done:
		if boundaryErr != nil {
			t.Fatal(boundaryErr)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("checkpoint chased queued async work")
	}
	restore()
	replayDir := t.TempDir()
	if err := model.MaterializeStable(replayDir); err != nil {
		t.Fatal(err)
	}
	release, err := model.InstallStableIdentityOverrides(replayDir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts.Dir, opts.ReadOnly = replayDir, true
	reopened, closeReopened, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReopened()
	stableCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"prepared", "queued_before"} {
		if got, err := stableCol.Get([]byte(key)); err != nil || !bytes.Equal(got, doc) {
			t.Fatalf("pre-entry ACK %s=%x err=%v", key, got, err)
		}
	}
}

func TestNoWALIndexedAsyncDrainDefersSiblingWorkAndResumesAfterLastWaiter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users", Options: CollectionOptions{BufferedIndexedWrites: true, BufferedIndexedAsyncFlush: true},
		Indexes: []IndexDefinition{{Name: "name", Field: "name", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatal(err)
	}
	own, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	resumeFirst := mgr.holdIndexedAsyncFlushForSync([]*collectionWriteDomain{own.writeDomain})
	resumeFirstOnce := sync.OnceFunc(resumeFirst)
	defer resumeFirstOnce()
	resumeLast := mgr.holdIndexedAsyncFlushForSync([]*collectionWriteDomain{own.writeDomain})
	resumeLastOnce := sync.OnceFunc(resumeLast)
	defer resumeLastOnce()
	// Registration occurs after both pauses. A fixed domain snapshot would
	// miss this sibling and admit its worker during the boundary.
	sibling, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if sibling.writeDomain.beginIndexedAsyncFlush() {
		sibling.writeDomain.finishIndexedAsyncFlush(nil)
		t.Fatal("sibling worker admitted while synchronous drain was waiting")
	}
	if _, err := sibling.InsertBatch([][]byte{[]byte("later")}, [][]byte{[]byte(`{"name":"ada"}`)}); err != nil {
		t.Fatal(err)
	}
	domain := sibling.writeDomain
	domain.mu.Lock()
	rotateIndexedMutableToFlushUnitForAsyncLocked(domain)
	accepted := sibling.scheduleIndexedAsyncFlush(domain)
	deferred := domain.indexedAsyncFlushDeferred
	domain.mu.Unlock()
	if !accepted || !deferred || domain.indexedAsyncFlushRunning() {
		t.Fatal("queued work was not deferred")
	}
	resumeFirstOnce()
	if !domain.indexedSyncDrainPending() || domain.indexedAsyncFlushRunning() {
		t.Fatal("first release bypassed the remaining synchronous waiter")
	}
	resumeLastOnce()
	domain.waitIndexedAsyncFlush()
	domain.mu.RLock()
	pending := domain.count
	domain.mu.RUnlock()
	if pending != 0 || domain.indexedSyncDrainPending() {
		t.Fatalf("deferred work stranded: pending=%d", pending)
	}
}
