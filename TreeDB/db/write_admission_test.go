package db

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
)

func runAfterCloseWinsWriteAdmission(t *testing.T, db *DB, call func() error) {
	t.Helper()

	db.writeMu.RLock()
	readLockHeld := true
	db.closing.Store(false)
	t.Cleanup(func() {
		db.closing.Store(false)
		if readLockHeld {
			db.writeMu.RUnlock()
		}
	})

	done := make(chan error, 1)
	go func() { done <- call() }()

	deadline := time.Now().Add(5 * time.Second)
	for db.writeMu.TryRLock() {
		db.writeMu.RUnlock()
		select {
		case err := <-done:
			t.Fatalf("writer returned before queuing for writeMu: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not queue for writeMu")
		}
		runtime.Gosched()
	}

	db.closing.Store(true)
	db.writeMu.RUnlock()
	readLockHeld = false
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("writer error=%v, want %v", err, ErrClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer remained blocked after writeMu became available")
	}
	db.closing.Store(false)
}

func TestExclusiveWritersRejectCloseAfterQueuingForWriteMu(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	appliedLSN := db.meta.AppliedCommandLSN
	db.mu.RUnlock()

	newIter := func() iterator.UnsafeIterator {
		return &closeCountingUnsafeIterator{}
	}
	maintenanceRootDelta := mustFrozenSystemMemtable(t, "maintenance/root", "value")
	tests := []struct {
		name string
		call func() error
	}{
		{name: "commit", call: func() error { return db.Commit(userRoot) }},
		{name: "checkpoint", call: db.Checkpoint},
		{name: "compact-index", call: db.CompactIndex},
		{name: "compact-storage-plan", call: func() error {
			_, err := db.CompactStoragePlan(nil, CompactStorageOptions{})
			return err
		}},
		{name: "set-leaf-page-log", call: func() error {
			db.writeMu.RLock()
			beforeVersion := db.leafPageLogVersion
			db.writeMu.RUnlock()
			db.SetLeafPageLog(replayInlineLeafPageLog{})
			db.writeMu.RLock()
			unchanged := db.leafPageLogVersion == beforeVersion
			db.writeMu.RUnlock()
			if unchanged {
				return ErrClosed
			}
			return nil
		}},
		{name: "command-wal-roots", call: func() error {
			return db.publishCommandWALRoots(userRoot, systemRoot, appliedLSN, nil, false)
		}},
		{name: "system-root-iterator", call: func() error {
			_, err := db.PublishSystemRootIterator(newIter())
			return err
		}},
		{name: "ordered-root-iterator", call: func() error {
			_, err := db.PublishOrderedRootIterator(userRoot, newIter())
			return err
		}},
		{name: "ordered-root-group", call: func() error {
			_, _, err := db.PublishOrderedRootGroup(newIter(), nil)
			return err
		}},
		{name: "ordered-root-delta-group", call: func() error {
			_, _, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
				return nil, errors.New("system builder ran after Close won write admission")
			})
			return err
		}},
		{name: "storage-maintenance-ordered-root-delta-group", call: func() error {
			_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
				storagemaintenance.ColumnAssetRewritePlan(),
				[]StorageMaintenanceRootDeltaPublishInput{{
					BaseRoot: 0,
					Iter:     maintenanceRootDelta.NewIterator(nil, nil),
				}},
				nil,
				func([]uint64) (iterator.UnsafeIterator, error) {
					return nil, errors.New("maintenance system builder ran after Close won write admission")
				},
			)
			return err
		}},
		{name: "ordered-root-delta-batch-group", call: func() error {
			_, _, err := db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized(nil, nil, nil, func([]uint64) (iterator.UnsafeIterator, error) {
				return nil, errors.New("system builder ran after Close won write admission")
			}, orderedRootCommandWALPublishOptions{})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runAfterCloseWinsWriteAdmission(t, db, test.call)
		})
	}
}

func TestCommandWALPublishDoesNotAppendAfterCloseWinsWriteAdmission(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer func() { _ = db.Close() }()

	tests := []struct {
		name string
		call func(*CommandWALIntent) error
	}{
		{name: "noop", call: func(intent *CommandWALIntent) error {
			return db.PublishStagedCommandWALNoop(intent, false)
		}},
		{name: "ordered-root-context", call: func(intent *CommandWALIntent) error {
			_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(nil, intent, func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
				return nil, errors.New("system builder ran after Close won write admission")
			})
			return err
		}},
		{name: "ordered-root-batch-context", call: func(intent *CommandWALIntent) error {
			_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(nil, intent, func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
				return nil, errors.New("system builder ran after Close won write admission")
			})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := mustRawKVCommandWALIntent(t, db, "close-race/"+test.name, "value")
			runAfterCloseWinsWriteAdmission(t, db, func() error { return test.call(intent) })
			if got := intent.AssignedLSN(); got != 0 {
				t.Fatalf("AssignedLSN=%d after rejected publish, want 0", got)
			}
			if got := db.State().AppliedCommandLSN; got != 0 {
				t.Fatalf("AppliedCommandLSN=%d after rejected publish, want 0", got)
			}
		})
	}
}

func TestCloseClearsCommandWALLeafPageLogAfterWriteAdmissionCloses(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if db.leafPageLog == nil {
		t.Fatal("command-WAL open did not install a leaf page log")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db.writeMu.RLock()
	leafPageLog := db.leafPageLog
	db.writeMu.RUnlock()
	if leafPageLog != nil {
		t.Fatalf("leaf page log retained after Close: %T", leafPageLog)
	}
}

func TestOptimisticOrderedRootPublishRejectsClosingAfterAdmissionPreflight(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		db.testOrderedRootBatchAfterClosePreflightHook = nil
		db.closing.Store(false)
		_ = db.Close()
	})

	preflightDone := make(chan struct{})
	releasePreflight := make(chan struct{})
	db.testOrderedRootBatchAfterClosePreflightHook = func() {
		close(preflightDone)
		<-releasePreflight
	}

	db.writeMu.Lock()
	publishDone := make(chan error, 1)
	go func() {
		_, _, _, err := db.tryPublishOrderedRootDeltaBatchGroupOptimistic(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return nil, errors.New("system builder ran after Close won shared write admission")
		})
		publishDone <- err
	}()
	select {
	case <-preflightDone:
	case <-time.After(5 * time.Second):
		db.writeMu.Unlock()
		t.Fatal("optimistic ordered-root publish did not complete its close preflight")
	}

	db.closing.Store(true)
	close(releasePreflight)
	db.writeMu.Unlock()

	select {
	case err := <-publishDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("optimistic ordered-root publish error=%v, want %v", err, ErrClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("optimistic ordered-root publish remained blocked after writeMu became available")
	}
}
