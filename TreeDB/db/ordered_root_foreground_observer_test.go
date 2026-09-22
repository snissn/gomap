package db

import (
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func registerOrderedRootForegroundLockWitness(t *testing.T, db *DB) (*atomic.Int64, func()) {
	t.Helper()
	var calls atomic.Int64
	unregister := db.RegisterLogicalOrderedRootPublicationObserver(func() {
		if !db.writeMu.TryLock() {
			t.Error("foreground observer ran while write serialization was held")
		} else {
			db.writeMu.Unlock()
		}
		if !db.commitMu.TryLock() {
			t.Error("foreground observer ran while commit serialization was held")
		} else {
			db.commitMu.Unlock()
		}
		calls.Add(1)
	})
	return &calls, unregister
}

func TestLogicalOrderedRootPublicationsNotifyForegroundObserverAfterCommit(t *testing.T) {
	t.Run("iterator", func(t *testing.T) {
		db := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
		defer db.Close()
		calls, unregister := registerOrderedRootForegroundLockWitness(t, db)
		defer unregister()

		rootDelta := mustFrozenSystemMemtable(t, "root/iterator", "value")
		_, _, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(
			[]OrderedRootDeltaPublishInput{{BaseRoot: 0, Iter: rootDelta.NewIterator(nil, nil)}},
			func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenSystemMemtable(t, "system/iterator", "published").NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("PublishOrderedRootDeltaGroupWithSystemDeltaBuilder: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("foreground notifications=%d want 1", got)
		}
	})

	t.Run("batch", func(t *testing.T) {
		db := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
		defer db.Close()
		if _, _, err := db.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "system/seed", "seed").NewIterator(nil, nil), nil
		}); err != nil {
			t.Fatalf("seed system root: %v", err)
		}
		calls, unregister := registerOrderedRootForegroundLockWitness(t, db)
		defer unregister()

		rootIter := mustFrozenSystemMemtable(t, "root/batch", "value").NewIterator(nil, nil)
		rootDelta, err := OrderedRootDeltaBatchFromIterator(rootIter)
		_ = rootIter.Close()
		if err != nil {
			t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
		}
		defer rootDelta.Close()
		_, _, err = db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
			[]OrderedRootDeltaBatchPublishInput{{BaseRoot: 0, Delta: rootDelta}},
			func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenSystemMemtable(t, "system/batch", "published").NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("foreground notifications=%d want 1", got)
		}
	})

	t.Run("staged-command-wal", func(t *testing.T) {
		dir := t.TempDir()
		enableCommandWALFormat(t, dir)
		db := openCommandWALDB(t, dir)
		defer db.Close()
		calls, unregister := registerOrderedRootForegroundLockWitness(t, db)

		db.teardownMu.RLock()
		db.commandWALRawPublishMu.Lock()
		_, _, err := db.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(
			nil,
			nil,
			mustRawKVCommandWALIntent(t, db, "cmd/staged", "1"),
			func(_ CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenSystemMemtable(t, "system/staged", "published").NewIterator(nil, nil), nil
			},
		)
		db.commandWALRawPublishMu.Unlock()
		db.teardownMu.RUnlock()
		if err != nil {
			t.Fatalf("PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("foreground notifications=%d want 1", got)
		}

		unregister()
		_, _, err = db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
			nil,
			mustRawKVCommandWALIntent(t, db, "cmd/unregistered", "1"),
			func(_ CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenSystemMemtable(t, "system/unregistered", "published").NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("publish after unregister: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("foreground notifications after unregister=%d want 1", got)
		}
	})
}
