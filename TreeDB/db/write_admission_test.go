package db

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
		return mustFrozenSystemMemtable(t).NewIterator(nil, nil)
	}
	tests := []struct {
		name string
		call func() error
	}{
		{name: "commit", call: func() error { return db.Commit(userRoot) }},
		{name: "checkpoint", call: db.Checkpoint},
		{name: "compact-index", call: db.CompactIndex},
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
				t.Fatal("system builder ran after Close won write admission")
				return nil, nil
			})
			return err
		}},
		{name: "ordered-root-delta-batch-group", call: func() error {
			_, _, err := db.publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized(nil, nil, nil, func([]uint64) (iterator.UnsafeIterator, error) {
				t.Fatal("system builder ran after Close won write admission")
				return nil, nil
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

func TestCommandWALNoopDoesNotAppendAfterCloseWinsWriteAdmission(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer func() { _ = db.Close() }()

	intent := mustRawKVCommandWALIntent(t, db, "close-race", "value")
	runAfterCloseWinsWriteAdmission(t, db, func() error {
		return db.PublishStagedCommandWALNoop(intent, false)
	})
	if got := intent.AssignedLSN(); got != 0 {
		t.Fatalf("AssignedLSN=%d after rejected publish, want 0", got)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d after rejected publish, want 0", got)
	}
}
