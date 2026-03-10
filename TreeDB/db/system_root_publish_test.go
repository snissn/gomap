package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestPublishSystemRootIterator_PersistsAndPreservesUserRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}

	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte("sys/a"), []byte("sv"))
	mt.Freeze()

	newSystemRoot, err := db.PublishSystemRootIterator(mt.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish system root: %v", err)
	}

	after := db.State()
	if after == nil {
		t.Fatal("expected state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != newSystemRoot {
		t.Fatalf("system root=%d want %d", after.SystemRootPageID, newSystemRoot)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	entry, err := snap.GetEntryAtRoot(after.SystemRootPageID, []byte("sys/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(system): %v", err)
	}
	if got := string(entry.Value); got != "sv" {
		t.Fatalf("system value=%q want %q", got, "sv")
	}
	if got, err := snap.Get([]byte("user/a")); err != nil || string(got) != "uv" {
		t.Fatalf("user get got=%q err=%v want uv", string(got), err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	reopenSnap := reopened.AcquireSnapshot()
	if reopenSnap == nil {
		t.Fatal("expected reopen snapshot")
	}
	defer reopenSnap.Close()
	reopenState := reopened.State()
	if reopenState == nil {
		t.Fatal("expected reopen state")
	}
	entry, err = reopenSnap.GetEntryAtRoot(reopenState.SystemRootPageID, []byte("sys/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(system): %v", err)
	}
	if got := string(entry.Value); got != "sv" {
		t.Fatalf("reopen system value=%q want %q", got, "sv")
	}
}

func TestPublishSystemRootIterator_DoesNotCreateBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	called := 0
	db.testBatchCreateHook = func() { called++ }

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte("sys/a"), []byte("sv"))
	mt.Freeze()

	if _, err := db.PublishSystemRootIterator(mt.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system root: %v", err)
	}
	if called != 0 {
		t.Fatalf("testBatchCreateHook called %d times; want 0", called)
	}
}
