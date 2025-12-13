package treedb

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/page"
	"github.com/snissn/gomap/TreeDB/internal/tree"
)

func TestBatchInvalidReuse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Set([]byte("k2"), []byte("v2")); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("expected ErrBatchClosed after write, got %v", err)
	}
	if err := b.Write(); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("expected ErrBatchClosed on second write, got %v", err)
	}
	if _, err := b.GetByteSize(); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("expected ErrBatchClosed on GetByteSize, got %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close idempotent: %v", err)
	}
}

func TestWriteSyncDurabilityOrdering(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var (
		mu    sync.Mutex
		order []string
	)
	db.hooks = &dbHooks{
		slabSynced: func() {
			mu.Lock()
			order = append(order, "slab")
			mu.Unlock()
		},
		indexSynced: func() {
			mu.Lock()
			order = append(order, "index")
			mu.Unlock()
		},
	}

	val := bytes.Repeat([]byte("x"), 64)
	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte("k"), val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("writesync: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "slab" || got[1] != "index" {
		t.Fatalf("expected [slab index], got %v", got)
	}
}

func TestAtomicityPanicMidMerge(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := db.SetSync([]byte("base"), []byte("v0")); err != nil {
		t.Fatalf("set base: %v", err)
	}

	db.hooks = &dbHooks{panicAfterOps: 1}
	b := db.NewBatch().(*Batch)
	_ = b.Set([]byte("a"), []byte("va"))
	_ = b.Set([]byte("b"), []byte("vb"))

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic")
			}
		}()
		_ = b.Write()
	}()

	_ = db.Close()

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	v, err := db2.Get([]byte("base"))
	if err != nil || !bytes.Equal(v, []byte("v0")) {
		t.Fatalf("base value changed: %v %q", err, v)
	}
	if v, _ := db2.Get([]byte("a")); v != nil {
		t.Fatalf("unexpected partial commit for key a")
	}
	if v, _ := db2.Get([]byte("b")); v != nil {
		t.Fatalf("unexpected partial commit for key b")
	}
}

func TestLargeValueStoredOutOfLine(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, InlineThreshold: 4})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("y"), 128)
	if err := db.Set([]byte("k"), val); err != nil {
		t.Fatalf("set: %v", err)
	}

	out, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(out, val) {
		t.Fatalf("value mismatch")
	}

	snap, err := db.state.AcquireSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	st := snap.State()
	ut := tree.NewUserTree(db.pager, st.UserRootPageID)
	ent, err := ut.GetRaw([]byte("k"))
	if err != nil {
		t.Fatalf("getraw: %v", err)
	}
	if ent.Flags != page.LeafFlagPointer {
		t.Fatalf("expected pointer leaf flag, got %v", ent.Flags)
	}
}

