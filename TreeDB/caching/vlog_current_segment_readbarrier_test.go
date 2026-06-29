package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type valueLogReadBarrierTrackingTreeDB struct {
	*treedb.DB
	barrierWithSize   func(fileID uint32) (int64, error)
	barrierCalls      atomic.Int64
	lastBarrierFileID atomic.Uint32
}

func (b *valueLogReadBarrierTrackingTreeDB) SetCurrentValueLogReadBarrierWithSize(fn func(fileID uint32) (int64, error)) {
	if fn == nil {
		b.barrierWithSize = nil
		b.DB.SetCurrentValueLogReadBarrierWithSize(nil)
		return
	}
	wrapped := func(fileID uint32) (int64, error) {
		b.barrierCalls.Add(1)
		b.lastBarrierFileID.Store(fileID)
		return fn(fileID)
	}
	b.barrierWithSize = wrapped
	b.DB.SetCurrentValueLogReadBarrierWithSize(wrapped)
}

func TestReadValueLogAppend_FlushesCurrentSegmentBufferedTailEvenWhenDirtyBitCleared(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 30,
		MemtableShards:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	l := &db.lanes[0]
	value := []byte("buffered-tail-value")
	ptr, _, err := db.appendValueLogOneInternal(l, 0, nil, 1, value, journalDurabilityNone, false)
	if err != nil {
		t.Fatalf("appendValueLogOneInternal: %v", err)
	}

	l.vlogMu.Lock()
	pending := 0
	if w, ok := l.vlog.(interface{ PendingBytes() int }); ok {
		pending = w.PendingBytes()
	}
	l.vlogMu.Unlock()
	if pending == 0 {
		t.Fatalf("expected pending buffered tail bytes after append")
	}

	// Reproduce the visibility hole we saw live: a pointer is readable through
	// the cache path while the writer still holds the grouped record in appendBuf.
	l.vlogDirty.Store(false)

	got, err := db.readValueLogAppend([]byte("k"), ptr, nil)
	if err != nil {
		t.Fatalf("readValueLogAppend: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("value=%q want=%q", got, value)
	}
	if l.vlogDirty.Load() {
		t.Fatalf("expected vlogDirty=false after flush barrier")
	}
}

func TestBackendReadFlushesCurrentSegmentBufferedTailForGroupedPointer(t *testing.T) {
	dir := t.TempDir()
	backendDir := filepath.Join(dir, "backend")
	if err := os.MkdirAll(backendDir, 0o755); err != nil {
		t.Fatalf("mkdir backend dir: %v", err)
	}
	backend, err := treedb.Open(treedb.Options{
		Dir: backendDir,
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	db, err := Open(dir, backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 30,
		MemtableShards:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	backendOwnedByDB = true
	defer func() { _ = db.Close() }()

	l := &db.lanes[0]
	want0 := bytes.Repeat([]byte("a"), 1024)
	want1 := bytes.Repeat([]byte("b"), 1024)
	ptrs, err := db.appendValueLog(l, 0, nil, []valuelog.Record{
		{RID: 1, Value: want0},
		{RID: 2, Value: want1},
	}, journalDurabilityNone)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	defer putValueLogPtrs(ptrs)
	if got, want := len(ptrs), 2; got != want {
		t.Fatalf("ptr count=%d want %d", got, want)
	}
	if !page.ValuePtrIsGrouped(ptrs[0]) {
		t.Fatalf("expected grouped pointer, got %+v", ptrs[0])
	}

	l.vlogMu.Lock()
	pending := 0
	if w, ok := l.vlog.(interface{ PendingBytes() int }); ok {
		pending = w.PendingBytes()
	}
	l.vlogMu.Unlock()
	if pending == 0 {
		t.Fatalf("expected pending buffered tail bytes after grouped append")
	}

	b := backend.NewBatch().(*treedb.Batch)
	if err := b.SetPointer([]byte("k0"), ptrs[0]); err != nil {
		t.Fatalf("SetPointer(k0): %v", err)
	}
	if err := b.SetPointer([]byte("k1"), ptrs[1]); err != nil {
		t.Fatalf("SetPointer(k1): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("backend Write: %v", err)
	}
	_ = b.Close()

	// Match the real durability-none path: backend-visible pointer keys exist
	// while the current segment tail may still be buffered in-process.
	db.backendReadVlogDirtySeq.Add(1)

	gotBackend0, err := backend.Get([]byte("k0"))
	if err != nil {
		t.Fatalf("backend.Get(k0): %v", err)
	}
	if !bytes.Equal(gotBackend0, want0) {
		t.Fatalf("backend.Get(k0) len=%d want %d", len(gotBackend0), len(want0))
	}

	gotBackend1, err := backend.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("backend.Get(k1): %v", err)
	}
	if !bytes.Equal(gotBackend1, want1) {
		t.Fatalf("backend.Get(k1) len=%d want %d", len(gotBackend1), len(want1))
	}

	got0, err := db.Get([]byte("k0"))
	if err != nil {
		t.Fatalf("Get(k0): %v", err)
	}
	if !bytes.Equal(got0, want0) {
		t.Fatalf("Get(k0) len=%d want %d", len(got0), len(want0))
	}

	got1, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get(k1): %v", err)
	}
	if !bytes.Equal(got1, want1) {
		t.Fatalf("Get(k1) len=%d want %d", len(got1), len(want1))
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	gotSnap0, err := snap.Get([]byte("k0"))
	if err != nil {
		t.Fatalf("Snapshot.Get(k0): %v", err)
	}
	if !bytes.Equal(gotSnap0, want0) {
		t.Fatalf("Snapshot.Get(k0) len=%d want %d", len(gotSnap0), len(want0))
	}

	gotSnap1, err := snap.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Snapshot.Get(k1): %v", err)
	}
	if !bytes.Equal(gotSnap1, want1) {
		t.Fatalf("Snapshot.Get(k1) len=%d want %d", len(gotSnap1), len(want1))
	}
}

func TestCachedModeValueLogPointerReadBarrierResolvesBackendRootRead(t *testing.T) {
	dir := t.TempDir()
	backendDir := filepath.Join(dir, "backend")
	if err := os.MkdirAll(backendDir, 0o755); err != nil {
		t.Fatalf("mkdir backend dir: %v", err)
	}
	rawBackend, err := treedb.Open(treedb.Options{Dir: backendDir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backend := &valueLogReadBarrierTrackingTreeDB{DB: rawBackend}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	db, err := Open(dir, backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		FlushThreshold:           1 << 30,
		MemtableShards:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	backendOwnedByDB = true
	defer func() { _ = db.Close() }()
	if backend.barrierWithSize == nil {
		t.Fatalf("expected cached open to install backend value-log read barrier")
	}

	l := &db.lanes[0]
	want := bytes.Repeat([]byte("r"), 1024)
	ptr, _, err := db.appendValueLogOneInternal(l, 0, nil, 1, want, journalDurabilityNone, false)
	if err != nil {
		t.Fatalf("appendValueLogOneInternal: %v", err)
	}
	l.vlogMu.Lock()
	pending := 0
	if w, ok := l.vlog.(interface{ PendingBytes() int }); ok {
		pending = w.PendingBytes()
	}
	l.vlogMu.Unlock()
	if pending == 0 {
		t.Fatalf("expected pending buffered tail bytes after append")
	}

	b := backend.NewBatch().(*treedb.Batch)
	if err := b.SetPointer([]byte("root/doc"), ptr); err != nil {
		t.Fatalf("SetPointer(root/doc): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("backend Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("backend batch close: %v", err)
	}

	l.backendReadDirtySeq.Add(1)
	db.backendReadVlogDirtySeq.Add(1)
	if got := backend.barrierCalls.Load(); got != 0 {
		t.Fatalf("backend read barrier calls before backend.Get=%d want 0", got)
	}

	got, err := backend.Get([]byte("root/doc"))
	if err != nil {
		t.Fatalf("backend.Get(root/doc): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("backend value len=%d want %d", len(got), len(want))
	}
	if got := backend.barrierCalls.Load(); got == 0 {
		t.Fatalf("backend.Get did not invoke backend value-log read barrier")
	}
	if got := backend.lastBarrierFileID.Load(); got != ptr.FileID {
		t.Fatalf("backend read barrier fileID=%d want %d", got, ptr.FileID)
	}

	gotCached, err := db.Get([]byte("root/doc"))
	if err != nil {
		t.Fatalf("Get(root/doc): %v", err)
	}
	if !bytes.Equal(gotCached, want) {
		t.Fatalf("cached value len=%d want %d", len(gotCached), len(want))
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer snap.Close()
	gotSnap, err := snap.Get([]byte("root/doc"))
	if err != nil {
		t.Fatalf("Snapshot.Get(root/doc): %v", err)
	}
	if !bytes.Equal(gotSnap, want) {
		t.Fatalf("snapshot value len=%d want %d", len(gotSnap), len(want))
	}
}
