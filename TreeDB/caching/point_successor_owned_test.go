package caching

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

// Observe the real concrete capability without changing its capture or output.
// The wrapper preserves optional capability isolation: old generic-only wrappers
// in the existing publication/physical-delete schedule controls still fall back.
type ownedSuccessorBackend struct {
	*backenddb.DB
	ownedCalls  atomic.Uint64
	cursorCalls atomic.Uint64
	lastRoot    atomic.Uint64
	refusal     atomic.Bool
	entered     chan struct{}
	resume      chan struct{}
	barrierOnce sync.Once
	ownedError  error
}

var errOwnedSuccessorTestRefusal = errors.New("owned backend read refused")

func (b *ownedSuccessorBackend) SeekGEOwnedAtRoot(root uint64, start, end []byte) (node.LeafEntry, bool, error) {
	b.ownedCalls.Add(1)
	b.lastRoot.Store(root)
	if b.ownedError != nil {
		return node.LeafEntry{}, false, b.ownedError
	}
	if b.refusal.Load() {
		return node.LeafEntry{}, false, errOwnedSuccessorTestRefusal
	}
	if b.entered != nil {
		b.barrierOnce.Do(func() { close(b.entered); <-b.resume })
	}
	return b.DB.SeekGEOwnedAtRoot(root, start, end)
}

func (b *ownedSuccessorBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	b.cursorCalls.Add(1)
	return b.DB.Iterator(start, end)
}

func TestSeekGEVersionRangeOwnedBackendOutputRootAndFallback(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	lower, _ := mvcckey.Encode([]byte("one-key"), 100)
	oldKey, _ := mvcckey.Encode([]byte("one-key"), 10)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, []byte("one-key"))
	if err := backend.Set(oldKey, []byte("old")); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "btree", FlushThreshold: 1 << 30, MemtableShards: 4, MaxQueuedMemtables: -1})
	if err != nil {
		t.Fatal(err)
	}
	cacheClosed := false
	defer func() {
		if !cacheClosed {
			_ = db.Close()
		}
	}()
	// The maintained one-time range discovery owns its cursors; isolate the
	// synchronous successor itself only after that ordinary setup is complete.
	if err := db.ensureBackendRange(); err != nil {
		t.Fatal(err)
	}
	observed := &ownedSuccessorBackend{DB: backend}
	db.backend = observed
	key, value, found, err := db.SeekGEVersionRange(lower, upper)
	if err != nil || !found || !bytes.Equal(key, oldKey) || string(value) != "old" {
		t.Fatal(key, value, found, err)
	}
	if observed.ownedCalls.Load() != 1 || observed.cursorCalls.Load() != 0 {
		t.Fatal("shared attempt did not use synchronous capture")
	}
	key[0], value[0] = 'X', 'X'
	if _, got, _, err := db.SeekGEVersionRange(lower, upper); err != nil || string(got) != "old" {
		t.Fatal(got, err)
	}
	// Publish a real frozen iterator root through the maintained grouped-root
	// publisher. Ordinary Checkpoint alone need not install root-domain state.
	newer, _ := mvcckey.Encode([]byte("one-key"), 80)
	if err := db.Set(newer, []byte("newer")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rootTable := newRootDomainTestTable(t, rootDomainTestOp{key: string(newer), value: "newer"})
	db.mu.Lock()
	db.rootIteratorState.immutables = []memtable.Table{rootTable}
	db.mu.Unlock()
	if err := db.publishInstalledRootSet(&publishedRootSet{generation: 1, iterator: publishedRootRef{}}); err != nil {
		t.Fatal("publish real iterator root", err)
	}
	view := db.retainMemtableView()
	snap, ok := liveIteratorRootDomainSnapshot(view)
	if !ok || snap.publishedRootID == 0 || snap.published != nil {
		t.Fatal("fixture did not publish concrete backend root", snap)
	}
	root := snap.publishedRootID
	db.releaseMemtableView(view)
	_, got, _, err := db.SeekGEVersionRange(lower, upper)
	if err != nil || string(got) != "newer" || observed.lastRoot.Load() != root {
		t.Fatal("published root selection", got, err, observed.lastRoot.Load(), root)
	}
	calls := observed.ownedCalls.Load()
	if _, _, _, err := db.SeekGE(lower, upper); err != nil {
		t.Fatal(err)
	}
	if observed.ownedCalls.Load() != calls {
		t.Fatal("generic seek acquired synchronous capability")
	}
	observed.refusal.Store(true)
	if _, _, found, err := db.SeekGEVersionRange(lower, upper); found || !errors.Is(err, errOwnedSuccessorTestRefusal) {
		t.Fatal("owned failure was hidden", found, err)
	}
	observed.refusal.Store(false)
	// Concurrent attempts independently own their captured bytes. A point writer
	// may enter under the shared gate, but cannot invalidate returned buffers.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				k, v, found, err := db.SeekGEVersionRange(lower, upper)
				if err != nil || !found || !bytes.Equal(k, newer) || string(v) != "newer" {
					t.Errorf("concurrent read %q %q %t %v", k, v, found, err)
					return
				}
				k[0], v[0] = 'X', 'X'
			}
		}()
	}
	for i := range 20 {
		if err := db.Set([]byte("unrelated"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	closeErr := db.Close()
	cacheClosed = true
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
}

// The optional concrete capture must keep the original shared publication gate:
// point writers proceed, while Close waits until capture/output copying finishes.
func TestSeekGEVersionRangeOwnedBackendWriterAndCloseExclusion(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	lower, _ := mvcckey.Encode([]byte("one-key"), 100)
	oldKey, _ := mvcckey.Encode([]byte("one-key"), 10)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, []byte("one-key"))
	if err := backend.Set(oldKey, []byte("old")); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "btree", FlushThreshold: 1 << 30, MemtableShards: 4, MaxQueuedMemtables: -1})
	if err != nil {
		t.Fatal(err)
	}
	resume := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	closeDone := make(chan struct{})
	closeStarted := false
	defer func() {
		release()
		if closeStarted {
			<-closeDone
		} else {
			_ = db.Close()
		}
	}()
	if err := db.ensureBackendRange(); err != nil {
		t.Fatal(err)
	}
	observed := &ownedSuccessorBackend{DB: backend, entered: make(chan struct{}), resume: resume}
	db.backend = observed
	readDone := make(chan struct{})
	var key, value []byte
	var found bool
	var readErr error
	go func() {
		defer close(readDone)
		key, value, found, readErr = db.SeekGEVersionRange(lower, upper)
	}()
	awaitSuccessorSignal(t, observed.entered)
	if !db.writeMu.TryRLock() {
		t.Fatal("owned capture serialized point writers")
	}
	db.writeMu.RUnlock()
	writeDone := make(chan struct{})
	var writeErr error
	go func() { defer close(writeDone); writeErr = db.Set([]byte("unrelated"), []byte("writer")) }()
	awaitSuccessorSignal(t, writeDone)
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	var closeErr error
	closeStarted = true
	go func() { defer close(closeDone); closeErr = db.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for !db.closing.Load() {
		if time.Now().After(deadline) {
			t.Fatal("Close did not reach its admission fence")
		}
		runtime.Gosched()
	}
	select {
	case <-closeDone:
		t.Fatal("Close retired an active owned capture")
	default:
	}
	release()
	awaitSuccessorSignal(t, readDone)
	awaitSuccessorSignal(t, closeDone)
	if readErr != nil || !found || !bytes.Equal(key, oldKey) || string(value) != "old" {
		t.Fatalf("owned read = (%x,%q,%t,%v)", key, value, found, readErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if observed.ownedCalls.Load() != 1 || observed.cursorCalls.Load() != 0 {
		t.Fatal("capture used cursor fallback")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSeekGEVersionRangeOwnedBackendPointerFailureWinnerAndCleanup(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "btree", FlushThreshold: 1 << 30, MemtableShards: 4, MaxQueuedMemtables: -1, ValueLogPointerThreshold: 1, ValueLogCompression: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lower, _ := mvcckey.Encode([]byte("one-key"), 100)
	oldKey, _ := mvcckey.Encode([]byte("one-key"), 10)
	newer, _ := mvcckey.Encode([]byte("one-key"), 80)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, []byte("one-key"))
	if err := db.Set(oldKey, bytes.Repeat([]byte("pointer"), 1024)); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	ptr := mustBackendUserPointer(t, backend, oldKey)
	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("missing original value-log capture")
	}
	defer snapshot.Close()
	file, _, err := snapshot.PinnedValueLogFile(ptr.FileID)
	if err != nil {
		t.Fatal(err)
	}
	corrupt, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer corrupt.Close()
	var saved [1]byte
	if _, err := corrupt.ReadAt(saved[:], int64(ptr.Offset)); err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.WriteAt([]byte{saved[0] ^ 0xff}, int64(ptr.Offset)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := corrupt.WriteAt(saved[:], int64(ptr.Offset)); err != nil {
			t.Error(err)
		}
	}()
	_, _, payloadErr := backend.SeekGEOwnedAtRoot(0, lower, upper)
	if _, ok := payloadErr.(*backenddb.OwnedSuccessorPointerError); !ok {
		t.Fatalf("fixture did not fail only eager payload: %T %v", payloadErr, payloadErr)
	}
	if err := db.ensureBackendRange(); err != nil {
		t.Fatal(err)
	}
	observed := &ownedSuccessorBackend{DB: backend}
	db.backend = observed
	// Non-exact mutable version wins over the corrupt older disk candidate.
	if err := db.Set(newer, []byte("newer")); err != nil {
		t.Fatal(err)
	}
	key, value, found, err := db.SeekGEVersionRange(lower, upper)
	if err != nil || !found || !bytes.Equal(key, newer) || string(value) != "newer" {
		t.Fatalf("losing pointer: %x %q %t %v", key, value, found, err)
	}
	if observed.ownedCalls.Load() != 1 {
		t.Fatal("eager capture was not attempted")
	}
	// Starting at the older version excludes the newer key: payload failure is
	// still reported when that real disk pointer wins the metadata-only merge.
	if _, _, found, err := db.SeekGEVersionRange(oldKey, upper); err == nil || found {
		t.Fatal("winning corrupt pointer was hidden", found, err)
	}
	cleanupErr := errors.New("original capture cleanup refused")
	observed.ownedError = errors.Join(payloadErr, cleanupErr)
	if _, _, found, err := db.SeekGEVersionRange(lower, upper); found || !errors.Is(err, cleanupErr) {
		t.Fatal("joined cleanup failure bypassed", found, err)
	}
}
