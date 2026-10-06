package caching

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type cowRefusingBasisBackend struct {
	*backenddb.DB
	refuse bool
}

func (b *cowRefusingBasisBackend) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	if b.refuse {
		return nil, memtable.ErrCOWCapacity
	}
	return b.DB.AcquireSnapshotWithAllocationAdmission(admit)
}

// Until Open enables the mode, this fixture installs the private COW owner into
// a fully initialized cached DB. All point writes still use the ordinary batch
// placement and canonical publication entry point.
func cowPointerFixture(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30,
		MemtableShards: 2, ValueLogPointerThreshold: 1,
		ValueLogCompression: uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	c, err := newCOWCache(backend, 2, memtable.DefaultCOWLimits())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	db.cow = c
	t.Cleanup(func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	})

	return db
}

func TestCOWPointerImmediatePointAndIteratorOldCut(t *testing.T) {
	db := cowPointerFixture(t)
	oldValue, newValue := bytes.Repeat([]byte("old"), 4096), bytes.Repeat([]byte("new"), 4096)
	if err := db.Set([]byte("a"), oldValue); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := db.Set([]byte("a"), newValue); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("d"), []byte("other")); err != nil {
		t.Fatal(err)
	}
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	for _, test := range []struct {
		s    *Snapshot
		want []byte
	}{{old, oldValue}, {current, newValue}} {
		got, err := test.s.Get([]byte("a"))
		if err != nil || !bytes.Equal(got, test.want) {
			t.Fatalf("point len=%d err=%v", len(got), err)
		}
		got[0] ^= 1
		again, err := test.s.Get([]byte("a"))
		if err != nil || !bytes.Equal(again, test.want) {
			t.Fatal("caller result aliased retained value")
		}
		it, err := test.s.Iterator(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !it.Valid() || string(it.Key()) != "a" || !bytes.Equal(it.Value(), test.want) {
			t.Fatalf("iterator key=%q value=%d error=%v", it.Key(), len(it.Value()), it.Error())
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		_ = it.Close()
	}
	for i := range db.lanes {
		l := &db.lanes[i]
		l.vlogMu.Lock()
		if w, ok := l.vlog.(interface{ PendingBytes() int }); ok && w.PendingBytes() != 0 {
			t.Error("publication left producer bytes buffered")
		}
		l.vlogMu.Unlock()
	}
}

func TestCOWPointerCheckpointCapturedBasisAndTombstones(t *testing.T) {
	db := cowPointerFixture(t)
	db.flushBackendMaxEntries = 1 // every physical chunk stays private until final
	want := bytes.Repeat([]byte("value"), 2048)
	for _, key := range []string{"a", "d", "gone"} {
		if err := db.Set([]byte(key), want); err != nil {
			t.Fatal(err)
		}
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if len(db.cow.cut.frozen) != 0 {
		t.Fatal("covered frozen prefix retained after successful handoff")
	}
	for _, key := range []string{"a", "d"} {
		got, err := db.backend.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("backend %s len=%d err=%v", key, len(got), err)
		}
		got, err = db.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("basis %s len=%d err=%v", key, len(got), err)
		}
	}
	if found, err := db.backend.Has([]byte("gone")); err != nil || found {
		t.Fatalf("flushed tombstone: found=%t err=%v", found, err)
	}
	if got, err := old.Get([]byte("gone")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("old prefix len=%d err=%v", len(got), err)
	}
}

func TestCOWCheckpointAcceptedPrefixHandoffRefusalDrainRetry(t *testing.T) {
	db := cowPointerFixture(t)
	backend := &cowRefusingBasisBackend{DB: db.backend.(*backenddb.DB)}
	db.backend = backend
	if err := db.Set([]byte("a"), []byte("prefix")); err != nil {
		t.Fatal(err)
	}
	backend.refuse = true
	if err := db.Checkpoint(); err != memtable.ErrCOWCapacity {
		t.Fatalf("handoff refusal=%v", err)
	}
	h := db.cow.handoff
	if h == nil || !h.accepted || len(db.cow.cut.frozen) == 0 || db.cow.cut.basis != h.cut.basis {
		t.Fatal("refusal dropped accepted prefix or changed basis")
	}
	if got, err := backend.Get([]byte("a")); err != nil || string(got) != "prefix" {
		t.Fatalf("accepted backend=%q err=%v", got, err)
	}
	if err := db.Set([]byte("a"), []byte("late")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "late" {
		t.Fatalf("late precedence=%q err=%v", got, err)
	}
	backend.refuse = false
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if db.cow.handoff != nil || len(db.cow.cut.frozen) != 0 {
		t.Fatal("retry failed to drain handoff")
	}
	if got, err := backend.Get([]byte("a")); err != nil || string(got) != "late" {
		t.Fatalf("retried backend=%q err=%v", got, err)
	}
}

func TestCOWMaintenanceRefreshPreservesOldCutsAndSubsequentCheckpoint(t *testing.T) {
	db := cowPointerFixture(t)
	if err := db.Set([]byte("a"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	backend := db.backend.(*backenddb.DB)
	maintenance := func() error { return backend.VacuumIndexOnline(context.Background()) }
	if runtime.GOOS == "windows" {
		if err := backend.VacuumIndexOnline(context.Background()); !errors.Is(err, backenddb.ErrVacuumUnsupported) {
			t.Fatalf("Windows online-vacuum refusal: %v", err)
		}
		// Keep the old-cut, basis-refresh and later checkpoint assertions on
		// Windows using real supported maintenance.
		maintenance = backend.CompactIndex
	}
	if err := db.RunBackendMaintenance(maintenance); err != nil {
		t.Fatal(err)
	}
	if db.cow.cut.basis == old.cowCut.basis {
		t.Fatal("maintenance did not refresh basis")
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "before" {
		t.Fatalf("old maintenance cut=%q err=%v", got, err)
	}
	if err := db.Set([]byte("a"), []byte("after")); err != nil {
		t.Fatal(err)
	}
	key, value, found, err := db.SeekGE([]byte("a"), []byte("z"))
	if err != nil || !found || string(key) != "a" || string(value) != "after" {
		t.Fatalf("COW successor=%q/%q found=%t err=%v", key, value, found, err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := backend.Get([]byte("a")); err != nil || string(got) != "after" {
		t.Fatalf("maintenance checkpoint=%q err=%v", got, err)
	}
}

func cowWait(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("COW operation did not progress")
	}
}

func TestCOWMaintenanceOwnsFlushFenceAndWaitingWriterProgress(t *testing.T) {
	db := cowPointerFixture(t)
	if err := db.Set([]byte("a"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	maintenance := make(chan error, 1)
	go func() {
		maintenance <- db.RunBackendMaintenance(func() error {
			close(entered)
			<-resume
			return nil
		})
	}()
	<-entered
	if db.flushMu.TryLock() {
		db.flushMu.Unlock()
		close(resume)
		t.Fatal("backend maintenance did not own background flush fence")
	}
	writer := make(chan error, 1)
	go func() { writer <- db.Set([]byte("a"), []byte("after")) }()
	// A writer encountering an active maintenance operation waits for the
	// coherent basis refresh, instead of reporting its temporary refresh flag.
	select {
	case err := <-writer:
		close(resume)
		t.Fatalf("writer crossed active maintenance: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	background := make(chan error, 1)
	go func() {
		db.flushMu.Lock()
		defer db.flushMu.Unlock()
		background <- db.flushCOWFrozen(false, nil)
	}()
	close(resume)
	cowWait(t, maintenance)
	cowWait(t, writer)
	cowWait(t, background)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("a"))
	if err != nil || string(got) != "after" {
		t.Fatalf("after maintenance=%q err=%v", got, err)
	}
}

func TestCOWDBCloseWaitsForAdmittedPointerReadAndDelayedOwnersClose(t *testing.T) {
	db := cowPointerFixture(t)
	value := bytes.Repeat([]byte("owned-pointer"), 1024)
	if err := db.Set([]byte("a"), value); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	it, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.cowReadMu.Lock()
	read := make(chan error, 1)
	go func() {
		got, err := snapshot.Get([]byte("a"))
		if err == nil && !bytes.Equal(got, value) {
			err = errors.New("admitted pointer value changed")
		}
		read <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		// Get increments the existing snapshot operation count, then admits
		// through the DB storage gate, before waiting on its read workspace.
		if snapshot.readState.Load() != 0 {
			if !db.cow.readMu.TryLock() {
				break
			}
			db.cow.readMu.Unlock()
		}
		if time.Now().After(deadline) {
			snapshot.cowReadMu.Unlock()
			t.Fatal("pointer read did not admit")
		}
		runtime.Gosched()
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		snapshot.cowReadMu.Unlock()
		t.Fatalf("DB close crossed admitted storage read: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	snapshot.cowReadMu.Unlock()
	cowWait(t, read)
	cowWait(t, closed)
	if _, err := snapshot.Get([]byte("a")); err != backenddb.ErrClosed {
		t.Fatalf("late read=%v", err)
	}
	it.Next()
	it.Seek([]byte("a"))
	if it.Valid() {
		t.Fatal("iterator usable after DB teardown")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if db.cow.activeCuts.Load() != 0 {
		t.Fatal("delayed close retained read cuts")
	}
}

func TestCOWViewCallbacksOutsideReadLocksAndClosedEmptyRoutes(t *testing.T) {
	db := cowPointerFixture(t)
	if err := db.Set([]byte("empty"), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("value"), bytes.Repeat([]byte("v"), 8192)); err != nil {
		t.Fatal(err)
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before := db.cow.budget.Stats().ExternalBytes
	err = s.GetManyView([][]byte{[]byte("empty"), []byte("missing"), []byte("value")}, func(i int, key, value []byte, found bool) error {
		if !db.cow.readMu.TryLock() {
			return errors.New("callback retained storage read gate")
		}
		db.cow.readMu.Unlock()
		if !s.cowReadMu.TryLock() {
			return errors.New("callback retained read workspace lock")
		}
		s.cowReadMu.Unlock()
		if found != (i != 1) {
			return errors.New("empty value treated as missing")
		}
		if err := db.Set([]byte("later"), []byte("value")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Retained read workspace may grow; each callback copy must be refunded.
	if db.cow.budget.Stats().ExternalBytes < before {
		t.Fatal("lost retained accounting")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.GetManyView(nil, func(int, []byte, []byte, bool) error { return nil }); err != backenddb.ErrClosed {
		t.Fatalf("empty closed view=%v", err)
	}
	if _, err := s.HasPrefixes(nil); err != backenddb.ErrClosed {
		t.Fatalf("empty closed prefix=%v", err)
	}
}
