package caching

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/tree"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type cowChunkPauseBackend struct {
	*cowAcceptedReportBackend
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (b *cowChunkPauseBackend) NewPhysicalBatchWithSize(n int) batch.Interface {
	return &cowChunkPauseBatch{cowAcceptedReportBatch: &cowAcceptedReportBatch{Batch: b.DB.NewPhysicalBatchWithSize(n).(*backenddb.Batch), owner: b.cowAcceptedReportBackend}, owner: b}
}
func (b *cowChunkPauseBackend) NewPhysicalBatch() batch.Interface {
	return b.NewPhysicalBatchWithSize(0)
}

type cowChunkPauseBatch struct {
	*cowAcceptedReportBatch
	owner *cowChunkPauseBackend
}

func (b *cowChunkPauseBatch) wait(err error) error {
	if err == nil && b.group != nil && !b.final {
		b.owner.once.Do(func() { close(b.owner.paused); <-b.owner.resume })
	}
	return err
}
func (b *cowChunkPauseBatch) Write() error     { return b.wait(b.cowAcceptedReportBatch.Write()) }
func (b *cowChunkPauseBatch) WriteSync() error { return b.wait(b.cowAcceptedReportBatch.WriteSync()) }

func TestCOWStreamedPrivateChunksPreserveCapturedPrefixAndNewerSuffix(t *testing.T) {
	db := cowPointerFixture(t)
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Set([]byte(key), []byte("base")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	backend := &cowChunkPauseBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: db.backend.(*backenddb.DB)}, paused: make(chan struct{}), resume: make(chan struct{})}
	db.backend = backend
	db.flushBackendMaxEntries = 1
	if err := db.Set([]byte("a"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("a"), []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("d"), []byte("covered")); err != nil {
		t.Fatal(err)
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	before := backend.State().CommitSeq
	done := make(chan error, 1)
	go func() { db.flushMu.Lock(); defer db.flushMu.Unlock(); done <- db.flushCOWFrozen(true, nil) }()
	resumed := false
	defer func() {
		if !resumed {
			close(backend.resume)
		}
	}()
	select {
	case <-backend.paused:
	case <-time.After(10 * time.Second):
		t.Fatal("streamed chunk did not pause")
	}
	if backend.State().CommitSeq != before || backend.publications.Load() != 0 {
		t.Fatal("private intermediate chunk published backend root")
	}
	if err = db.Set([]byte("a"), []byte("late-frozen")); err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("b"), []byte("late-frozen")); err != nil {
		t.Fatal(err)
	}
	if err = db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("a"), []byte("latest-mutable")); err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "second" {
		t.Fatalf("captured old=(%q,%v)", got, err)
	}
	if value, err := old.Get([]byte("b")); !errors.Is(err, tree.ErrKeyNotFound) || value != nil {
		t.Fatalf("covered tombstone=%v", err)
	}
	close(backend.resume)
	resumed = true
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("chunk handoff did not finish")
	}
	if backend.publications.Load() != 1 || backend.State().CommitSeq != before+1 {
		t.Fatal("captured chunks did not publish exactly once")
	}
	if len(db.cow.cut.frozen) == 0 {
		t.Fatal("new frozen suffix was removed with covered prefix")
	}
	for key, want := range map[string]string{"a": "latest-mutable", "b": "late-frozen", "c": "base", "d": "covered"} {
		if got, err := db.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("handoff Get(%s)=(%q,%v)", key, got, err)
		}
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"a": "latest-mutable", "b": "late-frozen", "c": "base", "d": "covered"} {
		if got, err := backend.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("final backend Get(%s)=(%q,%v)", key, got, err)
		}
	}
}
