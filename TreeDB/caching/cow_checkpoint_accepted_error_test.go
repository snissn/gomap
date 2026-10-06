package caching

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var errCOWAcceptedReport = errors.New("test: reported error after real accepted publication")

type cowAcceptedReportBackend struct {
	*backenddb.DB
	inject        atomic.Bool
	refuseCapture atomic.Bool
	publications  atomic.Int64
}

func (b *cowAcceptedReportBackend) NewPhysicalBatchWithSize(size int) batch.Interface {
	return &cowAcceptedReportBatch{Batch: b.DB.NewPhysicalBatchWithSize(size).(*backenddb.Batch), owner: b}
}
func (b *cowAcceptedReportBackend) NewPhysicalBatch() batch.Interface {
	return b.NewPhysicalBatchWithSize(0)
}
func (b *cowAcceptedReportBackend) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	if b.refuseCapture.Load() {
		return nil, memtable.ErrCOWCapacity
	}
	return b.DB.AcquireSnapshotWithAllocationAdmission(admit)
}

type cowAcceptedReportBatch struct {
	*backenddb.Batch
	owner *cowAcceptedReportBackend
	group *backenddb.RootPublicationBuildGroup
	final bool
}

func (b *cowAcceptedReportBatch) SetRootPublicationBuildGroup(g *backenddb.RootPublicationBuildGroup, final bool) error {
	b.group, b.final = g, final
	return b.Batch.SetRootPublicationBuildGroup(g, final)
}
func (b *cowAcceptedReportBatch) report(err error) error {
	if b.final && b.group.Accepted() {
		b.owner.publications.Add(1)
		if err == nil && b.owner.inject.Swap(false) {
			b.owner.refuseCapture.Store(true)
			return errCOWAcceptedReport
		}
	}
	return err
}
func (b *cowAcceptedReportBatch) Write() error     { return b.report(b.Batch.Write()) }
func (b *cowAcceptedReportBatch) WriteSync() error { return b.report(b.Batch.WriteSync()) }

func TestCOWCheckpointRealAcceptedReportedErrorRetainsPrefixAndDoesNotReplay(t *testing.T) {
	db := cowPointerFixture(t)
	backend := &cowAcceptedReportBackend{DB: db.backend.(*backenddb.DB)}
	db.backend = backend
	db.flushBackendMaxEntries = 1
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Set([]byte(key), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	basis := db.cow.cut.basis
	backend.inject.Store(true)
	err = db.Checkpoint()
	if !errors.Is(err, errCOWAcceptedReport) {
		t.Fatalf("accepted report=%v", err)
	}
	db.flushMu.Lock()
	h := db.cow.handoff
	if h == nil || !h.accepted || h.cut.basis != basis || len(h.cut.frozen) == 0 {
		db.flushMu.Unlock()
		t.Fatal("accepted prefix receipt lost")
	}
	for i, table := range h.cut.frozen {
		if db.cow.cut.frozen[i].root != table.root {
			db.flushMu.Unlock()
			t.Fatal("covered prefix was dequeued")
		}
	}
	acceptedSeq := backend.State().CommitSeq
	db.flushMu.Unlock()
	if backend.publications.Load() != 1 {
		t.Fatal("streamed chunks did not publish once")
	}
	if got, err := backend.Get([]byte("a")); err != nil || string(got) != "old" {
		t.Fatalf("real accepted backend=(%q,%v)", got, err)
	}
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("late"), []byte("write")); err != nil {
		t.Fatal(err)
	}
	backend.refuseCapture.Store(false)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if backend.publications.Load() != 2 {
		t.Fatalf("accepted prefix replayed: publications=%d", backend.publications.Load())
	}
	if got := backend.State().CommitSeq; got != acceptedSeq+1 {
		t.Fatalf("retry published %d roots want one", got-acceptedSeq)
	}
	if db.cow.handoff != nil || len(db.cow.cut.frozen) != 0 {
		t.Fatal("retry left accepted prefix")
	}
	for key, want := range map[string]string{"a": "new", "b": "old", "c": "old", "late": "write"} {
		if got, err := db.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("Get(%s)=(%q,%v)", key, got, err)
		}
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "old" {
		t.Fatalf("old snapshot=(%q,%v)", got, err)
	}
}
