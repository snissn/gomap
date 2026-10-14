package db

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type snapshotCleanupReceipt struct {
	mu       sync.Mutex
	calls    int
	err      error
	snapshot *Snapshot
	scrubbed bool
}

func (r *snapshotCleanupReceipt) SnapshotCleanupCompleted(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.err = err
	r.scrubbed = r.snapshot.treePager == nil && r.snapshot.idx == nil && r.snapshot.state == nil
}
func TestSnapshotCleanupCompletionSharesOriginalFinalReader(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := d.AcquireSnapshot()
	r := &snapshotCleanupReceipt{snapshot: s}
	if err = s.AdoptSnapshotCleanupCompletion(r); err != nil {
		t.Fatal(err)
	}
	if err = s.AdoptSnapshotCleanupCompletion(&snapshotCleanupReceipt{}); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal("duplicate consumer accepted")
	}
	if err = s.beginRead(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	calls := r.calls
	r.mu.Unlock()
	if calls != 0 {
		t.Fatal("Close request reported actual completion")
	}
	s.endRead()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls != 1 || r.err != nil || !r.scrubbed {
		t.Fatalf("completion before scrub or not once: %+v", r)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotCleanupCompletionJoinsRunningAndCompletedOriginal(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "running"
		if late {
			name = "completed"
		}
		t.Run(name, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			s := d.AcquireSnapshot()
			r := &snapshotCleanupReceipt{snapshot: s}
			started, proceed, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			s.foregroundReadEnd = func() { close(started); <-proceed }
			go func() { done <- s.Close() }()
			<-started
			if late {
				close(proceed)
				if err = <-done; err != nil {
					t.Fatal(err)
				}
			}
			if err = s.AdoptSnapshotCleanupCompletion(r); err != nil {
				t.Fatal(err)
			}
			if !late {
				r.mu.Lock()
				calls := r.calls
				r.mu.Unlock()
				if calls != 0 {
					t.Fatal("running finalizer mistaken for completion")
				}
				close(proceed)
				if err = <-done; err != nil {
					t.Fatal(err)
				}
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.calls != 1 || r.err != nil || !r.scrubbed {
				t.Fatal("exact original completion missing")
			}
		})
	}
}
func TestSnapshotCleanupCompletionReportsActualPagerLastPhysicalError(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	s := d.AcquireStableSnapshot()
	if s == nil || s.primaryRoot == nil {
		t.Fatal("actual PRIMARY capture")
	}
	physicalPager := s.primaryRoot.arena.Pager()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := retainedalloc.EnrollPair(s.primaryRoot.arena.MetadataOwner(), d.StableResourceIdentityPinRegistry().MetadataOwner(), budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.AdoptPrimaryMetadataEnrollment(enrollment); err != nil {
		t.Fatal(err)
	}
	token, err := s.captureOwnedStableIndexFileResource()
	if err != nil {
		t.Fatal(err)
	}
	operations := s.stablePagerCompletion
	r := &snapshotCleanupReceipt{snapshot: s}
	if err = s.AdoptSnapshotCleanupCompletion(r); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if err = physicalPager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	token.Release()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls != 1 || r.err == nil || !r.scrubbed {
		t.Fatalf("actual pager final action not observed once after scrub: calls=%d err=%v scrubbed=%v", r.calls, r.err, r.scrubbed)
	}
	if got := budget.Stats(); got.ExternalLeases < 2 || got.ExternalBytes == 0 {
		t.Fatal("actual pager error arrived after Snapshot governor refund")
	}
	if operations.owner == nil || operations.originalCustody == nil || operations.charge == 0 {
		t.Fatal("failed original pager operations refunded")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	token.Release()
	if r.calls != 1 {
		t.Fatal("actual physical failure replayed completion")
	}
}

func TestSnapshotCleanupCompletionDoesNotWaitForIndependentPagerDescriptors(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	s := d.AcquireStableSnapshot()
	if s == nil {
		t.Fatal("capture")
	}
	token, err := s.captureOwnedStableIndexFileResource()
	if err != nil {
		t.Fatal(err)
	}
	operations := s.stablePagerCompletion
	r := &snapshotCleanupReceipt{snapshot: s}
	if err = s.AdoptSnapshotCleanupCompletion(r); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	completed := r.calls == 1 && r.err == nil && r.scrubbed
	r.mu.Unlock()
	operations.completionMu.Lock()
	detached := operations.snapshot == nil && operations.snapshotCompleted && operations.owner != nil && operations.charge > 0
	operations.completionMu.Unlock()
	if !completed || !detached {
		t.Fatal("Snapshot-owned payload conflated with surviving physical descriptor")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	token.Release()
	operations.completionMu.Lock()
	disposed := operations.owner == nil && operations.charge == 0
	operations.completionMu.Unlock()
	if !disposed {
		t.Fatal("independent final descriptor did not complete original physical disposal")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls != 1 {
		t.Fatal("independent descriptor replayed Snapshot completion")
	}
}

func TestSnapshotAllocationAdmissionRefusesRefreshedExactView(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	appendPointersInNewSegment(t, d.dir, 0, 1, 420001, 1, func(int) []byte { return []byte("actual-persistent-value") })
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	original := d.snapshotViewRO.Load()
	if original.state.ValueLogSet == nil || len(original.state.ValueLogSet.Files) != 1 || original.state.ValueLogSet.RefCount.Load() != 1 {
		t.Fatal("original persistent Set not singly owned by actual view")
	}
	snapshot, err := d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error {
		if refreshErr := d.RefreshValueLogSet(); refreshErr != nil {
			t.Fatal(refreshErr)
		}
		if d.snapshotViewRO.Load() == original || original.state.ValueLogSet.RefCount.Load() != 0 {
			t.Fatal("actual refresh did not retire original Set during admission")
		}
		return nil
	})
	if snapshot != nil {
		defer snapshot.Close()
	}
	if snapshot != nil || err != ErrSnapshotCapacity || original.state.ValueLogSet.RefCount.Load() != 0 {
		t.Fatalf("retired view resurrected after admission: snapshot=%p err=%v originalSetRefs=%d", snapshot, err, original.state.ValueLogSet.RefCount.Load())
	}
	if d.MinPinnedSnapshotCommitSeq() != ^uint64(0) {
		t.Fatal("refused original view registered a reader")
	}
	retry, err := d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if retry.state.ValueLogSet != d.state.Load().ValueLogSet {
		t.Fatal("retry used superseded physical Set")
	}
	if err = retry.Close(); err != nil {
		t.Fatal(err)
	}
}
