package caching

import (
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

func TestCOWBasisCompletionWaitsForActualOriginalIterator(t *testing.T) {
	for _, primary := range []bool{false, true} {
		name := "DATA"
		if primary {
			name = "PRIMARY"
		}
		t.Run(name, func(t *testing.T) {
			backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexPrimaryDirectory: primary})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			if err = backend.SetSync([]byte("disk"), []byte("basis")); err != nil {
				t.Fatal(err)
			}
			c, err := newCOWCache(backend, 2, memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			cache := &DB{backend: backend, cow: c, mutableShards: make([]memShard, 2), mutableShardMask: 1}
			outer := cache.AcquireSnapshot()
			if outer == nil {
				t.Fatal("outer capture")
			}
			original := outer.cowCut.basis
			reader, err := original.snapshot.Iterator(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.closeWithBudget(false); err != nil {
				t.Fatal(err)
			}
			if err = backend.Close(); err != nil {
				t.Fatal(err)
			}
			c.budget.Close()
			if err = outer.Close(); err != nil {
				t.Fatal(err)
			}
			original.completionMu.Lock()
			pending := !original.completed && original.snapshot != nil
			original.completionMu.Unlock()
			before := c.budget.Stats()
			if !pending || before.ExternalBytes == 0 || before.ExternalLeases == 0 {
				t.Fatalf("Close request refunded original pending payload: %+v", before)
			}
			if err = outer.Close(); err != nil {
				t.Fatal(err)
			}
			if got := c.budget.Stats(); got != before {
				t.Fatal("repeat outer Close changed pending capacity")
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			original.completionMu.Lock()
			completed := original.completed && original.cleanupErr == nil && original.snapshot == nil && original.pins == nil
			original.completionMu.Unlock()
			if got := c.budget.Stats(); !completed || got.ExternalBytes != 0 || got.ExternalLeases != 0 {
				t.Fatalf("actual completion retained original payload or charge: %+v", got)
			}
		})
	}
}

type cowBasisAlreadyClosedProvider struct {
	*backenddb.DB
	closeCapture bool
}

func (p *cowBasisAlreadyClosedProvider) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	s, err := p.DB.AcquireSnapshotWithAllocationAdmission(admit)
	if err == nil && p.closeCapture {
		err = s.Close()
	}
	return s, err
}
func TestCOWBasisStartupAndHandoffPostcaptureClose(t *testing.T) {
	for _, primary := range []bool{false, true} {
		name := "DATA"
		if primary {
			name = "PRIMARY"
		}
		t.Run(name, func(t *testing.T) {
			backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexPrimaryDirectory: primary})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			provider := &cowBasisAlreadyClosedProvider{DB: backend, closeCapture: true}
			if c, err := newCOWCache(provider, 2, memtable.DefaultCOWLimits()); c != nil || !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("closed startup capture admitted: cache=%v err=%v", c, err)
			}
			provider.closeCapture = false
			c, err := newCOWCache(provider, 2, memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer c.close()
			before := c.budget.Stats()
			provider.closeCapture = true
			if b, err := c.captureBackendBasis(provider); b != nil || !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("closed handoff capture admitted: basis=%v err=%v", b, err)
			}
			after := c.budget.Stats()
			if after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases {
				t.Fatalf("actually completed refusal leaked charge: before=%+v after=%+v", before, after)
			}
		})
	}
}

type cowBasisCompletionReceipt struct{ calls int }

func (r *cowBasisCompletionReceipt) SnapshotCleanupCompleted(error) { r.calls++ }
func TestCOWBasisCompletionRegistrationRefusalRetainsOriginalDebt(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	var original *cowBackendBasis
	snapshot, err := backend.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
		var e error
		original, e = admitCOWBackendBasis(budget, s)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt := &cowBasisCompletionReceipt{}
	if err = snapshot.AdoptSnapshotCleanupCompletion(receipt); err != nil {
		t.Fatal(err)
	}
	if err = original.adoptSnapshot(snapshot); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("duplicate owner admitted: %v", err)
	}
	if err = original.release(); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("first refusal lost: %v", err)
	}
	if receipt.calls != 1 || original.snapshot != snapshot || original.completed {
		t.Fatal("refusal invented consumer completion or lost exact Snapshot")
	}
	if got := budget.Stats(); got.ExternalBytes == 0 || got.ExternalLeases == 0 {
		t.Fatal("registration refusal refunded retained original basis")
	}
	before := budget.Stats()
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if receipt.calls != 1 || budget.Stats() != before {
		t.Fatal("repeat original Close retried/refunded refusal")
	}
}

func TestCOWBasisPostcaptureRefusalJoinsPendingOriginalIterator(t *testing.T) {
	for _, primary := range []bool{false, true} {
		name := "DATA"
		if primary {
			name = "PRIMARY"
		}
		t.Run(name, func(t *testing.T) {
			backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexPrimaryDirectory: primary})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			var original *cowBackendBasis
			snapshot, err := backend.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
				var e error
				original, e = admitCOWBackendBasis(budget, s)
				return e
			})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := snapshot.Iterator(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err = original.adoptSnapshot(snapshot); err == nil {
				t.Fatal("closed pending original capture admitted")
			}
			if err = original.release(); err != nil {
				t.Fatal(err)
			}
			budget.Close()
			original.completionMu.Lock()
			pending := !original.completed && original.snapshot == snapshot
			original.completionMu.Unlock()
			if got := budget.Stats(); !pending || got.ExternalBytes == 0 || got.ExternalLeases == 0 {
				t.Fatalf("postcapture refusal treated Close acceptance as completion: %+v", got)
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			original.completionMu.Lock()
			done := original.completed && original.snapshot == nil && original.cleanupErr == nil
			original.completionMu.Unlock()
			if got := budget.Stats(); !done || got.ExternalBytes != 0 || got.ExternalLeases != 0 {
				t.Fatalf("actual original completion failed to settle refusal: %+v", got)
			}
		})
	}
}

func TestCOWBasisOwnedIteratorScrubsConsumedWrapperAliases(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err = backend.SetSync([]byte("disk"), []byte("basis")); err != nil {
		t.Fatal(err)
	}
	c, err := newCOWCache(backend, 2, memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	cache := &DB{backend: backend, cow: c, mutableShards: make([]memShard, 2), mutableShardMask: 1}
	public, err := cache.cowIterator(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	owned := public.(*cowOwnedIterator)
	bound := owned.Iterator.(*snapshotBoundIterator)
	child := bound.inner.(*cowMergeIterator)
	if !public.Valid() {
		t.Fatal("actual backend child not constructed")
	}
	if err = public.Close(); err != nil {
		t.Fatal(err)
	}
	if owned.snapshot != nil || owned.lease != nil || bound.inner != nil || bound.owner != nil || child.snapshot != nil || child.Iterator != nil || child.workspace != nil || child.lease != nil {
		t.Fatal("consumed charged wrapper retains original payload aliases")
	}
	if public.Valid() || public.Error() != backenddb.ErrClosed {
		t.Fatal("post-Close public behavior changed")
	}
	before := c.budget.Stats()
	if err = public.Close(); err != nil {
		t.Fatal(err)
	}
	if c.budget.Stats() != before {
		t.Fatal("repeat wrapper Close changed original accounting")
	}
}
