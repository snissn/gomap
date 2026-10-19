package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type primaryRefundBudget struct {
	actual  *memtable.COWBudget
	observe func()
}
type primaryRefundLease struct {
	actual  retainedalloc.Lease
	bytes   uint64
	observe func()
}

func (b *primaryRefundBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	lease, err := b.actual.AcquireRetention(n)
	if err != nil {
		return nil, err
	}
	return &primaryRefundLease{actual: lease, bytes: n, observe: b.observe}, nil
}
func (l *primaryRefundLease) Resize(n uint64) error {
	if n < l.bytes {
		l.observe()
	}
	if err := l.actual.Resize(n); err != nil {
		return err
	}
	l.bytes = n
	return nil
}
func (l *primaryRefundLease) Close() { l.actual.Close() }

// Observe the real governor refund during the original public close routes.
// A completed physical operation alone must not refund an owner still named by
// its generation, Snapshot or exported pager callback environment.
func TestPrimaryOwnerRefundFollowsOriginalCallerAliasAndUnlock(t *testing.T) {
	for _, route := range []string{"generation", "snapshot", "pager"} {
		t.Run(route, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			idx := d.idx.Load()
			owner, metadata := idx.primaryOwner, idx.primary.MetadataOwner()
			var snapshot *Snapshot
			var operations *stablePagerOwnedOperations
			var token *rootpublication.StableResourceToken
			if route == "snapshot" {
				snapshot = d.AcquireSnapshot()
			}
			if route == "pager" {
				snapshot = d.AcquireStableSnapshot()
				token, err = snapshot.captureOwnedStableIndexFileResource()
				if err != nil {
					t.Fatal(err)
				}
				operations = snapshot.stablePagerCompletion
				if operations.owner != owner {
					t.Fatalf("exported callback owner=%p captured generation owner=%p", operations.owner, owner)
				}
			}
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			observed := false
			capture, err := metadata.Enroll(&primaryRefundBudget{actual: budget, observe: func() {
				// Earlier child refunds occur inside original physical cleanup. Only the
				// final containing-owner refund can observe the unlocked, detached owner.
				if !owner.closeWork.mu.TryLock() {
					return
				}
				defer owner.closeWork.mu.Unlock()
				if owner.metadataCharge != 0 {
					return
				}
				observed = true
				if idx.primaryOwner != nil || idx.primary != nil {
					t.Error("generation retained physical owner alias during refund")
				}
				if !idx.wrapperMu.TryLock() {
					t.Error("generation lock held during owner refund")
				} else {
					idx.wrapperMu.Unlock()
				}
				if snapshot != nil {
					if snapshot.primaryOwner != nil {
						t.Error("Snapshot retained physical owner alias during refund")
					}
					if !snapshot.iteratorMu.TryLock() {
						t.Error("Snapshot lock held during owner refund")
					} else {
						snapshot.iteratorMu.Unlock()
					}
				}
				if operations != nil {
					if operations.owner != nil {
						t.Error("pager callback retained physical owner alias during refund")
					}
					if !operations.completionMu.TryLock() {
						t.Error("pager callback lock held during owner refund")
					} else {
						operations.completionMu.Unlock()
					}
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			if route != "generation" && observed {
				t.Fatal("DB close refunded retained borrower owner")
			}
			if route == "snapshot" {
				if err = snapshot.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if route == "pager" {
				if err = token.ReleaseWithError(); err != nil {
					t.Fatal(err)
				}
			}
			if !observed || !metadata.PhysicalClosed() {
				t.Fatal("original terminal caller did not complete physical owner refund")
			}
			capture.Close()
			budget.Close()
			if budget.Stats().TotalBytes != 0 {
				t.Fatal("original governing budget retained terminal charge")
			}
		})
	}
}
