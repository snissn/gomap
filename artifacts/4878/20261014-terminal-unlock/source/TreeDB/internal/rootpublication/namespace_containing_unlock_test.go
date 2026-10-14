package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"testing"
)

type containingUnlockBudget struct {
	actual  *memtable.COWBudget
	observe func()
}
type containingUnlockLease struct {
	actual  retainedalloc.Lease
	bytes   uint64
	observe func()
}

func (b *containingUnlockBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	l, e := b.actual.AcquireRetention(n)
	if e != nil {
		return nil, e
	}
	return &containingUnlockLease{actual: l, bytes: n, observe: b.observe}, nil
}
func (l *containingUnlockLease) Resize(n uint64) error {
	if n < l.bytes {
		l.observe()
	}
	e := l.actual.Resize(n)
	if e == nil {
		l.bytes = n
	}
	return e
}
func (l *containingUnlockLease) Close() { l.actual.Close() }

func TestNamespaceContainingRefundAfterOriginalCallUnlock(t *testing.T) {
	dir := t.TempDir()
	parent, e := os.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	child, e := os.Create(filepath.Join(dir, "primary"))
	if e != nil {
		t.Fatal(e)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	proof, e := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
	if e != nil {
		t.Fatal(e)
	}
	budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	observed, locked := false, false
	wrapper := &containingUnlockBudget{actual: budget, observe: func() {
		if proof.metadata == nil {
			observed = true
			if !proof.closeMu.TryLock() {
				locked = true
			} else {
				proof.closeMu.Unlock()
			}
		}
	}}
	capture, e := owner.Enroll(wrapper)
	if e != nil {
		t.Fatal(e)
	}
	if e = proof.ReleaseWithError(); e != nil {
		t.Fatal(e)
	}
	if !observed || locked {
		t.Fatalf("actual containing Resize before original caller unlock: observed=%t locked=%t", observed, locked)
	}
	capture.Close()
	budget.Close()
}
func TestPrimaryConstructionContainingRefundAfterOriginalCallUnlock(t *testing.T) {
	a, e := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	original, ready, e := NewPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || e != nil {
		t.Fatal(e)
	}
	txn := original.(*DurableRootTransaction)
	budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	observed, locked := false, false
	wrapper := &containingUnlockBudget{actual: budget, observe: func() {
		if txn.constructionV6 == nil {
			observed = true
			if !txn.mu.TryLock() {
				locked = true
			} else {
				txn.mu.Unlock()
			}
		}
	}}
	capture, e := a.MetadataOwner().Enroll(wrapper)
	if e != nil {
		t.Fatal(e)
	}
	if e = txn.Abort(); e != nil {
		t.Fatal(e)
	}
	if !observed || locked {
		t.Fatalf("actual containing Resize before original caller unlock: observed=%t locked=%t", observed, locked)
	}
	capture.Close()
	budget.Close()
}

func TestNamespaceOrdinaryContainingReleaseDoesNotAllocateReceipt(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	var proofs [2]*StableNamespaceCreationProof
	for i := range proofs {
		dir := t.TempDir()
		parent, e := os.Open(dir)
		if e != nil {
			t.Fatal(e)
		}
		child, e := os.Create(filepath.Join(dir, "primary"))
		if e != nil {
			t.Fatal(e)
		}
		proofs[i], e = NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
		if e != nil {
			t.Fatal(e)
		}
	}
	next := 0
	count := testing.AllocsPerRun(1, func() {
		p := proofs[next]
		proofs[next] = nil
		next++
		if e := p.ReleaseWithError(); e != nil {
			panic(e)
		}
	})
	if count != 0 {
		t.Fatalf("ordinary physical proof release allocated unadmitted local receipt: %v", count)
	}
}
