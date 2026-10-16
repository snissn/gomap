package rootpublication

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOriginalCleanupCustodyTypedGrowthAndRefundV6(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	// Header-only and partial binding admission must both refuse without
	// growing any original lease, pending bytes or registry custody edge.
	for _, limit := range []uint64{0, 2, 3, 6, 7} {
		w := iterator.OrdinalScanWork{RecordLimit: limit, ByteLimit: 1 << 20}
		custody, ready, err := NewOriginalCleanupCustodyWithWorkV6(registry, &owner, &w)
		if custody != nil || ready || err != nil || owner.Bytes() != before || registry.originalCleanupEdges != 0 {
			t.Fatal("effect before complete resize admission", limit, ready, err)
		}
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	custody, ready, err := NewOriginalCleanupCustodyWithWorkV6(registry, &owner, &w)
	if !ready || err != nil || custody == nil || registry.originalCleanupEdges != 1 || owner.Bytes() <= before {
		t.Fatal("original typed construction", ready, err)
	}
	charged := owner.Bytes()
	if _, ready, err := custody.CompleteWithWorkV6(&iterator.OrdinalScanWork{}); ready || err != nil || registry.originalCleanupEdges != 1 || owner.Bytes() != charged {
		t.Fatal("completion refusal lost original custody", ready, err)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	completion, ready, err := custody.CompleteWithWorkV6(&w)
	if !ready || err != nil || registry.originalCleanupEdges != 0 {
		t.Fatal(ready, err)
	}
	custody = nil // Drop the original alias BEFORE actual accounting completion.
	var removal retainedalloc.RemovalWork
	if ready, err := completion.CompleteWithWorkV6(&removal, &iterator.OrdinalScanWork{}); ready || err != nil || owner.Bytes() != charged {
		t.Fatal("refund refusal lost retained delta", ready, err)
	}
	for turn := 0; ; turn++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := completion.CompleteWithWorkV6(&removal, &w)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		if turn > 100 {
			t.Fatal("original refund did not resume")
		}
	}
	if owner.Bytes() != before {
		t.Fatal("original custody charge retained or refunded twice", owner.Bytes(), before)
	}
}

type custodyUnsupportedLeaseV6 struct{ retainedalloc.Lease }
type custodyUnsupportedBudgetV6 struct{ actual *memtable.COWBudget }

func (b *custodyUnsupportedBudgetV6) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	l, err := b.actual.AcquireRetention(n)
	if err != nil {
		return nil, err
	}
	return &custodyUnsupportedLeaseV6{l}, nil
}

func TestOriginalCleanupCustodyTypedUnsupportedAndOrdinaryV6(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	enrollment, err := owner.Enroll(&custodyUnsupportedBudgetV6{budget})
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if custody, ready, err := NewOriginalCleanupCustodyWithWorkV6(registry, &owner, &w); custody != nil || ready || !errors.Is(err, retainedalloc.ErrWorkRequired) || owner.Bytes() != before || registry.originalCleanupEdges != 0 {
		t.Fatal("unsupported native lease mutated original custody", ready, err)
	}
	if custody, ready, err := NewOriginalCleanupCustodyWithWorkV6(registry, &owner, nil); custody != nil || ready || !errors.Is(err, retainedalloc.ErrWorkRequired) {
		t.Fatal("native nil-work fallback", ready, err)
	}
	// SAME ordinary constructor remains valid for its original untyped lease.
	custody, err := NewOriginalCleanupCustody(registry, &owner)
	if err != nil || custody == nil || registry.originalCleanupEdges != 1 {
		t.Fatal(err)
	}
	custody.Complete(nil, nil)
	if owner.Bytes() != before || registry.originalCleanupEdges != 0 {
		t.Fatal("ordinary lifecycle changed")
	}
}

func TestOriginalCleanupCustodyTypedClosingAndOverflowV6(t *testing.T) {
	registry := NewIdentityPinRegistry()
	owner := registry.MetadataOwner()
	before := owner.Bytes()
	registry.originalCleanupEdges = ^uint64(0)
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if custody, ready, err := NewOriginalCleanupCustodyWithWorkV6(registry, owner, &w); custody != nil || ready || !errors.Is(err, retainedalloc.ErrClosed) || owner.Bytes() != before || registry.originalCleanupEdges != ^uint64(0) {
		t.Fatal("overflow consumed original admission", ready, err)
	}
	registry.originalCleanupEdges = 0
	registry.Close()
	before = owner.Bytes()
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if custody, ready, err := NewOriginalCleanupCustodyWithWorkV6(registry, owner, &w); custody != nil || ready || !errors.Is(err, retainedalloc.ErrClosed) || owner.Bytes() != before {
		t.Fatal("closed custody accepted", ready, err)
	}
}
