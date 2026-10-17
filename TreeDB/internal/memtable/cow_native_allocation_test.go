package memtable

import (
	"errors"
	"testing"
	"unsafe"
)

func TestCOWNativeAllocationSplitTransfersExactPromiseAfterClose(t *testing.T) {
	if child, err := (*COWExternalLease)(nil).SplitNativeBytes(0, 0); !errors.Is(err, ErrCOWClosed) || child != nil {
		t.Fatal(child, err)
	}
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	header := COWAllocationCharge(uint64(unsafe.Sizeof(COWExternalLease{})))
	floor, bytes := COWAllocationCharge(91), COWAllocationCharge(173)
	parent, err := b.AcquireExternal(floor + bytes + header)
	if err != nil {
		t.Fatal(err)
	}
	before := b.Stats()
	b.Close()
	child, err := parent.SplitNativeBytes(bytes, floor)
	if err != nil {
		t.Fatal(err)
	}
	after := b.Stats()
	if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases+1 {
		t.Fatal("split changed original byte admission", before, after)
	}
	if parent.charge != floor+header || child.charge != bytes+header || child.budget != b {
		t.Fatal("split lost original ledger floor or budget identity")
	}
	parent.Close()
	parent = nil
	if got := b.Stats(); got.TotalBytes != before.ControlBytes+bytes+header || got.ExternalLeases != 1 {
		t.Fatal("parent completion refunded child", got)
	}
	child.Close()
	child = nil
	if got := b.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatal(got)
	}
}

func TestCOWNativeAllocationSplitRefusalPreservesOriginalReceipt(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	parent, err := b.AcquireExternal(1024)
	if err != nil {
		t.Fatal(err)
	}
	before, charge := b.Stats(), parent.charge
	for _, request := range [][2]uint64{{1024, 1}, {1, 1024}, {^uint64(0), 0}, {0, ^uint64(0)}} {
		if child, err := parent.SplitNativeBytes(request[0], request[1]); !errors.Is(err, ErrCOWCapacity) || child != nil {
			t.Fatal(child, err)
		}
		if got := b.Stats(); got != before || parent.charge != charge {
			t.Fatal("failed split changed original proof", got, before)
		}
	}
	parent.Close()
	if child, err := parent.SplitNativeBytes(0, 0); !errors.Is(err, ErrCOWClosed) || child != nil {
		t.Fatal(child, err)
	}
	parent = nil
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}
