package memtable

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
)

// A finite external owner can leave enough room for the actual private tree
// without leaving room for the independent-path upper estimate. Admission may
// follow the existing mutation engine, but must preserve whole-batch refusal.
func TestCOWRawPreparationUsesRemainingCapacity(t *testing.T) {
	for _, headroom := range []uint64{2 << 20, 256 << 10, 64 << 10, 32 << 10} {
		t.Run(fmt.Sprint(headroom), func(t *testing.T) {
			budget, writer := cowTestWriter(t, DefaultCOWLimits())
			old := cowTestPublish(t, writer, cowTestEntries(1024))
			var next *COWRoot
			var blocker *COWExternalLease
			t.Cleanup(func() {
				if writer.pending != nil {
					retirement := writer.pending.Cancel()
					retirement.Drain()
				}
				if blocker != nil {
					blocker.Close()
				}
				if next != nil {
					cowTestRelease(next)
				}
				cowTestRelease(old)
				cowTestClose(writer)
				budget.Close()
				if got := budget.Stats(); got.TotalBytes != 0 {
					t.Errorf("final retained charge: %+v", got)
				}
			})
			entries := make([]COWMutation, 256)
			for i := range entries {
				entries[i] = COWMutation{Key: []byte(fmt.Sprintf("%08d", i+1024)), Value: []byte("new"), Flags: node.FlagInline}
			}
			upper, err := writer.Estimate(entries, COWPrepareOptions{})
			if err != nil || upper.Total() <= headroom {
				t.Fatalf("fixture needs coarse admission refusal: upper=%+v headroom=%d err=%v", upper, headroom, err)
			}
			header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
			blocker, err = budget.AcquireExternal(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes - headroom - header)
			if err != nil {
				t.Fatal(err)
			}
			before := budget.Stats()
			prepared, err := writer.Prepare(entries, COWPrepareOptions{})
			if headroom == 32<<10 {
				if !errors.Is(err, ErrCOWCapacity) || prepared != nil {
					t.Fatalf("partial private preparation must refuse: prepared=%v err=%v", prepared, err)
				}
				after := budget.Stats()
				if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.HistoryBytes != before.HistoryBytes || writer.pending != nil {
					t.Fatalf("refused private postimage retained a receipt: before=%+v after=%+v", before, after)
				}
				blocker.Close()
				blocker = nil
				prepared, err = writer.Prepare(entries, COWPrepareOptions{})
			}
			if err != nil {
				t.Fatalf("actual raw preparation must fit: upper=%+v remaining=%d err=%v", upper, headroom, err)
			}
			if prepared.Charge().Nodes != prepared.private.AllocationCharge() {
				t.Fatal("node receipt is not the actual reconciled allocation")
			}
			charge := prepared.Charge()
			expectedTemporary := upper.Temporary
			if headroom != 32<<10 { // The retry after releasing that blocker uses the coarse path.
				expectedTemporary += cowAllocation(uint64(unsafe.Sizeof(cowPreparedNodeWork{})))
			}
			if charge.Payload != upper.Payload || charge.Wrappers != upper.Wrappers || charge.Temporary != expectedTemporary || charge.Resources != upper.Resources || charge.Extra != upper.Extra {
				t.Fatalf("incremental reconciliation refunded non-node ownership: charge=%+v upper=%+v", charge, upper)
			}
			t.Logf("headroom=%d actual=%+v coarse=%+v", headroom, charge, upper)
			next = prepared.Publish()
			if old.Len() != 1024 || next.Len() != 1280 {
				t.Fatalf("whole images old=%d new=%d", old.Len(), next.Len())
			}
			if _, found := old.Get(entries[0].Key); found {
				t.Fatal("held old root changed")
			}
			for _, entry := range entries {
				if got, found := next.Get(entry.Key); !found || got.Value != "new" {
					t.Fatalf("missing accepted row %q: %+v %t", entry.Key, got, found)
				}
			}
		})
	}
}
