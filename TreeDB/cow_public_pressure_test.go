package treedb

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func cowPublicProducerBudget(t *testing.T, database *DB) *memtable.COWBudget {
	t.Helper()
	capture, err := database.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	budget, ok := capture.NativeAllocator().(*memtable.COWBudget)
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("public capture did not expose its original producer authority")
	}
	return budget
}

// Saturation fixtures must exclude revocable idle bytes. Even a refused
// admission reclaims idle storage before returning capacity; it cannot revoke
// active receipts. Keep the original refusal and allocation assertions after
// this control, with pressure recomputed from the resulting live reservations.
func cowPublicPressureEvictIdle(t *testing.T, database *DB) {
	t.Helper()
	budget := cowPublicProducerBudget(t, database)
	before := budget.Stats()
	denied, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes)
	if denied != nil {
		denied.Close()
	}
	if denied != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("over-limit pressure was admitted: %v", err)
	}
	after := budget.Stats()
	if after.ReservedBytes > before.ReservedBytes {
		t.Fatal("refused admission increased reservations")
	}
	refunded := before.ReservedBytes - after.ReservedBytes
	want := before
	want.TotalBytes -= refunded
	want.ReservedBytes -= refunded
	want.ExternalBytes -= refunded
	if after != want {
		t.Fatalf("pressure changed active ownership: before=%+v after=%+v refunded=%d", before, after, refunded)
	}
	if value, leases := budget.TakeIdleReadScratch(); value != nil || leases != [2]*memtable.COWExternalLease{} {
		t.Fatal("over-limit pressure retained idle storage")
	}
}

func TestCOWPublicBatchPressureEvictsIdleWithoutRevokingActive(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			budget := cowPublicProducerBudget(t, database)
			baseline := budget.Stats()
			active := database.NewBatch().(*commandWALPublicBatch)
			if active == &cowDeniedPublicBatch || active.cowState.lease == nil {
				t.Fatal("active wrapper was not admitted")
			}
			defer active.Close()
			live := budget.Stats()
			park := func() {
				lease, err := budget.AcquireExternal(32 << 10)
				if err != nil {
					t.Fatal(err)
				}
				if !budget.PutIdleReadScratch(new([32 << 10]byte), [2]*memtable.COWExternalLease{lease, nil}) {
					lease.Close()
					t.Fatal("idle fixture was not parked")
				}
			}
			park()
			idle := budget.Stats()
			if idle.ReservedBytes-live.ReservedBytes != 32<<10 || idle.ExternalLeases != live.ExternalLeases {
				t.Fatal("idle fixture did not retain exactly its charged payload")
			}
			header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - idle.ReservedBytes - header)
			if err != nil {
				t.Fatal(err)
			}
			// Reproduce the obsolete fixture assumption deterministically: all
			// bytes appear reserved, but admission can still reclaim idle bytes.
			candidate := database.NewBatch().(*commandWALPublicBatch)
			if candidate == &cowDeniedPublicBatch {
				t.Fatal("idle storage denied an otherwise admissible public wrapper")
			}
			candidate.Close()
			pressure.Close()
			got := budget.Stats()
			got.PeakBytes = live.PeakBytes
			if got != live {
				t.Fatalf("reclaim revoked or leaked active ownership: got=%+v want=%+v", got, live)
			}
			park()
			cowPublicPressureEvictIdle(t, database)
			got = budget.Stats()
			got.PeakBytes = live.PeakBytes
			if got != live {
				t.Fatal("failed admission did not refund exactly the idle payload")
			}
			active.Close()
			got = budget.Stats()
			got.PeakBytes = baseline.PeakBytes
			if got != baseline {
				t.Fatalf("active wrapper lost its independent final refund: got=%+v want=%+v", got, baseline)
			}
		})
	}
}
