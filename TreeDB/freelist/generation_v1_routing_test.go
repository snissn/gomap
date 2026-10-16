package freelist

import "testing"

func TestCOWGenerationRequirementDistinguishesInheritedMetadataV1(t *testing.T) {
	fresh := func(t *testing.T) *FreelistTxn {
		t.Helper()
		base := MustNewFreelistGenerationV1(1, 32, []uint64{4, 5, 6}, map[uint64]uint64{8: 1})
		base.record.Extents = []ReservationExtentV1{{StartPageID: 7, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 2}}
		txn, err := BeginCandidateV1(base, base.ref, NewReservationLedger())
		if err != nil {
			t.Fatal(err)
		}
		if len(txn.changedChunks) == 0 || txn.root.retiredCount < 2 {
			t.Fatal("inherited pending metadata was not applied")
		}
		if txn.requiresGeneration {
			t.Fatal("inherited metadata alone requires a DATA generation")
		}
		clone, err := txn.cloneForAllocatorPrepare()
		if err != nil || clone.requiresGeneration {
			t.Fatal("clean clone changed mutation origin", err)
		}
		return txn
	}
	tests := []struct {
		name   string
		mutate func(*testing.T, *FreelistTxn)
	}{
		{"append", func(t *testing.T, x *FreelistTxn) {
			if _, err := x.AllocateAppend(); err != nil {
				t.Fatal(err)
			}
		}},
		{"reuse", func(t *testing.T, x *FreelistTxn) {
			if _, err := x.Allocate(0); err != nil {
				t.Fatal(err)
			}
		}},
		{"contiguous append", func(t *testing.T, x *FreelistTxn) {
			if _, err := x.allocateContiguousRange(4); err != nil {
				t.Fatal(err)
			}
		}},
		{"reserve reused", func(t *testing.T, x *FreelistTxn) {
			if err := x.ReservePage(4); err != nil {
				t.Fatal(err)
			}
		}},
		{"retire", func(t *testing.T, x *FreelistTxn) { x.Retire(9, 3) }},
		{"retire batch", func(t *testing.T, x *FreelistTxn) {
			x.retireMany([]retiredPage{{id: 9, lastReachableCommitSeq: 3}, {id: 10, lastReachableCommitSeq: 3}})
		}},
		{"prune", func(t *testing.T, x *FreelistTxn) {
			cap, err := NewReuseCapability(10, 10, 0)
			if err != nil {
				t.Fatal(err)
			}
			x.PruneWithCapability(cap)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := fresh(t)
			tt.mutate(t, txn)
			if !txn.requiresGeneration {
				t.Fatal("actual allocator mutation lost its generation obligation")
			}
			clone, err := txn.cloneForAllocatorPrepare()
			if err != nil || !clone.requiresGeneration {
				t.Fatal("clone lost actual generation obligation", err)
			}
		})
	}
}
