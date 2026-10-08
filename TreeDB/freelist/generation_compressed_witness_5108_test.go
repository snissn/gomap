package freelist

import (
	"crypto/sha256"
	"hash"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

// Test-only escape sinks expose real Go classes; production has no pool/cache.
var patriciaDigestClassWitness5108 [64]hash.Hash
var patriciaBranchClassWitness5108 [64]*stateNode
var patriciaChunkClassWitness5108 [64]*stateChunk
var patriciaPlanClassWitness5108 [64]*indexPagePlanV1
var patriciaTxnClassWitness5108 [64]*FreelistTxn
var patriciaGenerationClassWitness5108 [64]*FreelistGenerationV1
var patriciaCandidateClassWitness5108 [64]*FreelistCandidateV1
var patriciaPreparedClassWitness5108 [64]*PreparedCOWCandidateV1
var patriciaCreatorClassWitness5108 [64]*allocationCreditLeaseV1
var patriciaLedgerClassWitness5108 [64]*ReservationLedger
var patriciaReservationClassWitness5108 [64]*reservation
var patriciaCutClassWitness5108 [64]*PublishedGenerationLeaseV1
var patriciaRadixClassWitness5108 [64]*numericRadixV1[uint64, struct{}]
var patriciaRadixChunkClassWitness5108 [64]*numericRadixChunkV1[uint64, struct{}]
var patriciaOwnerRadixChunkClassWitness5108 [64]*numericRadixChunkV1[uint64, CandidateIDV1]
var patriciaCandidateRadixChunkClassWitness5108 [64]*numericRadixChunkV1[CandidateIDV1, *reservation]

type patriciaClassWitnessV25108 struct {
	name                string
	raw, wantRaw, class uint64
	birth, clear        func()
}

var patriciaAllocatorClassWitness5108 [64]*Allocator
var patriciaCOWControlClassWitness5108 [64]*allocatorCOWStateV1
var patriciaCondClassWitness5108 [64]*sync.Cond
var patriciaRecordingClassWitness5108 [64]*recordingSink
var patriciaPrunePlanClassWitness5108 [64]*boundedPrunePlanV1
var patriciaOwnedSinkClassWitness5108 [64]AppendPageSink
var patriciaPacketClassWitness5108 [64]*PreparedCOWPublicationPacketV1
var patriciaPacketProofClassWitness5108 [64]*packetReservationProofV1
var patriciaStageControlClassWitness5108 [64]*privateCOWStageControlV1

func TestCompressedAllocatorActualClassWitness5108(t *testing.T) {
	t.Run("owned-sink-zero-backing", func(t *testing.T) {
		if unsafe.Sizeof(ownedCandidatePageSinkV1{}) != 0 {
			t.Fatal("owned sink has retained fields")
		}
		allocations := testing.AllocsPerRun(100, func() {
			for i := range patriciaOwnedSinkClassWitness5108 {
				patriciaOwnedSinkClassWitness5108[i] = NewOwnedCandidatePageSinkV1()
			}
		})
		clear(patriciaOwnedSinkClassWitness5108[:])
		if allocations != 0 {
			t.Fatalf("zero-field value boxed heap backing: %g", allocations)
		}
	})

	classes := []patriciaClassWitnessV25108{
		{"branch", uint64(unsafe.Sizeof(stateNode{})), 336, stateNodeCopyCapacityV1,
			func() {
				for i := range patriciaBranchClassWitness5108 {
					patriciaBranchClassWitness5108[i] = new(stateNode)
				}
			},
			func() { clear(patriciaBranchClassWitness5108[:]) }},
		{"chunk", uint64(unsafe.Sizeof(stateChunk{})), 2136, stateChunkCopyCapacityV1,
			func() {
				for i := range patriciaChunkClassWitness5108 {
					patriciaChunkClassWitness5108[i] = new(stateChunk)
				}
			},
			func() { clear(patriciaChunkClassWitness5108[:]) }},
		{"transaction", uint64(unsafe.Sizeof(FreelistTxn{})), 368, 384,
			func() {
				for i := range patriciaTxnClassWitness5108 {
					patriciaTxnClassWitness5108[i] = new(FreelistTxn)
				}
			},
			func() { clear(patriciaTxnClassWitness5108[:]) }},
		{"generation", uint64(unsafe.Sizeof(FreelistGenerationV1{})), 320, 320,
			func() {
				for i := range patriciaGenerationClassWitness5108 {
					patriciaGenerationClassWitness5108[i] = new(FreelistGenerationV1)
				}
			},
			func() { clear(patriciaGenerationClassWitness5108[:]) }},
		{"plan", uint64(unsafe.Sizeof(indexPagePlanV1{})), 592, 640,
			func() {
				for i := range patriciaPlanClassWitness5108 {
					patriciaPlanClassWitness5108[i] = new(indexPagePlanV1)
				}
			},
			func() { clear(patriciaPlanClassWitness5108[:]) }},
	}
	// These controls and full 32-slot chunks are present in the retained
	// caller census even when physical state uses only a direct chunk root.
	// Check the charge function against actual heap classes, without treating
	// successful ordinary allocations as finite admission.
	classes = append(classes, patriciaClassWitnessV25108{"candidate", uint64(unsafe.Sizeof(FreelistCandidateV1{})), uint64(unsafe.Sizeof(FreelistCandidateV1{})), allocationClassV1(uint64(unsafe.Sizeof(FreelistCandidateV1{})), true),
		func() {
			for i := range patriciaCandidateClassWitness5108 {
				patriciaCandidateClassWitness5108[i] = new(FreelistCandidateV1)
			}
		},
		func() { clear(patriciaCandidateClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"prepared", uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})), uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})), allocationClassV1(uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})), true),
		func() {
			for i := range patriciaPreparedClassWitness5108 {
				patriciaPreparedClassWitness5108[i] = new(PreparedCOWCandidateV1)
			}
		},
		func() { clear(patriciaPreparedClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"creator", uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), allocationClassV1(uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), true),
		func() {
			for i := range patriciaCreatorClassWitness5108 {
				patriciaCreatorClassWitness5108[i] = new(allocationCreditLeaseV1)
			}
		},
		func() { clear(patriciaCreatorClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"ledger", uint64(unsafe.Sizeof(ReservationLedger{})), uint64(unsafe.Sizeof(ReservationLedger{})), allocationClassV1(uint64(unsafe.Sizeof(ReservationLedger{})), true),
		func() {
			for i := range patriciaLedgerClassWitness5108 {
				patriciaLedgerClassWitness5108[i] = new(ReservationLedger)
			}
		},
		func() { clear(patriciaLedgerClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"reservation", uint64(unsafe.Sizeof(reservation{})), uint64(unsafe.Sizeof(reservation{})), allocationClassV1(uint64(unsafe.Sizeof(reservation{})), true),
		func() {
			for i := range patriciaReservationClassWitness5108 {
				patriciaReservationClassWitness5108[i] = new(reservation)
			}
		},
		func() { clear(patriciaReservationClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"physical-cut", uint64(unsafe.Sizeof(PublishedGenerationLeaseV1{})), uint64(unsafe.Sizeof(PublishedGenerationLeaseV1{})), allocationClassV1(uint64(unsafe.Sizeof(PublishedGenerationLeaseV1{})), true),
		func() {
			for i := range patriciaCutClassWitness5108 {
				patriciaCutClassWitness5108[i] = new(PublishedGenerationLeaseV1)
			}
		},
		func() { clear(patriciaCutClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"radix-header", uint64(unsafe.Sizeof(numericRadixV1[uint64, struct{}]{})), uint64(unsafe.Sizeof(numericRadixV1[uint64, struct{}]{})), allocationClassV1(uint64(unsafe.Sizeof(numericRadixV1[uint64, struct{}]{})), true),
		func() {
			for i := range patriciaRadixClassWitness5108 {
				patriciaRadixClassWitness5108[i] = new(numericRadixV1[uint64, struct{}])
			}
		},
		func() { clear(patriciaRadixClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"radix-set-chunk", uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, struct{}]{})), uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, struct{}]{})), allocationClassV1(uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, struct{}]{})), true),
		func() {
			for i := range patriciaRadixChunkClassWitness5108 {
				patriciaRadixChunkClassWitness5108[i] = new(numericRadixChunkV1[uint64, struct{}])
			}
		},
		func() { clear(patriciaRadixChunkClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"radix-owner-chunk", uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, CandidateIDV1]{})), uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, CandidateIDV1]{})), allocationClassV1(uint64(unsafe.Sizeof(numericRadixChunkV1[uint64, CandidateIDV1]{})), true),
		func() {
			for i := range patriciaOwnerRadixChunkClassWitness5108 {
				patriciaOwnerRadixChunkClassWitness5108[i] = new(numericRadixChunkV1[uint64, CandidateIDV1])
			}
		},
		func() { clear(patriciaOwnerRadixChunkClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"radix-candidate-chunk", uint64(unsafe.Sizeof(numericRadixChunkV1[CandidateIDV1, *reservation]{})), uint64(unsafe.Sizeof(numericRadixChunkV1[CandidateIDV1, *reservation]{})), allocationClassV1(uint64(unsafe.Sizeof(numericRadixChunkV1[CandidateIDV1, *reservation]{})), true),
		func() {
			for i := range patriciaCandidateRadixChunkClassWitness5108 {
				patriciaCandidateRadixChunkClassWitness5108[i] = new(numericRadixChunkV1[CandidateIDV1, *reservation])
			}
		},
		func() { clear(patriciaCandidateRadixChunkClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"digest-control", uint64(reflect.TypeOf(sha256.New()).Elem().Size()), 120, 128,
		func() {
			for i := range patriciaDigestClassWitness5108 {
				patriciaDigestClassWitness5108[i] = sha256.New()
			}
		},
		func() { clear(patriciaDigestClassWitness5108[:]) },
	})
	classes = append(classes, patriciaClassWitnessV25108{"allocator-control", uint64(unsafe.Sizeof(Allocator{})), uint64(unsafe.Sizeof(Allocator{})), allocationClassV1(uint64(unsafe.Sizeof(Allocator{})), true), func() {
		for i := range patriciaAllocatorClassWitness5108 {
			patriciaAllocatorClassWitness5108[i] = new(Allocator)
		}
	}, func() { clear(patriciaAllocatorClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"cow-control", uint64(unsafe.Sizeof(allocatorCOWStateV1{})), uint64(unsafe.Sizeof(allocatorCOWStateV1{})), allocationClassV1(uint64(unsafe.Sizeof(allocatorCOWStateV1{})), true), func() {
		for i := range patriciaCOWControlClassWitness5108 {
			patriciaCOWControlClassWitness5108[i] = new(allocatorCOWStateV1)
		}
	}, func() { clear(patriciaCOWControlClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"condition-control", uint64(unsafe.Sizeof(sync.Cond{})), uint64(unsafe.Sizeof(sync.Cond{})), allocationClassV1(uint64(unsafe.Sizeof(sync.Cond{})), true), func() {
		for i := range patriciaCondClassWitness5108 {
			patriciaCondClassWitness5108[i] = new(sync.Cond)
		}
	}, func() { clear(patriciaCondClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"recording-control", uint64(unsafe.Sizeof(recordingSink{})), uint64(unsafe.Sizeof(recordingSink{})), allocationClassV1(uint64(unsafe.Sizeof(recordingSink{})), true), func() {
		for i := range patriciaRecordingClassWitness5108 {
			patriciaRecordingClassWitness5108[i] = new(recordingSink)
		}
	}, func() { clear(patriciaRecordingClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"bounded-prune-control", uint64(unsafe.Sizeof(boundedPrunePlanV1{})), uint64(unsafe.Sizeof(boundedPrunePlanV1{})), allocationClassV1(uint64(unsafe.Sizeof(boundedPrunePlanV1{})), true), func() {
		for i := range patriciaPrunePlanClassWitness5108 {
			patriciaPrunePlanClassWitness5108[i] = new(boundedPrunePlanV1)
		}
	}, func() { clear(patriciaPrunePlanClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"publication-packet", uint64(unsafe.Sizeof(PreparedCOWPublicationPacketV1{})), uint64(unsafe.Sizeof(PreparedCOWPublicationPacketV1{})), allocationClassV1(uint64(unsafe.Sizeof(PreparedCOWPublicationPacketV1{})), true), func() {
		for i := range patriciaPacketClassWitness5108 {
			patriciaPacketClassWitness5108[i] = new(PreparedCOWPublicationPacketV1)
		}
	}, func() { clear(patriciaPacketClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"publication-proof", uint64(unsafe.Sizeof(packetReservationProofV1{})), uint64(unsafe.Sizeof(packetReservationProofV1{})), allocationClassV1(uint64(unsafe.Sizeof(packetReservationProofV1{})), true), func() {
		for i := range patriciaPacketProofClassWitness5108 {
			patriciaPacketProofClassWitness5108[i] = new(packetReservationProofV1)
		}
	}, func() { clear(patriciaPacketProofClassWitness5108[:]) }})
	classes = append(classes, patriciaClassWitnessV25108{"private-stage-control", uint64(unsafe.Sizeof(privateCOWStageControlV1{})), uint64(unsafe.Sizeof(privateCOWStageControlV1{})), allocationClassV1(uint64(unsafe.Sizeof(privateCOWStageControlV1{})), true), func() {
		for i := range patriciaStageControlClassWitness5108 {
			patriciaStageControlClassWitness5108[i] = new(privateCOWStageControlV1)
		}
	}, func() { clear(patriciaStageControlClassWitness5108[:]) }})
	for _, tc := range classes {
		scan := tc.name != "plan"
		if tc.raw != tc.wantRaw || allocationClassV1(tc.raw, scan) != tc.class {
			t.Fatalf("%s raw=%d/class%d want%d/%d", tc.name, tc.raw, allocationClassV1(tc.raw, scan), tc.wantRaw, tc.class)
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		tc.birth()
		runtime.ReadMemStats(&after)
		found := false
		for i, size := range after.BySize {
			if uint64(size.Size) == tc.class {
				delta := size.Mallocs - before.BySize[i].Mallocs
				if delta < 64 {
					t.Fatalf("%s actual class births=%d want>=64", tc.name, delta)
				}
				t.Logf("actual %s raw=%d allocation_class=%d births=%d", tc.name, tc.raw, tc.class, delta)
				found = true
			}
		}
		tc.clear()
		if !found {
			t.Fatalf("runtime has no class %d", tc.class)
		}
	}
	if unsafe.Sizeof(stateRefV1{}) != 16 {
		t.Fatal("value edge gained wrapper backing")
	}
}

func TestCompressedAllocatorTailCountMatchesActualCoalescing5108(t *testing.T) {
	for _, entries := range []int{0, 161, 162, 163, 164} {
		for _, kind := range []ReservationKindV1{ReservationAbandonedAppend, ReservationAppendedData} {
			for _, skipped := range []uint64{0, 1, 2, uint64(^uint32(0)) - 1, uint64(^uint32(0)), uint64(^uint32(0)) + 1} {
				base := make([]ReservationExtentV1, entries)
				for i := range base {
					base[i] = ReservationExtentV1{StartPageID: uint64(2 + 4*i), Count: 1, Kind: kind}
				}
				minimum := uint64(2)
				if entries != 0 {
					minimum = base[entries-1].StartPageID + 1
				}
				assembled := appendReservationRange(append([]ReservationExtentV1(nil), base...), minimum, skipped, ReservationAbandonedAppend, 0)
				assembled = append(assembled, ReservationExtentV1{StartPageID: minimum + skipped, Count: 3, Kind: ReservationTargetMetadata})
				normalized, err := normalizeExtents(assembled)
				if err != nil {
					t.Fatal(err)
				}
				count, err := tailReservationEntryCountV1(base, minimum, minimum+skipped)
				if err != nil || count != uint64(len(assembled)) || count != uint64(len(normalized)) {
					t.Fatalf("E=%d kind=%d skip=%d count=%d assembled=%d normalized=%d err=%v", entries, kind, skipped, count, len(assembled), len(normalized), err)
				}
			}
		}
	}
}

func TestCompressedAllocatorBranchBirthDenialPreservesAllAliases5108(t *testing.T) {
	account := &radixCredit5105{limit: ^uint64(0)}
	creator, err := newComponentAllocationCreator5108(&componentBorrowedRequest5108{}, account)
	if err != nil {
		t.Fatal(err)
	}
	root := mutateChunk(nil, stateRefV1{}, 0, 0, func(c *stateChunk) { c.setFree(2, true) })
	defer releaseStateNodeV1(root)
	before := *root.chunk
	account.limit = account.bytes + stateChunkCopyCapacityV1 // chunk fits, complete split does not
	plan := mutationStateBirthPlanV1(root, 1, 0, true, false, false)
	if plan.nodes != 1 || plan.chunks != 1 {
		t.Fatalf("split plan=%+v", plan)
	}
	op, err := admitAllocationOperationV1(requestForCreator5108(creator), creator, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err == nil {
		op.close()
		t.Fatal("incomplete whole-branch prepayment accepted")
	}
	if !reflect.DeepEqual(*root.chunk, before) {
		t.Fatal("denied split changed old owner/bytes")
	}
	creator.release()
	if account.released != 1 {
		t.Fatal("denied split retained creating owner")
	}
}

func TestCompressedAllocatorSplitCollapseRetainsExactCreators5108(t *testing.T) {
	firstAccount, secondAccount := &radixCredit5105{limit: ^uint64(0)}, &radixCredit5105{limit: ^uint64(0)}
	first, err := newComponentAllocationCreator5108(&componentBorrowedRequest5108{}, firstAccount)
	if err != nil {
		t.Fatal(err)
	}
	original, err := cloneStateChunkOwnedV1(requestForCreator5108(first), nil, false, first)
	if err != nil {
		t.Fatal(err)
	}
	original.setFree(2, true)
	original.setFree(3, true)
	refreshChunkSummaryV1(original)
	root := stateRefV1{chunk: original}
	first.release() // mutable birth frontier retired; chunk remains owned
	second, err := newComponentAllocationCreator5108(&componentBorrowedRequest5108{}, secondAccount)
	if err != nil {
		t.Fatal(err)
	}
	plan := mutationStateBirthPlanV1(root, 1, 0, true, false, false)
	op, err := admitAllocationOperationV1(requestForCreator5108(second), second, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		t.Fatal(err)
	}
	split, err := mutateStateOwnedOperationV1(nil, root, 1, 0, func(c *stateChunk) { c.setFree(2, true) }, true, nil, &op)
	op.close()
	if err != nil {
		t.Fatal(err)
	}
	replaceStateNodeV1(&root, split)
	branch := root.branch
	newChunk := root.branch.child[1].chunk
	if branch.creator != second || original.creator != first || newChunk.creator != second || firstAccount.released != 0 {
		t.Fatal("split rebound an existing physical creator")
	}
	before := firstAccount.bytes
	plan = mutationStateBirthPlanV1(root, 0, 0, true, false, false)
	op, err = admitAllocationOperationV1(requestForCreator5108(second), second, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		t.Fatal(err)
	}
	edited, err := mutateStateOwnedOperationV1(nil, root, 0, 0, func(c *stateChunk) { c.setFree(2, false) }, true, nil, &op)
	op.close()
	if err != nil {
		t.Fatal(err)
	}
	replaceStateNodeV1(&root, edited)
	if firstAccount.bytes != before || firstAccount.released != 0 || original.creator != first {
		t.Fatal("partial deletion refunded or rebound the original whole chunk")
	}
	plan = mutationStateBirthPlanV1(root, 0, 0, true, false, false)
	op, err = admitAllocationOperationV1(requestForCreator5108(second), second, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		t.Fatal(err)
	}
	collapsed, err := mutateStateOwnedOperationV1(nil, root, 0, 0, func(c *stateChunk) { c.setFree(3, false) }, true, nil, &op)
	op.close()
	if err != nil {
		t.Fatal(err)
	}
	replaceStateNodeV1(&root, collapsed)
	if root.chunk != newChunk || root.branch != nil || firstAccount.released != 1 || original.creator != nil || original.freeCount() != 0 || branch.creator != nil {
		t.Fatal("collapse did not scrub/release removed owners and transfer sole child")
	}
	for _, child := range branch.child {
		if !child.zero() {
			t.Fatal("collapsed branch retained alias")
		}
	}
	second.release()
	if secondAccount.released != 0 {
		t.Fatal("retired second frontier lost direct surviving chunk")
	}
	releaseStateNodeV1(root)
	if secondAccount.released != 1 || newChunk.creator != nil || newChunk.freeCount() != 0 {
		t.Fatal("last direct chunk failed exact release")
	}
}
