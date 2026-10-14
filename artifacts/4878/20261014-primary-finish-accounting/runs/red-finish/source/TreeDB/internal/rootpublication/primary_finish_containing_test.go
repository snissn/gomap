package rootpublication
import ("path/filepath";"testing";"github.com/snissn/gomap/TreeDB/internal/iterator";"github.com/snissn/gomap/TreeDB/internal/memtable";"github.com/snissn/gomap/TreeDB/internal/primaryarena")
// This isolates the original physical constructor Finish/recovery terminal
// boundary. Public caller controls separately exercise its publication route.
func TestPrimaryFinishContainingRefundJoinsOriginalCall(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "finish"
		if recovery {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			a, e := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close()
			original, ready, e := NewPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if !ready || e != nil {
				t.Fatal(ready, e)
			}
			txn := original.(*DurableRootTransaction)
			set, e := NewStableResourceSetBuilder().Freeze()
			if e != nil {
				t.Fatal(e)
			}
			set.owner.Store(uint32(ResourceOwnerCoordinator))
			txn.constructionV6.resources = set
			txn.owner = ResourceOwnerCoordinator
			txn.phase = durableRootFinishing
			budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if e != nil {
				t.Fatal(e)
			}
			observed, locked, lateState := false, false, false
			wrapper := &containingUnlockBudget{actual: budget, observe: func() {
				if txn.constructionV6 == nil {
					observed = true
					lateState = txn.owner != ResourceOwnerReleased || txn.phase != durableRootConsumed
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
			if recovery {
				txn.owner = ResourceOwnerRecovery
				set.owner.Store(uint32(ResourceOwnerRecovery))
				if e = txn.prepareFromRecovery(); e != nil {
					t.Fatal(e)
				}
				if e = set.releaseFrom(ResourceOwnerRecovery); e != nil {
					t.Fatal(e)
				}
				e = txn.releaseFromRecovery()
			} else {
				e = txn.finishOrdinaryPrimaryV6(&iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}, nil)
			}
			if e != nil {
				t.Fatal(e)
			}
			if !observed || locked || lateState {
				t.Fatalf("actual containing refund: observed=%t locked=%t state-written-after-refund=%t", observed, locked, lateState)
			}
			capture.Close()
			budget.Close()
		})
	}
}

