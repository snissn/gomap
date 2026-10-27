package caching

import (
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type testBasisRefreshState struct {
	*testSemanticState
	partial       backenddb.NativeSemanticState
	failure       error
	before, after *backenddb.Snapshot
}

func (s *testBasisRefreshState) PrepareBasisRefresh(before, after *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	s.before, s.after = before, after
	return s.partial, s.failure
}

func TestCOWBasisRefreshRefusalKeepsOriginalAndOwnsPartialCleanup(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsupported_owner", true: "owned_partial"}[partial], func(t *testing.T) {
			db, c := cowCutFixture(t)
			db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
			original := newTestSemanticState(nil)
			var refreshed *testSemanticState
			var refresh *testBasisRefreshState
			failure := backenddb.ErrNativeSemanticUnsupported
			if partial {
				failure = errors.New("original basis refresh refusal")
				refreshed = newTestSemanticState(func() error {
					if !c.writerMu.TryLock() {
						t.Error("private cleanup ran under writer gate")
					} else {
						c.writerMu.Unlock()
					}
					return nil
				})
				refresh = &testBasisRefreshState{testSemanticState: original, partial: refreshed, failure: failure}
				c.cut.semanticRootSet = refresh
			} else {
				c.cut.semanticRootSet = original
			}
			opening := c.cut
			basis, err := c.captureBackendBasis(db.backend.(*backenddb.DB))
			if err != nil {
				t.Fatal(err)
			}
			old, err := db.installCOWBasis(basis, nil)
			if !errors.Is(err, failure) || old != nil || c.cut != opening || original.refs.Load() != 1 {
				t.Fatalf("refusal changed original cut: old=%p error=%v refs=%d", old, err, original.refs.Load())
			}
			if partial {
				if refresh.before != opening.basis.snapshot || refresh.after != basis.snapshot {
					t.Fatal("not the exact paired original snapshots")
				}
				refresh.before, refresh.after, refresh.partial = nil, nil, nil
				if refreshed.refs.Load() != 0 || refreshed.calls != 0 || c.semanticHead != &refreshed.cleanup {
					t.Fatal("private partial lost or cleaned under install gate")
				}
				db.drainSemanticCleanup()
				if refreshed.calls != 1 || refreshed.resource != nil {
					t.Fatal("original partial cleanup did not finish exactly once")
				}
			}
			if err = basis.release(); err != nil {
				t.Fatal(err)
			}
			// This bare cache fixture has no accepted-prefix owner. installCOWBasis
			// already returned the original refusal; Close reports actual cleanup.
			if err = db.backend.(*backenddb.DB).CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
				t.Fatalf("backend publication fence lost refusal: %v", err)
			}
			if err = c.close(); err != nil {
				t.Fatal(err)
			}
			db.drainSemanticCleanup()
			if original.calls != 1 || original.resource != nil {
				t.Fatal("original opening state did not finish")
			}
		})
	}
}

func TestCOWPhysicalBasisRefreshPreservesAppliedLogicalIdentity(t *testing.T) {
	for _, engine := range []string{"cow_btree", "cow_ordered"} {
		for _, frozen := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/frozen=%t", engine, frozen), func(t *testing.T) {
				cache, backend := cowAutomaticFallbackFixture(t, engine)
				writeCOWAutoFallbackRaw(t, cache, backend, "prefix")
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				before := cache.cow.retainCut()
				logical := before.basis.logicalSeq
				applied, _ := before.basis.snapshot.StateToken()
				if err := cache.cow.releaseCut(before); err != nil {
					t.Fatal(err)
				}
				writeCOWAutoFallbackRaw(t, cache, backend, "suffix")
				if frozen {
					if err := cache.rolloverCOWForFlush(); err != nil {
						t.Fatal(err)
					}
				}
				next := backend.CommandWALNextLSN()
				// Exercise refreshCOWBackendBasis, the other production nil-coverage
				// consumer, under its original writer/maintenance fence.
				if err := cache.RunBackendMaintenance(nil); err != nil {
					t.Fatal(err)
				}
				cut := cache.cow.retainCut()
				if cut == nil {
					t.Fatal("missing refreshed cut")
				}
				state, live := cut.basis.snapshot.StateToken()
				if !live || state.AppliedCommandLSN != applied.AppliedCommandLSN || cut.basis.logicalSeq != logical || cut.logicalSeq <= logical || frozen && len(cut.frozen) == 0 {
					t.Errorf("physical refresh consumed or relabeled suffix: applied=%+v state=%+v basis=%d want=%d visible=%d frozen=%d", applied, state, cut.basis.logicalSeq, logical, cut.logicalSeq, len(cut.frozen))
				}
				if err := cache.cow.releaseCut(cut); err != nil {
					t.Fatal(err)
				}
				if backend.CommandWALNextLSN() != next {
					t.Fatal("physical refresh appended a command")
				}
				if got, err := cache.Get([]byte("key")); err != nil || string(got) != "suffix" {
					t.Fatalf("logical suffix=%q err=%v", got, err)
				}
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if got, err := backend.Get([]byte("key")); err != nil || string(got) != "suffix" {
					t.Fatalf("checkpoint after refresh=%q err=%v", got, err)
				}
			})
		}
	}
}
