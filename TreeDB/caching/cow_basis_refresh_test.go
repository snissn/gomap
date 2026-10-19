package caching

import (
	"errors"
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
			if err = c.close(); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			db.drainSemanticCleanup()
			if original.calls != 1 || original.resource != nil {
				t.Fatal("original opening state did not finish")
			}
		})
	}
}
