package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"sync/atomic"
	"testing"
)

func TestSnapshotOriginalCompletionWaitsForHeldReadAndScrub(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	s := database.AcquireSnapshot()
	if s == nil {
		t.Fatal("capture")
	}
	c := s.originalCleanup
	if err = c.RetainOriginalCleanupV1(); err != nil {
		t.Fatal(err)
	}
	defer c.ReleaseOriginalCleanupV1()
	if err = s.beginRead(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if c.ObserveOriginalCleanupV1().Complete() || s.pagerCreator == nil {
		t.Fatal("nil Close mistook held reader for completion")
	}
	if err = s.endReadChecked(); err != nil {
		t.Fatal(err)
	}
	if !c.ObserveOriginalCleanupV1().Complete() || s.treePager != nil || s.idx != nil || s.state != nil || s.pagerCreator != nil {
		t.Fatal("completion preceded real Snapshot scrub")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotOriginalCompletionReentrantCloseAndPanicUncertainty(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "reentrant", true: "panic"}[panics], func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			s := database.AcquireSnapshot()
			c := s.originalCleanup
			calls := 0
			t.Cleanup(func() { finishKnownSnapshotCallbackFaultFixtureV1(t, database, s, c, &calls) })
			s.iteratorMu.Lock()
			s.foregroundReadEnd = func() {
				calls++
				if err := s.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
					t.Errorf("reentrant cleanup: %v", err)
				}
				if panics {
					panic("cleanup fault")
				}
			}
			s.iteratorMu.Unlock()
			func() {
				defer func() {
					v := recover()
					if panics && v == nil {
						t.Error("lost cleanup panic")
					}
					if !panics && v != nil {
						t.Errorf("unexpected panic: %v", v)
					}
				}()
				_ = s.Close()
			}()
			if panics {
				out := c.ObserveOriginalCleanupV1()
				if out.Phase != rootpublication.StableCleanupUncertainV1 || c.creator == nil {
					t.Fatal("panic lost original custody")
				}
				if err = s.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
					t.Fatal("uncertain cleanup replayed")
				}
			} else if !s.CleanupCompleteV1() {
				t.Fatal("normal callback did not complete")
			}
			if calls != 1 {
				t.Fatal("callback replay")
			}
		})
	}
}
func TestSnapshotOriginalCompletionCounterOnce(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	s := database.AcquireSnapshot()
	var counter atomic.Int64
	counter.Store(1)
	s.stableIndexCapture = true
	s.stableIndexCaptureCounter = &counter
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if counter.Load() != 0 {
		t.Fatal("original counter replayed")
	}
}

// This fixture owns the entire injected callback: its only effect is the local
// call counter before panic. After the uncertainty/no-replay assertions, the
// test custodian can acknowledge that ONE effect. Production hidden callbacks
// have no such proof and remain uncertain. Real cleanup roles still run normally.
func finishKnownSnapshotCallbackFaultFixtureV1(t *testing.T, database *DB, s *Snapshot, c *OriginalSnapshotCleanupV1, calls *int) {
	t.Helper()
	beforeCalls := *calls
	// A failed assertion before the initial Close must not leave the known
	// test callback installed for cleanup. No real resource role is removed.
	s.iteratorMu.Lock()
	s.foregroundReadEnd = nil
	s.iteratorMu.Unlock()
	c.mu.Lock()
	if c.outcome.Phase == rootpublication.StableCleanupUncertainV1 {
		prior := snapshotVMRoleV1 | snapshotIndexRoleV1 | snapshotLeafRoleV1 | snapshotCounterRoleV1
		if *calls != 1 || c.invoking || c.outcome.ConsumedRoles != prior || c.outcome.StartedRoles&snapshotCallbackRoleV1 == 0 {
			c.mu.Unlock()
			t.Error("injected callback is not the exact remaining uncertain role")
			return
		}
		// Preserve all real consumed roles and retry only scrub/control. This is
		// acknowledgment of the known test effect, never a complete certificate.
		c.outcome.ConsumedRoles |= snapshotCallbackRoleV1
		c.outcome.PendingRoles = snapshotAllRolesV1 &^ c.outcome.ConsumedRoles
		c.outcome.Phase = rootpublication.StableCleanupPendingV1
		c.outcome.Debt = rootpublication.StableCleanupExactHoldsV1
	}
	c.mu.Unlock()
	if err := s.Close(); err != nil {
		t.Errorf("remaining genuine Snapshot cleanup: %v", err)
	}
	if !s.CleanupCompleteV1() || database.hasFailedSnapshotCleanupV1() || *calls != beforeCalls {
		t.Error("fixture recovery replayed callback or bypassed actual cleanup")
	}
	if err := database.Close(); err != nil {
		t.Errorf("joined fixture DB close: %v", err)
	}
}
