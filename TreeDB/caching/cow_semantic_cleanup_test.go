package caching

import (
	"errors"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type testSemanticState struct {
	refs     atomic.Int32
	cleanup  rootpublication.SemanticCleanup
	finish   func() error
	calls    int
	resource *struct{ charge uint64 }
}

func newTestSemanticState(finish func() error) *testSemanticState {
	s := &testSemanticState{finish: finish, resource: &struct{ charge uint64 }{8192}}
	s.refs.Store(1)
	s.cleanup.Work = s
	return s
}
func (s *testSemanticState) Retain() {
	if s.refs.Add(1) <= 1 {
		panic("retain dead semantic state")
	}
}
func (s *testSemanticState) Release() *rootpublication.SemanticCleanup {
	n := s.refs.Add(-1)
	if n < 0 {
		panic("semantic underflow")
	}
	if n == 0 {
		return &s.cleanup
	}
	return nil
}
func (s *testSemanticState) SparseProjections() []rootpublication.ParticipantProjection { return nil }
func (s *testSemanticState) PrepareBuild(*backenddb.RootPublicationBuildGroup, *backenddb.Snapshot, *backenddb.StableResourceCaptureLease) error {
	return nil
}
func (s *testSemanticState) PrepareHandoff(latest backenddb.NativeSemanticState, _ *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	latest.Retain()
	return latest, nil
}
func (s *testSemanticState) FinishSemanticCleanup() error {
	s.calls++
	if s.finish != nil {
		if err := s.finish(); err != nil {
			return err
		}
	}
	s.resource = nil
	return nil
}

func TestCOWSemanticPrivateCancelEnqueuesBeforeOpeningCutWithdrawal(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	c.flushWake = make(chan struct{}, 1)
	state := newTestSemanticState(nil)
	c.cut.semanticRootSet = state
	c.writerMu.Lock()
	prepared := cowCutPrepare(t, c, "private")
	prepared.cancel()
	c.writerMu.Unlock()
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	if c.activeCuts.Load() != 1 || db.semanticCleanupTerminal() {
		t.Fatal("worker exited before private opening cut")
	}
	if err := prepared.drainCancelled(); err != nil {
		t.Fatal(err)
	}
	if c.activeCuts.Load() != 0 || c.semanticHead != &state.cleanup || db.semanticCleanupTerminal() {
		t.Fatal("last cut exited before original cleanup transfer")
	}
	db.drainSemanticCleanup()
	if state.calls != 1 || state.resource != nil || !db.semanticCleanupTerminal() {
		t.Fatal("original cleanup did not drain once")
	}
	if db.flushRequested.Load() {
		t.Fatal("cleanup wake requested unrelated writes")
	}
}

func TestCOWSemanticFailureParksSameWorkAndGuardsDestructorAndNotificationClose(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	c.flushWake = make(chan struct{}, 1)
	failure := errors.New("original semantic cleanup failure")
	var destructClose, notifyClose error
	failed := newTestSemanticState(func() error { destructClose = db.Close(); return failure })
	successful := newTestSemanticState(nil)
	original := failed.resource
	notifications := 0
	db.notifyError = func(err error) {
		notifications++
		if err != failure {
			t.Error(err)
		}
		notifyClose = db.Close()
	}
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(failed.Release())
	c.enqueueSemanticLocked(successful.Release())
	c.semanticMu.Unlock()
	db.drainSemanticCleanup()
	if destructClose == nil || notifyClose == nil || db.closing.Load() {
		t.Fatal("same-caller Close was not rejected before side effects")
	}
	if notifications != 1 || db.backgroundError() != failure || c.semanticFailed != &failed.cleanup || failed.cleanup.Work != failed || failed.cleanup.Err != failure || failed.resource != original || original.charge != 8192 {
		t.Fatal("lost original failure/resource/charge")
	}
	db.drainSemanticCleanup()
	if failed.calls != 1 || successful.calls != 1 || successful.resource != nil || failed.resource != original {
		t.Fatal("replayed original work or lost successful sibling refund")
	}
}

func TestCOWSemanticGuardRefusalPoisonsWithoutUnsafeNotification(t *testing.T) {
	db, c := cowCutFixture(t)
	state := newTestSemanticState(nil)
	notifications := 0
	db.notifyError = func(error) { notifications++ }
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(state.Release())
	c.semanticMu.Unlock()
	db.drainSemanticCleanup() // bare test factory deliberately has no original guard
	if db.backgroundError() != rootpublication.ErrPublisherProtocol || state.calls != 0 || state.cleanup.Work != state || c.semanticFailed != &state.cleanup || notifications != 0 {
		t.Fatal("guard refusal lost original terminal custody")
	}
}
