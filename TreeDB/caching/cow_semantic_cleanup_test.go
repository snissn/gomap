package caching

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCOWSemanticPressureJoinWaitsForOriginalRefundFrontier(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	c.flushWake = make(chan struct{}, 1)
	started, release := make(chan struct{}), make(chan struct{})
	lateStarted, lateRelease := make(chan struct{}), make(chan struct{})
	first := newTestSemanticState(func() error { close(started); <-release; return nil })
	late := newTestSemanticState(func() error { close(lateStarted); <-lateRelease; return nil })
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(first.Release())
	c.semanticMu.Unlock()
	workerDone := make(chan struct{})
	go func() { db.drainSemanticCleanup(); close(workerDone) }()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-lateRelease:
		default:
			close(lateRelease)
		}
		<-workerDone
	}()
	<-started
	joined := make(chan error, 1)
	go func() { joined <- db.joinSemanticCleanupFrontier() }()
	// The wake is issued under semanticMu after capturing the fixed frontier.
	<-c.flushWake
	select {
	case err := <-joined:
		t.Fatalf("joined before refund: %v", err)
	default:
	}
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(late.Release())
	c.semanticMu.Unlock()
	close(release)
	<-lateStarted
	select {
	case err := <-joined:
		if err != nil || first.resource != nil || first.calls != 1 {
			t.Fatalf("original refund incomplete: %v %+v", err, first)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fixed frontier waited for later producer cleanup")
	}
	close(lateRelease)
	<-workerDone
}

type testSemanticState struct {
	refs     atomic.Int32
	cleanup  rootpublication.SemanticCleanup
	finish   func() error
	refund   rootpublication.SemanticCleanupRefund
	calls    int
	resource *struct{ charge uint64 }
}

func newTestSemanticState(finish func() error) *testSemanticState {
	s := &testSemanticState{finish: finish, resource: &struct{ charge uint64 }{8192}}
	s.refs.Store(1)
	s.cleanup.Work = s
	return s
}
func (s *testSemanticState) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
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
func (s *testSemanticState) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	s.calls++
	if s.finish != nil {
		if err := s.finish(); err != nil {
			return rootpublication.SemanticCleanupCompletion{}, err
		}
	}
	s.resource = nil
	refund := s.refund
	s.refund = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
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

func TestCOWCutReleaseWakesOnlyRunnableCleanupOrClosedTerminal(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "retained_native"}[native], func(t *testing.T) {
			db, c := cowCutFixture(t)
			db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
			c.flushWake = make(chan struct{}, 1)
			var state *testSemanticState
			if native {
				state = newTestSemanticState(nil)
				c.cut.semanticRootSet = state
			}
			c.writerMu.Lock()
			prepared := cowCutPrepare(t, c, "next")
			old := prepared.publish()
			c.writerMu.Unlock()
			if old == nil {
				t.Fatal("original cut was not released")
			}
			if err := old.drain(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.flushWake:
				t.Fatal("open cut release woke worker without runnable cleanup")
			default:
			}
			if err := c.close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.flushWake:
			default:
				t.Fatal("last closed cut did not wake terminal cleanup")
			}
			db.drainSemanticCleanup()
			if !db.semanticCleanupTerminal() || (state != nil && state.calls != 1) {
				t.Fatal("original cleanup/terminal completion did not finish")
			}
		})
	}
}

func TestCOWSemanticFailureParksSameWorkAndGuardsDestructorAndNotificationClose(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	c.flushWake = make(chan struct{}, 1)
	failure := errors.New("original semantic cleanup failure")
	limit := c.budget.Limits().MaxInFlightBytes
	filler, err := c.budget.AcquireExternal(limit - limit/4 - c.budget.Stats().ReservedBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	var destructClose, notifyClose, destructPrepare, notifyPrepare error
	var firstClose, lastClose, firstPrepare, lastPrepare error
	first := newTestSemanticState(func() error {
		firstClose, firstPrepare = db.Close(), db.PrepareNativeProducer()
		return nil
	})
	failed := newTestSemanticState(func() error {
		destructClose = db.Close()
		destructPrepare = db.PrepareNativeProducer()
		return failure
	})
	successful := newTestSemanticState(func() error {
		lastClose, lastPrepare = db.Close(), db.PrepareNativeProducer()
		return nil
	})
	original := failed.resource
	notifications := 0
	db.notifyError = func(err error) {
		notifications++
		if err != failure {
			t.Error(err)
		}
		notifyClose = db.Close()
		notifyPrepare = db.PrepareNativeProducer()
	}
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(first.Release())
	c.enqueueSemanticLocked(failed.Release())
	c.enqueueSemanticLocked(successful.Release())
	c.semanticMu.Unlock()
	db.drainSemanticCleanup()
	if firstClose == nil || lastClose == nil || firstPrepare == nil || lastPrepare == nil || destructClose == nil || notifyClose == nil || destructPrepare == nil || notifyPrepare == nil || db.closing.Load() {
		t.Fatal("same-caller Close was not rejected before side effects")
	}
	if notifications != 1 || db.backgroundError() != failure || c.semanticFailed != &failed.cleanup || failed.cleanup.Work != failed || failed.cleanup.Err != failure || failed.resource != original || original.charge != 8192 {
		t.Fatal("lost original failure/resource/charge")
	}
	db.drainSemanticCleanup()
	if err := db.joinSemanticCleanupFrontier(); err != failure {
		t.Fatalf("pressure join lost original cleanup failure: %v", err)
	}
	if first.calls != 1 || first.resource != nil || failed.calls != 1 || successful.calls != 1 || successful.resource != nil || failed.resource != original {
		t.Fatal("replayed original work or lost successful sibling refund")
	}
}

func TestCOWSemanticGuardRefusalPoisonsWithoutUnsafeNotification(t *testing.T) {
	db, c := cowCutFixture(t)
	state := newTestSemanticState(nil)
	sibling := newTestSemanticState(nil)
	notifications := 0
	db.notifyError = func(error) { notifications++ }
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(state.Release())
	c.enqueueSemanticLocked(sibling.Release())
	c.semanticMu.Unlock()
	db.drainSemanticCleanup() // bare test factory deliberately has no original guard
	if db.backgroundError() != rootpublication.ErrPublisherProtocol || state.calls != 0 || sibling.calls != 0 || state.cleanup.Work != state || sibling.cleanup.Work != sibling || c.semanticFailed != &sibling.cleanup || sibling.cleanup.Next != &state.cleanup || c.semanticCompleted != 2 || notifications != 0 {
		t.Fatal("guard refusal lost original terminal custody")
	}
}

// This test observer deliberately inspects the original owner at the actual
// refund boundary. Production receipts are independent accounting leases only.
type observingSemanticRefund struct {
	lease   *memtable.COWExternalLease
	observe func()
	calls   int
}

func (r *observingSemanticRefund) Close() {
	r.calls++
	r.observe()
	r.lease.Close()
}

func TestCOWSemanticOriginalCompletionScrubsBeforeContainingRefund(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	lease, err := c.budget.AcquireExternal(8192)
	if err != nil {
		t.Fatal(err)
	}
	before := c.budget.Stats()
	state := newTestSemanticState(nil)
	first := newTestSemanticState(nil)
	refund := &observingSemanticRefund{lease: lease}
	refund.observe = func() {
		if state.cleanup.Work != nil || state.cleanup.Next != nil || state.cleanup.Err != nil || state.resource != nil || state.refund != nil {
			t.Fatal("original owner/record aliases survived until containing refund")
		}
		if c.budget.Stats() != before {
			t.Fatal("containing charge refunded before original completion")
		}
		if err := db.Close(); err == nil || db.closing.Load() {
			t.Fatal("refund escaped original Close guard")
		}
		if !c.semanticMu.TryLock() {
			t.Fatal("containing refund ran under queue latch")
		}
		c.semanticMu.Unlock()
	}
	state.refund = refund
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(first.Release())
	c.enqueueSemanticLocked(state.Release())
	c.semanticMu.Unlock()
	db.drainSemanticCleanup()
	if first.calls != 1 || first.resource != nil || refund.calls != 1 || c.budget.Stats().ExternalBytes >= before.ExternalBytes {
		t.Fatal("original containing charge not refunded exactly once")
	}
	db.drainSemanticCleanup()
	if refund.calls != 1 {
		t.Fatal("completion replayed")
	}
}

// Isolate the budget effect of an original queued cleanup refund. The same
// request must refuse while that receipt is held and succeed after the worker
// returns it. This is a controlled owner-level mechanism witness, not evidence
// that cleanup ran during the retained macOS scalar-forward failure.
func TestCOWSemanticRefundChangesAdmissionAfterFullSnapshot(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	c.flushWake = make(chan struct{}, 1)
	budget := c.budget
	beforeReceipt := budget.Stats()
	original, err := budget.AcquireExternal(8192)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	refundCharge := budget.Stats().ReservedBytes - beforeReceipt.ReservedBytes
	request, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	defer request.Close()
	blocker, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	started, release, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	state := newTestSemanticState(func() error { close(started); <-release; return nil })
	state.refund = original
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(state.Release())
	c.semanticMu.Unlock()
	go func() { db.drainSemanticCleanup(); close(workerDone) }()
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Error("controlled cleanup worker did not terminate")
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("invalid witness: original cleanup did not enter held Finish")
	}
	if err = blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatalf("invalid witness: parent saturation: %v", err)
	}
	full := budget.Stats()
	if full.ReservedBytes != budget.Limits().MaxInFlightBytes {
		t.Fatalf("invalid witness: parent not full: %+v", full)
	}
	if err = request.Resize(1024); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("held original refund must preserve parent refusal: %v", err)
	}
	if got := budget.Stats(); got != full {
		t.Fatalf("refused request changed original budget: before=%+v after=%+v", full, got)
	}
	// No sleep or retry: release exactly the original queued work, then wait for
	// its completion/refund. The timeout is solely a failed-witness bound.
	releaseOnce.Do(func() { close(release) })
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("invalid witness: released cleanup did not finish")
	}
	if err = db.joinSemanticCleanupFrontier(); err != nil {
		t.Fatal(err)
	}
	after := budget.Stats()
	if after.ReservedBytes+refundCharge != full.ReservedBytes || after.TotalBytes+refundCharge != full.TotalBytes || after.ExternalLeases+1 != full.ExternalLeases || state.calls != 1 || state.resource != nil || state.refund != nil {
		t.Fatalf("original refund not exact/terminal: charge=%d before=%+v after=%+v calls=%d resource=%p", refundCharge, full, after, state.calls, state.resource)
	}
	if err = request.Resize(1024); err != nil {
		t.Fatalf("same request must admit after original refund: %v", err)
	}
	if got := budget.Stats(); got.ReservedBytes != after.ReservedBytes+1024 || got.ExternalLeases != after.ExternalLeases || got.ReservedBytes > budget.Limits().MaxInFlightBytes {
		t.Fatalf("post-refund admission charge/cap changed: before=%+v after=%+v", after, got)
	}
}

func TestCOWSemanticWakeCoalescingNoLostWakeup(t *testing.T) {
	t.Run("coalesce_and_drain_lifecycle", func(t *testing.T) {
		var nilCache *cowCache
		nilCache.wakeSemanticCleanup()
		emptyCache := &cowCache{}
		emptyCache.wakeSemanticCleanup()
		if emptyCache.semanticWakePending.Load() {
			t.Fatal("nil flushWake should not mark semanticWakePending")
		}

		db, c := cowCutFixture(t)
		db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
		c.flushWake = make(chan struct{}, 1)

		for i := 0; i < 16; i++ {
			c.wakeSemanticCleanup()
		}
		if !c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending not set after wakeSemanticCleanup")
		}
		select {
		case <-c.flushWake:
		default:
			t.Fatal("expected coalesced signal on flushWake")
		}
		select {
		case <-c.flushWake:
			t.Fatal("repeated wakes produced more than one signal")
		default:
		}

		// While pending remains true, additional wakes must coalesce without sending.
		for i := 0; i < 8; i++ {
			c.wakeSemanticCleanup()
		}
		if !c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending unexpectedly cleared before drain")
		}
		select {
		case <-c.flushWake:
			t.Fatal("wake sent while semanticWakePending was already true")
		default:
		}

		started, release, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		first := newTestSemanticState(func() error {
			close(started)
			<-release
			return nil
		})
		second := newTestSemanticState(nil)
		c.semanticMu.Lock()
		c.enqueueSemanticLocked(first.Release())
		c.semanticMu.Unlock()

		go func() {
			db.drainSemanticCleanup()
			close(workerDone)
		}()
		var releaseOnce sync.Once
		defer func() {
			releaseOnce.Do(func() { close(release) })
			<-workerDone
		}()

		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("drainSemanticCleanup did not start first record")
		}
		if !c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending cleared while drain was actively processing")
		}

		// Enqueuing and waking while drain is active stays coalesced with zero channel sends.
		c.semanticMu.Lock()
		c.enqueueSemanticLocked(second.Release())
		c.semanticMu.Unlock()
		c.wakeSemanticCleanup()
		if !c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending cleared during active drain")
		}
		select {
		case <-c.flushWake:
			t.Fatal("unexpected flushWake signal during active drain")
		default:
		}

		releaseOnce.Do(func() { close(release) })
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("drainSemanticCleanup did not finish")
		}
		if first.calls != 1 || second.calls != 1 || first.resource != nil || second.resource != nil {
			t.Fatalf("active drain missed queued records: first=%+v second=%+v", first, second)
		}
		if c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending not cleared when queue drained empty")
		}

		// Next enqueue/wake after drain empties must deliver a fresh signal.
		third := newTestSemanticState(nil)
		c.semanticMu.Lock()
		c.enqueueSemanticLocked(third.Release())
		c.semanticMu.Unlock()
		c.wakeSemanticCleanup()
		if !c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending not re-armed after drain")
		}
		select {
		case <-c.flushWake:
		default:
			t.Fatal("expected fresh flushWake signal after drain cleared semanticWakePending")
		}
		db.drainSemanticCleanup()
		if third.calls != 1 || third.resource != nil || c.semanticWakePending.Load() {
			t.Fatalf("post-rearm drain incomplete: calls=%d resource=%p pending=%v", third.calls, third.resource, c.semanticWakePending.Load())
		}
	})

	t.Run("stress_no_lost_wakeup", func(t *testing.T) {
		db, c := cowCutFixture(t)
		db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
		c.flushWake = make(chan struct{}, 1)

		stop := make(chan struct{})
		workerDone := make(chan struct{})
		go func() {
			defer close(workerDone)
			for {
				select {
				case <-c.flushWake:
					db.drainSemanticCleanup()
				case <-stop:
					db.drainSemanticCleanup()
					return
				}
			}
		}()
		var stopOnce sync.Once
		defer func() {
			stopOnce.Do(func() { close(stop) })
			<-workerDone
		}()

		const (
			producers     = 8
			phase1PerProd = 250
			phase2PerProd = 100
			phase1Total   = producers * phase1PerProd
			totalItems    = producers * (phase1PerProd + phase2PerProd)
		)
		var completed atomic.Int64
		phase1Done := make(chan struct{})

		// Phase 1: Pure wakeSemanticCleanup coalescing stress without joinSemanticCleanupFrontier.
		var wg sync.WaitGroup
		wg.Add(producers)
		for p := 0; p < producers; p++ {
			go func() {
				defer wg.Done()
				for i := 0; i < phase1PerProd; i++ {
					s := newTestSemanticState(func() error {
						if completed.Add(1) == phase1Total {
							close(phase1Done)
						}
						return nil
					})
					c.semanticMu.Lock()
					c.enqueueSemanticLocked(s.Release())
					c.semanticMu.Unlock()
					c.wakeSemanticCleanup()
				}
			}()
		}
		wg.Wait()
		select {
		case <-phase1Done:
		case <-time.After(10 * time.Second):
			t.Fatalf("lost wakeup in phase 1: completed=%d/%d pending=%v", completed.Load(), phase1Total, c.semanticWakePending.Load())
		}

		// Phase 2: Concurrent producers + joinSemanticCleanupFrontier.
		errCh := make(chan error, producers)
		wg.Add(producers)
		for p := 0; p < producers; p++ {
			go func() {
				defer wg.Done()
				for i := 0; i < phase2PerProd; i++ {
					s := newTestSemanticState(func() error {
						completed.Add(1)
						return nil
					})
					c.semanticMu.Lock()
					c.enqueueSemanticLocked(s.Release())
					c.semanticMu.Unlock()
					c.wakeSemanticCleanup()
					if i%25 == 24 {
						if err := db.joinSemanticCleanupFrontier(); err != nil {
							errCh <- err
							return
						}
					}
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatalf("joinSemanticCleanupFrontier failed: %v", err)
		}
		if err := db.joinSemanticCleanupFrontier(); err != nil {
			t.Fatalf("final joinSemanticCleanupFrontier failed: %v", err)
		}
		if got := completed.Load(); got != totalItems {
			t.Fatalf("incomplete cleanup: got %d want %d", got, totalItems)
		}
		stopOnce.Do(func() { close(stop) })
		<-workerDone
		if c.semanticWakePending.Load() {
			t.Fatal("semanticWakePending remained true after final drain")
		}
	})
}

func BenchmarkCOWWakeSemanticCleanupCoalesced(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		c := &cowCache{flushWake: make(chan struct{}, 1)}
		c.wakeSemanticCleanup()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.wakeSemanticCleanup()
		}
	})
	b.Run("parallel", func(b *testing.B) {
		c := &cowCache{flushWake: make(chan struct{}, 1)}
		c.wakeSemanticCleanup()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.wakeSemanticCleanup()
			}
		})
	})
}
