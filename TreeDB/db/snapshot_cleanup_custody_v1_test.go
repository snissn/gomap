package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
	"time"
)

func TestPrivateSnapshotFailedCloseRetainsExactBackingUntilReaderExit(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	creator := database.idx.Load().creator
	warm, err := database.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	if err = closeOneShotReadV1(&warm); err != nil {
		t.Fatal(err)
	}
	before := creator.OwnerStats()
	r, err := database.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	s := r.snapshot
	c := s.originalCleanup
	if err = c.RetainOriginalCleanupV1(); err != nil {
		t.Fatal(err)
	}
	if err = s.beginRead(); err != nil {
		t.Fatal(err)
	}
	if err = closeOneShotReadV1(&r); err != nil {
		t.Fatal(err)
	}
	if r != nil || c.ObserveOriginalCleanupV1().Complete() || !database.hasFailedSnapshotCleanupV1() {
		t.Fatal("private close dropped original waiting custody")
	}
	held := creator.OwnerStats()
	if held.Live <= before.Live {
		t.Fatal("waiting private backing was refunded")
	}
	if err = s.endReadChecked(); err != nil {
		t.Fatal(err)
	}
	s = nil
	if !c.ObserveOriginalCleanupV1().Complete() {
		t.Fatal("last read did not complete")
	}
	c.ReleaseOriginalCleanupV1()
	c = nil
	if database.hasFailedSnapshotCleanupV1() {
		t.Fatal("completed invocation remained attached")
	}
	after := creator.OwnerStats()
	if after.Live != before.Live || after.Births <= before.Births {
		t.Fatalf("last private observer did not discharge exact backing: before=%+v after=%+v", before, after)
	}
}

func TestSnapshotCleanupReentrantDBCloseRefusesBeforePhysicalTeardown(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s := database.AcquireSnapshot()
	if s == nil {
		t.Fatal("capture")
	}
	calls := 0
	s.iteratorMu.Lock()
	s.foregroundReadEnd = func() {
		calls++
		if err := database.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
			t.Errorf("reentrant DB Close: %v", err)
		}
		if database.closing.Load() || database.idx.Load().handlesClosed.Load() {
			t.Error("reentrant close crossed physical teardown")
		}
	}
	s.iteratorMu.Unlock()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || database.hasFailedSnapshotCleanupV1() {
		t.Fatal("original callback/custody was replayed or retained")
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCleanupConcurrentDBCloseDoesNotWaitForOwnCallback(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s := database.AcquireSnapshot()
	entered, proceed := make(chan struct{}), make(chan struct{})
	s.iteratorMu.Lock()
	s.foregroundReadEnd = func() { close(entered); <-proceed }
	s.iteratorMu.Unlock()
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	<-entered
	closing := make(chan error, 1)
	go func() { closing <- database.Close() }()
	select {
	case err := <-closing:
		if !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
			t.Fatalf("concurrent close: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(proceed)
		t.Fatal("Close waited for its active cleanup callback")
	}
	if database.closing.Load() {
		t.Fatal("running callback lost pre-teardown custody")
	}
	close(proceed)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not join")
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCleanupPanicLeavesActualDBOwnerAndNoCallbackReplay(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s := database.AcquireSnapshot()
	c := s.originalCleanup
	calls := 0
	t.Cleanup(func() { finishKnownSnapshotCallbackFaultFixtureV1(t, database, s, c, &calls) })
	s.iteratorMu.Lock()
	s.foregroundReadEnd = func() { calls++; panic("uncertain original callback") }
	s.iteratorMu.Unlock()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic missing")
			}
		}()
		_ = s.Close()
	}()
	if c.ObserveOriginalCleanupV1().Phase != rootpublication.StableCleanupUncertainV1 || !database.hasFailedSnapshotCleanupV1() || c.creator == nil {
		t.Fatal("panic dropped original owner")
	}
	if err = s.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
		t.Fatalf("uncertain Close: %v", err)
	}
	if err = database.Close(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
		t.Fatalf("uncertain DB Close: %v", err)
	}
	if calls != 1 || !database.hasFailedSnapshotCleanupV1() {
		t.Fatal("uncertainty was replayed or laundered")
	}
}

// The set must retain its exact original token while a real Snapshot iterator
// delays cleanup, then retry only the remaining token role after iterator Close.
func TestStableSnapshotTokenSetReleaseRetriesHeldIteratorWithoutCounterReplay(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact stable relative namespace unavailable on this platform")
	}
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("stable Snapshot unavailable")
	}
	it, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := snapshot.CaptureStableIndexFileResource()
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err = resources.Release(); !errors.Is(err, rootpublication.ErrStableResourceOperationBusy) {
		t.Fatalf("held iterator release: %v", err)
	}
	if resources.Owner() == rootpublication.ResourceOwnerReleased || token.CleanupCompleteV1() || snapshot.CleanupCompleteV1() || database.stableIndexCaptures.Load() != 1 {
		t.Fatal("set dropped actual original Snapshot/counter responsibility")
	}
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
	it = nil
	if !snapshot.CleanupCompleteV1() || database.stableIndexCaptures.Load() != 1 {
		t.Fatal("iterator exit did not finish Snapshot roles with token-owned counter retained")
	}
	if err = resources.Release(); err != nil {
		t.Fatal(err)
	}
	if !token.CleanupCompleteV1() || resources.Owner() != rootpublication.ResourceOwnerReleased || database.stableIndexCaptures.Load() != 0 {
		t.Fatal("retry did not discharge the same actual token/counter")
	}
	if err = resources.Release(); err != nil {
		t.Fatal(err)
	}
}
