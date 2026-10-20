package caching

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is the original publication's custody, not a replacement frontier. Its
// release pauses after physical completion while the accepted queue owns it.
type checkpointRetirementCustody struct {
	entered, release chan struct{}
	calls            atomic.Int32
}

func (c *checkpointRetirementCustody) ReleaseNativeCustody() *rootpublication.SemanticCleanup {
	if c.calls.Add(1) != 1 {
		panic("original checkpoint custody released twice")
	}
	close(c.entered)
	<-c.release
	return nil
}

type checkpointRetirementBackend struct {
	*backenddb.DB
	unpausePhysical func()
	physicalDone    chan struct{}
	returnBoundary  chan struct{}
	first           atomic.Bool
}

func (b *checkpointRetirementBackend) Checkpoint() error {
	if !b.first.Swap(false) {
		return b.DB.Checkpoint()
	}
	// The cache has installed the accepted basis and completed its handoff.
	// Only now permit the original physical publisher to reach durability.
	b.unpausePhysical()
	err := b.DB.Checkpoint()
	close(b.physicalDone)
	<-b.returnBoundary
	return err
}

func TestCOWCheckpointJoinsOriginalAcceptedRetirement(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	registerNativeBridgeTestCommand()
	custody := &checkpointRetirementCustody{entered: make(chan struct{}), release: make(chan struct{})}
	physicalRelease, returnBoundary := make(chan struct{}), make(chan struct{})
	var physicalOnce, custodyOnce, boundaryOnce sync.Once
	unpause := func() { physicalOnce.Do(func() { close(physicalRelease) }) }
	releaseCustody := func() { custodyOnce.Do(func() { close(custody.release) }) }
	returnPhysical := func() { boundaryOnce.Do(func() { close(returnBoundary) }) }
	defer func() { unpause(); releaseCustody(); returnPhysical() }()
	var observerOnce sync.Once
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == backend.Dir() && event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.BeforeMetaWrite {
			observerOnce.Do(func() { <-physicalRelease })
		}
		return nil
	})
	defer restore()
	wrapped := &checkpointRetirementBackend{DB: backend, unpausePhysical: unpause, physicalDone: make(chan struct{}), returnBoundary: returnBoundary}
	wrapped.first.Store(true)

	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Custody: custody}
	result, writeErr := backend.WriteNativePublication(publication, intent, false)
	closeErr := capture.Close()
	if writeErr != nil || closeErr != nil || !result.LogicalInstalled || !result.Assigned {
		t.Fatalf("original publication=%+v write=%v capture=%v", result, writeErr, closeErr)
	}
	cache.backend = wrapped
	through := cache.cow.prefix.LogicalFrontiers()
	done := make(chan error, 1)
	go func() { done <- cache.Checkpoint() }()
	wait := func(ch <-chan struct{}, label string) {
		t.Helper()
		select {
		case <-ch:
		case err := <-done:
			t.Fatalf("checkpoint returned before %s: %v", label, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("checkpoint did not reach %s", label)
		}
	}
	wait(wrapped.physicalDone, "original physical completion")
	wait(custody.entered, "original accepted retirement")
	// The wrapper rendezvous keeps the checkpoint at its physical boundary;
	// channel synchronization makes this observation independent of scheduling.
	cut := cache.cow.retainCut()
	frontiers := cache.cow.prefix.LogicalFrontiers()
	basisSequence := cut.basis.logicalSeq
	if err := cache.cow.releaseCut(cut); err != nil {
		t.Fatal(err)
	}
	if basisSequence != through.V || frontiers.S.LogicalSequence >= through.V {
		t.Fatalf("did not expose real physical/logical gap: basis=%d through=%+v now=%+v", basisSequence, through, frontiers)
	}
	returnPhysical()
	select {
	case err := <-done:
		t.Fatalf("checkpoint returned before original custody release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseCustody()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint did not join original accepted retirement")
	}
	if got := cache.cow.prefix.LogicalFrontiers(); got.S.LogicalSequence != through.V || got.S.CanonicalLSN != through.C || custody.calls.Load() != 1 {
		t.Fatalf("original retirement not complete once: %+v through=%+v calls=%d", got, through, custody.calls.Load())
	}
	if err := cache.RefreshCOWCommandWALCheckpointFallback(); err != nil {
		t.Fatalf("exact fallback after joined checkpoint: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cache.cow.budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("original final accounting: %+v", stats)
	}
}
