package db

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type nativeCloseBarrierOwnerV6 struct {
	coordinator *rootpublication.Coordinator
	receipt     *rootpublication.NativePublicationV6
	failure     error
	calls       int
	gate        *DB
}

func (o *nativeCloseBarrierOwnerV6) CloseNativePublicationV6() (retainedalloc.Lease, error) {
	o.calls++
	// Genuine join callbacks must be outside the DB teardown gate.
	if o.gate != nil {
		o.gate.teardownMu.Lock()
		o.gate.teardownMu.Unlock()
	}
	if o.failure != nil {
		return nil, o.failure
	}
	return nil, o.coordinator.CompleteNativePublicationV6(o.receipt, nil, false)
}
func newNativeCloseBarrierCoordinatorV6(t *testing.T) *rootpublication.Coordinator {
	t.Helper()
	c, err := rootpublication.New(rootpublication.Options{Publisher: rootpublication.PublisherFunc(func(context.Context, *rootpublication.PreparedRootCandidate) rootpublication.PublishResult {
		t.Error("unexpected publisher")
		return rootpublication.PublishResult{}
	})})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNativeIncompleteClosePreservesOriginalRuntimeV6(t *testing.T) {
	c := newNativeCloseBarrierCoordinatorV6(t)
	d := &DB{rootPublication: &rootPublicationRuntimeV1{coordinator: c}}
	runtime := d.rootPublication
	failure := errors.New("original native completion refused")
	owner := &nativeCloseBarrierOwnerV6{coordinator: c, failure: failure, gate: d}
	native := &NativeMVCCCurrentDeleteV6{db: d, phase: 4}
	owner.receipt = &native.publication
	// Close has started, but its pre-close hook still runs before closing=true.
	// This producer crosses the original read gate and registers before Close
	// crosses the exclusive gate. The actual join must see that original link.
	d.RegisterCloseHook(func() error {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := native.BindNativeMVCCPublicationV6(owner, &w)
		if !ready && err == nil {
			return errors.New("prior bind unexpectedly refused")
		}
		return err
	})
	first := d.Close()
	if !errors.Is(first, failure) || d.rootPublication != runtime || !c.HasOutstandingNativePublicationsV6() || !d.closing.Load() {
		t.Fatal("incomplete close detached custody", first)
	}
	if again := d.Close(); !errors.Is(again, failure) || owner.calls != 1 || d.rootPublication != runtime {
		t.Fatal("sticky close replayed/detached original graph", again, owner.calls)
	}
	if err := c.Stop(context.Background()); !errors.Is(err, failure) || !c.HasOutstandingNativePublicationsV6() {
		t.Fatal("stop lost live receipt", err)
	}
	// Test-only original cancellation, before any enqueue/adoption: settle the
	// same receipt so this control does not leave a scheduler/owner behind.
	owner.failure = nil
	if err := c.JoinNativePublicationsV6(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.HasOutstandingNativePublicationsV6() {
		t.Fatal("completed original still live")
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCompletedCloseRetainsOrdinaryTeardownV6(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	c := d.rootPublication.coordinator
	r := rootpublication.NativePublicationV6{}
	owner := &nativeCloseBarrierOwnerV6{coordinator: c, receipt: &r, gate: d}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := c.BindNativePublicationV6(&r, owner, &w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if owner.calls != 1 || c.HasOutstandingNativePublicationsV6() || d.rootPublication != nil {
		t.Fatal("completed join blocked ordinary teardown")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeBindingCrossesOriginalTeardownGateV6(t *testing.T) {
	c := newNativeCloseBarrierCoordinatorV6(t)
	d := &DB{rootPublication: &rootPublicationRuntimeV1{coordinator: c}}
	native := &NativeMVCCCurrentDeleteV6{db: d, phase: 4}
	owner := &nativeCloseBarrierOwnerV6{coordinator: c, receipt: &native.publication}
	d.teardownMu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := native.BindNativeMVCCPublicationV6(owner, &w)
		if ready {
			done <- errors.New("late bind accepted")
			return
		}
		done <- err
	}()
	<-started
	d.closing.Store(true)
	// Detaching under the SAME gate also proves Bind must recheck runtime.
	d.rootPublication = nil
	d.teardownMu.Unlock()
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatal("late registration crossed close cut", err)
	}
	if c.HasOutstandingNativePublicationsV6() || native.publicationBound {
		t.Fatal("late registration retained an edge")
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNoDebtClosePreservesOrdinaryHookErrorV6(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := errors.New("ordinary close hook failed")
	d.RegisterCloseHook(func() error { return ordinary })
	if err := d.Close(); !errors.Is(err, ordinary) || d.rootPublication != nil {
		t.Fatal("ordinary error became incomplete native teardown", err)
	}
}
