package memtable

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCOWViewConcurrentAdmissionCapacityAndRefund(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxViews = 7
	b, w := cowTestWriter(t, limits)
	root := cowTestPublish(t, w, nil)
	t.Cleanup(func() {
		cowTestRelease(root)
		cowTestClose(w)
		b.Close()
	})
	before := b.Stats()
	const attempts = 32
	type result struct {
		view *COWView
		err  error
	}
	results := make(chan result, attempts)
	start := make(chan struct{})
	for range attempts {
		go func() {
			<-start
			view, err := root.Acquire(4096)
			results <- result{view, err}
		}()
	}
	close(start)
	var views []*COWView
	for range attempts {
		r := <-results
		if r.err == nil {
			views = append(views, r.view)
			t.Cleanup(r.view.Close)
		} else if !errors.Is(r.err, ErrCOWCapacity) || r.view != nil {
			t.Errorf("admission: view=%p err=%v", r.view, r.err)
		}
	}
	if len(views) != limits.MaxViews || b.Stats().Views != limits.MaxViews {
		t.Fatalf("admitted %d views: %+v", len(views), b.Stats())
	}
	// Concurrent duplicate closes must refund each successful admission once.
	var closes sync.WaitGroup
	for _, view := range views {
		for range 2 {
			closes.Go(view.Close)
		}
	}
	closes.Wait()
	if after := b.Stats(); after.Views != 0 || after.TotalBytes != before.TotalBytes || after.DeferredBytes != 0 {
		t.Fatalf("view charges did not refund: before=%+v after=%+v", before, after)
	}
	for _, extra := range []uint64{limits.MaxTotalBytes - before.TotalBytes, ^uint64(0)} {
		before = b.Stats()
		if view, err := root.Acquire(extra); view != nil || !errors.Is(err, ErrCOWCapacity) {
			t.Fatalf("byte capacity admission with extra=%d: view=%p err=%v", extra, view, err)
		}
		if after := b.Stats(); after != before {
			t.Fatalf("refused admission changed accounting: before=%+v after=%+v", before, after)
		}
	}
}

func TestCOWViewConcurrentAdmissionAndLastOwnerClose(t *testing.T) {
	for range 32 {
		b, w := cowTestWriter(t, DefaultCOWLimits())
		root := cowTestPublish(t, w, []COWMutation{{Key: []byte("key"), Value: []byte("old")}})
		anchor, err := root.Acquire(0)
		if err != nil {
			t.Fatal(err)
		}
		const attempts = 16
		results := make(chan *COWView, attempts)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range attempts {
			workers.Go(func() {
				<-start
				view, err := root.Acquire(128)
				if err != nil && !errors.Is(err, ErrCOWClosed) {
					t.Errorf("admission racing close: %v", err)
				}
				results <- view
			})
		}
		workers.Go(func() {
			<-start
			b.Close()
			cowTestRelease(root)
			cowTestClose(w)
			anchor.Close()
		})
		close(start)
		workers.Wait()
		close(results)
		// Only successful admissions can keep this generation alive now, even
		// when its other owners closed before the acquiring goroutine returned.
		for view := range results {
			if view == nil {
				continue
			}
			got, ok, err := view.Get([]byte("key"))
			if err != nil || !ok || got.Value != "old" {
				t.Errorf("admitted view lost generation: record=%+v found=%v err=%v", got, ok, err)
			}
			view.Close()
		}
		if view, err := root.Acquire(0); view != nil || !errors.Is(err, ErrCOWClosed) {
			t.Fatalf("closed owner admitted: view=%p err=%v", view, err)
		}
		if stats := b.Stats(); stats.TotalBytes != 0 || stats.Views != 0 || stats.Generations != 0 || stats.DeferredBytes != 0 {
			t.Fatalf("admission racing close leaked: %+v", stats)
		}
	}
}

func TestCOWViewSingleAllocationAndClosePaths(t *testing.T) {
	t.Run("single-alloc-and-fast-close", func(t *testing.T) {
		b, w := cowTestWriter(t, DefaultCOWLimits())
		root := cowTestPublish(t, w, []COWMutation{{Key: []byte("k"), Value: []byte("v")}})
		defer func() {
			cowTestRelease(root)
			cowTestClose(w)
			b.Close()
			if stats := b.Stats(); stats.TotalBytes != 0 || stats.Views != 0 || stats.DeferredBytes != 0 {
				t.Errorf("leaked after close: %+v", stats)
			}
		}()

		before := b.Stats()
		allocs := testing.AllocsPerRun(100, func() {
			v, err := root.Acquire(0)
			if err != nil {
				t.Fatal(err)
			}
			v.Close()
		})
		if allocs > 1 {
			t.Fatalf("COWRoot.Acquire+Close allocated %.0f objects, want <= 1", allocs)
		}
		after := b.Stats()
		after.PeakBytes = before.PeakBytes
		if after != before {
			t.Fatalf("fast close accounting mismatch: before=%+v after=%+v", before, after)
		}
	})

	t.Run("retiring-generation-holds-deferred-bytes-until-callbacks-finish", func(t *testing.T) {
		b, w := cowTestWriter(t, DefaultCOWLimits())
		p, err := w.Prepare([]COWMutation{{Key: []byte("k"), Value: []byte("v")}}, COWPrepareOptions{ResourceSlots: 1})
		if err != nil {
			t.Fatal(err)
		}
		var callbackRan atomic.Int32
		var deferredDuringCallback atomic.Uint64
		var viewsDuringCallback atomic.Int64
		if err := p.AttachResources([]COWResourceID{{Kind: 1, ID: 42}}, func() {
			st := b.Stats()
			deferredDuringCallback.Store(st.DeferredBytes)
			viewsDuringCallback.Store(int64(st.Views))
			callbackRan.Add(1)
		}); err != nil {
			t.Fatal(err)
		}
		root := p.Publish()
		view, err := root.Acquire(0)
		if err != nil {
			t.Fatal(err)
		}
		// Release root, close writer, and close budget so view is the sole last owner.
		cowTestRelease(root)
		cowTestClose(w)
		b.Close()

		view.Close()
		view.Close() // idempotent duplicate Close

		if callbackRan.Load() != 1 {
			t.Fatalf("resource callback ran %d times, want 1", callbackRan.Load())
		}
		if deferredDuringCallback.Load() == 0 {
			t.Fatal("view charge was not held in DeferredBytes during retiring generation callback")
		}
		if viewsDuringCallback.Load() != 0 {
			t.Fatalf("Views=%d during callback, want 0", viewsDuringCallback.Load())
		}
		if stats := b.Stats(); stats.TotalBytes != 0 || stats.Views != 0 || stats.Generations != 0 || stats.DeferredBytes != 0 {
			t.Fatalf("retiring view close leaked: %+v", stats)
		}
	})

	t.Run("maintenance-child-fast-close-releases-control-and-reservation", func(t *testing.T) {
		parent, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		child, err := parent.ReserveMaintenance(1 << 20)
		if err != nil {
			parent.Close()
			t.Fatal(err)
		}
		w, err := NewCOWWriter(child)
		if err != nil {
			child.Close()
			parent.Close()
			t.Fatal(err)
		}
		root := cowTestPublish(t, w, []COWMutation{{Key: []byte("k"), Value: []byte("v")}})
		v1, err := root.Acquire(0)
		if err != nil {
			t.Fatal(err)
		}
		v2, err := root.Acquire(0)
		if err != nil {
			t.Fatal(err)
		}
		cowTestRelease(root)
		cowTestClose(w)
		child.Close()
		// Closing v1 takes fast path (retirement.generation == nil because v2 pins root).
		v1.Close()
		if child.Stats().Views != 1 || child.Stats().ControlBytes == 0 {
			t.Fatalf("after v1 close: %+v", child.Stats())
		}
		// Closing v2 retires the generation and drains child + maintenance reservation.
		v2.Close()
		if stats := child.Stats(); stats.TotalBytes != 0 || stats.ControlBytes != 0 || stats.Views != 0 {
			t.Fatalf("child leaked: %+v", stats)
		}
		parent.Close()
		if stats := parent.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Fatalf("parent leaked: %+v", stats)
		}
	})
}
