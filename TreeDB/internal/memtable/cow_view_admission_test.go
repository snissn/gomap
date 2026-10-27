package memtable

import (
	"errors"
	"sync"
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
