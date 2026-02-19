package caching

import (
	"testing"
	"time"
)

func TestRetainMemtableViewSelfHealsZeroRefPublishedView(t *testing.T) {
	db := &DB{}
	view := &memtableView{}
	db.memtables.Store(view)

	done := make(chan *memtableView, 1)
	go func() {
		done <- db.retainMemtableView()
	}()

	var got *memtableView
	select {
	case got = <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retainMemtableView timed out on zero-ref published view")
	}
	if got != view {
		t.Fatalf("retainMemtableView returned unexpected view: %p want %p", got, view)
	}
	if refs := view.refs.Load(); refs < 2 {
		t.Fatalf("view refs=%d want >= 2 after retain", refs)
	}

	db.releaseMemtableView(got)
	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs after release=%d want=1", refs)
	}
}
