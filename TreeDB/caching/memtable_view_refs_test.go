package caching

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
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

func TestPublishMemtablesDefersRetiredMemtableRecycleUntilFinalRelease(t *testing.T) {
	db := &DB{}
	published := &memtableView{}
	published.refs.Store(1)
	db.memtables.Store(published)

	held := db.retainMemtableView()
	if held != published {
		t.Fatalf("retainMemtableView returned %p want %p", held, published)
	}
	if refs := published.refs.Load(); refs != 2 {
		t.Fatalf("published refs after retain=%d want=2", refs)
	}

	retired := memtable.NewAppendOnlyWithCapacity(0)
	retired.Set([]byte("k"), []byte("v"))
	retired.Freeze()
	if got := retired.Len(); got != 1 {
		t.Fatalf("retired memtable len=%d want=1 before publish", got)
	}

	db.mu.Lock()
	db.queueRetiredMemtableLocked(retired)
	db.publishMemtablesLocked()
	db.mu.Unlock()

	current := db.memtables.Load()
	if current == nil || current == published {
		t.Fatalf("expected newly published view, got=%p", current)
	}
	if refs := published.refs.Load(); refs != 1 {
		t.Fatalf("retained old view refs=%d want=1", refs)
	}
	if got := len(published.retiredMems); got != 1 || published.retiredMems[0] != retired {
		t.Fatalf("old view retired memtables=%v want [%p]", published.retiredMems, retired)
	}
	if got := retired.Len(); got != 1 {
		t.Fatalf("retired memtable reset too early len=%d want=1", got)
	}
	db.appendOnlyMemLeaseMu.Lock()
	leasesBefore := len(db.appendOnlyMemLeases)
	db.appendOnlyMemLeaseMu.Unlock()
	if leasesBefore != 0 {
		t.Fatalf("append-only leases before final release=%d want=0", leasesBefore)
	}

	db.releaseMemtableView(held)

	if refs := published.refs.Load(); refs != 0 {
		t.Fatalf("old view refs after final release=%d want=0", refs)
	}
	if got := retired.Len(); got != 0 {
		t.Fatalf("retired memtable len after final release=%d want=0", got)
	}
	if got := len(published.retiredMems); got != 0 {
		t.Fatalf("old view retired memtables not cleared len=%d", got)
	}
	db.appendOnlyMemLeaseMu.Lock()
	leasesAfter := len(db.appendOnlyMemLeases)
	var leased *memtable.AppendOnly
	if leasesAfter > 0 {
		leased = db.appendOnlyMemLeases[leasesAfter-1]
	}
	db.appendOnlyMemLeaseMu.Unlock()
	if leasesAfter != 1 {
		t.Fatalf("append-only leases after final release=%d want=1", leasesAfter)
	}
	if leased != retired {
		t.Fatalf("leased memtable=%p want retired=%p", leased, retired)
	}
}

func TestPutAppendOnlyMemLease_RespectsCap(t *testing.T) {
	db := &DB{}
	for i := 0; i < maxAppendOnlyMemLeases+8; i++ {
		mt := memtable.NewAppendOnlyWithCapacity(0)
		_ = db.putAppendOnlyMemLease(mt)
	}
	db.appendOnlyMemLeaseMu.Lock()
	defer db.appendOnlyMemLeaseMu.Unlock()
	if got, want := len(db.appendOnlyMemLeases), maxAppendOnlyMemLeases; got != want {
		t.Fatalf("append-only lease count=%d want=%d", got, want)
	}
}
