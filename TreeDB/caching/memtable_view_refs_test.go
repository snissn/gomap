package caching

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type releaseTrackingMemtable struct {
	*memtable.AppendOnly
	released bool
}

func (m *releaseTrackingMemtable) Release() {
	m.released = true
	m.AppendOnly.Release()
}

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

func TestReleaseClosingEmptyMemtablesDefersCleanupForRetainedView(t *testing.T) {
	db := &DB{}
	mutable := &releaseTrackingMemtable{AppendOnly: memtable.NewAppendOnlyWithCapacity(0)}
	retired := memtable.NewAppendOnlyWithCapacity(0)
	retired.Set([]byte("retired"), []byte("value"))
	retired.Freeze()

	view := &memtableView{
		mutables:    []memtable.Table{mutable},
		retiredMems: []memtable.Table{retired},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	held := db.retainMemtableView()
	if held != view {
		t.Fatalf("retainMemtableView returned %p want %p", held, view)
	}
	if refs := view.refs.Load(); refs != 2 {
		t.Fatalf("view refs after retain=%d want=2", refs)
	}

	db.releaseClosingEmptyMemtables()

	if got := db.memtables.Load(); got != nil {
		t.Fatalf("published view after close cleanup=%p want nil", got)
	}
	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs after closing cleanup=%d want=1", refs)
	}
	if mutable.released {
		t.Fatalf("mutable released before retained reader dropped view")
	}
	if got := len(view.retiredMems); got != 1 {
		t.Fatalf("retired memtables after closing cleanup=%d want=1", got)
	}
	// Closing-empty mems should be registered in the DB map (not on the view).
	db.closingEmptyMemsMu.Lock()
	pendingCount := len(db.closingEmptyByView[view])
	db.closingEmptyMemsMu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("pending closing-empty mems in DB map=%d want=1", pendingCount)
	}

	db.releaseMemtableView(held)

	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after final release=%d want=0", refs)
	}
	if !mutable.released {
		t.Fatalf("mutable was not released after final view release")
	}
	if got := retired.Len(); got != 0 {
		t.Fatalf("retired memtable len after final release=%d want=0", got)
	}
	if got := len(view.retiredMems); got != 0 {
		t.Fatalf("retired memtables not cleared len=%d", got)
	}
	db.closingEmptyMemsMu.Lock()
	remainingCount := len(db.closingEmptyByView[view])
	db.closingEmptyMemsMu.Unlock()
	if remainingCount != 0 {
		t.Fatalf("DB closing-empty map not cleared after final release: len=%d", remainingCount)
	}
}

func TestReleaseClosingEmptyMemtablesCleansZeroRefPublishedView(t *testing.T) {
	db := &DB{}
	mutable := &releaseTrackingMemtable{AppendOnly: memtable.NewAppendOnlyWithCapacity(0)}
	retired := memtable.NewAppendOnlyWithCapacity(0)
	retired.Set([]byte("retired"), []byte("value"))
	retired.Freeze()

	view := &memtableView{
		mutables:    []memtable.Table{mutable},
		retiredMems: []memtable.Table{retired},
	}
	db.memtables.Store(view)

	db.releaseClosingEmptyMemtables()

	if got := db.memtables.Load(); got != nil {
		t.Fatalf("published view after close cleanup=%p want nil", got)
	}
	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after closing cleanup=%d want=0", refs)
	}
	if !mutable.released {
		t.Fatalf("mutable was not released")
	}
	if got := retired.Len(); got != 0 {
		t.Fatalf("retired memtable len after cleanup=%d want=0", got)
	}
	if got := len(view.mutables); got != 0 {
		t.Fatalf("mutable memtables not cleared len=%d", got)
	}
	if got := len(view.retiredMems); got != 0 {
		t.Fatalf("retired memtables not cleared len=%d", got)
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
