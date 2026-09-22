package caching

import (
	"testing"
	"time"
)

func TestMemtableViewDeferredEnter_SkipsReleasedView(t *testing.T) {
	var db DB
	view := &memtableView{}
	view.refs.Store(0)

	db.noteMemtableViewDeferredEnter(view, 2, 256)

	if got := db.memtableViewTelemetry.deferredViewsCurrent.Load(); got != 0 {
		t.Fatalf("deferred views current=%d want=0", got)
	}
	if got := db.memtableViewTelemetry.deferredMemtablesCurrent.Load(); got != 0 {
		t.Fatalf("deferred memtables current=%d want=0", got)
	}
	if got := db.memtableViewTelemetry.deferredBytesCurrent.Load(); got != 0 {
		t.Fatalf("deferred bytes current=%d want=0", got)
	}
	if got := db.memtableViewTelemetry.oldestDeferredUnixNano.Load(); got != 0 {
		t.Fatalf("oldest deferred timestamp=%d want=0", got)
	}
	db.memtableViewTelemetry.deferredMu.Lock()
	_, found := db.memtableViewTelemetry.deferred[view]
	db.memtableViewTelemetry.deferredMu.Unlock()
	if found {
		t.Fatalf("released view should not be tracked as deferred")
	}
}

func TestMemtableViewDeferredExit_RecomputesOldestTrackedView(t *testing.T) {
	var db DB
	older := &memtableView{}
	newer := &memtableView{}
	older.refs.Store(1)
	newer.refs.Store(1)

	db.noteMemtableViewDeferredEnter(older, 1, 256)
	time.Sleep(1 * time.Millisecond)
	db.noteMemtableViewDeferredEnter(newer, 3, 1024)

	if got := db.memtableViewTelemetry.deferredViewsCurrent.Load(); got != 2 {
		t.Fatalf("deferred views current=%d want=2", got)
	}
	if got := db.memtableViewTelemetry.deferredMemtablesCurrent.Load(); got != 4 {
		t.Fatalf("deferred memtables current=%d want=4", got)
	}
	if got := db.memtableViewTelemetry.deferredBytesCurrent.Load(); got != 1280 {
		t.Fatalf("deferred bytes current=%d want=1280", got)
	}

	db.noteMemtableViewDeferredExit(older)

	db.memtableViewTelemetry.deferredMu.Lock()
	newerInfo, found := db.memtableViewTelemetry.deferred[newer]
	db.memtableViewTelemetry.deferredMu.Unlock()
	if !found {
		t.Fatalf("newer deferred view missing after exiting older view")
	}
	if got := db.memtableViewTelemetry.oldestDeferredUnixNano.Load(); got != newerInfo.sinceUnixNano {
		t.Fatalf("oldest deferred timestamp=%d want=%d", got, newerInfo.sinceUnixNano)
	}
	if got := db.memtableViewTelemetry.deferredViewsCurrent.Load(); got != 1 {
		t.Fatalf("deferred views current=%d want=1", got)
	}
	if got := db.memtableViewTelemetry.deferredMemtablesCurrent.Load(); got != 3 {
		t.Fatalf("deferred memtables current=%d want=3", got)
	}
	if got := db.memtableViewTelemetry.deferredBytesCurrent.Load(); got != 1024 {
		t.Fatalf("deferred bytes current=%d want=1024", got)
	}

	db.noteMemtableViewDeferredExit(newer)
	if got := db.memtableViewTelemetry.oldestDeferredUnixNano.Load(); got != 0 {
		t.Fatalf("oldest deferred timestamp=%d want=0", got)
	}
	if got := db.memtableViewTelemetry.deferredViewsCurrent.Load(); got != 0 {
		t.Fatalf("deferred views current=%d want=0", got)
	}
	if got := db.memtableViewTelemetry.deferredMemtablesCurrent.Load(); got != 0 {
		t.Fatalf("deferred memtables current=%d want=0", got)
	}
	if got := db.memtableViewTelemetry.deferredBytesCurrent.Load(); got != 0 {
		t.Fatalf("deferred bytes current=%d want=0", got)
	}
}
