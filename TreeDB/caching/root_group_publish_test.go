package caching

import "testing"

func TestInstallPublishedRootSetLocked_PublishesOnePinnedGeneration(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 2),
		mutableShardMask: 1,
	}

	key0 := rootGroupTestShardKey(t, db, 0)
	key1 := rootGroupTestShardKey(t, db, 1)

	setA := &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "a0"}), rootID: 101},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "a1"}), rootID: 202},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "aiter"}),
			rootID: 303,
		},
	}
	setB := &publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "b0"}), rootID: 111},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "b1"}), rootID: 222},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "biter"}),
			rootID: 333,
		},
	}

	db.mu.Lock()
	db.installPublishedRootSetLocked(setA)
	viewA := db.retainMemtableView()
	db.mu.Unlock()
	if viewA == nil {
		t.Fatal("expected published view A")
	}
	defer db.releaseMemtableView(viewA)

	db.mu.Lock()
	db.installPublishedRootSetLocked(setB)
	viewB := db.retainMemtableView()
	db.mu.Unlock()
	if viewB == nil {
		t.Fatal("expected published view B")
	}
	defer db.releaseMemtableView(viewB)

	if viewA.rootVersion == viewB.rootVersion {
		t.Fatalf("expected distinct root versions, got %d", viewA.rootVersion)
	}

	assertRootDomainVisibleValue(t, viewA.rootPointShards[0], string(key0), "a0")
	assertRootDomainVisibleValue(t, viewA.rootPointShards[1], string(key1), "a1")
	assertRootDomainVisibleValue(t, viewA.rootIterator, "iter/a", "aiter")
	if viewA.rootPointShards[0].publishedRootID != 101 || viewA.rootPointShards[1].publishedRootID != 202 || viewA.rootIterator.publishedRootID != 303 {
		t.Fatalf("unexpected root ids in viewA: %d %d %d", viewA.rootPointShards[0].publishedRootID, viewA.rootPointShards[1].publishedRootID, viewA.rootIterator.publishedRootID)
	}

	assertRootDomainVisibleValue(t, viewB.rootPointShards[0], string(key0), "b0")
	assertRootDomainVisibleValue(t, viewB.rootPointShards[1], string(key1), "b1")
	assertRootDomainVisibleValue(t, viewB.rootIterator, "iter/a", "biter")
	if viewB.rootPointShards[0].publishedRootID != 111 || viewB.rootPointShards[1].publishedRootID != 222 || viewB.rootIterator.publishedRootID != 333 {
		t.Fatalf("unexpected root ids in viewB: %d %d %d", viewB.rootPointShards[0].publishedRootID, viewB.rootPointShards[1].publishedRootID, viewB.rootIterator.publishedRootID)
	}
}

func TestInstallPublishedRootSetLocked_NilClearsPublishedViews(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	key := []byte("k")

	db.mu.Lock()
	db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v"}), rootID: 42},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "iter-v"}), rootID: 43,
		},
	})
	db.installPublishedRootSetLocked(nil)
	view := db.retainMemtableView()
	db.mu.Unlock()
	if view == nil {
		t.Fatal("expected published view")
	}
	defer db.releaseMemtableView(view)

	if got := view.rootPointShards[0].publishedRootID; got != 0 {
		t.Fatalf("point published root id=%d want 0", got)
	}
	if got := view.rootIterator.publishedRootID; got != 0 {
		t.Fatalf("iterator published root id=%d want 0", got)
	}
	if _, ok := view.rootPointShards[0].visibleValue(key); ok {
		t.Fatal("expected cleared point published view")
	}
	if _, ok := view.rootIterator.visibleValue([]byte("iter/k")); ok {
		t.Fatal("expected cleared iterator published view")
	}
	if stats := db.rootDomainPublishStatsSnapshot(); stats.installs != 1 || stats.clears != 1 {
		t.Fatalf("unexpected publish stats after clear: %+v", stats)
	}
}

func TestInstallPublishedRootSetLocked_ClonesCallerSlices(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	key := []byte("k")

	set := &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v1"}), rootID: 41},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "iter-v1"}), rootID: 42,
		},
	}

	db.mu.Lock()
	if !db.installPublishedRootSetLocked(set) {
		db.mu.Unlock()
		t.Fatal("expected install to succeed")
	}
	view := db.retainMemtableView()
	db.mu.Unlock()
	if view == nil {
		t.Fatal("expected published view")
	}
	defer db.releaseMemtableView(view)

	set.pointShards[0] = publishedRootRef{
		lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "mutated"}), rootID: 99,
	}
	set.iterator = publishedRootRef{
		lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "iter-mutated"}), rootID: 100,
	}

	assertRootDomainVisibleValue(t, view.rootPointShards[0], string(key), "v1")
	assertRootDomainVisibleValue(t, view.rootIterator, "iter/k", "iter-v1")
	if got := view.rootPointShards[0].publishedRootID; got != 41 {
		t.Fatalf("point published root id=%d want 41", got)
	}
	if got := view.rootIterator.publishedRootID; got != 42 {
		t.Fatalf("iterator published root id=%d want 42", got)
	}
	if stats := db.rootDomainPublishStatsSnapshot(); stats.installs != 1 || stats.clears != 0 || stats.staleRejects != 0 {
		t.Fatalf("unexpected publish stats after clone-safe install: %+v", stats)
	}
}

func TestInstallPublishedRootSetLocked_RejectsStaleGeneration(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	db.mu.Lock()
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 7,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "fresh"}), rootID: 70},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "fresh-iter"}), rootID: 71,
		},
	}) {
		db.mu.Unlock()
		t.Fatal("expected initial install to succeed")
	}
	viewA := db.retainMemtableView()
	staleRejected := !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 6,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "stale"}), rootID: 60},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "stale-iter"}), rootID: 61,
		},
	})
	viewB := db.retainMemtableView()
	stats := db.rootDomainPublishStatsSnapshot()
	db.mu.Unlock()
	if viewA == nil || viewB == nil {
		t.Fatal("expected published views")
	}
	defer db.releaseMemtableView(viewA)
	defer db.releaseMemtableView(viewB)

	if !staleRejected {
		t.Fatal("expected stale generation install to be rejected")
	}
	assertRootDomainVisibleValue(t, viewB.rootPointShards[0], "k", "fresh")
	assertRootDomainVisibleValue(t, viewB.rootIterator, "iter/k", "fresh-iter")
	if viewB.rootVersion != viewA.rootVersion {
		t.Fatalf("expected stale reject not to republish view: got %d want %d", viewB.rootVersion, viewA.rootVersion)
	}
	if stats.staleRejects != 1 {
		t.Fatalf("staleRejects=%d want 1", stats.staleRejects)
	}
}
