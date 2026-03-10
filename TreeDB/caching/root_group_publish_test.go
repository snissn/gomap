package caching

import (
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

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
	if stats.publishFailures != 0 {
		t.Fatalf("publishFailures=%d want 0", stats.publishFailures)
	}
}

func TestPublishInstalledRootSetLocked_HookFailureKeepsPreviousGeneration(t *testing.T) {
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
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "aiter"}), rootID: 303,
		},
	}
	setB := &publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "b0"}), rootID: 111},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "b1"}), rootID: 222},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "biter"}), rootID: 333,
		},
	}

	db.mu.Lock()
	if err := db.publishInstalledRootSetLocked(setA); err != nil {
		db.mu.Unlock()
		t.Fatalf("publish setA: %v", err)
	}
	viewA := db.retainMemtableView()
	db.rootPublishHook = func(*publishedRootSet) error { return errors.New("boom") }
	err := db.publishInstalledRootSetLocked(setB)
	viewB := db.retainMemtableView()
	stats := db.rootDomainPublishStatsSnapshot()
	db.rootPublishHook = nil
	db.mu.Unlock()
	if viewA == nil || viewB == nil {
		t.Fatal("expected published views")
	}
	defer db.releaseMemtableView(viewA)
	defer db.releaseMemtableView(viewB)

	if err == nil {
		t.Fatal("expected hook failure")
	}
	if viewB.rootVersion != viewA.rootVersion {
		t.Fatalf("rootVersion changed on failed publish: got %d want %d", viewB.rootVersion, viewA.rootVersion)
	}
	assertRootDomainVisibleValue(t, viewB.rootPointShards[0], string(key0), "a0")
	assertRootDomainVisibleValue(t, viewB.rootPointShards[1], string(key1), "a1")
	assertRootDomainVisibleValue(t, viewB.rootIterator, "iter/a", "aiter")
	if stats.publishFailures != 1 {
		t.Fatalf("publishFailures=%d want 1", stats.publishFailures)
	}
}

func TestPublishInstalledRootSetLocked_RetryPublishesExactlyOnce(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	setA := &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "a"}), rootID: 101},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "aiter"}), rootID: 201,
		},
	}
	setB := &publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "b"}), rootID: 102},
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/k", value: "biter"}), rootID: 202,
		},
	}

	db.mu.Lock()
	if err := db.publishInstalledRootSetLocked(setA); err != nil {
		db.mu.Unlock()
		t.Fatalf("publish setA: %v", err)
	}
	failOnce := true
	db.rootPublishHook = func(*publishedRootSet) error {
		if failOnce {
			failOnce = false
			return errors.New("retry me")
		}
		return nil
	}
	if err := db.publishInstalledRootSetLocked(setB); err == nil {
		db.mu.Unlock()
		t.Fatal("expected first publish of setB to fail")
	}
	viewAfterFail := db.retainMemtableView()
	if err := db.publishInstalledRootSetLocked(setB); err != nil {
		db.mu.Unlock()
		t.Fatalf("retry publish setB: %v", err)
	}
	db.rootPublishHook = nil
	viewAfterRetry := db.retainMemtableView()
	stats := db.rootDomainPublishStatsSnapshot()
	db.mu.Unlock()
	if viewAfterFail == nil || viewAfterRetry == nil {
		t.Fatal("expected published views")
	}
	defer db.releaseMemtableView(viewAfterFail)
	defer db.releaseMemtableView(viewAfterRetry)

	assertRootDomainVisibleValue(t, viewAfterFail.rootPointShards[0], "k", "a")
	assertRootDomainVisibleValue(t, viewAfterFail.rootIterator, "iter/k", "aiter")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootPointShards[0], "k", "b")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootIterator, "iter/k", "biter")
	if stats.publishFailures != 1 {
		t.Fatalf("publishFailures=%d want 1", stats.publishFailures)
	}
	if stats.retrySuccesses != 1 {
		t.Fatalf("retrySuccesses=%d want 1", stats.retrySuccesses)
	}
}

func TestInstallPublishedRootSetLocked_MarksDirtyPublishGroup(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	db.mu.Lock()
	ok := db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 9,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v"}), rootID: 91},
		},
	})
	dirtyPending := db.dirtyRootPublishGroupPending
	dirtyID := db.dirtyRootPublishGroupID
	db.mu.Unlock()

	if !ok {
		t.Fatal("expected install to succeed")
	}
	if !dirtyPending {
		t.Fatal("expected dirty publish group to be pending")
	}
	if dirtyID != 9 {
		t.Fatalf("dirty publish group id=%d want 9", dirtyID)
	}
}

func TestPublishInstalledRootSetLocked_SuccessClearsDirtyPublishGroup(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	set := &publishedRootSet{
		generation: 5,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v"}), rootID: 51},
		},
	}

	db.mu.Lock()
	if !db.installPublishedRootSetLocked(set) {
		db.mu.Unlock()
		t.Fatal("expected install to succeed")
	}
	if err := db.publishInstalledRootSetLocked(set); err != nil {
		db.mu.Unlock()
		t.Fatalf("publishInstalledRootSetLocked: %v", err)
	}
	dirtyPending := db.dirtyRootPublishGroupPending
	dirtyID := db.dirtyRootPublishGroupID
	db.mu.Unlock()

	if dirtyPending {
		t.Fatal("expected dirty publish group to clear")
	}
	if dirtyID != 0 {
		t.Fatalf("dirty publish group id=%d want 0", dirtyID)
	}
}

func TestPublishInstalledRootSetLocked_FailureLeavesDirtyPublishGroupPending(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	set := &publishedRootSet{
		generation: 6,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v"}), rootID: 61},
		},
	}

	db.mu.Lock()
	if !db.installPublishedRootSetLocked(set) {
		db.mu.Unlock()
		t.Fatal("expected install to succeed")
	}
	db.rootPublishHook = func(*publishedRootSet) error { return errors.New("boom") }
	err := db.publishInstalledRootSetLocked(set)
	dirtyPending := db.dirtyRootPublishGroupPending
	dirtyID := db.dirtyRootPublishGroupID
	db.rootPublishHook = nil
	db.mu.Unlock()

	if err == nil {
		t.Fatal("expected publish to fail")
	}
	if !dirtyPending {
		t.Fatal("expected dirty publish group to remain pending")
	}
	if dirtyID != 6 {
		t.Fatalf("dirty publish group id=%d want 6", dirtyID)
	}
}

func TestInstallPublishedRootSetLocked_NewerGenerationReplacesDirtyPublishGroup(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	db.mu.Lock()
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 7,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "a"}), rootID: 71},
		},
	}) {
		db.mu.Unlock()
		t.Fatal("expected first install to succeed")
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 8,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "b"}), rootID: 81},
		},
	}) {
		db.mu.Unlock()
		t.Fatal("expected second install to succeed")
	}
	dirtyPending := db.dirtyRootPublishGroupPending
	dirtyID := db.dirtyRootPublishGroupID
	db.mu.Unlock()

	if !dirtyPending {
		t.Fatal("expected dirty publish group to remain pending")
	}
	if dirtyID != 8 {
		t.Fatalf("dirty publish group id=%d want 8", dirtyID)
	}
}

func TestFlushSome_PrefersDirtyRootPublishGroupBeforeLaneWork(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		JournalLanes:             1,
		MemtableShards:           1,
		ValueLogPointerThreshold: 1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
		AllowUnsafe:              true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 10,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "p", value: "v"}), rootID: 101},
		},
	}) {
		db.mu.Unlock()
		t.Fatal("expected install to succeed")
	}
	hookCalls := 0
	db.rootPublishHook = func(*publishedRootSet) error {
		hookCalls++
		return nil
	}
	db.mu.Unlock()

	flushed := db.flushSome(false, 1, time.Second)

	db.mu.Lock()
	queueLen := len(db.queue)
	dirtyPending := db.dirtyRootPublishGroupPending
	db.rootPublishHook = nil
	db.mu.Unlock()

	if flushed != 1 {
		t.Fatalf("flushSome flushed=%d want 1", flushed)
	}
	if hookCalls != 1 {
		t.Fatalf("hookCalls=%d want 1", hookCalls)
	}
	if queueLen != 1 {
		t.Fatalf("queue len=%d want 1", queueLen)
	}
	if dirtyPending {
		t.Fatal("expected dirty publish group to clear after flushSome publish")
	}
}

func TestFlushSome_DirtyRootPublishFailureStopsBeforeLaneWork(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		JournalLanes:             1,
		MemtableShards:           1,
		ValueLogPointerThreshold: 1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
		AllowUnsafe:              true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 11,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "p", value: "v"}), rootID: 111},
		},
	}) {
		db.mu.Unlock()
		t.Fatal("expected install to succeed")
	}
	hookCalls := 0
	db.rootPublishHook = func(*publishedRootSet) error {
		hookCalls++
		return errors.New("boom")
	}
	db.mu.Unlock()

	flushed := db.flushSome(false, 1, time.Second)

	db.mu.Lock()
	queueLen := len(db.queue)
	dirtyPending := db.dirtyRootPublishGroupPending
	dirtyID := db.dirtyRootPublishGroupID
	db.rootPublishHook = nil
	db.mu.Unlock()

	if flushed != 0 {
		t.Fatalf("flushSome flushed=%d want 0", flushed)
	}
	if hookCalls != 1 {
		t.Fatalf("hookCalls=%d want 1", hookCalls)
	}
	if queueLen != 1 {
		t.Fatalf("queue len=%d want 1", queueLen)
	}
	if !dirtyPending {
		t.Fatal("expected dirty publish group to remain pending")
	}
	if dirtyID != 11 {
		t.Fatalf("dirty publish group id=%d want 11", dirtyID)
	}
}
