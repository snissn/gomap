package caching

import (
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type panicBatchSystemPublishBackend struct {
	panicBackend
	state            backenddb.DBState
	publishes        int
	values           map[string]string
	orderedPublishes int
	orderedBaseRoot  uint64
	orderedValues    map[string]string
	orderedCalls     []orderedRootPublishCall
	orderedErr       error
	groupedPublishes int
	groupedSystem    map[string]string
	groupedOrdered   []orderedRootPublishCall
	groupedErr       error
}

type orderedRootPublishCall struct {
	baseRoot uint64
	values   map[string]string
}

func (b *panicBatchSystemPublishBackend) State() *backenddb.DBState {
	state := b.state
	return &state
}

func (b *panicBatchSystemPublishBackend) PublishSystemRootIterator(iter iterator.UnsafeIterator) (uint64, error) {
	if iter != nil {
		defer iter.Close()
	}
	if b.values == nil {
		b.values = make(map[string]string)
	}
	for iter.Valid() {
		b.values[string(iter.UnsafeKey())] = string(iter.UnsafeValue())
		iter.Next()
	}
	b.publishes++
	b.state.SystemRootPageID++
	return b.state.SystemRootPageID, iter.Error()
}

func (b *panicBatchSystemPublishBackend) PublishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator) (uint64, error) {
	if iter != nil {
		defer iter.Close()
	}
	if b.orderedErr != nil {
		return 0, b.orderedErr
	}
	if b.orderedValues == nil {
		b.orderedValues = make(map[string]string)
	}
	for k := range b.orderedValues {
		delete(b.orderedValues, k)
	}
	for iter.Valid() {
		b.orderedValues[string(iter.UnsafeKey())] = string(iter.UnsafeValue())
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	b.orderedPublishes++
	b.orderedBaseRoot = baseRoot
	callValues := make(map[string]string, len(b.orderedValues))
	for k, v := range b.orderedValues {
		callValues[k] = v
	}
	b.orderedCalls = append(b.orderedCalls, orderedRootPublishCall{
		baseRoot: baseRoot,
		values:   callValues,
	})
	if b.state.RootPageID == 0 {
		b.state.RootPageID = 1
	} else {
		b.state.RootPageID++
	}
	return b.state.RootPageID, nil
}

func (b *panicBatchSystemPublishBackend) PublishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []backenddb.OrderedRootPublishInput) (uint64, []uint64, error) {
	if systemIter != nil {
		defer systemIter.Close()
	}
	for idx := range ordered {
		if ordered[idx].Iter != nil {
			defer ordered[idx].Iter.Close()
		}
	}
	if b.groupedErr != nil {
		return 0, nil, b.groupedErr
	}
	b.groupedPublishes++
	b.groupedSystem = nil
	b.groupedOrdered = b.groupedOrdered[:0]
	if systemIter != nil {
		b.groupedSystem = make(map[string]string)
		for systemIter.Valid() {
			b.groupedSystem[string(systemIter.UnsafeKey())] = string(systemIter.UnsafeValue())
			systemIter.Next()
		}
		if err := systemIter.Error(); err != nil {
			return 0, nil, err
		}
		if b.state.SystemRootPageID == 0 {
			b.state.SystemRootPageID = 1
		} else {
			b.state.SystemRootPageID++
		}
	}
	rootIDs := make([]uint64, len(ordered))
	for idx := range ordered {
		callValues := make(map[string]string)
		iter := ordered[idx].Iter
		for iter.Valid() {
			callValues[string(iter.UnsafeKey())] = string(iter.UnsafeValue())
			iter.Next()
		}
		if err := iter.Error(); err != nil {
			return 0, nil, err
		}
		b.groupedOrdered = append(b.groupedOrdered, orderedRootPublishCall{
			baseRoot: ordered[idx].BaseRoot,
			values:   callValues,
		})
		if b.state.RootPageID == 0 {
			b.state.RootPageID = 1
		} else {
			b.state.RootPageID++
		}
		rootIDs[idx] = b.state.RootPageID
	}
	return b.state.SystemRootPageID, rootIDs, nil
}

type splitSystemOrderedPublishBackend struct {
	panicBackend
	inner panicBatchSystemPublishBackend
}

func (b *splitSystemOrderedPublishBackend) State() *backenddb.DBState {
	return b.inner.State()
}

func (b *splitSystemOrderedPublishBackend) PublishSystemRootIterator(iter iterator.UnsafeIterator) (uint64, error) {
	return b.inner.PublishSystemRootIterator(iter)
}

func (b *splitSystemOrderedPublishBackend) PublishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator) (uint64, error) {
	return b.inner.PublishOrderedRootIterator(baseRoot, iter)
}

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
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "asys"}), rootID: 302,
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
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "bsys"}), rootID: 332,
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
	db.rootPublishHook = func(*rootPublishGroup) error { return errors.New("boom") }
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
	assertRootDomainVisibleValue(t, viewB.rootSystem, "sys/a", "asys")
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
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/k", value: "asys"}), rootID: 151,
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
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/k", value: "bsys"}), rootID: 152,
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
	db.rootPublishHook = func(*rootPublishGroup) error {
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
	assertRootDomainVisibleValue(t, viewAfterFail.rootSystem, "sys/k", "asys")
	assertRootDomainVisibleValue(t, viewAfterFail.rootIterator, "iter/k", "aiter")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootPointShards[0], "k", "b")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootSystem, "sys/k", "bsys")
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
	db.rootPublishHook = func(*rootPublishGroup) error { return errors.New("boom") }
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

func TestFlushCheckpointEmptyFrontierPublishesDirtyRoots(t *testing.T) {
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

	db.mu.Lock()
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
	db.rootPublishHook = func(*rootPublishGroup) error {
		hookCalls++
		return nil
	}
	db.mu.Unlock()

	db.flushMu.Lock()
	db.flushCheckpointFrontierLocked(false, nil, checkpointFrontier{captured: true})
	db.flushMu.Unlock()

	db.mu.Lock()
	dirtyPending := db.dirtyRootPublishGroupPending
	db.rootPublishHook = nil
	db.mu.Unlock()

	if hookCalls != 1 {
		t.Fatalf("hookCalls=%d want 1", hookCalls)
	}
	if dirtyPending {
		t.Fatal("expected dirty publish group to clear after empty-frontier checkpoint publish")
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
	db.rootPublishHook = func(*rootPublishGroup) error {
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
	db.rootPublishHook = func(*rootPublishGroup) error {
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

func TestPublishInstalledRootSetLocked_HookSeesOrderedRunsAsOneGroup(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 2),
		mutableShardMask: 1,
		rootPointStates: []rootDomainState{
			{mutable: newRootDomainTestTable(t, rootDomainTestOp{key: "live-0", value: "mutable-0"}), immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "p0", value: "queue-0"}),
			}},
			{mutable: newRootDomainTestTable(t, rootDomainTestOp{key: "live-1", value: "mutable-1"}), immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "p1", value: "queue-1"}),
			}},
		},
		rootIteratorState: rootDomainState{
			mutable: newRootDomainTestTable(t, rootDomainTestOp{key: "live-iter", value: "mutable-iter"}),
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "queue-iter"}),
			},
		},
		rootSystemState: rootDomainState{
			mutable: newRootDomainTestTable(t, rootDomainTestOp{key: "live-sys", value: "mutable-sys"}),
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "queue-sys"}),
			},
		},
	}
	set := &publishedRootSet{
		generation: 12,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "p0", value: "published-0"}), rootID: 121},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "p1", value: "published-1"}), rootID: 122},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "published-sys"}), rootID: 124,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "published-iter"}), rootID: 123,
		},
	}

	var captured *rootPublishGroup
	db.mu.Lock()
	db.rootPublishHook = func(group *rootPublishGroup) error {
		captured = group
		return nil
	}
	if err := db.publishInstalledRootSetLocked(set); err != nil {
		db.mu.Unlock()
		t.Fatalf("publishInstalledRootSetLocked: %v", err)
	}
	db.rootPublishHook = nil
	db.mu.Unlock()

	if captured == nil {
		t.Fatal("expected grouped publish payload")
	}
	if captured.generation != 12 {
		t.Fatalf("group generation=%d want 12", captured.generation)
	}
	if len(captured.pointShards) != 2 {
		t.Fatalf("point shard count=%d want 2", len(captured.pointShards))
	}
	if captured.pointShards[0].mutable != nil || captured.pointShards[1].mutable != nil || captured.system.mutable != nil || captured.iterator.mutable != nil {
		t.Fatal("expected grouped publish ordered runs to exclude live mutable state")
	}
	assertRootDomainVisibleValue(t, captured.pointShards[0], "p0", "queue-0")
	assertRootDomainVisibleValue(t, captured.pointShards[1], "p1", "queue-1")
	assertRootDomainVisibleValue(t, captured.system, "sys/a", "queue-sys")
	assertRootDomainVisibleValue(t, captured.iterator, "iter/a", "queue-iter")
	if captured.published == nil || captured.published.generation != 12 || captured.published.system.rootID != 124 {
		t.Fatalf("captured published metadata=%v", captured.published)
	}
}

func TestPublishInstalledRootSetLocked_HookClonesOrderedRunSlices(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{
			{immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "queue-0"}),
			}},
		},
		rootIteratorState: rootDomainState{
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "iter-0"}),
			},
		},
		rootSystemState: rootDomainState{
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "sys-0"}),
			},
		},
	}

	var captured *rootPublishGroup
	db.mu.Lock()
	db.rootPublishHook = func(group *rootPublishGroup) error {
		captured = group
		return nil
	}
	if err := db.publishInstalledRootSetLocked(&publishedRootSet{
		generation: 13,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "published"}), rootID: 131},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "published-sys"}), rootID: 133,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "published-iter"}), rootID: 132,
		},
	}); err != nil {
		db.mu.Unlock()
		t.Fatalf("publishInstalledRootSetLocked: %v", err)
	}
	db.rootPointStates[0].immutables = append(db.rootPointStates[0].immutables,
		newRootDomainTestTable(t, rootDomainTestOp{key: "k2", value: "queue-1"}))
	db.rootIteratorState.immutables = append(db.rootIteratorState.immutables,
		newRootDomainTestTable(t, rootDomainTestOp{key: "iter/b", value: "iter-1"}))
	db.rootSystemState.immutables = append(db.rootSystemState.immutables,
		newRootDomainTestTable(t, rootDomainTestOp{key: "sys/b", value: "sys-1"}))
	db.rootPublishHook = nil
	db.mu.Unlock()

	if captured == nil {
		t.Fatal("expected grouped publish payload")
	}
	if got := len(captured.pointShards[0].immutables); got != 1 {
		t.Fatalf("captured point immutables=%d want 1", got)
	}
	if got := len(captured.iterator.immutables); got != 1 {
		t.Fatalf("captured iterator immutables=%d want 1", got)
	}
	if got := len(captured.system.immutables); got != 1 {
		t.Fatalf("captured system immutables=%d want 1", got)
	}
	assertRootDomainVisibleValue(t, captured.pointShards[0], "k", "queue-0")
	assertRootDomainVisibleValue(t, captured.system, "sys/a", "sys-0")
	assertRootDomainVisibleValue(t, captured.iterator, "iter/a", "iter-0")
}

func TestPublishInstalledRootSet_HookRunsOutsideDBMu(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	set := &publishedRootSet{
		generation: 14,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v"}), rootID: 141},
		},
	}

	hookSawUnlocked := false
	db.rootPublishHook = func(*rootPublishGroup) error {
		if db.mu.TryLock() {
			hookSawUnlocked = true
			db.mu.Unlock()
		}
		return nil
	}
	if err := db.publishInstalledRootSet(set); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}
	db.rootPublishHook = nil

	if !hookSawUnlocked {
		t.Fatal("expected publish hook to run outside db.mu")
	}
}

func TestPublishInstalledRootSet_NewerGenerationInstalledDuringHookWins(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	setA := &publishedRootSet{
		generation: 20,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "a"}), rootID: 201},
		},
	}
	setB := &publishedRootSet{
		generation: 21,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "b"}), rootID: 211},
		},
	}

	db.rootPublishHook = func(*rootPublishGroup) error {
		db.mu.Lock()
		defer db.mu.Unlock()
		if !db.installPublishedRootSetLocked(setB) {
			t.Fatal("expected newer install to succeed")
		}
		return nil
	}
	if err := db.publishInstalledRootSet(setA); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}
	db.rootPublishHook = nil

	db.mu.Lock()
	view := db.retainMemtableView()
	dirtyPending := db.dirtyRootPublishGroupPending
	dirtyID := db.dirtyRootPublishGroupID
	db.mu.Unlock()
	if view == nil {
		t.Fatal("expected published view")
	}
	defer db.releaseMemtableView(view)

	assertRootDomainVisibleValue(t, view.rootPointShards[0], "k", "b")
	if view.rootPointShards[0].publishedRootID != 211 {
		t.Fatalf("published root id=%d want 211", view.rootPointShards[0].publishedRootID)
	}
	if !dirtyPending {
		t.Fatal("expected newer dirty publish group to remain pending")
	}
	if dirtyID != 21 {
		t.Fatalf("dirty publish group id=%d want 21", dirtyID)
	}
}

func TestPublishInstalledRootSet_HookReceivesCurrentSystemRootPageID(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	var captured *rootPublishGroup
	db.rootPublishHook = func(group *rootPublishGroup) error {
		captured = group
		return nil
	}
	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 22,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "v"}), rootID: 221},
		},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}
	db.rootPublishHook = nil

	if captured == nil {
		t.Fatal("expected grouped publish payload")
	}
	if got, want := captured.systemRootPageID, backend.State().SystemRootPageID; got != want {
		t.Fatalf("systemRootPageID=%d want %d", got, want)
	}
}

func TestPublishInstalledRootSet_PublishesSystemDescriptorRunToBackend(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	sysTable := newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "sv"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootSystemState: rootDomainState{
			immutables: []memtable.Table{sysTable},
		},
	}

	before := backend.State()
	if before == nil {
		t.Fatal("expected backend state")
	}
	oldSystemRoot := before.SystemRootPageID

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 23,
		system: publishedRootRef{
			lookup: sysTable,
			rootID: oldSystemRoot,
		},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	after := backend.State()
	if after == nil {
		t.Fatal("expected backend state after publish")
	}
	if after.SystemRootPageID == oldSystemRoot {
		t.Fatalf("system root did not change: still %d", oldSystemRoot)
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected installed published root set")
	}
	if got := db.rootPublishedSet.system.rootID; got != after.SystemRootPageID {
		t.Fatalf("installed system root id=%d want %d", got, after.SystemRootPageID)
	}
	if db.rootPublishedSet.system.lookup != nil {
		t.Fatal("expected updated system root to clear stale published lookup")
	}

	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected backend snapshot")
	}
	defer snap.Close()
	entry, err := snap.GetEntryAtRoot(after.SystemRootPageID, []byte("sys/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(system): %v", err)
	}
	if got := string(entry.Value); got != "sv" {
		t.Fatalf("system value=%q want %q", got, "sv")
	}
}

func TestPublishInstalledRootSet_PublishesSystemDescriptorRunWithoutBackendBatch(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{SystemRootPageID: 77},
	}
	sysTable := newRootDomainTestTable(t, rootDomainTestOp{key: "sys/a", value: "sv"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootSystemState: rootDomainState{
			immutables: []memtable.Table{sysTable},
		},
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 24,
		system: publishedRootRef{
			lookup: sysTable,
			rootID: 77,
		},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	if backend.publishes != 1 {
		t.Fatalf("publishes=%d want 1", backend.publishes)
	}
	if got := backend.values["sys/a"]; got != "sv" {
		t.Fatalf("published system value=%q want %q", got, "sv")
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected installed published root set")
	}
	if got, want := db.rootPublishedSet.system.rootID, backend.state.SystemRootPageID; got != want {
		t.Fatalf("installed system root id=%d want %d", got, want)
	}
	if db.rootPublishedSet.system.lookup != nil {
		t.Fatal("expected updated system root to clear stale published lookup")
	}
	stats := db.rootDomainPublishStatsSnapshot()
	if stats.nativeSystemPublishes != 1 {
		t.Fatalf("nativeSystemPublishes=%d want 1", stats.nativeSystemPublishes)
	}
	if stats.batchReplayFallbacks != 0 {
		t.Fatalf("batchReplayFallbacks=%d want 0", stats.batchReplayFallbacks)
	}
}

func TestPublishInstalledRootSet_PublishesIteratorRunThroughOrderedRootPublisher(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{RootPageID: 77},
	}
	oldIter := newRootDomainTestTable(t,
		rootDomainTestOp{key: "iter/a", value: "old-a"},
		rootDomainTestOp{key: "iter/b", value: "old-b"},
	)
	targetIter := newRootDomainTestTable(t,
		rootDomainTestOp{key: "iter/a", value: "new-a"},
		rootDomainTestOp{key: "iter/b", value: "old-b"},
	)
	deltaIter := newRootDomainTestTable(t,
		rootDomainTestOp{key: "iter/a", value: "new-a"},
	)
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates:  make([]rootDomainState, 1),
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 77,
			immutables:      []memtable.Table{deltaIter},
		},
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 25,
		iterator: publishedRootRef{
			lookup: targetIter,
			rootID: 77,
		},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	if backend.orderedPublishes != 1 {
		t.Fatalf("orderedPublishes=%d want 1", backend.orderedPublishes)
	}
	if backend.orderedBaseRoot != 77 {
		t.Fatalf("orderedBaseRoot=%d want 77", backend.orderedBaseRoot)
	}
	if got := backend.orderedValues["iter/a"]; got != "new-a" {
		t.Fatalf("ordered iter/a=%q want %q", got, "new-a")
	}
	if got := backend.orderedValues["iter/b"]; got != "old-b" {
		t.Fatalf("ordered iter/b=%q want %q", got, "old-b")
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected installed published root set")
	}
	if got, want := db.rootPublishedSet.iterator.rootID, backend.state.RootPageID; got != want {
		t.Fatalf("installed iterator root id=%d want %d", got, want)
	}
}

func TestPublishInstalledRootSet_IteratorRootPublishFailureKeepsPreviousGeneration(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state:      backenddb.DBState{RootPageID: 70},
		orderedErr: errors.New("boom"),
	}
	oldIter := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-a"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates:  make([]rootDomainState, 1),
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 70,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-a"}),
			},
		},
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		iterator: publishedRootRef{
			lookup: oldIter,
			rootID: 70,
		},
	}) {
		t.Fatal("expected initial install")
	}
	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 2,
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-a"}),
			rootID: 70,
		},
	}); err == nil {
		t.Fatal("expected iterator publish failure")
	}
	if backend.orderedPublishes != 0 {
		t.Fatalf("orderedPublishes=%d want 0", backend.orderedPublishes)
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected prior published root set")
	}
	if got := db.rootPublishedSet.generation; got != 1 {
		t.Fatalf("generation=%d want 1", got)
	}
	if got := db.rootPublishedSet.iterator.rootID; got != 70 {
		t.Fatalf("iterator root id=%d want 70", got)
	}
	if !db.rootPublishRetryPending {
		t.Fatal("expected retry pending after iterator publish failure")
	}
}

func TestPublishInstalledRootSet_WarmGroupedNonSystemRoots_PublishesDirtyRoots(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{RootPageID: 90},
	}
	oldPoint := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldIter := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-i"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPoint,
			publishedRootID: 90,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
			},
		}},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 91,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}),
			},
		},
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPoint, rootID: 90},
		},
		iterator: publishedRootRef{lookup: oldIter, rootID: 91},
	}) {
		t.Fatal("expected initial install")
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: 90},
		},
		iterator: publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}), rootID: 91},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	if backend.orderedPublishes != 2 {
		t.Fatalf("orderedPublishes=%d want 2", backend.orderedPublishes)
	}
	if len(backend.orderedCalls) != 2 {
		t.Fatalf("orderedCalls=%d want 2", len(backend.orderedCalls))
	}
	if got := backend.orderedCalls[0].values["primary/doc"]; got != "new-p" {
		t.Fatalf("first ordered publish value=%q want new-p", got)
	}
	if got := backend.orderedCalls[1].values["iter/a"]; got != "new-i" {
		t.Fatalf("second ordered publish value=%q want new-i", got)
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected installed published root set")
	}
	if got := db.rootPublishedSet.generation; got != 2 {
		t.Fatalf("generation=%d want 2", got)
	}
	if db.rootPublishedSet.pointShards[0].rootID == 90 {
		t.Fatal("expected point shard root id to advance")
	}
	if db.rootPublishedSet.iterator.rootID == 91 {
		t.Fatal("expected iterator root id to advance")
	}
}

func TestPublishInstalledRootSet_SystemThenNonSystemWithoutGroupedPublisher_PublishesBoth(t *testing.T) {
	backend := &splitSystemOrderedPublishBackend{
		inner: panicBatchSystemPublishBackend{
			state: backenddb.DBState{
				SystemRootPageID: 80,
				RootPageID:       90,
			},
		},
	}
	oldPoint := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldIter := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/doc", value: "old-i"})
	oldSystem := newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "old-s"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPoint,
			publishedRootID: 90,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
			},
		}},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 91,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/doc", value: "new-i"}),
			},
		},
		rootSystemState: rootDomainState{
			published:       oldSystem,
			publishedRootID: 80,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}),
			},
		},
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPoint, rootID: 90},
		},
		system:   publishedRootRef{lookup: oldSystem, rootID: 80},
		iterator: publishedRootRef{lookup: oldIter, rootID: 91},
	}) {
		t.Fatal("expected initial install")
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: 90},
		},
		system:   publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}), rootID: 80},
		iterator: publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/doc", value: "new-i"}), rootID: 91},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	if backend.inner.publishes != 1 {
		t.Fatalf("system publishes=%d want 1", backend.inner.publishes)
	}
	if backend.inner.orderedPublishes != 2 {
		t.Fatalf("ordered publishes=%d want 2", backend.inner.orderedPublishes)
	}
	if got := backend.inner.values["sys/catalog"]; got != "new-s" {
		t.Fatalf("system value=%q want new-s", got)
	}
	if got := backend.inner.orderedCalls[0].values["primary/doc"]; got != "new-p" {
		t.Fatalf("point value=%q want new-p", got)
	}
	if got := backend.inner.orderedCalls[1].values["iter/doc"]; got != "new-i" {
		t.Fatalf("iterator value=%q want new-i", got)
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected installed published root set")
	}
	if got := db.rootPublishedSet.generation; got != 2 {
		t.Fatalf("generation=%d want 2", got)
	}
	if db.rootPublishedSet.system.rootID == 80 {
		t.Fatal("expected system root id to advance")
	}
	if db.rootPublishedSet.pointShards[0].rootID == 90 {
		t.Fatal("expected point root id to advance")
	}
	if db.rootPublishedSet.iterator.rootID == 91 {
		t.Fatal("expected iterator root id to advance")
	}
}

func TestPublishInstalledRootSet_WarmGroupedNonSystemRoots_FailureKeepsPreviousGeneration(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state:      backenddb.DBState{RootPageID: 90},
		orderedErr: errors.New("boom"),
	}
	oldPoint := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldIter := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-i"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPoint,
			publishedRootID: 90,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
			},
		}},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 91,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}),
			},
		},
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPoint, rootID: 90},
		},
		iterator: publishedRootRef{lookup: oldIter, rootID: 91},
	}) {
		t.Fatal("expected initial install")
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: 90},
		},
		iterator: publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}), rootID: 91},
	}); err == nil {
		t.Fatal("expected grouped non-system publish failure")
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected prior published root set")
	}
	if got := db.rootPublishedSet.generation; got != 1 {
		t.Fatalf("generation=%d want 1", got)
	}
	if got := db.rootPublishedSet.pointShards[0].rootID; got != 90 {
		t.Fatalf("point root id=%d want 90", got)
	}
	if got := db.rootPublishedSet.iterator.rootID; got != 91 {
		t.Fatalf("iterator root id=%d want 91", got)
	}
	if !db.rootPublishRetryPending {
		t.Fatal("expected retry pending")
	}
}

func TestPublishInstalledRootSet_WarmMixedSystemAndNonSystemRoots_PublishesAtomically(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{
			SystemRootPageID: 80,
			RootPageID:       90,
		},
	}
	oldPoint := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldIter := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-i"})
	oldSystem := newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "old-s"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPoint,
			publishedRootID: 90,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
			},
		}},
		rootSystemState: rootDomainState{
			published:       oldSystem,
			publishedRootID: 80,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}),
			},
		},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 91,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}),
			},
		},
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPoint, rootID: 90},
		},
		system:   publishedRootRef{lookup: oldSystem, rootID: 80},
		iterator: publishedRootRef{lookup: oldIter, rootID: 91},
	}) {
		t.Fatal("expected initial install")
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: 90},
		},
		system:   publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}), rootID: 80},
		iterator: publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}), rootID: 91},
	}); err != nil {
		t.Fatalf("publishInstalledRootSet: %v", err)
	}

	if backend.groupedPublishes != 1 {
		t.Fatalf("groupedPublishes=%d want 1", backend.groupedPublishes)
	}
	if backend.publishes != 0 {
		t.Fatalf("publishes=%d want 0", backend.publishes)
	}
	if backend.orderedPublishes != 0 {
		t.Fatalf("orderedPublishes=%d want 0", backend.orderedPublishes)
	}
	if got := backend.groupedSystem["sys/catalog"]; got != "new-s" {
		t.Fatalf("grouped system value=%q want new-s", got)
	}
	if len(backend.groupedOrdered) != 2 {
		t.Fatalf("groupedOrdered=%d want 2", len(backend.groupedOrdered))
	}
	if got := backend.groupedOrdered[0].values["primary/doc"]; got != "new-p" {
		t.Fatalf("point grouped value=%q want new-p", got)
	}
	if got := backend.groupedOrdered[1].values["iter/a"]; got != "new-i" {
		t.Fatalf("iterator grouped value=%q want new-i", got)
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected installed published root set")
	}
	if got := db.rootPublishedSet.generation; got != 2 {
		t.Fatalf("generation=%d want 2", got)
	}
	if db.rootPublishedSet.system.rootID == 80 {
		t.Fatal("expected system root id to advance")
	}
	if db.rootPublishedSet.pointShards[0].rootID == 90 {
		t.Fatal("expected point root id to advance")
	}
	if db.rootPublishedSet.iterator.rootID == 91 {
		t.Fatal("expected iterator root id to advance")
	}
}

func TestPublishInstalledRootSet_WarmMixedSystemAndNonSystemRoots_FailureKeepsPreviousGeneration(t *testing.T) {
	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{
			SystemRootPageID: 80,
			RootPageID:       90,
		},
		groupedErr: errors.New("boom"),
	}
	oldPoint := newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "old-p"})
	oldIter := newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "old-i"})
	oldSystem := newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "old-s"})
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPoint,
			publishedRootID: 90,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}),
			},
		}},
		rootSystemState: rootDomainState{
			published:       oldSystem,
			publishedRootID: 80,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}),
			},
		},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 91,
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}),
			},
		},
	}
	if !db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{lookup: oldPoint, rootID: 90},
		},
		system:   publishedRootRef{lookup: oldSystem, rootID: 80},
		iterator: publishedRootRef{lookup: oldIter, rootID: 91},
	}) {
		t.Fatal("expected initial install")
	}

	if err := db.publishInstalledRootSet(&publishedRootSet{
		generation: 2,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "new-p"}), rootID: 90},
		},
		system:   publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "new-s"}), rootID: 80},
		iterator: publishedRootRef{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "new-i"}), rootID: 91},
	}); err == nil {
		t.Fatal("expected grouped mixed publish failure")
	}
	if backend.groupedPublishes != 0 {
		t.Fatalf("groupedPublishes=%d want 0", backend.groupedPublishes)
	}
	if db.rootPublishedSet == nil {
		t.Fatal("expected prior published root set")
	}
	if got := db.rootPublishedSet.generation; got != 1 {
		t.Fatalf("generation=%d want 1", got)
	}
	if got := db.rootPublishedSet.system.rootID; got != 80 {
		t.Fatalf("system root id=%d want 80", got)
	}
	if got := db.rootPublishedSet.pointShards[0].rootID; got != 90 {
		t.Fatalf("point root id=%d want 90", got)
	}
	if got := db.rootPublishedSet.iterator.rootID; got != 91 {
		t.Fatalf("iterator root id=%d want 91", got)
	}
	if !db.rootPublishRetryPending {
		t.Fatal("expected retry pending")
	}
}

func TestPublishInstalledRootSet_RetriesWholeGroupedPrimaryIndexStateSecondaryAndSystem(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 3),
		mutableShardMask: 3,
	}

	setA := &publishedRootSet{
		generation: 30,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "p-old"}), rootID: 301},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "index-state/doc", value: "s-old"}), rootID: 302},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "secondary/email", value: "i-old"}), rootID: 303},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "c-old"}), rootID: 304,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/doc", value: "it-old"}), rootID: 305,
		},
	}
	setB := &publishedRootSet{
		generation: 31,
		pointShards: []publishedRootRef{
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "primary/doc", value: "p-new"}), rootID: 311},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "index-state/doc", value: "s-new"}), rootID: 312},
			{lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "secondary/email", value: "i-new"}), rootID: 313},
		},
		system: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "sys/catalog", value: "c-new"}), rootID: 314,
		},
		iterator: publishedRootRef{
			lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/doc", value: "it-new"}), rootID: 315,
		},
	}

	db.mu.Lock()
	if !db.installPublishedRootSetLocked(setA) {
		db.mu.Unlock()
		t.Fatal("expected initial grouped install to succeed")
	}
	db.rootPublishHook = func(*rootPublishGroup) error { return errors.New("boom") }
	err := db.publishInstalledRootSetLocked(setB)
	viewAfterFail := db.retainMemtableView()
	db.rootPublishHook = nil
	db.mu.Unlock()
	if err == nil {
		t.Fatal("expected grouped publish failure")
	}
	if viewAfterFail == nil {
		t.Fatal("expected retained view after failed grouped publish")
	}
	defer db.releaseMemtableView(viewAfterFail)

	assertRootDomainVisibleValue(t, viewAfterFail.rootPointShards[0], "primary/doc", "p-old")
	assertRootDomainVisibleValue(t, viewAfterFail.rootPointShards[1], "index-state/doc", "s-old")
	assertRootDomainVisibleValue(t, viewAfterFail.rootPointShards[2], "secondary/email", "i-old")
	assertRootDomainVisibleValue(t, viewAfterFail.rootSystem, "sys/catalog", "c-old")
	assertRootDomainVisibleValue(t, viewAfterFail.rootIterator, "iter/doc", "it-old")

	db.mu.Lock()
	if err := db.publishInstalledRootSetLocked(setB); err != nil {
		db.mu.Unlock()
		t.Fatalf("retry grouped publish: %v", err)
	}
	viewAfterRetry := db.retainMemtableView()
	stats := db.rootDomainPublishStatsSnapshot()
	db.mu.Unlock()
	if viewAfterRetry == nil {
		t.Fatal("expected retained view after retry")
	}
	defer db.releaseMemtableView(viewAfterRetry)

	assertRootDomainVisibleValue(t, viewAfterRetry.rootPointShards[0], "primary/doc", "p-new")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootPointShards[1], "index-state/doc", "s-new")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootPointShards[2], "secondary/email", "i-new")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootSystem, "sys/catalog", "c-new")
	assertRootDomainVisibleValue(t, viewAfterRetry.rootIterator, "iter/doc", "it-new")
	if stats.retrySuccesses != 1 {
		t.Fatalf("retrySuccesses=%d want 1", stats.retrySuccesses)
	}
}

func BenchmarkPublishInstalledRootSet_GroupedSystemRootPublish(b *testing.B) {
	primaryTable := newRootDomainBenchTable(b, rootDomainTestOp{key: "primary/doc", value: "p"})
	indexStateTable := newRootDomainBenchTable(b, rootDomainTestOp{key: "index-state/doc", value: "s"})
	secondaryTable := newRootDomainBenchTable(b, rootDomainTestOp{key: "secondary/email", value: "i"})
	iterTable := newRootDomainBenchTable(b, rootDomainTestOp{key: "iter/doc", value: "it"})
	systemTable := newRootDomainBenchTable(b, rootDomainTestOp{key: "sys/catalog", value: "c"})

	template := publishedRootSet{
		pointShards: []publishedRootRef{
			{lookup: primaryTable, rootID: 101},
			{lookup: indexStateTable, rootID: 102},
			{lookup: secondaryTable, rootID: 103},
		},
		system: publishedRootRef{
			lookup: systemTable,
			rootID: 104,
		},
		iterator: publishedRootRef{
			lookup: iterTable,
			rootID: 105,
		},
	}
	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{SystemRootPageID: 1000},
	}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 3),
		mutableShardMask: 3,
		rootSystemState: rootDomainState{
			immutables: []memtable.Table{systemTable},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set := template
		set.generation = uint64(i + 1)
		if err := db.publishInstalledRootSet(&set); err != nil {
			b.Fatalf("publishInstalledRootSet: %v", err)
		}
		if backend.publishes != i+1 {
			b.Fatalf("publishes=%d want %d", backend.publishes, i+1)
		}
	}
	stats := db.rootDomainPublishStatsSnapshot()
	if stats.batchReplayFallbacks != 0 {
		b.Fatalf("batchReplayFallbacks=%d want 0", stats.batchReplayFallbacks)
	}
	if stats.nativeSystemPublishes != uint64(b.N) {
		b.Fatalf("nativeSystemPublishes=%d want %d", stats.nativeSystemPublishes, b.N)
	}
	b.ReportMetric(float64(stats.nativeSystemPublishes), "native_system_publishes")
}

func BenchmarkPublishInstalledRootSet_GroupedNonSystemOrderedPublish(b *testing.B) {
	oldPrimary := newRootDomainBenchTable(b, rootDomainTestOp{key: "primary/doc", value: "p-old"})
	oldIter := newRootDomainBenchTable(b, rootDomainTestOp{key: "iter/doc", value: "it-old"})
	newPrimary := newRootDomainBenchTable(b, rootDomainTestOp{key: "primary/doc", value: "p-new"})
	newIter := newRootDomainBenchTable(b, rootDomainTestOp{key: "iter/doc", value: "it-new"})

	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{RootPageID: 1000},
	}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPrimary,
			publishedRootID: 1001,
			immutables:      []memtable.Table{newPrimary},
		}},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 1002,
			immutables:      []memtable.Table{newIter},
		},
	}

	template := publishedRootSet{
		pointShards: []publishedRootRef{
			{lookup: newPrimary, rootID: 1001},
		},
		iterator: publishedRootRef{
			lookup: newIter,
			rootID: 1002,
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set := template
		set.generation = uint64(i + 1)
		if err := db.publishInstalledRootSet(&set); err != nil {
			b.Fatalf("publishInstalledRootSet: %v", err)
		}
		if backend.orderedPublishes != (i+1)*2 {
			b.Fatalf("orderedPublishes=%d want %d", backend.orderedPublishes, (i+1)*2)
		}
	}
	b.ReportMetric(float64(backend.orderedPublishes), "native_ordered_publishes")
}

func BenchmarkPublishInstalledRootSet_GroupedMixedOrderedPublish(b *testing.B) {
	oldPrimary := newRootDomainBenchTable(b, rootDomainTestOp{key: "primary/doc", value: "p-old"})
	oldIter := newRootDomainBenchTable(b, rootDomainTestOp{key: "iter/doc", value: "it-old"})
	oldSystem := newRootDomainBenchTable(b, rootDomainTestOp{key: "sys/catalog", value: "c-old"})
	newPrimary := newRootDomainBenchTable(b, rootDomainTestOp{key: "primary/doc", value: "p-new"})
	newIter := newRootDomainBenchTable(b, rootDomainTestOp{key: "iter/doc", value: "it-new"})
	newSystem := newRootDomainBenchTable(b, rootDomainTestOp{key: "sys/catalog", value: "c-new"})

	backend := &panicBatchSystemPublishBackend{
		state: backenddb.DBState{
			SystemRootPageID: 1000,
			RootPageID:       2000,
		},
	}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		rootPointStates: []rootDomainState{{
			published:       oldPrimary,
			publishedRootID: 2001,
			immutables:      []memtable.Table{newPrimary},
		}},
		rootSystemState: rootDomainState{
			published:       oldSystem,
			publishedRootID: 1001,
			immutables:      []memtable.Table{newSystem},
		},
		rootIteratorState: rootDomainState{
			published:       oldIter,
			publishedRootID: 2002,
			immutables:      []memtable.Table{newIter},
		},
	}

	template := publishedRootSet{
		pointShards: []publishedRootRef{
			{lookup: newPrimary, rootID: 2001},
		},
		system: publishedRootRef{
			lookup: newSystem,
			rootID: 1001,
		},
		iterator: publishedRootRef{
			lookup: newIter,
			rootID: 2002,
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set := template
		set.generation = uint64(i + 1)
		if err := db.publishInstalledRootSet(&set); err != nil {
			b.Fatalf("publishInstalledRootSet: %v", err)
		}
		if backend.groupedPublishes != i+1 {
			b.Fatalf("groupedPublishes=%d want %d", backend.groupedPublishes, i+1)
		}
	}
	stats := db.rootDomainPublishStatsSnapshot()
	if stats.batchReplayFallbacks != 0 {
		b.Fatalf("batchReplayFallbacks=%d want 0", stats.batchReplayFallbacks)
	}
	if stats.nativeSystemPublishes != uint64(b.N) {
		b.Fatalf("nativeSystemPublishes=%d want %d", stats.nativeSystemPublishes, b.N)
	}
	b.ReportMetric(float64(backend.groupedPublishes), "native_grouped_publishes")
}

func newRootDomainBenchTable(b *testing.B, ops ...rootDomainTestOp) memtable.Table {
	b.Helper()
	table, err := newRootDomainTable(ops...)
	if err != nil {
		b.Fatalf("newRootDomainTable: %v", err)
	}
	return table
}
