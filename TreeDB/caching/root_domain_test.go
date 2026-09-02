package caching

import (
	"reflect"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
)

type rootDomainTestOp struct {
	key       string
	value     string
	tombstone bool
}

func newRootDomainTestTable(t *testing.T, ops ...rootDomainTestOp) memtable.Table {
	t.Helper()

	mt, err := newRootDomainTable(ops...)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	return mt
}

func newRootDomainTable(ops ...rootDomainTestOp) (memtable.Table, error) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
	for _, op := range ops {
		if op.tombstone {
			mt.Delete([]byte(op.key))
			continue
		}
		mt.Set([]byte(op.key), []byte(op.value))
	}
	mt.Freeze()
	return mt, nil
}

func TestRootDomainSnapshot_GetEntryNewestWinsAcrossRuns(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		publishedRootID: 11,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "a", value: "published-a"},
			rootDomainTestOp{key: "b", value: "published-b"},
			rootDomainTestOp{key: "c", value: "published-c"},
		),
		immutables: []memtable.Table{
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "b", value: "immutable-b"},
				rootDomainTestOp{key: "c", tombstone: true},
			),
		},
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "c", value: "mutable-c"},
			rootDomainTestOp{key: "d", value: "mutable-d"},
		),
	}

	assertRootDomainVisibleValue(t, snap, "a", "published-a")
	assertRootDomainVisibleValue(t, snap, "b", "immutable-b")
	assertRootDomainVisibleValue(t, snap, "c", "mutable-c")
	assertRootDomainVisibleValue(t, snap, "d", "mutable-d")
}

func TestRootDomainSnapshot_GetEntryNewestImmutableWins(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "k", value: "published"},
		),
		immutables: []memtable.Table{
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "k", value: "older"},
			),
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "k", value: "newer"},
			),
		},
	}

	assertRootDomainVisibleValue(t, snap, "k", "newer")
}

func TestRootDomainSnapshot_TombstoneHidesOlderRuns(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		publishedRootID: 22,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "x", value: "published-x"},
		),
		immutables: []memtable.Table{
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "x", value: "immutable-x"},
			),
		},
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "x", tombstone: true},
		),
	}

	if _, _, flags, found := snap.getEntry([]byte("x")); !found {
		t.Fatal("expected tombstoned key to resolve as found")
	} else if flags&node.FlagTombstone == 0 {
		t.Fatalf("flags=%08b want tombstone", flags)
	}
	if snap.hasVisibleKey([]byte("x")) {
		t.Fatal("expected tombstoned key to be invisible")
	}
}

func TestRootDomainUnsafeIteratorSeekPreservesSourcePriority(t *testing.T) {
	t.Parallel()

	older := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "older"}).NewIterator(nil, nil)
	newer := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "newer"}).NewIterator(nil, nil)
	it := newRootDomainUnsafeIterator([]merging.IteratorSource{
		{Iter: older, Priority: 1},
		{Iter: newer, Priority: 0},
	}, nil, nil)
	defer it.Close()

	it.Seek([]byte("k"))
	if !it.Valid() {
		t.Fatal("expected iterator hit after seek")
	}
	if got, want := string(it.UnsafeValue()), "newer"; got != want {
		t.Fatalf("seek value=%q want %q", got, want)
	}
}

func TestRootDomainState_SealMutablePreservesVisibility(t *testing.T) {
	t.Parallel()

	state := &rootDomainState{
		publishedRootID: 33,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "a", value: "published-a"},
		),
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "a", value: "mutable-a"},
			rootDomainTestOp{key: "b", value: "mutable-b"},
		),
	}

	before := state.snapshot()
	assertRootDomainVisibleValue(t, before, "a", "mutable-a")
	assertRootDomainVisibleValue(t, before, "b", "mutable-b")

	nextMutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	state.sealMutable(nextMutable)
	after := state.snapshot()

	assertRootDomainVisibleValue(t, after, "a", "mutable-a")
	assertRootDomainVisibleValue(t, after, "b", "mutable-b")
	if got, want := len(after.immutables), 1; got != want {
		t.Fatalf("immutable runs=%d want %d", got, want)
	}
	if after.mutable == nil {
		t.Fatal("expected new mutable run after seal")
	}
}

func TestRootDomainState_SnapshotDoesNotAliasLaterImmutableReuse(t *testing.T) {
	t.Parallel()

	first := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "first"})
	second := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "second"})

	state := &rootDomainState{
		immutables: make([]memtable.Table, 1, 4),
	}
	state.immutables[0] = first

	snap := state.snapshot()
	assertRootDomainVisibleValue(t, snap, "k", "first")

	state.immutables = state.immutables[:0]
	state.immutables = append(state.immutables, second)

	assertRootDomainVisibleValue(t, snap, "k", "first")
}

func TestRootDomainSnapshot_SameHandleVisibilityIncludesMutableRun(t *testing.T) {
	t.Parallel()

	state := &rootDomainState{publishedRootID: 44}
	mutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	mutable.Set([]byte("k"), []byte("v"))
	state.mutable = mutable

	snap := state.snapshot()
	assertRootDomainVisibleValue(t, snap, "k", "v")
}

func TestRootDomainSnapshotFromMemtableView_ForKeyIncludesMutableAndMatchingQueueRuns(t *testing.T) {
	t.Parallel()

	view := &memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "wrong-shard"}),
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "a", value: "mutable-a"},
				rootDomainTestOp{key: "b", value: "mutable-b"},
			),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "queue-older"}),
			newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "ignored-other-shard"}),
			newRootDomainTestTable(t, rootDomainTestOp{key: "c", value: "queue-newer"}),
		},
		queueShardIDs: []uint16{1, 0, 1},
	}

	snap := rootDomainSnapshotFromMemtableView(view, 1, true)
	assertRootDomainVisibleValue(t, snap, "a", "mutable-a")
	assertRootDomainVisibleValue(t, snap, "b", "mutable-b")
	assertRootDomainVisibleValue(t, snap, "c", "queue-newer")
	if got, want := len(snap.immutables), 2; got != want {
		t.Fatalf("immutables=%d want %d", got, want)
	}
}

func TestRootDomainSnapshotFromMemtableView_ForSnapshotReadsExcludesMutableRun(t *testing.T) {
	t.Parallel()

	view := &memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "mutable"}),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "queue"}),
		},
		queueShardIDs: []uint16{0},
	}

	snap := rootDomainSnapshotFromMemtableView(view, 0, false)
	assertRootDomainVisibleValue(t, snap, "k", "queue")
	if snap.mutable != nil {
		t.Fatal("expected snapshot view to exclude mutable run")
	}
}

func TestRootDomainSnapshotFromMemtableView_ForIteratorReadsIncludesAllQueueRuns(t *testing.T) {
	t.Parallel()

	view := &memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "m", value: "mutable"}),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "queue-older"}),
			newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "queue-newer"}),
		},
		queueShardIDs: []uint16{0, 1},
	}

	snap := rootDomainSnapshotFromMemtableView(view, -1, false)
	if snap.mutable != nil {
		t.Fatal("iterator snapshot should not include mutable run")
	}
	if got, want := len(snap.immutables), 2; got != want {
		t.Fatalf("immutables=%d want %d", got, want)
	}
	assertRootDomainVisibleValue(t, snap, "a", "queue-older")
	assertRootDomainVisibleValue(t, snap, "b", "queue-newer")
}

func TestRootDomainSnapshotFromMemtableView_UsesPublishedShardAndIteratorSnapshots(t *testing.T) {
	t.Parallel()

	view := &memtableView{
		rootVersion: 17,
		rootPointShards: []rootDomainSnapshot{
			{
				publishedRootID: 101,
				immutables: []memtable.Table{
					newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "shard-0"}),
				},
			},
			{
				publishedRootID: 202,
				immutables: []memtable.Table{
					newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "shard-1"}),
				},
			},
		},
		rootIterator: rootDomainSnapshot{
			publishedRootID: 303,
			immutables: []memtable.Table{
				newRootDomainTestTable(t,
					rootDomainTestOp{key: "a", value: "iter-a"},
					rootDomainTestOp{key: "b", value: "iter-b"},
				),
			},
		},
	}

	point := rootDomainSnapshotFromMemtableView(view, 1, true)
	if got, want := point.publishedRootID, uint64(202); got != want {
		t.Fatalf("point publishedRootID=%d want %d", got, want)
	}
	assertRootDomainVisibleValue(t, point, "b", "shard-1")

	iter := rootDomainSnapshotFromMemtableView(view, -1, false)
	if got, want := iter.publishedRootID, uint64(303); got != want {
		t.Fatalf("iterator publishedRootID=%d want %d", got, want)
	}
	assertRootDomainVisibleValue(t, iter, "a", "iter-a")
	assertRootDomainVisibleValue(t, iter, "b", "iter-b")
}

func TestRootDomainSnapshotFromCachedSnapshot_UsesBackendFallbackLookup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	if err := backend.SetSync([]byte("k"), []byte("backend-v")); err != nil {
		t.Fatalf("set backend value: %v", err)
	}

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("expected backend snapshot")
	}
	defer backendSnap.Close()

	snap := &Snapshot{backend: backendSnap}
	rootSnap := rootDomainSnapshotFromCachedSnapshot(snap, []byte("k"))
	if rootSnap.published == nil {
		t.Fatal("expected backend fallback lookup")
	}
	if rootSnap.publishedRootID == 0 {
		t.Fatal("expected backend published root id")
	}
	assertRootDomainVisibleValue(t, rootSnap, "k", "backend-v")
}

func TestPublishRootDomainSnapshotsLocked_FiltersIteratorRangesWithNilQueueEntries(t *testing.T) {
	t.Parallel()

	first := newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "a"})
	second := newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "b"})
	db := &DB{
		mutableShards: []memShard{{}},
		queue:         []memtable.Table{first, nil, second},
		queueRanges: []keyRange{
			{valid: true, min: []byte("a"), max: []byte("a")},
			{valid: true, min: []byte("nil"), max: []byte("nil")},
			{valid: true, min: []byte("b"), max: []byte("b")},
		},
		rootPointStates: []rootDomainState{{}},
		rootIteratorState: rootDomainState{
			immutables: []memtable.Table{first, second},
		},
	}
	view := &memtableView{queue: db.queue, queueRanges: db.queueRanges}

	db.publishRootDomainSnapshotsLocked(view)
	if got, want := len(view.rootIterator.immutables), 2; got != want {
		t.Fatalf("iterator runs=%d want %d", got, want)
	}
	if got, want := len(view.rootIteratorRanges), 2; got != want {
		t.Fatalf("iterator ranges=%d want %d", got, want)
	}
	if string(view.rootIteratorRanges[0].min) != "a" || string(view.rootIteratorRanges[1].min) != "b" {
		t.Fatalf("unexpected filtered ranges: %+v", view.rootIteratorRanges)
	}
}

func TestRawIteratorRootDomainSnapshot_CopiesQueuedRunsAndRanges(t *testing.T) {
	t.Parallel()

	db := &DB{
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "va"}),
			newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "vb"}),
		},
		queueRanges: []keyRange{
			{valid: true, min: []byte("a"), max: []byte("a")},
			{valid: true, min: []byte("b"), max: []byte("b")},
		},
	}

	snap, ranges := db.rawIteratorRootDomainSnapshot()
	if got, want := len(snap.immutables), 2; got != want {
		t.Fatalf("immutables=%d want %d", got, want)
	}
	if !reflect.DeepEqual(ranges, db.queueRanges) {
		t.Fatalf("ranges=%v want %v", ranges, db.queueRanges)
	}

	ranges[0] = keyRange{}
	if !db.queueRanges[0].valid {
		t.Fatal("expected raw iterator ranges to be copied")
	}
}

func TestRawPointRootDomainEntry_NewestWinsAcrossMutableAndQueue(t *testing.T) {
	t.Parallel()

	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	db.mutableShards[0].mem = newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "mutable"})
	db.queue = []memtable.Table{
		newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "older"}),
		newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "newer"}),
	}
	db.queueShardIDs = []uint16{0, 0}

	val, _, flags, found := db.rawPointRootDomainEntry([]byte("k"))
	if !found {
		t.Fatal("expected raw point entry hit")
	}
	if flags != node.FlagInline {
		t.Fatalf("flags=%08b want inline", flags)
	}
	if string(val) != "mutable" {
		t.Fatalf("value=%q want %q", string(val), "mutable")
	}
}

func TestRawPointRootDomainEntry_TombstoneHidesOlderQueue(t *testing.T) {
	t.Parallel()

	db := &DB{
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "older"}),
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", tombstone: true}),
		},
		queueShardIDs: []uint16{0, 0},
	}

	_, _, flags, found := db.rawPointRootDomainEntry([]byte("k"))
	if !found {
		t.Fatal("expected raw point entry hit")
	}
	if flags&node.FlagTombstone == 0 {
		t.Fatalf("flags=%08b want tombstone", flags)
	}
}

func TestRootDomainSnapshotFromCachedSnapshot_QueueTombstoneBeatsPublishedState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	if err := backend.SetSync([]byte("k"), []byte("backend-v")); err != nil {
		t.Fatalf("set backend value: %v", err)
	}

	backendSnap := backend.AcquireSnapshot()
	if backendSnap == nil {
		t.Fatal("expected backend snapshot")
	}
	defer backendSnap.Close()

	view := &memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "mutable-v"}),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", tombstone: true}),
		},
		queueShardIDs: []uint16{0},
	}
	snap := &Snapshot{
		db: &DB{
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		},
		view:            view,
		backend:         backendSnap,
		rootPointShards: []rootDomainSnapshot{{immutables: []memtable.Table{view.queue[0]}}},
	}
	rootSnap := rootDomainSnapshotFromCachedSnapshot(snap, []byte("k"))
	if rootSnap.mutable != nil {
		t.Fatal("expected cached snapshot tuple to exclude mutable run")
	}
	if rootSnap.hasVisibleKey([]byte("k")) {
		t.Fatal("expected queue tombstone to hide published backend state")
	}
}

func TestPublishMemtablesLocked_UsesRootDomainStateAuthority(t *testing.T) {
	t.Parallel()

	legacyQueued := newRootDomainTestTable(t, rootDomainTestOp{key: "scan", value: "legacy-iter"})
	stateMutable := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "state-mutable"})
	stateQueued := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "state-queued"})
	iterQueued := newRootDomainTestTable(t, rootDomainTestOp{key: "scan", value: "state-iter"})

	db := &DB{
		mutableShards: []memShard{
			{mem: stateMutable},
		},
		queue:         []memtable.Table{legacyQueued},
		queueShardIDs: []uint16{0},
		queueRanges:   []keyRange{{valid: true, min: []byte("scan"), max: []byte("scan")}},
		rootPointStates: []rootDomainState{
			{
				mutable:    stateMutable,
				immutables: []memtable.Table{stateQueued},
			},
		},
		rootIteratorState: rootDomainState{
			immutables: []memtable.Table{iterQueued},
		},
	}

	db.publishMemtablesLocked()
	view := db.retainMemtableView()
	if view == nil {
		t.Fatal("expected published memtable view")
	}
	defer db.releaseMemtableView(view)

	if got := len(view.rootPointShards); got != 1 {
		t.Fatalf("root point shards=%d want 1", got)
	}
	assertRootDomainVisibleValue(t, view.rootPointShards[0], "k", "state-mutable")
	if got := len(view.rootSnapshotShards); got != 1 {
		t.Fatalf("snapshot shards=%d want 1", got)
	}
	assertRootDomainVisibleValue(t, view.rootSnapshotShards[0], "k", "state-queued")
	assertRootDomainVisibleValue(t, view.rootIterator, "scan", "state-iter")
}

func TestPublishMemtablesLocked_RootDomainSnapshotsDoNotAliasLaterStateReuse(t *testing.T) {
	t.Parallel()

	first := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "first"})
	second := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "second"})
	iterFirst := newRootDomainTestTable(t, rootDomainTestOp{key: "scan", value: "iter-first"})
	iterSecond := newRootDomainTestTable(t, rootDomainTestOp{key: "scan", value: "iter-second"})

	db := &DB{
		mutableShards: []memShard{
			{mem: newRootDomainTestTable(t, rootDomainTestOp{key: "m", value: "mutable"})},
		},
		rootPointStates: []rootDomainState{
			{immutables: []memtable.Table{first}},
		},
		rootIteratorState: rootDomainState{
			immutables: []memtable.Table{iterFirst},
		},
	}

	db.publishMemtablesLocked()
	view := db.retainMemtableView()
	if view == nil {
		t.Fatal("expected published memtable view")
	}
	defer db.releaseMemtableView(view)

	db.rootPointStates[0].immutables = db.rootPointStates[0].immutables[:0]
	db.rootPointStates[0].immutables = append(db.rootPointStates[0].immutables, second)
	db.rootIteratorState.immutables = db.rootIteratorState.immutables[:0]
	db.rootIteratorState.immutables = append(db.rootIteratorState.immutables, iterSecond)

	assertRootDomainVisibleValue(t, view.rootPointShards[0], "k", "first")
	assertRootDomainVisibleValue(t, view.rootSnapshotShards[0], "k", "first")
	assertRootDomainVisibleValue(t, view.rootIterator, "scan", "iter-first")
}

func TestRotateMutableShardsLocked_UpdatesRootDomainStates(t *testing.T) {
	t.Parallel()

	mutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	mutable.Set([]byte("k"), []byte("v"))

	db := &DB{
		mutableShards: []memShard{
			{mem: mutable, rng: keyRange{valid: true, min: []byte("k"), max: []byte("k")}},
		},
		rootPointStates: []rootDomainState{{mutable: mutable}},
	}
	db.storeMemtableMode(memtable.ModeAppendOnly)
	db.bpCond = sync.NewCond(&db.bpMu)

	db.mu.Lock()
	err = db.rotateMutableShardsLocked(0, false)
	db.mu.Unlock()
	if err != nil {
		t.Fatalf("rotate mutable shards: %v", err)
	}

	if got := len(db.rootPointStates); got != 1 {
		t.Fatalf("root point states=%d want 1", got)
	}
	if got := len(db.rootPointStates[0].immutables); got != 1 {
		t.Fatalf("root point immutables=%d want 1", got)
	}
	assertRootDomainVisibleValue(t, db.rootPointStates[0].snapshot(), "k", "v")
	if db.rootPointStates[0].mutable == nil {
		t.Fatal("expected replacement mutable after rotate")
	}
	if db.rootPointStates[0].mutable == mutable {
		t.Fatal("expected rotate to install a new mutable memtable")
	}
	if got := len(db.rootIteratorState.immutables); got != 1 {
		t.Fatalf("iterator immutables=%d want 1", got)
	}
	assertRootDomainVisibleValue(t, db.rootIteratorState.snapshot(), "k", "v")
}

func TestPromoteRootDomainMutableLocked_SkipsEmptySealedMemtable(t *testing.T) {
	t.Parallel()

	mutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	next, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new next mutable: %v", err)
	}

	db := &DB{
		mutableShards: []memShard{{mem: mutable}},
		rootPointStates: []rootDomainState{
			{mutable: mutable},
		},
	}
	db.promoteRootDomainMutableLocked(0, mutable, next)

	if got := len(db.rootPointStates[0].immutables); got != 0 {
		t.Fatalf("root point immutables=%d want 0", got)
	}
	if got := len(db.rootIteratorState.immutables); got != 0 {
		t.Fatalf("iterator immutables=%d want 0", got)
	}
	if db.rootPointStates[0].mutable != next {
		t.Fatal("expected replacement mutable to be installed")
	}
}

func TestRemoveQueuedUnitsLocked_TrimsRootDomainStates(t *testing.T) {
	t.Parallel()

	first := newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "first"})
	second := newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "second"})

	db := &DB{
		mutableShards: make([]memShard, 1),
		queue:         []memtable.Table{first, second},
		queueShardIDs: []uint16{0, 0},
		queueIDs:      []uint64{11, 12},
		rootPointStates: []rootDomainState{
			{immutables: []memtable.Table{first, second}},
		},
		rootIteratorState: rootDomainState{
			immutables: []memtable.Table{first, second},
		},
	}

	units := []flushUnit{
		{mem: first, id: 11},
	}
	db.removeQueuedUnitsLocked(map[uint64]struct{}{11: {}}, units, first.Size())

	if got := len(db.queue); got != 1 {
		t.Fatalf("queue len=%d want 1", got)
	}
	if db.queue[0] != second {
		t.Fatal("expected second memtable to remain queued")
	}
	if got := len(db.rootPointStates[0].immutables); got != 1 {
		t.Fatalf("root point immutables=%d want 1", got)
	}
	if db.rootPointStates[0].immutables[0] != second {
		t.Fatal("expected first queued memtable trimmed from point state")
	}
	if got := len(db.rootIteratorState.immutables); got != 1 {
		t.Fatalf("iterator immutables=%d want 1", got)
	}
	if db.rootIteratorState.immutables[0] != second {
		t.Fatal("expected first queued memtable trimmed from iterator state")
	}
}

func TestRootDomainSnapshot_HasPrefixesSortedExactness(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		publishedRootID: 66,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "uniq/alice/doc-1", value: "published-alice"},
			rootDomainTestOp{key: "uniq/bob/doc-1", value: "published-bob"},
			rootDomainTestOp{key: "uniq/nested/doc-1", value: "published-nested"},
		),
		immutables: []memtable.Table{
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "uniq/alice/doc-1", tombstone: true},
			),
		},
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "uniq/carol/doc-2", value: "mutable-carol"},
			rootDomainTestOp{key: "uniq/nested/deep/doc-3", value: "mutable-nested"},
		),
	}

	prefixes := [][]byte{
		[]byte("uniq/alice/"),
		[]byte("uniq/bob/"),
		[]byte("uniq/carol/"),
		[]byte("uniq/dan/"),
		[]byte("uniq/nested/"),
		[]byte("uniq/nested/deep/"),
	}
	got := make([]bool, len(prefixes))
	if err := snap.hasPrefixesSorted(prefixes, got); err != nil {
		t.Fatalf("hasPrefixesSorted: %v", err)
	}

	want := []bool{false, true, true, false, true, true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix visibility mismatch: got=%v want=%v", got, want)
	}
}

func TestRootDomainSnapshot_HasPrefixesSortedUsesOneIteratorPerSource(t *testing.T) {
	t.Parallel()

	published := &countingTable{inner: newRootDomainTestTable(t,
		rootDomainTestOp{key: "acct/a/doc-1", value: "published-a"},
	)}
	immutable := &countingTable{inner: newRootDomainTestTable(t,
		rootDomainTestOp{key: "acct/b/doc-1", value: "immutable-b"},
	)}
	mutable := &countingTable{inner: newRootDomainTestTable(t,
		rootDomainTestOp{key: "acct/c/doc-1", value: "mutable-c"},
	)}

	snap := rootDomainSnapshot{
		publishedRootID: 77,
		published:       published,
		immutables:      []memtable.Table{immutable},
		mutable:         mutable,
	}
	prefixes := [][]byte{
		[]byte("acct/a/"),
		[]byte("acct/b/"),
		[]byte("acct/c/"),
	}
	got := make([]bool, len(prefixes))
	if err := snap.hasPrefixesSorted(prefixes, got); err != nil {
		t.Fatalf("hasPrefixesSorted: %v", err)
	}
	if !reflect.DeepEqual(got, []bool{true, true, true}) {
		t.Fatalf("prefix visibility mismatch: got=%v", got)
	}
	if mutable.iterCalls != 1 || immutable.iterCalls != 1 || published.iterCalls != 1 {
		t.Fatalf("expected one iterator per source, got mutable=%d immutable=%d published=%d", mutable.iterCalls, immutable.iterCalls, published.iterCalls)
	}
	if mutable.getEntryCalls != 0 || immutable.getEntryCalls != 0 || published.getEntryCalls != 0 {
		t.Fatalf("expected no GetEntry calls, got mutable=%d immutable=%d published=%d", mutable.getEntryCalls, immutable.getEntryCalls, published.getEntryCalls)
	}
}

func TestRootDomainSnapshot_HasPrefixesSortedSkipsTombstonedFirstMatch(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		publishedRootID: 88,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "uniq/alice/doc-1", value: "published-doc-1"},
			rootDomainTestOp{key: "uniq/alice/doc-2", value: "published-doc-2"},
		),
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "uniq/alice/doc-1", tombstone: true},
		),
	}

	prefixes := [][]byte{[]byte("uniq/alice/")}
	got := make([]bool, 1)
	if err := snap.hasPrefixesSorted(prefixes, got); err != nil {
		t.Fatalf("hasPrefixesSorted: %v", err)
	}
	if !got[0] {
		t.Fatal("expected later visible key under prefix to keep probe true")
	}
}

func BenchmarkRootDomainSnapshotVisibleValue(b *testing.B) {
	published, err := newRootDomainTable(rootDomainTestOp{key: "a", value: "published-a"})
	if err != nil {
		b.Fatalf("new published table: %v", err)
	}
	immutable, err := newRootDomainTable(rootDomainTestOp{key: "b", value: "immutable-b"})
	if err != nil {
		b.Fatalf("new immutable table: %v", err)
	}
	mutable, err := newRootDomainTable(rootDomainTestOp{key: "c", value: "mutable-c"})
	if err != nil {
		b.Fatalf("new mutable table: %v", err)
	}
	snap := rootDomainSnapshot{
		publishedRootID: 55,
		published:       published,
		immutables: []memtable.Table{
			immutable,
		},
		mutable: mutable,
	}
	key := []byte("c")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got, ok := snap.visibleValue(key); !ok || string(got) != "mutable-c" {
			b.Fatalf("visibleValue mismatch: ok=%v got=%q", ok, string(got))
		}
	}
}

func BenchmarkRootDomainStateSealMutable(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
		if err != nil {
			b.Fatalf("new mutable: %v", err)
		}
		mutable.Set([]byte("a"), []byte("1"))
		state := &rootDomainState{mutable: mutable}
		next, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
		if err != nil {
			b.Fatalf("new next mutable: %v", err)
		}
		b.StartTimer()
		state.sealMutable(next)
		b.StopTimer()
		if len(state.immutables) != 1 {
			b.Fatalf("immutables=%d want 1", len(state.immutables))
		}
	}
}

func BenchmarkPublishMemtablesLocked_RootDomainBuild(b *testing.B) {
	const (
		shards         = 8
		queuedPerShard = 4
	)

	db := &DB{
		mutableShards: make([]memShard, shards),
		queueRanges:   make([]keyRange, 0, shards*queuedPerShard),
		queueShardIDs: make([]uint16, 0, shards*queuedPerShard),
	}
	for shardIdx := 0; shardIdx < shards; shardIdx++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
		if err != nil {
			b.Fatalf("new mutable shard %d: %v", shardIdx, err)
		}
		mt.Set([]byte{byte('m'), byte('0' + shardIdx)}, []byte("v"))
		db.mutableShards[shardIdx].mem = mt
	}
	for run := 0; run < queuedPerShard; run++ {
		for shardIdx := 0; shardIdx < shards; shardIdx++ {
			key := []byte{byte('q'), byte('0' + run), byte('a' + shardIdx)}
			mt, err := newRootDomainTable(rootDomainTestOp{key: string(key), value: "v"})
			if err != nil {
				b.Fatalf("new queued table run=%d shard=%d: %v", run, shardIdx, err)
			}
			db.queue = append(db.queue, mt)
			db.queueShardIDs = append(db.queueShardIDs, uint16(shardIdx))
			db.queueRanges = append(db.queueRanges, keyRange{valid: true, min: append([]byte(nil), key...), max: append([]byte(nil), key...)})
		}
	}
	db.resetRootDomainStatesLocked()
	db.resyncRootDomainQueuedRunsLocked()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.publishMemtablesLocked()
		view := db.retainMemtableView()
		if view == nil {
			b.Fatal("expected published memtable view")
		}
		if got := len(view.rootPointShards); got != shards {
			b.Fatalf("root point shards=%d want %d", got, shards)
		}
		if got := len(view.rootIterator.immutables); got != len(db.queue) {
			b.Fatalf("root iterator immutables=%d want %d", got, len(db.queue))
		}
		db.releaseMemtableView(view)
	}
}

func BenchmarkRootDomainSnapshotHasPrefixesSorted(b *testing.B) {
	published, err := newRootDomainTable(
		rootDomainTestOp{key: "uniq/00/doc", value: "published-00"},
		rootDomainTestOp{key: "uniq/01/doc", value: "published-01"},
		rootDomainTestOp{key: "uniq/02/doc", value: "published-02"},
		rootDomainTestOp{key: "uniq/03/doc", value: "published-03"},
	)
	if err != nil {
		b.Fatalf("new published table: %v", err)
	}
	immutable, err := newRootDomainTable(
		rootDomainTestOp{key: "uniq/04/doc", value: "immutable-04"},
		rootDomainTestOp{key: "uniq/05/doc", value: "immutable-05"},
	)
	if err != nil {
		b.Fatalf("new immutable table: %v", err)
	}
	mutable, err := newRootDomainTable(
		rootDomainTestOp{key: "uniq/06/doc", value: "mutable-06"},
		rootDomainTestOp{key: "uniq/07/doc", value: "mutable-07"},
	)
	if err != nil {
		b.Fatalf("new mutable table: %v", err)
	}
	snap := rootDomainSnapshot{
		published:  published,
		immutables: []memtable.Table{immutable},
		mutable:    mutable,
	}
	prefixes := [][]byte{
		[]byte("uniq/00/"),
		[]byte("uniq/01/"),
		[]byte("uniq/02/"),
		[]byte("uniq/03/"),
		[]byte("uniq/04/"),
		[]byte("uniq/05/"),
		[]byte("uniq/06/"),
		[]byte("uniq/07/"),
	}
	out := make([]bool, len(prefixes))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range out {
			out[j] = false
		}
		if err := snap.hasPrefixesSorted(prefixes, out); err != nil {
			b.Fatalf("hasPrefixesSorted: %v", err)
		}
	}
}

func assertRootDomainVisibleValue(t *testing.T, snap rootDomainSnapshot, key, want string) {
	t.Helper()

	got, ok := snap.visibleValue([]byte(key))
	if !ok {
		t.Fatalf("key %q not visible", key)
	}
	if string(got) != want {
		t.Fatalf("key %q: got=%q want=%q", key, string(got), want)
	}
}
