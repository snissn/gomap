package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

var errOwnedPointReport = errors.New("test: accepted point prefix reported failure")

type ownedPointBackend struct {
	*backenddb.DB
	dir         string
	groups      atomic.Int64
	writes      atomic.Int64
	inject      atomic.Bool
	afterWrite  func(bool)
	beforeWrite func(bool) error
}

func (b *ownedPointBackend) BeginRootPublicationBuildGroup() (*backenddb.RootPublicationBuildGroup, error) {
	b.groups.Add(1)
	return b.DB.BeginRootPublicationBuildGroup()
}
func (b *ownedPointBackend) NewPhysicalBatchWithSize(n int) batch.Interface {
	return &ownedPointBatch{Batch: b.DB.NewPhysicalBatchWithSize(n).(*backenddb.Batch), owner: b}
}
func (b *ownedPointBackend) NewPhysicalBatch() batch.Interface { return b.NewPhysicalBatchWithSize(0) }

type ownedPointBatch struct {
	*backenddb.Batch
	owner *ownedPointBackend
	group *backenddb.RootPublicationBuildGroup
	final bool
}

func (b *ownedPointBatch) SetRootPublicationBuildGroup(g *backenddb.RootPublicationBuildGroup, final bool) error {
	b.group, b.final = g, final
	return b.Batch.SetRootPublicationBuildGroup(g, final)
}
func (b *ownedPointBatch) written(err error) error {
	b.owner.writes.Add(1)
	if err == nil && b.owner.afterWrite != nil {
		b.owner.afterWrite(b.final)
	}
	if err == nil && b.group != nil && b.final && b.group.Accepted() && b.owner.inject.Swap(false) {
		return errOwnedPointReport
	}
	return err
}
func (b *ownedPointBatch) Write() error {
	if b.owner.beforeWrite != nil {
		if err := b.owner.beforeWrite(b.final); err != nil {
			return err
		}
	}
	return b.written(b.Batch.Write())
}
func (b *ownedPointBatch) WriteSync() error {
	if b.owner.beforeWrite != nil {
		if err := b.owner.beforeWrite(b.final); err != nil {
			return err
		}
	}
	return b.written(b.Batch.WriteSync())
}

func ownedPointFixture(t *testing.T, cap int) (*DB, *ownedPointBackend) {
	return ownedPointValueFixture(t, cap, false)
}
func ownedPointValueFixture(t *testing.T, cap int, pointers bool) (*DB, *ownedPointBackend) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: pointers, ValueLog: backenddb.ValueLogOptions{ForcePointers: pointers}})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &ownedPointBackend{DB: backend, dir: dir}
	db, err := Open(dir, wrapped, Options{FlushThreshold: 1 << 30, MemtableShards: 2, JournalLanes: 2, MemtableMode: "hash_sorted", FlushBuildConcurrency: 2, FlushBackendMaxEntries: cap, FlushBackendMaxBatches: -1, ForceValueLogPointers: pointers, IndexOuterLeavesInValueLog: pointers, ValueLogMaxSegmentBytes: 512})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	})
	return db, wrapped
}
func enqueueOwnedPointLane(t *testing.T, db *DB, lane int, pairs ...string) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	for i := 0; i < len(pairs); i += 2 {
		setMutable(db, ownedPointKey(t, db, lane, pairs[i]), []byte(pairs[i+1]))
	}
	if err := db.rotateMemtableLocked(false); err != nil {
		t.Fatal(err)
	}
	if got := db.queueLaneIDs[len(db.queueLaneIDs)-1]; int(got) != lane {
		t.Fatalf("actual source lane=%d want %d", got, lane)
	}
}

func TestOwnedPointPrefixPacksAcrossActualSourceLanes(t *testing.T) {
	db, backend := ownedPointFixture(t, 2)
	enqueueOwnedPointLane(t, db, 0, "a", "first")
	enqueueOwnedPointLane(t, db, 1, "b", "second")
	seq := backend.State().CommitSeq
	db.flushAll(false)
	if got := backend.writes.Load(); got != 1 {
		t.Fatalf("physical batches=%d want 1 packed across lanes", got)
	}
	if got := backend.State().CommitSeq - seq; got != 1 {
		t.Fatalf("published roots=%d want 1", got)
	}
	for key, want := range map[string]string{"a": "first", "b": "second"} {
		got, err := backend.Get(ownedPointKey(t, db, map[string]int{"a": 0, "b": 1}[key], key))
		if err != nil || string(got) != want {
			t.Fatalf("%s=(%q,%v)", key, got, err)
		}
	}
}

func ownedPointKey(t *testing.T, db *DB, lane int, base string) []byte {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("%s/%04d", base, i))
		if db.laneForShardIndex(db.shardIndex(key)) == lane {
			return key
		}
	}
	t.Fatal("no key for actual source lane")
	return nil
}
func ownedPointWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not join")
	}
}

func TestOwnedPointPrefixStagedRootsAndSharedDrainPreserveNewerFrontier(t *testing.T) {
	db, backend := ownedPointFixture(t, 1)
	enqueueOwnedPointLane(t, db, 0, "a", "first", "c", "third")
	enqueueOwnedPointLane(t, db, 1, "b", "second")
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.mu.Unlock()
	seq := backend.State().CommitSeq
	old := db.AcquireSnapshot()
	defer old.Close()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	var paused atomic.Bool
	backend.afterWrite = func(final bool) {
		if !final && paused.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	}
	firstDone := make(chan struct{})
	go func() { db.flushCheckpointFrontierLocked(true, nil, frontier); close(firstDone) }()
	ownedPointWait(t, entered)
	if got := backend.State().CommitSeq; got != seq {
		t.Fatalf("private chunk exposed seq=%d want %d", got, seq)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 0, "a")); err != nil || got != nil {
		t.Fatalf("private chunk exposed value (%q,%v)", got, err)
	}
	if got, err := db.Get(ownedPointKey(t, db, 0, "a")); err != nil || string(got) != "first" {
		t.Fatalf("queued read=(%q,%v)", got, err)
	}
	enqueueOwnedPointLane(t, db, 1, "late", "newer")
	db.mu.Lock()
	lateID := db.queueIDs[len(db.queueIDs)-1]
	if err := db.enqueueRangeSpanLayerLocked([]batch.DeleteRange{{Start: []byte("a"), End: []byte("d")}}); err != nil {
		db.mu.Unlock()
		t.Fatal(err)
	}
	rangeID := db.queueIDs[len(db.queueIDs)-1]
	db.publishMemtablesLocked()
	db.mu.Unlock()
	secondStarted, secondDone := make(chan struct{}), make(chan struct{})
	go func() { close(secondStarted); db.flushCheckpointFrontierLocked(true, nil, frontier); close(secondDone) }()
	ownedPointWait(t, secondStarted)
	// The claimed prefix excludes both new queue IDs even while a second shared
	// checkpoint participant is trying to drain the same original frontier.
	release()
	ownedPointWait(t, firstDone)
	ownedPointWait(t, secondDone)
	if got := backend.writes.Load(); got != 3 {
		t.Fatalf("covered prefix replayed: physical writes=%d want 3", got)
	}
	if got := backend.State().CommitSeq - seq; got != 1 {
		t.Fatalf("roots=%d want 1", got)
	}
	db.mu.RLock()
	remaining := append([]uint64(nil), db.queueIDs...)
	db.mu.RUnlock()
	if len(remaining) != 2 || remaining[0] != lateID || remaining[1] != rangeID {
		t.Fatalf("newer queue IDs changed: %v", remaining)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 1, "late")); err != nil || got != nil {
		t.Fatalf("post-frontier backend value=(%q,%v)", got, err)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 0, "a")); err != nil || string(got) != "first" {
		t.Fatalf("newer range crossed frontier=(%q,%v)", got, err)
	}
	if got, err := old.Get(ownedPointKey(t, db, 0, "a")); err != nil || string(got) != "first" {
		t.Fatalf("retained queue snapshot lost accepted source=(%q,%v)", got, err)
	}
	if got, err := old.Get(ownedPointKey(t, db, 1, "late")); (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || got != nil {
		t.Fatalf("retained snapshot exposed later arrival=(%q,%v)", got, err)
	}
}

func TestOwnedPointPrefixAcceptedReportedErrorCannotReplay(t *testing.T) {
	db, backend := ownedPointFixture(t, 1)
	enqueueOwnedPointLane(t, db, 0, "a", "first")
	enqueueOwnedPointLane(t, db, 1, "b", "second")
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.mu.Unlock()
	db.mu.RLock()
	var walPaths []string
	for _, paths := range db.queueWALPaths {
		walPaths = append(walPaths, paths...)
	}
	db.mu.RUnlock()
	if len(walPaths) == 0 {
		t.Fatal("fixture did not capture WAL paths")
	}
	backend.inject.Store(true)
	seq := backend.State().CommitSeq
	db.flushCheckpointFrontierLocked(true, nil, frontier)
	if !errors.Is(db.backgroundError(), errOwnedPointReport) {
		t.Fatalf("reported error=%v", db.backgroundError())
	}
	if backend.State().CommitSeq != seq+1 {
		t.Fatal("real group did not accept")
	}
	db.mu.RLock()
	left := len(db.queue)
	db.mu.RUnlock()
	if left != 0 {
		t.Fatalf("accepted IDs retained without coverage: %d", left)
	}
	for _, path := range walPaths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("accepted reported error deleted WAL %s: %v", path, err)
		}
	}
	enqueueOwnedPointLane(t, db, 1, "late", "newer")
	db.flushCheckpointFrontierLocked(true, nil, frontier)
	if backend.writes.Load() != 2 || backend.State().CommitSeq != seq+1 {
		t.Fatal("accepted prefix replayed on frontier retry")
	}
	db.mu.RLock()
	left = len(db.queue)
	db.mu.RUnlock()
	if left != 1 {
		t.Fatal("frontier retry removed newer queue")
	}
	db.flushAll(true)
	if got, err := backend.Get(ownedPointKey(t, db, 1, "late")); err != nil || string(got) != "newer" {
		t.Fatalf("later drain=(%q,%v)", got, err)
	}
}

func TestOwnedPointPrefixAbortPreservesQueueAndRoot(t *testing.T) {
	db, backend := ownedPointFixture(t, 1)
	enqueueOwnedPointLane(t, db, 0, "a", "first")
	enqueueOwnedPointLane(t, db, 1, "b", "second")
	seq := backend.State().CommitSeq
	backend.beforeWrite = func(final bool) error {
		if final {
			return errors.New("test: refuse final chunk")
		}
		return nil
	}
	db.flushAll(false)
	if backend.State().CommitSeq != seq {
		t.Fatal("aborted private group exposed a root")
	}
	db.mu.RLock()
	left := len(db.queue)
	db.mu.RUnlock()
	if left != 2 {
		t.Fatalf("aborted queue units=%d want 2", left)
	}
	backend.beforeWrite = nil
	db.flushAll(true)
	if backend.State().CommitSeq != seq+1 {
		t.Fatal("retry did not publish exactly one root")
	}
}

func TestOwnedPointPrefixFiniteBudgetAndBusyLaneFallback(t *testing.T) {
	db, backend := ownedPointFixture(t, 64)
	for i := 0; i < 40; i++ {
		enqueueOwnedPointLane(t, db, i%2, fmt.Sprintf("key-%03d", i), "value")
	}
	db.flushLaneMu[1].Lock()
	attempted, _ := db.tryFlushOwnedPointPrefix(false, false, nil, false)
	db.flushLaneMu[1].Unlock()
	if attempted {
		t.Fatal("busy source lane was claimed")
	}
	if !db.flushLaneMu[0].TryLock() {
		t.Fatal("partial claim retained another lane lock")
	}
	db.flushLaneMu[0].Unlock()
	if backend.groups.Load() != 0 {
		t.Fatal("backend group opened before all source claims")
	}
	attempted, progress := db.tryFlushOwnedPointPrefix(false, false, nil, false)
	if !attempted || !progress {
		t.Fatalf("claim=(%v,%v)", attempted, progress)
	}
	max, _ := db.baseFlushUnitBudget()
	db.mu.RLock()
	left := len(db.queue)
	db.mu.RUnlock()
	if left != 40-max {
		t.Fatalf("finite claim drained %d want existing budget %d", 40-left, max)
	}
	if backend.writes.Load() != 1 {
		t.Fatal("finite prefix was not packed")
	}
	db.flushSpanRunTargetPlanning = true
	if attempted, _ := db.tryFlushOwnedPointPrefix(false, false, nil, false); attempted {
		t.Fatal("unsupported target planner claimed")
	}
	db.flushSpanRunTargetPlanning = false
	db.checkpointFlushPreemptActive.Add(1)
	attempted, progress = db.tryFlushOwnedPointPrefix(false, false, nil, false)
	db.checkpointFlushPreemptActive.Add(-1)
	if !attempted || progress {
		t.Fatal("checkpoint preemption did not stop background claim")
	}
	if backend.writes.Load() != 1 {
		t.Fatal("preempted claim wrote backend")
	}
}

func TestOwnedPointPrefixWinningSourcePreservesPointerRevisionAndDelete(t *testing.T) {
	older := memtable.NewAppendOnlyWithEntryCapacity(2)
	older.Set([]byte("dup"), []byte("old"))
	older.Set([]byte("old"), []byte("kept"))
	older.Freeze()
	newer := memtable.NewAppendOnlyWithEntryCapacity(2)
	ptr := page.ValuePtr{FileID: 7, Offset: 88, Length: 9}
	newer.SetEntryWithRevision([]byte("dup"), nil, ptr, node.FlagPointer, 44)
	newer.Delete([]byte("gone"))
	newer.Freeze()
	units := []flushUnit{{mem: older, laneID: 0}, {mem: newer, laneID: 1}}
	run := &canonicalFlushRun{}
	err := mergeCanonicalStableIteratorUnitsWithSource(units, run, func(entry batch.Entry, lane int) error {
		switch string(entry.Key) {
		case "dup":
			if lane != 1 || !entry.IsPtr || entry.ValuePtr != ptr || entry.Revision != 44 {
				t.Fatalf("winning pointer source changed: lane=%d entry=%+v", lane, entry)
			}
		case "gone":
			if lane != 1 || entry.Type != batch.OpDelete {
				t.Fatal("winning tombstone source changed")
			}
		case "old":
			if lane != 0 {
				t.Fatal("old-only source changed")
			}
		default:
			t.Fatal("unexpected entry")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.shadowedPointOps != 1 || run.plannedPointOps != 3 {
		t.Fatal("duplicate membership changed")
	}
}

func TestOwnedPointPrefixPointerRotationAndReopen(t *testing.T) {
	db, backend := ownedPointValueFixture(t, 2, true)
	values := make(map[string][]byte)
	source := make(map[string]int)
	for lane := 0; lane < 2; lane++ {
		for i := 0; i < 4; i++ {
			key := ownedPointKey(t, db, lane, fmt.Sprintf("pointer-%d-%d", lane, i))
			value := bytes.Repeat([]byte(fmt.Sprintf("value-%d-%d|", lane, i)), 128)
			if err := db.Set(key, value); err != nil {
				t.Fatal(err)
			}
			values[string(key)] = value
			source[string(key)] = lane
			if i == 1 {
				if err := db.rotateValueLogLocked(&db.lanes[lane]); err != nil {
					t.Fatal(err)
				}
			}
		}
		db.mu.Lock()
		err := db.rotateMemtableLocked(false)
		db.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	seq := backend.State().CommitSeq
	db.flushAll(true)
	if backend.State().CommitSeq != seq+1 {
		t.Fatal("rotating pointers did not publish one root")
	}
	snapshot := backend.AcquireSnapshot()
	generations := make(map[uint32]bool)
	for key, want := range values {
		entry, err := snapshot.GetEntry([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Flags&node.FlagPointer == 0 {
			t.Fatal("fixture did not force persistent pointers")
		}
		lane, _ := valuelog.DecodeFileID(entry.ValuePtr.FileID)
		if int(lane) != source[key] {
			t.Fatalf("pointer source %q lane=%d want %d", key, lane, source[key])
		}
		generations[entry.ValuePtr.FileID] = true
		got, err := snapshot.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("pointer read %q=(%q,%v)", key, got, err)
		}
	}
	_ = snapshot.Close()
	if len(generations) <= 2 {
		t.Fatalf("fixture did not rotate both actual lane writers: %v", generations)
	}
	dir := backend.dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true, ValueLog: backenddb.ValueLogOptions{ForcePointers: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range values {
		got, err := reopened.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopened default-integrity pointer %q=(%q,%v)", key, got, err)
		}
	}
}

// Factory presence on a wrapper does not prove that its returned batch carries
// the actual group attachment capability. The old single-lane route remains.
type ownedPointUnsupportedBatch struct{ batch.Interface }
type ownedPointUnsupportedBackend struct{ *ownedPointBackend }

func (b *ownedPointUnsupportedBackend) NewPhysicalBatchWithSize(n int) batch.Interface {
	return &ownedPointUnsupportedBatch{b.ownedPointBackend.NewPhysicalBatchWithSize(n)}
}
func (b *ownedPointUnsupportedBackend) NewPhysicalBatch() batch.Interface {
	return b.NewPhysicalBatchWithSize(0)
}
func TestOwnedPointPrefixUnsupportedBatchFallsBack(t *testing.T) {
	db, backend := ownedPointFixture(t, 2)
	db.backend = &ownedPointUnsupportedBackend{backend}
	enqueueOwnedPointLane(t, db, 0, "a", "first")
	enqueueOwnedPointLane(t, db, 1, "b", "second")
	db.flushAll(true)
	if backend.groups.Load() != 0 {
		t.Fatal("wrapper factory was treated as group authority")
	}
	if backend.writes.Load() != 2 {
		t.Fatal("unsupported producer did not use existing lane flush")
	}
	db.mu.RLock()
	left := len(db.queue)
	db.mu.RUnlock()
	if left != 0 {
		t.Fatal("fallback did not drain")
	}
}

func TestOwnedPointPrefixActiveFrontierRemainsCooperative(t *testing.T) {
	db, backend := ownedPointFixture(t, 4)
	for i := 0; i < 4; i++ {
		enqueueOwnedPointLane(t, db, i%2, fmt.Sprintf("frontier-%d", i), "old")
	}
	db.mu.Lock()
	frontier := db.captureCheckpointFrontierLocked()
	db.mu.Unlock()
	enqueueOwnedPointLane(t, db, 0, "late", "newer")
	db.setActiveCheckpointFrontier(frontier)
	db.checkpointing.Store(true)
	defer func() { db.clearActiveCheckpointFrontier(); db.checkpointing.Store(false) }()
	db.flushAll(false)
	db.mu.RLock()
	remaining := append([]uint64(nil), db.queueIDs...)
	db.mu.RUnlock()
	if len(remaining) != 3 {
		t.Fatalf("shared participant exceeded active-lane unit budget: remaining=%v", remaining)
	}
	if backend.writes.Load() != 1 {
		t.Fatal("one cooperative claim did not pack two eligible lanes")
	}
	if !frontier.contains(remaining[0]) || !frontier.contains(remaining[1]) || frontier.contains(remaining[2]) {
		t.Fatalf("captured frontier changed: %v", remaining)
	}
	if got, err := backend.Get(ownedPointKey(t, db, 0, "late")); err != nil || got != nil {
		t.Fatalf("shared drain crossed captured frontier=(%q,%v)", got, err)
	}
}
