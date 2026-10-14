package mvcc

import (
	"bytes"
	"errors"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"strconv"
	"testing"
	"time"
)

// This is the public maintenance route, with an ordinary cold cached source;
// it must not use a pre-checkpoint, prewarmed iterator, or disabled cache.
func TestPruneQuantumColdDefaultCompressedHugeHistory(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	const history = 4096
	// More than 256 order-breaking writes exercise the ordinary unordered
	// fallback. The physical MVCC codec reverses timestamp ordering.
	for i := uint64(1); i <= history; i++ {
		timestamp := ((i * 2053) % history) + 1
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("huge"), Value: []byte("record")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(history, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	const recordBudget = 32
	stats, err := store.PruneVersions(PruneOptions{
		BatchSize: 8, Mode: CommitRelaxed,
		WorkRecords: recordBudget, WorkBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("small ordinary quantum must make progress: %v", err)
	}
	if stats.Visited == 0 || stats.Visited >= history {
		t.Fatalf("quantum did not stop inside huge history: visited=%d history=%d work=%d", stats.Visited, history, stats.WorkRecords)
	}
	if stats.WorkRecords == 0 || stats.WorkRecords > recordBudget || stats.WorkBytes > 1<<20 {
		t.Fatalf("all physical work must fit declared quantum: %+v", stats)
	}
	if stats.SetupRecords == 0 || stats.CleanupRecords == 0 {
		t.Fatalf("setup and cleanup were not accounted: %+v", stats)
	}
	t.Logf("cache discovery: visited=%d physical/source/setup/cleanup work=%d bytes=%d setup=%d cleanup=%d deletes=%d", stats.Visited, stats.WorkRecords, stats.WorkBytes, stats.SetupRecords, stats.CleanupRecords, stats.Pruned)
	if stats.Complete {
		t.Fatalf("one quantum incorrectly completed huge history: %+v", stats)
	}
}

// The candidate is not complete until repeated fixed quanta perform actual
// reclamation. This stays red while only raw cache discovery is implemented.
func TestPruneQuantumEventualFullReclaim(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	const history = 1024
	for timestamp := uint64(1); timestamp <= history; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("huge"), Value: []byte("record")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(history, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	var cursor *PruneCursor
	var deleted uint64
	for quantum := 0; quantum < history*4; quantum++ {
		stats, err := store.PruneVersions(PruneOptions{BatchSize: 8, Mode: CommitRelaxed, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if err != nil {
			t.Fatalf("quantum %d could not resume: %v; stats=%+v", quantum, err, stats)
		}
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
			t.Fatalf("quantum %d exceeded work: %+v", quantum, stats)
		}
		deleted += stats.Pruned
		if stats.Complete {
			if deleted != history-1 {
				t.Fatalf("deleted %d want %d", deleted, history-1)
			}
			requireResult(t, store, []byte("huge"), history+1, Present, history, []byte("record"))
			return
		}
		if stats.Cursor == nil {
			t.Fatalf("quantum %d omitted continuation", quantum)
		}
		cursor = stats.Cursor
	}
	t.Fatal("fixed small quantum never completed history")
}

// The real public route retains no borrowed key when it stops inside dense
// shadows. Later quanta leave the earlier caller-owned cursor intact.
func TestPruneQuantumOwnedCandidateDenseShadows(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	for i := 0; i < 4096; i++ {
		if err := store.CommitAt(1, []Mutation{{Key: []byte("same"), Value: []byte("value"), Delete: i%2 != 0}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(1, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	wantKey, err := mvcckey.Encode([]byte("same"), 1)
	if err != nil {
		t.Fatal(err)
	}
	owned := func(c *PruneCursor) pruneDiscoveryCandidate {
		if len(c.candidate.physical) != 0 {
			return c.candidate
		}
		return c.anchor
	}
	// Floor capture and winner qualification are bounded phases. Observe their
	// actual completion rather than assuming the first quantum has a candidate.
	var first PruneStats
	for quantum := 0; quantum < 4096; quantum++ {
		first, err = store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: first.Cursor})
		if err != nil || first.WorkRecords > 32 || first.WorkBytes > 1<<20 {
			t.Fatalf("owned candidate quantum %d: %+v %v", quantum, first, err)
		}
		if first.Cursor == nil {
			t.Fatalf("completed before observing an owned winner: %+v", first)
		}
		if c := owned(first.Cursor); len(c.physical) != 0 && c.revision != 0 {
			break
		}
	}
	before := owned(first.Cursor)
	if !bytes.Equal(before.physical, wantKey) || before.timestamp != 1 || before.revision == 0 {
		t.Fatal("wrong reverse-order candidate")
	}
	// Compare with the exact native source entry without rotating the cache via
	// a snapshot iterator. No mutation means later quanta keep this revision.
	w := treedb.MaintenanceOrdinalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	key, _, _, _, revision, admitted, err := db.ReadMaintenanceEntry(first.Cursor.ordinal, before.sourcePriority, before.entryOrdinal, before.entryRef, &w)
	if err != nil || !admitted || !bytes.Equal(key, wantKey) || before.revision != revision {
		t.Fatalf("candidate did not retain its actual native entry: %v %v", admitted, err)
	}
	beforeKey := bytes.Clone(before.physical)
	second, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: first.Cursor})
	if err != nil {
		t.Fatal(err)
	}
	if second.Cursor == nil || owned(second.Cursor).revision != revision || !bytes.Equal(owned(second.Cursor).physical, wantKey) {
		t.Fatal("next quantum lost the unchanged dense winner")
	}
	if owned(first.Cursor).revision != before.revision || !bytes.Equal(owned(first.Cursor).physical, beforeKey) {
		t.Fatal("next quantum mutated the previous cursor")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(owned(first.Cursor).physical, beforeKey) || !bytes.Equal(owned(second.Cursor).physical, wantKey) {
		t.Fatal("cursor retained borrowed source memory")
	}
}

// Natural persistence exercises cold floor capture and compressed outer-leaf
// pointer decoding. This is an implementation gate; the cache-only green
// fixture cannot qualify the persisted-source path.
func TestPruneQuantumColdReopenedCompressedPointers(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	options.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	const history = 1024
	payload := bytes.Repeat([]byte("persisted-payload"), 256)
	for ts := uint64(1); ts <= history; ts++ {
		if err := store.CommitAt(ts, []Mutation{{Key: []byte("huge"), Value: payload}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(history, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20})
	if err != nil {
		t.Fatalf("cold persisted source failed admission: %v", err)
	}
	if stats.WorkRecords == 0 || stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Complete {
		t.Fatalf("invalid cold quantum: %+v", stats)
	}
	if stats.Cursor == nil {
		t.Fatal("cold persisted quantum omitted owned continuation")
	}
	_, err = store.GetAt([]byte("huge"), history)
	if !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("floor read: %v", err)
	}
}

// Freeze ordinary queued puts before publishing the floor and deletion. After
// every bounded delete ACK, ordinary checkpoint/reopen must not replay those
// older puts behind a removed logical tombstone. The pinned reader also keeps
// its old value until it closes; maintenance cannot reclaim its backing log.
func TestPruneQuantumQueuedPutsBeforeFloorDeleteACKNoResurrection(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	options.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	const history = 64
	payload := bytes.Repeat([]byte("queued-old"), 128)
	for ts := uint64(1); ts < history; ts++ {
		if err := store.CommitAt(ts, []Mutation{{Key: []byte("gone"), Value: payload}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	oldReader := db.AcquireSnapshot()
	if oldReader == nil {
		t.Fatal("old reader capture")
	}
	defer oldReader.Close()
	queueLen, err := strconv.ParseUint(db.Stats()["treedb.cache.queue_len"], 10, 64)
	if err != nil || queueLen == 0 {
		t.Fatalf("fixture did not retain queued puts: queue=%d err=%v", queueLen, err)
	}
	if err := store.CommitAt(history, []Mutation{{Key: []byte("gone"), Delete: true}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(history, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	var cursor *PruneCursor
	var deleted uint64
	complete := false
	for quantum := 0; quantum < 2048; quantum++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if err != nil {
			t.Fatalf("quantum %d: %v", quantum, err)
		}
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
			t.Fatalf("unadmitted ACK/cleanup: %+v", stats)
		}
		deleted += stats.Pruned
		if stats.Complete {
			complete = true
			break
		}
		if stats.Cursor == nil {
			t.Fatal("missing owned continuation")
		}
		cursor = stats.Cursor
	}
	if !complete || deleted != history {
		t.Fatalf("full logical-tomb reclamation complete=%v deleted=%d", complete, deleted)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	requireResult(t, store, []byte("gone"), history+1, Absent, 0, nil)
	oldKey, err := mvcckey.Encode([]byte("gone"), history-1)
	if err != nil {
		t.Fatal(err)
	}
	oldRecord, err := oldReader.Get(oldKey)
	if err != nil || len(oldRecord) != len(payload)+1 || !bytes.Equal(oldRecord[1:], payload) {
		t.Fatalf("old reader payload lost: bytes=%d err=%v", len(oldRecord), err)
	}
	if err := oldReader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	requireResult(t, store, []byte("gone"), history+1, Absent, 0, nil)
	_, err = store.GetAt([]byte("gone"), history)
	if !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("persisted floor lost: %v", err)
	}
}

// Pager pages are one physical source of the full route, not qualification of
// compressed leaf-log frames. The same public quantum must admit page reads,
// checksum verification, prefix reconstruction, capture and unwind.
func TestPruneQuantumPersistedPagerDiscovery(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = false
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	for timestamp := uint64(1); timestamp <= 1024; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("persisted"), Value: []byte("record")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(1024, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var cursor *PruneCursor
	for q := 0; q < 4096; q++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if err != nil {
			t.Fatalf("persisted page discovery quantum %d: %v", q, err)
		}
		if stats.Cursor == nil || stats.WorkRecords == 0 || stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Pruned != 0 || stats.Complete {
			t.Fatalf("unbounded or unaccounted persisted discovery quantum %d: %+v", q, stats)
		}
		cursor = stats.Cursor
		// Checkpoint may leave source ownership pending retirement. That work
		// must resume at the same Q before the persisted page is admitted.
		if stats.Visited != 0 {
			if stats.Visited >= 1024 || stats.WorkBytes < 4096 {
				t.Fatalf("unaccounted persisted page discovery: %+v", stats)
			}
			return
		}
	}
	t.Fatal("fixed quantum did not reach the persisted page after charged cleanup")
}

// An adequate typed retry must advance the ordinary physical path rather than
// spin forever when setup/page/prefix work consumes the entire small allowance.
func TestPruneQuantumPagerTooSmallAdequateRetry(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = false
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	for i := uint64(1); i <= 256; i++ {
		if err := store.CommitAt(i, []Mutation{{Key: []byte("key"), Value: []byte("record")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(256, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Cache source/slot observations are legitimate first-unit progress.
	var cursor *PruneCursor
	var needed *treedb.MaintenanceOrdinalUnitTooLarge
	for q := 0; q < 4096; q++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 4, WorkBytes: 1 << 20, Cursor: cursor})
		cursor = stats.Cursor
		if err != nil {
			if !errors.As(err, &needed) {
				t.Fatal(err)
			}
			break
		}
		if cursor == nil || stats.WorkRecords == 0 || stats.WorkRecords > 4 || stats.WorkBytes > 1<<20 {
			t.Fatalf("unbounded or stalled small discovery quantum %d: %+v", q, stats)
		}
	}
	if needed == nil || needed.Records <= 4 || needed.Bytes == 0 || cursor == nil {
		t.Fatalf("missing adequate typed admission: %+v", needed)
	}
	cacheBudget := *needed
	cacheBefore := cursor.ordinal
	stats, err := store.PruneVersions(PruneOptions{WorkRecords: cacheBudget.Records, WorkBytes: cacheBudget.Bytes, Cursor: cursor})
	if err != nil {
		t.Fatalf("advertised cache retry failed: %+v: %v", cacheBudget, err)
	}
	if stats.Cursor == nil || stats.WorkRecords > cacheBudget.Records || stats.WorkBytes > cacheBudget.Bytes {
		t.Fatalf("typed cache retry unbounded/missing continuation: %+v", stats)
	}
	cacheAfter := stats.Cursor.ordinal
	if cacheAfter.Source == cacheBefore.Source && cacheAfter.Ordinal == cacheBefore.Ordinal && cacheAfter.CacheDone == cacheBefore.CacheDone {
		t.Fatal("adequate cache retry did not advance its source/slot")
	}
	cursor = stats.Cursor
	// Reuse that exact advertised envelope until an actual backend edge refuses.
	needed = nil
	for q := 0; q < 4096; q++ {
		stats, err = store.PruneVersions(PruneOptions{WorkRecords: cacheBudget.Records, WorkBytes: cacheBudget.Bytes, Cursor: cursor})
		cursor = stats.Cursor
		if err != nil {
			if !errors.As(err, &needed) {
				t.Fatal(err)
			}
			if cursor == nil || !cursor.ordinal.CacheDone || !cursor.ordinal.BackendCaptured {
				t.Fatalf("retry did not reach backend refusal: %+v: %v", stats, err)
			}
			break
		}
		if cursor == nil || stats.WorkRecords > cacheBudget.Records || stats.WorkBytes > cacheBudget.Bytes {
			t.Fatalf("advertised discovery envelope lost continuation: %+v", stats)
		}
	}
	if needed == nil {
		t.Fatal("missing actual backend admission refusal")
	}
	backendBefore := cursor.ordinal // by value: the retry mutates the cursor
	stats, err = store.PruneVersions(PruneOptions{WorkRecords: needed.Records, WorkBytes: needed.Bytes, Cursor: cursor})
	if err != nil {
		t.Fatalf("advertised backend quantum did not advance: %+v: %v", needed, err)
	}
	if stats.Cursor == nil || stats.WorkRecords > needed.Records || stats.WorkBytes > needed.Bytes {
		t.Fatalf("typed backend retry unbounded/missing continuation: %+v", stats)
	}
	backendAfter := stats.Cursor.ordinal
	if stats.Visited == 0 && backendAfter.BackendDepth <= backendBefore.BackendDepth && backendAfter.BackendStack == backendBefore.BackendStack {
		t.Fatalf("adequate backend retry did not traverse an edge/entry: needed=%+v", needed)
	}
}

// A valid pointer CRC does not excuse a corrupt live MVCC envelope, including
// a winner above the fixed floor. Metadata discovery alone cannot authorize
// deletion. This gate stays red until live-winner materialization is admitted.
func TestPruneQuantumCorruptLivePointerBeforeDeletion(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	options.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	for i := uint64(1); i <= 64; i++ {
		if err := store.CommitAt(i, []Mutation{{Key: []byte("live"), Value: bytes.Repeat([]byte("payload"), 256)}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	key, err := mvcckey.Encode([]byte("live"), 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(key, bytes.Repeat([]byte{255}, 256)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(32, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	var cursor *PruneCursor
	for q := 0; q < 128; q++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if stats.Pruned != 0 || stats.Complete {
			t.Fatalf("corrupt live winner authorized deletion: %+v", stats)
		}
		if err != nil {
			if !errors.Is(err, ErrMalformedRecord) {
				t.Fatalf("live pointer envelope was not validated: %v", err)
			}
			return
		}
		cursor = stats.Cursor
	}
	t.Fatal("corrupt live pointer was never validated")
}

// Public default compressed outer-leaf source must admit encoded and decoded
// frame work before expanding one page. This gate authorizes no deletion.
func TestPruneQuantumPersistedOuterLeafDiscovery(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	for timestamp := uint64(1); timestamp <= 1024; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("persisted"), Value: []byte("record")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(1024, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var cursor *PruneCursor
	for q := 0; q < 64; q++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if err != nil {
			t.Fatalf("persisted outer-leaf discovery quantum %d: %v", q, err)
		}
		if stats.Cursor == nil || stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Pruned != 0 || stats.Complete {
			t.Fatalf("unbounded or unaccounted persisted outer-leaf discovery: %+v", stats)
		}
		if stats.Visited != 0 {
			if stats.Visited >= 1024 || stats.WorkBytes < 4096 {
				t.Fatalf("unaccounted frame: %+v", stats)
			}
			return
		}
		cursor = stats.Cursor
	}
	t.Fatal("fixed quantum made no outer-leaf entry progress")
}

// Do not call DiscardFloor/GetAt during capture: those legacy reads would load
// the floor through an unbounded iterator and mask an incomplete quantum path.
func TestPruneQuantumColdFloorWinnerCapture(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	options.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	const history = 128
	for timestamp := uint64(1); timestamp <= history; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("cold"), Value: bytes.Repeat([]byte("payload"), 128)}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(history, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	if store.floorLoaded {
		t.Fatal("floor unexpectedly warm")
	}
	var cursor *PruneCursor
	for quantum := 0; quantum < 1024; quantum++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if err != nil {
			t.Fatalf("cold floor quantum %d: %v", quantum, err)
		}
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Pruned != 0 || stats.Complete || stats.Cursor == nil {
			t.Fatalf("unaccounted or unsafe floor capture: %+v", stats)
		}
		if store.floorLoaded {
			if store.discardFloor != history || stats.Cursor.floor != history || stats.Cursor.loadingFloor {
				t.Fatalf("wrong captured fixed floor: loaded=%d cursor=%+v", store.discardFloor, stats.Cursor)
			}
			if stats.Cursor.floorCandidate.winner.physical != nil {
				t.Fatal("floor payload/metadata retained after capture cleanup")
			}
			return
		}
		cursor = stats.Cursor
	}
	t.Fatal("fixed quantum never validated cold floor winner")
}

func TestPruneQuantumColdMalformedFloorWinnerBeforeDeletion(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	options.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	for timestamp := uint64(1); timestamp <= 32; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("cold"), Value: []byte("payload")}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AdvanceDiscardFloor(32, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	// Real index/value-log CRC remains valid; only the floor record is malformed.
	if err := db.Set(discardFloorKey, bytes.Repeat([]byte{0xff}, 9)); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store = New(db)
	var cursor *PruneCursor
	for quantum := 0; quantum < 512; quantum++ {
		stats, err := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if stats.Pruned != 0 || stats.Complete || store.floorLoaded {
			t.Fatalf("malformed floor trusted: %+v", stats)
		}
		if err != nil {
			if !errors.Is(err, ErrMalformedRecord) {
				t.Fatalf("wrong malformed floor error: %v", err)
			}
			return
		}
		if stats.Cursor == nil || stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
			t.Fatalf("unaccounted floor pass: %+v", stats)
		}
		cursor = stats.Cursor
	}
	t.Fatal("malformed live floor winner not rejected")
}

// A fresh Store shares the live ordinary cache without any checkpoint or
// iterator warmup. Its persisted floor winner can still be an encoded writer
// tail; capture must admit only that record, not flush unrelated lane work.
func TestPruneQuantumColdBufferedFloorWinnerCapture(t *testing.T) {
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	options.IndexOuterLeavesInValueLog = true
	options.ValueLog.PointerThreshold = 1
	db, err := treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writer := New(db)
	const history = 128
	for timestamp := uint64(1); timestamp <= history; timestamp++ {
		if err := writer.CommitAt(timestamp, []Mutation{{Key: []byte("buffered"), Value: bytes.Repeat([]byte("v"), 4096)}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.AdvanceDiscardFloor(history, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	store := New(db)
	var cursor *PruneCursor
	for quantum := 0; quantum < 1024; quantum++ {
		stats, err := store.PruneVersions(PruneOptions{Mode: CommitRelaxed, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if err != nil {
			t.Fatalf("buffered floor quantum %d: %v", quantum, err)
		}
		if stats.Pruned != 0 || stats.Complete || stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
			t.Fatalf("unsafe or unbounded capture: %+v", stats)
		}
		cursor = stats.Cursor
		if store.floorLoaded {
			if store.discardFloor != history || cursor == nil || cursor.floor != history || cursor.loadingFloor {
				t.Fatalf("wrong captured floor: %+v", cursor)
			}
			t.Logf("buffered floor captured in %d fixed quanta", quantum+1)
			return
		}
	}
	t.Fatal("bounded buffered floor capture made no eventual progress")
}

// Ordinary producer ordering must support total fixed-Q progress. Inline
// records isolate this contract from independently oversized frame decoding.
func TestPruneQuantumOrdinaryOrderedTotalProgressScaling(t *testing.T) {
	for _, order := range []string{"shuffled", "ascending"} {
		t.Run(order, func(t *testing.T) {
			var firstWork uint64
			for _, history := range []int{512, 1024} {
				t.Run(strconv.Itoa(history), func(t *testing.T) {
					db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					store := New(db)
					for i := 1; i <= history; i++ {
						timestamp := uint64(((i * 2053) % history) + 1)
						if order == "ascending" {
							timestamp = uint64(i)
						}
						if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("ordered"), Value: []byte("v")}}, CommitRelaxed); err != nil {
							t.Fatal(err)
						}
					}
					if err := store.AdvanceDiscardFloor(uint64(history), CommitRelaxed); err != nil {
						t.Fatal(err)
					}
					var cursor *PruneCursor
					var total, visited, deleted uint64
					complete := false
					for quantum := 0; quantum < history*16; quantum++ {
						stats, err := store.PruneVersions(PruneOptions{Mode: CommitDurable, BatchSize: 1, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
						if quantum%256 == 0 && stats.Cursor != nil {
							c := stats.Cursor
							t.Logf("phase q=%d work=%d bytes=%d action=%d prepDone=%v synced=%v loading=%v anchor=%v candidate=%v orderedDone=%v pruned=%d epoch=%d root=%d seq=%d", quantum, stats.WorkRecords, stats.WorkBytes, c.action, c.preparationDone, c.floorSynced, c.loadingFloor, c.haveAnchor, c.candidate.physical != nil, c.orderedDone, stats.Pruned, c.ordinal.Epoch, c.ordinal.BackendRoot, c.ordinal.BackendCommit)
						}
						total += stats.WorkRecords
						visited += stats.Visited
						deleted += stats.Pruned
						if err != nil {
							t.Fatalf("history=%d quantum=%d total=%d visits=%d deleted=%d: %v", history, quantum, total, visited, deleted, err)
						}
						if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
							t.Fatalf("unadmitted quantum: %+v", stats)
						}
						if stats.Complete {
							complete = true
							break
						}
						if stats.Cursor == nil {
							t.Fatal("unfinished pass omitted continuation")
						}
						cursor = stats.Cursor
					}
					t.Logf("history=%d total_work=%d visits=%d deleted=%d complete=%v", history, total, visited, deleted, complete)
					if !complete || deleted != uint64(history-1) {
						t.Fatalf("original history*16 ceiling failed: total=%d visits=%d deleted=%d", total, visited, deleted)
					}
					if total > uint64(history)*512 {
						t.Fatalf("state-sized repeated passes: total=%d history=%d", total, history)
					}
					if firstWork != 0 && total > firstWork*3 {
						t.Fatalf("doubling cardinality exceeded near-linear charged work: first=%d doubled=%d", firstWork, total)
					}
					firstWork = total
				})
			}
		})
	}
}

type pruneTerminalBoundaryDB struct {
	*treedb.DB
	store   *Store
	entered chan bool
}

func (db *pruneTerminalBoundaryDB) WaitMaintenanceTransfer(done <-chan struct{}) {
	unlocked := db.store.maintenanceMu.TryLock()
	if unlocked {
		db.store.maintenanceMu.Unlock()
	}
	readUnlocked := db.store.mu.TryLock()
	if readUnlocked {
		db.store.mu.Unlock()
	}
	db.entered <- unlocked && readUnlocked
	db.DB.WaitMaintenanceTransfer(done)
}

// Isolate the outer return/wait contract. The caching causal test separately
// drives the real grouped producer and acceptance/removal terminal signal.
func TestPruneQuantumPendingWaitOutsideStoreAndLifecycleAdmission(t *testing.T) {
	db, e := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if e != nil {
		t.Fatal(e)
	}
	closed := false
	defer func() {
		if !closed {
			db.Close()
		}
	}()
	observed := &pruneTerminalBoundaryDB{DB: db, entered: make(chan bool, 1)}
	store := newStore(observed)
	observed.store = store
	for ts := uint64(1); ts <= 3; ts++ {
		if e = store.CommitAt(ts, []Mutation{{Key: []byte("key"), Value: []byte("value")}}, CommitRelaxed); e != nil {
			t.Fatal(e)
		}
	}
	if e = store.AdvanceDiscardFloor(3, CommitRelaxed); e != nil {
		t.Fatal(e)
	}
	var cursor *PruneCursor
	for i := 0; i < 32; i++ {
		stats, e := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		if e != nil {
			t.Fatal(e)
		}
		cursor = stats.Cursor
		if cursor != nil && cursor.frontier != nil {
			break
		}
	}
	if cursor == nil || cursor.frontier == nil {
		t.Fatal("ordinary prune did not register frontier")
	}
	defer cursor.Close()
	transfer := cursor.frontier.BeginTransfer(cursor.ordinal)
	if transfer == nil {
		t.Fatal("exact owner refusal")
	}
	_, _, pending, done, e := cursor.frontier.CurrentWithCompletion(cursor.ordinal)
	if e != nil || !pending || done == nil {
		t.Fatalf("pending %v %v", pending, e)
	}
	result := make(chan PruneStats, 1)
	failure := make(chan error, 1)
	go func() {
		stats, e := store.PruneVersions(PruneOptions{WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
		result <- stats
		failure <- e
	}()
	select {
	case unlocked := <-observed.entered:
		if !unlocked {
			t.Fatal("outer wait retains Store guard")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("charged quantum did not reach exact wait")
	}
	select {
	case <-result:
		t.Fatal("wait returned before terminal")
	default:
	}
	closing := make(chan error, 1)
	go func() { closing <- db.Close() }()
	select {
	case e = <-closing:
		if e != nil {
			t.Fatal(e)
		}
		closed = true
	case <-time.After(10 * time.Second):
		t.Fatal("TreeDB closing cut blocked on wait admission")
	}
	select {
	case stats := <-result:
		if e = <-failure; e != nil {
			t.Fatal(e)
		}
		if stats.Complete || stats.Pruned != 0 || stats.Cursor == nil || stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
			t.Fatalf("terminal wait changed quantum: %+v", stats)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("closing did not wake exact owner")
	}
	if _, _, _, e = cursor.frontier.Current(cursor.ordinal); !errors.Is(e, iterator.ErrOrdinalScanStale) {
		t.Fatalf("closed owner revived: %v", e)
	}
}
