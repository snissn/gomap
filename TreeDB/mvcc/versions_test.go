package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func collectVersions(t testing.TB, it *VersionIterator) []Version {
	t.Helper()
	defer func() {
		if err := it.Close(); err != nil {
			t.Fatalf("Close iterator: %v", err)
		}
	}()
	var out []Version
	for it.Valid() {
		out = append(out, it.Entry())
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterate versions: %v", err)
	}
	return out
}

func versionLabels(versions []Version) []string {
	out := make([]string, len(versions))
	for i, version := range versions {
		out[i] = fmt.Sprintf("%s@%d:%d:%s", version.Key, version.Timestamp, version.State, version.Value)
	}
	return out
}

func requireVersionLabels(t testing.TB, got []Version, want ...string) {
	t.Helper()
	gotLabels := versionLabels(got)
	if fmt.Sprint(gotLabels) != fmt.Sprint(want) {
		t.Fatalf("versions=%v want %v", gotLabels, want)
	}
}

func commitHistory(t testing.TB, store *Store, key string, entries ...MutationAt) {
	t.Helper()
	for _, entry := range entries {
		if err := store.CommitAt(entry.Timestamp, []Mutation{{Key: []byte(key), Value: entry.Value, Delete: entry.Delete}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%s,%d): %v", key, entry.Timestamp, err)
		}
	}
}

type MutationAt struct {
	Timestamp uint64
	Value     []byte
	Delete    bool
}

func TestVersionIteratorDirectionalBoundsTimestampTombstonesAndSeek(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	commitHistory(t, store, "a", MutationAt{10, []byte("a10"), false}, MutationAt{20, nil, true}, MutationAt{30, []byte("a30"), false})
	commitHistory(t, store, "b", MutationAt{5, []byte("b5"), false}, MutationAt{25, []byte("b25"), false})
	commitHistory(t, store, "ba", MutationAt{15, []byte("ba15"), false})
	commitHistory(t, store, "c", MutationAt{1, []byte("c1"), false})

	it, err := store.IterateVersions(VersionIteratorOptions{LowerBound: []byte("a"), UpperBound: []byte("c")})
	if err != nil {
		t.Fatalf("IterateVersions forward: %v", err)
	}
	requireVersionLabels(t, collectVersions(t, it),
		"a@30:1:a30", "a@20:2:", "a@10:1:a10", "b@25:1:b25", "b@5:1:b5", "ba@15:1:ba15")

	it, err = store.IterateVersions(VersionIteratorOptions{Prefix: []byte("b"), ReadTimestamp: 20, Reverse: true})
	if err != nil {
		t.Fatalf("IterateVersions reverse: %v", err)
	}
	requireVersionLabels(t, collectVersions(t, it), "ba@15:1:ba15", "b@5:1:b5")
	stats := it.Stats()
	if stats.Visited != 3 || stats.Skipped != 1 || stats.Retained != 2 {
		t.Fatalf("stats=%+v", stats)
	}

	it, err = store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("IterateVersions seek: %v", err)
	}
	it.Seek([]byte("b"), 20)
	if !it.Valid() {
		t.Fatalf("Seek invalid: %v", it.Error())
	}
	entry := it.Entry()
	if string(entry.Key) != "b" || entry.Timestamp != 5 {
		t.Fatalf("Seek entry=%+v want b@5", entry)
	}
	entry.Key[0], entry.Value[0] = 'x', 'x'
	it.Seek([]byte("b"), 20)
	again := it.Entry()
	if string(again.Key) != "b" || string(again.Value) != "b5" {
		t.Fatalf("Entry did not own copies: %+v", again)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close seek iterator: %v", err)
	}

	it, err = store.IterateVersions(VersionIteratorOptions{Reverse: true})
	if err != nil {
		t.Fatalf("IterateVersions reverse seek: %v", err)
	}
	it.Seek([]byte("b"), 20)
	if !it.Valid() {
		t.Fatalf("Reverse Seek invalid: %v", it.Error())
	}
	entry = it.Entry()
	if string(entry.Key) != "b" || entry.Timestamp != 25 {
		t.Fatalf("Reverse Seek entry=%+v want b@25", entry)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close reverse seek iterator: %v", err)
	}
}

func TestVersionIteratorPinsSnapshotAcrossCommit(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	commitHistory(t, store, "k", MutationAt{10, []byte("old"), false})
	it, err := store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("IterateVersions: %v", err)
	}
	if err := store.CommitAt(20, []Mutation{{Key: []byte("k"), Value: []byte("new")}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt after snapshot: %v", err)
	}
	requireVersionLabels(t, collectVersions(t, it), "k@10:1:old")

	fresh, err := store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("fresh IterateVersions: %v", err)
	}
	requireVersionLabels(t, collectVersions(t, fresh), "k@20:1:new", "k@10:1:old")
}

func TestVersionIteratorCopiesOptionsAtOpen(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	commitHistory(t, store, "a/one", MutationAt{10, []byte("one"), false})
	commitHistory(t, store, "a/two", MutationAt{10, []byte("two"), false})
	commitHistory(t, store, "b/one", MutationAt{10, []byte("other"), false})
	prefix := []byte("a/")
	lower := []byte("a/one")
	upper := []byte("b")
	it, err := store.IterateVersions(VersionIteratorOptions{Prefix: prefix, LowerBound: lower, UpperBound: upper})
	if err != nil {
		t.Fatalf("IterateVersions: %v", err)
	}
	copy(prefix, "b/")
	copy(lower, "z/zzz")
	copy(upper, "a")
	requireVersionLabels(t, collectVersions(t, it), "a/one@10:1:one", "a/two@10:1:two")
}

func TestDiscardFloorPruneReopenAndIdempotence(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, treedb.DurabilityDurable)
	store := New(db)
	commitHistory(t, store, "value-anchor", MutationAt{10, []byte("v10"), false}, MutationAt{20, []byte("v20"), false}, MutationAt{30, []byte("v30"), false})
	commitHistory(t, store, "delete-anchor", MutationAt{10, []byte("live"), false}, MutationAt{20, nil, true}, MutationAt{30, []byte("reborn"), false})
	commitHistory(t, store, "only-delete", MutationAt{10, []byte("live"), false}, MutationAt{20, nil, true})

	if err := store.AdvanceDiscardFloor(25, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	if err := store.AdvanceDiscardFloor(24, CommitDurable); !errors.Is(err, ErrDiscardFloorRegression) {
		t.Fatalf("regression error=%v", err)
	}
	for _, timestamp := range []uint64{1, 25} {
		if _, err := store.GetAt([]byte("value-anchor"), timestamp); !errors.Is(err, ErrReadBeforeDiscardFloor) {
			t.Fatalf("GetAt(%d) error=%v", timestamp, err)
		}
		if _, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: timestamp}); !errors.Is(err, ErrReadBeforeDiscardFloor) {
			t.Fatalf("IterateVersions(%d) error=%v", timestamp, err)
		}
	}
	if err := store.CommitAt(25, []Mutation{{Key: []byte("late")}}, CommitRelaxed); !errors.Is(err, ErrVersionBelowDiscardFloor) {
		t.Fatalf("CommitAt at floor error=%v", err)
	}

	stats, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
	if err != nil {
		t.Fatalf("PruneVersions: %v", err)
	}
	if stats.Visited != 8 || stats.Skipped != 2 || stats.Pruned != 5 || stats.Retained != 3 || stats.Batches != 5 {
		t.Fatalf("prune stats=%+v", stats)
	}
	if stats.Visited != stats.Retained+stats.Pruned || stats.Skipped > stats.Retained {
		t.Fatalf("prune accounting relationship=%+v", stats)
	}
	if stats.PrunedBytes == 0 || stats.DeleteWriteBytes == 0 {
		t.Fatalf("prune byte accounting=%+v", stats)
	}
	requireResult(t, store, []byte("value-anchor"), 26, Present, 20, []byte("v20"))
	requireResult(t, store, []byte("value-anchor"), math.MaxUint64, Present, 30, []byte("v30"))
	requireResult(t, store, []byte("delete-anchor"), 26, Absent, 0, nil)
	requireResult(t, store, []byte("delete-anchor"), 30, Present, 30, []byte("reborn"))
	requireResult(t, store, []byte("only-delete"), 26, Absent, 0, nil)

	second, err := store.PruneVersions(PruneOptions{BatchSize: 2, Mode: CommitDurable})
	if err != nil || second.Pruned != 0 {
		t.Fatalf("idempotent prune stats=%+v err=%v", second, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db = openTestDB(t, dir, treedb.DurabilityDurable)
	defer db.Close()
	store = New(db)
	if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
		t.Fatalf("reopened floor=%d err=%v", floor, err)
	}
	if _, err := store.GetAt([]byte("value-anchor"), 25); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("reopened floor read error=%v", err)
	}
	requireResult(t, store, []byte("value-anchor"), 26, Present, 20, []byte("v20"))
}

func TestAdvanceDiscardFloorEqualDurableResyncs(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	counting := &writeSyncCountingDB{DB: db}
	store := newStore(counting)
	if err := store.AdvanceDiscardFloor(25, CommitRelaxed); err != nil {
		t.Fatalf("relaxed advance: %v", err)
	}
	if got := counting.writeSyncs.Load(); got != 0 {
		t.Fatalf("relaxed write syncs=%d want 0", got)
	}
	if err := store.AdvanceDiscardFloor(25, CommitDurable); err != nil {
		t.Fatalf("equal durable advance: %v", err)
	}
	if got := counting.writeSyncs.Load(); got != 1 {
		t.Fatalf("equal durable write syncs=%d want 1", got)
	}
}

func TestAdvanceDiscardFloorCommitAmbiguityInvalidatesCache(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	injected := errors.New("post-commit acknowledgement failure")
	ambiguous := &floorAmbiguousDB{DB: db, failWriteSyncAt: 2, err: injected}
	store := newStore(ambiguous)
	if err := store.AdvanceDiscardFloor(25, CommitDurable); err != nil {
		t.Fatalf("initial advance: %v", err)
	}
	if err := store.AdvanceDiscardFloor(30, CommitDurable); !errors.Is(err, injected) {
		t.Fatalf("ambiguous advance error=%v", err)
	}
	if floor, err := store.DiscardFloor(); err != nil || floor != 30 {
		t.Fatalf("reloaded floor=%d err=%v want 30", floor, err)
	}
	if _, err := store.GetAt([]byte("k"), 26); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("read under ambiguously committed floor error=%v", err)
	}
}

func TestDiscardFloorLoadPreservesIteratorCloseError(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	injected := errors.New("floor iterator close failure")
	closeDB := &floorCloseErrorDB{DB: db, err: injected}
	closeDB.fail.Store(true)
	store := newStore(closeDB)
	if _, err := store.DiscardFloor(); !errors.Is(err, ErrStorage) || !errors.Is(err, injected) {
		t.Fatalf("DiscardFloor close error=%v", err)
	}
	closeDB.fail.Store(false)
	if floor, err := store.DiscardFloor(); err != nil || floor != 0 {
		t.Fatalf("DiscardFloor retry floor=%d err=%v", floor, err)
	}
}

func TestDiscardFloorValueReadErrorIsRetryableStorageError(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	injected := errors.New("floor value read failure")
	failing := &floorValueErrorDB{DB: db, err: injected}
	store := newStore(failing)
	store.mu.Lock()
	err := store.persistDiscardFloorLocked(25, CommitDurable)
	store.mu.Unlock()
	if err != nil {
		t.Fatalf("persist discard floor fixture: %v", err)
	}

	failing.fail.Store(true)
	_, err = store.DiscardFloor()
	assertStorageValueError(t, err, injected, "read discard floor value")
	if store.floorLoaded {
		t.Fatal("failed floor value read populated cache")
	}

	failing.fail.Store(false)
	if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
		t.Fatalf("retry floor=%d err=%v want 25", floor, err)
	}
}

func TestVersionValueReadErrorsRemainStorageErrors(t *testing.T) {
	t.Run("version iterator", func(t *testing.T) {
		db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
		defer db.Close()
		injected := errors.New("version value read failure")
		store := newStore(&snapshotValueErrorDB{DB: db, err: injected})
		if err := store.CommitAt(10, []Mutation{{Key: []byte("k"), Value: []byte("value")}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt: %v", err)
		}
		it, err := store.IterateVersions(VersionIteratorOptions{})
		if err != nil {
			t.Fatalf("IterateVersions: %v", err)
		}
		if it.Valid() {
			t.Fatal("iterator remained valid after value read failure")
		}
		assertStorageValueError(t, it.Error(), injected, "read version iterator value")
		if err := it.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("prune", func(t *testing.T) {
		db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
		defer db.Close()
		injected := errors.New("prune value read failure")
		store := newStore(&snapshotValueErrorDB{DB: db, err: injected})
		commitHistory(t, store, "k", MutationAt{10, []byte("old"), false}, MutationAt{20, []byte("new"), false})
		if err := store.AdvanceDiscardFloor(15, CommitDurable); err != nil {
			t.Fatalf("AdvanceDiscardFloor: %v", err)
		}
		_, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
		assertStorageValueError(t, err, injected, "read prune iterator value")
	})
}

func assertStorageValueError(t testing.TB, err, injected error, operation string) {
	t.Helper()
	if !errors.Is(err, ErrStorage) || !errors.Is(err, injected) {
		t.Fatalf("error=%v want ErrStorage and injected cause", err)
	}
	if errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("error=%v was misclassified as ErrMalformedRecord", err)
	}
	if !strings.Contains(err.Error(), operation) {
		t.Fatalf("error=%v missing operation %q", err, operation)
	}
}

func TestPruneInterruptedBatchRestartCannotResurrect(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, treedb.DurabilityDurable)
	store := New(db)
	commitHistory(t, store, "k", MutationAt{10, []byte("old"), false}, MutationAt{20, nil, true}, MutationAt{30, []byte("new"), false})
	if err := store.AdvanceDiscardFloor(25, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}

	injected := errors.New("interrupt prune")
	failing := &pruneFailureDB{DB: db, failWriteSyncAt: 3, err: injected}
	failedStore := newStore(failing)
	if _, err := failedStore.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable}); !errors.Is(err, injected) {
		t.Fatalf("interrupted prune error=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after interrupted prune: %v", err)
	}

	db = openTestDB(t, dir, treedb.DurabilityDurable)
	defer db.Close()
	store = New(db)
	if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
		t.Fatalf("recovered floor=%d err=%v", floor, err)
	}
	if _, err := store.GetAt([]byte("k"), 25); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("recovered forbidden read error=%v", err)
	}
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable}); err != nil {
		t.Fatalf("restart prune: %v", err)
	}
	requireResult(t, store, []byte("k"), 26, Absent, 0, nil)
	requireResult(t, store, []byte("k"), 30, Present, 30, []byte("new"))
}

func TestPruneDurableProcessCrashAfterDeleteBatch(t *testing.T) {
	const childEnv = "TREEDB_MVCC_PRUNE_CRASH_CHILD"
	const dirEnv = "TREEDB_MVCC_PRUNE_CRASH_DIR"
	if os.Getenv(childEnv) == "1" {
		db := openTestDB(t, os.Getenv(dirEnv), treedb.DurabilityDurable)
		crashDB := &pruneCrashDB{DB: db, crashAfterWriteSync: 2}
		_, _ = newStore(crashDB).PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
		os.Exit(91)
	}

	dir := t.TempDir()
	db := openTestDB(t, dir, treedb.DurabilityDurable)
	store := New(db)
	commitHistory(t, store, "k", MutationAt{10, []byte("old"), false}, MutationAt{20, nil, true}, MutationAt{30, []byte("new"), false})
	if err := store.AdvanceDiscardFloor(25, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("prepare Close: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestPruneDurableProcessCrashAfterDeleteBatch$")
	cmd.Env = append(os.Environ(), childEnv+"=1", dirEnv+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}

	db = openTestDB(t, dir, treedb.DurabilityDurable)
	defer db.Close()
	store = New(db)
	if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
		t.Fatalf("crash-recovered floor=%d err=%v", floor, err)
	}
	if _, err := store.GetAt([]byte("k"), 25); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("crash-recovered forbidden read error=%v", err)
	}
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable}); err != nil {
		t.Fatalf("resume prune: %v", err)
	}
	requireResult(t, store, []byte("k"), 26, Absent, 0, nil)
	requireResult(t, store, []byte("k"), 30, Present, 30, []byte("new"))
}

func TestPruneConcurrentSnapshotReaders(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	for timestamp := uint64(1); timestamp <= 100; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("k"), Value: []byte(fmt.Sprint(timestamp))}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%d): %v", timestamp, err)
		}
	}
	it, err := store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("IterateVersions: %v", err)
	}
	if err := store.AdvanceDiscardFloor(80, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	var wg sync.WaitGroup
	var readCount atomic.Uint64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for it.Valid() {
			entry := it.Entry()
			if string(entry.Value) != fmt.Sprint(entry.Timestamp) {
				t.Errorf("snapshot entry=%+v", entry)
				return
			}
			readCount.Add(1)
			it.Next()
		}
		if err := it.Error(); err != nil {
			t.Errorf("snapshot iterator: %v", err)
		}
	}()
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 7, Mode: CommitDurable}); err != nil {
		t.Fatalf("PruneVersions: %v", err)
	}
	wg.Wait()
	if err := it.Close(); err != nil {
		t.Fatalf("Close snapshot iterator: %v", err)
	}
	if readCount.Load() != 100 {
		t.Fatalf("snapshot read count=%d want 100", readCount.Load())
	}
}

func TestPruneAfterSnapshotCaptureDoesNotBlockForegroundOperations(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	pausingDB := &pruneSnapshotPauseDB{
		DB:      db,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	store := newStore(pausingDB)
	for timestamp := uint64(1); timestamp <= 8; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("k"), Value: []byte(fmt.Sprint(timestamp))}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%d): %v", timestamp, err)
		}
	}

	pinned, err := store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("open pinned iterator: %v", err)
	}
	defer func() { _ = pinned.Close() }()
	if err := store.AdvanceDiscardFloor(5, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}

	type pruneResult struct {
		stats PruneStats
		err   error
	}
	pausingDB.pauseNextReverse.Store(true)
	pruneDone := make(chan pruneResult, 1)
	go func() {
		stats, err := store.PruneVersions(PruneOptions{BatchSize: 2, Mode: CommitDurable})
		pruneDone <- pruneResult{stats: stats, err: err}
	}()

	select {
	case <-pausingDB.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not pause after snapshot capture")
	}
	if !store.mu.TryLock() {
		t.Fatal("prune retained floor lock after snapshot capture")
	}
	store.mu.Unlock()
	if store.maintenanceMu.TryLock() {
		store.maintenanceMu.Unlock()
		t.Fatal("prune released maintenance lock before completion")
	}
	var releaseOnce sync.Once
	releasePrune := func() { releaseOnce.Do(func() { close(pausingDB.release) }) }
	defer releasePrune()

	advanceStarted := make(chan struct{})
	advanceDone := make(chan error, 1)
	go func() {
		close(advanceStarted)
		advanceDone <- store.AdvanceDiscardFloor(6, CommitDurable)
	}()
	<-advanceStarted

	requireMVCCOperationCompletes(t, "GetAt while prune paused", func() error {
		result, err := store.GetAt([]byte("k"), 8)
		if err != nil {
			return err
		}
		if result.State != Present || result.Timestamp != 8 || string(result.Value) != "8" {
			return fmt.Errorf("GetAt result=%+v want k@8", result)
		}
		return nil
	})
	requireMVCCOperationCompletes(t, "CommitAt while prune paused", func() error {
		return store.CommitAt(9, []Mutation{{Key: []byte("k"), Value: []byte("9")}}, CommitRelaxed)
	})
	requireMVCCOperationCompletes(t, "IterateVersions acquisition while prune paused", func() error {
		it, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 9})
		if err != nil {
			return err
		}
		return it.Close()
	})
	select {
	case err := <-advanceDone:
		t.Fatalf("floor advance completed while prune held maintenance lock: %v", err)
	default:
	}

	releasePrune()
	var pruned pruneResult
	select {
	case pruned = <-pruneDone:
	case <-time.After(10 * time.Second):
		t.Fatal("prune did not complete after release")
	}
	if pruned.err != nil {
		t.Fatalf("PruneVersions: %v", pruned.err)
	}
	if pruned.stats.Visited != 8 || pruned.stats.Skipped != 3 || pruned.stats.Retained != 4 || pruned.stats.Pruned != 4 {
		t.Fatalf("prune stats=%+v", pruned.stats)
	}
	select {
	case err := <-advanceDone:
		if err != nil {
			t.Fatalf("serialized AdvanceDiscardFloor: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("floor advance did not complete after prune")
	}
	if floor, err := store.DiscardFloor(); err != nil || floor != 6 {
		t.Fatalf("final discard floor=%d err=%v want 6", floor, err)
	}

	requireVersionLabels(t, collectVersions(t, pinned),
		"k@8:1:8", "k@7:1:7", "k@6:1:6", "k@5:1:5",
		"k@4:1:4", "k@3:1:3", "k@2:1:2", "k@1:1:1")
	fresh, err := store.IterateVersions(VersionIteratorOptions{})
	if err != nil {
		t.Fatalf("open fresh iterator: %v", err)
	}
	requireVersionLabels(t, collectVersions(t, fresh),
		"k@9:1:9", "k@8:1:8", "k@7:1:7", "k@6:1:6", "k@5:1:5")
}

func requireMVCCOperationCompletes(t testing.TB, name string, operation func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- operation() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked", name)
	}
}

func TestFloorAdvanceRaceNeverServesPrunedHistoricalRead(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	for timestamp := uint64(1); timestamp <= 100; timestamp++ {
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte("k"), Value: []byte(fmt.Sprint(timestamp))}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%d): %v", timestamp, err)
		}
	}
	var stop atomic.Bool
	errCh := make(chan error, 4)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				got, err := store.GetAt([]byte("k"), 25)
				if errors.Is(err, ErrReadBeforeDiscardFloor) {
					continue
				}
				if err != nil {
					errCh <- err
					return
				}
				if got.State != Present || got.Timestamp != 25 || string(got.Value) != "25" {
					errCh <- fmt.Errorf("historical read=%+v", got)
					return
				}
			}
		}()
	}
	if err := store.AdvanceDiscardFloor(50, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 3, Mode: CommitDurable}); err != nil {
		t.Fatalf("PruneVersions: %v", err)
	}
	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if _, err := store.GetAt([]byte("k"), 25); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("final historical read error=%v", err)
	}
	requireResult(t, store, []byte("k"), 100, Present, 100, []byte("100"))
}

func TestPruneRandomizedHistoryOracle(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	type modelVersion struct {
		timestamp uint64
		value     []byte
		deleted   bool
	}
	model := make(map[string][]modelVersion)
	rng := rand.New(rand.NewSource(3669))
	for timestamp := uint64(1); timestamp <= 100; timestamp++ {
		key := fmt.Sprintf("key-%02d", rng.Intn(24))
		deleted := rng.Intn(5) == 0
		value := []byte(fmt.Sprintf("v-%03d", timestamp))
		if err := store.CommitAt(timestamp, []Mutation{{Key: []byte(key), Value: value, Delete: deleted}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%d): %v", timestamp, err)
		}
		model[key] = append(model[key], modelVersion{timestamp: timestamp, value: append([]byte(nil), value...), deleted: deleted})
	}
	const floor = uint64(60)
	if err := store.AdvanceDiscardFloor(floor, CommitDurable); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 9, Mode: CommitDurable}); err != nil {
		t.Fatalf("PruneVersions: %v", err)
	}
	for key, history := range model {
		for probe := 0; probe < 20; probe++ {
			readTimestamp := floor + 1 + uint64(rng.Intn(50))
			want := Result{State: Absent}
			for index := len(history) - 1; index >= 0; index-- {
				version := history[index]
				if version.timestamp > readTimestamp {
					continue
				}
				if version.deleted {
					// A tombstone at/below the discard floor and all of its older
					// history may be collapsed to logical absence.
					if version.timestamp > floor {
						want = Result{State: Tombstone, Timestamp: version.timestamp}
					}
				} else {
					want = Result{State: Present, Timestamp: version.timestamp, Value: version.value}
				}
				break
			}
			got, err := store.GetAt([]byte(key), readTimestamp)
			if err != nil {
				t.Fatalf("GetAt(%s,%d): %v", key, readTimestamp, err)
			}
			if got.State != want.State || got.Timestamp != want.Timestamp || !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("GetAt(%s,%d)=%+v want %+v", key, readTimestamp, got, want)
			}
		}
	}
}

type pruneFailureDB struct {
	*treedb.DB
	writes          atomic.Int64
	failWriteSyncAt int64
	err             error
}

func (db *pruneFailureDB) NewBatchWithSize(size int) treedb.Batch {
	return &pruneFailureBatch{Batch: db.DB.NewBatchWithSize(size), owner: db}
}

type pruneFailureBatch struct {
	treedb.Batch
	owner *pruneFailureDB
}

func (b *pruneFailureBatch) WriteSync() error {
	call := b.owner.writes.Add(1)
	if call == b.owner.failWriteSyncAt {
		return b.owner.err
	}
	return b.Batch.WriteSync()
}

var _ treeDB = (*pruneFailureDB)(nil)

type pruneSnapshotPauseDB struct {
	*treedb.DB
	pauseNextReverse atomic.Bool
	entered          chan struct{}
	release          chan struct{}
}

func (db *pruneSnapshotPauseDB) AcquireSnapshot() treedb.Snapshot {
	snapshot := db.DB.AcquireSnapshot()
	if snapshot == nil {
		return nil
	}
	return &pruneSnapshotPause{Snapshot: snapshot, owner: db}
}

type pruneSnapshotPause struct {
	treedb.Snapshot
	owner *pruneSnapshotPauseDB
}

func (snapshot *pruneSnapshotPause) ReverseIterator(start, end []byte) (treedb.Iterator, error) {
	if snapshot.owner.pauseNextReverse.CompareAndSwap(true, false) {
		close(snapshot.owner.entered)
		<-snapshot.owner.release
	}
	return snapshot.Snapshot.ReverseIterator(start, end)
}

var _ treeDB = (*pruneSnapshotPauseDB)(nil)
var _ snapshotDB = (*pruneSnapshotPauseDB)(nil)

type writeSyncCountingDB struct {
	*treedb.DB
	writeSyncs atomic.Int64
}

func (db *writeSyncCountingDB) NewBatchWithSize(size int) treedb.Batch {
	return &writeSyncCountingBatch{Batch: db.DB.NewBatchWithSize(size), owner: db}
}

type writeSyncCountingBatch struct {
	treedb.Batch
	owner *writeSyncCountingDB
}

func (b *writeSyncCountingBatch) WriteSync() error {
	b.owner.writeSyncs.Add(1)
	return b.Batch.WriteSync()
}

var _ treeDB = (*writeSyncCountingDB)(nil)

type floorAmbiguousDB struct {
	*treedb.DB
	writes          atomic.Int64
	failWriteSyncAt int64
	err             error
}

func (db *floorAmbiguousDB) NewBatchWithSize(size int) treedb.Batch {
	return &floorAmbiguousBatch{Batch: db.DB.NewBatchWithSize(size), owner: db}
}

type floorAmbiguousBatch struct {
	treedb.Batch
	owner *floorAmbiguousDB
}

func (b *floorAmbiguousBatch) WriteSync() error {
	if err := b.Batch.WriteSync(); err != nil {
		return err
	}
	if b.owner.writes.Add(1) == b.owner.failWriteSyncAt {
		return b.owner.err
	}
	return nil
}

var _ treeDB = (*floorAmbiguousDB)(nil)

type floorCloseErrorDB struct {
	*treedb.DB
	fail atomic.Bool
	err  error
}

func (db *floorCloseErrorDB) Iterator(start, end []byte) (treedb.Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &floorCloseErrorIterator{Iterator: it, owner: db}, nil
}

type floorCloseErrorIterator struct {
	treedb.Iterator
	owner *floorCloseErrorDB
}

func (it *floorCloseErrorIterator) Close() error {
	err := it.Iterator.Close()
	if it.owner.fail.Load() {
		return errors.Join(err, it.owner.err)
	}
	return err
}

var _ treeDB = (*floorCloseErrorDB)(nil)

type floorValueErrorDB struct {
	*treedb.DB
	fail atomic.Bool
	err  error
}

func (db *floorValueErrorDB) Iterator(start, end []byte) (treedb.Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &floorValueErrorIterator{Iterator: it, owner: db}, nil
}

type floorValueErrorIterator struct {
	treedb.Iterator
	owner     *floorValueErrorDB
	valueRead bool
}

func (it *floorValueErrorIterator) Value() []byte {
	if it.owner.fail.Load() {
		it.valueRead = true
		return nil
	}
	return it.Iterator.Value()
}

func (it *floorValueErrorIterator) Error() error {
	if it.valueRead {
		return errors.Join(it.Iterator.Error(), it.owner.err)
	}
	return it.Iterator.Error()
}

var _ treeDB = (*floorValueErrorDB)(nil)

type snapshotValueErrorDB struct {
	*treedb.DB
	err error
}

func (db *snapshotValueErrorDB) AcquireSnapshot() treedb.Snapshot {
	snapshot := db.DB.AcquireSnapshot()
	if snapshot == nil {
		return nil
	}
	return &snapshotValueErrorSnapshot{Snapshot: snapshot, err: db.err}
}

type snapshotValueErrorSnapshot struct {
	treedb.Snapshot
	err error
}

func (snapshot *snapshotValueErrorSnapshot) Iterator(start, end []byte) (treedb.Iterator, error) {
	it, err := snapshot.Snapshot.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &valueErrorIterator{Iterator: it, err: snapshot.err}, nil
}

func (snapshot *snapshotValueErrorSnapshot) ReverseIterator(start, end []byte) (treedb.Iterator, error) {
	it, err := snapshot.Snapshot.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &valueErrorIterator{Iterator: it, err: snapshot.err}, nil
}

type valueErrorIterator struct {
	treedb.Iterator
	err       error
	valueRead bool
}

func (it *valueErrorIterator) Value() []byte {
	it.valueRead = true
	return nil
}

func (it *valueErrorIterator) Error() error {
	if it.valueRead {
		return errors.Join(it.Iterator.Error(), it.err)
	}
	return it.Iterator.Error()
}

var _ treeDB = (*snapshotValueErrorDB)(nil)
var _ snapshotDB = (*snapshotValueErrorDB)(nil)

type pruneCrashDB struct {
	*treedb.DB
	writes              atomic.Int64
	crashAfterWriteSync int64
}

func (db *pruneCrashDB) NewBatchWithSize(size int) treedb.Batch {
	return &pruneCrashBatch{Batch: db.DB.NewBatchWithSize(size), owner: db}
}

type pruneCrashBatch struct {
	treedb.Batch
	owner *pruneCrashDB
}

func (b *pruneCrashBatch) WriteSync() error {
	if err := b.Batch.WriteSync(); err != nil {
		return err
	}
	if b.owner.writes.Add(1) == b.owner.crashAfterWriteSync {
		os.Exit(0)
	}
	return nil
}

var _ treeDB = (*pruneCrashDB)(nil)

func TestVersionIteratorEmptyRange(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	it, err := store.IterateVersions(VersionIteratorOptions{LowerBound: []byte("z"), UpperBound: []byte("a")})
	if err != nil {
		t.Fatalf("empty range: %v", err)
	}
	if it.Valid() || it.Error() != nil || len(collectVersions(t, it)) != 0 {
		t.Fatal("empty range produced a record or error")
	}
	if err := store.CommitAt(1, []Mutation{{Key: []byte("present"), Value: []byte("value")}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt: %v", err)
	}
	it, err = store.IterateVersions(VersionIteratorOptions{UpperBound: []byte{}})
	if err != nil {
		t.Fatalf("empty upper bound: %v", err)
	}
	if versions := collectVersions(t, it); len(versions) != 0 {
		t.Fatalf("non-nil empty upper bound expanded to %v", versionLabels(versions))
	}
}

func TestVersionIteratorArbitraryBinaryLogicalKeys(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	key := []byte{'a', 0, 'b'}
	if err := store.CommitAt(9, []Mutation{{Key: key, Value: []byte{0, 1}}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt: %v", err)
	}
	it, err := store.IterateVersions(VersionIteratorOptions{Prefix: []byte{'a', 0}})
	if err != nil {
		t.Fatalf("IterateVersions: %v", err)
	}
	versions := collectVersions(t, it)
	if len(versions) != 1 || !bytes.Equal(versions[0].Key, key) || !bytes.Equal(versions[0].Value, []byte{0, 1}) {
		t.Fatalf("versions=%+v", versions)
	}
}
