package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func openTestDB(t testing.TB, dir string, durability treedb.DurabilityMode) *treedb.DB {
	t.Helper()
	db, err := treedb.Open(treedb.Options{
		Dir:                          dir,
		Durability:                   durability,
		CommandWAL:                   durability != treedb.DurabilityWALOffRelaxed,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func requireResult(t testing.TB, store *Store, key []byte, readTimestamp uint64, state ReadState, version uint64, value []byte) {
	t.Helper()
	got, err := store.GetAt(key, readTimestamp)
	if err != nil {
		t.Fatalf("GetAt(%q,%d): %v", key, readTimestamp, err)
	}
	if got.State != state || got.Timestamp != version || !bytes.Equal(got.Value, value) {
		t.Fatalf("GetAt(%q,%d) = {state:%d ts:%d value:%q}, want {state:%d ts:%d value:%q}", key, readTimestamp, got.State, got.Timestamp, got.Value, state, version, value)
	}
}

func TestCommitAtGetAtGoldenHistories(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	key := []byte{'k', 0, 'x'}

	if err := store.CommitAt(10, []Mutation{{Key: key, Value: []byte("ten")}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt(10): %v", err)
	}
	if err := store.CommitAt(30, []Mutation{{Key: key, Value: []byte("thirty")}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt(30): %v", err)
	}
	if err := store.CommitAt(40, []Mutation{{Key: key, Delete: true}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt(40 tombstone): %v", err)
	}
	if err := store.CommitAt(50, []Mutation{{Key: key, Value: nil}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt(50 empty): %v", err)
	}

	requireResult(t, store, key, 1, Absent, 0, nil)
	requireResult(t, store, key, 10, Present, 10, []byte("ten"))
	requireResult(t, store, key, 20, Present, 10, []byte("ten"))
	requireResult(t, store, key, 30, Present, 30, []byte("thirty"))
	requireResult(t, store, key, 40, Tombstone, 40, nil)
	requireResult(t, store, key, 49, Tombstone, 40, nil)
	requireResult(t, store, key, math.MaxUint64, Present, 50, []byte{})

	// Separate commits at the same logical key/timestamp address one physical
	// version. The later successful atomic commit deterministically replaces it.
	if err := store.CommitAt(30, []Mutation{{Key: key, Value: []byte("thirty-replaced")}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt duplicate timestamp: %v", err)
	}
	requireResult(t, store, key, 30, Present, 30, []byte("thirty-replaced"))
}

func TestCommitAtMultiKeyAtomicAndDuplicatePolicy(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)

	mutations := []Mutation{
		{Key: nil, Value: []byte("empty")},
		{Key: []byte("b"), Value: []byte("bee")},
		{Key: []byte("c"), Delete: true},
	}
	if err := store.CommitAt(7, mutations, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt: %v", err)
	}
	requireResult(t, store, []byte{}, 7, Present, 7, []byte("empty"))
	requireResult(t, store, []byte("b"), 7, Present, 7, []byte("bee"))
	requireResult(t, store, []byte("c"), 7, Tombstone, 7, nil)

	err := store.CommitAt(8, []Mutation{
		{Key: nil, Value: []byte("first")},
		{Key: []byte{}, Delete: true},
	}, CommitRelaxed)
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate error = %v, want ErrDuplicateKey", err)
	}
	requireResult(t, store, nil, 8, Present, 7, []byte("empty"))
}

func TestCommitAtValidationPrecedesStorageWrite(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	spy := &batchSpyDB{DB: db}
	store := newStore(spy)

	if err := store.CommitAt(0, []Mutation{{Key: []byte("k")}}, CommitRelaxed); !errors.Is(err, ErrZeroTimestamp) {
		t.Fatalf("zero timestamp error = %v", err)
	}
	if err := store.CommitAt(1, []Mutation{{Key: []byte("k")}}, CommitMode(99)); !errors.Is(err, ErrInvalidCommitMode) {
		t.Fatalf("invalid mode error = %v", err)
	}
	tooLarge := bytes.Repeat([]byte{0}, mvcckey.MaxEncodedKeySize)
	if err := store.CommitAt(1, []Mutation{{Key: tooLarge}}, CommitRelaxed); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("oversized key error = %v", err)
	}
	if got := spy.created.Load(); got != 0 {
		t.Fatalf("batches created after validation failures = %d, want 0", got)
	}
}

func TestCommitAtInjectedFailureIsAllOrNone(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_commit_%t", afterCommit), func(t *testing.T) {
			dir := t.TempDir()
			db := openTestDB(t, dir, treedb.DurabilityDurable)
			injected := errors.New("injected batch failure")
			writer := newStore(&writeFailureDB{DB: db, afterCommit: afterCommit, err: injected})
			reader := New(db)
			err := writer.CommitAt(11, []Mutation{
				{Key: []byte("a"), Value: []byte("A")},
				{Key: []byte("b"), Value: []byte("B")},
			}, CommitRelaxed)
			if !errors.Is(err, injected) {
				t.Fatalf("CommitAt error = %v, want injected", err)
			}
			want := Absent
			if afterCommit {
				want = Present
			}
			for _, key := range []string{"a", "b"} {
				got, getErr := reader.GetAt([]byte(key), 11)
				if getErr != nil {
					t.Fatalf("GetAt(%q): %v", key, getErr)
				}
				if got.State != want {
					t.Fatalf("GetAt(%q).State=%d want %d", key, got.State, want)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close after injected failure: %v", err)
			}
			db = openTestDB(t, dir, treedb.DurabilityDurable)
			defer db.Close()
			reader = New(db)
			for _, key := range []string{"a", "b"} {
				got, getErr := reader.GetAt([]byte(key), 11)
				if getErr != nil {
					t.Fatalf("reopened GetAt(%q): %v", key, getErr)
				}
				if got.State != want {
					t.Fatalf("reopened GetAt(%q).State=%d want %d", key, got.State, want)
				}
			}
		})
	}
}

func TestCommitAtInjectedStagingFailurePublishesNothing(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	injected := errors.New("injected staging failure")
	writer := newStore(&stageFailureDB{DB: db, failAt: 1, err: injected})
	err := writer.CommitAt(12, []Mutation{
		{Key: []byte("a"), Value: []byte("A")},
		{Key: []byte("b"), Value: []byte("B")},
	}, CommitRelaxed)
	if !errors.Is(err, injected) {
		t.Fatalf("CommitAt error=%v want injected", err)
	}
	reader := New(db)
	requireResult(t, reader, []byte("a"), 12, Absent, 0, nil)
	requireResult(t, reader, []byte("b"), 12, Absent, 0, nil)
}

func TestGetAtMalformedAndStorageErrors(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	store := New(db)
	physical, err := mvcckey.Encode([]byte("bad-value"), 9)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(physical, []byte{recordTombstoneV1, 0xff}); err != nil {
		t.Fatalf("raw malformed Set: %v", err)
	}
	if _, err := store.GetAt([]byte("bad-value"), 9); !errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("GetAt malformed value error=%v want ErrMalformedRecord", err)
	}
	physical, err = mvcckey.Encode([]byte("bad-key"), 9)
	if err != nil {
		t.Fatal(err)
	}
	physical = append(physical, 0x00)
	if err := db.Set(physical, []byte{recordValueV1, 'x'}); err != nil {
		t.Fatalf("raw malformed key Set: %v", err)
	}
	if _, err := store.GetAt([]byte("bad-key"), 9); !errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("GetAt malformed key error=%v want ErrMalformedRecord", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := store.GetAt([]byte("bad-value"), 9); !errors.Is(err, treedb.ErrClosed) {
		t.Fatalf("GetAt closed error=%v want ErrClosed", err)
	}
}

func TestCommitAtDurabilityModesAndReopen(t *testing.T) {
	t.Run("durable", func(t *testing.T) {
		dir := t.TempDir()
		db := openTestDB(t, dir, treedb.DurabilityDurable)
		if err := New(db).CommitAt(21, []Mutation{{Key: []byte("k"), Value: []byte("durable")}}, CommitDurable); err != nil {
			t.Fatalf("CommitAt durable: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		db = openTestDB(t, dir, treedb.DurabilityDurable)
		defer db.Close()
		requireResult(t, New(db), []byte("k"), 21, Present, 21, []byte("durable"))
	})

	for _, durability := range []treedb.DurabilityMode{treedb.DurabilityWALOnRelaxed, treedb.DurabilityWALOffRelaxed} {
		t.Run("relaxed_"+strconv.Itoa(int(durability)), func(t *testing.T) {
			dir := t.TempDir()
			db := openTestDB(t, dir, durability)
			store := New(db)
			if err := store.CommitAt(22, []Mutation{{Key: []byte("k"), Value: []byte("relaxed")}}, CommitDurable); !errors.Is(err, ErrDurabilityUnavailable) {
				t.Fatalf("durable request error=%v want ErrDurabilityUnavailable", err)
			}
			requireResult(t, store, []byte("k"), 22, Absent, 0, nil)
			if err := store.CommitAt(22, []Mutation{{Key: []byte("k"), Value: []byte("relaxed")}}, CommitRelaxed); err != nil {
				t.Fatalf("CommitAt relaxed: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db = openTestDB(t, dir, durability)
			defer db.Close()
			requireResult(t, New(db), []byte("k"), 22, Present, 22, []byte("relaxed"))
		})
	}
}

func TestCommitAtDurableProcessCrashRecovery(t *testing.T) {
	const childEnv = "TREEDB_MVCC_CRASH_CHILD"
	if dir := os.Getenv(childEnv); dir != "" {
		db := openTestDB(t, dir, treedb.DurabilityDurable)
		if err := New(db).CommitAt(31, []Mutation{
			{Key: []byte("a"), Value: []byte("A")},
			{Key: []byte("b"), Delete: true},
		}, CommitDurable); err != nil {
			t.Fatalf("child CommitAt: %v", err)
		}
		// Deliberately skip Close to exercise WAL recovery after process death.
		os.Exit(0)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommitAtDurableProcessCrashRecovery$")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "wal", "*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("discover crash WAL segments: files=%v err=%v", segments, err)
	}
	tail, err := os.OpenFile(segments[len(segments)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open crash WAL tail: %v", err)
	}
	if _, err := tail.Write([]byte{0x7f, 0x01, 0x02}); err != nil {
		_ = tail.Close()
		t.Fatalf("append truncated crash WAL tail: %v", err)
	}
	if err := tail.Close(); err != nil {
		t.Fatalf("close crash WAL tail: %v", err)
	}
	db := openTestDB(t, dir, treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	requireResult(t, store, []byte("a"), 31, Present, 31, []byte("A"))
	requireResult(t, store, []byte("b"), 31, Tombstone, 31, nil)
}

func TestGetAtRandomizedOracle(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	rng := rand.New(rand.NewSource(3672))
	type oracleVersion struct {
		timestamp uint64
		value     []byte
		deleted   bool
	}
	oracle := make(map[string]map[uint64]oracleVersion)
	keys := [][]byte{nil, []byte("a"), []byte{'a', 0}, []byte("z")}

	for step := 0; step < 500; step++ {
		key := keys[rng.Intn(len(keys))]
		timestamp := uint64(1 + rng.Intn(100))
		deleted := rng.Intn(5) == 0
		value := []byte(fmt.Sprintf("v-%d-%d", step, rng.Intn(10)))
		if err := store.CommitAt(timestamp, []Mutation{{Key: key, Value: value, Delete: deleted}}, CommitRelaxed); err != nil {
			t.Fatalf("step %d CommitAt: %v", step, err)
		}
		byTimestamp := oracle[string(key)]
		if byTimestamp == nil {
			byTimestamp = make(map[uint64]oracleVersion)
			oracle[string(key)] = byTimestamp
		}
		byTimestamp[timestamp] = oracleVersion{timestamp: timestamp, value: append([]byte(nil), value...), deleted: deleted}

		for probe := 0; probe < 4; probe++ {
			probeKey := keys[rng.Intn(len(keys))]
			readTimestamp := uint64(1 + rng.Intn(110))
			got, err := store.GetAt(probeKey, readTimestamp)
			if err != nil {
				t.Fatalf("step %d GetAt: %v", step, err)
			}
			want := Result{State: Absent}
			for ts, version := range oracle[string(probeKey)] {
				if ts <= readTimestamp && ts > want.Timestamp {
					want.Timestamp = ts
					want.Value = append([]byte(nil), version.value...)
					want.State = Present
					if version.deleted {
						want.State = Tombstone
						want.Value = nil
					}
				}
			}
			if got.State != want.State || got.Timestamp != want.Timestamp || !bytes.Equal(got.Value, want.Value) {
				t.Fatalf("step %d GetAt(%q,%d)=%+v want %+v", step, probeKey, readTimestamp, got, want)
			}
		}
	}
}

func TestGetAtConcurrentReaders(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	const commits = 100
	for ts := uint64(1); ts <= commits; ts++ {
		if err := store.CommitAt(ts, []Mutation{{Key: []byte("k"), Value: []byte(strconv.FormatUint(ts, 10))}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%d): %v", ts, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for reader := 0; reader < 8; reader++ {
		reader := reader
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(3672 + reader)))
			for probe := 0; probe < 250; probe++ {
				ts := uint64(1 + rng.Intn(commits))
				got, err := store.GetAt([]byte("k"), ts)
				if err != nil {
					errCh <- err
					return
				}
				if got.State != Present || got.Timestamp != ts || string(got.Value) != strconv.FormatUint(ts, 10) {
					errCh <- fmt.Errorf("GetAt(%d)=%+v", ts, got)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

type batchSpyDB struct {
	*treedb.DB
	created atomic.Int64
}

func (db *batchSpyDB) NewBatchWithSize(size int) treedb.Batch {
	db.created.Add(1)
	return db.DB.NewBatchWithSize(size)
}

var errInjectedBatch = errors.New("injected batch failure")

type writeFailureDB struct {
	*treedb.DB
	afterCommit bool
	err         error
}

func (db *writeFailureDB) NewBatchWithSize(size int) treedb.Batch {
	return &writeFailureBatch{Batch: db.DB.NewBatchWithSize(size), afterCommit: db.afterCommit, err: db.err}
}

type writeFailureBatch struct {
	treedb.Batch
	afterCommit bool
	err         error
}

func (b *writeFailureBatch) Write() error {
	if b.afterCommit {
		if err := b.Batch.Write(); err != nil {
			return err
		}
	}
	if b.err != nil {
		return b.err
	}
	return errInjectedBatch
}

func (b *writeFailureBatch) WriteSync() error {
	if b.afterCommit {
		if err := b.Batch.WriteSync(); err != nil {
			return err
		}
	}
	if b.err != nil {
		return b.err
	}
	return errInjectedBatch
}

type stageFailureDB struct {
	*treedb.DB
	failAt int
	err    error
}

func (db *stageFailureDB) NewBatchWithSize(size int) treedb.Batch {
	return &stageFailureBatch{Batch: db.DB.NewBatchWithSize(size), failAt: db.failAt, err: db.err}
}

type stageFailureBatch struct {
	treedb.Batch
	sets   int
	failAt int
	err    error
}

func (b *stageFailureBatch) Set(key, value []byte) error {
	if b.sets == b.failAt {
		return b.err
	}
	b.sets++
	return b.Batch.Set(key, value)
}

// Keep compile-time coverage that injected batches retain the complete public
// contract instead of testing a narrower fake API.
var _ treedb.Batch = (*writeFailureBatch)(nil)
var _ treedb.Batch = (*stageFailureBatch)(nil)
