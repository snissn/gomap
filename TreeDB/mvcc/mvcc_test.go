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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func openTestDB(t testing.TB, dir string, durability treedb.DurabilityMode) *treedb.DB {
	t.Helper()
	var profile treedb.Profile
	switch durability {
	case treedb.DurabilityDurable:
		profile = treedb.ProfileCommandWALDurable
	case treedb.DurabilityWALOnRelaxed:
		profile = treedb.ProfileCommandWALRelaxed
	case treedb.DurabilityWALOffRelaxed:
		profile = treedb.ProfileNoWALFast
	default:
		t.Fatalf("unsupported durability mode %d", durability)
	}
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
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

func TestGetAtUsesPointSuccessorWithoutIteratorRotation(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityWALOffRelaxed)
	defer db.Close()
	store := New(db)
	if err := store.CommitAt(7, []Mutation{{Key: []byte("k"), Value: []byte("seven")}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt: %v", err)
	}
	before := db.Stats()
	requireResult(t, store, []byte("k"), 9, Present, 7, []byte("seven"))
	after := db.Stats()
	for _, name := range []string{
		"treedb.cache.iterator.calls_total",
		"treedb.cache.iterator.snapshot_rotations_total",
	} {
		if before[name] != after[name] {
			t.Fatalf("%s changed across GetAt: %q -> %q", name, before[name], after[name])
		}
	}
	if before["treedb.cache.queue_len"] != after["treedb.cache.queue_len"] {
		t.Fatalf("queue length changed across GetAt: %q -> %q", before["treedb.cache.queue_len"], after["treedb.cache.queue_len"])
	}
}

func TestGetAtPointSuccessorValueLogCheckpointAndReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *treedb.DB {
		db, err := treedb.Open(treedb.Options{
			Dir:                          dir,
			Durability:                   treedb.DurabilityWALOffRelaxed,
			DisableSideStores:            true,
			BackgroundCheckpointInterval: -1,
			ValueLog: treedb.ValueLogOptions{
				ForcePointers:    true,
				PointerThreshold: 1,
			},
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	db := open()
	store := New(db)
	value := bytes.Repeat([]byte("v"), 4096)
	if err := store.CommitAt(7, []Mutation{{Key: []byte("k"), Value: value}}, CommitRelaxed); err != nil {
		t.Fatalf("CommitAt: %v", err)
	}
	requireResult(t, store, []byte("k"), 9, Present, 7, value)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db = open()
	defer db.Close()
	requireResult(t, New(db), []byte("k"), 9, Present, 7, value)
}

func TestGetAtPointSuccessorDiscardFloorAndPruneBoundary(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityWALOffRelaxed)
	defer db.Close()
	store := New(db)
	for ts := uint64(1); ts <= 5; ts++ {
		if err := store.CommitAt(ts, []Mutation{{Key: []byte("k"), Value: []byte(strconv.FormatUint(ts, 10))}}, CommitRelaxed); err != nil {
			t.Fatalf("CommitAt(%d): %v", ts, err)
		}
	}
	if err := store.AdvanceDiscardFloor(4, CommitRelaxed); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	if _, err := store.GetAt([]byte("k"), 3); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("GetAt below floor error = %v", err)
	}
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitRelaxed}); err != nil {
		t.Fatalf("PruneVersions: %v", err)
	}
	if _, err := store.GetAt([]byte("k"), 4); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("GetAt at floor error = %v", err)
	}
	requireResult(t, store, []byte("k"), 5, Present, 5, []byte("5"))
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

func TestCommitAtOversizedKeyRejectsBeforeDuplicateIdentityAllocation(t *testing.T) {
	spy := &validationSpyDB{}
	store := newStore(spy)
	oversized := make([]byte, 16<<20)
	mutations := []Mutation{
		{Key: []byte("valid-before-oversized"), Value: []byte("must-not-publish")},
		{Key: oversized, Value: []byte("invalid")},
		{Key: []byte("valid-after-oversized"), Value: []byte("must-not-publish")},
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := store.CommitAt(1, mutations, CommitRelaxed)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrInvalidKey) || !strings.Contains(err.Error(), "key index 1") {
		t.Fatalf("oversized multi-mutation error = %v, want ErrInvalidKey at index 1", err)
	}
	if got := spy.created.Load(); got != 0 {
		t.Fatalf("batches created after oversized-key rejection = %d, want 0", got)
	}
	// The 16 MiB caller-owned key is intentionally much larger than the codec
	// envelope. Rejecting it must not copy it into duplicate-detection state.
	// TotalAlloc remains useful after the temporary state is freed; a retained
	// []byte-to-string conversion before validation would add at least 16 MiB.
	allocated := after.TotalAlloc - before.TotalAlloc
	if allocated > 2<<20 {
		t.Fatalf("oversized-key validation allocated %d bytes, want <= %d", allocated, 2<<20)
	}
	t.Logf("oversized-key validation allocated %d bytes without creating a batch", allocated)
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
			if !errors.Is(err, ErrStorage) {
				t.Fatalf("CommitAt error = %v, want ErrStorage", err)
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
	if _, err := store.GetAt([]byte("bad-value"), 9); !errors.Is(err, treedb.ErrClosed) || !errors.Is(err, ErrStorage) {
		t.Fatalf("GetAt closed error=%v want ErrStorage wrapping ErrClosed", err)
	}
}

func TestNilStoreReturnsStorageErrors(t *testing.T) {
	store := New(nil)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("k")}}, CommitRelaxed); !errors.Is(err, ErrStorage) || !errors.Is(err, treedb.ErrClosed) {
		t.Fatalf("CommitAt nil store error=%v", err)
	}
	if _, err := store.GetAt([]byte("k"), 1); !errors.Is(err, ErrStorage) || !errors.Is(err, treedb.ErrClosed) {
		t.Fatalf("GetAt nil store error=%v", err)
	}

	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	store = New(db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.CommitAt(1, nil, CommitDurable); err != nil {
		t.Fatalf("empty CommitAt on closed non-nil store must not access storage: %v", err)
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
			if err := store.CommitAt(22, nil, CommitDurable); err != nil {
				t.Fatalf("empty durable CommitAt must be a no-op: %v", err)
			}
			if err := store.CommitAt(22, []Mutation{{Key: []byte("k"), Value: []byte("explicit-sync")}}, CommitDurable); err != nil {
				t.Fatalf("durable request on relaxed ordinary-ACK profile: %v", err)
			}
			requireResult(t, store, []byte("k"), 22, Present, 22, []byte("explicit-sync"))
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db = openTestDB(t, dir, durability)
			defer db.Close()
			requireResult(t, New(db), []byte("k"), 22, Present, 22, []byte("explicit-sync"))
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
	frames, err := commitlog.ScanCommandFrameSegments(segments, commitlog.Options{})
	if err != nil {
		t.Fatalf("scan crash WAL segments: %v", err)
	}
	if len(frames) == 0 {
		t.Fatal("crash WAL has no complete command frames")
	}
	incompleteSuffixLSN := frames[len(frames)-1].LSN + 1
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:  commitlog.RawKVOpDelete,
		Key: []byte("incomplete-relaxed-suffix"),
	}})
	if err != nil {
		t.Fatalf("encode incomplete relaxed suffix payload: %v", err)
	}
	fixturePath := filepath.Join(t.TempDir(), "incomplete-relaxed.log")
	fixture, err := commitlog.NewWriter(fixturePath)
	if err != nil {
		t.Fatalf("create incomplete relaxed suffix fixture: %v", err)
	}
	if err := fixture.AppendCommandV2(commitlog.CommandEnvelope{
		Version:         commitlog.CommandFrameVersionV2,
		LSN:             incompleteSuffixLSN,
		DurabilityClass: commitlog.CommandDurabilityRelaxed,
		Kind:            commitlog.CommandKindRawKVBatch,
		Scope:           commitlog.CommandScopeRawKV,
		PayloadFormat:   commitlog.PayloadFormatRawKVBatchV1,
		Payload:         payload,
	}); err != nil {
		_ = fixture.Close()
		t.Fatalf("append incomplete relaxed suffix fixture: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatalf("close incomplete relaxed suffix fixture: %v", err)
	}
	record, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read incomplete relaxed suffix fixture: %v", err)
	}
	const classifiablePrefixBytes = 8 + 56
	if len(record) <= classifiablePrefixBytes {
		t.Fatalf("incomplete relaxed suffix fixture length=%d, want >%d", len(record), classifiablePrefixBytes)
	}
	tail, err := os.OpenFile(segments[len(segments)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open crash WAL tail: %v", err)
	}
	if _, err := tail.Write(record[:classifiablePrefixBytes]); err != nil {
		_ = tail.Close()
		t.Fatalf("append classifiable incomplete relaxed suffix: %v", err)
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

type validationSpyDB struct {
	created atomic.Int64
}

func (*validationSpyDB) Iterator(_, _ []byte) (treedb.Iterator, error) {
	panic("Iterator must not be called during CommitAt validation")
}

func (db *validationSpyDB) NewBatchWithSize(_ int) treedb.Batch {
	db.created.Add(1)
	panic("NewBatchWithSize must not be called during CommitAt validation")
}

func (*validationSpyDB) DurabilityMode() string {
	return string(treedb.DurabilityWALOffRelaxed)
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
var _ treeDB = (*validationSpyDB)(nil)
