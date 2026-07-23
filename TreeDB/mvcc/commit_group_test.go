package mvcc

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestCommitGroupAtPublishesVersionsAndTombstones(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	if err := store.CommitGroupAt([]CommitGroup{
		{Timestamp: 7, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("A")}, {Key: []byte("gone"), Delete: true}}},
		{Timestamp: 9, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("new")}, {Key: []byte("b"), Value: []byte("B")}}},
	}, CommitRelaxed); err != nil {
		t.Fatalf("CommitGroupAt: %v", err)
	}
	requireResult(t, store, []byte("a"), 7, Present, 7, []byte("A"))
	requireResult(t, store, []byte("a"), 9, Present, 9, []byte("new"))
	requireResult(t, store, []byte("b"), 9, Present, 9, []byte("B"))
	requireResult(t, store, []byte("gone"), 9, Tombstone, 7, nil)
}

func TestCommitGroupAtValidationPrecedesStorageWrite(t *testing.T) {
	spy := &validationSpyDB{}
	store := newStore(spy)

	cases := []struct {
		name   string
		groups []CommitGroup
		want   error
	}{
		{"zero timestamp", []CommitGroup{{Timestamp: 0}}, ErrZeroTimestamp},
		{"invalid key", []CommitGroup{{Timestamp: 1, Mutations: []Mutation{{Key: make([]byte, 16<<20)}}}}, ErrInvalidKey},
		{"same timestamp duplicate", []CommitGroup{{Timestamp: 1, Mutations: []Mutation{{Key: []byte("k")}}}, {Timestamp: 1, Mutations: []Mutation{{Key: []byte("k")}}}}, ErrDuplicateKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.CommitGroupAt(tc.groups, CommitRelaxed); !errors.Is(err, tc.want) {
				t.Fatalf("CommitGroupAt error=%v want %v", err, tc.want)
			}
		})
	}
	if got := spy.created.Load(); got != 0 {
		t.Fatalf("batches created after validation failures = %d, want 0", got)
	}
}

func TestCommitValidationOrderAndEmptyGroups(t *testing.T) {
	store := New(nil)
	if err := store.CommitAt(0, nil, CommitMode(99)); !errors.Is(err, ErrZeroTimestamp) {
		t.Fatalf("CommitAt zero timestamp precedence error=%v want ErrZeroTimestamp", err)
	}
	if err := store.CommitGroupAt([]CommitGroup{{Timestamp: 0}}, CommitMode(99)); !errors.Is(err, ErrInvalidCommitMode) {
		t.Fatalf("CommitGroupAt mode precedence error=%v want ErrInvalidCommitMode", err)
	}
	if err := store.CommitGroupAt(nil, CommitMode(99)); !errors.Is(err, ErrInvalidCommitMode) {
		t.Fatalf("empty CommitGroupAt invalid mode error=%v want ErrInvalidCommitMode", err)
	}
	if err := store.CommitGroupAt(nil, CommitRelaxed); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-store empty CommitGroupAt error=%v want ErrStorage", err)
	}

	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := New(db).CommitGroupAt([]CommitGroup{{Timestamp: 1}}, CommitDurable); err != nil {
		t.Fatalf("empty group on closed non-nil store must not access storage: %v", err)
	}
}

func TestCommitGroupAtDistinctTimestampsAndDiscardFloor(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	if err := store.CommitGroupAt([]CommitGroup{
		{Timestamp: 3, Mutations: []Mutation{{Key: []byte("same"), Value: []byte("old")}}},
		{Timestamp: 4, Mutations: []Mutation{{Key: []byte("same"), Value: []byte("new")}}},
	}, CommitRelaxed); err != nil {
		t.Fatalf("distinct timestamps: %v", err)
	}
	if err := store.AdvanceDiscardFloor(4, CommitRelaxed); err != nil {
		t.Fatalf("AdvanceDiscardFloor: %v", err)
	}
	err := store.CommitGroupAt([]CommitGroup{
		{Timestamp: 4, Mutations: []Mutation{{Key: []byte("below"), Value: []byte("x")}}},
		{Timestamp: 5, Mutations: []Mutation{{Key: []byte("above"), Value: []byte("y")}}},
	}, CommitRelaxed)
	if !errors.Is(err, ErrVersionBelowDiscardFloor) {
		t.Fatalf("floor error=%v want ErrVersionBelowDiscardFloor", err)
	}
	requireResult(t, store, []byte("below"), 5, Absent, 0, nil)
	requireResult(t, store, []byte("above"), 5, Absent, 0, nil)
}

func TestCommitGroupAtUsesOneWritePerMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode CommitMode
		stat string
	}{
		{"relaxed", CommitRelaxed, "treedb.public.batch.write.calls_total"},
		{"durable", CommitDurable, "treedb.public.batch.write_sync.calls_total"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t, t.TempDir(), treedb.DurabilityWALOnRelaxed)
			defer db.Close()
			before := mvccTestStat(t, db, tc.stat)
			err := New(db).CommitGroupAt([]CommitGroup{
				{Timestamp: 1, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("A")}}},
				{Timestamp: 2, Mutations: []Mutation{{Key: []byte("b"), Value: []byte("B")}}},
			}, tc.mode)
			if err != nil {
				t.Fatalf("CommitGroupAt: %v", err)
			}
			if got := mvccTestStat(t, db, tc.stat) - before; got != 1 {
				t.Fatalf("%s delta=%d want 1", tc.stat, got)
			}
		})
	}
}

func TestCommitGroupAtRoutesSingletonToPointWriter(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      CommitMode
		wantSet   int64
		wantBatch int64
	}{
		{name: "relaxed", mode: CommitRelaxed, wantSet: 1},
		{name: "durable", mode: CommitDurable, wantBatch: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t, t.TempDir(), treedb.DurabilityWALOnRelaxed)
			defer db.Close()
			spy := &pointRouteSpyDB{DB: db}
			store := newStore(spy)
			if err := store.CommitAt(7, []Mutation{{Key: []byte("key"), Value: []byte("value")}}, tc.mode); err != nil {
				t.Fatalf("CommitAt: %v", err)
			}
			if got := spy.setCalls.Load(); got != tc.wantSet {
				t.Fatalf("Set calls=%d want %d", got, tc.wantSet)
			}
			if got := spy.setSyncCalls.Load(); got != 0 {
				t.Fatalf("SetSync calls=%d want 0", got)
			}
			if got := spy.batchCalls.Load(); got != tc.wantBatch {
				t.Fatalf("batch creations=%d want %d", got, tc.wantBatch)
			}
			requireResult(t, store, []byte("key"), 7, Present, 7, []byte("value"))
		})
	}
}

func TestCommitGroupAtSingletonPointWriterFailureIsStorageError(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	injected := errors.New("injected point failure")
	spy := &pointRouteSpyDB{DB: db, pointErr: injected}
	err := newStore(spy).CommitAt(8, []Mutation{{Key: []byte("key"), Delete: true}}, CommitRelaxed)
	if !errors.Is(err, ErrStorage) || !errors.Is(err, injected) {
		t.Fatalf("CommitAt error=%v want ErrStorage and injected error", err)
	}
	if got := spy.setCalls.Load(); got != 1 {
		t.Fatalf("Set calls=%d want 1", got)
	}
	if got := spy.batchCalls.Load(); got != 0 {
		t.Fatalf("batch creations=%d want 0", got)
	}
}

func TestCommitGroupAtCommandWALSingletonUsesPointFrame(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	store := New(db)

	before := db.Stats()
	if err := store.CommitAt(9, []Mutation{{Key: []byte("single"), Value: []byte("value")}}, CommitRelaxed); err != nil {
		t.Fatalf("singleton CommitAt: %v", err)
	}
	afterPoint := db.Stats()
	requireMVCCStatDelta(t, before, afterPoint, "treedb.command_wal.append.count_total", 1)
	requireMVCCStatDelta(t, before, afterPoint, "treedb.command_wal.append.point.count_total", 1)
	requireMVCCStatDelta(t, before, afterPoint, "treedb.command_wal.append.payload.count_total", 0)
	requireMVCCStatDelta(t, before, afterPoint, "treedb.command_wal.flush.count_total", 1)
	requireMVCCStatDelta(t, before, afterPoint, "treedb.command_wal.write.syscalls_total", 1)
	requireMVCCStatDelta(t, before, afterPoint, "treedb.command_wal.file_sync.calls_total", 0)
	requireMVCCStatDelta(t, before, afterPoint, "treedb.public.batch.write.calls_total", 0)

	if err := store.CommitAt(10, []Mutation{
		{Key: []byte("multi-a"), Value: []byte("A")},
		{Key: []byte("multi-b"), Delete: true},
	}, CommitRelaxed); err != nil {
		t.Fatalf("multi CommitAt: %v", err)
	}
	afterBatch := db.Stats()
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.command_wal.append.count_total", 1)
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.command_wal.append.point.count_total", 0)
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.command_wal.append.payload.count_total", 1)
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.command_wal.flush.count_total", 1)
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.command_wal.write.syscalls_total", 1)
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.command_wal.file_sync.calls_total", 0)
	requireMVCCStatDelta(t, afterPoint, afterBatch, "treedb.public.batch.write.calls_total", 1)
}

func TestCommitGroupAtEmptyGroupsAreNoOp(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	before := mvccTestStat(t, db, "treedb.public.batch.write.calls_total")
	if err := New(db).CommitGroupAt([]CommitGroup{{Timestamp: 1}, {Timestamp: 2}}, CommitRelaxed); err != nil {
		t.Fatalf("empty groups: %v", err)
	}
	if got := mvccTestStat(t, db, "treedb.public.batch.write.calls_total") - before; got != 0 {
		t.Fatalf("empty groups write delta=%d want 0", got)
	}
}

func TestCommitGroupAtFailureHasNoPartialVisibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		db   func(*treedb.DB) treeDB
	}{
		{"stage", func(db *treedb.DB) treeDB { return &stageFailureDB{DB: db, failAt: 1, err: errors.New("stage")} }},
		{"write", func(db *treedb.DB) treeDB { return &writeFailureDB{DB: db, err: errors.New("write")} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
			defer db.Close()
			err := newStore(tc.db(db)).CommitGroupAt([]CommitGroup{
				{Timestamp: 11, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("A")}}},
				{Timestamp: 12, Mutations: []Mutation{{Key: []byte("b"), Value: []byte("B")}}},
			}, CommitRelaxed)
			if !errors.Is(err, ErrStorage) {
				t.Fatalf("CommitGroupAt error=%v want ErrStorage", err)
			}
			reader := New(db)
			requireResult(t, reader, []byte("a"), 12, Absent, 0, nil)
			requireResult(t, reader, []byte("b"), 12, Absent, 0, nil)
		})
	}
}

func TestCommitGroupAtAmbiguousStorageFailureIsWholeGroup(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir, treedb.DurabilityDurable)
	injected := errors.New("write after commit")
	err := newStore(&writeFailureDB{DB: db, afterCommit: true, err: injected}).CommitGroupAt([]CommitGroup{
		{Timestamp: 11, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("A")}}},
		{Timestamp: 12, Mutations: []Mutation{{Key: []byte("b"), Value: []byte("B")}}},
	}, CommitRelaxed)
	if !errors.Is(err, injected) || !errors.Is(err, ErrStorage) {
		t.Fatalf("CommitGroupAt error=%v want ambiguous storage error", err)
	}
	reader := New(db)
	requireResult(t, reader, []byte("a"), 12, Present, 11, []byte("A"))
	requireResult(t, reader, []byte("b"), 12, Present, 12, []byte("B"))
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db = openTestDB(t, dir, treedb.DurabilityDurable)
	defer db.Close()
	reader = New(db)
	requireResult(t, reader, []byte("a"), 12, Present, 11, []byte("A"))
	requireResult(t, reader, []byte("b"), 12, Present, 12, []byte("B"))
}

func TestCommitGroupAtDurableProcessCrashRecovery(t *testing.T) {
	const childEnv = "TREEDB_MVCC_GROUP_CRASH_CHILD"
	if dir := os.Getenv(childEnv); dir != "" {
		db := openTestDB(t, dir, treedb.DurabilityDurable)
		if err := New(db).CommitGroupAt([]CommitGroup{
			{Timestamp: 31, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("A")}}},
			{Timestamp: 32, Mutations: []Mutation{{Key: []byte("b"), Delete: true}}},
		}, CommitDurable); err != nil {
			t.Fatalf("child CommitGroupAt: %v", err)
		}
		os.Exit(0) // Deliberately omit Close to exercise WAL recovery after death.
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommitGroupAtDurableProcessCrashRecovery$")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	db := openTestDB(t, dir, treedb.DurabilityDurable)
	defer db.Close()
	store := New(db)
	requireResult(t, store, []byte("a"), 32, Present, 31, []byte("A"))
	requireResult(t, store, []byte("b"), 32, Tombstone, 32, nil)
}

func mvccTestStat(t *testing.T, db *treedb.DB, name string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(db.Stats()[name], 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return value
}

func requireMVCCStatDelta(t *testing.T, before, after map[string]string, name string, want uint64) {
	t.Helper()
	parse := func(stats map[string]string) uint64 {
		value, err := strconv.ParseUint(stats[name], 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", name, stats[name], err)
		}
		return value
	}
	start, finish := parse(before), parse(after)
	if finish < start || finish-start != want {
		t.Fatalf("%s delta=%d want %d (before=%d after=%d)", name, finish-start, want, start, finish)
	}
}

type pointRouteSpyDB struct {
	*treedb.DB
	pointErr     error
	setCalls     atomic.Int64
	setSyncCalls atomic.Int64
	batchCalls   atomic.Int64
}

func (db *pointRouteSpyDB) Set(key, value []byte) error {
	db.setCalls.Add(1)
	if db.pointErr != nil {
		return db.pointErr
	}
	return db.DB.Set(key, value)
}

func (db *pointRouteSpyDB) SetSync(key, value []byte) error {
	db.setSyncCalls.Add(1)
	if db.pointErr != nil {
		return db.pointErr
	}
	return db.DB.SetSync(key, value)
}

func (db *pointRouteSpyDB) NewBatchWithSize(size int) treedb.Batch {
	db.batchCalls.Add(1)
	return db.DB.NewBatchWithSize(size)
}

var _ treeDB = (*pointRouteSpyDB)(nil)
var _ pointWriter = (*pointRouteSpyDB)(nil)
