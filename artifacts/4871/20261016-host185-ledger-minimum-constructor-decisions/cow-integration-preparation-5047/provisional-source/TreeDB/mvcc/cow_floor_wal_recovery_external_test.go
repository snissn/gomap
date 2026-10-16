package mvcc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

const cowFloorWALChildDir = "TREEDB_COW_FLOOR_WAL_CHILD_DIR"

// This characterizes acknowledged process-exit WAL recovery through Store.
// It does not qualify power-loss recovery, native pruning, or performance.
func TestPublicCOWDiscardFloorWALProcessRecovery(t *testing.T) {
	if dir := os.Getenv(cowFloorWALChildDir); dir != "" {
		profile := treedb.Profile(os.Getenv(cowFloorWALChildDir + "_PROFILE"))
		if profile != treedb.ProfileCommandWALDurable && profile != treedb.ProfileCommandWALRelaxed {
			t.Fatalf("invalid child profile %q", profile)
		}
		pointers, err := strconv.ParseBool(os.Getenv(cowFloorWALChildDir + "_POINTERS"))
		if err != nil {
			t.Fatal(err)
		}
		writeCOWFloorWALChild(t, cowFloorWALOptions(profile, dir, pointers), pointers)
		if t.Failed() {
			t.Fatal("child ownership or witness assertion failed")
		}
		os.Exit(0) // No Checkpoint or DB.Close: only acknowledged WAL work recovers.
	}
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		for _, pointers := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointers=%t", profile, pointers), func(t *testing.T) {
				opts := cowFloorWALOptions(profile, t.TempDir(), pointers)
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, binary, "-test.run=^TestPublicCOWDiscardFloorWALProcessRecovery$", "-test.v")
				cmd.Env = append(os.Environ(), cowFloorWALChildDir+"="+opts.Dir,
					cowFloorWALChildDir+"_PROFILE="+string(profile), cowFloorWALChildDir+"_POINTERS="+strconv.FormatBool(pointers))
				output, err := cmd.CombinedOutput() // Wait joins the child, including timeout cleanup.
				if err != nil || ctx.Err() != nil {
					t.Fatalf("process-exit child: %v context=%v\n%s", err, ctx.Err(), output)
				}
				data, err := os.ReadFile(filepath.Join(opts.Dir, "cow-floor-wal-witness.json"))
				if err != nil {
					t.Fatal(err)
				}
				var witness cowFloorWALWitness
				if err := json.Unmarshal(data, &witness); err != nil {
					t.Fatal(err)
				}
				if witness.Profile != profile || witness.Pointers != pointers || witness.Mode != "cow_btree" || witness.Shards != 4 || witness.Syncs < 3 || witness.Appends < 3 || witness.Checkpoints != 0 {
					t.Fatalf("unexpected acknowledged child witness: %+v", witness)
				}
				t.Logf("joined process-exit child: %+v", witness)
				for reopen := 1; reopen <= 2; reopen++ {
					t.Run(fmt.Sprintf("reopen=%d", reopen), func(t *testing.T) {
						checkCOWFloorWALReopen(t, opts, pointers)
					})
				}
			})
		}
	}
}

func cowFloorWALOptions(profile treedb.Profile, dir string, pointers bool) treedb.Options {
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxWALBytes = -1
	opts.ValueLog.ForcePointers = pointers
	opts.ValueLog.PointerThreshold = 1 << 30
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	return opts
}

type cowFloorWALWitness struct {
	Profile     treedb.Profile
	Pointers    bool
	Mode        string
	Shards      uint64
	Syncs       uint64
	Appends     uint64
	Checkpoints uint64
}

func cowFloorWALStat(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	value, ok := stats[key]
	if !ok {
		t.Fatalf("missing public counter %s", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("invalid public counter %s=%q: %v", key, value, err)
	}
	return parsed
}

func cowFloorWALDelta(t *testing.T, before, after map[string]string, key string) uint64 {
	t.Helper()
	a, b := cowFloorWALStat(t, before, key), cowFloorWALStat(t, after, key)
	if b < a {
		t.Fatalf("counter regressed %s: %d -> %d", key, a, b)
	}
	return b - a
}

func cowFloorWALValues() ([]byte, []byte) {
	return bytes.Repeat([]byte("old/"), 64), bytes.Repeat([]byte("new/"), 64)
}

func writeCOWFloorWALChild(t *testing.T, opts treedb.Options, pointers bool) {
	t.Helper()
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately do not install DB.Close cleanup in this process-exit branch.
	if !db.SupportsMVCCReadCut() {
		t.Fatal("COW read-cut capability unavailable")
	}
	before := db.Stats()
	if before["treedb.profile.resolved"] != string(opts.ResolvedProfile) || before["treedb.cache.memtable_mode"] != "cow_btree" || cowFloorWALStat(t, before, "treedb.cache.memtable_shards") != 4 {
		t.Fatal("resolved storage is not four-shard COW")
	}
	store := mvcc.New(db)
	oldValue, newValue := cowFloorWALValues()
	if err := store.CommitGroupAt([]mvcc.CommitGroup{
		{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: []byte("deleted"), Value: oldValue}, {Key: []byte("survivor"), Value: oldValue}}},
		{Timestamp: 20, Mutations: []mvcc.Mutation{{Key: []byte("deleted"), Delete: true}}},
	}, mvcc.CommitDurable); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(20, mvcc.CommitDurable); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitGroupAt([]mvcc.CommitGroup{
		{Timestamp: 30, Mutations: []mvcc.Mutation{{Key: []byte("survivor"), Value: newValue}}},
		{Timestamp: 40, Mutations: []mvcc.Mutation{{Key: []byte("acknowledged"), Value: newValue}}},
	}, mvcc.CommitDurable); err != nil {
		t.Fatal(err)
	}
	checkCOWFloorWALLayout(t, db, pointers)
	after := db.Stats()
	witness := cowFloorWALWitness{
		Profile: treedb.Profile(after["treedb.profile.resolved"]), Pointers: pointers, Mode: after["treedb.cache.memtable_mode"],
		Shards:      cowFloorWALStat(t, after, "treedb.cache.memtable_shards"),
		Syncs:       cowFloorWALDelta(t, before, after, "treedb.command_wal.file_sync.calls_total"),
		Appends:     cowFloorWALDelta(t, before, after, "treedb.command_wal.append.count_total"),
		Checkpoints: cowFloorWALDelta(t, before, after, "treedb.public.checkpoint.calls_total"),
	}
	if witness.Syncs < 3 || witness.Appends < 3 || witness.Checkpoints != 0 {
		t.Fatalf("missing durable WAL/no-checkpoint witness: %+v", witness)
	}
	data, err := json.Marshal(witness)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(opts.Dir, "cow-floor-wal-witness.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func checkCOWFloorWALLayout(t *testing.T, db *treedb.DB, pointers bool) {
	t.Helper()
	// Encoding is observational only; every write and admission uses public Store.
	key, err := mvcckey.Encode([]byte("survivor"), 30)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("layout snapshot unavailable")
	}
	defer func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	}()
	entry, err := snapshot.GetEntry(key)
	if err != nil {
		t.Fatal(err)
	}
	if actual := entry.Flags&node.FlagPointer != 0; actual != pointers {
		t.Fatalf("actual value pointer=%t want=%t", actual, pointers)
	}
}

func checkCOWFloorWALReopen(t *testing.T, opts treedb.Options, pointers bool) {
	t.Helper()
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !db.SupportsMVCCReadCut() {
		t.Fatal("reopened COW read-cut capability unavailable")
	}
	store := mvcc.New(db)
	// Prune is the first Store operation; it must not load the persisted floor.
	before := db.Stats()
	if before["treedb.profile.resolved"] != string(opts.ResolvedProfile) || before["treedb.cache.memtable_mode"] != "cow_btree" || cowFloorWALStat(t, before, "treedb.cache.memtable_shards") != 4 {
		t.Fatal("reopened storage/profile selection changed")
	}
	stats, err := store.PruneVersions(mvcc.PruneOptions{BatchSize: 1, Mode: mvcc.CommitDurable})
	if !errors.Is(err, caching.ErrCOWUnsupported) || stats != (mvcc.PruneStats{}) {
		t.Fatalf("fresh Store prune=%+v err=%v", stats, err)
	}
	after := db.Stats()
	for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total", "treedb.cache.cow.publications_total", "treedb.cache.cow.capture_calls_total"} {
		if delta := cowFloorWALDelta(t, before, after, key); delta != 0 {
			t.Fatalf("refused prune changed %s by %d", key, delta)
		}
	}
	if floor, err := store.DiscardFloor(); err != nil || floor != 20 {
		t.Fatalf("replayed floor=%d err=%v", floor, err)
	}
	if cowFloorWALDelta(t, after, db.Stats(), "treedb.cache.cow.capture_calls_total") == 0 {
		t.Fatal("floor load did not exercise the observed capture path")
	}
	if _, err := store.GetAt([]byte("deleted"), 20); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
		t.Fatalf("floor-equal point read: %v", err)
	}
	it, err := store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 20})
	if it != nil {
		if closeErr := it.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal("floor-equal iterator returned an owner")
	}
	if !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
		t.Fatalf("floor-equal iterator: %v", err)
	}
	for _, ts := range []uint64{19, 20} {
		if err := store.CommitAt(ts, []mvcc.Mutation{{Key: []byte("rejected-point"), Value: []byte("bad")}}, mvcc.CommitDurable); !errors.Is(err, mvcc.ErrVersionBelowDiscardFloor) {
			t.Fatalf("floor-bound point commit: %v", err)
		}
		if err := store.CommitGroupAt([]mvcc.CommitGroup{
			{Timestamp: 50, Mutations: []mvcc.Mutation{{Key: []byte("survivor"), Value: []byte("bad")}, {Key: []byte("rejected-group"), Value: []byte("bad")}}},
			{Timestamp: ts, Mutations: []mvcc.Mutation{{Key: []byte("rejected-floor"), Value: []byte("bad")}}},
		}, mvcc.CommitDurable); !errors.Is(err, mvcc.ErrVersionBelowDiscardFloor) {
			t.Fatalf("mixed admissible-first group: %v", err)
		}
	}
	oldValue, newValue := cowFloorWALValues()
	for _, want := range []struct {
		key    string
		ts     uint64
		result mvcc.Result
	}{
		{"deleted", 21, mvcc.Result{State: mvcc.Tombstone, Timestamp: 20}},
		{"survivor", 21, mvcc.Result{State: mvcc.Present, Timestamp: 10, Value: oldValue}},
		{"survivor", 100, mvcc.Result{State: mvcc.Present, Timestamp: 30, Value: newValue}},
		{"acknowledged", 100, mvcc.Result{State: mvcc.Present, Timestamp: 40, Value: newValue}},
		{"rejected-point", 100, mvcc.Result{State: mvcc.Absent}},
		{"rejected-group", 100, mvcc.Result{State: mvcc.Absent}},
		{"rejected-floor", 100, mvcc.Result{State: mvcc.Absent}},
	} {
		got, err := store.GetAt([]byte(want.key), want.ts)
		if err != nil || got.State != want.result.State || got.Timestamp != want.result.Timestamp || !bytes.Equal(got.Value, want.result.Value) {
			t.Fatalf("GetAt(%q,%d)=%+v err=%v want=%+v", want.key, want.ts, got, err, want.result)
		}
	}
	checkCOWFloorRecoveryHistory(t, store, []mvcc.Version{
		{Key: []byte("acknowledged"), Timestamp: 40, State: mvcc.Present, Value: newValue},
		{Key: []byte("deleted"), Timestamp: 20, State: mvcc.Tombstone},
		{Key: []byte("deleted"), Timestamp: 10, State: mvcc.Present, Value: oldValue},
		{Key: []byte("survivor"), Timestamp: 30, State: mvcc.Present, Value: newValue},
		{Key: []byte("survivor"), Timestamp: 10, State: mvcc.Present, Value: oldValue},
	})
	checkCOWFloorWALLayout(t, db, pointers)
}
