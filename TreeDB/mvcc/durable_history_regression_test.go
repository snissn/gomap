package mvcc_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// TestPublicHistoryVacuumAndSubsequentFlush keeps the failed durable mixed
// workload's keys, single-commit history, and 100k-operation trace, while using
// NoWAL/CommitRelaxed to localize maintenance boundaries without fsync timing.
// It does not replace the original durable workload obligation.
func TestPublicHistoryVacuumAndSubsequentFlush(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if !db.SupportsMVCCReadCut() || db.ResolvedProfile() != treedb.ProfileNoWALFast {
		t.Fatal("public NoWAL COW read cuts inactive")
	}
	store := mvcc.New(db)
	keys := make([][]byte, 256)
	// Five adapter-envelope bytes plus the original 128-byte payload retain the
	// physical value size; MVCC treats these bytes as an opaque application value.
	value := make([]byte, 133)
	copy(value, []byte{0xd7, 0x47, 1, 0, 0})
	for i := 0; i < 128; i++ {
		value[i+5] = byte(i*31 + 17)
	}
	want := make(map[string][]byte)
	var latest [256]mvcc.Result
	seed := make([]mvcc.Mutation, len(keys))
	record := func(k int, ts uint64, deleted bool) {
		physical, err := mvcckey.Encode(keys[k], ts)
		if err != nil {
			t.Fatal(err)
		}
		if deleted {
			want[string(physical)] = []byte{2}
			latest[k] = mvcc.Result{State: mvcc.Tombstone, Timestamp: ts}
		} else {
			want[string(physical)] = append([]byte{1}, value...)
			latest[k] = mvcc.Result{State: mvcc.Present, Timestamp: ts, Value: value}
		}
	}
	for i := range keys {
		keys[i] = make([]byte, 24)
		copy(keys[i], "dgraph-posting-")
		binary.BigEndian.PutUint64(keys[i][16:], uint64(i))
		seed[i] = mvcc.Mutation{Key: keys[i], Value: value}
		record(i, 1, false)
	}
	if err := store.CommitAt(1, seed, mvcc.CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	read := func(label string, k int, ts uint64) {
		got, err := store.GetAt(keys[k], ts)
		if err != nil || got.State != latest[k].State || got.Timestamp != latest[k].Timestamp || !bytes.Equal(got.Value, latest[k].Value) {
			t.Fatalf("%s key=%d read_ts=%d got=%+v err=%v want=%+v", label, k, ts, got, err, latest[k])
		}
	}
	physicalHistory := func(label string, cut treedb.MVCCReadSnapshot, expected map[string][]byte) {
		it, err := cut.Iterator(nil, nil)
		if err != nil {
			t.Fatalf("%s iterator: %v", label, err)
		}
		defer it.Close()
		seen := make(map[string]bool, len(expected))
		var key0Count int
		var key0Latest uint64
		for ; it.Valid(); it.Next() {
			key := string(it.Key())
			entry, ok := expected[key]
			if !ok || seen[key] || !bytes.Equal(it.Value(), entry) {
				t.Fatalf("%s unexpected/duplicate/corrupt physical record key=%x got=%x want=%x", label, it.Key(), it.Value(), entry)
			}
			seen[key] = true
			logical, ts, err := mvcckey.Decode(it.Key())
			if err != nil {
				t.Fatalf("%s decode: %v", label, err)
			}
			if bytes.Equal(logical, keys[0]) {
				key0Count++
				if ts > key0Latest {
					key0Latest = ts
				}
			}
		}
		if err := it.Error(); err != nil {
			t.Fatalf("%s iterator: %v", label, err)
		}
		state := cut.State()
		t.Logf("BOUNDARY %s physical=%d/%d key0_versions=%d key0_latest=%d commit=%d root=%d applied=%d revision=%d", label, len(seen), len(expected), key0Count, key0Latest, state.CommitSeq, state.RootPageID, state.AppliedCommandLSN, state.MaxEntryRevision)
		if len(seen) != len(expected) {
			missingByKey := make(map[string]int)
			var missingKey0 []uint64
			for key := range expected {
				if !seen[key] {
					logical, ts, _ := mvcckey.Decode([]byte(key))
					missingByKey[fmt.Sprintf("%x", logical)]++
					if bytes.Equal(logical, keys[0]) {
						missingKey0 = append(missingKey0, ts)
					}
				}
			}
			sort.Slice(missingKey0, func(i, j int) bool { return missingKey0[i] < missingKey0[j] })
			t.Fatalf("%s missing physical history: count=%d want=%d by_logical_key=%v key0_timestamps=%v", label, len(seen), len(expected), missingByKey, missingKey0)
		}
	}
	var timestamp uint64 = 1
	verify := func(label string) {
		cut, err := db.AcquireMVCCReadCut()
		if err != nil {
			t.Fatal(err)
		}
		defer cut.Close()
		physicalHistory(label, cut, want)
		for k := range keys {
			read(label, k, timestamp)
		}
		// The maintained decoded iterator is independent of the optimized point
		// successor and checks every retained logical version, including deletes.
		it, err := store.IterateVersions(mvcc.VersionIteratorOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		count := 0
		for ; it.Valid(); it.Next() {
			v := it.EntryView()
			physical, err := mvcckey.Encode(v.Key, v.Timestamp)
			if err != nil {
				t.Fatal(err)
			}
			expected, ok := want[string(physical)]
			if !ok || (expected[0] == 2 && (v.State != mvcc.Tombstone || len(v.Value) != 0)) || (expected[0] == 1 && (v.State != mvcc.Present || !bytes.Equal(v.Value, value))) {
				t.Fatalf("%s decoded version key=%x ts=%d state=%d", label, v.Key, v.Timestamp, v.State)
			}
			count++
		}
		if err := it.Error(); err != nil || count != len(want) {
			t.Fatalf("%s decoded history count=%d want=%d err=%v", label, count, len(want), err)
		}
	}
	checkpoint := func(label string) {
		verify(label + "/before")
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("%s checkpoint: %v", label, err)
		}
		verify(label + "/after")
	}
	checkpoint("seed")
	next := 0
	watchFlush := false
	var priorCommit, priorRoot, priorRevision uint64
	observePhysicalChange := func() {
		cut, err := db.AcquireMVCCReadCut()
		if err != nil {
			t.Fatal(err)
		}
		defer cut.Close()
		state := cut.State()
		if state.CommitSeq != priorCommit || state.RootPageID != priorRoot || uint64(state.MaxEntryRevision) != priorRevision {
			t.Logf("PHYSICAL_CHANGE after_op=%d timestamp=%d previous_commit=%d previous_root=%d previous_revision=%d", next, timestamp, priorCommit, priorRoot, priorRevision)
			physicalHistory(fmt.Sprintf("natural-flush/op=%d", next), cut, want)
			priorCommit, priorRoot, priorRevision = state.CommitSeq, state.RootPageID, uint64(state.MaxEntryRevision)
		}
	}
	advance := func(end int) {
		for ; next < end; next++ {
			k := (next * 131) % len(keys)
			if next%10 < 6 {
				read(fmt.Sprintf("trace/op=%d", next), k, timestamp)
				continue
			}
			timestamp++
			deleted := next%10 >= 8
			if err := store.CommitAt(timestamp, []mvcc.Mutation{{Key: keys[k], Value: value, Delete: deleted}}, mvcc.CommitRelaxed); err != nil {
				t.Fatalf("trace/op=%d ts=%d commit: %v", next, timestamp, err)
			}
			record(k, timestamp, deleted)
			if watchFlush {
				observePhysicalChange()
			}
		}
	}
	// The probe's last surviving key0 version was DELETE ts23348 at op58368.
	// Two real checkpoints provide distinct recoverable roots before vacuum.
	advance(58370)
	checkpoint("baseline")
	held, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	heldWant := make(map[string][]byte, len(want))
	for key, entry := range want {
		heldWant[key] = entry
	}
	verify("vacuum/before")
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	verify("vacuum/after")
	physicalHistory("held/after-vacuum", held, heldWant)
	// Watch public physical identity after each subsequent accepted mutation.
	// Scan all versions only when the captured physical root changes, so the
	// first natural flush is identified without timing hooks or quadratic scans.
	initial, err := db.AcquireMVCCReadCut()
	if err != nil {
		t.Fatal(err)
	}
	state := initial.State()
	priorCommit, priorRoot, priorRevision = state.CommitSeq, state.RootPageID, uint64(state.MaxEntryRevision)
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	watchFlush = true
	advance(74753)
	checkpoint("first-post-vacuum-flush")
	physicalHistory("held/after-first-flush", held, heldWant)
	advance(100000)
	checkpoint("reuse-flush")
	physicalHistory("held/after-reuse-flush", held, heldWant)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store = mvcc.New(db)
	verify("reopen")
}
