package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func nativeOrderedPruneOpen(t *testing.T, profile treedb.Profile, dir string) *treedb.DB {
	t.Helper()
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_ordered"
	opts.MemtableShards = 4
	opts.FlushThreshold = 1 << 30
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if !db.SupportsLogicalMVCC() || db.Stats()["treedb.cache.memtable_mode"] != "cow_ordered" {
		_ = db.Close()
		t.Fatal("native logical data plane did not activate")
	}
	return db
}

func nativePruneRequireVersions(t *testing.T, got, want []Version) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("retained versions: got %v, want %v", versionLabels(got), versionLabels(want))
	}
	for n := range want {
		if !bytes.Equal(got[n].Key, want[n].Key) || got[n].Timestamp != want[n].Timestamp ||
			got[n].State != want[n].State || !bytes.Equal(got[n].Value, want[n].Value) {
			t.Fatalf("version %d: got %+v, want %+v", n, got[n], want[n])
		}
	}
}

func nativePruneCollect(t *testing.T, store *Store, options VersionIteratorOptions) []Version {
	t.Helper()
	it, err := store.IterateVersions(options)
	if err != nil {
		t.Fatal(err)
	}
	return collectVersions(t, it)
}

// Pruning spans immutable disk basis and current logical overlays. A logical
// tombstone is a retained version until the floor makes its entire old prefix
// obsolete; a physical prune must never reveal any earlier basis value.
func TestNativeOrderedPruneBasisOverlayHeldCutReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			db := nativeOrderedPruneOpen(t, profile, dir)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			store := New(db)
			binaryKey := []byte{0, 0xff}
			for _, seed := range []struct {
				key      []byte
				versions []MutationAt
			}{
				{[]byte("a"), []MutationAt{{10, []byte("a10"), false}, {20, []byte("a20"), false}, {30, []byte("a30"), false}, {40, nil, true}, {50, []byte("a50"), false}}},
				{[]byte("dead"), []MutationAt{{10, []byte("dead10"), false}, {30, nil, true}}},
				{[]byte("future"), []MutationAt{{10, []byte("base"), false}, {60, []byte("tail"), false}}},
				{[]byte("same"), []MutationAt{{10, []byte("old"), false}, {20, []byte("anchor"), false}, {50, []byte("first"), false}}},
				{binaryKey, []MutationAt{{15, nil, false}, {25, nil, true}, {45, []byte("binary45"), false}}},
			} {
				groups := make([]CommitGroup, len(seed.versions))
				for n, v := range seed.versions {
					groups[n] = CommitGroup{Timestamp: v.Timestamp, Mutations: []Mutation{{Key: seed.key, Value: v.Value, Delete: v.Delete}}}
				}
				if err := store.CommitGroupAt(groups, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitGroupAt([]CommitGroup{
				{Timestamp: 20, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("replacement20")}}},
				{Timestamp: 50, Mutations: []Mutation{{Key: []byte("same"), Value: []byte("second")}}},
				{Timestamp: 55, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("a55")}}},
			}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			held, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 35})
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if err := store.AdvanceDiscardFloor(35, CommitDurable); err != nil {
				t.Fatal(err)
			}
			stats, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
			if err != nil || stats.Pruned != 7 || stats.Visited != stats.Pruned+stats.Retained {
				t.Fatalf("prune stats=%+v error=%v; want 7 obsolete physical records", stats, err)
			}
			requireResult(t, store, []byte("a"), 36, Present, 30, []byte("a30"))
			requireResult(t, store, []byte("a"), 40, Tombstone, 40, nil)
			requireResult(t, store, []byte("dead"), 36, Absent, 0, nil)
			requireResult(t, store, binaryKey, 36, Absent, 0, nil)
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// This iterator was admitted before the floor and owns pre-prune
			// history, including physically removed records, through checkpoint.
			nativePruneRequireVersions(t, collectVersions(t, held), []Version{
				{Key: binaryKey, Timestamp: 25, State: Tombstone},
				{Key: binaryKey, Timestamp: 15, State: Present},
				{Key: []byte("a"), Timestamp: 30, State: Present, Value: []byte("a30")},
				{Key: []byte("a"), Timestamp: 20, State: Present, Value: []byte("replacement20")},
				{Key: []byte("a"), Timestamp: 10, State: Present, Value: []byte("a10")},
				{Key: []byte("dead"), Timestamp: 30, State: Tombstone},
				{Key: []byte("dead"), Timestamp: 10, State: Present, Value: []byte("dead10")},
				{Key: []byte("future"), Timestamp: 10, State: Present, Value: []byte("base")},
				{Key: []byte("same"), Timestamp: 20, State: Present, Value: []byte("anchor")},
				{Key: []byte("same"), Timestamp: 10, State: Present, Value: []byte("old")},
			})
			if err := store.CommitAt(35, []Mutation{{Key: []byte("dead"), Value: []byte("forbidden")}}, CommitRelaxed); !errors.Is(err, ErrVersionBelowDiscardFloor) {
				t.Fatalf("commit at floor error=%v", err)
			}
			if err := store.CommitGroupAt([]CommitGroup{
				{Timestamp: 36, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("late36")}}},
				{Timestamp: 37, Mutations: []Mutation{{Key: []byte("dead"), Value: []byte("reborn")}}},
				{Timestamp: 50, Mutations: []Mutation{{Key: []byte("same"), Value: []byte("third")}}},
			}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			want := []Version{
				{Key: binaryKey, Timestamp: 45, State: Present, Value: []byte("binary45")},
				{Key: []byte("a"), Timestamp: 55, State: Present, Value: []byte("a55")},
				{Key: []byte("a"), Timestamp: 50, State: Present, Value: []byte("a50")},
				{Key: []byte("a"), Timestamp: 40, State: Tombstone},
				{Key: []byte("a"), Timestamp: 36, State: Present, Value: []byte("late36")},
				{Key: []byte("a"), Timestamp: 30, State: Present, Value: []byte("a30")},
				{Key: []byte("dead"), Timestamp: 37, State: Present, Value: []byte("reborn")},
				{Key: []byte("future"), Timestamp: 60, State: Present, Value: []byte("tail")},
				{Key: []byte("future"), Timestamp: 10, State: Present, Value: []byte("base")},
				{Key: []byte("same"), Timestamp: 50, State: Present, Value: []byte("third")},
				{Key: []byte("same"), Timestamp: 20, State: Present, Value: []byte("anchor")},
			}
			check := func() {
				t.Helper()
				if floor, err := store.DiscardFloor(); err != nil || floor != 35 {
					t.Fatalf("floor=%d error=%v", floor, err)
				}
				if _, err := store.GetAt([]byte("a"), 35); !errors.Is(err, ErrReadBeforeDiscardFloor) {
					t.Fatalf("floor read error=%v", err)
				}
				nativePruneRequireVersions(t, nativePruneCollect(t, store, VersionIteratorOptions{}), want)
				reversed := append([]Version(nil), want...)
				for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
					reversed[left], reversed[right] = reversed[right], reversed[left]
				}
				nativePruneRequireVersions(t, nativePruneCollect(t, store, VersionIteratorOptions{Reverse: true}), reversed)
				for _, obsolete := range []struct {
					key []byte
					ts  uint64
				}{{[]byte("a"), 10}, {[]byte("a"), 20}, {[]byte("dead"), 10}, {[]byte("dead"), 30}, {[]byte("same"), 10}, {binaryKey, 15}, {binaryKey, 25}} {
					physical, err := mvcckey.Encode(obsolete.key, obsolete.ts)
					if err != nil {
						t.Fatal(err)
					}
					value, err := db.Get(physical)
					if err != nil || value != nil {
						t.Fatalf("obsolete physical record survived/resurrected: %x@%d value=%x error=%v", obsolete.key, obsolete.ts, value, err)
					}
				}
				requireResult(t, store, []byte("a"), 36, Present, 36, []byte("late36"))
				requireResult(t, store, []byte("dead"), 36, Absent, 0, nil)
				requireResult(t, store, []byte("dead"), 37, Present, 37, []byte("reborn"))
				requireResult(t, store, []byte("same"), math.MaxUint64, Present, 50, []byte("third"))
			}
			check()
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db = nativeOrderedPruneOpen(t, profile, dir)
			store = New(db)
			check()
			if again, err := store.PruneVersions(PruneOptions{BatchSize: 2, Mode: CommitDurable}); err != nil || again.Pruned != 0 {
				t.Fatalf("repeat prune after reopen=%+v error=%v", again, err)
			}
		})
	}
}

func TestNativeOrderedPruneInterruptedTombstonePrefix(t *testing.T) {
	dir := t.TempDir()
	db := nativeOrderedPruneOpen(t, treedb.ProfileCommandWALDurable, dir)
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	store := New(db)
	groups := make([]CommitGroup, 67)
	for n := range groups {
		ts := uint64(n + 1)
		groups[n] = CommitGroup{Timestamp: ts, Mutations: []Mutation{{Key: []byte("history"), Value: []byte(fmt.Sprintf("v%d", ts))}}}
	}
	groups[64].Mutations[0] = Mutation{Key: []byte("history"), Delete: true}
	if err := store.CommitGroupAt(groups, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(65, CommitDurable); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("interrupt ordered physical prune")
	failing := &pruneFailureDB{DB: db, failWriteSyncAt: 3, err: injected}
	if _, err := newStore(failing).PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable}); !errors.Is(err, injected) {
		t.Fatalf("interrupted prune error=%v", err)
	}
	// Older physical versions may remain, but the retained logical tombstone
	// must still shadow all of them until the full prefix has been erased.
	tombstoneKey, err := mvcckey.Encode([]byte("history"), 65)
	if err != nil {
		t.Fatal(err)
	}
	if record, err := db.Get(tombstoneKey); err != nil || !bytes.Equal(record, []byte{recordTombstoneV1}) {
		t.Fatalf("interruption removed tombstone before older prefix: record=%x error=%v", record, err)
	}
	requireResult(t, store, []byte("history"), 66, Present, 66, []byte("v66"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db = nativeOrderedPruneOpen(t, treedb.ProfileCommandWALDurable, dir)
	store = New(db)
	if _, err := store.PruneVersions(PruneOptions{BatchSize: 7, Mode: CommitDurable}); err != nil {
		t.Fatal(err)
	}
	want := []Version{{Key: []byte("history"), Timestamp: 67, State: Present, Value: []byte("v67")}, {Key: []byte("history"), Timestamp: 66, State: Present, Value: []byte("v66")}}
	nativePruneRequireVersions(t, nativePruneCollect(t, store, VersionIteratorOptions{}), want)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	nativePruneRequireVersions(t, nativePruneCollect(t, store, VersionIteratorOptions{}), want)
}
