package mvcc_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

// This covers checkpoint and clean-close recovery of the public floor contract.
// It pins the current COW prune refusal, not native deletion or crash/WAL replay.
// Future native prune eligibility will need a positive maintenance contract.
func TestPublicCOWDiscardFloorCheckpointReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, pointers := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointers=%t", profile, pointers), func(t *testing.T) {
				opts := treedb.OptionsFor(profile, t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.MemtableShards = 4
				opts.DisableSideStores = true
				opts.BackgroundCheckpointInterval = -1
				opts.ValueLog.ForcePointers = pointers
				opts.ValueLog.PointerThreshold = 1 << 30
				if pointers {
					opts.ValueLog.PointerThreshold = 1
				}
				oldValue := bytes.Repeat([]byte("old"), 64)
				newValue := bytes.Repeat([]byte("new"), 64)
				wantHistory := []mvcc.Version{
					{Key: []byte("deleted"), Timestamp: 20, State: mvcc.Tombstone},
					{Key: []byte("deleted"), Timestamp: 10, State: mvcc.Present, Value: oldValue},
					{Key: []byte("survivor"), Timestamp: 30, State: mvcc.Present, Value: newValue},
					{Key: []byte("survivor"), Timestamp: 10, State: mvcc.Present, Value: oldValue},
				}
				func() {
					db, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					}()
					store := mvcc.New(db)
					if err := store.CommitGroupAt([]mvcc.CommitGroup{
						{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: []byte("deleted"), Value: oldValue}, {Key: []byte("survivor"), Value: oldValue}}},
						{Timestamp: 20, Mutations: []mvcc.Mutation{{Key: []byte("deleted"), Delete: true}}},
						{Timestamp: 30, Mutations: []mvcc.Mutation{{Key: []byte("survivor"), Value: newValue}}},
					}, mvcc.CommitDurable); err != nil {
						t.Fatal(err)
					}
					if err := store.AdvanceDiscardFloor(20, mvcc.CommitDurable); err != nil {
						t.Fatal(err)
					}
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}()

				for reopen := 1; reopen <= 2; reopen++ {
					t.Run(fmt.Sprintf("reopen=%d", reopen), func(t *testing.T) {
						db, err := treedb.Open(opts)
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							if err := db.Close(); err != nil {
								t.Error(err)
							}
						}()
						store := mvcc.New(db)
						// Prune must be the first Store operation: a fresh Store has not
						// loaded the persisted floor through DiscardFloor or a read.
						before := db.Stats()
						outcome, err := store.PruneVersions(mvcc.PruneOptions{BatchSize: 1, Mode: mvcc.CommitDurable})
						if !errors.Is(err, caching.ErrCOWUnsupported) || outcome != (mvcc.PruneStats{}) {
							t.Fatalf("prune outcome=%+v err=%v", outcome, err)
						}
						after := db.Stats()
						for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total", "treedb.cache.cow.publications_total", "treedb.cache.cow.capture_calls_total"} {
							old, oldOK := before[key]
							current, currentOK := after[key]
							if !oldOK || !currentOK {
								t.Fatalf("missing public counter %s: before=%t after=%t", key, oldOK, currentOK)
							}
							if _, err := strconv.ParseUint(old, 10, 64); err != nil {
								t.Fatalf("public counter %s=%q: %v", key, old, err)
							}
							if old != current {
								t.Fatalf("refused prune changed %s: %q (%t) -> %q (%t)", key, old, oldOK, current, currentOK)
							}
							t.Logf("refused prune left %s=%s unchanged", key, old)
						}
						if floor, err := store.DiscardFloor(); err != nil || floor != 20 {
							t.Fatalf("floor=%d err=%v", floor, err)
						}
						// Loading the persisted floor must exercise the same observable
						// capture path that the refused prune was required to avoid.
						captureKey := "treedb.cache.cow.capture_calls_total"
						capturesBefore, err := strconv.ParseUint(before[captureKey], 10, 64)
						if err != nil {
							t.Fatal(err)
						}
						capturesLoaded, err := strconv.ParseUint(db.Stats()[captureKey], 10, 64)
						if err != nil || capturesLoaded <= capturesBefore {
							t.Fatalf("floor loading capture witness: before=%d loaded=%d err=%v", capturesBefore, capturesLoaded, err)
						}
						t.Logf("floor loading increased %s: %d -> %d", captureKey, capturesBefore, capturesLoaded)
						if _, err := store.GetAt([]byte("deleted"), 20); !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
							t.Fatalf("floor-equal point read: %v", err)
						}
						it, err := store.IterateVersions(mvcc.VersionIteratorOptions{ReadTimestamp: 20})
						if it != nil {
							if closeErr := it.Close(); closeErr != nil {
								t.Error(closeErr)
							}
							t.Fatal("floor-equal scan returned an iterator")
						}
						if !errors.Is(err, mvcc.ErrReadBeforeDiscardFloor) {
							t.Fatalf("floor-equal scan: %v", err)
						}
						for _, rejectedTS := range []uint64{19, 20} {
							if err := store.CommitAt(rejectedTS, []mvcc.Mutation{{Key: []byte("rejected-point"), Value: newValue}}, mvcc.CommitDurable); !errors.Is(err, mvcc.ErrVersionBelowDiscardFloor) {
								t.Fatalf("CommitAt(%d): %v", rejectedTS, err)
							}
							// Put the admissible group first to catch partial publication
							// before a later below/equal-floor group is rejected.
							if err := store.CommitGroupAt([]mvcc.CommitGroup{
								{Timestamp: 40, Mutations: []mvcc.Mutation{{Key: []byte("survivor"), Value: []byte("rejected")}, {Key: []byte("rejected-group"), Value: newValue}}},
								{Timestamp: rejectedTS, Mutations: []mvcc.Mutation{{Key: []byte("rejected-floor"), Value: newValue}}},
							}, mvcc.CommitDurable); !errors.Is(err, mvcc.ErrVersionBelowDiscardFloor) {
								t.Fatalf("mixed group with timestamp %d: %v", rejectedTS, err)
							}
						}
						for _, want := range []struct {
							key    string
							ts     uint64
							result mvcc.Result
						}{
							{"deleted", 21, mvcc.Result{State: mvcc.Tombstone, Timestamp: 20}},
							{"survivor", 21, mvcc.Result{State: mvcc.Present, Timestamp: 10, Value: oldValue}},
							{"survivor", 100, mvcc.Result{State: mvcc.Present, Timestamp: 30, Value: newValue}},
							{"rejected-point", 100, mvcc.Result{State: mvcc.Absent}},
							{"rejected-group", 100, mvcc.Result{State: mvcc.Absent}},
							{"rejected-floor", 100, mvcc.Result{State: mvcc.Absent}},
						} {
							got, err := store.GetAt([]byte(want.key), want.ts)
							if err != nil || got.State != want.result.State || got.Timestamp != want.result.Timestamp || !bytes.Equal(got.Value, want.result.Value) {
								t.Fatalf("GetAt(%q, %d)=%+v err=%v want=%+v", want.key, want.ts, got, err, want.result)
							}
						}
						checkCOWFloorRecoveryHistory(t, store, wantHistory)
					})
				}
			})
		}
	}
}

func checkCOWFloorRecoveryHistory(t *testing.T, store *mvcc.Store, want []mvcc.Version) {
	t.Helper()
	it, err := store.IterateVersions(mvcc.VersionIteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Error(err)
		}
	}()
	index := 0
	for it.Valid() {
		got := it.Entry()
		if index >= len(want) {
			t.Fatalf("unexpected retained version: %+v", got)
		}
		expected := want[index]
		if !bytes.Equal(got.Key, expected.Key) || !bytes.Equal(got.Value, expected.Value) || got.Timestamp != expected.Timestamp || got.State != expected.State {
			t.Fatalf("history[%d]=%+v want=%+v", index, got, expected)
		}
		index++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if index != len(want) {
		t.Fatalf("retained versions=%d want=%d", index, len(want))
	}
}
