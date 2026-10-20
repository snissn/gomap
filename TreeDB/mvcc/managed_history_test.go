package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func managedHistoryOptions(profile treedb.Profile, dir string, pointers bool) treedb.Options {
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.ForcePointers = pointers
	opts.ValueLog.PointerThreshold = 1 << 30
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	return opts
}

func TestManagedHistoryFloorAdvancePreservesOpenCut(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, pointers := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointers=%t", profile, pointers), func(t *testing.T) {
				opts := managedHistoryOptions(profile, t.TempDir(), pointers)
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if db != nil {
						_ = db.Close()
					}
				})
				store := New(db)
				// Arrival order is deliberately independent of application time.
				commitHistory(t, store, "a", MutationAt{30, []byte("a30"), false}, MutationAt{10, []byte("a10"), false}, MutationAt{20, []byte("a20"), false})
				commitHistory(t, store, "b", MutationAt{20, nil, true}, MutationAt{10, []byte("b10"), false})
				old, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 15})
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				if err := store.AdvanceDiscardFloor(25, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
				stats, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
				if err != nil {
					t.Fatal(err)
				}
				if stats.Visited != 5 || stats.Pruned != 3 || stats.Retained != 2 {
					t.Fatalf("stats=%+v", stats)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				requireVersionLabels(t, collectVersions(t, old), "a@10:1:a10", "b@10:1:b10")
				if _, err := store.GetAt([]byte("a"), 25); !errors.Is(err, ErrReadBeforeDiscardFloor) {
					t.Fatalf("old request=%v", err)
				}
				requireResult(t, store, []byte("a"), 26, Present, 20, []byte("a20"))
				requireResult(t, store, []byte("b"), 26, Absent, 0, nil)
				// A late timestamp above the floor remains legal after pruning.
				if err := store.CommitAt(28, []Mutation{{Key: []byte("a"), Value: []byte("a28")}}, CommitDurable); err != nil {
					t.Fatal(err)
				}
				if err := store.CommitAt(25, []Mutation{{Key: []byte("a"), Value: []byte("bad")}}, CommitRelaxed); !errors.Is(err, ErrVersionBelowDiscardFloor) {
					t.Fatalf("floor write=%v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = nil
				db, err = treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				store = New(db)
				if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
					t.Fatalf("floor=%d %v", floor, err)
				}
				requireResult(t, store, []byte("a"), 29, Present, 28, []byte("a28"))
				requireResult(t, store, []byte("a"), 30, Present, 30, []byte("a30"))
				requireResult(t, store, []byte("b"), 26, Absent, 0, nil)
			})
		}
	}
}

func TestManagedHistoryTombstoneAnchorCrashOrder(t *testing.T) {
	const childEnv = "TREEDB_MANAGED_HISTORY_CRASH_CUT"
	const dirEnv = "TREEDB_MANAGED_HISTORY_CRASH_DIR"
	const pointerEnv = "TREEDB_MANAGED_HISTORY_CRASH_POINTERS"
	if cut, _ := strconv.Atoi(os.Getenv(childEnv)); cut > 0 {
		db, err := treedb.Open(managedHistoryOptions(treedb.ProfileCommandWALDurable, os.Getenv(dirEnv), os.Getenv(pointerEnv) == "true"))
		if err != nil {
			t.Fatal(err)
		}
		store := New(db)
		commitHistory(t, store, "k", MutationAt{30, []byte("new"), false}, MutationAt{10, bytes.Repeat([]byte("old"), 100), false}, MutationAt{20, nil, true}, MutationAt{15, []byte("middle"), false})
		if err := store.AdvanceDiscardFloor(25, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
		// The wrapper observes public WriteSync boundaries only. First comes
		// floor persistence, then oldest-to-newest deletion, anchor last.
		crashing := &pruneCrashDB{DB: db, crashAfterWriteSync: int64(cut)}
		_, _ = newStore(crashing).PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
		os.Exit(91)
	}
	for _, pointers := range []bool{false, true} {
		for cut := 1; cut <= 4; cut++ {
			t.Run(fmt.Sprintf("pointers=%t/cut=%d", pointers, cut), func(t *testing.T) {
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestManagedHistoryTombstoneAnchorCrashOrder$", "-test.timeout=45s")
				cmd.Env = append(os.Environ(), childEnv+"="+strconv.Itoa(cut), dirEnv+"="+dir, pointerEnv+"="+strconv.FormatBool(pointers))
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("child: %v\n%s", err, output)
				}
				db, err := treedb.Open(managedHistoryOptions(treedb.ProfileCommandWALDurable, dir, pointers))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				store := New(db)
				if floor, err := store.DiscardFloor(); err != nil || floor != 25 {
					t.Fatalf("floor=%d %v", floor, err)
				}
				if _, err := store.GetAt([]byte("k"), 25); !errors.Is(err, ErrReadBeforeDiscardFloor) {
					t.Fatalf("old request=%v", err)
				}
				want := Tombstone
				if cut == 4 {
					want = Absent
				}
				got, err := store.GetAt([]byte("k"), 26)
				if err != nil || got.State != want {
					t.Fatalf("read=%+v %v want %v", got, err, want)
				}
				requireResult(t, store, []byte("k"), 30, Present, 30, []byte("new"))
				it, err := store.IterateVersions(VersionIteratorOptions{})
				if err != nil {
					t.Fatal(err)
				}
				versions := collectVersions(t, it)
				wantVersions := 5 - cut
				if len(versions) != wantVersions {
					t.Fatalf("versions=%v want %d", versionLabels(versions), wantVersions)
				}
				if _, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable}); err != nil {
					t.Fatal(err)
				}
				requireResult(t, store, []byte("k"), 26, Absent, 0, nil)
			})
		}
	}
}

// Pause only construction after the original cut has been retained. All
// goroutines use the shared harness, including failure cleanup and DB.Close.
func TestManagedHistoryPruneCapturedCutReleasesAdmission(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		t.Run(fmt.Sprintf("pointers=%t", pointers), func(t *testing.T) {
			h := c3ReadHarness(t, pointers)
			paused := &c3CutDB{DB: h.db, enter: make(chan struct{}), release: make(chan struct{}), iterator: true}
			store := newStore(paused)
			commitHistory(t, store, "k", MutationAt{10, []byte("old"), false}, MutationAt{20, nil, true})
			if err := store.AdvanceDiscardFloor(25, CommitDurable); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			h.release = func() { once.Do(func() { close(paused.release) }) }
			prune := h.start("prune", func() c3COWOutcome {
				_, err := store.PruneVersions(PruneOptions{BatchSize: 1, Mode: CommitDurable})
				return c3COWOutcome{err: err}
			})
			select {
			case <-paused.enter:
			case <-time.After(c3COWCallBound):
				t.Fatal("prune did not capture cut")
			}
			if !store.mu.TryRLock() {
				t.Fatal("prune retained foreground admission after capture")
			}
			store.mu.RUnlock()
			// This write cannot alter any <=floor record selected by prune.
			write := h.start("write above floor", func() c3COWOutcome {
				return c3COWOutcome{err: store.CommitAt(30, []Mutation{{Key: []byte("k"), Value: []byte("new")}}, CommitDurable)}
			})
			h.require(write)
			// Release the captured iterator only after the foreground ACK.
			h.release()
			h.require(prune)
			store.readCuts = h.db
			requireResult(t, store, []byte("k"), 26, Absent, 0, nil)
			requireResult(t, store, []byte("k"), 30, Present, 30, []byte("new"))
		})
	}
}
