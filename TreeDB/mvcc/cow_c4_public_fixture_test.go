package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"strconv"
	"testing"
)

func TestCOWSustainedPublicMVCCConstruction(t *testing.T) {
	for _, p := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, m := range []string{"cow_btree", "append_only", "btree"} {
			for _, ptr := range []bool{false, true} {
				for _, n := range []int{512, 1024} {
					t.Run(fmt.Sprintf("%s/%s/pointer_%t/N%d", p, m, ptr, n), func(t *testing.T) {
						r, err := c4Run(t, p, m, ptr, n, 1)
						if err != nil {
							r.LifecycleOutcome = "error"
							r.LifecycleError = err.Error()
							if emitErr := c4Emit(r); emitErr != nil {
								t.Error(emitErr)
							}
							t.Fatal(err)
						}
						if len(r.Calls) > r.RecorderCapacity || r.NativeEligibility != "PENDING" || r.WholeMaintenanceCharge != "PENDING" {
							t.Fatal("invalid bounded recorder or qualification")
						}
						if err = c4Emit(r); err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}
func TestCOWSustainedPublicMVCCFiniteSchedule(t *testing.T) {
	for _, x := range []struct{ n, e int }{{0, 1}, {512, 0}, {512, 9}, {513, 1}} {
		if _, err := c4Run(t, treedb.ProfileNoWALFast, "cow_btree", false, x.n, x.e); err == nil {
			t.Fatal("invalid schedule accepted")
		}
	}
	r := &c4Record{Calls: make([]c4Call, 0)}
	if err := r.call("test", "operation", 0, func() (uint64, error) { return 0, nil }); err == nil {
		t.Fatal("recorder overflow accepted")
	}
	if err := c4Limits().Validate(); err != nil {
		t.Fatal(err)
	}
	keys := c4Keys(1024)
	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			t.Fatal("unordered keys")
		}
	}
	if len(c4Expected(keys, 1)) != 4096 {
		t.Fatal("necessary history count")
	}
}

// Actual public cut owners fill a finite positive view bound. Owners for the
// old/current oracles are constructed before exhaustion; refused point/history
// calls must preserve the genuine capacity error instead of taking a fallback.
func TestCOWSustainedPublicMVCCCutRefusalRecovery(t *testing.T) {
	for _, p := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, ptr := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointer_%t", p, ptr), func(t *testing.T) {
				opts := c4Options(p, "cow_btree", ptr, t.TempDir())
				opts.COWMemtableLimits.MaxViews = 16
				if err := opts.COWMemtableLimits.Validate(); err != nil {
					t.Fatal(err)
				}
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				}()
				s := New(db)
				keys := c4Keys(16)
				mutations := make([]Mutation, len(keys))
				for i, key := range keys {
					mutations[i] = Mutation{Key: key, Value: c4Value(0)}
				}
				if err = s.CommitAt(1, mutations, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
				old, err := s.IterateVersions(VersionIteratorOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				var cuts []treedb.MVCCReadSnapshot
				defer func() {
					for _, c := range cuts {
						if err := c.Close(); err != nil {
							t.Error(err)
						}
					}
				}()
				for attempt := 0; attempt < 32; attempt++ {
					cut, x := db.AcquireMVCCReadCut()
					if x != nil {
						err = x
						break
					}
					cuts = append(cuts, cut)
				}
				if !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("finite cut refusal: %v", err)
				}
				if _, err = s.GetAt(keys[0], 100); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("point fallback: %v", err)
				}
				if _, err = s.IterateVersions(VersionIteratorOptions{}); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("history fallback: %v", err)
				}
				// Existing iterator owns its full resource set independently of new capture.
				if _, err = c4CheckIterator(old, c4Expected(keys, 0)); err != nil {
					t.Fatal(err)
				}
				for _, c := range cuts {
					if err = c.Close(); err != nil {
						t.Fatal(err)
					}
				}
				cuts = nil
				for attempt := 0; attempt < 4; attempt++ {
					_, err = s.GetAt(keys[0], 100)
					if err == nil {
						break
					}
					if !errors.Is(err, memtable.ErrCOWCapacity) {
						t.Fatal(err)
					}
				}
				if err != nil {
					t.Fatalf("released capacity did not resume: %v", err)
				}
				if _, err = c4History(s, c4Expected(keys, 0), 0); err != nil {
					t.Fatal(err)
				}
				if err = db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err = s.GetAt(keys[0], 100); !errors.Is(err, treedb.ErrClosed) {
					t.Fatalf("post-Close read: %v", err)
				}
			})
		}
	}
}

// Bare public cuts charge their concrete snapshot/view storage against total
// bytes (cow_view.go), whereas batch staging admits owner/entry/copy storage
// against the same total authority (cow_batch_storage.go). Exhaust the former
// with real owners; a substantially larger ordinary group must refuse before
// command-WAL append and resume once those exact owners are closed.
func TestCOWSustainedPublicMVCCWriteRefusalRecovery(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, ptr := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointer_%t", profile, ptr), func(t *testing.T) {
				opts := c4Options(profile, "cow_btree", ptr, t.TempDir())
				opts.COWMemtableLimits.MaxViews = 65536
				opts.COWMemtableLimits.MaxGenerationBytes = 1 << 20
				opts.COWMemtableLimits.MaxTotalBytes = 4 << 20
				opts.COWMemtableLimits.MaxRetiredBytes = 4 << 20
				opts.COWMemtableLimits.MaxInFlightBytes = 1 << 20
				if err := opts.COWMemtableLimits.Validate(); err != nil {
					t.Fatal(err)
				}
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if x := db.Close(); x != nil {
						t.Error(x)
					}
				}()
				store := New(db)
				keys := c4Keys(16)
				seed := make([]Mutation, 16)
				for i, key := range keys {
					seed[i] = Mutation{Key: key, Value: c4Value(0)}
				}
				if err = store.CommitAt(1, seed, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
				old, err := store.IterateVersions(VersionIteratorOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer old.Close()
				// Prime the existing oracle owner's full value-decoding resources before
				// pressure; identical seed records share this bounded workspace.
				if !old.Valid() || !bytes.Equal(old.EntryView().Value, c4Value(0)) {
					t.Fatal("old pin oracle unavailable")
				}
				floor, err := store.DiscardFloor()
				if err != nil {
					t.Fatal(err)
				}
				pressureStart := db.Stats()
				cuts := make([]treedb.MVCCReadSnapshot, 0, 16384)
				defer func() {
					for _, cut := range cuts {
						if x := cut.Close(); x != nil {
							t.Error(x)
						}
					}
				}()
				for attempt := 0; attempt < 16384; attempt++ {
					cut, x := db.AcquireMVCCReadCut()
					if x != nil {
						err = x
						break
					}
					cuts = append(cuts, cut)
				}
				if !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("public total-budget pressure not reached under bound: %v cuts=%d", err, len(cuts))
				}
				before := db.Stats()
				startCharge, x := strconv.ParseUint(pressureStart["treedb.cache.cow.total_bytes"], 10, 64)
				if x != nil {
					t.Fatal(x)
				}
				endCharge, x := strconv.ParseUint(before["treedb.cache.cow.total_bytes"], 10, 64)
				if x != nil || endCharge <= startCharge {
					t.Fatal("public owners lack total charge growth")
				}
				views, x := strconv.ParseUint(before["treedb.cache.cow.views"], 10, 64)
				if x != nil || views >= uint64(opts.COWMemtableLimits.MaxViews) {
					t.Fatal("pressure was view-count limit rather than total bytes")
				}
				t.Logf("public pin pressure owners=%d added_total_bytes=%d charged_bytes_per_owner=%g max_total_bytes=%d", len(cuts), endCharge-startCharge, float64(endCharge-startCharge)/float64(len(cuts)), opts.COWMemtableLimits.MaxTotalBytes)
				groups := []CommitGroup{{Timestamp: 100}, {Timestamp: 101}}
				for g := range groups {
					for i := 0; i < 16; i++ {
						groups[g].Mutations = append(groups[g].Mutations, Mutation{Key: []byte(fmt.Sprintf("denied/%d/%02d", g, i)), Value: c4Value(1)})
					}
				}
				err = store.CommitGroupAt(groups, CommitRelaxed)
				if !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("ordinary group did not preserve prepublication capacity refusal: %v", err)
				}
				after := db.Stats()
				for _, key := range []string{"treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total", "treedb.cache.cow.publications_total"} {
					if before[key] != after[key] {
						t.Fatalf("refused group changed %s: %s -> %s", key, before[key], after[key])
					}
				}
				if got, x := store.DiscardFloor(); x != nil || got != floor {
					t.Fatalf("refused group changed floor: %d %v", got, x)
				}
				if _, err = c4CheckIterator(old, c4Expected(keys, 0)); err != nil {
					t.Fatalf("old oracle under pressure: %v", err)
				}
				for _, cut := range cuts {
					if err = cut.Close(); err != nil {
						t.Fatal(err)
					}
				}
				cuts = nil
				for _, group := range groups {
					for _, mutation := range group.Mutations {
						got, x := store.GetAt(mutation.Key, 1000)
						if x != nil || got.State != Absent {
							t.Fatalf("denied mutation became visible: %+v %v", got, x)
						}
					}
				}
				if _, err = c4History(store, c4Expected(keys, 0), 0); err != nil {
					t.Fatalf("current oracle after refusal: %v", err)
				}
				for attempt := 0; attempt < 4; attempt++ {
					err = store.CommitGroupAt(groups, CommitRelaxed)
					if err == nil {
						break
					}
					if !errors.Is(err, memtable.ErrCOWCapacity) {
						t.Fatal(err)
					}
				}
				if err != nil {
					t.Fatalf("exact owner release did not relieve group refusal: %v", err)
				}
				for _, group := range groups {
					for _, mutation := range group.Mutations {
						got, x := store.GetAt(mutation.Key, 1000)
						if x != nil || got.State != Present || got.Timestamp != group.Timestamp || !bytes.Equal(got.Value, mutation.Value) {
							t.Fatalf("retried group oracle: %+v %v", got, x)
						}
					}
				}
				if err = db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
