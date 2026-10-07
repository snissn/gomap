package mvcc

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestCOWSustainedPublicMVCCConstruction(t *testing.T) {
	for _, p := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, m := range []string{"cow_btree", "append_only", "btree"} {
			for _, ptr := range []bool{false, true} {
				for _, n := range []int{512, 1024} {
					t.Run(fmt.Sprintf("%s/%s/pointer_%t/N%d", p, m, ptr, n), func(t *testing.T) {
						r, err := c4Run(t, p, m, ptr, n, 1)
						if err != nil {
							if err != errC4NoPublicOverlap {
								r.LifecycleOutcome = "error"
								r.LifecycleError = err.Error()
								if emitErr := c4Emit(r); emitErr != nil {
									t.Error(emitErr)
								}
								t.Fatal(err)
							}
							if r.LifecycleOutcome != "refused" || r.LifecycleError != errC4NoPublicOverlap.Error() || r.OverlappingReaders != 0 {
								t.Fatal("invalid zero-overlap refusal")
							}
							t.Log("complete finite lifecycle; overlap qualification refused")
						}
						// A zero-overlap result is admissible only as unit construction,
						// after the same actual release, durability and reopen checks.
						closed, released := 0, 0
						for _, call := range r.Calls {
							if call.Outcome != "success" {
								t.Fatalf("lifecycle call failed: %+v", call)
							}
							if call.Operation == "Close" {
								closed++
							}
							if call.Phase == "old_pin_release" && call.Operation == "IterateVersions.consume_close" {
								released++
							}
						}
						if closed != 2 || released != 2 || len(r.OracleReceipts) == 0 || r.OracleReceipts[len(r.OracleReceipts)-1] != "reopen:complete_point_history_payload" {
							t.Fatal("incomplete release/Close/reopen construction")
						}
						if m == "cow_btree" {
							for _, boundary := range r.Boundaries {
								if boundary.Phase != "released" && boundary.Phase != "preclose" && boundary.Phase != "reopened" {
									continue
								}
								if err := c4QuiescentCOWOwners(boundary.Stats); err != nil {
									t.Fatalf("%s: %v", boundary.Phase, err)
								}
							}
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

// The open database owns its published cut and cache/basis/cut leases. With
// side stores disabled, each current or frozen generation owns one resolver
// lease. Quiescent boundaries must have no reader views or extra owners.
func c4QuiescentCOWOwners(stats map[string]string) error {
	values := make(map[string]uint64, 6)
	for _, key := range []string{"views", "active_cuts", "generations", "current_roots", "frozen_roots", "external_leases"} {
		value, err := strconv.ParseUint(stats["treedb.cache.cow."+key], 10, 64)
		if err != nil {
			return fmt.Errorf("quiescent owner counter %s: %w", key, err)
		}
		values[key] = value
	}
	if values["views"] != 0 || values["active_cuts"] != 1 || values["current_roots"] == 0 ||
		values["generations"] < values["current_roots"] || values["generations"]-values["current_roots"] != values["frozen_roots"] ||
		values["external_leases"] < 3 || values["external_leases"]-3 != values["generations"] {
		return fmt.Errorf("quiescent COW owners do not match the published database cut: %v", values)
	}
	return nil
}

func TestCOWSustainedPublicMVCCOwnerAccounting(t *testing.T) {
	base := map[string]string{
		"views": "0", "active_cuts": "1", "generations": "4",
		"current_roots": "4", "frozen_roots": "0", "external_leases": "7",
	}
	for _, mutation := range []struct{ key, value string }{
		{}, {"views", "1"}, {"active_cuts", "0"}, {"active_cuts", "2"},
		{"generations", "5"}, {"current_roots", "0"}, {"frozen_roots", "1"},
		{"external_leases", "8"}, {"external_leases", "6"}, {"external_leases", ""},
	} {
		stats := make(map[string]string, len(base))
		for key, value := range base {
			stats["treedb.cache.cow."+key] = value
		}
		if mutation.key != "" {
			stats["treedb.cache.cow."+mutation.key] = mutation.value
		}
		if err := c4QuiescentCOWOwners(stats); (err != nil) != (mutation.key != "") {
			t.Fatalf("owner mutation %+v: %v", mutation, err)
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

// A readable value in the opposite physical layout must refuse qualification
// and release its actual snapshot before the same database can be inspected again.
func TestCOWSustainedPublicMVCCRepresentationRefusalClosesSnapshot(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, pointers := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointer_%t", profile, pointers), func(t *testing.T) {
				db, err := treedb.Open(c4Options(profile, "cow_btree", pointers, t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				}()
				store := New(db)
				keys := c4Keys(16)
				mutations := make([]Mutation, len(keys))
				for i, key := range keys {
					mutations[i] = Mutation{Key: key, Value: c4Value(0)}
				}
				if err := store.CommitAt(1, mutations, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
				before := db.Stats()
				r := &c4Record{Mode: "cow_btree", Calls: make([]c4Call, 0, 64), origin: time.Now()}
				if err := r.proveLayout(db, "wrong_layout", c4Expected(keys, 0), !pointers); err == nil {
					t.Fatal("readable opposite representation accepted")
				}
				after := db.Stats()
				for _, k := range []string{"treedb.cache.cow.views", "treedb.cache.cow.active_cuts", "treedb.cache.cow.external_leases"} {
					if before[k] != after[k] {
						t.Fatalf("refused diagnostic leaked %s: %s -> %s", k, before[k], after[k])
					}
				}
				last := r.Calls[len(r.Calls)-1]
				if len(r.LayoutProofs) != 0 || last.Operation != "Snapshot.Close" || last.Outcome != "success" {
					t.Fatal("refused proof omitted its successful actual snapshot close")
				}
				if _, err := c4History(store, c4Expected(keys, 0), 0); err != nil {
					t.Fatal(err)
				}
				if err := r.proveLayout(db, "correct_layout", c4Expected(keys, 0), pointers); err != nil {
					t.Fatal(err)
				}
			})
		}
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

// This sentinel is deliberately untimed: it crosses periodic and idle defaults
// while real persistent-pointer seed owners survive a changed current image.
var c4MaintenanceOutputDir = flag.String("cow-c4-maintenance-output-dir", "", "new existing absolute directory for untimed maintenance admission receipts")

func TestCOWSustainedPublicMVCCManualMaintenancePins(t *testing.T) {
	for _, p := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(p), func(t *testing.T) {
			opts := c4Options(p, "cow_btree", true, t.TempDir())
			if err := c4AdmitOptions(opts); err != nil {
				t.Fatal(err)
			}
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := New(db)
			keys := c4Keys(16)
			var snapshots []c4Boundary
			observe := func(phase string) map[string]string {
				stats := db.Stats()
				if e := c4AdmitMaintenance(stats); e != nil {
					t.Fatal(e)
				}
				if e := cowPublicACKRouting(p, stats); e != nil {
					t.Fatal(e)
				}
				snapshots = append(snapshots, c4Boundary{phase, stats})
				return stats
			}
			observe("opened")
			muts := make([]Mutation, len(keys))
			for i, k := range keys {
				muts[i] = Mutation{Key: k, Value: c4Value(0)}
			}
			if err = store.CommitAt(1, muts, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			layoutRecord := &c4Record{Mode: "cow_btree", Calls: make([]c4Call, 0, 32), origin: time.Now()}
			if err = layoutRecord.proveLayout(db, "seed_layout", c4Expected(keys, 0), true); err != nil {
				t.Fatal(err)
			}
			pins := make([]*VersionIterator, 2)
			for i := range pins {
				pins[i], err = store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer pins[i].Close()
			}
			for i, k := range keys {
				muts[i] = Mutation{Key: k, Value: c4Value(1)}
			}
			if err = store.CommitAt(2, muts, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			before := observe("pinned_before_ticks")
			begin := time.Now()
			timer := time.NewTimer(3200 * time.Millisecond)
			defer timer.Stop()
			ticker := time.NewTicker(400 * time.Millisecond)
			defer ticker.Stop()
		holding:
			for {
				select {
				case <-timer.C:
					break holding
				case <-ticker.C:
					stats := observe("pinned_tick")
					for _, k := range []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total", "treedb.commit_seq"} {
						if stats[k] != before[k] {
							t.Fatalf("untimed tick changed %s", k)
						}
					}
					got, e := store.GetAt(keys[0], 100)
					if e != nil || got.Timestamp != 2 || !bytes.Equal(got.Value, c4Value(1)) {
						t.Fatalf("current pointer oracle %+v %v", got, e)
					}
				}
			}
			elapsed := time.Since(begin)
			if elapsed < 3200*time.Millisecond {
				t.Fatal("sentinel did not cross maintenance ticks")
			}
			for _, pin := range pins {
				if _, err = c4CheckIterator(pin, c4Expected(keys, 0)); err != nil {
					t.Fatal(err)
				}
			}
			released := observe("released")
			for _, k := range []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total"} {
				if released[k] != before[k] {
					t.Fatalf("pin release changed %s", k)
				}
			}
			if released["treedb.cache.cow.views"] != "0" {
				t.Fatal("seed owner leak")
			}
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			preclose := observe("preclose")
			a, e := strconv.ParseUint(released["treedb.cache.checkpoint.runs"], 10, 64)
			b, f := strconv.ParseUint(preclose["treedb.cache.checkpoint.runs"], 10, 64)
			if e != nil || f != nil || b != a+1 {
				t.Fatal("manual preclose checkpoint count")
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store = New(db)
			for _, k := range keys {
				got, e := store.GetAt(k, 100)
				if e != nil || got.Timestamp != 2 || !bytes.Equal(got.Value, c4Value(1)) {
					t.Fatalf("reopen pointer oracle %+v %v", got, e)
				}
			}
			reopened := observe("reopened")
			if reopened["treedb.cache.checkpoint.runs"] != "0" {
				t.Fatal("reopen read checkpoint")
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			if *c4MaintenanceOutputDir != "" {
				dir := *c4MaintenanceOutputDir
				info, e := os.Lstat(dir)
				if e != nil || !filepath.IsAbs(dir) || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					t.Fatal("invalid sentinel receipt directory")
				}
				receipt := map[string]any{"schema_version": 2, "profile": p, "requested_options": map[string]any{"value_log_generation_policy": opts.ValueLog.Generational.Policy, "background_checkpoint_interval": int64(opts.BackgroundCheckpointInterval), "background_checkpoint_idle_duration": int64(opts.BackgroundCheckpointIdleDuration), "max_wal_bytes": opts.MaxWALBytes, "background_index_vacuum_interval": int64(opts.BackgroundIndexVacuumInterval)}, "elapsed_ns": elapsed.Nanoseconds(), "boundaries": snapshots, "old_pin_history": "immutable_seed", "layout_proofs": layoutRecord.LayoutProofs, "diagnostic_calls": layoutRecord.Calls, "reopened_pointer_payloads": len(keys), "final_close": "success", "timed_claim": false}
				data, e := json.MarshalIndent(receipt, "", "  ")
				if e != nil {
					t.Fatal(e)
				}
				file, e := os.OpenFile(filepath.Join(dir, string(p)+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if e != nil {
					t.Fatal(e)
				}
				_, e = file.Write(data)
				e = errors.Join(e, file.Close())
				if e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
func TestCOWSustainedPublicMVCCMaintenanceAdmissionRefusal(t *testing.T) {
	base := c4Options(treedb.ProfileNoWALFast, "cow_btree", true, t.TempDir())
	changes := []func(*treedb.Options){func(o *treedb.Options) { o.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold }, func(o *treedb.Options) { o.BackgroundCheckpointInterval = 0 }, func(o *treedb.Options) { o.BackgroundCheckpointIdleDuration = 0 }, func(o *treedb.Options) { o.MaxWALBytes = 0 }, func(o *treedb.Options) { o.BackgroundIndexVacuumInterval = 0 }}
	for i, change := range changes {
		opts := base
		change(&opts)
		if c4AdmitOptions(opts) == nil {
			t.Fatalf("requested option %d accepted", i)
		}
	}
	// Real Open resolves the incompatible policy; actual Stats must be rejected.
	actual := base
	actual.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
	db, err := treedb.Open(actual)
	if err != nil {
		t.Fatal(err)
	}
	admission := c4AdmitMaintenance(db.Stats())
	closeErr := db.Close()
	if admission == nil || closeErr != nil {
		t.Fatalf("actual resolved refusal %v close %v", admission, closeErr)
	}
}
