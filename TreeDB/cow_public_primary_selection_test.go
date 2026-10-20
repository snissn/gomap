package treedb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func publicPhysicalBasis(t *testing.T, snapshot Snapshot) backenddb.StateToken {
	t.Helper()
	owner, ok := snapshot.(interface {
		StateToken() (backenddb.StateToken, bool)
	})
	if !ok {
		t.Fatalf("actual snapshot %T lacks existing basis token", snapshot)
	}
	token, ok := owner.StateToken()
	if !ok {
		t.Fatal("basis token unavailable")
	}
	return token
}

// Actual ordinary public defaults select DATA at the physical backend
// boundary. C1 foreground and checkpoint successor tokens remain distinct.
func TestCOWPublicDATASelectionLayeredPublication(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		for _, pointer := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointer=%v", profile, pointer), func(t *testing.T) {
				database, opts, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
					if root := os.Getenv("GOMAP_P5_RETAIN_FIXTURE_ROOT"); root != "" {
						opts.Dir = filepath.Join(root, t.Name())
					}
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					opts.DisableBackgroundPrune = true
					if !pointer {
						opts.ValueLog.PointerThreshold = 1 << 20
						opts.ValueLog.ForcePointers = false
					}
				})
				if opts.IndexPrimaryDirectory {
					t.Fatal("fixture selected explicit PRIMARY flag")
				}
				basis, ok := database.backend.StateToken()
				if !ok {
					t.Fatal("backend basis missing")
				}
				if basis.RootPageID&(uint64(1)<<62) != 0 {
					t.Fatal("main backend omitted DATA selection")
				}
				if _, err := os.Stat(filepath.Join(opts.Dir, "maindb", "index.db.primary")); !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if database.dictdb == nil {
					t.Fatal("default dictionary side store absent")
				}
				if state := database.dictdb.State(); state.RootPageID&(uint64(1)<<62) != 0 {
					t.Fatal("automatic main PRIMARY selection leaked into dictdb")
				}
				if _, err := os.Stat(filepath.Join(opts.Dir, "dictdb", "index.db.primary")); !os.IsNotExist(err) {
					t.Fatalf("default dict PRIMARY: %v", err)
				}
				if database.templateDB != nil {
					t.Fatal("default disabled template policy changed")
				}
				var mu sync.Mutex
				var authority []durabilitycut.Event
				var swaps int
				var observationErr error
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Root != filepath.Join(opts.Dir, "maindb") && event.Root != filepath.Join(opts.Dir, "maindb", "wal") {
						return nil
					}
					mu.Lock()
					defer mu.Unlock()
					if event.Point == durabilitycut.BeforeCOWCutSwap && event.Root == filepath.Join(opts.Dir, "maindb", "wal") {
						swaps++
					}
					if event.Resource != durabilitycut.ResourceMeta || (event.Point != durabilitycut.BeforeMetaWrite && event.Point != durabilitycut.AfterMetaWrite && event.Point != durabilitycut.BeforeMetaSync && event.Point != durabilitycut.AfterMetaSync) {
						return nil
					}
					if event.Path != filepath.Join(opts.Dir, "maindb", "index.db") || ((event.Point == durabilitycut.BeforeMetaWrite || event.Point == durabilitycut.AfterMetaWrite) && (event.Length != int64(page.PageSize) || (event.Offset != 0 && event.Offset != int64(page.PageSize)))) {
						observationErr = fmt.Errorf("selected checkpoint authority event: %+v", event)
						return observationErr
					}
					authority = append(authority, event)
					return nil
				})
				active := true
				defer func() {
					if active {
						restore()
					}
				}()
				value := []byte("first")
				if pointer {
					value = bytes.Repeat([]byte("pointer/"), 600)
				}
				batch := database.NewBatch()
				for _, key := range []string{"a", "b"} {
					if err := batch.Set([]byte(key), value); err != nil {
						t.Fatal(err)
					}
				}
				if err := batch.WriteSync(); err != nil {
					t.Fatal(err)
				}
				_ = batch.Close()
				// Command-WAL sync acknowledges its stable WAL prefix, then swaps the
				// prepared C1 cut; it must not force physical DATA checkpoint on ACK.
				if state, _ := database.backend.StateToken(); state != basis {
					t.Fatalf("ACK changed physical basis: %v -> %v", basis, state)
				}
				dirty := database.AcquireSnapshot()
				if dirty == nil {
					t.Fatal("dirty C1 cut missing")
				}
				defer dirty.Close()
				if token := publicPhysicalBasis(t, dirty); token != basis {
					t.Fatalf("dirty cut rebased: %v", token)
				}
				for _, key := range []string{"a", "b"} {
					cowPublicContractSnapshotValue(t, dirty, key, value, true)
				}
				entry, err := dirty.GetEntry([]byte("a"))
				if err != nil || (entry.Flags&node.FlagPointer != 0) != pointer {
					t.Fatal("actual C1 value placement", err)
				}
				mu.Lock()
				beforeAuthority, beforeSwaps := len(authority), swaps
				mu.Unlock()
				if beforeAuthority != 0 || beforeSwaps != 1 {
					t.Fatalf("foreground path DATA=%d C1=%d", beforeAuthority, beforeSwaps)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				successor, ok := database.backend.StateToken()
				if !ok || successor.CommitSeq <= basis.CommitSeq || (successor.RootPageID&(uint64(1)<<62) != 0) {
					t.Fatalf("physical successor %v", successor)
				}
				if token := publicPhysicalBasis(t, dirty); token != basis {
					t.Fatalf("old cut basis changed %v", token)
				}
				fresh := database.AcquireSnapshot()
				if fresh == nil {
					t.Fatal("successor cut missing")
				}
				if token := publicPhysicalBasis(t, fresh); token != successor {
					t.Fatalf("successor exact basis=%v want=%v", token, successor)
				}
				for _, key := range []string{"a", "b"} {
					cowPublicContractSnapshotValue(t, fresh, key, value, true)
				}
				_ = fresh.Close()
				mu.Lock()
				events := append([]durabilitycut.Event(nil), authority...)
				observedErr := observationErr
				mu.Unlock()
				restore()
				active = false
				if observedErr != nil {
					t.Fatal(observedErr)
				}
				want := []durabilitycut.Point{durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite, durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync}
				if len(events) != 2*len(want) {
					t.Fatalf("DATA seal plus fallback convergence events %+v", events)
				}
				for n, event := range events {
					if event.Point != want[n%len(want)] {
						t.Fatalf("DATA seal order %+v", events)
					}
				}
				if events[0].Offset == events[4].Offset || events[0].Offset != events[1].Offset || events[4].Offset != events[5].Offset {
					t.Fatalf("DATA seals did not alternate exact slots: %+v", events)
				}
				// Both records are still in the final two slots. Decode after the
				// explicit checkpoint rather than reading inside a physical seal.
				f, err := os.Open(filepath.Join(opts.Dir, "maindb", "index.db"))
				if err != nil {
					t.Fatal(err)
				}
				var metas [2]page.DurableMetaV1
				var records [2]rootpublication.DurableRootRecordV1
				for n := range records {
					image := make([]byte, page.PageSize)
					if _, err := f.ReadAt(image, events[4*n].Offset); err != nil {
						_ = f.Close()
						t.Fatal(err)
					}
					if !page.VerifyChecksumNonMutating(image) {
						_ = f.Close()
						t.Fatal("DATA meta checksum")
					}
					meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
					if err != nil {
						_ = f.Close()
						t.Fatal(err)
					}
					if _, err := f.ReadAt(image, int64(meta.RootRecordPageID)*int64(page.PageSize)); err != nil {
						_ = f.Close()
						t.Fatal(err)
					}
					record, err := rootpublication.DecodeDurableRootRecordV1(image, meta.RootRecordPageID, meta.RootRecordDigest)
					if err != nil || record.CommitSeq != meta.CommitSeq || record.DurableSeq != meta.DurableSeq {
						_ = f.Close()
						t.Fatalf("DATA slot record: %+v %v", record, err)
					}
					metas[n], records[n] = meta, record
					t.Logf("checkpoint seal%d slot=%d commit=%d record=%d digest=%x roots=%d/%d appliedLSN=%d", n+1, events[4*n].Offset/int64(page.PageSize), record.CommitSeq, meta.RootRecordPageID, meta.RootRecordDigest, record.UserRootPageID, record.SystemRootPageID, record.AppliedCommandLSN)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				first, final := records[0], records[1]
				if first.CommitSeq <= basis.CommitSeq || final.CommitSeq != first.CommitSeq+1 || final.DurableSeq != first.DurableSeq+1 ||
					first.AppliedCommandLSN == 0 || final.AppliedCommandLSN != first.AppliedCommandLSN || final.UserRootPageID != first.UserRootPageID || final.SystemRootPageID != first.SystemRootPageID ||
					final.ParentCommitSeq != first.CommitSeq || final.ParentRecordPageID != metas[0].RootRecordPageID || final.ParentRecordDigest != metas[0].RootRecordDigest ||
					final.CommitSeq != successor.CommitSeq || final.UserRootPageID != successor.RootPageID || final.SystemRootPageID != successor.SystemRootPageID || final.AppliedCommandLSN != successor.AppliedCommandLSN {
					t.Fatalf("DATA checkpoint fallback did not preserve exact same-root/LSN convergence: first=%+v final=%+v basis=%+v", first, final, successor)
				}
				stats := database.Stats()
				handoffs, err := strconv.ParseUint(stats["treedb.cache.cow.handoffs_total"], 10, 64)
				if err != nil || handoffs == 0 {
					t.Fatalf("physical successor handoff %d %v", handoffs, err)
				}
				if err := database.Set([]byte("a"), []byte("second")); err != nil {
					t.Fatal(err)
				}
				cowPublicContractSnapshotValue(t, dirty, "a", value, true)
				next := database.AcquireSnapshot()
				if next == nil {
					t.Fatal("successor dirty C1 missing")
				}
				if token := publicPhysicalBasis(t, next); token != successor {
					t.Fatalf("next C1 basis=%v want=%v", token, successor)
				}
				cowPublicContractSnapshotValue(t, next, "a", []byte("second"), true)
				_ = next.Close()
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				cowPublicContractSnapshotValue(t, dirty, "a", value, true)
				_ = dirty.Close()
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal("ordinary reopen", err)
				}
				defer reopened.Close()
				if reopened.backend.State().RootPageID&(uint64(1)<<62) != 0 {
					t.Fatal("reopen lost DATA authority")
				}
				if got, err := reopened.Get([]byte("b")); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("reopened pointer/inline %d %v", len(got), err)
				}
				if got, err := reopened.Get([]byte("a")); err != nil || string(got) != "second" {
					t.Fatalf("reopen successor %q %v", got, err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal("ordinary reopened Close", err)
				}
			})
		}
	}
}

func TestCOWPublicDATASelectionReopensExistingDATA(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "btree"
			opts.DisableSideStores = true
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("legacy-data"), []byte("preserved")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 2
			candidate, err := Open(opts)
			if err != nil {
				t.Fatal("COW reopen of existing DATA", err)
			}
			defer candidate.Close()
			if candidate.backend.State().RootPageID&(uint64(1)<<62) != 0 {
				t.Fatal("existing DATA reopened as PRIMARY")
			}
			if got, err := candidate.Get([]byte("legacy-data")); err != nil || string(got) != "preserved" {
				t.Fatalf("DATA reopen changed value %q %v", got, err)
			}
		})
	}
}

// Fallback convergence may publish a new physical DATA root even when the
// cached checkpoint had no dirty entries. The next C1 write must inherit it.
func TestCOWPublicDATAEmptyCheckpointPreservesSuccessorBasis(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		for _, pointer := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointer=%v", profile, pointer), func(t *testing.T) {
				database, opts, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
					opts.DisableSideStores = true
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					opts.DisableBackgroundPrune = true
					if !pointer {
						opts.ValueLog.PointerThreshold = 1 << 20
						opts.ValueLog.ForcePointers = false
					}
				})
				first, second, third := []byte("first"), []byte("second"), []byte("third")
				if pointer {
					first = bytes.Repeat(first, 900)
					second = bytes.Repeat(second, 900)
					third = bytes.Repeat(third, 900)
				}
				for _, value := range [][]byte{first, second} {
					if err := database.Set([]byte("a"), value); err != nil {
						t.Fatal(err)
					}
					if err := database.Checkpoint(); err != nil {
						t.Fatal("dirty checkpoint", err)
					}
				}
				old := database.AcquireSnapshot()
				if old == nil {
					t.Fatal("missing old cut")
				}
				defer old.Close()
				oldBasis := publicPhysicalBasis(t, old)
				if err := database.Checkpoint(); err != nil {
					t.Fatal("empty checkpoint", err)
				}
				successor, ok := database.backend.StateToken()
				if !ok || (successor.RootPageID&(uint64(1)<<62) != 0) {
					t.Fatal("missing actual DATA successor")
				}
				fresh := database.AcquireSnapshot()
				if fresh == nil {
					t.Fatal("missing fresh cut")
				}
				actual := publicPhysicalBasis(t, fresh)
				if err := fresh.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("empty checkpoint basis old=%v backend=%v fresh=%v", oldBasis, successor, actual)
				if actual != successor {
					t.Errorf("empty checkpoint left stale COW basis: got=%v want=%v", actual, successor)
				}
				if publicPhysicalBasis(t, old) != oldBasis {
					t.Fatal("empty checkpoint mutated retained old cut")
				}
				cowPublicContractSnapshotValue(t, old, "a", second, true)
				if err := database.Set([]byte("a"), third); err != nil {
					t.Fatal("post-checkpoint write", err)
				}
				cowPublicContractSnapshotValue(t, old, "a", second, true)
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				if err := database.Close(); err != nil {
					t.Fatal("checked Close after empty checkpoint and write", err)
				}
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal("reopen", err)
				}
				defer reopened.Close()
				if got, err := reopened.Get([]byte("a")); err != nil || !bytes.Equal(got, third) {
					t.Fatalf("reopen value length=%d err=%v", len(got), err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal("reopened Close", err)
				}
			})
		}
	}
}
