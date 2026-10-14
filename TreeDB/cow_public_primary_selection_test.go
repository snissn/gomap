package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func publicPrimaryBasis(t *testing.T, snapshot Snapshot) backenddb.StateToken {
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

// Actual ordinary public defaults select PRIMARY only at the physical backend
// boundary. C1 foreground and checkpoint successor tokens remain distinct.
func TestCOWPublicPrimarySelectionLayeredPublication(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		for _, pointer := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pointer=%v", profile, pointer), func(t *testing.T) {
				database, opts, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
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
				if !primaryarena.IsPage(basis.RootPageID) {
					t.Fatal("main backend omitted PRIMARY selection")
				}
				if _, err := os.Stat(filepath.Join(opts.Dir, "maindb", "index.db.primary")); err != nil {
					t.Fatal(err)
				}
				if database.dictdb == nil {
					t.Fatal("default dictionary side store absent")
				}
				if state := database.dictdb.State(); primaryarena.IsPage(state.RootPageID) {
					t.Fatal("automatic main PRIMARY selection leaked into dictdb")
				}
				if _, err := os.Stat(filepath.Join(opts.Dir, "dictdb", "index.db.primary")); !os.IsNotExist(err) {
					t.Fatalf("default dict PRIMARY: %v", err)
				}
				if database.templateDB != nil {
					t.Fatal("default disabled template policy changed")
				}
				var mu sync.Mutex
				var authority []durabilitycut.Point
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
					if event.Path != filepath.Join(opts.Dir, "maindb", "index.db.primary") || event.StablePath == nil || event.Length != 3*int64(page.PageSize) || (event.Offset != 2*int64(page.PageSize) && event.Offset != 5*int64(page.PageSize)) {
						observationErr = fmt.Errorf("selected checkpoint authority event: %+v", event)
						return observationErr
					}
					authority = append(authority, event.Point)
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
				// prepared C1 cut; it must not force physical PRIMARY checkpoint on ACK.
				if state, _ := database.backend.StateToken(); state != basis {
					t.Fatalf("ACK changed physical basis: %v -> %v", basis, state)
				}
				dirty := database.AcquireSnapshot()
				if dirty == nil {
					t.Fatal("dirty C1 cut missing")
				}
				defer dirty.Close()
				if token := publicPrimaryBasis(t, dirty); token != basis {
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
					t.Fatalf("foreground path PRIMARY=%d C1=%d", beforeAuthority, beforeSwaps)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				successor, ok := database.backend.StateToken()
				if !ok || successor.CommitSeq <= basis.CommitSeq || !primaryarena.IsPage(successor.RootPageID) {
					t.Fatalf("physical successor %v", successor)
				}
				if token := publicPrimaryBasis(t, dirty); token != basis {
					t.Fatalf("old cut basis changed %v", token)
				}
				fresh := database.AcquireSnapshot()
				if fresh == nil {
					t.Fatal("successor cut missing")
				}
				if token := publicPrimaryBasis(t, fresh); token != successor {
					t.Fatalf("successor exact basis=%v want=%v", token, successor)
				}
				for _, key := range []string{"a", "b"} {
					cowPublicContractSnapshotValue(t, fresh, key, value, true)
				}
				_ = fresh.Close()
				mu.Lock()
				points := append([]durabilitycut.Point(nil), authority...)
				observedErr := observationErr
				mu.Unlock()
				restore()
				active = false
				if observedErr != nil {
					t.Fatal(observedErr)
				}
				want := []durabilitycut.Point{durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite, durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync}
				if len(points) != len(want) {
					t.Fatalf("PRIMARY promotion cuts %v", points)
				}
				for n := range want {
					if points[n] != want[n] {
						t.Fatalf("PRIMARY promotion order %v", points)
					}
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
				if token := publicPrimaryBasis(t, next); token != successor {
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
				if !primaryarena.IsPage(reopened.backend.State().RootPageID) {
					t.Fatal("reopen lost PRIMARY authority")
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

func TestCOWPublicPrimarySelectionModeAndExplicitFlag(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		for _, mode := range []string{"append_only", "btree", "cow_btree"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/explicit=%v", profile, mode, explicit), func(t *testing.T) {
					opts := OptionsFor(profile, t.TempDir())
					opts.MemtableMode = mode
					opts.MemtableShards = 2
					opts.IndexPrimaryDirectory = explicit
					opts.DisableSideStores = true
					opts.DisableBackgroundPrune = true
					database, err := Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					defer database.Close()
					selected := primaryarena.IsPage(database.backend.State().RootPageID)
					if want := explicit || mode == "cow_btree" || profile == ProfileNoWALFast; selected != want {
						t.Fatalf("actual selection=%v want=%v", selected, want)
					}
				})
			}
		}
	}
}

func TestCOWPublicPrimarySelectionNonemptyDATARequiresRebuild(t *testing.T) {
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
			if candidate != nil {
				candidate.Close()
				t.Fatal("nonempty DATA silently reinterpreted")
			}
			if !errors.Is(err, ErrLegacyFormatRebuildRequired) {
				t.Fatalf("rebuild refusal %v", err)
			}
			if _, err := os.Stat(filepath.Join(opts.Dir, "index.db.primary")); !os.IsNotExist(err) {
				t.Fatalf("refused open installed PRIMARY: %v", err)
			}
			opts.MemtableMode = "btree"
			original, err := Open(opts)
			if err != nil {
				t.Fatal("original mode reopen", err)
			}
			defer original.Close()
			if got, err := original.Get([]byte("legacy-data")); err != nil || string(got) != "preserved" {
				t.Fatalf("refusal changed old DATA %q %v", got, err)
			}
		})
	}
}

// Fallback convergence may publish a new physical PRIMARY root even when the
// cached checkpoint had no dirty entries. The next C1 write must inherit it.
func TestCOWPublicPrimaryEmptyCheckpointPreservesSuccessorBasis(t *testing.T) {
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
				oldBasis := publicPrimaryBasis(t, old)
				if err := database.Checkpoint(); err != nil {
					t.Fatal("empty checkpoint", err)
				}
				successor, ok := database.backend.StateToken()
				if !ok || !primaryarena.IsPage(successor.RootPageID) {
					t.Fatal("missing actual PRIMARY successor")
				}
				fresh := database.AcquireSnapshot()
				if fresh == nil {
					t.Fatal("missing fresh cut")
				}
				actual := publicPrimaryBasis(t, fresh)
				if err := fresh.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("empty checkpoint basis old=%v backend=%v fresh=%v", oldBasis, successor, actual)
				if actual != successor {
					t.Errorf("empty checkpoint left stale COW basis: got=%v want=%v", actual, successor)
				}
				if publicPrimaryBasis(t, old) != oldBasis {
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
