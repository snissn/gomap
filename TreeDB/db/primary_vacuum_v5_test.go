package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// createSavedPrimaryFormatV5 writes a complete genuine V5 initial root using
// the production initializer. Fresh public databases select V6; these fixtures
// keep qualifying the retained V5 shared-arena path without changing defaults.
func createSavedPrimaryFormatV5(t *testing.T, dir string, directory bool) {
	t.Helper()
	p, err := pager.Open(filepath.Join(dir, indexFileName), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	a, err := primaryarena.Open(filepath.Join(dir, primaryIndexFileName))
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	if err = a.SetMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5); err != nil {
		t.Fatal(err)
	}
	if err = p.AttachPrimaryBankPager(a.Pager()); err != nil {
		t.Fatal(err)
	}
	alloc := freelist.New(p, 0)
	z := zipper.New(p, alloc)
	z.SetPrimaryArena(a)
	g := newIndexGen(1, p, alloc, z)
	g.primary = a
	g.primaryOwner, err = newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	d := &DB{dir: dir, dependencyDirectoryRequiredFeature: directory}
	if err = d.initializeDurablePrimaryV5(g); err != nil {
		t.Fatal(err)
	}
	if a.CapsuleFormatV6() {
		t.Fatal("saved fixture unexpectedly selects V6")
	}
	releasePrimaryRuntimeV5(a, d.durableRoot.primary)
	for _, r := range d.durableRoot.slotResources {
		r.Release()
	}
	if err = g.close(); err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryVacuumSharedArenaHeldSnapshotCutAndIndependentSlots(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	for _, directory := range []bool{false, true} {
		name := "manifest"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if directory {
				if err := SaveFormatConfig(dir, FormatConfig{DurabilityProfile: ProfileNoWALFast, RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}); err != nil {
					t.Fatal(err)
				}
			}
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 128}}
			createSavedPrimaryFormatV5(t, dir, directory)
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if database != nil {
					_ = database.Close()
				}
			}()
			values := [][]byte{bytes.Repeat([]byte("first"), 100), bytes.Repeat([]byte("second"), 100)}
			pointers := appendPointersInNewSegment(t, dir, 0, 1, 10000, 2, func(i int) []byte { return values[i] })
			if err = database.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			put := func(key string, ptr page.ValuePtr) {
				t.Helper()
				batch := database.NewBatch().(*Batch)
				defer batch.Close()
				if err := batch.SetPointer([]byte(key), ptr); err != nil {
					t.Fatal(err)
				}
				if err := batch.WriteSync(); err != nil {
					t.Fatal(err)
				}
			}
			put("first", pointers[0])
			held := database.AcquireSnapshot()
			defer held.Close()
			oldGen := database.idx.Load()
			owner := oldGen.primaryOwner
			if owner == nil {
				t.Fatal("unchanged default profile did not select primary")
			}
			put("second", pointers[1])
			commits := database.durableRoot.slotCommit
			latestSlot := database.durableRoot.slot
			if commits[0] == 0 || commits[1] == 0 || commits[0] == commits[1] {
				t.Fatalf("slots %v", commits)
			}
			cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err = database.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
				cut.Close()
				t.Fatalf("held cut vacuum: %v", err)
			}
			if database.idx.Load() != oldGen {
				cut.Close()
				t.Fatal("cut refusal replaced DATA")
			}
			if err = cut.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.VacuumIndexOnline(context.Background()); err != nil {
				t.Fatal(err)
			}
			current := database.idx.Load()
			if current == oldGen || current.primaryOwner != owner || current.primary != oldGen.primary {
				t.Fatal("replacement did not share independently retained arena")
			}
			if !primaryarena.IsPage(database.State().RootPageID) || database.State().primaryRoot == nil {
				t.Fatal("vacuum did not publish actual primary state")
			}
			// The existing vacuum protocol publishes one new visible commit in the
			// latest slot and preserves the independent older slot's exact commit.
			wantCommits := commits
			wantCommits[latestSlot]++
			if database.durableRoot.slotCommit != wantCommits {
				t.Fatalf("slots %v want %v", database.durableRoot.slotCommit, wantCommits)
			}
			commits = wantCommits
			if got, err := held.Get([]byte("first")); err != nil || !bytes.Equal(got, values[0]) {
				t.Fatalf("held first %d %v", len(got), err)
			}
			if got, err := held.Get([]byte("second")); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
				t.Fatalf("held newest leaked %q %v", got, err)
			}
			for i, key := range []string{"first", "second"} {
				got, err := database.Get([]byte(key))
				if err != nil || !bytes.Equal(got, values[i]) {
					t.Fatalf("current %s %d %v", key, len(got), err)
				}
			}
			if err = held.Close(); err != nil {
				t.Fatal(err)
			}
			database.idxMu.Lock()
			generations := len(database.idxAll)
			database.idxMu.Unlock()
			if generations != 1 {
				t.Fatalf("old DATA generation retained after owners drain: %d", generations)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			// Damage each actual META independently. No new write or copy changes the
			// fixture between slots; each remaining complete root must select its own
			// commit and logical contents with the exact original pointer dependency.
			path := filepath.Join(dir, indexFileName)
			for damaged := 0; damaged < 2; damaged++ {
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				image := make([]byte, page.PageSize)
				offset := int64(damaged * page.PageSize)
				if _, err = file.ReadAt(image, offset); err != nil {
					t.Fatal(err)
				}
				corrupt := append([]byte(nil), image...)
				corrupt[32] ^= 1
				if _, err = file.WriteAt(corrupt, offset); err != nil {
					t.Fatal(err)
				}
				if err = file.Sync(); err != nil {
					t.Fatal(err)
				}
				file.Close()
				readOpts := opts
				readOpts.ReadOnly = true
				recovered, err := Open(readOpts)
				if err != nil {
					t.Fatal(err)
				}
				want := commits[damaged^1]
				if recovered.State().CommitSeq != want {
					t.Fatalf("damaged %d selected %d want %d", damaged, recovered.State().CommitSeq, want)
				}
				got, err := recovered.Get([]byte("first"))
				if err != nil || !bytes.Equal(got, values[0]) {
					t.Fatalf("recovered first %d %v", len(got), err)
				}
				got, err = recovered.Get([]byte("second"))
				newest := commits[0]
				if commits[1] > newest {
					newest = commits[1]
				}
				if err != nil || (want == newest && !bytes.Equal(got, values[1])) || (want != newest && got != nil) {
					t.Fatalf("recovered second seq%d %d %v", want, len(got), err)
				}
				if err = recovered.Close(); err != nil {
					t.Fatal(err)
				}
				file, err = os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = file.WriteAt(image, offset); err != nil {
					t.Fatal(err)
				}
				if err = file.Sync(); err != nil {
					t.Fatal(err)
				}
				file.Close()
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err = database.SetSync([]byte("after-vacuum"), []byte("write")); err != nil {
				t.Fatal(err)
			}
			reusedSnapshot := database.AcquireSnapshot()
			defer reusedSnapshot.Close()
			before := database.idx.Load().primary.Counters()
			seen := make(map[uint64]bool)
			reused := false
			var latest []byte
			for i := 0; i < 96; i++ {
				latest = []byte{byte(i), 1, 2, 3}
				if err = database.SetSync([]byte("first"), latest); err != nil {
					t.Fatal(err)
				}
				id := database.durableRoot.primary.records[database.durableRoot.slot].PageID
				reused = reused || seen[id]
				seen[id] = true
			}
			after := database.idx.Load().primary.Counters()
			if !reused || after.BanksFreed <= before.BanksFreed {
				t.Fatal("ordinary path did not reuse released physical banks")
			}
			if got, err := reusedSnapshot.Get([]byte("first")); err != nil || !bytes.Equal(got, values[0]) {
				t.Fatalf("snapshot through bank reuse %d %v", len(got), err)
			}
			if got, err := database.Get([]byte("first")); err != nil || !bytes.Equal(got, latest) {
				t.Fatalf("current through bank reuse %q %v", got, err)
			}
			if err = reusedSnapshot.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPrimaryVacuumCutoverFaultsPreserveCompleteRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported")
	}
	for _, phase := range []string{"primary-sync", "ready-create", "old-rename", "new-rename"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			createSavedPrimaryFormatV5(t, dir, false)
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err = database.SetSync([]byte("first"), []byte("older")); err != nil {
				t.Fatal(err)
			}
			if err = database.SetSync([]byte("second"), []byte("newer")); err != nil {
				t.Fatal(err)
			}
			oldCommit := database.State().CommitSeq
			owner := database.idx.Load().primaryOwner
			cut := errors.New("injected primary vacuum cut")
			injected := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if injected {
					return nil
				}
				matched := phase == "primary-sync" && event.Point == durabilitycut.BeforeIndexDataSync && filepath.Base(event.Path) == primaryIndexFileName ||
					phase == "ready-create" && event.Namespace == durabilitycut.NamespaceCreate && filepath.Base(event.NewPath) == indexReadyFileName ||
					phase == "old-rename" && event.Namespace == durabilitycut.NamespaceRename && filepath.Base(event.OldPath) == indexFileName && filepath.Base(event.NewPath) == indexBakFileName ||
					phase == "new-rename" && event.Namespace == durabilitycut.NamespaceRename && filepath.Base(event.OldPath) == indexNewFileName && filepath.Base(event.NewPath) == indexFileName
				if matched {
					injected = true
					return cut
				}
				return nil
			})
			err = database.VacuumIndexOnline(context.Background())
			restore()
			if !injected || err == nil {
				t.Fatalf("phase %s injection=%v error=%v", phase, injected, err)
			}
			if database.idx.Load().primaryOwner != owner {
				t.Fatal("failed cutover replaced arena owner")
			}
			_ = database.Close()
			database, err = Open(opts)
			if err != nil {
				t.Fatalf("recovery after %s: %v", phase, err)
			}
			defer database.Close()
			seq := database.State().CommitSeq
			if seq != oldCommit && seq != oldCommit+1 {
				t.Fatalf("partial recovery commit %d from %d", seq, oldCommit)
			}
			for key, want := range map[string]string{"first": "older", "second": "newer"} {
				got, err := database.Get([]byte(key))
				if err != nil || string(got) != want {
					t.Fatalf("%s %q %v", key, got, err)
				}
			}
			if err = database.SetSync([]byte("after"), []byte("recovered")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
