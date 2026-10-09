package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type offlineRootValueV6 struct {
	value   []byte
	missing bool
}

func prepareOfflinePointerPairV6(t *testing.T, dir string) uint64 {
	t.Helper()
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 2000, 2, func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i)}, 512) })
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"first", "second"} {
		b := d.NewBatch().(*Batch)
		if err = b.SetPointer([]byte(key), ptrs[i]); err != nil {
			t.Fatal(err)
		}
		if err = errors.Join(b.WriteSync(), b.Close()); err != nil {
			t.Fatal(err)
		}
	}
	seq := d.State().CommitSeq
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(filepath.Join(dir, valueLogRefCountsFileName))
	if err != nil {
		t.Fatal(err)
	}
	cache, err := decodeValueLogRefCounts(image)
	if err != nil {
		t.Fatal(err)
	}
	if cache.commitSeq != seq || cache.counts[ptrs[0].FileID] != 2 {
		t.Fatalf("missing original stale-cache seed: %+v", cache)
	}
	return seq
}

func assertOfflinePointerPairV6(t *testing.T, dir string, seq uint64) {
	t.Helper()
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.State().CommitSeq != seq {
		t.Fatalf("logical sequence %d want %d", d.State().CommitSeq, seq)
	}
	for i, key := range []string{"first", "second"} {
		v, e := d.Get([]byte(key))
		if e != nil || !bytes.Equal(v, bytes.Repeat([]byte{byte('a' + i)}, 512)) {
			t.Fatalf("%s %q %v", key, v, e)
		}
	}
	if err = d.SetSync([]byte("allocator"), []byte("write")); err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryOfflineVlogV6CacheInvalidationRecovery(t *testing.T) {
	for _, phase := range []string{"before-commit", "after-commit"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			seq := prepareOfflinePointerPairV6(t, dir)
			segments, err := listValueLogSegments(dir)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("cache boundary cut")
			testHookPrimaryJointSwapV6 = func(p string) error {
				if p == phase {
					return injected
				}
				return nil
			}
			_, err = ValueLogRewriteOffline(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			testHookPrimaryJointSwapV6 = nil
			if !errors.Is(err, injected) || (phase == "after-commit" && !errors.Is(err, ErrRecoveryRequired)) {
				t.Fatalf("cut %s: %v", phase, err)
			}
			for _, segment := range segments {
				if _, e := os.Stat(segment.path); e != nil {
					t.Fatalf("failure removed old segment: %v", e)
				}
			}
			if _, e := os.Stat(filepath.Join(dir, valueLogRefCountsFileName)); !os.IsNotExist(e) {
				t.Fatalf("old same-sequence cache survives: %v", e)
			}
			assertOfflinePointerPairV6(t, dir, seq)
		})
	}
}

func TestPrimaryOfflineVlogV6IndependentFallback(t *testing.T) {
	for badSlot := uint64(0); badSlot < 2; badSlot++ {
		t.Run(fmt.Sprint(badSlot), func(t *testing.T) {
			dir := t.TempDir()
			prepareOfflinePointerPairV6(t, dir)
			opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			if _, err := ValueLogRewriteOffline(opts); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(filepath.Join(dir, primaryIndexFileName), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			image := make([]byte, 3*page.PageSize)
			if _, err = f.ReadAt(image, int64(2+(badSlot^1)*3)*page.PageSize); err != nil {
				t.Fatal(err)
			}
			// Inspect the actual surviving eligible slot before corrupting its peer.
			d, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			view, err := rootpublication.DecodePrimaryCapsuleV6(image, badSlot^1, d.idx.Load().primary.UUID())
			if err != nil {
				t.Fatal(err)
			}
			want := view.Current().Record.CommitSeq
			if want != d.durableRoot.slotCommit[badSlot^1] {
				t.Fatal("surviving capsule disagrees with selected slot inventory")
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err = f.WriteAt([]byte{0xff}, int64(2+badSlot*3)*page.PageSize); err != nil {
				t.Fatal(err)
			}
			if err = errors.Join(f.Sync(), f.Close()); err != nil {
				t.Fatal(err)
			}
			d, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if d.State().CommitSeq != want {
				t.Fatalf("fallback sequence %d want %d", d.State().CommitSeq, want)
			}
			if value, e := d.Get([]byte("first")); e != nil || !bytes.Equal(value, bytes.Repeat([]byte("a"), 512)) {
				t.Fatalf("old-root pointer after old deletion: %q %v", value, e)
			}
			if err = d.SetSync([]byte("fallback-allocator"), []byte("write")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPrimaryOfflinePairV6SourceCloseFailurePreventsCommit(t *testing.T) {
	dir := t.TempDir()
	preparePrimaryJointCrashV6(t, dir)
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	roots, err := d.CaptureRecoverableRootSetForInspection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	original, err := os.Stat(filepath.Join(dir, indexFileName))
	if err != nil {
		t.Fatal(err)
	}
	scope, err := d.captureOfflinePrimaryScopeV6()
	if err != nil {
		t.Fatal(err)
	}
	defer scope.releaseOfflineNamespaceScopeV6()
	parent := scope.stableNamespaceParent
	pair, err := d.newOfflinePrimaryPairV6(scope, defaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	p := pair.pager
	injected := errors.New("actual source Close hook failure")
	d.RegisterCloseHook(func() error { return injected })
	err = d.finishOfflinePrimaryPairV6(parent, pair, roots, func(root RecoverableRoot) (rebuiltDurableRootV1, error) {
		value, _, e := d.rebuildRecoverableRootWithPublicationLockV1(context.Background(), roots, root, p, pair.allocator, false)
		return value, e
	}, false, nil)
	if !errors.Is(err, injected) {
		t.Fatalf("source Close failure lost: %v", err)
	}
	if _, e := os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(e) {
		t.Fatalf("cleanup failure published COMMIT: %v", e)
	}
	current, err := os.Stat(filepath.Join(dir, indexFileName))
	if err != nil || !os.SameFile(original, current) {
		t.Fatal("cleanup failure changed canonical pair")
	}
}

func TestPrimaryOfflinePairV6RejectsBeforeRenameIdentityReplacement(t *testing.T) {
	dir := t.TempDir()
	prior := preparePrimaryJointCrashV6(t, dir)
	original, err := os.Stat(filepath.Join(dir, indexFileName))
	if err != nil {
		t.Fatal(err)
	}
	testHookPrimaryJointSwapV6 = func(phase string) error {
		if phase != "before-data-rename" {
			return nil
		}
		if e := os.Rename(filepath.Join(dir, indexNewFileName), filepath.Join(dir, "held-staging")); e != nil {
			return e
		}
		return os.WriteFile(filepath.Join(dir, indexNewFileName), []byte("foreign"), 0600)
	}
	err = VacuumIndexOffline(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	testHookPrimaryJointSwapV6 = nil
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("replacement accepted: %v", err)
	}
	current, err := os.Stat(filepath.Join(dir, indexFileName))
	if err != nil || !os.SameFile(original, current) {
		t.Fatal("canonical old pair changed before identity refusal")
	}
	if image, e := os.ReadFile(filepath.Join(dir, indexNewFileName)); e != nil || string(image) != "foreign" {
		t.Fatalf("foreign replacement removed: %q %v", image, e)
	}
	if err = os.Rename(filepath.Join(dir, "held-staging"), filepath.Join(dir, indexNewFileName)); err != nil {
		t.Fatal(err)
	}
	assertPrimaryJointRecoveredV6(t, dir, prior)
}

func TestPrimaryOfflinePairV6PreservesSlotsProofsAndPointerClosure(t *testing.T) {
	for _, mode := range []string{"vacuum", "vlog"} {
		t.Run(mode, func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			d, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if d != nil {
					_ = d.Close()
				}
			}()
			values := [][]byte{bytes.Repeat([]byte("proof-only"), 64), bytes.Repeat([]byte("alternate"), 64), bytes.Repeat([]byte("latest"), 64)}
			ptrs := appendPointersInNewSegment(t, opts.Dir, 0, 1, 1000, len(values), func(i int) []byte { return values[i] })
			if err = d.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			for i, key := range []string{"old", "alternate", "latest"} {
				b := d.NewBatch().(*Batch)
				if err = b.SetPointer([]byte(key), ptrs[i]); err != nil {
					t.Fatal(err)
				}
				if i == 2 {
					if err = b.Delete([]byte("old")); err != nil {
						t.Fatal(err)
					}
				}
				err = errors.Join(b.WriteSync(), b.Close())
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := d.CaptureRecoverableRootSetForInspection(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			basis := before.durable
			want := make(map[uint64][]offlineRootValueV6)
			for _, root := range before.Roots() {
				s := before.AcquireSnapshotForRoot(root)
				if s == nil {
					t.Fatal("missing original root")
				}
				var row []offlineRootValueV6
				for _, key := range []string{"old", "alternate", "latest"} {
					value, e := s.Get([]byte(key))
					if e != nil && !errors.Is(e, tree.ErrKeyNotFound) {
						t.Fatal(e)
					}
					row = append(row, offlineRootValueV6{bytes.Clone(value), errors.Is(e, tree.ErrKeyNotFound)})
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				want[root.CommitSeq] = row
			}
			if len(want) < 3 || basis.primaryProof[0].Record.CommitSeq == 0 || basis.primaryProof[1].Record.CommitSeq == 0 {
				t.Fatalf("fixture lacks slot/proof closure: %+v", basis)
			}
			before.Release()
			oldData, err := os.Stat(filepath.Join(opts.Dir, indexFileName))
			if err != nil {
				t.Fatal(err)
			}
			oldPrimary, err := os.Stat(filepath.Join(opts.Dir, primaryIndexFileName))
			if err != nil {
				t.Fatal(err)
			}
			oldSegments, err := listValueLogSegments(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d = nil
			if mode == "vacuum" {
				err = VacuumIndexOffline(opts)
			} else {
				_, err = ValueLogRewriteOffline(opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			newData, err := os.Stat(filepath.Join(opts.Dir, indexFileName))
			if err != nil {
				t.Fatal(err)
			}
			newPrimary, err := os.Stat(filepath.Join(opts.Dir, primaryIndexFileName))
			if err != nil {
				t.Fatal(err)
			}
			if os.SameFile(oldData, newData) || os.SameFile(oldPrimary, newPrimary) {
				t.Fatal("physical pair not replaced")
			}
			if mode == "vlog" {
				for _, segment := range oldSegments {
					if _, e := os.Stat(segment.path); !os.IsNotExist(e) {
						t.Fatalf("old segment retained after complete rewrite: %s %v", segment.path, e)
					}
				}
			}
			d, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			after, err := d.CaptureRecoverableRootSetForInspection(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer after.Release()
			for slot, original := range basis.slotRecord {
				r := after.durable.slotRecord[slot]
				if r.CommitSeq != original.CommitSeq || r.DurableSeq != original.DurableSeq || r.AppliedCommandLSN != original.AppliedCommandLSN || r.MaxEntryRevision != original.MaxEntryRevision || r.LastCommitHeight != original.LastCommitHeight || r.ParentCommitSeq != original.ParentCommitSeq {
					t.Fatalf("slot %d logical metadata changed: %+v -> %+v", slot, original, r)
				}
				if after.durable.primaryProof[slot].Record.CommitSeq != basis.primaryProof[slot].Record.CommitSeq {
					t.Fatalf("slot %d changed exact one-hop proof", slot)
				}
				if r.Freelist == original.Freelist {
					t.Fatal("replacement retained old physical DATA generation")
				}
			}
			seen := make(map[uint64]bool)
			for _, root := range after.Roots() {
				row, ok := want[root.CommitSeq]
				if !ok {
					t.Fatalf("manufactured logical commit %d", root.CommitSeq)
				}
				s := after.AcquireSnapshotForRoot(root)
				if s == nil {
					t.Fatal("missing rebuilt root")
				}
				for i, key := range []string{"old", "alternate", "latest"} {
					value, e := s.Get([]byte(key))
					if (row[i].missing && (!errors.Is(e, tree.ErrKeyNotFound) || value != nil)) || (!row[i].missing && (e != nil || !bytes.Equal(value, row[i].value))) {
						t.Fatalf("root %d %s: %q %v want %+v", root.CommitSeq, key, value, e, row[i])
					}
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				seen[root.CommitSeq] = true
			}
			if len(seen) != len(want) {
				t.Fatalf("lost original recovery closure: %v vs %v", seen, want)
			}
			after.Release()
			if err = d.SetSync([]byte("allocator-after-reopen"), []byte("ok")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPrimaryOfflinePairV6RetainsUncertainDecision(t *testing.T) {
	for _, phase := range []string{"after-commit", "after-data-rename", "after-primary-rename", "before-decision-delete"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			prior := preparePrimaryJointCrashV6(t, dir)
			injected := fmt.Errorf("offline injected %s", phase)
			testHookPrimaryJointSwapV6 = func(p string) error {
				if p == phase {
					return injected
				}
				return nil
			}
			err := VacuumIndexOffline(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			testHookPrimaryJointSwapV6 = nil
			if !errors.Is(err, injected) || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("uncertainty: %v", err)
			}
			image, err := os.ReadFile(filepath.Join(dir, indexReadyFileName))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = decodePrimaryJointDecisionV6(image); err != nil {
				t.Fatal(err)
			}
			_, err = Open(Options{Dir: dir, ReadOnly: true, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed})
			if !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("readonly accepted pending pair: %v", err)
			}
			assertPrimaryJointRecoveredV6(t, dir, prior)
		})
	}
}

func TestPrimaryOfflinePairV6ConstructionUsesRetainedParent(t *testing.T) {
	for _, phase := range []string{"before-offline-primary-create", "before-offline-data-create"} {
		t.Run(phase, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "db")
			held := filepath.Join(base, "held-original")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			preparePrimaryJointCrashV6(t, dir)
			d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			scope, err := d.captureOfflinePrimaryScopeV6()
			if err != nil {
				t.Fatal(err)
			}
			defer scope.releaseOfflineNamespaceScopeV6()
			parent := scope.stableNamespaceParent
			testHookPrimaryJointSwapV6 = func(actual string) error {
				if actual != phase {
					return nil
				}
				if err := os.Rename(dir, held); err != nil {
					return err
				}
				if err := os.Mkdir(dir, 0700); err != nil {
					return err
				}
				for _, name := range []string{indexNewFileName, primaryNewFileName, indexReadyFileName} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte("foreign "+name), 0600); err != nil {
						return err
					}
				}
				return nil
			}
			defer func() { testHookPrimaryJointSwapV6 = nil }()
			pair, err := d.newOfflinePrimaryPairV6(scope, defaultChunkSize)
			testHookPrimaryJointSwapV6 = nil
			if err != nil {
				t.Fatal(err)
			}
			decision, err := primaryJointDecisionForV6(parent, pair, durableRootSelectionV1{})
			if err != nil {
				t.Fatal(err)
			}
			if err := pair.close(); err != nil {
				t.Fatal(err)
			}
			if err := cleanupOfflinePrimaryStageV6(dir, parent, indexNewFileName, decision.Data); err != nil {
				t.Fatal(err)
			}
			if err := cleanupOfflinePrimaryStageV6(dir, parent, primaryNewFileName, decision.Primary); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{indexNewFileName, primaryNewFileName, indexReadyFileName} {
				b, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || string(b) != "foreign "+name {
					t.Fatalf("foreign child changed: %s %q %v", name, b, err)
				}
			}
			for _, name := range []string{indexNewFileName, primaryNewFileName, indexReadyFileName} {
				if _, err := os.Stat(filepath.Join(held, name)); !os.IsNotExist(err) {
					t.Fatalf("original staging/COMMIT not drained: %s %v", name, err)
				}
			}
		})
	}
}

func TestPrimaryOfflinePairV6ConstructionFailureCleansOnlyOwnedStage(t *testing.T) {
	dir := t.TempDir()
	preparePrimaryJointCrashV6(t, dir)
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	scope, err := d.captureOfflinePrimaryScopeV6()
	if err != nil {
		t.Fatal(err)
	}
	defer scope.releaseOfflineNamespaceScopeV6()
	if err := os.WriteFile(filepath.Join(dir, indexNewFileName), []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	pair, err := d.newOfflinePrimaryPairV6(scope, defaultChunkSize)
	if err == nil || pair == nil {
		t.Fatalf("exclusive collision accepted: %p %v", pair, err)
	}
	if pair.close() != nil {
		t.Fatal("successful partial cleanup was lost")
	}
	b, err := os.ReadFile(filepath.Join(dir, indexNewFileName))
	if err != nil || string(b) != "foreign" {
		t.Fatalf("collision child changed: %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(dir, primaryNewFileName)); !os.IsNotExist(err) {
		t.Fatalf("own PRIMARY stage retained without debt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(err) {
		t.Fatalf("failure published COMMIT: %v", err)
	}
}

func TestPrimaryOfflineScopeV6ParentOutlivesPhysicalPair(t *testing.T) {
	dir := t.TempDir()
	preparePrimaryJointCrashV6(t, dir)
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	metadata := d.idx.Load().primary.MetadataOwner()
	scope, err := d.captureOfflinePrimaryScopeV6()
	if err != nil {
		t.Fatal(err)
	}
	parent := scope.stableNamespaceParent
	pair, err := d.newOfflinePrimaryPairV6(scope, defaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := primaryJointDecisionForV6(parent, pair, durableRootSelectionV1{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pair.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Stat(); err != nil {
		t.Fatalf("physical Close consumed operation parent: %v", err)
	}
	if scope.stableNamespaceParentCharge == 0 || scope.offlineScopeControlCharge == 0 {
		t.Fatal("live operation scope was refunded")
	}
	if err := cleanupOfflinePrimaryStageV6(dir, parent, indexNewFileName, staged.Data); err != nil {
		t.Fatal(err)
	}
	if err := cleanupOfflinePrimaryStageV6(dir, parent, primaryNewFileName, staged.Primary); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if !metadata.PhysicalClosed() || metadata.Bytes() == 0 {
		t.Fatal("original metadata scope was not retained after source physical Close")
	}
	if err := scope.releaseOfflineNamespaceScopeV6(); err != nil {
		t.Fatal(err)
	}
	if metadata.Bytes() != 0 {
		t.Fatalf("completed scope left original metadata: %d", metadata.Bytes())
	}
	if _, err := parent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("original parent not closed: %v", err)
	}
	if err := scope.releaseOfflineNamespaceScopeV6(); err != nil {
		t.Fatalf("completed phase not idempotent: %v", err)
	}
}

func TestPrimaryOfflineScopeV6PhysicalFailureRetainsControls(t *testing.T) {
	dir := t.TempDir()
	preparePrimaryJointCrashV6(t, dir)
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	scope, err := d.captureOfflinePrimaryScopeV6()
	if err != nil {
		t.Fatal(err)
	}
	pair, err := d.newOfflinePrimaryPairV6(scope, defaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	metadata := scope.stableNamespaceMetadata
	before := metadata.Bytes()
	fileCharge := scope.stableNamespaceParentCharge - scope.offlineScopeControlCharge
	originalPager, originalArena, originalOwner := pair.pager, pair.primary, pair.primaryOwner
	t.Cleanup(func() {
		// Join the same real retained PRIMARY cleanup after all custody assertions.
		// The generation's cached DATA error and its retained control charge remain.
		if err := originalArena.Close(); err != nil {
			t.Errorf("retained PRIMARY teardown: %v", err)
		}
	})
	// The original exact file is closed externally to make the producer's
	// physical Close report a genuine error; it must not claim completion.
	if err := pair.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	err = scope.releaseOfflineNamespaceScopeV6()
	if !errors.Is(err, os.ErrClosed) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("lost physical uncertainty: %v", err)
	}
	if scope.pager != originalPager || scope.primary != originalArena || scope.primaryOwner != originalOwner {
		t.Fatal("failed physical operands cleared")
	}
	if scope.stableNamespaceParent != nil {
		t.Fatal("successfully closed parent retained")
	}
	if scope.offlineScopeControlCharge == 0 || metadata.Bytes() != before-fileCharge {
		t.Fatalf("wrong scope refund: before=%d after=%d file=%d controls=%d", before, metadata.Bytes(), fileCharge, scope.offlineScopeControlCharge)
	}
	originalOwner.mu.Lock()
	retained := originalOwner.failedDataGenerations == scope
	originalOwner.mu.Unlock()
	if !retained {
		t.Fatal("original physical owner lost failed generation")
	}
	if err := scope.releaseOfflineNamespaceScopeV6(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cached physical debt disappeared: %v", err)
	}
	if metadata.Bytes() != before-fileCharge {
		t.Fatal("repeated phase refunded failed controls")
	}
}

func TestPrimaryOfflineScopeV6ParentFailureRetainsExactCustody(t *testing.T) {
	dir := t.TempDir()
	preparePrimaryJointCrashV6(t, dir)
	d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	scope, err := d.captureOfflinePrimaryScopeV6()
	if err != nil {
		t.Fatal(err)
	}
	parent, metadata, owner := scope.stableNamespaceParent, scope.stableNamespaceMetadata, scope.offlineScopePhysicalOwner
	charge, before := scope.stableNamespaceParentCharge, metadata.Bytes()
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	err = scope.releaseOfflineNamespaceScopeV6()
	if !errors.Is(err, os.ErrClosed) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("lost parent uncertainty: %v", err)
	}
	if scope.stableNamespaceParent != parent || scope.stableNamespaceParentCharge != charge || metadata.Bytes() != before {
		t.Fatal("failed parent/refund custody lost")
	}
	owner.mu.Lock()
	retained := owner.failedDataGenerations == scope && scope.failedNext == nil
	owner.mu.Unlock()
	if !retained {
		t.Fatal("original producer lost exact namespace scope")
	}
	if err := scope.releaseOfflineNamespaceScopeV6(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cached parent debt disappeared: %v", err)
	}
	owner.mu.Lock()
	duplicate := scope.failedNext == scope
	owner.mu.Unlock()
	if duplicate {
		t.Fatal("repeated failure duplicated the intrusive custody link")
	}
	if _, err := os.Stat(filepath.Join(dir, indexReadyFileName)); !os.IsNotExist(err) {
		t.Fatalf("parent failure published COMMIT: %v", err)
	}
}
