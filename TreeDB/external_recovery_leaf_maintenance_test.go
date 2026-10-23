package treedb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This test uses only maintained API/base exports. The periodic scheduler is
// qualified separately; here the external owner and existing cached maintenance
// fence exercise the same backend collector against exact retained roots.
func TestExternalRecoveryLeafMaintenanceRetainsAndReclaimsExactCuts(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	opts.ValueLog.Compression = ValueLogCompressionOff
	opts.ValueLog.Generational.Policy = ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 8 << 10
	opts.ValueLog.Generational.HotSegmentTargetBytes = 64 << 10
	config := ExternalRecoveryConfig{Identity: "leaf-maintenance-exact-cuts"}
	o, err := OpenExternalRecovery(opts, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if o != nil {
			_ = o.Close()
		}
	}()
	const keys = 1024
	key := func(i int) []byte { return []byte(fmt.Sprintf("leaf-external-%08d", i)) }
	value := func(round, i int) []byte {
		out := make([]byte, 512)
		for j := range out {
			out[j] = byte((round*37 + i*19 + j*13) % 251)
		}
		return out
	}
	write := func(round int) {
		t.Helper()
		if err := o.Apply(func() error {
			b := o.DB().NewBatch()
			defer b.Close()
			for i := 0; i < keys; i++ {
				if err := b.Set(key(i), value(round, i)); err != nil {
					return err
				}
			}
			if err := b.Write(); err != nil {
				return err
			}
			return o.DB().Checkpoint()
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkValues := func(get func([]byte) ([]byte, error), round int) {
		t.Helper()
		for i := 0; i < keys; i++ {
			got, err := get(key(i))
			if err != nil || !bytes.Equal(got, value(round, i)) {
				t.Fatalf("round%d key%d bytes=%d err=%v", round, i, len(got), err)
			}
		}
	}
	checkpoint := func(round int) (ExternalRecoveryCheckpoint, backenddb.RetainedPhysicalRootV1) {
		t.Helper()
		cert, err := o.Checkpoint(context.Background(), []byte(fmt.Sprintf("round-%d", round)))
		if err != nil {
			t.Fatal(err)
		}
		root, err := o.readRetainedRoot(cert)
		if err != nil {
			t.Fatal(err)
		}
		return cert, root
	}
	working := filepath.Join(dir, "working")
	leafDir := backenddb.LeafLogDirPath(working)
	// Freeze the cached producers and background maintenance while observing
	// names and identities; ReadDir followed by Info must be one fenced view.
	inspect := func(fn func() error) {
		t.Helper()
		if err := o.Apply(func() error { return o.DB().cached.RunBackendMaintenance(fn) }); err != nil {
			t.Fatal(err)
		}
	}
	inventory := func() map[string]os.FileInfo {
		t.Helper()
		out := make(map[string]os.FileInfo)
		inspect(func() error {
			entries, err := os.ReadDir(leafDir)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".log") || strings.HasPrefix(entry.Name(), "manifest.durable.") {
					info, err := entry.Info()
					if err != nil {
						return err
					}
					out[entry.Name()] = info
				}
			}
			return nil
		})
		return out
	}
	manifest := func() (string, os.FileInfo) {
		t.Helper()
		var name string
		var info os.FileInfo
		inspect(func() error {
			raw, err := os.ReadFile(filepath.Join(leafDir, "manifest.json"))
			if err != nil {
				return err
			}
			var header struct {
				Revision uint64 `json:"manifest_revision"`
			}
			if err := json.Unmarshal(raw, &header); err != nil || header.Revision == 0 {
				t.Fatalf("manifest revision: %+v %v", header, err)
			}
			name = fmt.Sprintf("manifest.durable.%016x.json", header.Revision)
			info, err = os.Stat(filepath.Join(leafDir, name))
			return err
		})
		return name, info
	}
	var total backenddb.LeafGenerationGCStats
	collect := func() {
		t.Helper()
		err := o.Apply(func() error {
			db := o.DB()
			if err := db.Checkpoint(); err != nil {
				return err
			}
			return db.cached.RunBackendMaintenance(func() error {
				roots, system := db.cached.ProtectedLeafGenerationRootIDPair()
				stats, err := db.backend.LeafGenerationGC(context.Background(), backenddb.LeafGenerationGCOptions{ProtectedRootIDs: roots, ProtectedSystemRootIDs: system})
				total.GenerationsDeleted += stats.GenerationsDeleted
				total.FilesDeleted += stats.FilesDeleted
				total.BytesDeleted += stats.BytesDeleted
				total.ManifestRevisionsDeleted += stats.ManifestRevisionsDeleted
				total.ManifestRevisionBytesDeleted += stats.ManifestRevisionBytesDeleted
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	write(0)
	first, firstRoot := checkpoint(0)
	initial := inventory()
	firstManifest, firstManifestInfo := manifest()
	leafFiles := 0
	for name := range initial {
		if strings.HasSuffix(name, ".log") {
			leafFiles++
		}
	}
	if leafFiles < 2 {
		t.Fatalf("fixture did not cross small leaf target: leaf_files=%d", leafFiles)
	}
	write(1)
	second, secondRoot := checkpoint(1)
	secondManifest, secondManifestInfo := manifest()
	if firstManifest == secondManifest {
		t.Fatal("fixture did not publish distinct immutable manifests")
	}
	// Working roots advance well beyond both certificates. No certificate or
	// old file pin is held by this test; only the external owner owns those cuts.
	for round := 2; round <= 5; round++ {
		write(round)
		collect()
	}
	inspect(func() error {
		for name, identity := range map[string]os.FileInfo{firstManifest: firstManifestInfo, secondManifest: secondManifestInfo} {
			info, err := os.Stat(filepath.Join(leafDir, name))
			if err != nil || !os.SameFile(identity, info) {
				t.Fatalf("certified manifest identity lost %s: %v", name, err)
			}
		}
		return nil
	})
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = nil
	// Physically select and read BOTH operands after closing the external owner,
	// so descriptor parsing and live process pins cannot mask lost dependencies.
	for _, cut := range []struct {
		root  backenddb.RetainedPhysicalRootV1
		round int
	}{{firstRoot, 0}, {secondRoot, 1}} {
		if err := backenddb.RestoreRetainedPhysicalRootsV1(working, cut.root, nil); err != nil {
			t.Fatal(err)
		}
		physical := opts
		physical.Dir = working
		physical.DisableSideStores = true
		physical.DisableBackgroundPrune = true
		physical.BackgroundCheckpointInterval = -1
		physical.BackgroundCheckpointIdleDuration = -1
		physical.BackgroundIndexVacuumInterval = -1
		db, err := backenddb.Open(physical)
		if err != nil {
			t.Fatal(err)
		}
		checkValues(db.Get, cut.round)
		if state, ok := db.StateToken(); !ok || externalPersistentRoot(state) != externalPersistentRoot(cut.root.State) {
			t.Fatalf("physical selection differs: %+v want%+v", state, cut.root.State)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	o, err = OpenExternalRecovery(opts, config)
	if err != nil {
		t.Fatal(err)
	}
	selected, ok := o.RecoveredCheckpoint()
	if !ok || selected.Digest != second.Digest || selected.Generation != second.Generation || selected.PreviousDigest != first.Digest {
		t.Fatalf("wrong selected certificate: %+v", selected)
	}
	checkValues(o.DB().Get, 1)
	// Three new certified cuts release old owner cuts and advance both ordinary
	// durable slots. Subsequent fixed churn creates sealed, wholly obsolete files.
	var latest ExternalRecoveryCheckpoint
	for round := 6; round <= 8; round++ {
		write(round)
		latest, _ = checkpoint(round)
		collect()
	}
	for round := 9; round <= 11; round++ {
		write(round)
		collect()
	}
	removedLeaf := 0
	inspect(func() error {
		for name, identity := range initial {
			info, err := os.Stat(filepath.Join(leafDir, name))
			if os.IsNotExist(err) {
				if strings.HasSuffix(name, ".log") {
					removedLeaf++
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(identity, info) {
				t.Fatalf("old resource name rebound instead of tracked deletion: %s", name)
			}
		}
		if removedLeaf == 0 {
			t.Fatal("old certified leaf generations not reclaimed after retirement")
		}
		for _, name := range []string{firstManifest, secondManifest} {
			if _, err := os.Stat(filepath.Join(leafDir, name)); !os.IsNotExist(err) {
				t.Fatalf("old immutable manifest not deleted after retirement %s: %v", name, err)
			}
		}
		return nil
	})
	if total.GenerationsDeleted == 0 || total.BytesDeleted == 0 || total.ManifestRevisionsDeleted == 0 || total.ManifestRevisionBytesDeleted == 0 {
		t.Fatalf("missing actual physical reclamation: %+v", total)
	}
	t.Logf("exact current/previous preserved then retired: old_leaf_files_removed=%d generations=%d leaf_bytes=%d manifest_revisions=%d manifest_bytes=%d", removedLeaf, total.GenerationsDeleted, total.BytesDeleted, total.ManifestRevisionsDeleted, total.ManifestRevisionBytesDeleted)
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o = nil
	o, err = OpenExternalRecovery(opts, config)
	if err != nil {
		t.Fatal(err)
	}
	selected, ok = o.RecoveredCheckpoint()
	if !ok || selected.Digest != latest.Digest || selected.Generation != latest.Generation {
		t.Fatalf("latest certificate lost: %+v", selected)
	}
	checkValues(o.DB().Get, 8)
}
