package powerlossoracle

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func directoryRenameFixture(t *testing.T) *Model {
	t.Helper()
	m := newModel()
	for path, value := range map[string]string{"stage/db/root": "sealed-root", "stage/db/value": "old-value", "to/keep": "keep"} {
		if err := m.Create(path, []byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := m.SyncFile(path); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"stage/db", "stage", "to", "."} {
		if err := m.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func assertNoStablePath(t *testing.T, m *Model, path string) {
	t.Helper()
	root := t.TempDir()
	if err := m.MaterializeStable(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected stable path %q: %v", path, err)
	}
}

func TestDirectoryRenameParentBarrierPreservesSyncedContents(t *testing.T) {
	m := directoryRenameFixture(t)
	dirIdentity := m.volatileDirs["stage/db"]
	fileID := m.volatile["stage/db/value"]
	fingerprint := m.StableFingerprint()
	if err := m.Write("stage/db/value", []byte("dirty-value")); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename("stage", "final"); err != nil {
		t.Fatal(err)
	}
	if m.StableFingerprint() != fingerprint {
		t.Fatal("rename promoted namespace or dirty bytes before parent barrier")
	}
	if m.volatile["final/db/value"] != fileID || !rootpublication.SamePhysicalIdentity(m.volatileDirs["final/db"], dirIdentity) {
		t.Fatal("rename changed directory or file inode identity")
	}
	before := m.Clone()
	before.Crash()
	assertStableFile(t, before, "stage/db/value", "old-value")
	assertNoStablePath(t, before, "final")
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertStableFile(t, m, "final/db/root", "sealed-root")
	assertStableFile(t, m, "final/db/value", "old-value")
	assertNoStablePath(t, m, "stage")
	if m.volatile["final/db/value"] != fileID || !rootpublication.SamePhysicalIdentity(m.volatileDirs["final/db"], dirIdentity) {
		t.Fatal("parent barrier/crash changed child inode identity")
	}
}

func TestDirectoryRenameChildSyncUsesDirectoryIdentity(t *testing.T) {
	m := directoryRenameFixture(t)
	if err := m.Rename("stage", "final"); err != nil {
		t.Fatal(err)
	}
	if err := m.Create("final/db/new", []byte("new-data")); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncFile("final/db/new"); err != nil {
		t.Fatal(err)
	}
	// Syncing a file alone does not publish its new child name.
	assertNoStablePath(t, m, "stage/db/new")
	if err := m.SyncDir("final/db"); err != nil {
		t.Fatal(err)
	}
	// The child inode was synced through its new volatile path, but the stable
	// parent still names stage. Directory contents belong to the shared inode.
	assertStableFile(t, m, "stage/db/new", "new-data")
	assertNoStablePath(t, m, "final")
	if err := m.Create("final/db/unsynced", []byte("must-not-survive")); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertStableFile(t, m, "final/db/new", "new-data")
	assertNoStablePath(t, m, "final/db/unsynced")
}

func TestDirectoryRenameCrossParentSelectiveSyncAndRemoval(t *testing.T) {
	for _, order := range [][]string{{"to", "."}, {".", "to"}} {
		t.Run(order[0], func(t *testing.T) {
			m := directoryRenameFixture(t)
			identity := m.volatileDirs["stage"]
			if err := m.Rename("stage", "to/final"); err != nil {
				t.Fatal(err)
			}
			if err := m.SyncDir(order[0]); err != nil {
				t.Fatal(err)
			}
			if order[0] == "to" {
				// Destination durability does not imply source-parent removal.
				assertStableFile(t, m, "stage/db/root", "sealed-root")
				assertStableFile(t, m, "to/final/db/root", "sealed-root")
				if !rootpublication.SamePhysicalIdentity(m.stableDirs["stage"], m.stableDirs["to/final"]) {
					t.Fatal("selective sync copied directory inode")
				}
			} else {
				// Losing the old edge does not invent an unsynced destination edge.
				assertNoStablePath(t, m, "stage")
				assertNoStablePath(t, m, "to/final")
			}
			if err := m.SyncDir(order[1]); err != nil {
				t.Fatal(err)
			}
			assertStableFile(t, m, "to/final/db/value", "old-value")
			if !rootpublication.SamePhysicalIdentity(m.stableDirs["to/final"], identity) {
				t.Fatal("cross-parent rename changed identity")
			}
			assertNoStablePath(t, m, "stage")
			if err := m.Unlink("to/final"); err != nil {
				t.Fatal(err)
			}
			assertStableFile(t, m, "to/final/db/root", "sealed-root")
			if err := m.SyncDir("to"); err != nil {
				t.Fatal(err)
			}
			m.Crash()
			assertNoStablePath(t, m, "to/final")
			assertStableFile(t, m, "to/keep", "keep")
			if _, ok := m.durableDirs[physicalStableIdentity(identity)]; ok {
				t.Fatal("crash retained unreachable removed directory object")
			}
		})
	}
}

func TestDirectoryRenameSelectiveVariantsKeepNamespaceAndBytesIndependent(t *testing.T) {
	m := directoryRenameFixture(t)
	if err := m.Rename("stage", "final"); err != nil {
		t.Fatal(err)
	}
	if err := m.Write("final/db/value", []byte("new-value")); err != nil {
		t.Fatal(err)
	}
	spec := CutSpec{
		ID: "directory-rename", Point: BeforeNewFileDirectorySync, Model: m,
		Dependencies:     []DirtyResource{{Kind: ResourceValueLog, ID: "value", Path: "final/db/value", NewName: true, NamespaceDirs: []string{"."}}},
		RequiredFamilies: []VariantFamily{VariantSyncedOnly, VariantDataWithoutNamespace, VariantNamespaceWithoutData, VariantFullWriteback},
		ExpectedByFamily: map[VariantFamily]ExpectedResult{VariantSyncedOnly: ExpectedOldRoot, VariantDataWithoutNamespace: ExpectedOldRoot, VariantNamespaceWithoutData: ExpectedNewRoot, VariantFullWriteback: ExpectedNewRoot},
	}
	variants, coverage, err := GenerateVariants(spec)
	if err != nil {
		t.Fatal(err)
	}
	if coverage.Generated != 4 {
		t.Fatalf("variants=%d want=4", coverage.Generated)
	}
	for _, v := range variants {
		t.Run(string(v.Family), func(t *testing.T) {
			v.Model.Crash()
			switch v.Family {
			case VariantSyncedOnly:
				assertStableFile(t, v.Model, "stage/db/value", "old-value")
				assertNoStablePath(t, v.Model, "final")
			case VariantDataWithoutNamespace:
				assertStableFile(t, v.Model, "stage/db/value", "new-value")
				assertNoStablePath(t, v.Model, "final")
			case VariantNamespaceWithoutData:
				assertStableFile(t, v.Model, "final/db/value", "old-value")
				assertNoStablePath(t, v.Model, "stage")
			case VariantFullWriteback:
				assertStableFile(t, v.Model, "final/db/value", "new-value")
				assertNoStablePath(t, v.Model, "stage")
			default:
				t.Fatalf("unexpected family %s", v.Family)
			}
		})
	}
	// Variant promotion/Crash must not mutate their shared source objects.
	assertStableFile(t, m, "stage/db/value", "old-value")
	assertNoStablePath(t, m, "final")
}

func TestDirectoryRenameObservedAfterOverlayRetainsPhysicalObjects(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "stage", "db"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "stage", "db", "value"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	fileID, identity := m.volatile["stage/db/value"], m.volatileDirs["stage/db"]
	if err := os.Rename(filepath.Join(root, "stage"), filepath.Join(root, "final")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "final", "db", "value"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	// Simulate a different producer overlaying this completed rename first.
	if err := m.Overlay(root); err != nil {
		t.Fatal(err)
	}
	if m.volatile["final/db/value"] != fileID {
		t.Fatal("overlay allocated another object for renamed file inode")
	}
	if err := m.Observe(root, durabilitycut.Event{Namespace: durabilitycut.NamespaceRename, OldPath: filepath.Join(root, "stage"), NewPath: filepath.Join(root, "final")}); err != nil {
		t.Fatal(err)
	}
	assertStableFile(t, m, "stage/db/value", "old")
	if !rootpublication.SamePhysicalIdentity(m.volatileDirs["final/db"], identity) {
		t.Fatal("observed rename lost directory identity")
	}
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertStableFile(t, m, "final/db/value", "old")
	assertNoStablePath(t, m, "stage")
	image := t.TempDir()
	if err := m.MaterializeStable(image); err != nil {
		t.Fatal(err)
	}
	release, err := m.InstallStableIdentityOverrides(image)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, _, restoredIdentity, err := capturePathSnapshot(filepath.Join(image, "final", "db"))
	if err != nil {
		t.Fatal(err)
	}
	if !rootpublication.SamePhysicalIdentity(restoredIdentity, identity) {
		t.Fatal("materialized identity override lost renamed directory identity")
	}
}

func TestDirectoryRenameRejectsInvalidMovesWithoutEffects(t *testing.T) {
	for _, pair := range [][2]string{{"stage", "stage/db/child"}, {"stage", "missing/final"}, {"stage", "to"}, {"stage", "to/keep"}, {"missing", "final"}} {
		t.Run(pair[1], func(t *testing.T) {
			m := directoryRenameFixture(t)
			before := m.Clone()
			if err := m.Rename(pair[0], pair[1]); err == nil {
				t.Fatalf("rename %v unexpectedly accepted", pair)
			}
			if !reflect.DeepEqual(m.volatileDirs, before.volatileDirs) || !reflect.DeepEqual(m.volatile, before.volatile) || m.StableFingerprint() != before.StableFingerprint() {
				t.Fatal("invalid directory rename changed model")
			}
		})
	}
}

func TestDirectoryRenameReplacesEmptyDirectoryAndChains(t *testing.T) {
	m := directoryRenameFixture(t)
	// Create then remove a child to obtain an empty destination inode with
	// separately stable parent edge and contents.
	if err := m.Create("empty/tmp", nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Unlink("empty/tmp"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("empty"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	oldEmpty := m.volatileDirs["empty"]
	if err := m.Rename("stage", "empty"); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename("empty", "final"); err != nil {
		t.Fatal(err)
	}
	assertStableFile(t, m, "stage/db/root", "sealed-root")
	assertNoStablePath(t, m, "empty/db")
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertStableFile(t, m, "final/db/root", "sealed-root")
	assertNoStablePath(t, m, "stage")
	assertNoStablePath(t, m, "empty")
	if _, ok := m.durableDirs[physicalStableIdentity(oldEmpty)]; ok {
		t.Fatal("empty overwritten inode survived crash without a name")
	}
}

func TestDirectoryRenamePartialBytesAndMissingAncestor(t *testing.T) {
	m := newModel()
	if err := m.Create("unpublished/stage/value", []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := m.PromoteRange("unpublished/stage/value", 0, 3); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("unpublished/stage"); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename("unpublished/stage", "unpublished/final"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("unpublished"); err != nil {
		t.Fatal(err)
	}
	// The renamed edge and partial bytes are durable within this directory
	// object. Its own parent edge is still missing, so neither can be reached.
	assertNoStablePath(t, m, "unpublished")
	hidden := m.Clone()
	hidden.Crash()
	assertNoStablePath(t, hidden, "unpublished")
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertStableFile(t, m, "unpublished/final/value", "abc")
	assertNoStablePath(t, m, "unpublished/stage")
}

func TestDirectoryRemovalObservedAfterOverlayAndNameReuse(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "stage")
	if err := os.MkdirAll(filepath.Join(old, "db"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "db", "old"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity := m.volatileDirs["stage"]
	if err := os.RemoveAll(old); err != nil {
		t.Fatal(err)
	}
	if err := m.Overlay(root); err != nil {
		t.Fatal(err)
	}
	if err := m.Observe(root, durabilitycut.Event{Namespace: durabilitycut.NamespaceUnlink, OldPath: old}); err != nil {
		t.Fatal(err)
	}
	assertStableFile(t, m, "stage/db/old", "old")
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	assertNoStablePath(t, m, "stage")
	// A subsequent same-name directory has fresh contents and identity. Its
	// durable parent edge must not revive the removed object's old children.
	if err := m.Create("stage/new", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if rootpublication.SamePhysicalIdentity(oldIdentity, m.volatileDirs["stage"]) {
		t.Fatal("name reuse retained old directory identity")
	}
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertNoStablePath(t, m, "stage/db")
	assertNoStablePath(t, m, "stage/new")
}
