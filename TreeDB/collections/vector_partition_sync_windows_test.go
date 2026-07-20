//go:build windows

package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Windows has no generic proof for the link/remove namespace transitions VPM
// needs. This runtime test protects against accidentally reviving a no-op
// directory Sync or a raw write-through-only publication claim.
func TestVectorPartitionWindowsMutationsFailClosedWithoutNamespaceProof(t *testing.T) {
	root := t.TempDir()
	if _, err := OpenVectorPartitionStoreV1(root); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("store creation err=%v want namespace persistence unsupported", err)
	}
	if _, err := os.Stat(filepath.Join(root, "vector_partitions")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed store creation mutated namespace: stat err=%v", err)
	}
}

func TestVectorPartitionWindowsExistingNamespaceMutatorsLeaveNoTrace(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "vector_partitions")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sentinel"), []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotVPMWindowsTree(t, root)
	building := testVectorPartitionManifestV1()
	building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	building.Canonicalize()
	for name, mutate := range map[string]func() error{
		"publish":    func() error { return store.Publish(building) },
		"deactivate": func() error { return store.Deactivate(building.Collection, building.IndexName) },
		"delete": func() error {
			return store.Delete(building.Collection, building.IndexName, building.Generation, VectorPartitionCleanupEligibilityV1{})
		},
	} {
		if err := mutate(); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("%s err=%v want namespace persistence unsupported", name, err)
		}
		if after := snapshotVPMWindowsTree(t, root); !bytes.Equal(after, before) {
			t.Fatalf("%s mutated namespace: before=%q after=%q", name, before, after)
		}
	}
}

func TestVectorPartitionWindowsOpenExistingReadyNamespaceIsReadOnly(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "vector_partitions")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	name := safeVPM(m.Collection) + "-" + safeVPM(m.IndexName) + "-" + strconv.FormatUint(m.Generation, 10) + ".vpm"
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active"), []byte(strconv.FormatUint(m.Generation, 10)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before := snapshotVPMWindowsTree(t, root)
	store, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.OpenActive(m.Collection, m.IndexName)
	if err != nil || got.Generation != m.Generation || got.State != "ready" {
		t.Fatalf("OpenActive=%+v err=%v", got, err)
	}
	if after := snapshotVPMWindowsTree(t, root); !bytes.Equal(before, after) {
		t.Fatal("read-only recovery mutated restored namespace")
	}
}

func snapshotVPMWindowsTree(t *testing.T, root string) []byte {
	t.Helper()
	var snapshot []byte
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		snapshot = append(snapshot, rel...)
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		snapshot = append(snapshot, raw...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
