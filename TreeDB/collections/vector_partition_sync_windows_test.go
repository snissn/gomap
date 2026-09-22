//go:build windows

package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	for name, tt := range map[string]struct {
		mutate                  func() error
		wantCollectionAuthority bool
	}{
		"publish": {
			mutate:                  func() error { return store.Publish(building) },
			wantCollectionAuthority: true,
		},
		"deactivate": {
			mutate:                  func() error { return store.Deactivate(building.Collection, building.IndexName) },
			wantCollectionAuthority: true,
		},
		"delete": {
			mutate: func() error {
				return store.Delete(building.Collection, building.IndexName, building.Generation, VectorPartitionCleanupEligibilityV1{})
			},
			wantCollectionAuthority: true,
		},
	} {
		err := tt.mutate()
		if tt.wantCollectionAuthority {
			if err == nil || !strings.Contains(err.Error(), "requires collection authority") {
				t.Fatalf("%s err=%v want collection-authority rejection before namespace mutation", name, err)
			}
		} else if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
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
	buildingRaw, building := lifecycleManifestPayloadV1(t, "building")
	_, m := lifecycleManifestPayloadV1(t, "ready")
	build := lifecycleRecordV1(t, 1, [32]byte{}, vectorPartitionLifecycleBuildV1, m.Generation, buildingRaw)
	ready := lifecycleRecordV1(t, 2, build.Digest, vectorPartitionLifecycleReadyV1, m.Generation, lifecycleReadyPromotionPayloadV1(t, building, m))
	active := lifecycleRecordV1(t, 3, ready.Digest, vectorPartitionLifecycleLocalActivateV1, m.Generation, nil)
	checkpoint := lifecycleCheckpointV1(t, []vectorPartitionLifecycleRecordV1{build, ready, active}, 1)
	raw, err := encodeVectorPartitionLifecycleCheckpointCanonicalV1(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	name, err := vectorPartitionLifecycleCheckpointNameV1(m.Collection, m.IndexName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
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
