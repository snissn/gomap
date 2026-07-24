package collections

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVectorPartitionLifecyclePublicV1PublishesAndReopensCheckpointAuthority(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	building := cloneVectorPartitionManifestForCheckpointV1(ready)
	building.State = "building"
	building.RouterGeneration = 0
	building.RouterAsset = VectorPartitionAssetV1{}
	building.ReadySetDigest = ""
	building.Canonicalize()
	if err := store.publishValidatedBuilding(building); err != nil {
		t.Fatal(err)
	}
	if err := store.publishValidatedBuilding(building); err != nil {
		t.Fatalf("idempotent building retry: %v", err)
	}
	reopened, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Open(building.Collection, building.IndexName, building.Generation)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(got, building) {
		t.Fatalf("reopened building=%+v err=%v", got, err)
	}
	if _, err := reopened.OpenActive(building.Collection, building.IndexName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("building unexpectedly active: %v", err)
	}

	if err := reopened.publishValidatedReady(ready); err != nil {
		t.Fatal(err)
	}
	if err := reopened.publishValidatedReady(ready); err != nil {
		t.Fatalf("idempotent ready retry: %v", err)
	}
	active, err := reopened.OpenActive(ready.Collection, ready.IndexName)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(active, ready) {
		t.Fatalf("active ready=%+v err=%v", active, err)
	}
	entries, err := os.ReadDir(reopened.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".vpm") ||
			strings.HasSuffix(entry.Name(), ".active") ||
			strings.HasSuffix(entry.Name(), ".retired") ||
			strings.HasSuffix(entry.Name(), ".inactive") ||
			strings.HasSuffix(entry.Name(), ".deleting") {
			t.Fatalf("public lifecycle recreated mutable authority %q", entry.Name())
		}
	}
}

func TestVectorPartitionLifecyclePublicV1DeactivateAndDeletePrepare(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	if err := store.publishValidatedReady(ready); err != nil {
		t.Fatal(err)
	}
	if err := store.deactivateLocked(ready.Collection, ready.IndexName); err != nil {
		t.Fatal(err)
	}
	if err := store.deactivateLocked(ready.Collection, ready.IndexName); err != nil {
		t.Fatalf("idempotent deactivate retry: %v", err)
	}
	retired, err := store.OpenRetired(ready.Collection, ready.IndexName)
	if err != nil || retired.Generation != ready.Generation {
		t.Fatalf("retired=%+v err=%v", retired, err)
	}
	if err := store.deleteLocked(ready.Collection, ready.IndexName, ready.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	if err := store.deleteLocked(ready.Collection, ready.IndexName, ready.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatalf("idempotent delete retry: %v", err)
	}
	if _, err := store.Open(ready.Collection, ready.IndexName, ready.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleting generation remained open: %v", err)
	}
	loaded, present, err := store.loadVectorPartitionLifecycleAuthorityV1(ready.Collection, ready.IndexName)
	if err != nil || !present {
		t.Fatalf("load deleting authority present=%v err=%v", present, err)
	}
	entry := loaded.state.Generations[ready.Generation]
	if !entry.Deleting || entry.Reclaim == nil || len(entry.Reclaim.OriginalRefs) == 0 {
		t.Fatalf("delete prepare state=%+v", entry)
	}
	if generation, err := store.readInactiveGeneration(ready.Collection, ready.IndexName); err != nil || generation != ready.Generation {
		t.Fatalf("inactive generation=%d err=%v", generation, err)
	}
}

func TestVectorPartitionLifecyclePublicV1ReadyActivationRetry(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	building := cloneVectorPartitionManifestForCheckpointV1(ready)
	building.State = "building"
	building.RouterGeneration = 0
	building.RouterAsset = VectorPartitionAssetV1{}
	building.ReadySetDigest = ""
	building.Canonicalize()
	if err := store.publishValidatedBuilding(building); err != nil {
		t.Fatal(err)
	}

	forced := errors.New("interrupt before activation")
	deltaInstalls := 0
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "before_delta_install" {
			deltaInstalls++
			if deltaInstalls == 2 {
				return forced
			}
		}
		return nil
	})
	err = store.publishValidatedReady(ready)
	restore()
	if !errors.Is(err, forced) {
		t.Fatalf("ready-to-active interruption err=%v", err)
	}
	prepared, err := store.Open(ready.Collection, ready.IndexName, ready.Generation)
	if err != nil || prepared.State != "ready" {
		t.Fatalf("prepared ready=%+v err=%v", prepared, err)
	}
	if _, err := store.OpenActive(ready.Collection, ready.IndexName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted ready unexpectedly active: %v", err)
	}
	if err := store.publishValidatedReady(ready); err != nil {
		t.Fatalf("ready activation retry: %v", err)
	}
	active, err := store.OpenActive(ready.Collection, ready.IndexName)
	if err != nil || !vectorPartitionManifestCanonicalEqualV1(active, ready) {
		t.Fatalf("active retry=%+v err=%v", active, err)
	}
}

func TestVectorPartitionLifecyclePublicV1RejectsConflictsAndLegacyAuthority(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	if err := store.publishValidatedReady(ready); err != nil {
		t.Fatal(err)
	}
	changed := cloneVectorPartitionManifestForCheckpointV1(ready)
	changed.Assets[0].Checksum = strings.Repeat("c", 64)
	changed.Canonicalize()
	if err := store.publishValidatedReady(changed); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("conflicting ready retry err=%v", err)
	}
	legacy := safeVPM(ready.Collection) + "-" + safeVPM(ready.IndexName) + ".active"
	if err := os.WriteFile(store.dir+"/"+legacy, []byte("7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(ready.Collection, ready.IndexName, ready.Generation); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("legacy authority was not rejected: %v", err)
	}
}

func TestVectorPartitionSnapshotEntriesV1SelectsHighestCheckpointAndCurrentTail(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	second, _ := persistLifecycleCheckpointBuildV1(t, store, first.Generation+1)
	_, readyPayload := lifecycleCheckpointReadyForBuildingV1(t, second)
	if err := store.persistVectorPartitionLifecycleOperationV1(
		second.Collection,
		second.IndexName,
		vectorPartitionLifecycleReadyV1,
		second.Generation,
		readyPayload,
	); err != nil {
		t.Fatal(err)
	}
	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	entries, err := VectorPartitionSnapshotEntriesV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, _ := vectorPartitionLifecycleCheckpointNameV1(second.Collection, second.IndexName, 2)
	delta, _ := vectorPartitionLifecycleDeltaNameV1(second.Collection, second.IndexName, 2, 3)
	if len(entries) != 2 || entries[0].Name != checkpoint || entries[1].Name != delta {
		t.Fatalf("snapshot entries=%+v, want current checkpoint and tail", entries)
	}
	firstCheckpoint, _ := vectorPartitionLifecycleCheckpointNameV1(first.Collection, first.IndexName, 1)
	for _, entry := range entries {
		if entry.Name == firstCheckpoint {
			t.Fatalf("snapshot retained superseded audit checkpoint %q", entry.Name)
		}
	}
}

func TestVectorPartitionSnapshotEntriesV1RejectsCorruptHighestAndLegacyAuthority(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	t.Run("corrupt highest has no fallback", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		_, base := lifecycleManifestPayloadV1(t, "building")
		first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
		second, _ := persistLifecycleCheckpointBuildV1(t, store, first.Generation+1)
		highest, _ := vectorPartitionLifecycleCheckpointNameV1(second.Collection, second.IndexName, 2)
		if err := os.WriteFile(store.dir+"/"+highest, []byte("corrupt-highest"), 0o600); err != nil {
			t.Fatal(err)
		}
		dir, err := store.openDir()
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()
		if _, err := VectorPartitionSnapshotEntriesV1(dir); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
			t.Fatalf("corrupt highest snapshot err=%v", err)
		}
	})

	t.Run("legacy authority", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		_, base := lifecycleManifestPayloadV1(t, "building")
		building, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
		legacy := safeVPM(building.Collection) + "-" + safeVPM(building.IndexName) + ".active"
		if err := os.WriteFile(store.dir+"/"+legacy, []byte("7\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		dir, err := store.openDir()
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()
		if _, err := VectorPartitionSnapshotEntriesV1(dir); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
			t.Fatalf("legacy snapshot authority err=%v", err)
		}
	})
}

func TestVectorPartitionSnapshotEntriesV1RejectsHardLinkAlias(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	second, _ := persistLifecycleCheckpointBuildV1(t, store, first.Generation+1)
	firstName, _ := vectorPartitionLifecycleCheckpointNameV1(first.Collection, first.IndexName, 1)
	secondName, _ := vectorPartitionLifecycleCheckpointNameV1(second.Collection, second.IndexName, 2)
	if err := os.Remove(store.dir + "/" + firstName); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(store.dir+"/"+secondName, store.dir+"/"+firstName); err != nil {
		t.Skipf("hard links unsupported: %v", err)
	}
	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if _, err := VectorPartitionSnapshotEntriesV1(dir); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("hard-link alias snapshot err=%v", err)
	}
}

func TestValidateVectorPartitionSnapshotNamespaceV1RequiresExactSelection(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	liveRoot := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(liveRoot)
	if err != nil {
		t.Fatal(err)
	}
	_, base := lifecycleManifestPayloadV1(t, "building")
	first, _ := persistLifecycleCheckpointBuildV1(t, store, base.Generation)
	persistLifecycleCheckpointBuildV1(t, store, first.Generation+1)
	if err := ValidateVectorPartitionSnapshotNamespaceV1(liveRoot); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("live audit namespace validation err=%v", err)
	}

	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	selected, err := VectorPartitionSnapshotEntriesV1(dir)
	if err != nil {
		_ = dir.Close()
		t.Fatal(err)
	}
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	snapshotRoot := t.TempDir()
	snapshotDir := filepath.Join(snapshotRoot, "vector_partitions")
	if err := os.Mkdir(snapshotDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, entry := range selected {
		raw, err := os.ReadFile(filepath.Join(store.dir, entry.Name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshotDir, entry.Name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateVectorPartitionSnapshotNamespaceV1(snapshotRoot); err != nil {
		t.Fatalf("exact snapshot namespace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "legacy.active"), []byte("7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateVectorPartitionSnapshotNamespaceV1(snapshotRoot); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("legacy extracted namespace err=%v", err)
	}
}
