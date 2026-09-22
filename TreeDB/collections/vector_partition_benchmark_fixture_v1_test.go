//go:build treedb_benchmark

package collections

import (
	"testing"
	"time"
)

func stageReadyVectorPartitionBenchmarkFixtureV1(t *testing.T, root, collection, index string) VectorPartitionManifestV1 {
	t.Helper()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	ready.Collection = collection
	ready.IndexName = index
	ready.Canonicalize()
	building := cloneVectorPartitionManifestForCheckpointV1(ready)
	building.State = "building"
	building.RouterGeneration = 0
	building.RouterAsset = VectorPartitionAssetV1{}
	building.ReadySetDigest = ""
	building.Canonicalize()
	if err := store.publishValidatedBuilding(building); err != nil {
		t.Fatal(err)
	}
	if err := store.publishValidatedReady(ready); err != nil {
		t.Fatal(err)
	}
	return ready
}

func TestStageSyntheticReadyVectorPartitionForBenchmarkV1ReplacesExclusiveFixture(t *testing.T) {
	root := t.TempDir()
	expected := stageReadyVectorPartitionBenchmarkFixtureV1(t, root, "docs", "embedding")
	replacement := expected
	replacement.SourceChecksum++
	replacement.Canonicalize()
	done := make(chan error, 1)
	go func() {
		done <- StageSyntheticReadyVectorPartitionForBenchmarkV1(root, expected, replacement)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("synthetic benchmark fixture replacement deadlocked")
	}
	store, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	active, err := store.OpenActive(replacement.Collection, replacement.IndexName)
	if err != nil || active.SourceChecksum != replacement.SourceChecksum {
		t.Fatalf("replacement active=%+v err=%v", active, err)
	}
}

func TestStageSyntheticReadyVectorPartitionForBenchmarkV1RejectsAdditionalIdentity(t *testing.T) {
	root := t.TempDir()
	expected := stageReadyVectorPartitionBenchmarkFixtureV1(t, root, "docs", "embedding")
	other := stageReadyVectorPartitionBenchmarkFixtureV1(t, root, "docs", "other")
	replacement := expected
	replacement.SourceChecksum++
	replacement.Canonicalize()
	if err := StageSyntheticReadyVectorPartitionForBenchmarkV1(root, expected, replacement); err == nil {
		t.Fatal("synthetic benchmark fixture accepted another lifecycle identity")
	}
	store, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenActive(other.Collection, other.IndexName); err != nil {
		t.Fatalf("rejected replacement removed other identity: %v", err)
	}
}

func TestStageSyntheticReadyVectorPartitionForBenchmarkV1RejectsInvalidReplacementBeforeRemoval(t *testing.T) {
	root := t.TempDir()
	expected := stageReadyVectorPartitionBenchmarkFixtureV1(t, root, "docs", "embedding")
	invalid := expected
	invalid.State = "invalid"
	if err := StageSyntheticReadyVectorPartitionForBenchmarkV1(root, expected, invalid); err == nil {
		t.Fatal("synthetic benchmark fixture accepted invalid replacement")
	}
	store, err := OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenActive(expected.Collection, expected.IndexName); err != nil {
		t.Fatalf("invalid replacement removed expected fixture: %v", err)
	}
}
