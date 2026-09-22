//go:build treedb_benchmark

package collections

// This file is excluded from default and production builds. It provides the
// explicitly tagged scale harness with a way to replace a one-row,
// collection-authorized fixture by checkpoint-backed synthetic metadata.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// StageSyntheticReadyVectorPartitionForBenchmarkV1 publishes a canonical
// checkpoint chain without revalidating the synthetic row count against a live
// TVIS. The caller must provide the exact active fixture it observed before
// deriving replacement; the namespace is removed only while that expectation
// still matches byte-for-byte. This function exists only under treedb_benchmark.
func StageSyntheticReadyVectorPartitionForBenchmarkV1(root string, expected, replacement VectorPartitionManifestV1) error {
	if root == "" {
		return fmt.Errorf("collections: synthetic vector partition benchmark root is empty")
	}
	vectorDir := filepath.Join(root, "vector_partitions")
	if filepath.Dir(vectorDir) != filepath.Clean(root) || filepath.Base(vectorDir) != "vector_partitions" {
		return fmt.Errorf("collections: invalid synthetic vector partition benchmark root")
	}
	if replacement.Collection != expected.Collection ||
		replacement.IndexName != expected.IndexName ||
		replacement.Generation != expected.Generation {
		return fmt.Errorf("collections: synthetic vector partition benchmark replacement identity mismatch")
	}
	if _, err := EncodeVectorPartitionManifestV1(replacement); err != nil {
		return fmt.Errorf("collections: invalid synthetic vector partition benchmark replacement: %w", err)
	}
	return WithVectorPartitionStorageBarrierV1(root, func() error {
		store, err := OpenExistingVectorPartitionStoreV1(root)
		if err != nil {
			return err
		}
		active, err := store.OpenActive(expected.Collection, expected.IndexName)
		if err != nil {
			return err
		}
		activeRaw, err := EncodeVectorPartitionManifestV1(active)
		if err != nil {
			return err
		}
		expectedRaw, err := EncodeVectorPartitionManifestV1(expected)
		if err != nil {
			return err
		}
		if !bytes.Equal(activeRaw, expectedRaw) {
			return fmt.Errorf("collections: synthetic vector partition benchmark fixture expectation mismatch")
		}
		if err := requireExclusiveVectorPartitionBenchmarkFixtureV1(store, expected); err != nil {
			return err
		}
		if err := os.RemoveAll(vectorDir); err != nil {
			return err
		}
		store, err = OpenVectorPartitionStoreV1(root)
		if err != nil {
			return err
		}
		return store.publishLocked(replacement)
	})
}

func requireExclusiveVectorPartitionBenchmarkFixtureV1(store *VectorPartitionStoreV1, expected VectorPartitionManifestV1) error {
	dir, err := store.openDir()
	if err != nil {
		return err
	}
	selected, err := VectorPartitionSnapshotEntriesV1(dir)
	if err != nil {
		_ = dir.Close()
		return err
	}
	all, err := readVectorPartitionDirEntriesBoundedV1(dir)
	if err != nil {
		_ = dir.Close()
		return err
	}
	closeErr := dir.Close()
	if closeErr != nil {
		return closeErr
	}
	if len(all) != len(selected) {
		return fmt.Errorf("collections: synthetic vector partition benchmark fixture contains audit history")
	}
	prefix := vectorPartitionLifecycleNamePrefixV1(expected.Collection, expected.IndexName)
	for _, entry := range all {
		if !strings.HasPrefix(entry.Name(), prefix) {
			return fmt.Errorf("collections: synthetic vector partition benchmark fixture contains another identity")
		}
	}
	if len(all) == 0 {
		return errors.New("collections: synthetic vector partition benchmark fixture namespace is empty")
	}
	return nil
}
