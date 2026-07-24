//go:build treedb_benchmark

package collections

// This file is excluded from default and production builds. It provides the
// explicitly tagged scale harness with a way to replace a one-row,
// collection-authorized fixture by checkpoint-backed synthetic metadata.

import (
	"fmt"
	"os"
	"path/filepath"
)

// StageSyntheticReadyVectorPartitionForBenchmarkV1 publishes a canonical
// checkpoint chain without revalidating the synthetic row count against a live
// TVIS. Callers must first derive manifest and asset refs from a genuinely
// authorized ready fixture. This function exists only under treedb_benchmark.
func StageSyntheticReadyVectorPartitionForBenchmarkV1(root string, manifest VectorPartitionManifestV1) error {
	if root == "" {
		return fmt.Errorf("collections: synthetic vector partition benchmark root is empty")
	}
	vectorDir := filepath.Join(root, "vector_partitions")
	if filepath.Dir(vectorDir) != filepath.Clean(root) || filepath.Base(vectorDir) != "vector_partitions" {
		return fmt.Errorf("collections: invalid synthetic vector partition benchmark root")
	}
	if err := os.RemoveAll(vectorDir); err != nil {
		return err
	}
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		return err
	}
	return store.publishValidatedReady(manifest)
}
