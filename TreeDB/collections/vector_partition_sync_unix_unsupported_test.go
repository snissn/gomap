//go:build darwin || freebsd || netbsd || openbsd

package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestVectorPartitionUnsupportedUnixMutationsFailClosedWithoutNamespaceProof(t *testing.T) {
	if VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Fatal("vector partition namespace persistence unexpectedly reported as supported")
	}
	root := t.TempDir()
	if _, err := OpenVectorPartitionStoreV1(root); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("store creation err=%v want namespace persistence unsupported", err)
	}
	if _, err := os.Stat(filepath.Join(root, "vector_partitions")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed store creation mutated namespace: stat err=%v", err)
	}
}
