//go:build windows

package collections

import (
	"errors"
	"os"
	"path/filepath"
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
