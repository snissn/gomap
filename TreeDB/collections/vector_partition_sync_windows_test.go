//go:build windows

package collections

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Windows has no generic proof for the link/remove namespace transitions VPM
// needs. This runtime test protects against accidentally reviving a no-op
// directory Sync or a raw write-through-only publication claim.
func TestVectorPartitionWindowsMutationsFailClosedWithoutNamespaceProof(t *testing.T) {
	if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("store creation err=%v want namespace persistence unsupported", err)
	}
}
