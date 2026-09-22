package collections

import (
	"path/filepath"
	"testing"
)

func TestColumnAssetManagerNamespaceForRootCachedM1634(t *testing.T) {
	root := filepath.Join(t.TempDir(), "column_assets")
	namespaceName := "events/column-assets"
	first, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot first: %v", err)
	}
	if first.ManagerRootDir != root || first.RootDir == "" || first.SegmentDir == "" {
		t.Fatalf("unexpected namespace paths: %+v", first)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
		if err != nil {
			t.Fatalf("columnAssetManagerNamespaceForRoot cached: %v", err)
		}
		if got != first {
			t.Fatalf("cached namespace mismatch got=%+v want=%+v", got, first)
		}
	})
	if allocs != 0 {
		t.Fatalf("cached namespace path allocs/run=%0.2f want 0", allocs)
	}
}
