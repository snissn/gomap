package collections

import (
	"errors"
	"testing"
)

func TestTypedGraphFoldConstructionWorkspaceAdmission(t *testing.T) {
	view := columnPhysicalScanSnapshotView{AssetRefs: []columnManifestAssetRefForScan{{Rows: 8, Ref: ColumnAssetRef{Length: 64}}}}
	cold := typedGraphColdLimits{DecodedTermBytes: 1 << 20, AssetBytes: 1 << 20}
	if _, err := validateTypedGraphFoldView(view, cold, 8, VectorIndexDefinition{M: 16}); err != nil {
		t.Fatal(err)
	}
	for _, m := range []int{int(^uint(0) >> 1), 1 << 20} {
		if _, err := validateTypedGraphFoldView(view, cold, 8, VectorIndexDefinition{M: m}); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
			t.Fatalf("M=%d err=%v want pre-allocation denial", m, err)
		}
	}
	// Keep the profiled 16K/8D/M16 case usable under its existing limits.
	view.AssetRefs[0].Rows = 16384
	cold.DecodedTermBytes = 512 << 20
	if _, err := validateTypedGraphFoldView(view, cold, 16384, VectorIndexDefinition{M: 16, EfConstruction: int(^uint(0) >> 1)}); err != nil {
		t.Fatalf("bounded graph population must not scale scratch by EF: %v", err)
	}
	// N*M and layer headers fit, but concurrent planning scratch does not.
	cold.DecodedTermBytes = 16 << 20
	if _, err := validateTypedGraphFoldView(view, cold, 16384, VectorIndexDefinition{M: 1}); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("planning scratch err=%v", err)
	}
	// Small N, large degree: reciprocal scratch exceeds the individual budget.
	view.AssetRefs[0].Rows = 8
	cold.DecodedTermBytes = 32 << 20
	if _, err := validateTypedGraphFoldView(view, cold, 8, VectorIndexDefinition{M: 3000}); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("reciprocal scratch err=%v", err)
	}
}
