package collections

import (
	"os"
	"testing"
)

func writeColumnVectorGraphAssetRawForTest2041(t testing.TB, rootDir string, ref ColumnAssetRef, raw []byte) {
	t.Helper()
	if int64(len(raw)) != ref.Length {
		t.Fatalf("corrupt raw bytes=%d want ref length=%d", len(raw), ref.Length)
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open asset segment: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(raw, ref.Offset); err != nil {
		t.Fatalf("write corrupt asset raw: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt asset raw: %v", err)
	}
}

func replaceColumnVectorIndexStateAssetForTest2041(t testing.TB, state columnVectorIndexStateSnapshot, replacement columnVectorIndexStateAssetSnapshot) columnVectorIndexStateSnapshot {
	t.Helper()
	state.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	for i := range state.Assets {
		if state.Assets[i].Role == replacement.Role && state.Assets[i].AssetID == replacement.AssetID {
			state.Assets[i] = replacement
			return state
		}
	}
	t.Fatalf("replacement asset role=%q id=%q not found", replacement.Role, replacement.AssetID)
	return state
}
