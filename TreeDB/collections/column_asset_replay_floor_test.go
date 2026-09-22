package collections

import (
	"reflect"
	"testing"
)

func TestRecoverableColumnAssetReplayFloorUnknownAuthority(t *testing.T) {
	meta, err := normalizeCollectionMeta(typedMinimaCollectionMeta())
	if err != nil {
		t.Fatal(err)
	}
	meta.Options.ColumnStore.ActiveManifest = &ColumnManifestIdentity{Generation: 3}
	meta.Options.ColumnStore.RecoveryAuthoritativeManifest = &ColumnManifestIdentity{Generation: 3}
	meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN = 8
	if !recoverableColumnAssetReplayFloorCompatible(true, nil, &meta) {
		t.Fatal("valid first root rejected")
	}
	if recoverableColumnAssetReplayFloorCompatible(false, &meta, &meta) {
		t.Fatal("WAL-off authority admitted")
	}
	if recoverableColumnAssetReplayFloorCompatible(true, &meta, nil) {
		t.Fatal("missing root admitted")
	}
	for _, mutate := range []func(*CollectionMeta){
		func(m *CollectionMeta) { m.Name = "recreated" },
		func(m *CollectionMeta) { m.Options.ColumnStore.Enabled = false },
		func(m *CollectionMeta) { m.Options.ColumnStore.ActiveManifest = nil },
		func(m *CollectionMeta) { m.Options.ColumnStore.ActiveManifest.Generation = 0 },
		func(m *CollectionMeta) { m.Options.ColumnStore.RecoveryAuthoritativeManifest = nil },
		func(m *CollectionMeta) { m.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN = 0 },
		func(m *CollectionMeta) { m.Options.ColumnStore.SchemaHash++ },
		func(m *CollectionMeta) { m.Options.ColumnStore.AssetManager.Namespace = "other" },
	} {
		other := copyCollectionMeta(meta)
		mutate(&other)
		if recoverableColumnAssetReplayFloorCompatible(true, &meta, &other) {
			t.Fatalf("incompatible root admitted: %+v", other)
		}
	}
}

func TestRecoverableColumnAssetReplayStrictFloor(t *testing.T) {
	for _, floor := range []uint64{0, 3} {
		basis := recoverableColumnAssetReplayBasis{minimumGeneration: floor, manifestGeneration: 5, appliedCommandLSN: 10, config: ColumnStoreConfig{AssetManager: &ColumnAssetManagerConfig{Namespace: "managed"}}}
		candidates := []ColumnAssetRef{{Generation: 2, Namespace: "managed"}, {Generation: 3, Namespace: "managed"}, {Generation: 4, Namespace: "managed"}, {Generation: 2, Namespace: "other"}}
		refs, err := recoverableColumnAssetReplayRefs(candidates, []recoverableColumnAssetReplayBasis{basis}, func(uint64) bool { return true }, func(ColumnAssetRef, recoverableColumnAssetReplayBasis) (bool, error) { return true, nil })
		want := 4
		if floor != 0 {
			want = 3
		}
		if err != nil || len(refs) != want {
			t.Fatalf("floor=%d refs=%+v err=%v", floor, refs, err)
		}
		expected := candidates
		if floor != 0 {
			expected = candidates[1:]
		}
		if !reflect.DeepEqual(refs, expected) {
			t.Fatalf("floor=%d identities=%+v want=%+v", floor, refs, expected)
		}
	}
}
