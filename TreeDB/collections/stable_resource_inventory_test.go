package collections

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableResourceInventoryClassifiesEveryColumnAssetKind(t *testing.T) {
	want := map[ColumnAssetKind]rootpublication.ReachabilityField{
		ColumnAssetKindTCS1PartImage:              rootpublication.ReachabilityTypedColumnMultipart,
		ColumnAssetKindTCS1TypedColumnPart:        rootpublication.ReachabilityTypedColumnMultipart,
		ColumnAssetKindTCS1AggregateMetadata:      rootpublication.ReachabilityTypedColumnValue,
		ColumnAssetKindTCS1DictionaryCodes:        rootpublication.ReachabilityTypedColumnCode,
		ColumnAssetKindTCS1Int64Values:            rootpublication.ReachabilityTypedColumnValue,
		ColumnAssetKindTCS1HNSWSearchPack:         rootpublication.ReachabilityHNSWSearchPack,
		ColumnAssetKindQueryReadyBase:             rootpublication.ReachabilityQueryReadyBase,
		ColumnAssetKindQueryReadyDelta:            rootpublication.ReachabilityQueryReadyDelta,
		ColumnAssetKindQueryReadyConsolidatedBase: rootpublication.ReachabilityQueryReadyConsolidatedBase,
	}
	for kind, field := range want {
		gotKind, gotField, err := stableColumnAssetResourceClassification(kind)
		if err != nil {
			t.Errorf("classify %q: %v", kind, err)
			continue
		}
		if gotField != field {
			t.Errorf("kind %q field=%q want %q", kind, gotField, field)
		}
		if gotKind == "" {
			t.Errorf("kind %q has empty resource kind", kind)
		}
	}
	if _, _, err := stableColumnAssetResourceClassification(ColumnAssetKind("future-authoritative-kind")); err == nil {
		t.Fatal("unknown column asset kind did not fail inventory coverage")
	}
}

func TestStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "creation-first", true: "creation-last"}[reverse], func(t *testing.T) {
			dir := t.TempDir()
			cfg := ColumnStoreConfig{
				Enabled: true,
				AssetManager: &ColumnAssetManagerConfig{
					Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "coalesce_resource",
				},
			}
			firstRef, first, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("first-payload"), ColumnAssetKindQueryReadyBase, 7, 11)
			if err != nil {
				t.Fatal(err)
			}
			secondRef, second, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("second-payload"), ColumnAssetKindQueryReadyBase, 7, 12)
			if err != nil {
				first.Release()
				t.Fatal(err)
			}
			if first.Namespace() == nil || second.Namespace() != nil {
				first.Release()
				second.Release()
				t.Fatalf("namespace obligations first=%v second=%v", first.Namespace() != nil, second.Namespace() != nil)
			}
			ordered := []*rootpublication.StableResourceToken{first, second}
			if reverse {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityQueryReadyBase)
			for _, token := range ordered {
				if err := builder.Add(token); err != nil {
					builder.Abandon()
					t.Fatal(err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			if set.Len() != 1 {
				t.Fatalf("coalesced len=%d want 1", set.Len())
			}
			if got, want := set.FrontierFor(first.Identity(), uint64(firstRef.FileID)).Bytes, uint64(secondRef.Offset+secondRef.Length); got != want {
				t.Fatalf("coalesced frontier=%d want %d", got, want)
			}
			stats := set.Stats(time.Now())
			if len(stats) != 1 || stats[0].ActivePins != 1 || stats[0].NamespaceSyncs != 1 {
				t.Fatalf("coalesced stats=%+v", stats)
			}
		})
	}
}

func TestStableColumnAssetTokenBindsExactSegmentAndRange(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "stable_resource",
		},
	}
	ref, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("stable-column-payload"), ColumnAssetKindQueryReadyBase, 7, 11)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if token.Kind() != rootpublication.ResourceQueryReadyAsset || token.Reachability() != rootpublication.ReachabilityQueryReadyBase {
		t.Fatalf("token kind=%q field=%q", token.Kind(), token.Reachability())
	}
	if token.Frontier().Bytes != uint64(ref.Offset+ref.Length) {
		t.Fatalf("frontier=%d want %d", token.Frontier().Bytes, ref.Offset+ref.Length)
	}
	if token.Digest() == [32]byte{} {
		t.Fatal("column asset token missing immutable ref digest")
	}
	if token.Namespace() == nil {
		t.Fatal("new column segment token missing stable namespace operation")
	}

	segmentPath, err := columnAssetSegmentPath(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPath := filepath.Join(filepath.Dir(segmentPath), "rotated-original.seg")
	if err := os.Rename(segmentPath, rotatedPath); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement-path-bytes")
	if err := os.WriteFile(segmentPath, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, ref.Length)
	if _, err := token.ReadAt(got, ref.Offset); err != nil {
		t.Fatal(err)
	}
	if string(got) != "stable-column-payload" {
		t.Fatalf("pinned token read %q after path replacement", got)
	}
}
