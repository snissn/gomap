package collections

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "captured_parent",
		},
	}
	var fileSyncs atomic.Uint64
	originalFileSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
		if file == nil {
			t.Fatal("nil column segment sync handle")
		}
		fileSyncs.Add(1)
		return nil
	}
	defer func() { syncColumnAssetSegmentFileForPublish = originalFileSync }()
	var movedDir, replacementDir string
	originalOpenParent := openStableColumnAssetParent
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		movedDir, replacementDir = path+"-moved", path
		if err := os.Rename(path, movedDir); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			_ = parent.Close()
			return nil, err
		}
		return parent, nil
	}
	defer func() { openStableColumnAssetParent = originalOpenParent }()
	ref, token, err := writeColumnAssetToManagerWithStableResource(dir, cfg, []byte("stable-column-payload"), ColumnAssetKindQueryReadyBase, 7, 11)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	segmentPath, err := columnAssetSegmentPath(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(movedDir, filepath.Base(segmentPath))); err != nil {
		t.Fatalf("captured-parent column segment: %v", err)
	}
	if _, err := os.Stat(filepath.Join(replacementDir, filepath.Base(segmentPath))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path unexpectedly received column segment: %v", err)
	}
	if got := fileSyncs.Load(); got != 1 {
		t.Fatalf("column creation file syncs=%d want exactly 1", got)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityQueryReadyBase)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := fileSyncs.Load(); got != 1 {
		t.Fatalf("already-synced column token added content sync: %d", got)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != 1 || stats[0].Flushes != 1 || stats[0].Syncs != 1 {
		t.Fatalf("column stable operation counts=%+v", stats)
	}
}

func TestStableResourceInventoryClassifiesEveryColumnAssetKind(t *testing.T) {
	type expectedPolicy struct {
		field          rootpublication.ReachabilityField
		classification string
	}
	want := map[ColumnAssetKind]expectedPolicy{
		ColumnAssetKindTCS1PartImage:              {rootpublication.ReachabilityTypedColumnMultipart, "authoritative"},
		ColumnAssetKindTCS1TypedColumnPart:        {rootpublication.ReachabilityTypedColumnMultipart, "authoritative"},
		ColumnAssetKindTCS1AggregateMetadata:      {rootpublication.ReachabilityTypedColumnValue, "authoritative"},
		ColumnAssetKindTCS1DictionaryCodes:        {rootpublication.ReachabilityTypedColumnCode, "authoritative"},
		ColumnAssetKindTCS1Int64Values:            {rootpublication.ReachabilityTypedColumnValue, "authoritative"},
		ColumnAssetKindTCS1HNSWSearchPack:         {rootpublication.ReachabilityHNSWSearchPack, "authoritative"},
		ColumnAssetKindQueryReadyBase:             {rootpublication.ReachabilityQueryReadyBase, "rebuildable-non-authoritative"},
		ColumnAssetKindQueryReadyDelta:            {rootpublication.ReachabilityQueryReadyDelta, "rebuildable-non-authoritative"},
		ColumnAssetKindQueryReadyConsolidatedBase: {rootpublication.ReachabilityQueryReadyConsolidatedBase, "rebuildable-non-authoritative"},
	}
	for kind, expected := range want {
		gotKind, gotField, gotClassification, err := stableColumnAssetResourceClassification(kind)
		if err != nil {
			t.Errorf("classify %q: %v", kind, err)
			continue
		}
		if gotField != expected.field {
			t.Errorf("kind %q field=%q want %q", kind, gotField, expected.field)
		}
		if gotKind == "" {
			t.Errorf("kind %q has empty resource kind", kind)
		}
		if gotClassification != expected.classification {
			t.Errorf("kind %q classification=%q want literal %q", kind, gotClassification, expected.classification)
		}
	}
	if _, _, _, err := stableColumnAssetResourceClassification(ColumnAssetKind("future-authoritative-kind")); err == nil {
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
