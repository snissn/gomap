//go:build windows

package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableColumnAssetCaptureFailsClosedWithoutRelativeParentOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{
		Enabled: true,
		AssetManager: &ColumnAssetManagerConfig{
			Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_parent",
		},
	}
	ref, token, err := writeColumnAssetToManagerWithStableResource(
		dir, cfg, []byte("must-not-become-visible"), ColumnAssetKindQueryReadyBase, 7, 11,
	)
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("stable column capture error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if token != nil || ref != (ColumnAssetRef{}) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("unsupported capture returned ref=%+v token=%v", ref, token)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(columnAssetM12ASegmentFileID))
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported capture exposed segment %q: %v", segmentPath, err)
	}
}

func TestStableColumnAppendSessionFailsClosedBeforeSegmentVisibility(t *testing.T) {
	dir := t.TempDir()
	cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "unsupported_session_parent",
	}}
	registry := rootpublication.NewIdentityPinRegistry()
	session := newColumnPhysicalAssetAppendSessionWithStableResources(dir, cfg, registry)
	refs, err := session.appendKinds(columnAssetM12ASegmentFileID, []columnPhysicalAssetAppendItem{{
		payload: []byte("must-not-become-visible"), kind: ColumnAssetKindTCS1PartImage, generation: 1, partID: 1,
	}})
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		_ = session.abort()
		t.Fatalf("stable session append error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if len(refs) != 0 {
		t.Fatalf("unsupported stable session returned refs=%+v", refs)
	}
	if err := session.abort(); err != nil {
		t.Fatal(err)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(dir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(columnAssetM12ASegmentFileID))
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported stable session exposed segment %q: %v", segmentPath, err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("unsupported stable session active identities=%d want 0", got)
	}
}
