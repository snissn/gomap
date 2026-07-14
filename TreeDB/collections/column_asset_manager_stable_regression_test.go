package collections

import (
	"errors"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestColumnPhysicalAssetAppendSessionAbortLegacyWithoutStableBuilder(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	session := newColumnPhysicalAssetAppendSession(backenddb.ColumnAssetRootDirPath(t.TempDir()), *cfg)
	if _, err := session.appender(columnAssetM12ASegmentFileID); err != nil {
		t.Fatalf("appender: %v", err)
	}
	if err := session.abort(); err != nil {
		t.Fatalf("abort legacy session: %v", err)
	}
}

func TestColumnPhysicalAssetFailedAppenderDoesNotCaptureStableResources(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	appender, err := newColumnPhysicalAssetSegmentAppendWriter(backenddb.ColumnAssetRootDirPath(t.TempDir()), *cfg, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
	}
	ref, err := appender.appendKind([]byte("failed-appender"), ColumnAssetKindTCS1PartImage, 1, columnPhysicalRowAssetPartID)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("appendKind: %v", err)
	}
	appender.failed = true
	resources, err := appender.closeWithStableResources([]ColumnAssetRef{ref}, nil)
	if err == nil {
		t.Fatal("closeWithStableResources error=nil, want failed appender error")
	}
	if errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("failed appender attempted stable capture: %v", err)
	}
	if resources != nil {
		resources.Release()
		t.Fatal("failed appender returned stable resources")
	}
}

func pinColumnAssetRefIdentityForTest(t testing.TB, rootDir string, registry *rootpublication.IdentityPinRegistry, ref ColumnAssetRef) *rootpublication.StableResourceToken {
	t.Helper()
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open column asset segment: %v", err)
	}
	token, tokenErr := stableColumnAssetResourceTokenWithRegistry(file, ref, nil, registry)
	closeErr := file.Close()
	if tokenErr != nil {
		t.Fatalf("stableColumnAssetResourceTokenWithRegistry: %v", tokenErr)
	}
	if closeErr != nil {
		token.Release()
		t.Fatalf("close column asset segment: %v", closeErr)
	}
	return token
}
