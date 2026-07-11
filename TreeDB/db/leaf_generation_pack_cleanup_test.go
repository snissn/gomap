package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCleanupRewriteCreatedManagerSegmentPostUnlinkCutRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(42, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := valuelog.SegmentPath(dir, fileID)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	mgr, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	db := &DB{valueLogManager: mgr}
	wantErr := errors.New("injected post-unlink cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink &&
			event.Resource == durabilitycut.ResourceValueLog &&
			filepath.Clean(event.OldPath) == filepath.Clean(path) {
			return wantErr
		}
		return nil
	})
	err = db.cleanupRewriteCreatedSegments([]rewriteCreatedSegment{{path: path, fileID: fileID}})
	restore()
	if !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("cleanupRewriteCreatedSegments error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("post-unlink cut path stat=%v, want removed", statErr)
	}
}
