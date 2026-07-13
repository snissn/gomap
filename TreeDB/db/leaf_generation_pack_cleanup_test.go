package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCleanupRewriteCreatedSegmentRespectsStableIdentityPin(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(42, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := valuelog.SegmentPath(dir, fileID)
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("rewrite-created")); err != nil {
		_ = writer.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr, err := valuelog.NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	token, err := mgr.StableResourceToken(fileID, valuelog.StableResourceRegistration{
		Kind:           rootpublication.ResourceValueLog,
		LogicalLane:    "rewrite",
		Generation:     1,
		DiagnosticPath: filepath.Base(path),
		Reachability:   rootpublication.ReachabilityValueLogPointer,
		Digest:         [32]byte{1},
	})
	if err != nil {
		t.Fatalf("StableResourceToken: %v", err)
	}

	database := &DB{valueLogManager: mgr}
	created := []rewriteCreatedSegment{{path: path, fileID: fileID}}
	err = database.cleanupRewriteCreatedSegments(created)
	if !errors.Is(err, valuelog.ErrFilePinned) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("cleanup while pinned error=%v, want ErrFilePinned and ErrRecoveryRequired", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stable-pinned rewrite segment removed: %v", err)
	}

	token.Release()
	if err := database.cleanupRewriteCreatedSegments(created); err != nil {
		t.Fatalf("cleanup after release: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rewrite segment remains after pin release: %v", err)
	}
}

func TestCleanupRewriteCreatedManagerSegmentPostUnlinkCutRequiresRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), leafVLogDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll leaf_vlog: %v", err)
	}
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
			event.Resource == durabilitycut.ResourceOuterLeaf &&
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
