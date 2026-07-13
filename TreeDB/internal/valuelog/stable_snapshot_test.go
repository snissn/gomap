package valuelog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestStableWriterSnapshot_RemainsBoundAcrossRotationAndPathReplacement(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")

	w, err := NewWriter(path1, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Append(0, nil, 1, []byte("captured-value")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	snapshot, err := w.CaptureStableSnapshot(path1)
	if err != nil {
		t.Fatalf("CaptureStableSnapshot: %v", err)
	}
	defer snapshot.Release()
	if snapshot.Frontier() == 0 {
		t.Fatal("captured frontier is zero")
	}

	if err := w.RotateToWithSync(path2, page.ValueLogFileID(2), false); err != nil {
		t.Fatalf("RotateToWithSync: %v", err)
	}
	if runtime.GOOS == "windows" {
		if err := snapshot.SyncThrough(context.Background(), snapshot.Frontier()); err != nil {
			t.Fatalf("SyncThrough captured Windows handle after rotation: %v", err)
		}
		return
	}
	archived := filepath.Join(dir, "captured.log")
	if err := os.Rename(path1, archived); err != nil {
		t.Fatalf("Rename captured segment: %v", err)
	}
	if err := os.WriteFile(path1, []byte("replacement"), 0o644); err != nil {
		t.Fatalf("Write replacement: %v", err)
	}

	replacementInfo, err := os.Stat(path1)
	if err != nil {
		t.Fatalf("Stat replacement: %v", err)
	}
	capturedInfo, err := snapshot.f.Stat()
	if err != nil {
		t.Fatalf("Stat captured descriptor: %v", err)
	}
	if os.SameFile(capturedInfo, replacementInfo) {
		t.Fatal("snapshot descriptor rebound to replacement path")
	}
	if err := snapshot.FlushThrough(context.Background(), snapshot.Frontier()); err != nil {
		t.Fatalf("FlushThrough after rotation: %v", err)
	}
	if err := snapshot.SyncThrough(context.Background(), snapshot.Frontier()); err != nil {
		t.Fatalf("SyncThrough after rotation: %v", err)
	}
	if got := w.FileID(); got != page.ValueLogFileID(2) {
		t.Fatalf("active writer file ID=%d want %d", got, page.ValueLogFileID(2))
	}
}

func TestStableWriterSnapshot_RejectsFrontierBeyondCapturedLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value-l0-000001.log")
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Append(0, nil, 1, []byte("value")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	snapshot, err := w.CaptureStableSnapshot(path)
	if err != nil {
		t.Fatalf("CaptureStableSnapshot: %v", err)
	}
	defer snapshot.Release()

	err = snapshot.FlushThrough(context.Background(), snapshot.Frontier()+1)
	if !errors.Is(err, ErrStableSnapshotFrontier) {
		t.Fatalf("FlushThrough error=%v want %v", err, ErrStableSnapshotFrontier)
	}
}

func TestStableWriterSnapshot_IdentityChangesForSamePathAndFileIDReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value-l0-000001.log")
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter old: %v", err)
	}
	if _, err := w.Append(0, nil, 1, []byte("old")); err != nil {
		t.Fatalf("Append old: %v", err)
	}
	oldSnapshot, err := w.CaptureStableSnapshot(path)
	if err != nil {
		t.Fatalf("Capture old: %v", err)
	}
	oldIdentity := oldSnapshot.FileIdentity()
	oldTokenIdentity := oldSnapshot.StableIdentity()
	defer oldSnapshot.Release()
	if err := w.Close(); err != nil {
		t.Fatalf("Close old: %v", err)
	}
	// Windows does not guarantee path replacement while a duplicated handle is
	// open. The opaque identity remains valid after releasing its descriptor.
	if runtime.GOOS == "windows" {
		oldSnapshot.Release()
	}
	if err := os.Rename(path, filepath.Join(filepath.Dir(path), "retired-old.log")); err != nil {
		t.Fatalf("Rename old: %v", err)
	}

	replacement, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter replacement: %v", err)
	}
	defer func() { _ = replacement.Close() }()
	if _, err := replacement.Append(0, nil, 2, []byte("replacement")); err != nil {
		t.Fatalf("Append replacement: %v", err)
	}
	replacementSnapshot, err := replacement.CaptureStableSnapshot(path)
	if err != nil {
		t.Fatalf("Capture replacement: %v", err)
	}
	defer replacementSnapshot.Release()

	if oldIdentity == replacementSnapshot.FileIdentity() {
		t.Fatal("same-path/same-ID replacement reused opaque file identity")
	}
	if oldTokenIdentity == replacementSnapshot.StableIdentity() {
		t.Fatalf("token identity reused across replacement: %q", oldTokenIdentity)
	}
}

func TestStableWriterSnapshot_EstablishedSegmentCaptureSupported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value-l0-000001.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create established segment: %v", err)
	}
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Append(0, nil, 1, []byte("value")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	snapshot, err := w.CaptureStableSnapshot(path)
	if err != nil {
		t.Fatalf("CaptureStableSnapshot established segment: %v", err)
	}
	defer snapshot.Release()
	if snapshot.StableIdentity() == "" {
		t.Fatal("established segment has empty platform identity")
	}
	if snapshot.NamespaceRequired() {
		t.Fatal("established segment unexpectedly requires namespace persistence")
	}
}

func TestStableWriterSnapshot_ExistingAppendHasZeroNamespaceSyncs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value-l0-000001.log")
	seed, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter seed: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("Close seed: %v", err)
	}
	w, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter existing: %v", err)
	}
	defer func() { _ = w.Close() }()
	if got := w.DurabilityStats().DirectorySyncCalls; got != 0 {
		t.Fatalf("existing segment open namespace sync calls=%d want 0", got)
	}
	if _, err := w.Append(0, nil, 1, []byte("ordinary-append")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	snapshot, err := w.CaptureStableSnapshot(path)
	if err != nil {
		t.Fatalf("CaptureStableSnapshot: %v", err)
	}
	defer snapshot.Release()
	if snapshot.NamespaceRequired() {
		t.Fatal("ordinary append has namespace obligation")
	}
	if err := snapshot.EstablishNamespace(context.Background()); err != nil {
		t.Fatalf("EstablishNamespace ordinary append: %v", err)
	}
	if got := w.DurabilityStats().DirectorySyncCalls; got != 0 {
		t.Fatalf("ordinary append namespace sync calls=%d want 0", got)
	}
}

func TestStableWriterSnapshot_NewRotationNamespaceEstablishedExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	w, err := NewWriter(path1, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Append(0, nil, 1, []byte("old")); err != nil {
		t.Fatalf("Append old: %v", err)
	}
	if err := w.RotateToWithSync(path2, page.ValueLogFileID(2), false); err != nil {
		t.Fatalf("RotateToWithSync: %v", err)
	}
	if _, err := w.Append(0, nil, 2, []byte("new")); err != nil {
		t.Fatalf("Append new: %v", err)
	}

	snapshot, err := w.CaptureStableSnapshot(path2)
	if runtime.GOOS == "windows" {
		if !errors.Is(err, ErrStableNamespaceUnsupported) {
			t.Fatalf("CaptureStableSnapshot error=%v want %v", err, ErrStableNamespaceUnsupported)
		}
		return
	}
	if err != nil {
		t.Fatalf("CaptureStableSnapshot: %v", err)
	}
	defer snapshot.Release()
	if !snapshot.NamespaceRequired() {
		t.Fatal("new rotated segment has no namespace obligation")
	}
	if snapshot.NamespaceTargetIdentity() != snapshot.FileIdentity() {
		t.Fatal("namespace target is not bound to captured file identity")
	}
	archived := filepath.Join(dir, "captured-new.log")
	if err := os.Rename(path2, archived); err != nil {
		t.Fatalf("Rename captured namespace target: %v", err)
	}
	if err := os.WriteFile(path2, []byte("replacement"), 0o644); err != nil {
		t.Fatalf("Write replacement namespace target: %v", err)
	}
	replacementFile, err := os.Open(path2)
	if err != nil {
		t.Fatalf("Open replacement namespace target: %v", err)
	}
	replacementIdentity, identityErr := StableFileIdentityFromFile(replacementFile)
	_ = replacementFile.Close()
	if identityErr != nil {
		t.Fatalf("replacement identity: %v", identityErr)
	}
	if snapshot.NamespaceTargetIdentity() == replacementIdentity {
		t.Fatal("namespace obligation rebound to same-path replacement")
	}

	before := w.DurabilityStats().DirectorySyncCalls
	if err := snapshot.EstablishNamespace(context.Background()); err == nil {
		t.Fatal("EstablishNamespace accepted a same-path replacement")
	}
	if got := w.DurabilityStats().DirectorySyncCalls - before; got != 0 {
		t.Fatalf("namespace sync calls after conflict=%d want 0", got)
	}
	if err := os.Remove(path2); err != nil {
		t.Fatalf("Remove replacement namespace target: %v", err)
	}
	if err := os.Rename(archived, path2); err != nil {
		t.Fatalf("Restore captured namespace target: %v", err)
	}
	if err := snapshot.EstablishNamespace(context.Background()); err != nil {
		t.Fatalf("EstablishNamespace restored target: %v", err)
	}
	if err := snapshot.EstablishNamespace(context.Background()); err != nil {
		t.Fatalf("EstablishNamespace second: %v", err)
	}
	if got := w.DurabilityStats().DirectorySyncCalls - before; got != 1 {
		t.Fatalf("namespace sync calls=%d want 1", got)
	}

	ordinary, err := w.CaptureStableSnapshot(path2)
	if err != nil {
		t.Fatalf("Capture ordinary append: %v", err)
	}
	defer ordinary.Release()
	if ordinary.NamespaceRequired() {
		t.Fatal("established segment retained namespace debt on ordinary append")
	}
}

func TestStableWriterSnapshot_UnsupportedNamespaceFailsClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "value-l0-000001.log"), page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	original := captureStableNamespaceDirectory
	captureStableNamespaceDirectory = func(string) (*os.File, StableFileIdentity, error) {
		return nil, StableFileIdentity{}, ErrStableNamespaceUnsupported
	}
	t.Cleanup(func() { captureStableNamespaceDirectory = original })
	path2 := filepath.Join(dir, "value-l0-000002.log")
	if err := w.RotateToWithSync(path2, page.ValueLogFileID(2), false); err != nil {
		t.Fatalf("RotateToWithSync: %v", err)
	}
	if _, err := w.Append(0, nil, 2, []byte("new")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := w.CaptureStableSnapshot(path2); !errors.Is(err, ErrStableNamespaceUnsupported) {
		t.Fatalf("CaptureStableSnapshot error=%v want %v", err, ErrStableNamespaceUnsupported)
	}
}
