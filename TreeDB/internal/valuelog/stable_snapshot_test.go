package valuelog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
