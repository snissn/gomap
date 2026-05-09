package treedb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCompactStorageCachedAdvancesWritersPastBackendSegments(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true
	opts.JournalLanes = 1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	valueLogDir := backenddb.ValueLogDirPath(dir)
	seededPath := filepath.Join(valueLogDir, "value-l0-000001.log")
	writeTestValueLogSegment(t, seededPath, 0, 1, []byte("backend-created-segment"))
	seededSize := testFileSize(t, seededPath)

	if _, err := db.CompactStorage(context.Background(), CompactStorageOptions{
		ValueLogProtectedPaths: []string{seededPath},
	}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}

	if err := db.SetSync([]byte("after-compact"), bytes.Repeat([]byte("v"), 512)); err != nil {
		t.Fatalf("SetSync after CompactStorage: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after CompactStorage write: %v", err)
	}

	if got := testFileSize(t, seededPath); got != seededSize {
		t.Fatalf("backend-created segment was reused: size=%d want %d", got, seededSize)
	}
	nextPath := filepath.Join(valueLogDir, "value-l0-000002.log")
	if got := testFileSize(t, nextPath); got == 0 {
		t.Fatalf("next cached value-log segment was not written: %s", nextPath)
	}
}

func writeTestValueLogSegment(t *testing.T, path string, lane, seq uint32, value []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir value-log dir: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, value); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func testFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}
