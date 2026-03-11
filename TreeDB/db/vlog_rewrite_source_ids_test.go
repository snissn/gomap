package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestSelectRewriteSourceSegments_SourceFileIDs_NilLiveBytesTreatsAsFullyLive(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "value-l0-000001.log")
	payload := []byte("0123456789abcdef")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	fileID := uint32(123)
	files := map[uint32]*valuelog.File{
		fileID: {Path: path},
	}

	got := selectRewriteSourceSegmentsRanked(ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{fileID},
	}, files, nil, nil)
	if len(got) != 1 {
		t.Fatalf("selected=%d want=1", len(got))
	}
	if got[0].fileID != fileID {
		t.Fatalf("file_id=%d want=%d", got[0].fileID, fileID)
	}
	if got[0].liveBytes != int64(len(payload)) {
		t.Fatalf("live_bytes=%d want=%d", got[0].liveBytes, len(payload))
	}
	if got[0].staleBytes != 0 {
		t.Fatalf("stale_bytes=%d want=0", got[0].staleBytes)
	}
	if got[0].staleRatio != 0 {
		t.Fatalf("stale_ratio=%v want=0", got[0].staleRatio)
	}
}
