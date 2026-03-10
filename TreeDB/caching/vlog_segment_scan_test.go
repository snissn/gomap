package caching

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaxValueLogRIDFromSegments_IgnoresSegmentRemovedAfterScan(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, valueLogName(0, 1))
	if err := os.WriteFile(path, []byte{0x01}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	segments := []logSegmentInfo{
		{path: path, size: 1, seq: 1, lane: 0, valueLog: true},
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	maxRID, err := maxValueLogRIDFromSegments(segments)
	if err != nil {
		t.Fatalf("maxValueLogRIDFromSegments: %v", err)
	}
	if maxRID != 0 {
		t.Fatalf("maxRID=%d want 0", maxRID)
	}
}
