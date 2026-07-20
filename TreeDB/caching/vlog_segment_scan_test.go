package caching

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestMaxValueLogRIDFromSegmentsScansAllSegments(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	lane0Older := writeRIDValueLogSegment(t, dir, 0, 1, 200)
	lane0Tail := writeRIDValueLogSegment(t, dir, 0, 2, 22)
	lane1Tail := writeRIDValueLogSegment(t, dir, 1, 1, 33)

	segments := []logSegmentInfo{
		{path: lane0Older, size: fileSize(t, lane0Older), seq: 1, lane: 0, valueLog: true},
		{path: lane0Tail, size: fileSize(t, lane0Tail), seq: 2, lane: 0, valueLog: true},
		{path: lane1Tail, size: fileSize(t, lane1Tail), seq: 1, lane: 1, valueLog: true},
		{path: filepath.Join(dir, commitLogName(9, 9)), size: 128, seq: 9, lane: 9, valueLog: false},
	}

	maxRID, err := maxValueLogRIDFromSegments(segments)
	if err != nil {
		t.Fatalf("maxValueLogRIDFromSegments: %v", err)
	}
	if maxRID != 200 {
		t.Fatalf("maxRID=%d want 200 from older exact-RID segment", maxRID)
	}
}

func TestMaxValueLogRIDFromSegmentsScansAllLeafLogSegments(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	leafSeq1 := writeRIDValueLogSegment(t, dir, leafLogLaneID, 1, 99)
	leafSeq2 := writeRIDValueLogSegment(t, dir, leafLogLaneID, 2, 11)
	lane0Tail := writeRIDValueLogSegment(t, dir, 0, 1, 22)

	segments := []logSegmentInfo{
		{path: leafSeq1, size: fileSize(t, leafSeq1), seq: 1, lane: leafLogLaneID, valueLog: true},
		{path: leafSeq2, size: fileSize(t, leafSeq2), seq: 2, lane: leafLogLaneID, valueLog: true},
		{path: lane0Tail, size: fileSize(t, lane0Tail), seq: 1, lane: 0, valueLog: true},
	}

	maxRID, err := maxValueLogRIDFromSegments(segments)
	if err != nil {
		t.Fatalf("maxValueLogRIDFromSegments: %v", err)
	}
	if maxRID != 99 {
		t.Fatalf("maxRID=%d want 99 from older leaf-log physical writer", maxRID)
	}
}

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

func writeRIDValueLogSegment(t testing.TB, dir string, lane, seq int, rid uint64) string {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, valueLogName(lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, rid, []byte("value")); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

func fileSize(t testing.TB, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return st.Size()
}
