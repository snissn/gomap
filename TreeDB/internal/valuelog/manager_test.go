package valuelog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func writeTestSegment(t *testing.T, dir string, lane, seq uint32, rid uint64, value []byte) uint32 {
	t.Helper()

	fileID, err := EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Append(0, nil, rid, value); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return fileID
}

func TestManagerCurrentSetNoRefresh(t *testing.T) {
	dir := t.TempDir()
	seg1 := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("a"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	set1 := mgr.CurrentSetNoRefresh()
	if _, ok := set1.Files[seg1]; !ok {
		t.Fatalf("initial CurrentSetNoRefresh missing segment %d", seg1)
	}
	if err := mgr.Release(set1); err != nil {
		t.Fatalf("Release(set1): %v", err)
	}

	seg2 := writeTestSegment(t, dir, 0, 2, 2, bytes.Repeat([]byte("b"), 64))

	// No-refresh snapshots should not discover newly-created segments on disk.
	set2 := mgr.CurrentSetNoRefresh()
	if _, ok := set2.Files[seg2]; ok {
		t.Fatalf("CurrentSetNoRefresh unexpectedly discovered new segment %d", seg2)
	}
	if err := mgr.Release(set2); err != nil {
		t.Fatalf("Release(set2): %v", err)
	}

	// Refreshing snapshots should discover them.
	set3 := mgr.CurrentSet()
	if _, ok := set3.Files[seg2]; !ok {
		t.Fatalf("CurrentSet missing refreshed segment %d", seg2)
	}
	if err := mgr.Release(set3); err != nil {
		t.Fatalf("Release(set3): %v", err)
	}
}
