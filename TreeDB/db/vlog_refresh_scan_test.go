package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func writeValueLogSegment(t *testing.T, dir string, lane, seq uint32) (path string, fileID uint32) {
	t.Helper()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path = filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("x"), 256)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return path, fileID
}

func TestValueLogGC_DoesNotRescanWhenSetAlreadyPopulated(t *testing.T) {
	dir := t.TempDir()
	path1, id1 := writeValueLogSegment(t, dir, 0, 1)
	_, _ = writeValueLogSegment(t, dir, 0, 2)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	if !d.valueLogManager.HasSegment(id1) {
		t.Fatalf("expected segment %d to be registered at open", id1)
	}
	before := d.valueLogManager.RefreshScanCount()

	_, err = d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ValueLogGC triggered value-log refresh scan: before=%d after=%d", before, after)
	}
	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected GC to remove eligible segment %q, err=%v", path1, err)
	}
}

func TestValueLogRewritePlan_DoesNotRescanWhenSetAlreadyPopulated(t *testing.T) {
	dir := t.TempDir()
	_, _ = writeValueLogSegment(t, dir, 0, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	before := d.valueLogManager.RefreshScanCount()

	_, err = d.ValueLogRewritePlan(context.Background(), ValueLogRewriteOnlineOptions{})
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	after := d.valueLogManager.RefreshScanCount()
	if after != before {
		t.Fatalf("ValueLogRewritePlan triggered value-log refresh scan: before=%d after=%d", before, after)
	}
}
