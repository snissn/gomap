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
	walDir := filepath.Join(dir, "value_vlog")
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

func TestValueLogGC_RefreshesFullInventoryAfterBoundedOpen(t *testing.T) {
	dir := t.TempDir()
	path1, id1 := writeValueLogSegment(t, dir, 7, 1)

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.valueLogManager == nil {
		t.Fatalf("missing value log manager")
	}
	if d.valueLogManager.HasSegment(id1) {
		t.Fatalf("bounded open unexpectedly discovered unreferenced segment %d", id1)
	}
	// Bounded open leaves unreachable on-disk segments unregistered. Full GC
	// must discover them and retain the newest lane segment as the active guard.
	path2, id2 := writeValueLogSegment(t, dir, 7, 2)
	path3, id3 := writeValueLogSegment(t, dir, 7, 3)
	if d.valueLogManager.HasSegment(id2) || d.valueLogManager.HasSegment(id3) {
		t.Fatal("externally created siblings unexpectedly registered before GC")
	}
	before := d.valueLogManager.RefreshScanCount()

	_, err = d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	after := d.valueLogManager.RefreshScanCount()
	// Discovery from an empty registered inventory is followed by the
	// destructive-phase refresh after reachability and publication revalidation.
	if after != before+2 {
		t.Fatalf("ValueLogGC refresh scans=%d, want two from an empty registered inventory", after-before)
	}
	for _, path := range []string{path1, path2} {
		if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
			t.Fatalf("expected GC to remove eligible segment %q, err=%v", path, err)
		}
	}
	if _, err := os.Stat(path3); err != nil {
		t.Fatalf("expected GC to retain active segment %q: %v", path3, err)
	}
	if !d.valueLogManager.HasSegment(id3) {
		t.Fatalf("expected GC refresh to register retained segment %d", id3)
	}

	// Once there is a registered ordinary file, the destructive-phase refresh
	// alone discovers new siblings and preserves the newest lane head.
	path4, id4 := writeValueLogSegment(t, dir, 7, 4)
	if d.valueLogManager.HasSegment(id4) {
		t.Fatal("new sibling unexpectedly registered before GC")
	}
	before = d.valueLogManager.RefreshScanCount()
	if _, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC with a registered ordinary segment: %v", err)
	}
	if scans := d.valueLogManager.RefreshScanCount() - before; scans != 1 {
		t.Fatalf("ValueLogGC refresh scans=%d, want one from a populated registered inventory", scans)
	}
	if _, err := os.Stat(path3); !os.IsNotExist(err) {
		t.Fatalf("expected GC to remove the older sibling %q: %v", path3, err)
	}
	if _, err := os.Stat(path4); err != nil {
		t.Fatalf("expected GC to retain the newest sibling %q: %v", path4, err)
	}
}

func TestValueLogRewritePlan_RefreshesEmptyInventoryAfterBoundedOpen(t *testing.T) {
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
	if after != before+1 {
		t.Fatalf("ValueLogRewritePlan refresh scans=%d want exactly one: before=%d after=%d", after-before, before, after)
	}
}
