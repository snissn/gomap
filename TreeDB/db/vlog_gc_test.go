package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogGC_RemovesUnreferencedSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	path1 := filepath.Join(walDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	w1.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptr1, err := w1.Append(0, nil, 1, bytes.Repeat([]byte("value-1|"), 128))
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	path2 := filepath.Join(walDir, "value-l0-000002.log")
	id2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	ptr2, err := w2.Append(0, nil, 2, bytes.Repeat([]byte("value-2|"), 64))
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected at least one segment deleted, got %+v", stats)
	}

	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected segment1 to be removed, err=%v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected segment2 to remain, err=%v", err)
	}
}
