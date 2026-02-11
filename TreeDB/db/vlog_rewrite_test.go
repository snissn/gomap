package db

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogRewriteOffline_RewritesAndShrinks(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

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
	ptr1a, err := w1.Append(0, nil, 1, bytes.Repeat([]byte{0x01}, 128))
	if err != nil {
		t.Fatalf("append1a: %v", err)
	}
	_, err = w1.Append(0, nil, 2, bytes.Repeat([]byte{0x02}, 128))
	if err != nil {
		t.Fatalf("append1b: %v", err)
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
	ptr2a, err := w2.Append(0, nil, 3, bytes.Repeat([]byte{0x03}, 128))
	if err != nil {
		t.Fatalf("append2a: %v", err)
	}
	_, err = w2.Append(0, nil, 4, bytes.Repeat([]byte{0x04}, 128))
	if err != nil {
		t.Fatalf("append2b: %v", err)
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
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1a); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2a); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.BytesAfter >= stats.BytesBefore {
		t.Fatalf("expected bytes to shrink, before=%d after=%d", stats.BytesBefore, stats.BytesAfter)
	}

	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old segment1 removed, err=%v", err)
	}
	if _, err := os.Stat(path2); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old segment2 removed, err=%v", err)
	}

	db, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if !bytes.Equal(val, bytes.Repeat([]byte{0x01}, 128)) {
		t.Fatalf("k1 mismatch")
	}
	val, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if !bytes.Equal(val, bytes.Repeat([]byte{0x03}, 128)) {
		t.Fatalf("k2 mismatch")
	}
}

func TestValueLogRewrite_HealthMetadata_PreservedAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 90_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}
	db = nil

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	healthPath := valueLogHealthPath(dir)
	beforeReopen, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health before reopen: %v", err)
	}
	if len(beforeReopen) == 0 {
		t.Fatalf("expected health metadata after rewrite")
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close reopen: %v", err)
	}

	afterReopen, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after reopen: %v", err)
	}
	if !reflect.DeepEqual(beforeReopen, afterReopen) {
		t.Fatalf("health metadata changed across reopen: before=%+v after=%+v", beforeReopen, afterReopen)
	}
}
