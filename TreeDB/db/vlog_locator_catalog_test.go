package db

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestValueLogLocatorCatalog_RebuildAndLoad(t *testing.T) {
	t.Setenv(envEnableVlogLocatorCatalog, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 330_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 330_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogLocatorCatalog(context.Background()); err != nil {
		t.Fatalf("rebuild locator catalog: %v", err)
	}
	keys, ok, err := db.locatorKeysForSegments(context.Background(), []uint32{ptrs1[0].FileID})
	if err != nil {
		t.Fatalf("locator keys: %v", err)
	}
	if !ok || len(keys) != 1 || !bytes.Equal(keys[0], []byte("k1")) {
		t.Fatalf("locator keys=%q ok=%t", keys, ok)
	}

	path := filepath.Join(dir, valueLogLocatorCatalogFileName)
	if _, ok, err := loadValueLogLocatorCatalogFromPath(path, db.currentCommitSeq()); err != nil {
		t.Fatalf("load locator catalog: %v", err)
	} else if !ok {
		t.Fatalf("expected locator catalog to load")
	}
}
