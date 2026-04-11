package db

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
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

func TestValueLogRewriteOnline_SourceFileIDs_UsesLocatorCatalogWhenEnabled(t *testing.T) {
	t.Setenv(envEnableVlogLocatorCatalog, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 340_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 340_100, 1, func(i int) []byte {
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

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs1[0].FileID},
		BatchSize:     8,
	})
	if err != nil {
		t.Fatalf("rewrite online: %v", err)
	}
	if stats.CandidateScanMode != "locator_catalog" {
		t.Fatalf("candidate scan mode=%q want locator_catalog", stats.CandidateScanMode)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("records copied=%d want 1", stats.RecordsCopied)
	}

	ptrK1, flagsK1 := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, flagsK2 := readProjectedPointerByKey(t, db, []byte("k2"))
	if flagsK1&node.FlagPointer == 0 || flagsK2&node.FlagPointer == 0 {
		t.Fatalf("expected pointer flags for rewritten keys: k1=%#x k2=%#x", flagsK1, flagsK2)
	}
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off source segment %d", ptrs1[0].FileID)
	}
	if ptrK2.FileID != ptrs2[0].FileID {
		t.Fatalf("expected k2 pointer to remain on non-selected segment %d, got %d", ptrs2[0].FileID, ptrK2.FileID)
	}
}

func TestValueLogLocatorCatalog_AppliesCommitDeltaAcrossWrites(t *testing.T) {
	t.Setenv(envEnableVlogLocatorCatalog, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 350_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 350_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs1[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogLocatorCatalog(context.Background()); err != nil {
		t.Fatalf("rebuild locator catalog: %v", err)
	}
	baseSeq := db.currentCommitSeq()
	if !db.valueLogLocatorCatalog.canTrack(baseSeq) {
		t.Fatalf("expected locator catalog to track commit seq %d", baseSeq)
	}

	b = db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs2[0]); err != nil {
		t.Fatalf("move k1: %v", err)
	}
	if err := b.Delete([]byte("k2")); err != nil {
		t.Fatalf("delete k2: %v", err)
	}
	if err := b.SetPointer([]byte("k3"), ptrs1[0]); err != nil {
		t.Fatalf("set k3: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("delta write: %v", err)
	}
	closeNoErr(t, b)

	seq := db.currentCommitSeq()
	if !db.valueLogLocatorCatalog.canTrack(seq) {
		t.Fatalf("expected locator catalog to stay valid at commit seq %d", seq)
	}

	keys1, ok := db.valueLogLocatorCatalog.keysForSegments(seq, []uint32{ptrs1[0].FileID})
	if !ok {
		t.Fatalf("expected locator catalog keys for segment %d", ptrs1[0].FileID)
	}
	got1 := make([]string, 0, len(keys1))
	for _, key := range keys1 {
		got1 = append(got1, string(key))
	}
	slices.Sort(got1)
	if !slices.Equal(got1, []string{"k3"}) {
		t.Fatalf("segment %d keys=%v want [k3]", ptrs1[0].FileID, got1)
	}

	keys2, ok := db.valueLogLocatorCatalog.keysForSegments(seq, []uint32{ptrs2[0].FileID})
	if !ok {
		t.Fatalf("expected locator catalog keys for segment %d", ptrs2[0].FileID)
	}
	got2 := make([]string, 0, len(keys2))
	for _, key := range keys2 {
		got2 = append(got2, string(key))
	}
	slices.Sort(got2)
	if !slices.Equal(got2, []string{"k1"}) {
		t.Fatalf("segment %d keys=%v want [k1]", ptrs2[0].FileID, got2)
	}
}

func TestValueLogLocatorCatalog_TracksRewriteSwapCommits(t *testing.T) {
	t.Setenv(envEnableVlogLocatorCatalog, "1")
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 360_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.rebuildValueLogLocatorCatalog(context.Background()); err != nil {
		t.Fatalf("rebuild locator catalog: %v", err)
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs1[0].FileID},
		BatchSize:     8,
	})
	if err != nil {
		t.Fatalf("rewrite online: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("records copied=%d want 1", stats.RecordsCopied)
	}

	ptrK1, flags := readProjectedPointerByKey(t, db, []byte("k1"))
	if flags&node.FlagPointer == 0 {
		t.Fatalf("expected k1 pointer after rewrite, flags=%#x", flags)
	}
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off source segment %d", ptrs1[0].FileID)
	}

	seq := db.currentCommitSeq()
	if !db.valueLogLocatorCatalog.canTrack(seq) {
		t.Fatalf("expected locator catalog to stay valid at commit seq %d", seq)
	}

	oldKeys, ok := db.valueLogLocatorCatalog.keysForSegments(seq, []uint32{ptrs1[0].FileID})
	if !ok {
		t.Fatalf("expected locator catalog query to succeed for old segment")
	}
	if len(oldKeys) != 0 {
		t.Fatalf("old segment keys=%q want empty", oldKeys)
	}

	newKeys, ok := db.valueLogLocatorCatalog.keysForSegments(seq, []uint32{ptrK1.FileID})
	if !ok {
		t.Fatalf("expected locator catalog query to succeed for new segment")
	}
	if len(newKeys) != 1 || string(newKeys[0]) != "k1" {
		t.Fatalf("new segment keys=%q want [k1]", newKeys)
	}
}

func TestValueLogLocatorCatalog_TracksLeafRefRewriteCommits(t *testing.T) {
	t.Setenv(envEnableVlogLocatorCatalog, "1")
	db, sourceIDs, cleanup := setupLeafRefRewriteBench(t, 768)
	defer cleanup()

	if err := db.rebuildValueLogLocatorCatalog(context.Background()); err != nil {
		t.Fatalf("rebuild locator catalog: %v", err)
	}
	if !db.valueLogLocatorCatalog.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected locator catalog to become trackable before leafref rewrite")
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs:     sourceIDs,
		MaxSourceSegments: len(sourceIDs),
		BatchSize:         64,
		SyncEachBatch:     true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.LeafRefRecordsCopied == 0 {
		t.Fatalf("expected leafref rewrite to copy leaf pages, stats=%+v", stats)
	}
	if !db.valueLogLocatorCatalog.canTrack(db.currentCommitSeq()) {
		t.Fatalf("expected locator catalog to stay trackable after leafref rewrite")
	}
}
