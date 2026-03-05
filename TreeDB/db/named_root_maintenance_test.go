package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestReferencedValueLogSegments_IncludeNamedRootLeafRefs(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o755); err != nil {
		_ = db.Close()
		t.Fatalf("mkdir wal: %v", err)
	}
	leafLog := newRewriteWriter(filepath.Join(dir, "wal"), 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)
	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	manager := collections.NewCollectionManager(db)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	for i := 0; i < 64; i++ {
		docID := []byte(fmt.Sprintf("u%03d", i))
		doc := []byte(fmt.Sprintf(`{"email":"u%03d@example.com"}`, i))
		if _, err := coll.Insert(docID, doc); err != nil {
			t.Fatalf("insert %q: %v", docID, err)
		}
	}

	rootDesc := loadNamedRootDescriptorForTest(t, db, meta.PrimaryRoot)
	leafPtr, ok := page.DecodeLeafRef(rootDesc.RootPageID)
	if !ok {
		t.Fatalf("expected named primary root to be a leafref, got root=%d", rootDesc.RootPageID)
	}

	referenced, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if _, ok := referenced[leafPtr.FileID]; !ok {
		t.Fatalf("expected named-root leafref file %d to remain referenced", leafPtr.FileID)
	}
}

func TestValueLogGC_KeepsNamedRootPointerSegments(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
		WALMaxSegmentBytes:     2048,
		ValueLog: ValueLogOptions{
			Compression:   ValueLogCompressionOff,
			ReadIntegrity: IntegritySkipChecksums,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	manager := collections.NewCollectionManager(db)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	docID := []byte("u1")
	doc := append([]byte(`{"email":"a@example.com","blob":"`), bytes.Repeat([]byte("x"), 4096)...)
	doc = append(doc, []byte(`"}`)...)
	if _, err := coll.Insert(docID, doc); err != nil {
		t.Fatalf("insert collection doc: %v", err)
	}

	rootDesc := loadNamedRootDescriptorForTest(t, db, meta.PrimaryRoot)
	oldPtr := requireRootPointerForKey(t, db, rootDesc.RootPageID, docID)
	targetPath := db.valueLogManager.SegmentPath(oldPtr.FileID)

	if err := db.Set([]byte("rotate"), bytes.Repeat([]byte("z"), 4096)); err != nil {
		t.Fatalf("rotate segment: %v", err)
	}

	counts, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if counts[oldPtr.FileID] == 0 {
		t.Fatalf("expected named-root pointer file %d in full scan counts", oldPtr.FileID)
	}
	db.valueLogRefTracker.invalidate()

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if _, err := os.Stat(targetPath); err != nil {
		t.Fatalf("expected named-root pointer segment to remain, err=%v", err)
	}

	got, err := coll.Get(docID)
	if err != nil {
		t.Fatalf("get after GC: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document mismatch after GC: got=%d want=%d", len(got), len(doc))
	}
}

func TestValueLogRewriteOnline_RewritesCollectionNamedRootPointers(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			Compression:   ValueLogCompressionOff,
			ReadIntegrity: IntegritySkipChecksums,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	manager := collections.NewCollectionManager(db)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	docID := []byte("u1")
	doc := append([]byte(`{"email":"a@example.com","blob":"`), bytes.Repeat([]byte("x"), 1024)...)
	doc = append(doc, []byte(`"}`)...)
	if _, err := coll.Insert(docID, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rootDesc := loadNamedRootDescriptorForTest(t, db, meta.PrimaryRoot)
	oldPtr := requireRootPointerForKey(t, db, rootDesc.RootPageID, docID)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     8,
		SyncEachBatch: true,
		SourceFileIDs: []uint32{oldPtr.FileID},
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected named-root rewrite to copy at least one record")
	}

	got, err := coll.Get(docID)
	if err != nil {
		t.Fatalf("get after rewrite: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document mismatch after rewrite: got=%d want=%d", len(got), len(doc))
	}

	refreshed := loadNamedRootDescriptorForTest(t, db, meta.PrimaryRoot)
	newPtr := requireRootPointerForKey(t, db, refreshed.RootPageID, docID)
	if newPtr.FileID == oldPtr.FileID && newPtr.Offset == oldPtr.Offset {
		t.Fatalf("expected rewritten pointer to move off source record %+v", oldPtr)
	}
}

func TestValueLogRewriteOffline_RebuildsCollectionNamedRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			Compression:   ValueLogCompressionOff,
			ReadIntegrity: IntegritySkipChecksums,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	manager := collections.NewCollectionManager(db)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		_ = db.Close()
		t.Fatalf("create collection: %v", err)
	}
	if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		_ = db.Close()
		t.Fatalf("create index: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		_ = db.Close()
		t.Fatalf("open collection: %v", err)
	}
	docID := []byte("u1")
	doc := append([]byte(`{"email":"a@example.com","blob":"`), bytes.Repeat([]byte("y"), 2048)...)
	doc = append(doc, []byte(`"}`)...)
	if _, err := coll.Insert(docID, doc); err != nil {
		_ = db.Close()
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}

	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected offline rewrite to copy at least one named-root record")
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	reopenManager := collections.NewCollectionManager(reopen)
	reopenColl, err := reopenManager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection after rewrite: %v", err)
	}
	got, err := reopenColl.Get(docID)
	if err != nil {
		t.Fatalf("get after offline rewrite: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document mismatch after offline rewrite: got=%d want=%d", len(got), len(doc))
	}
	ids, err := reopenColl.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find by index after offline rewrite: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], docID) {
		t.Fatalf("index mismatch after offline rewrite: got=%#v", ids)
	}
}

func TestVacuumIndexOffline_PreservesCollectionNamedRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	manager := collections.NewCollectionManager(db)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		_ = db.Close()
		t.Fatalf("create collection: %v", err)
	}
	if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		_ = db.Close()
		t.Fatalf("create index: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		_ = db.Close()
		t.Fatalf("open collection: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"a@example.com","name":"Ada"}`)
	if _, err := coll.Insert(docID, doc); err != nil {
		_ = db.Close()
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before vacuum: %v", err)
	}

	if err := VacuumIndexOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("VacuumIndexOffline: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	reopenManager := collections.NewCollectionManager(reopen)
	reopenColl, err := reopenManager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection after vacuum: %v", err)
	}
	got, err := reopenColl.Get(docID)
	if err != nil {
		t.Fatalf("get after vacuum: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document mismatch after vacuum: got=%q want=%q", got, doc)
	}
	ids, err := reopenColl.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find by index after vacuum: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], docID) {
		t.Fatalf("index mismatch after vacuum: got=%#v", ids)
	}
}

func TestVacuumIndexOnline_PreservesCollectionNamedRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	manager := collections.NewCollectionManager(db)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"a@example.com","name":"Ada"}`)
	if _, err := coll.Insert(docID, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("VacuumIndexOnline: %v", err)
	}

	got, err := coll.Get(docID)
	if err != nil {
		t.Fatalf("get after online vacuum: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document mismatch after online vacuum: got=%q want=%q", got, doc)
	}
	ids, err := coll.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find by index after online vacuum: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], docID) {
		t.Fatalf("index mismatch after online vacuum: got=%#v", ids)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close after online vacuum: %v", err)
	}
	db = nil

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	reopenManager := collections.NewCollectionManager(reopen)
	reopenColl, err := reopenManager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection after online vacuum reopen: %v", err)
	}
	reopenIDs, err := reopenColl.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find by index after online vacuum reopen: %v", err)
	}
	if len(reopenIDs) != 1 || !bytes.Equal(reopenIDs[0], docID) {
		t.Fatalf("reopen index mismatch after online vacuum: got=%#v", reopenIDs)
	}
}

func loadNamedRootDescriptorForTest(t *testing.T, db *DB, rootName string) collections.CollectionRootDescriptor {
	t.Helper()
	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	raw, err := db.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("GetSystem root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("missing root descriptor for %q", rootName)
	}
	var desc collections.CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	return desc
}

func requireRootPointerForKey(t *testing.T, db *DB, rootID uint64, key []byte) page.ValuePtr {
	t.Helper()
	end := append(append([]byte{}, key...), 0xff)
	it, err := db.IteratorAtRootWithOptions(rootID, key, end, IteratorOptions{Mode: IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorAtRootWithOptions: %v", err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		entryKey := it.UnsafeKey()
		_, ptr, flags := it.UnsafeEntry()
		if !bytes.Equal(entryKey, key) {
			continue
		}
		if flags&node.FlagPointer == 0 {
			t.Fatalf("expected pointer entry for %q", key)
		}
		return ptr
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	t.Fatalf("missing root key %q", key)
	return page.ValuePtr{}
}
