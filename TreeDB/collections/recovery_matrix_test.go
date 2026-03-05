package collections

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestReopenVerification_CollectionPrimaryAndSecondary(t *testing.T) {
	dir := t.TempDir()
	collectionName := "users"
	dataPrefix, err := CollectionDataPrefix(collectionName)
	if err != nil {
		t.Fatalf("collection data prefix: %v", err)
	}
	opts := db.Options{
		Dir: dir,
		ValueLog: db.ValueLogOptions{
			DomainInlineThresholds: []db.ValueLogDomainThreshold{
				{Prefix: append(append([]byte{}, dataPrefix...), 0xff), InlineThreshold: 8},
			},
		},
	}

	d, err := db.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: collectionName})
	if err != nil {
		_ = d.Close()
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		_ = d.Close()
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("open collection: %v", err)
	}
	docID := []byte("u1")
	largePayload := bytes.Repeat([]byte("x"), 1024)
	doc := append([]byte(`{"email":"a@example.com","blob":"`), largePayload...)
	doc = append(doc, []byte(`"}`)...)
	if _, err := col.Insert(docID, doc); err != nil {
		_ = d.Close()
		t.Fatalf("insert: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := db.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()
	reopenMgr := NewCollectionManager(reopen)
	reopenCol, err := reopenMgr.OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	got, err := reopenCol.Get(docID)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document mismatch after reopen: got=%d want=%d", len(got), len(doc))
	}
	ids, err := reopenCol.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find by index after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], docID) {
		t.Fatalf("index mismatch after reopen: got=%#v", ids)
	}
}

func TestValueLogGC_IndexedDeleteClearsReachability(t *testing.T) {
	dir := t.TempDir()
	collectionName := "users"
	dataPrefix, err := CollectionDataPrefix(collectionName)
	if err != nil {
		t.Fatalf("collection data prefix: %v", err)
	}
	opts := db.Options{
		Dir: dir,
		ValueLog: db.ValueLogOptions{
			DomainInlineThresholds: []db.ValueLogDomainThreshold{
				{Prefix: append(append([]byte{}, dataPrefix...), 0xff), InlineThreshold: 8},
			},
		},
	}

	d, err := db.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: collectionName})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	beforeSegments, err := countValueLogSegments(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("count segments before: %v", err)
	}
	doc := append([]byte(`{"email":"a@example.com","blob":"`), bytes.Repeat([]byte("y"), 1024)...)
	doc = append(doc, []byte(`"}`)...)
	if _, err := col.Insert([]byte("u1"), doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	stats, err := d.ValueLogGC(context.Background(), db.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	afterSegments, err := countValueLogSegments(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("count segments after: %v", err)
	}
	if stats.BytesReferenced != 0 {
		t.Fatalf("expected no referenced value-log bytes after indexed delete, stats=%+v", stats)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted doc: %v", err)
	}
	if got != nil {
		t.Fatalf("expected deleted doc to remain absent after GC")
	}
	ids, err := col.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find by index after gc: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no index postings after delete+gc, got=%#v", ids)
	}
	if afterSegments < beforeSegments {
		t.Fatalf("segment count cannot increase after delete+gc accounting mismatch: before=%d after=%d", beforeSegments, afterSegments)
	}
}

func countValueLogSegments(walDir string) (int, error) {
	entries, err := os.ReadDir(walDir)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	total := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) > 0 && len(name) >= len("value-l0-000001.log") && name[:6] == "value-" {
			total++
		}
	}
	return total, nil
}
