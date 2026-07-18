package treedb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/template"
)

func TestSideStoreChunkSize_DefaultsIndependentOfMainChunkSize(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:       dir,
		ChunkSize: 64 << 20, // intentionally large; should not inflate side stores
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if db.dictdb == nil {
		t.Fatalf("expected dictdb to be open")
	}
	if err := db.dictdb.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("dictdb SetSync: %v", err)
	}
	dictIndex := filepath.Join(db.dictdb.Dir(), "index.db")
	info, err := os.Stat(dictIndex)
	if err != nil {
		t.Fatalf("stat dictdb index.db: %v", err)
	}
	// The durable-root V1 bootstrap needs more than one 64 KiB allocation
	// chunk (user/system roots, COW freelist, dependency manifest, and root
	// record). It must still use the side-store chunk size rather than the
	// intentionally huge main DB chunk size.
	if got := info.Size(); got <= 0 || got%int64(defaultDictChunkSize) != 0 || got >= int64(opts.ChunkSize) {
		t.Fatalf("unexpected dictdb index.db size: got=%d want positive %d-byte multiple below main chunk %d", got, defaultDictChunkSize, opts.ChunkSize)
	}
	if db.templateDB != nil {
		t.Fatalf("expected templatedb to remain disabled")
	}
}

func TestChunkSizeDefaults(t *testing.T) {
	if got := defaultChunkSize; got != 256*1024 {
		t.Fatalf("defaultChunkSize=%d want=%d", got, 256*1024)
	}
	if got := defaultDictChunkSize; got != 64*1024 {
		t.Fatalf("defaultDictChunkSize=%d want=%d", got, 64*1024)
	}
}

func TestSideStoreChunkSize_CustomDictDBChunkSize(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:             dir,
		ChunkSize:       64 << 20,
		DictDBChunkSize: 2 << 20,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if db.dictdb == nil {
		t.Fatalf("expected dictdb to be open")
	}
	if err := db.dictdb.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("dictdb SetSync: %v", err)
	}
	path := filepath.Join(db.dictdb.Dir(), "index.db")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat dictdb index.db: %v", err)
	}
	if got := info.Size(); got != (2 << 20) {
		t.Fatalf("unexpected dictdb index.db size: got=%d want=%d", got, 2<<20)
	}
	if db.templateDB != nil {
		t.Fatalf("expected templatedb to remain disabled")
	}
}

func TestSideStoreChunkSize_TemplateModeRequestIsIgnored(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                 dir,
		ChunkSize:           64 << 20,
		DictDBChunkSize:     2 << 20,
		TemplateDBChunkSize: 3 << 20,
		ValueLog: ValueLogOptions{
			TemplateMode: template.TemplatePrepass,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if db.dictdb == nil {
		t.Fatalf("expected dictdb to be open")
	}
	if err := db.dictdb.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("dictdb SetSync: %v", err)
	}
	dictPath := filepath.Join(db.dictdb.Dir(), "index.db")
	dictInfo, err := os.Stat(dictPath)
	if err != nil {
		t.Fatalf("stat dictdb index.db: %v", err)
	}
	if got := dictInfo.Size(); got != (2 << 20) {
		t.Fatalf("unexpected dictdb index.db size: got=%d want=%d", got, 2<<20)
	}
	if db.templateDB != nil {
		t.Fatalf("expected templatedb to remain disabled")
	}
}
