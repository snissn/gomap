package treedb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/template"
)

func TestSideStoreChunkSize_NoDictDBCreated(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:       dir,
		ChunkSize: 64 << 20,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(filepath.Join(dir, "dictdb")); !os.IsNotExist(err) {
		t.Fatalf("expected no dictdb directory, stat err=%v", err)
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

func TestSideStoreChunkSize_TemplateModeRequestIsIgnored(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                 dir,
		ChunkSize:           64 << 20,
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

	if _, err := os.Stat(filepath.Join(dir, "dictdb")); !os.IsNotExist(err) {
		t.Fatalf("expected no dictdb directory, stat err=%v", err)
	}
	if db.templateDB != nil {
		t.Fatalf("expected templatedb to remain disabled")
	}
}
