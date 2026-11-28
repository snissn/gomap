package treedb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestOpenBackend_ReadOnlyResolvesInMainDBDictWithoutWrites(t *testing.T) {
	root := t.TempDir()
	opened, err := Open(Options{Dir: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	store := dictdb.New(opened.backend)
	dictID, err := store.PutDictBytes(context.Background(), []byte("maintenance dictionary"))
	if err != nil {
		_ = opened.Close()
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ro, cleanup, err := OpenBackend(Options{Dir: root, ReadOnly: true})
	if err != nil {
		t.Fatalf("OpenBackend(ReadOnly): %v", err)
	}
	defer func() { _ = cleanup() }()

	got, err := ro.LookupSystemDictBytes(dictID)
	if err != nil || string(got) != "maintenance dictionary" {
		t.Fatalf("read-only dictionary lookup: got=%q err=%v", got, err)
	}
	roStore := dictdb.New(ro)
	if _, err := roStore.PutDictBytes(context.Background(), []byte("blocked write")); err == nil {
		t.Fatal("expected read-only PutDictBytes to fail")
	}
}

func TestWireSideStoreLookups_DoesNotPropagateCommandWALToTemplateDB(t *testing.T) {
	root := t.TempDir()
	templateDir := filepath.Join(root, "templatedb")
	templateBackend, err := db.Open(db.Options{Dir: templateDir})
	if err != nil {
		t.Fatalf("open templatedb: %v", err)
	}
	if err := templateBackend.Close(); err != nil {
		t.Fatalf("close templatedb: %v", err)
	}

	opts := &Options{CommandWAL: true}
	cleanup, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatalf("wireSideStoreLookups: %v", err)
	}
	defer func() { _ = cleanup() }()

	if !opts.CommandWAL {
		t.Fatal("wireSideStoreLookups cleared main DB CommandWAL option")
	}
	cfg, ok, err := db.LoadFormatConfig(templateDir)
	if err != nil {
		t.Fatalf("LoadFormatConfig(templatedb): %v", err)
	}
	if !ok {
		t.Fatalf("expected templatedb format.json to exist at %s", filepath.Join(templateDir, "format.json"))
	}
	if cfg.RequiresCommandWALV1() {
		t.Fatal("templatedb inherited command_wal_v1 required feature from main DB options")
	}
}

func TestWireSideStoreLookupsLockedReadOnlySnapshotOwners(t *testing.T) {
	root := t.TempDir()
	templateDir := filepath.Join(root, "templatedb")
	owner, err := db.Open(db.Options{Dir: templateDir})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	opts := &Options{ReadOnly: true}
	cleanup, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cleanup() }()
	if opts.PhysicalSnapshotSideStoreCapture == nil {
		t.Fatal("expected PhysicalSnapshotSideStoreCapture to be wired for templatedb")
	}
	cut, err := opts.PhysicalSnapshotSideStoreCapture(context.Background(), "templatedb")
	if err != nil {
		t.Fatalf("capture templatedb cut: %v", err)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
}
