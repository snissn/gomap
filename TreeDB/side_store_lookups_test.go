package treedb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestWireSideStoreLookups_ReadOnlyDoesNotExposeDictWrites(t *testing.T) {
	root := t.TempDir()
	dictDir := filepath.Join(root, "dictdb")
	dictBackend, err := db.Open(db.Options{Dir: dictDir})
	if err != nil {
		t.Fatalf("open dictdb: %v", err)
	}
	store := dictdb.New(dictBackend)
	dictID, err := store.PutDictBytes(context.Background(), []byte("maintenance dictionary"))
	if err != nil {
		t.Fatal(err)
	}
	if err := dictBackend.Close(); err != nil {
		t.Fatalf("close dictdb: %v", err)
	}

	opts := &Options{ReadOnly: true}
	cleanup, provider, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatalf("wireSideStoreLookups: %v", err)
	}
	defer func() { _ = cleanup() }()

	if provider == nil {
		t.Fatal("dictionary owner did not expose stable authority")
	}
	resources, err := provider.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		t.Fatal(err)
	}
	if resources == nil || resources.Len() == 0 {
		t.Fatal("empty dictionary authority")
	}
	resources.Release()
	if opts.ValueLog.DictLookup == nil {
		t.Fatal("expected DictLookup to be wired")
	}
	if opts.ValueLog.DictCurrentForClass == nil {
		t.Fatal("expected DictCurrentForClass to be wired")
	}
	if opts.ValueLog.DictLeafPayloadMode == nil {
		t.Fatal("expected DictLeafPayloadMode to be wired")
	}
	if opts.ValueLog.DictPut != nil {
		t.Fatal("expected DictPut to stay nil for read-only dictdb")
	}
	if opts.ValueLog.DictSetCurrentForClass != nil {
		t.Fatal("expected DictSetCurrentForClass to stay nil for read-only dictdb")
	}
	if opts.ValueLog.DictSetLeafPayloadMode != nil {
		t.Fatal("expected DictSetLeafPayloadMode to stay nil for read-only dictdb")
	}
	if _, ok, err := opts.ValueLog.DictLeafPayloadMode(context.Background(), 0); err != nil || ok {
		t.Fatalf("DictLeafPayloadMode(dict=0) = (_, %v, %v), want (_, false, nil)", ok, err)
	}
}

func TestWireSideStoreLookups_DoesNotPropagateCommandWAL(t *testing.T) {
	root := t.TempDir()
	dictDir := filepath.Join(root, "dictdb")
	dictBackend, err := db.Open(db.Options{Dir: dictDir})
	if err != nil {
		t.Fatalf("open dictdb: %v", err)
	}
	if err := dictBackend.Close(); err != nil {
		t.Fatalf("close dictdb: %v", err)
	}

	opts := &Options{CommandWAL: true}
	cleanup, _, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatalf("wireSideStoreLookups: %v", err)
	}
	defer func() { _ = cleanup() }()

	if !opts.CommandWAL {
		t.Fatal("wireSideStoreLookups cleared main DB CommandWAL option")
	}
	cfg, ok, err := db.LoadFormatConfig(dictDir)
	if err != nil {
		t.Fatalf("LoadFormatConfig(dictdb): %v", err)
	}
	if !ok {
		t.Fatalf("expected dictdb format.json to exist at %s", filepath.Join(dictDir, "format.json"))
	}
	if cfg.RequiresCommandWALV1() {
		t.Fatal("dictdb inherited command_wal_v1 required feature from main DB options")
	}
}

func TestWireSideStoreLookupsLockedReadOnlySnapshotOwners(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"dictdb", "templatedb"} {
		owner, err := db.Open(db.Options{Dir: filepath.Join(root, name)})
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.SetSync([]byte("fixture"), []byte("stable")); err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	opts := &Options{ReadOnly: true}
	cleanup, _, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if opts.PhysicalSnapshotSideStoreCapture == nil {
		t.Fatal("owners did not register capture")
	}
	for _, name := range []string{"dictdb", "templatedb"} {
		cut, err := opts.PhysicalSnapshotSideStoreCapture(context.Background(), name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := cut.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
