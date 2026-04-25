package treedb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestWireSideStoreLookups_ReadOnlyDoesNotExposeDictWrites(t *testing.T) {
	root := t.TempDir()
	dictDir := filepath.Join(root, "dictdb")
	dictBackend, err := db.Open(db.Options{Dir: dictDir})
	if err != nil {
		t.Fatalf("open dictdb: %v", err)
	}
	if err := dictBackend.Close(); err != nil {
		t.Fatalf("close dictdb: %v", err)
	}

	opts := &Options{ReadOnly: true}
	cleanup, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatalf("wireSideStoreLookups: %v", err)
	}
	defer func() { _ = cleanup() }()

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
