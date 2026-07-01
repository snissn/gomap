package treedb

import (
	"errors"
	"testing"
)

func TestPublicGetVersionedReadOnlyBackend(t *testing.T) {
	dir := t.TempDir()
	writer, cleanup, err := OpenBackend(Options{Dir: dir, DisableSideStores: true})
	if err != nil {
		t.Fatalf("OpenBackend writer: %v", err)
	}
	if err := writer.Set([]byte("k"), []byte("v")); err != nil {
		_ = cleanup()
		t.Fatalf("Set: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup writer: %v", err)
	}

	reader, err := Open(Options{Dir: dir, ReadOnly: true, DisableSideStores: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer func() { _ = reader.Close() }()

	entry, found, err := reader.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if !found {
		t.Fatal("GetVersioned found=false, want true")
	}
	if string(entry.Value) != "v" {
		t.Fatalf("GetVersioned value=%q want v", entry.Value)
	}
	if entry.Revision == 0 {
		t.Fatal("GetVersioned revision=0, want durable revision")
	}
}

func TestPublicGetVersionedCachedFailsClosed(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableSideStores: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	if err := database.Set([]byte("k"), []byte("cached")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, _, err := database.GetVersioned([]byte("k")); !errors.Is(err, ErrVersionedEntryCachedUnsupported) {
		t.Fatalf("GetVersioned cached error=%v want ErrVersionedEntryCachedUnsupported", err)
	}
}
