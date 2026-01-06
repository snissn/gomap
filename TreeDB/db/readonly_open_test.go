package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/wal"
)

func TestReadOnlyRejectsWrites(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetSync([]byte("k"), []byte("v")); err != nil {
		_ = w.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()

	if err := ro.Set([]byte("k2"), []byte("v2")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Set expected ErrReadOnly, got %v", err)
	}
	if err := ro.Delete([]byte("k")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Delete expected ErrReadOnly, got %v", err)
	}
}

func TestReadOnlyDoesNotReplayOrRemoveWAL(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetSync([]byte("base"), []byte("ok")); err != nil {
		_ = w.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	walPath := filepath.Join(walDir, "wal-1.log")

	ww, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("wal.NewWriter: %v", err)
	}
	if err := ww.Append(1, wal.OpSet, []byte("k"), []byte("v")); err != nil {
		_ = ww.Close()
		t.Fatalf("wal.Append: %v", err)
	}
	if err := ww.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected WAL segment to exist: %v", err)
	}

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	val, err := ro.Get([]byte("k"))
	if err != nil {
		_ = ro.Close()
		t.Fatalf("Get: %v", err)
	}
	if val != nil {
		_ = ro.Close()
		t.Fatalf("expected WAL key to be absent in read-only view, got %q", val)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("read-only open should not remove WAL segment: %v", err)
	}

	rw, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	val, err = rw.Get([]byte("k"))
	if err != nil {
		_ = rw.Close()
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("v")) {
		_ = rw.Close()
		t.Fatalf("expected WAL key to be replayed, got %q", val)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(walPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected WAL segment to be removed after write-open replay, got %v", err)
	}
}
