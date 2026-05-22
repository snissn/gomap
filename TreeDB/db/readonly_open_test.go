package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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

func TestReadOnlyOpenDoesNotAcquireCommandJournalOwner(t *testing.T) {
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

	owner, err := commitlog.AcquireJournalOwner(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("AcquireJournalOwner: %v", err)
	}
	defer owner.Close()

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only Open while journal owner held: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}

	noLock, err := openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("openReadOnlyNoLock while journal owner held: %v", err)
	}
	if err := noLock.Close(); err != nil {
		t.Fatalf("Close no-lock read-only: %v", err)
	}
}

func TestReadOnlyOpenConfiguresLeafPageReadCacheEntries(t *testing.T) {
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

	openers := []struct {
		name string
		open func(Options) (*DB, error)
	}{
		{
			name: "shared_lock",
			open: func(opts Options) (*DB, error) {
				opts.ReadOnly = true
				return Open(opts)
			},
		},
		{name: "no_lock", open: openReadOnlyNoLock},
	}
	for _, opener := range openers {
		t.Run(opener.name, func(t *testing.T) {
			ro, err := opener.open(Options{
				Dir:                      dir,
				ReadOnly:                 true,
				ChunkSize:                256 * 1024,
				LeafPageReadCacheEntries: 7,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ro.Close() }()

			stats := ro.Stats()
			if got := stats["treedb.process.read_path.outer_leaf.cache.capacity"]; got != "7" {
				t.Fatalf("outer leaf cache capacity=%q want 7", got)
			}
		})
	}
}

func TestReadOnlyNoLockDefaultsChunkSize(t *testing.T) {
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

	ro, err := openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("openReadOnlyNoLock: %v", err)
	}
	defer ro.Close()
	if ro.chunkSize != defaultChunkSize {
		t.Fatalf("read-only chunkSize=%d want default %d", ro.chunkSize, defaultChunkSize)
	}
}

func TestReadOnlyDoesNotReplayOrRemoveCommitLog(t *testing.T) {
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
	commitPath := filepath.Join(walDir, "commit-l0-000001.log")

	ww, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	if err := ww.AppendBatch([]commitlog.Record{{
		Op:    commitlog.OpSetInline,
		Key:   []byte("k"),
		Value: []byte("v"),
		Seq:   1,
	}}); err != nil {
		_ = ww.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := ww.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	if _, err := os.Stat(commitPath); err != nil {
		t.Fatalf("expected commitlog segment to exist: %v", err)
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
		t.Fatalf("expected commitlog key to be absent in read-only view, got %q", val)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(commitPath); err != nil {
		t.Fatalf("read-only open should not remove commitlog segment: %v", err)
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
		t.Fatalf("expected commitlog key to be replayed, got %q", val)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(commitPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected commitlog segment to be removed after write-open replay, got %v", err)
	}
}
