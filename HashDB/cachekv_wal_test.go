package hashdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type memKV struct {
	m map[string][]byte
}

func (m *memKV) Get(key []byte) ([]byte, error) {
	v, ok := m.m[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (m *memKV) Put(key, value []byte) error {
	m.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (m *memKV) Delete(key []byte) error {
	delete(m.m, string(key))
	return nil
}

type errKV struct {
	err error
}

func (e *errKV) Get([]byte) ([]byte, error) { return nil, nil }
func (e *errKV) Put([]byte, []byte) error   { return e.err }
func (e *errKV) Delete([]byte) error        { return e.err }

func TestCacheWALRecoveryWithoutFlush(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "cache.wal")

	backend := &memKV{m: make(map[string][]byte)}
	cache, err := NewCacheKVWithWAL(backend, 1000, 1<<20, 0, walPath, CacheWALOptions{FsyncPolicy: CacheWALFsyncAlways})
	if err != nil {
		t.Fatal(err)
	}

	if err := cache.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash: the cache is not flushed, but the WAL is present on disk.
	cache.walMu.Lock()
	_ = cache.wal.Close()
	cache.walMu.Unlock()

	if _, ok := backend.m["k"]; ok {
		t.Fatalf("backend should not have been flushed")
	}

	reopened, err := NewCacheKVWithWAL(backend, 1000, 1<<20, 0, walPath, CacheWALOptions{FsyncPolicy: CacheWALFsyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got, err := reopened.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v" {
		t.Fatalf("want %q, got %q", "v", string(got))
	}
}

func TestCacheWALFlushCompacts(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "cache.wal")

	backend := &memKV{m: make(map[string][]byte)}
	cache, err := NewCacheKVWithWAL(backend, 1000, 1<<20, 0, walPath, CacheWALOptions{FsyncPolicy: CacheWALFsyncOnSync})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	if err := cache.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := cache.Flush(); err != nil {
		t.Fatal(err)
	}

	if string(backend.m["a"]) != "1" || string(backend.m["b"]) != "2" {
		t.Fatalf("backend missing flushed keys")
	}

	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("expected compacted wal size 0, got %d", fi.Size())
	}
}

func TestCacheWALFlushErrorRestoresPending(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "cache.wal")

	backend := &errKV{err: errors.New("backend failure")}
	cache, err := NewCacheKVWithWAL(backend, 1000, 1<<20, 0, walPath, CacheWALOptions{FsyncPolicy: CacheWALFsyncOnSync})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	if err := cache.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := cache.Flush(); err == nil {
		t.Fatalf("expected flush error")
	}

	got, err := cache.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v" {
		t.Fatalf("want %q, got %q", "v", string(got))
	}

	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() == 0 {
		t.Fatalf("expected wal to retain records on flush failure")
	}
}
