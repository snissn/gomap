package caching

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCachingDB_CloseFlushesPendingMemtables(t *testing.T) {
	type testCase struct {
		name     string
		opts     Options
		useBatch bool
	}

	cases := []testCase{
		{
			name: "wal_on_set",
			opts: Options{
				FlushThreshold: 1 << 30,
			},
		},
		{
			name: "wal_on_batch",
			opts: Options{
				FlushThreshold: 1 << 30,
			},
			useBatch: true,
		},
		{
			name: "wal_off_set",
			opts: Options{
				FlushThreshold: 1 << 30,
				DisableWAL:     true,
				AllowUnsafe:    true,
			},
		},
		{
			name: "wal_off_batch",
			opts: Options{
				FlushThreshold: 1 << 30,
				DisableWAL:     true,
				AllowUnsafe:    true,
			},
			useBatch: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			backend := NewMockBackend()
			db, err := Open(dir, backend, tc.opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			entries := []struct {
				key []byte
				val []byte
			}{
				{[]byte("alpha"), []byte("one")},
				{[]byte("beta\x00"), []byte("two\x00")},
				{[]byte{0x01, 0x02, 0x03}, []byte{0xff, 0x00, 0x10}},
				{[]byte("utf8-\xf0\x9f\x98\x80"), []byte("val-\xe2\x9c\x93")},
			}

			if tc.useBatch {
				b := db.NewBatch()
				for _, e := range entries {
					if err := b.Set(e.key, e.val); err != nil {
						_ = b.Close()
						_ = db.Close()
						t.Fatalf("Batch.Set: %v", err)
					}
				}
				if err := b.Write(); err != nil {
					_ = b.Close()
					_ = db.Close()
					t.Fatalf("Batch.Write: %v", err)
				}
				if err := b.Close(); err != nil {
					_ = db.Close()
					t.Fatalf("Batch.Close: %v", err)
				}
			} else {
				for _, e := range entries {
					if err := db.Set(e.key, e.val); err != nil {
						_ = db.Close()
						t.Fatalf("Set: %v", err)
					}
				}
			}

			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			backend.mu.RLock()
			defer backend.mu.RUnlock()
			if len(backend.data) != len(entries) {
				t.Fatalf("backend entry count mismatch: %d != %d", len(backend.data), len(entries))
			}
			for _, e := range entries {
				got, ok := backend.data[string(e.key)]
				if !ok {
					t.Fatalf("missing key in backend: %x", e.key)
				}
				if !bytes.Equal(got, e.val) {
					t.Fatalf("value mismatch for key %x: %x != %x", e.key, got, e.val)
				}
			}
		})
	}
}

func TestCachingDB_CloseFlushesPendingMemtables_MultipleShards(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	opts := Options{
		FlushThreshold: 1 << 30,
		MemtableShards: 4,
	}
	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	keys := make([][]byte, 0, 64)
	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		val := []byte(fmt.Sprintf("v%03d", i))
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("Set: %v", err)
		}
		keys = append(keys, key)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()
	if len(backend.data) != len(keys) {
		t.Fatalf("backend entry count mismatch: %d != %d", len(backend.data), len(keys))
	}
	for _, key := range keys {
		if _, ok := backend.data[string(key)]; !ok {
			t.Fatalf("missing key in backend: %q", key)
		}
	}
}

func TestCachingDB_CloseFlushesPendingMemtables_ValuePointers(t *testing.T) {
	type testCase struct {
		name     string
		useBatch bool
	}

	cases := []testCase{
		{name: "set"},
		{name: "batch", useBatch: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 4 << 20})
			if err != nil {
				t.Fatalf("backend.Open: %v", err)
			}

			opts := Options{
				FlushThreshold:           1 << 30,
				ValueLogPointerThreshold: 1,
				AllowUnsafe:              true,
			}
			cache, err := Open(dir, backend, opts)
			if err != nil {
				_ = backend.Close()
				t.Fatalf("Open: %v", err)
			}

			entries := []struct {
				key []byte
				val []byte
			}{
				{[]byte("alpha"), []byte("one")},
				{[]byte("beta\x00"), []byte("two\x00")},
				{[]byte{0x01, 0x02, 0x03}, []byte{0xff, 0x00, 0x10}},
				{[]byte("utf8-\xf0\x9f\x98\x80"), []byte("val-\xe2\x9c\x93")},
			}

			if tc.useBatch {
				b := cache.NewBatch()
				for _, e := range entries {
					if err := b.Set(e.key, e.val); err != nil {
						_ = b.Close()
						_ = cache.Close()
						t.Fatalf("Batch.Set: %v", err)
					}
				}
				if err := b.Write(); err != nil {
					_ = b.Close()
					_ = cache.Close()
					t.Fatalf("Batch.Write: %v", err)
				}
				if err := b.Close(); err != nil {
					_ = cache.Close()
					t.Fatalf("Batch.Close: %v", err)
				}
			} else {
				for _, e := range entries {
					if err := cache.Set(e.key, e.val); err != nil {
						_ = cache.Close()
						t.Fatalf("Set: %v", err)
					}
				}
			}

			if err := cache.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := db.Open(db.Options{Dir: dir, ChunkSize: 4 << 20})
			if err != nil {
				t.Fatalf("backend reopen: %v", err)
			}
			defer reopened.Close()

			for _, e := range entries {
				got, err := reopened.Get(e.key)
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if !bytes.Equal(got, e.val) {
					t.Fatalf("value mismatch for key %x: %x != %x", e.key, got, e.val)
				}
			}
		})
	}
}

func TestCachingDB_CloseFlushesDeletesAndOverwrites(t *testing.T) {
	type testCase struct {
		name string
		opts Options
	}

	cases := []testCase{
		{
			name: "wal_on",
			opts: Options{
				FlushThreshold: 1 << 30,
			},
		},
		{
			name: "wal_off",
			opts: Options{
				FlushThreshold: 1 << 30,
				DisableWAL:     true,
				AllowUnsafe:    true,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			backend := NewMockBackend()
			db, err := Open(dir, backend, tc.opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			if err := db.Set([]byte("keep"), []byte("v1")); err != nil {
				_ = db.Close()
				t.Fatalf("Set keep: %v", err)
			}
			if err := db.Set([]byte("delete"), []byte("gone")); err != nil {
				_ = db.Close()
				t.Fatalf("Set delete: %v", err)
			}
			if err := db.Set([]byte("overwrite"), []byte("old")); err != nil {
				_ = db.Close()
				t.Fatalf("Set overwrite: %v", err)
			}
			if err := db.Delete([]byte("delete")); err != nil {
				_ = db.Close()
				t.Fatalf("Delete: %v", err)
			}
			if err := db.Set([]byte("overwrite"), []byte("new")); err != nil {
				_ = db.Close()
				t.Fatalf("Set overwrite new: %v", err)
			}

			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			backend.mu.RLock()
			defer backend.mu.RUnlock()
			if got := backend.data["keep"]; !bytes.Equal(got, []byte("v1")) {
				t.Fatalf("keep mismatch: %q", got)
			}
			if _, ok := backend.data["delete"]; ok {
				t.Fatalf("expected delete to be absent")
			}
			if got := backend.data["overwrite"]; !bytes.Equal(got, []byte("new")) {
				t.Fatalf("overwrite mismatch: %q", got)
			}
		})
	}
}

func TestCachingDB_CloseFlushesQueuedMemtable(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		_ = db.Close()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()
	if !bytes.Equal(backend.data["k1"], []byte("v1")) {
		t.Fatalf("k1 missing or wrong")
	}
	if !bytes.Equal(backend.data["k2"], []byte("v2")) {
		t.Fatalf("k2 missing or wrong")
	}
}
