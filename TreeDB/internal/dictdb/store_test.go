package dictdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestStorePutGetDedup(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	id1, err := store.PutDictBytes(ctx, []byte("alpha"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	id2, err := store.PutDictBytes(ctx, []byte("alpha"))
	if err != nil {
		t.Fatalf("put dedup: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("dedup mismatch: %d vs %d", id1, id2)
	}

	bytes, err := store.GetDictBytes(ctx, id1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(bytes) != "alpha" {
		t.Fatalf("get: got %q", string(bytes))
	}

	if err := store.SetCurrent(ctx, id1); err != nil {
		t.Fatalf("set current: %v", err)
	}
	cur, err := store.GetCurrent(ctx)
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if cur != id1 {
		t.Fatalf("current mismatch: got %d want %d", cur, id1)
	}
}

func TestStoreClearCurrent(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	id, err := store.PutDictBytes(ctx, []byte("alpha"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.SetCurrent(ctx, id); err != nil {
		t.Fatalf("set current: %v", err)
	}
	if err := store.SetCurrent(ctx, 0); err != nil {
		t.Fatalf("clear current: %v", err)
	}
	cur, err := store.GetCurrent(ctx)
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if cur != 0 {
		t.Fatalf("expected current=0 got %d", cur)
	}
}

func TestStoreKMeta(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	id, err := store.PutDictBytes(ctx, []byte("alpha"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	k, err := store.GetK(ctx, id)
	if err != nil {
		t.Fatalf("get k: %v", err)
	}
	if k != 0 {
		t.Fatalf("expected missing k=0 got %d", k)
	}
	if err := store.SetK(ctx, id, 4); err != nil {
		t.Fatalf("set k: %v", err)
	}
	k, err = store.GetK(ctx, id)
	if err != nil {
		t.Fatalf("get k: %v", err)
	}
	if k != 4 {
		t.Fatalf("expected k=4 got %d", k)
	}
}

func TestStoreRehydratesHashForExistingBytes(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	id, err := store.PutDictBytes(ctx, []byte("beta"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	sum := sha256.Sum256([]byte("beta"))
	if err := store.backend.DeleteSync(hashKey(sum)); err != nil {
		t.Fatalf("delete hash: %v", err)
	}
	got, err := store.PutDictBytes(ctx, []byte("beta"))
	if err != nil {
		t.Fatalf("put existing bytes: %v", err)
	}
	if got != id {
		t.Fatalf("expected id %d, got %d", id, got)
	}
	val, err := store.backend.Get(hashKey(sum))
	if err != nil {
		t.Fatalf("get hash: %v", err)
	}
	if len(val) != 8 {
		t.Fatalf("expected hash value length 8, got %d", len(val))
	}
	if gotID := binary.BigEndian.Uint64(val); gotID != id {
		t.Fatalf("expected hash id %d, got %d", id, gotID)
	}
}
