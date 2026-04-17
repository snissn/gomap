package dictdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
	if err := store.SetK(ctx, id, valuelog.MaxFrameK); err != nil {
		t.Fatalf("set k: %v", err)
	}
	k, err = store.GetK(ctx, id)
	if err != nil {
		t.Fatalf("get k: %v", err)
	}
	if k != valuelog.MaxFrameK {
		t.Fatalf("expected k=%d got %d", valuelog.MaxFrameK, k)
	}
}

func TestStoreCurrentForClass_FallbackToGlobal(t *testing.T) {
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
	got, err := store.GetCurrentForClass(ctx, "outer_leaf")
	if err != nil {
		t.Fatalf("get current for class: %v", err)
	}
	if got != id {
		t.Fatalf("class fallback current mismatch: got %d want %d", got, id)
	}
}

func TestStoreCurrentForClass_SetGetClear(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	idGlobal, err := store.PutDictBytes(ctx, []byte("global"))
	if err != nil {
		t.Fatalf("put global: %v", err)
	}
	idOuter, err := store.PutDictBytes(ctx, []byte("outer"))
	if err != nil {
		t.Fatalf("put outer: %v", err)
	}
	if err := store.SetCurrent(ctx, idGlobal); err != nil {
		t.Fatalf("set global current: %v", err)
	}
	if err := store.SetCurrentForClass(ctx, "outer_leaf", idOuter); err != nil {
		t.Fatalf("set class current: %v", err)
	}
	gotOuter, err := store.GetCurrentForClass(ctx, "outer_leaf")
	if err != nil {
		t.Fatalf("get class current: %v", err)
	}
	if gotOuter != idOuter {
		t.Fatalf("class current mismatch: got %d want %d", gotOuter, idOuter)
	}
	gotSingle, err := store.GetCurrentForClass(ctx, "single_value")
	if err != nil {
		t.Fatalf("get single class current: %v", err)
	}
	if gotSingle != idGlobal {
		t.Fatalf("single class fallback mismatch: got %d want %d", gotSingle, idGlobal)
	}
	if err := store.SetCurrentForClass(ctx, "outer_leaf", 0); err != nil {
		t.Fatalf("clear class current: %v", err)
	}
	gotFallback, err := store.GetCurrentForClass(ctx, "outer_leaf")
	if err != nil {
		t.Fatalf("get class current after clear: %v", err)
	}
	if gotFallback != idGlobal {
		t.Fatalf("class clear fallback mismatch: got %d want %d", gotFallback, idGlobal)
	}
}

func TestStoreLeafPayloadMode_SetGet(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	id, err := store.PutDictBytes(ctx, []byte("outer-compact"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if useRaw, ok, err := store.GetLeafPayloadMode(ctx, id); err != nil || ok || useRaw {
		t.Fatalf("missing mode = (%v, %v, %v), want (false, false, nil)", useRaw, ok, err)
	}
	if err := store.SetLeafPayloadMode(ctx, id, false); err != nil {
		t.Fatalf("set compact mode: %v", err)
	}
	if useRaw, ok, err := store.GetLeafPayloadMode(ctx, id); err != nil || !ok || useRaw {
		t.Fatalf("compact mode = (%v, %v, %v), want (false, true, nil)", useRaw, ok, err)
	}
	if err := store.SetLeafPayloadMode(ctx, id, true); err != nil {
		t.Fatalf("set raw mode: %v", err)
	}
	if useRaw, ok, err := store.GetLeafPayloadMode(ctx, id); err != nil || !ok || !useRaw {
		t.Fatalf("raw mode = (%v, %v, %v), want (true, true, nil)", useRaw, ok, err)
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

func TestStorePutGet_PointerPath_Reopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if store != nil {
			_ = store.Close()
		}
	})

	ctx := context.Background()
	inline := store.backend.InlineThreshold()
	if inline <= 0 {
		inline = 64
	}
	payload := bytes.Repeat([]byte("compressible-"), (inline/len("compressible-")+1)*8)
	if len(payload) <= inline {
		t.Fatalf("expected payload > inline threshold (%d), got %d", inline, len(payload))
	}

	id, err := store.PutDictBytes(ctx, payload)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := store.GetDictBytes(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("get mismatch (len=%d)", len(got))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	store = nil

	reopen, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got2, err := reopen.GetDictBytes(ctx, id)
	if err != nil {
		t.Fatalf("get reopen: %v", err)
	}
	if !bytes.Equal(got2, payload) {
		t.Fatalf("get reopen mismatch (len=%d)", len(got2))
	}
}
