package caching

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCachingDB_V1LeafLog_OverlapRewritePreservesFallbackOnlyKeys(t *testing.T) {
	dir := t.TempDir()
	backendOpts := backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLog,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold:          1,
			ForcePointers:             true,
			OuterLeafBlockCodec:       backenddb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 1 << 20,
		},
	}
	backend, err := backenddb.Open(backendOpts)
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                       true,
		DisableWAL:                        true,
		FlushThreshold:                    1 << 30,
		MemtableShards:                    1,
		JournalLanes:                      1,
		ForceValueLogPointers:             true,
		ValueLogPointerThreshold:          1,
		IndexOuterLeafMode:                backenddb.IndexOuterLeafModeV1LeafLog,
		ValueLogOuterLeafBlockCodec:       uint8(backenddb.ValueLogBlockLZ4),
		ValueLogOuterLeafBlockTargetBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	keyFor := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
	valueFor := func(i int) []byte { return bytes.Repeat([]byte{byte(i + 1)}, 256) }

	const totalKeys = 64
	for i := 0; i < totalKeys; i++ {
		if err := cache.Set(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(initial): %v", err)
	}

	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	if _, err := snap.GetEntryExact(keyFor(1)); !errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		t.Fatalf("expected fallback-only key before overlap rewrite, err=%v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close before overlap rewrite: %v", err)
	}

	overwrite := bytes.Repeat([]byte("x"), 320)
	inserted := bytes.Repeat([]byte("i"), 320)
	appended := bytes.Repeat([]byte("z"), 320)

	if err := cache.Set(keyFor(1), overwrite); err != nil {
		t.Fatalf("Set(overwrite existing fallback key): %v", err)
	}
	if err := cache.Set([]byte("k0015x"), inserted); err != nil {
		t.Fatalf("Set(insert in covered range): %v", err)
	}
	if err := cache.Set([]byte("k9999"), appended); err != nil {
		t.Fatalf("Set(append key): %v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(overlap rewrite): %v", err)
	}

	checkGet := func(key, want []byte) {
		t.Helper()
		got, err := backend.Get(key)
		if err != nil {
			t.Fatalf("backend Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("backend value mismatch for %q got_len=%d want_len=%d", key, len(got), len(want))
		}
	}

	checkGet(keyFor(0), valueFor(0))
	checkGet(keyFor(1), overwrite)
	checkGet(keyFor(totalKeys-1), valueFor(totalKeys-1))
	checkGet([]byte("k0015x"), inserted)
	checkGet([]byte("k9999"), appended)

	it, err := backend.IteratorWithOptions(nil, nil, backenddb.IteratorOptions{
		Mode:              backenddb.IteratorModePointerProjection,
		IncludeTombstones: true,
	})
	if err != nil {
		t.Fatalf("IteratorWithOptions(pointer_projection): %v", err)
	}
	defer it.Close()
	for it.Valid() {
		k := it.UnsafeKey()
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			if !page.ValuePtrIsFenceOuter(ptr) {
				t.Fatalf("expected fence-anchor pointer only in v1_leaflog overlap rewrite, key=%q ptr=%+v", k, ptr)
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("pointer-projection iterator error: %v", err)
	}

	snap = backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil after overlap rewrite")
	}
	if _, err := snap.GetEntryExact(keyFor(1)); !errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		t.Fatalf("expected no exact fanout for rewritten key, err=%v", err)
	}
	if _, err := snap.GetEntryExact([]byte("k0015x")); !errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		t.Fatalf("expected inserted key to be served from rewritten outer block, err=%v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close after overlap rewrite: %v", err)
	}
}
