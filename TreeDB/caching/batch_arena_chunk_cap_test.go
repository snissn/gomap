package caching

import (
	"bytes"
	"testing"
)

func TestBatchArenaCopy_ChunkCapClamped(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Force enough copied payload so the arena would previously grow beyond
	// batchCopyArenaMaxRetain via exponential chunk doubling (8MB+ tail chunk),
	// even though each individual Set() payload stays under the threshold.
	value := bytes.Repeat([]byte{0xAB}, (1<<19)-1) // 512KiB - 1; +1B key == 512KiB total copy

	b := db.NewBatchWithSize(0)
	defer b.Close()
	for i := 0; i < 16; i++ {
		key := []byte{byte(i)}
		if err := b.Set(key, value); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	maxCap := 0
	for _, chunk := range b.copyArenaChunks {
		if chunk == nil {
			continue
		}
		if c := cap(chunk); c > maxCap {
			maxCap = c
		}
	}
	if maxCap > batchCopyArenaMaxRetain {
		t.Fatalf("copy arena grew beyond pooling limit: maxCap=%d want<=%d", maxCap, batchCopyArenaMaxRetain)
	}
}
