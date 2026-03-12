package batch

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchReset_DoesNotRetainOversizedCopyArenaChunk(t *testing.T) {
	b := Acquire(nil, page.DefaultInlineThreshold)
	defer Release(b)

	// Force an oversized one-off chunk that should not be kept across Reset.
	_ = b.arenaAlloc(batchCopyArenaMaxRetain + 1)
	if got := cap(b.copyArena); got <= batchCopyArenaMaxRetain {
		t.Fatalf("setup: expected oversized arena cap=%d > %d", got, batchCopyArenaMaxRetain)
	}

	b.Reset()

	if b.copyArena != nil && cap(b.copyArena) > batchCopyArenaMaxRetain {
		t.Fatalf("Reset retained oversized arena cap=%d > %d", cap(b.copyArena), batchCopyArenaMaxRetain)
	}
	for i := range b.copyArenaChunks {
		if b.copyArenaChunks[i] != nil && cap(b.copyArenaChunks[i]) > batchCopyArenaMaxRetain {
			t.Fatalf("Reset retained oversized arena chunk cap=%d > %d", cap(b.copyArenaChunks[i]), batchCopyArenaMaxRetain)
		}
	}
}

