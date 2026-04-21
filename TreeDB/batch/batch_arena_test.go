package batch

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchSetCopiesKeyAndValue(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	key := []byte("k1")
	val := []byte("v1")
	if err := b.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Mutate inputs after Set to ensure the batch stored copies.
	key[0] = 'K'
	val[0] = 'V'

	if got := b.entries; len(got) != 1 {
		t.Fatalf("entries len=%d want=1", len(got))
	}
	if got := b.entries[0].Key; !bytes.Equal(got, []byte("k1")) {
		t.Fatalf("key=%q want=%q", got, "k1")
	}
	if got := b.entries[0].Value; !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("value=%q want=%q", got, "v1")
	}
}

func TestBatchSet_AllocFreeAfterWarm(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	key := []byte("key")
	val := []byte("value")
	if err := b.Set(key, val); err != nil {
		t.Fatalf("warm Set: %v", err)
	}
	b.Reset()

	allocs := testing.AllocsPerRun(1000, func() {
		if err := b.Set(key, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
		b.Reset()
	})
	if allocs != 0 {
		t.Fatalf("allocs/run=%f want=0", allocs)
	}
}

func TestBatchResetArenaLocked_KeepsLargestReusableChunk(t *testing.T) {
	b := &Batch{
		arenaChunks: [][]byte{
			make([]byte, 8, batchArenaDefaultChunkCap),
			make([]byte, 8, batchArenaDefaultChunkCap*2),
			make([]byte, 8, batchArenaDefaultChunkCap*4),
		},
	}

	b.resetArenaLocked()

	if got := len(b.arenaChunks); got != 1 {
		t.Fatalf("arena chunk count=%d want=1", got)
	}
	if got, want := cap(b.arenaChunks[0]), batchArenaDefaultChunkCap*4; got != want {
		t.Fatalf("retained chunk cap=%d want=%d", got, want)
	}
	if got := len(b.arenaChunks[0]); got != 0 {
		t.Fatalf("retained chunk len=%d want=0", got)
	}
}

func TestBatchResetArenaLocked_DropsOversizedOnlyChunks(t *testing.T) {
	b := &Batch{
		arenaChunks: [][]byte{
			make([]byte, 8, batchArenaMaxRetainCap+1),
			make([]byte, 8, batchArenaMaxRetainCap*2),
		},
	}

	b.resetArenaLocked()

	if len(b.arenaChunks) != 0 {
		t.Fatalf("expected oversized chunks to be dropped; got=%d", len(b.arenaChunks))
	}
}

func TestBatchEnsureArenaChunk_GeometricGrowth(t *testing.T) {
	b := &Batch{}

	b.ensureArenaChunk(1)
	if got, want := len(b.arenaChunks), 1; got != want {
		t.Fatalf("chunk count=%d want=%d", got, want)
	}
	firstCap := cap(b.arenaChunks[0])
	if firstCap != batchArenaDefaultChunkCap {
		t.Fatalf("first chunk cap=%d want=%d", firstCap, batchArenaDefaultChunkCap)
	}

	// Exhaust each chunk so ensureArenaChunk allocates the next one.
	b.arenaChunks[0] = b.arenaChunks[0][:firstCap]
	b.ensureArenaChunk(1)
	secondCap := cap(b.arenaChunks[1])
	if got, want := secondCap, firstCap*2; got != want {
		t.Fatalf("second chunk cap=%d want=%d", got, want)
	}

	b.arenaChunks[1] = b.arenaChunks[1][:secondCap]
	b.ensureArenaChunk(1)
	thirdCap := cap(b.arenaChunks[2])
	if got, want := thirdCap, secondCap*2; got != want {
		t.Fatalf("third chunk cap=%d want=%d", got, want)
	}
}

func TestBatchSet_OversizedValueDoesNotGrowArena(t *testing.T) {
	b := New(newMapValueReader(), 4)
	t.Cleanup(func() { _ = b.Close() })
	key := []byte("k2")

	if err := b.Set([]byte("k"), []byte("ok")); err != nil {
		t.Fatalf("warm Set: %v", err)
	}
	chunksBefore := len(b.arenaChunks)
	lastLenBefore := len(b.arenaChunks[chunksBefore-1])
	oversizedValue := bytes.Repeat([]byte("x"), b.inlineThresholdForKey(key)+1)

	if err := b.Set(key, oversizedValue); err != ErrValueTooLarge {
		t.Fatalf("Set oversized err=%v want=%v", err, ErrValueTooLarge)
	}

	if got := len(b.entries); got != 1 {
		t.Fatalf("entries len=%d want=1", got)
	}
	if got := len(b.arenaChunks); got != chunksBefore {
		t.Fatalf("arena chunk count=%d want=%d", got, chunksBefore)
	}
	if got := len(b.arenaChunks[chunksBefore-1]); got != lastLenBefore {
		t.Fatalf("arena chunk len=%d want=%d", got, lastLenBefore)
	}
}
