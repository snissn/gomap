package hashdb

import "sync"

// BatchWriter buffers writes and flushes them to the underlying HashDB using
// PutMany to reduce syscall and hashing overhead.
//
// Note: keys are copied into an internal arena so callers may safely reuse key
// buffers between Add() calls (common in hot loops). Values are not copied; the
// caller must not mutate the value slice until it has been flushed (explicitly
// via Flush() or implicitly by reaching the limit).
type BatchWriter struct {
	store *HashDB
	limit int

	mu  sync.Mutex
	buf []Item

	keyChunkSize int
	keyArena     *keyArena
	arenaPool    sync.Pool
}

// NewBatchWriter creates a new BatchWriter with the given flush threshold.
// A zero or negative limit defaults to 1024 items.
func NewBatchWriter(store *HashDB, limit int) *BatchWriter {
	if limit <= 0 {
		limit = 1024
	}
	bw := &BatchWriter{
		store: store,
		limit: limit,
		buf:   make([]Item, 0, limit),
		// 64KiB comfortably fits typical key workloads (e.g. 1k * 8B keys) in a
		// single chunk while keeping per-writer memory small.
		keyChunkSize: 64 * 1024,
	}

	bw.arenaPool.New = func() any {
		return newKeyArena(bw.keyChunkSize)
	}
	bw.keyArena = bw.arenaPool.Get().(*keyArena)
	bw.keyArena.reset()
	return bw
}

// Add buffers a key/value. It flushes automatically when the buffer reaches the limit.
func (b *BatchWriter) Add(key, value []byte) error {
	b.mu.Lock()
	if b.keyArena == nil {
		b.keyArena = b.arenaPool.Get().(*keyArena)
		b.keyArena.reset()
	}
	keyCopy := b.keyArena.copy(key)
	b.buf = append(b.buf, Item{Key: keyCopy, Value: value})
	shouldFlush := len(b.buf) >= b.limit
	buf := b.buf
	arena := b.keyArena
	if shouldFlush {
		b.buf = make([]Item, 0, b.limit)
		b.keyArena = b.arenaPool.Get().(*keyArena)
		b.keyArena.reset()
	}
	b.mu.Unlock()

	if shouldFlush {
		err := b.store.PutMany(buf)
		arena.reset()
		b.arenaPool.Put(arena)
		return err
	}
	return nil
}

// Flush writes any buffered items.
func (b *BatchWriter) Flush() error {
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	buf := b.buf
	arena := b.keyArena
	b.buf = make([]Item, 0, b.limit)
	b.keyArena = b.arenaPool.Get().(*keyArena)
	b.keyArena.reset()
	b.mu.Unlock()

	err := b.store.PutMany(buf)
	if arena != nil {
		arena.reset()
		b.arenaPool.Put(arena)
	}
	return err
}

type keyArena struct {
	chunkSize int
	blocks    [][]byte
	block     []byte
	off       int
	nextBlock int
}

func newKeyArena(chunkSize int) *keyArena {
	if chunkSize <= 0 {
		chunkSize = 64 * 1024
	}
	return &keyArena{chunkSize: chunkSize}
}

func (a *keyArena) reset() {
	// Retain a bounded number of blocks so steady-state workloads avoid
	// allocations, while still allowing bursty batches to release memory.
	const maxKeepBlocks = 16 // 16 * 64KiB = 1MiB of key arena per pooled instance
	if len(a.blocks) > maxKeepBlocks {
		for i := maxKeepBlocks; i < len(a.blocks); i++ {
			a.blocks[i] = nil
		}
		a.blocks = a.blocks[:maxKeepBlocks]
	}

	if len(a.blocks) == 0 {
		a.blocks = [][]byte{make([]byte, a.chunkSize)}
	}

	// Normalize lengths for reuse.
	for i := range a.blocks {
		if cap(a.blocks[i]) < a.chunkSize {
			a.blocks[i] = make([]byte, a.chunkSize)
		} else {
			a.blocks[i] = a.blocks[i][:a.chunkSize]
		}
	}

	a.block = a.blocks[0]
	a.off = 0
	a.nextBlock = 1
}

func (a *keyArena) grow() {
	if a.nextBlock < len(a.blocks) {
		a.block = a.blocks[a.nextBlock]
		a.nextBlock++
	} else {
		a.block = make([]byte, a.chunkSize)
		a.blocks = append(a.blocks, a.block)
		a.nextBlock = len(a.blocks)
	}
	a.off = 0
}

func (a *keyArena) copy(src []byte) []byte {
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		return []byte{}
	}
	if a.block == nil || a.off+len(src) > len(a.block) {
		// Large keys get their own allocation to avoid fragmenting the arena.
		if len(src) > a.chunkSize {
			dst := make([]byte, len(src))
			copy(dst, src)
			return dst
		}
		a.grow()
	}
	dst := a.block[a.off : a.off+len(src)]
	copy(dst, src)
	a.off += len(src)
	return dst
}
