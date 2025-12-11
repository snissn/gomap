package gomap

import "sync"

// BatchWriter buffers writes and flushes them to the underlying HashmapDistributed
// using AddMany to reduce syscall and hashing overhead.
type BatchWriter struct {
	store *HashmapDistributed
	limit int

	mu  sync.Mutex
	buf []Item
}

// NewBatchWriter creates a new BatchWriter with the given flush threshold.
// A zero or negative limit defaults to 1024 items.
func NewBatchWriter(store *HashmapDistributed, limit int) *BatchWriter {
	if limit <= 0 {
		limit = 1024
	}
	return &BatchWriter{
		store: store,
		limit: limit,
		buf:   make([]Item, 0, limit),
	}
}

// Add buffers a key/value. It flushes automatically when the buffer reaches the limit.
func (b *BatchWriter) Add(key, value []byte) error {
	b.mu.Lock()
	b.buf = append(b.buf, Item{Key: key, Value: value})
	shouldFlush := len(b.buf) >= b.limit
	buf := b.buf
	if shouldFlush {
		b.buf = make([]Item, 0, b.limit)
	}
	b.mu.Unlock()

	if shouldFlush {
		return b.store.AddMany(buf)
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
	b.buf = make([]Item, 0, b.limit)
	b.mu.Unlock()
	return b.store.AddMany(buf)
}
