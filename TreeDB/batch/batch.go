package batch

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/page"
)

var (
	ErrKeyEmpty           = errors.New("key cannot be empty")
	ErrBatchClosed        = errors.New("batch closed")
	ErrValueTooLarge      = errors.New("value exceeds inline threshold; use SetPointer")
	ErrMissingValueReader = errors.New("value reader unavailable")
)

// OpType represents the type of operation (Put or Delete).
type OpType uint8

const (
	OpPut OpType = iota
	OpDelete
)

// Entry represents a single operation in the batch.
type Entry struct {
	Type     OpType
	Key      []byte
	Value    []byte        // For inline values
	ValuePtr page.ValuePtr // For large values
	IsPtr    bool          // True if ValuePtr is valid
}

// ValueReader resolves value pointers for Replay callers that require full values.
type ValueReader interface {
	Read(ptr page.ValuePtr) ([]byte, error)
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}

// Interface defines the contract for a batch operation.
type Interface interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	SetOps(ops []Entry) error
	Write() error
	WriteSync() error
	Close() error
	Replay(func(Entry) error) error
	GetByteSize() (int, error)
}

// Batch accumulates writes and deletes before committing them.
type Batch struct {
	entries         []Entry
	byteSize        int
	inlineThreshold int
	touchedValueLog map[uint32]struct{}
	sorted          bool
	lastKey         []byte
	closed          bool
	reader          ValueReader
}

const maxBatchPoolCap = 1 << 16

var batchPool = sync.Pool{
	New: func() any {
		return &Batch{
			entries: make([]Entry, 0, 16),
			sorted:  true,
		}
	},
}

// New creates a new Batch.
func New(reader ValueReader, threshold int) *Batch {
	return Acquire(reader, threshold)
}

// Acquire returns a reusable Batch from the pool.
func Acquire(reader ValueReader, threshold int) *Batch {
	if threshold < 0 {
		threshold = page.DefaultInlineThreshold
	}
	b := batchPool.Get().(*Batch)
	b.reader = reader
	b.inlineThreshold = threshold
	b.closed = false
	b.resetLocked()
	return b
}

func (b *Batch) ensureOpen() error {
	if b.closed {
		return ErrBatchClosed
	}
	return nil
}

// Reset clears the batch for reuse without releasing its internal buffers.
func (b *Batch) Reset() {
	if b == nil || b.closed {
		return
	}
	b.resetLocked()
}

func (b *Batch) resetLocked() {
	if b.entries != nil {
		b.entries = b.entries[:0]
	}
	if len(b.touchedValueLog) > 0 {
		clear(b.touchedValueLog)
	}
	b.byteSize = 0
	b.sorted = true
	b.lastKey = nil
}

func (b *Batch) resetForPool() {
	if b.entries != nil {
		clear(b.entries)
		if cap(b.entries) > maxBatchPoolCap {
			b.entries = nil
		} else {
			b.entries = b.entries[:0]
		}
	}
	b.byteSize = 0
	if len(b.touchedValueLog) > 1024 {
		b.touchedValueLog = nil
	} else if len(b.touchedValueLog) > 0 {
		clear(b.touchedValueLog)
	}
	b.sorted = true
	b.lastKey = nil
}

// Close returns the batch to the pool.
func (b *Batch) Close() error {
	Release(b)
	return nil
}

// Release resets and returns the batch to the pool.
func Release(b *Batch) {
	if b == nil {
		return
	}
	b.resetForPool()
	b.reader = nil
	b.inlineThreshold = 0
	b.closed = true
	batchPool.Put(b)
}

func (b *Batch) noteKeyOrder(key []byte) {
	if !b.sorted {
		return
	}
	if b.lastKey != nil && bytes.Compare(b.lastKey, key) > 0 {
		b.sorted = false
		return
	}
	b.lastKey = key
}

// Reserve grows internal buffers to accommodate roughly n entries without
// reallocation. It is intended as a best-effort performance hint for
// high-throughput internal callers (e.g. flush).
func (b *Batch) Reserve(n int) {
	if b == nil || n <= 0 {
		return
	}
	if cap(b.entries) < n {
		b.entries = make([]Entry, 0, n)
	}
}

// SetView is an internal-performance helper that records a Put without copying
// key/value bytes. Callers must treat key/value as immutable until the batch is
// committed (Write/WriteSync) or closed.
func (b *Batch) SetView(key, value []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil {
		return ErrKeyEmpty
	}

	entry := Entry{
		Type: OpPut,
		Key:  key,
	}

	if len(value) > b.inlineThreshold {
		return ErrValueTooLarge
	}
	entry.Value = value

	b.entries = append(b.entries, entry)
	b.byteSize += len(key) + len(value)
	b.noteKeyOrder(entry.Key)
	return nil
}

// Set adds or replaces a key/value operation.
func (b *Batch) Set(key, value []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil {
		return ErrKeyEmpty
	}

	// Copy key to ensure immutability
	k := make([]byte, len(key))
	copy(k, key)

	entry := Entry{
		Type: OpPut,
		Key:  k,
	}

	// Check threshold
	if len(value) > b.inlineThreshold {
		return ErrValueTooLarge
	}
	// Store inline
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	entry.Value = valCopy

	b.entries = append(b.entries, entry)
	// Approximate size tracking (optional for now)
	b.byteSize += len(k) + len(value)
	b.noteKeyOrder(entry.Key)
	return nil
}

// DeleteView is an internal-performance helper that records a Delete without
// copying the key bytes. Callers must treat key as immutable until the batch is
// committed (Write/WriteSync) or closed.
func (b *Batch) DeleteView(key []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil {
		return ErrKeyEmpty
	}

	b.entries = append(b.entries, Entry{
		Type: OpDelete,
		Key:  key,
	})
	b.byteSize += len(key)
	b.noteKeyOrder(key)
	return nil
}

// SetPointer adds a pointer directly to the batch (used by Compaction).
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	// Copy key
	k := make([]byte, len(key))
	copy(k, key)

	entry := Entry{
		Type:     OpPut,
		Key:      k,
		ValuePtr: ptr,
		IsPtr:    true,
	}
	b.entries = append(b.entries, entry)
	b.noteTouchedValueLog(ptr)
	b.noteKeyOrder(entry.Key)
	return nil
}

// SetPointerView is an internal-performance helper that records a pointer Put
// without copying the key bytes. Callers must treat key as immutable until the
// batch is committed (Write/WriteSync) or closed.
//
// This is intentionally not part of the public batch.Interface; it is a
// best-effort optimization used by higher-level layers (e.g. cached flush
// streaming).
func (b *Batch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
	})
	b.noteTouchedValueLog(ptr)
	b.noteKeyOrder(key)
	return nil
}

// Delete adds or replaces a delete operation.
func (b *Batch) Delete(key []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil {
		return ErrKeyEmpty
	}

	// Copy key
	k := make([]byte, len(key))
	copy(k, key)

	b.entries = append(b.entries, Entry{
		Type: OpDelete,
		Key:  k,
	})
	b.byteSize += len(k)
	b.noteKeyOrder(k)
	return nil
}

// Replay iterates over the batch entries.
func (b *Batch) Replay(fn func(Entry) error) error {
	for _, entry := range b.entries {
		if entry.IsPtr && entry.Value == nil {
			if b.reader == nil {
				return ErrMissingValueReader
			}
			val, err := b.reader.Read(entry.ValuePtr)
			if err != nil {
				return err
			}
			entry.Value = val
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

// SetOps merges a slice of operations into the batch.
func (b *Batch) SetOps(ops []Entry) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	for i := range ops {
		op := &ops[i]
		if op.Type != OpPut || op.IsPtr {
			continue
		}
		if len(op.Value) > b.inlineThreshold {
			return ErrValueTooLarge
		}
	}

	// Just append them. Deduplication happens at Ops() time.
	for _, op := range ops {
		if op.IsPtr {
			b.noteTouchedValueLog(op.ValuePtr)
		}
		b.noteKeyOrder(op.Key)
		b.entries = append(b.entries, op)
		b.byteSize += len(op.Key) + len(op.Value) // Value is nil for pointers.
	}
	return nil
}

// SortedEntries returns the operations sorted by key.
// Duplicate keys are resolved (last write wins).
// This modifies the internal entries slice (sorts and compacts).
func (b *Batch) SortedEntries() []Entry {
	if len(b.entries) == 0 {
		return nil
	}
	if !b.sorted {
		sort.SliceStable(b.entries, func(i, j int) bool {
			return bytes.Compare(b.entries[i].Key, b.entries[j].Key) < 0
		})
		b.sorted = true
	}

	// Compact in place: keep only the last entry for each key
	out := b.entries[:0]
	for i := 0; i < len(b.entries); i++ {
		// If next is same key, skip this one (it's overwritten by next)
		if i+1 < len(b.entries) && bytes.Equal(b.entries[i].Key, b.entries[i+1].Key) {
			continue
		}
		out = append(out, b.entries[i])
	}
	b.entries = out
	if len(b.entries) > 0 {
		b.lastKey = b.entries[len(b.entries)-1].Key
	}
	return b.entries
}

// Ops returns the map of operations.
// WARNING: This constructs the map on demand. Duplicate keys in the batch
// are resolved so the last one wins.
func (b *Batch) Ops() map[string]Entry {
	ops := make(map[string]Entry, len(b.entries))
	for _, e := range b.entries {
		ops[string(e.Key)] = e
	}
	return ops
}

// ByteSize returns the approximate size of the batch.
func (b *Batch) ByteSize() int {
	return b.byteSize
}

// HasValueLogPointers reports whether this batch contains any value-log
// pointer operations.
func (b *Batch) HasValueLogPointers() bool {
	if b == nil {
		return false
	}
	return len(b.touchedValueLog) > 0
}

// TouchedValueLogSegments reports the value-log segments that were touched by
// pointer puts in this batch. The returned slice is sorted for deterministic
// commit/publish behavior.
func (b *Batch) TouchedValueLogSegments() []uint32 {
	if b == nil || len(b.touchedValueLog) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(b.touchedValueLog))
	for id := range b.touchedValueLog {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (b *Batch) noteTouchedValueLog(ptr page.ValuePtr) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return
	}
	if b.touchedValueLog == nil {
		b.touchedValueLog = make(map[uint32]struct{}, 4)
	}
	b.touchedValueLog[ptr.FileID] = struct{}{}
}
