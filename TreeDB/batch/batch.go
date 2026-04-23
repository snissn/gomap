package batch

import (
	"bytes"
	"errors"
	"fmt"
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
	arenaChunks     [][]byte
	byteSize        int
	inlineThreshold int
	thresholdForKey func([]byte) int
	// Small-set fast path: most batches touch <=4 value-log segments.
	touchedValueLogSmall    [4]uint32
	touchedValueLogSmallLen int
	touchedValueLog         map[uint32]struct{}
	hasValueLogPointers     bool
	sorted                  bool
	lastKey                 []byte
	closed                  bool
	reader                  ValueReader
}

const maxBatchPoolCap = 1 << 16

const (
	// Store key/value copies in chunks so Set/Delete avoid per-entry allocations.
	batchArenaDefaultChunkCap = 64 << 10
	// Avoid retaining huge arenas in the sync.Pool after spikes.
	batchArenaMaxRetainCap = 1 << 20
)

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

// SetInlineThresholdResolver installs an optional per-key threshold resolver
// used by Set/SetView/SetOps inline-size checks.
func (b *Batch) SetInlineThresholdResolver(resolver func([]byte) int) {
	if b == nil {
		return
	}
	b.thresholdForKey = resolver
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
		// Avoid holding onto key/value copies from previous uses.
		clear(b.entries)
		b.entries = b.entries[:0]
	}
	if len(b.touchedValueLog) > 0 {
		clear(b.touchedValueLog)
	}
	b.touchedValueLog = nil
	b.touchedValueLogSmallLen = 0
	b.hasValueLogPointers = false
	b.byteSize = 0
	b.sorted = true
	b.lastKey = nil
	b.resetArenaLocked()
}

func (b *Batch) resetForPool() {
	if b.entries != nil {
		// SortedEntries clears the compacted tail, so clearing len is sufficient
		// here while avoiding O(cap) work for large pooled batches.
		clear(b.entries)
		if cap(b.entries) > maxBatchPoolCap {
			b.entries = nil
		} else {
			b.entries = b.entries[:0]
		}
	}
	b.byteSize = 0
	if len(b.touchedValueLog) > 0 {
		clear(b.touchedValueLog)
	}
	b.touchedValueLog = nil
	b.touchedValueLogSmallLen = 0
	b.hasValueLogPointers = false
	b.sorted = true
	b.lastKey = nil
	b.resetArenaLocked()
}

func (b *Batch) resetArenaLocked() {
	if len(b.arenaChunks) == 0 {
		return
	}
	keepIdx := -1
	keepCap := 0
	for i := range b.arenaChunks {
		chunkCap := cap(b.arenaChunks[i])
		if chunkCap > batchArenaMaxRetainCap {
			continue
		}
		if keepIdx < 0 || chunkCap > keepCap {
			keepIdx = i
			keepCap = chunkCap
		}
	}
	if keepIdx < 0 {
		for i := range b.arenaChunks {
			b.arenaChunks[i] = nil
		}
		b.arenaChunks = nil
		return
	}
	keep := b.arenaChunks[keepIdx][:0]
	for i := 1; i < len(b.arenaChunks); i++ {
		b.arenaChunks[i] = nil
	}
	b.arenaChunks[0] = keep
	b.arenaChunks = b.arenaChunks[:1]
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
	b.thresholdForKey = nil
	b.closed = true
	batchPool.Put(b)
}

func (b *Batch) inlineThresholdForKey(key []byte) int {
	if b == nil {
		return page.DefaultInlineThreshold
	}
	if b.thresholdForKey != nil {
		if threshold := b.thresholdForKey(key); threshold >= 0 {
			return threshold
		}
		return b.inlineThreshold
	}
	return b.inlineThreshold
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

func (b *Batch) ensureArenaChunk(minFree int) {
	if minFree <= 0 {
		return
	}
	last := -1
	if n := len(b.arenaChunks); n > 0 {
		last = n - 1
		chunk := b.arenaChunks[last]
		if cap(chunk)-len(chunk) >= minFree {
			return
		}
	}
	chunkCap := batchArenaDefaultChunkCap
	if last >= 0 {
		prevCap := cap(b.arenaChunks[last])
		if prevCap > chunkCap {
			chunkCap = prevCap
		}
		// Grow arena chunks geometrically (bounded) to reduce repeated chunk
		// allocations on large batches with many small entries.
		if prevCap >= batchArenaDefaultChunkCap && prevCap < batchArenaMaxRetainCap {
			next := prevCap << 1
			if next > batchArenaMaxRetainCap {
				next = batchArenaMaxRetainCap
			}
			if next > chunkCap {
				chunkCap = next
			}
		}
	}
	if chunkCap < minFree {
		chunkCap = minFree
	}
	b.arenaChunks = append(b.arenaChunks, make([]byte, 0, chunkCap))
}

func (b *Batch) arenaCopy(src []byte) []byte {
	if len(src) == 0 {
		return []byte{}
	}
	b.ensureArenaChunk(len(src))
	last := len(b.arenaChunks) - 1
	chunk := b.arenaChunks[last]
	off := len(chunk)
	chunk = chunk[:off+len(src)]
	copy(chunk[off:], src)
	b.arenaChunks[last] = chunk
	// Match make([]byte, n) semantics (cap==len) to avoid accidental append
	// writing into the arena.
	return chunk[off : off+len(src) : off+len(src)]
}

func (b *Batch) arenaCopyPair(key, value []byte) ([]byte, []byte) {
	if len(key) == 0 {
		// Preserve non-nil empty semantics for callers that pass empty slices.
		if len(value) == 0 {
			return []byte{}, []byte{}
		}
		return []byte{}, b.arenaCopy(value)
	}
	if len(value) == 0 {
		return b.arenaCopy(key), []byte{}
	}

	total := len(key) + len(value)
	b.ensureArenaChunk(total)
	last := len(b.arenaChunks) - 1
	chunk := b.arenaChunks[last]
	off := len(chunk)
	chunk = chunk[:off+total]
	copy(chunk[off:], key)
	copy(chunk[off+len(key):], value)
	b.arenaChunks[last] = chunk

	k := chunk[off : off+len(key) : off+len(key)]
	v := chunk[off+len(key) : off+total : off+total]
	return k, v
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

// EntriesCap returns the current capacity of the internal entry buffer.
func (b *Batch) EntriesCap() int {
	if b == nil {
		return 0
	}
	return cap(b.entries)
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

	if len(value) > b.inlineThresholdForKey(key) {
		return ErrValueTooLarge
	}
	entry.Value = value

	b.entries = append(b.entries, entry)
	b.byteSize += len(key) + len(value)
	b.noteKeyOrder(entry.Key)
	return nil
}

// SetStealView records a Put without copying key/value bytes. Callers must
// treat key/value as transferred to the batch and must not mutate or reuse them
// after this call, because other batch implementations may retain the provided
// slices beyond Write/WriteSync or Close. This in-memory batch currently
// delegates to SetView internally, but callers must not rely on that weaker
// lifetime.
func (b *Batch) SetStealView(key, value []byte) error {
	return b.SetView(key, value)
}

// Set adds or replaces a key/value operation.
func (b *Batch) Set(key, value []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil {
		return ErrKeyEmpty
	}

	// Enforce inline threshold before copying into the arena so rejected values
	// do not consume batch-retained memory.
	if len(value) > b.inlineThresholdForKey(key) {
		return ErrValueTooLarge
	}

	// Copy key/value to ensure immutability (reduce per-entry allocations by
	// copying into a per-batch arena).
	k, valCopy := b.arenaCopyPair(key, value)

	entry := Entry{
		Type: OpPut,
		Key:  k,
	}

	// Store inline
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
	if !page.IsValueLogFileID(ptr.FileID) {
		return fmt.Errorf("invalid value-log pointer: file %d", ptr.FileID)
	}

	k := b.arenaCopy(key)

	entry := Entry{
		Type:     OpPut,
		Key:      k,
		ValuePtr: ptr,
		IsPtr:    true,
	}
	b.entries = append(b.entries, entry)
	b.hasValueLogPointers = true
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
	return b.setPointerViewInternal(key, ptr, true)
}

// SetPointerViewNoTouch is an internal-performance helper that records a
// pointer Put without copying key bytes and without tracking touched value-log
// segments. Callers must separately provide touched segment hints when commit
// publication needs them.
func (b *Batch) SetPointerViewNoTouch(key []byte, ptr page.ValuePtr) error {
	return b.setPointerViewInternal(key, ptr, false)
}

// AppendPointerViewNoTouchTrustedSorted appends a pointer Put without input
// validation, touched-segment tracking, or key-order checks. Caller must
// guarantee: batch is open, key is non-empty, ptr is a value-log pointer, and
// appended keys are already non-decreasing.
func (b *Batch) AppendPointerViewNoTouchTrustedSorted(key []byte, ptr page.ValuePtr) {
	if b == nil {
		return
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
	})
	b.hasValueLogPointers = true
	if b.sorted {
		b.lastKey = key
	}
}

// NoteTouchedValueLogFileID is an internal helper that records a touched
// value-log segment without appending a batch entry.
func (b *Batch) NoteTouchedValueLogFileID(fileID uint32) {
	if b == nil || !page.IsValueLogFileID(fileID) {
		return
	}
	b.hasValueLogPointers = true
	b.noteTouchedValueLogFileID(fileID)
}

func (b *Batch) setPointerViewInternal(key []byte, ptr page.ValuePtr, noteTouched bool) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return fmt.Errorf("invalid value-log pointer: file %d", ptr.FileID)
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
	})
	b.hasValueLogPointers = true
	if noteTouched {
		b.noteTouchedValueLog(ptr)
	}
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

	k := b.arenaCopy(key)

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
		if len(op.Value) > b.inlineThresholdForKey(op.Key) {
			return ErrValueTooLarge
		}
	}

	// Just append them. Deduplication happens at Ops() time.
	for _, op := range ops {
		if op.IsPtr {
			if !page.IsValueLogFileID(op.ValuePtr.FileID) {
				return fmt.Errorf("invalid value-log pointer in SetOps: file %d", op.ValuePtr.FileID)
			}
			b.hasValueLogPointers = true
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
	oldLen := len(b.entries)
	out := b.entries[:0]
	for i := 0; i < len(b.entries); i++ {
		// If next is same key, skip this one (it's overwritten by next)
		if i+1 < len(b.entries) && bytes.Equal(b.entries[i].Key, b.entries[i+1].Key) {
			continue
		}
		out = append(out, b.entries[i])
	}
	// Clear the now-unused tail to avoid pinning batch arenas via stale pointers.
	clear(b.entries[len(out):oldLen])
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
	return b.hasValueLogPointers
}

// TouchedValueLogSegments reports the value-log segments that were touched by
// pointer puts in this batch. The returned slice is sorted for deterministic
// commit/publish behavior.
//
// In the small-set fast path, the returned slice aliases internal batch
// storage and remains valid only until the batch is mutated, reset, or
// released. Callers must treat the result as read-only.
func (b *Batch) TouchedValueLogSegments() []uint32 {
	if b == nil {
		return nil
	}
	if b.touchedValueLogSmallLen == 0 && len(b.touchedValueLog) == 0 {
		return nil
	}
	if len(b.touchedValueLog) == 0 {
		out := b.touchedValueLogSmall[:b.touchedValueLogSmallLen]
		if len(out) > 1 {
			sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		}
		return out[:len(out):len(out)]
	}
	out := make([]uint32, 0, b.touchedValueLogSmallLen+len(b.touchedValueLog))
	out = append(out, b.touchedValueLogSmall[:b.touchedValueLogSmallLen]...)
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
	b.noteTouchedValueLogFileID(ptr.FileID)
}

func (b *Batch) noteTouchedValueLogFileID(id uint32) {
	for i := 0; i < b.touchedValueLogSmallLen; i++ {
		if b.touchedValueLogSmall[i] == id {
			return
		}
	}
	if b.touchedValueLog == nil && b.touchedValueLogSmallLen < len(b.touchedValueLogSmall) {
		b.touchedValueLogSmall[b.touchedValueLogSmallLen] = id
		b.touchedValueLogSmallLen++
		return
	}
	if b.touchedValueLog == nil {
		b.touchedValueLog = make(map[uint32]struct{}, b.touchedValueLogSmallLen+4)
		for i := 0; i < b.touchedValueLogSmallLen; i++ {
			b.touchedValueLog[b.touchedValueLogSmall[i]] = struct{}{}
		}
		b.touchedValueLogSmallLen = 0
	}
	b.touchedValueLog[id] = struct{}{}
}
