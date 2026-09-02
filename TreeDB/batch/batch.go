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

// OpType represents the type of operation recorded in a batch.
type OpType uint8

const (
	OpPut OpType = iota
	OpDelete
	OpDeleteRange
)

// Entry represents a single operation in the batch.
type Entry struct {
	Type     OpType
	Key      []byte        // Put/Delete key, or DeleteRange start bound (nil means unbounded)
	Value    []byte        // Inline value, or DeleteRange exclusive end bound (nil means unbounded)
	ValuePtr page.ValuePtr // For large values
	IsPtr    bool          // True if ValuePtr is valid
	Revision page.EntryRevision
}

// DeleteRange describes a half-open range delete [Start, End). Nil Start or
// End are unbounded on that side.
type DeleteRange struct {
	Start []byte
	End   []byte
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
	DeleteRange(start, end []byte) error
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
	poolKind        batchPoolKind
	maxPoolEntries  int
	// Small-set fast path: most batches touch <=4 value-log segments.
	touchedValueLogSmall    [4]uint32
	touchedValueLogSmallLen int
	touchedValueLog         map[uint32]struct{}
	hasValueLogPointers     bool
	hasDeleteRanges         bool
	sorted                  bool
	compacted               bool
	lastKey                 []byte
	closed                  bool
	reader                  ValueReader
}

const (
	maxBatchPoolCap           = 1 << 16
	maxLargeEntryBatchPoolCap = 1 << 18
)

type batchPoolKind uint8

const (
	batchPoolKindDefault batchPoolKind = iota
	batchPoolKindLargeEntries
)

const (
	// Store key/value copies in chunks so Set/Delete avoid per-entry allocations.
	batchArenaDefaultChunkCap = 64 << 10
	// Avoid retaining huge arenas in the sync.Pool after spikes.
	batchArenaMaxRetainCap = 1 << 20
)

var batchPool = sync.Pool{New: func() any { return newPooledBatch() }}

var largeEntryBatchPool = sync.Pool{New: func() any { return newPooledBatch() }}

func newPooledBatch() any {
	return &Batch{
		entries:   make([]Entry, 0, 16),
		sorted:    true,
		compacted: true,
	}
}

// New creates a new Batch.
func New(reader ValueReader, threshold int) *Batch {
	return Acquire(reader, threshold)
}

// NewRetainingLargeEntries returns a batch from a separate pool that can retain
// larger entry slices. Use this only for repeated internal materializations with
// known large entry counts; ordinary batches should use New/Acquire.
func NewRetainingLargeEntries(reader ValueReader, threshold int) *Batch {
	return AcquireRetainingLargeEntries(reader, threshold)
}

// Acquire returns a reusable Batch from the pool.
func Acquire(reader ValueReader, threshold int) *Batch {
	return acquireFromPool(&batchPool, batchPoolKindDefault, maxBatchPoolCap, reader, threshold)
}

// AcquireRetainingLargeEntries returns a reusable Batch from the large-entry
// pool. See NewRetainingLargeEntries.
func AcquireRetainingLargeEntries(reader ValueReader, threshold int) *Batch {
	return acquireFromPool(&largeEntryBatchPool, batchPoolKindLargeEntries, maxLargeEntryBatchPoolCap, reader, threshold)
}

func acquireFromPool(pool *sync.Pool, kind batchPoolKind, maxEntries int, reader ValueReader, threshold int) *Batch {
	if threshold < 0 {
		threshold = page.DefaultInlineThreshold
	}
	b := pool.Get().(*Batch)
	b.reader = reader
	b.inlineThreshold = threshold
	b.poolKind = kind
	b.maxPoolEntries = maxEntries
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

// IsEmpty reports whether the batch currently has no queued operations.
func (b *Batch) IsEmpty() bool {
	return b == nil || len(b.entries) == 0
}

// Len returns the number of queued logical operations.
func (b *Batch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.entries)
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
	b.hasDeleteRanges = false
	b.byteSize = 0
	b.sorted = true
	b.compacted = true
	b.lastKey = nil
	b.resetArenaLocked()
}

func (b *Batch) resetForPool() {
	if b.entries != nil {
		maxEntries := b.maxPoolEntries
		if maxEntries <= 0 {
			maxEntries = maxBatchPoolCap
		}
		if cap(b.entries) > maxEntries {
			// Drop oversized backing arrays without clearing them first. Once the
			// slice is nil, the batch no longer retains the array or its key/value
			// references, so clearing len entries would only add discard-path CPU.
			b.entries = nil
		} else {
			// SortedEntries clears the compacted tail, so clearing len here is
			// sufficient for normal pooled batches.
			clear(b.entries)
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
	b.hasDeleteRanges = false
	b.sorted = true
	b.compacted = true
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
	switch b.poolKind {
	case batchPoolKindLargeEntries:
		largeEntryBatchPool.Put(b)
	default:
		b.poolKind = batchPoolKindDefault
		b.maxPoolEntries = maxBatchPoolCap
		batchPool.Put(b)
	}
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
	return b.SetViewWithRevision(key, value, page.LegacyEntryRevision)
}

func (b *Batch) SetViewWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)

	entry := Entry{
		Type:     OpPut,
		Key:      key,
		Revision: revision,
	}

	if len(value) > b.inlineThresholdForKey(key) {
		return ErrValueTooLarge
	}
	entry.Value = value

	b.entries = append(b.entries, entry)
	b.byteSize += len(key) + len(value)
	b.compacted = false
	b.noteKeyOrder(entry.Key)
	return nil
}

// AppendViewTrustedSortedUnique records a Put without copying key/value bytes or
// invalidating the sorted/compacted state. Nil keys/values are canonicalized to
// zero-length byte strings. Caller must guarantee key/value remain immutable
// until commit/close, and appended keys are strictly increasing with no duplicates.
func (b *Batch) AppendViewTrustedSortedUnique(key, value []byte) error {
	return b.AppendViewTrustedSortedUniqueWithRevision(key, value, page.LegacyEntryRevision)
}

func (b *Batch) AppendViewTrustedSortedUniqueWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	if len(value) > b.inlineThresholdForKey(key) {
		return ErrValueTooLarge
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		Value:    value,
		Revision: revision,
	})
	b.byteSize += len(key) + len(value)
	if b.sorted {
		b.lastKey = key
	}
	return nil
}

// Set adds or replaces a key/value operation.
func (b *Batch) Set(key, value []byte) error {
	return b.SetWithRevision(key, value, page.LegacyEntryRevision)
}

func (b *Batch) SetWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)

	// Enforce inline threshold before copying into the arena so rejected values
	// do not consume batch-retained memory.
	if len(value) > b.inlineThresholdForKey(key) {
		return ErrValueTooLarge
	}

	// Copy key/value to ensure immutability (reduce per-entry allocations by
	// copying into a per-batch arena).
	k, valCopy := b.arenaCopyPair(key, value)

	entry := Entry{
		Type:     OpPut,
		Key:      k,
		Revision: revision,
	}

	// Store inline
	entry.Value = valCopy

	b.entries = append(b.entries, entry)
	// Approximate size tracking (optional for now)
	b.byteSize += len(k) + len(value)
	b.compacted = false
	b.noteKeyOrder(entry.Key)
	return nil
}

// DeleteView is an internal-performance helper that records a Delete without
// copying the key bytes. Callers must treat key as immutable until the batch is
// committed (Write/WriteSync) or closed.
func (b *Batch) DeleteView(key []byte) error {
	return b.DeleteViewWithRevision(key, page.LegacyEntryRevision)
}

func (b *Batch) DeleteViewWithRevision(key []byte, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)

	b.entries = append(b.entries, Entry{
		Type:     OpDelete,
		Key:      key,
		Revision: revision,
	})
	b.byteSize += len(key)
	b.compacted = false
	b.noteKeyOrder(key)
	return nil
}

// AppendDeleteViewTrustedSortedUnique records a Delete without copying key bytes
// or invalidating sorted/compacted state. Nil keys are canonicalized to the
// empty key. Caller must guarantee key remains immutable until commit/close,
// and appended keys are strictly increasing with no duplicates.
func (b *Batch) AppendDeleteViewTrustedSortedUnique(key []byte) error {
	return b.AppendDeleteViewTrustedSortedUniqueWithRevision(key, page.LegacyEntryRevision)
}

func (b *Batch) AppendDeleteViewTrustedSortedUniqueWithRevision(key []byte, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	b.entries = append(b.entries, Entry{
		Type:     OpDelete,
		Key:      key,
		Revision: revision,
	})
	b.byteSize += len(key)
	if b.sorted {
		b.lastKey = key
	}
	return nil
}

// SetPointer adds a pointer directly to the batch (used by Compaction).
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	return b.SetPointerWithRevision(key, ptr, page.LegacyEntryRevision)
}

func (b *Batch) SetPointerWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	if !page.IsValueLogFileID(ptr.FileID) {
		return fmt.Errorf("invalid value-log pointer: file %d", ptr.FileID)
	}

	k := b.arenaCopy(key)

	entry := Entry{
		Type:     OpPut,
		Key:      k,
		ValuePtr: ptr,
		IsPtr:    true,
		Revision: revision,
	}
	b.entries = append(b.entries, entry)
	b.hasValueLogPointers = true
	b.noteTouchedValueLog(ptr)
	b.compacted = false
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
	return b.SetPointerViewWithRevision(key, ptr, page.LegacyEntryRevision)
}

func (b *Batch) SetPointerViewWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error {
	return b.setPointerViewInternal(key, ptr, true, revision)
}

// SetPointerViewNoTouch is an internal-performance helper that records a
// pointer Put without copying key bytes and without tracking touched value-log
// segments. Callers must separately provide touched segment hints when commit
// publication needs them.
func (b *Batch) SetPointerViewNoTouch(key []byte, ptr page.ValuePtr) error {
	return b.SetPointerViewNoTouchWithRevision(key, ptr, page.LegacyEntryRevision)
}

func (b *Batch) SetPointerViewNoTouchWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error {
	return b.setPointerViewInternal(key, ptr, false, revision)
}

// AppendPointerViewTrustedSortedUnique records a pointer Put without copying key
// bytes or invalidating sorted/compacted state. Nil keys are canonicalized to
// the empty key. Caller must guarantee key remains immutable until commit/close,
// ptr is a value-log pointer, and appended keys are strictly increasing with no
// duplicates.
func (b *Batch) AppendPointerViewTrustedSortedUnique(key []byte, ptr page.ValuePtr) error {
	return b.AppendPointerViewTrustedSortedUniqueWithRevision(key, ptr, page.LegacyEntryRevision)
}

func (b *Batch) AppendPointerViewTrustedSortedUniqueWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	if !page.IsValueLogFileID(ptr.FileID) {
		return fmt.Errorf("invalid value-log pointer: file %d", ptr.FileID)
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
		Revision: revision,
	})
	b.hasValueLogPointers = true
	b.noteTouchedValueLog(ptr)
	if b.sorted {
		b.lastKey = key
	}
	return nil
}

// AppendPointerViewNoTouchTrustedSorted appends a pointer Put without input
// validation, touched-segment tracking, or key-order checks. Caller must
// guarantee: batch is open, key is canonicalized if nil, ptr is a value-log
// pointer, and appended keys are already non-decreasing.
func (b *Batch) AppendPointerViewNoTouchTrustedSorted(key []byte, ptr page.ValuePtr) {
	b.AppendPointerViewNoTouchTrustedSortedWithRevision(key, ptr, page.LegacyEntryRevision)
}

func (b *Batch) AppendPointerViewNoTouchTrustedSortedWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) {
	if b == nil {
		return
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
		Revision: revision,
	})
	b.hasValueLogPointers = true
	b.compacted = false
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

func (b *Batch) setPointerViewInternal(key []byte, ptr page.ValuePtr, noteTouched bool, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	if !page.IsValueLogFileID(ptr.FileID) {
		return fmt.Errorf("invalid value-log pointer: file %d", ptr.FileID)
	}
	b.entries = append(b.entries, Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
		Revision: revision,
	})
	b.hasValueLogPointers = true
	if noteTouched {
		b.noteTouchedValueLog(ptr)
	}
	b.compacted = false
	b.noteKeyOrder(key)
	return nil
}

// Delete adds or replaces a delete operation.
func (b *Batch) Delete(key []byte) error {
	return b.DeleteWithRevision(key, page.LegacyEntryRevision)
}

func (b *Batch) DeleteWithRevision(key []byte, revision page.EntryRevision) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)

	k := b.arenaCopy(key)

	b.entries = append(b.entries, Entry{
		Type:     OpDelete,
		Key:      k,
		Revision: revision,
	})
	b.byteSize += len(k)
	b.compacted = false
	b.noteKeyOrder(k)
	return nil
}

// DeleteRange records a half-open range delete [start, end). Nil bounds are
// unbounded. Empty or reversed bounded ranges are no-ops.
func (b *Batch) DeleteRange(start, end []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if IsDeleteRangeNoop(start, end) {
		return nil
	}
	return b.deleteRangeInternal(start, end, true)
}

// DeleteRangeView records a range delete without copying bound bytes. Callers
// must treat start/end as immutable until the batch is committed or closed.
func (b *Batch) DeleteRangeView(start, end []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if IsDeleteRangeNoop(start, end) {
		return nil
	}
	return b.deleteRangeInternal(start, end, false)
}

func (b *Batch) deleteRangeInternal(start, end []byte, copyBounds bool) error {
	var startCopy, endCopy []byte
	if copyBounds {
		if start != nil {
			startCopy = b.arenaCopy(start)
		}
		if end != nil {
			endCopy = b.arenaCopy(end)
		}
	} else {
		startCopy = start
		endCopy = end
	}
	b.entries = append(b.entries, Entry{
		Type:  OpDeleteRange,
		Key:   startCopy,
		Value: endCopy,
	})
	b.byteSize += len(startCopy) + len(endCopy)
	b.hasDeleteRanges = true
	// Keep the point-entry sorted/compacted state intact for point-only batches;
	// range batches use OrderedEntries/ApplyPlan instead of mutating this slice.
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
		op := ops[i]
		if op.Type == OpDeleteRange {
			continue
		}
		op.Key = normalizeRawKVPointKey(op.Key)
		if op.Type == OpPut && !op.IsPtr {
			op.Value = normalizeRawKVValue(op.Value)
		}
		if op.Type != OpPut || op.IsPtr {
			continue
		}
		if len(op.Value) > b.inlineThresholdForKey(op.Key) {
			return ErrValueTooLarge
		}
	}

	// Just append them. Deduplication happens at Ops() time.
	if len(ops) > 0 {
		b.compacted = false
	}
	for _, op := range ops {
		if op.Type == OpDeleteRange {
			if IsDeleteRangeNoop(op.Key, op.Value) {
				continue
			}
			b.hasDeleteRanges = true
			b.entries = append(b.entries, op)
			b.byteSize += len(op.Key) + len(op.Value)
			continue
		}
		op.Key = normalizeRawKVPointKey(op.Key)
		if op.Type == OpPut && !op.IsPtr {
			op.Value = normalizeRawKVValue(op.Value)
		}
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
// The returned slice aliases batch-owned storage and must be treated as
// read-only. It remains valid only until the batch is mutated, reset, released,
// or closed.
func (b *Batch) SortedEntries() []Entry {
	if b.hasDeleteRanges {
		points, _ := b.ApplyPlan()
		return points
	}
	if len(b.entries) == 0 {
		b.sorted = true
		b.compacted = true
		b.lastKey = nil
		return nil
	}
	if b.sorted && b.compacted {
		return b.entries
	}
	if !b.sorted {
		sort.SliceStable(b.entries, func(i, j int) bool {
			return bytes.Compare(b.entries[i].Key, b.entries[j].Key) < 0
		})
		b.sorted = true
	}

	// Compact in place: keep only the last entry for each key
	if !b.compacted {
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
		b.compacted = true
	}
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
		if e.Type == OpDeleteRange {
			continue
		}
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

// HasDeleteRanges reports whether this batch contains any non-empty range
// deletes. Point-only callers can keep using SortedEntries' compacted fast path.
func (b *Batch) HasDeleteRanges() bool {
	return b != nil && b.hasDeleteRanges
}

// AssignLegacyPointRevisions assigns revision to point entries that do not
// already carry explicit revision metadata. It returns the maximum point
// revision observed after assignment.
func (b *Batch) AssignLegacyPointRevisions(revision page.EntryRevision) page.EntryRevision {
	if b == nil || len(b.entries) == 0 {
		return page.LegacyEntryRevision
	}
	maxRevision := page.LegacyEntryRevision
	for i := range b.entries {
		entry := &b.entries[i]
		if entry.Type == OpDeleteRange {
			continue
		}
		if entry.Revision == page.LegacyEntryRevision && revision != page.LegacyEntryRevision {
			entry.Revision = revision
		}
		if entry.Revision > maxRevision {
			maxRevision = entry.Revision
		}
	}
	return maxRevision
}

// PointRevisionStats returns the highest point-entry revision and whether any
// point entry is still legacy-versioned.
func (b *Batch) PointRevisionStats() (page.EntryRevision, bool) {
	if b == nil || len(b.entries) == 0 {
		return page.LegacyEntryRevision, false
	}
	maxRevision := page.LegacyEntryRevision
	hasLegacy := false
	for i := range b.entries {
		entry := &b.entries[i]
		if entry.Type == OpDeleteRange {
			continue
		}
		if entry.Revision == page.LegacyEntryRevision {
			hasLegacy = true
			continue
		}
		if entry.Revision > maxRevision {
			maxRevision = entry.Revision
		}
	}
	return maxRevision, hasLegacy
}

// MaxPointRevision returns the highest explicit point-entry revision without
// mutating the batch.
func (b *Batch) MaxPointRevision() page.EntryRevision {
	maxRevision, _ := b.PointRevisionStats()
	return maxRevision
}

// OrderedEntries returns the submission-order operation stream. The returned
// slice aliases batch-owned storage and must be treated as read-only.
func (b *Batch) OrderedEntries() []Entry {
	if b == nil || len(b.entries) == 0 {
		return nil
	}
	return b.entries
}

// ApplyPlan canonicalizes an ordered mixed batch into sorted final point ops and
// merged range tombstones over the base tree. The point-only SortedEntries path
// remains allocation-free and is preferred when HasDeleteRanges is false.
func (b *Batch) ApplyPlan() ([]Entry, []DeleteRange) {
	if b == nil || len(b.entries) == 0 {
		return nil, nil
	}
	if !b.hasDeleteRanges {
		return b.SortedEntries(), nil
	}
	return BuildApplyPlanFromEntries(b.entries, true)
}

// BuildApplyPlanFromEntries canonicalizes an ordered mixed operation stream into
// sorted final point ops and merged range tombstones over the base tree.
func BuildApplyPlanFromEntries(entries []Entry, hasDeleteRanges bool) ([]Entry, []DeleteRange) {
	if len(entries) == 0 {
		return nil, nil
	}
	if !hasDeleteRanges {
		points := append([]Entry(nil), entries...)
		sort.SliceStable(points, func(i, j int) bool {
			return bytes.Compare(points[i].Key, points[j].Key) < 0
		})
		out := points[:0]
		for i := 0; i < len(points); i++ {
			if i+1 < len(points) && bytes.Equal(points[i].Key, points[i+1].Key) {
				continue
			}
			out = append(out, points[i])
		}
		return out, nil
	}
	type pointCandidate struct {
		entry Entry
		seq   int
	}
	type sequencedRange struct {
		rangeOp DeleteRange
		seq     int
	}
	pointsByKey := make(map[string]pointCandidate)
	ranges := make([]sequencedRange, 0, 4)
	for seq, entry := range entries {
		switch entry.Type {
		case OpPut, OpDelete:
			pointsByKey[string(entry.Key)] = pointCandidate{entry: entry, seq: seq}
		case OpDeleteRange:
			if IsDeleteRangeNoop(entry.Key, entry.Value) {
				continue
			}
			ranges = append(ranges, sequencedRange{rangeOp: DeleteRange{Start: entry.Key, End: entry.Value}, seq: seq})
		}
	}
	points := make([]Entry, 0, len(pointsByKey))
	for _, point := range pointsByKey {
		shadowed := false
		for _, r := range ranges {
			if r.seq > point.seq && DeleteRangeContainsKey(r.rangeOp, point.entry.Key) {
				shadowed = true
				break
			}
		}
		if !shadowed {
			points = append(points, point.entry)
		}
	}
	sort.Slice(points, func(i, j int) bool {
		return bytes.Compare(points[i].Key, points[j].Key) < 0
	})
	merged := make([]DeleteRange, 0, len(ranges))
	for _, r := range ranges {
		merged = append(merged, r.rangeOp)
	}
	merged = MergeDeleteRanges(merged)
	return points, merged
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

// IsDeleteRangeNoop reports whether [start,end) cannot delete any valid TreeDB
// key. Nil means unbounded; a non-nil end <= non-nil start is empty/reversed.
func IsDeleteRangeNoop(start, end []byte) bool {
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return true
	}
	// The empty byte string is the minimum concrete key. An unbounded-lower
	// range ending at that exclusive bound contains no keys.
	return start == nil && end != nil && len(end) == 0
}

// DeleteRangeContainsKey reports whether key is inside r's half-open range.
func DeleteRangeContainsKey(r DeleteRange, key []byte) bool {
	if key == nil {
		return false
	}
	if r.Start != nil && bytes.Compare(key, r.Start) < 0 {
		return false
	}
	if r.End != nil && bytes.Compare(key, r.End) >= 0 {
		return false
	}
	return true
}

// DeleteRangesContainKey reports whether key is covered by any merged or
// unmerged range in ranges.
func DeleteRangesContainKey(ranges []DeleteRange, key []byte) bool {
	for _, r := range ranges {
		if DeleteRangeContainsKey(r, key) {
			return true
		}
	}
	return false
}

// MergeDeleteRanges returns sorted, non-overlapping ranges equivalent to the
// union of ranges. It does not copy bound bytes; callers must keep them alive.
func MergeDeleteRanges(ranges []DeleteRange) []DeleteRange {
	if len(ranges) <= 1 {
		if len(ranges) == 1 && IsDeleteRangeNoop(ranges[0].Start, ranges[0].End) {
			return nil
		}
		return ranges
	}
	out := ranges[:0]
	for _, r := range ranges {
		if !IsDeleteRangeNoop(r.Start, r.End) {
			out = append(out, r)
		}
	}
	if len(out) <= 1 {
		return out
	}
	sort.Slice(out, func(i, j int) bool {
		return compareRangeStarts(out[i].Start, out[j].Start) < 0
	})
	merged := out[:0]
	cur := out[0]
	for i := 1; i < len(out); i++ {
		next := out[i]
		if deleteRangesOverlapOrTouch(cur, next) {
			cur.End = maxRangeEnd(cur.End, next.End)
			continue
		}
		merged = append(merged, cur)
		cur = next
	}
	merged = append(merged, cur)
	return merged
}

func compareRangeStarts(a, b []byte) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	return bytes.Compare(a, b)
}

func deleteRangesOverlapOrTouch(a, b DeleteRange) bool {
	if a.End == nil || b.Start == nil {
		return true
	}
	return bytes.Compare(b.Start, a.End) <= 0
}

func maxRangeEnd(a, b []byte) []byte {
	if a == nil || b == nil {
		return nil
	}
	if bytes.Compare(a, b) >= 0 {
		return a
	}
	return b
}

// DeleteRangeOverlapsSpan reports whether r intersects the half-open child span
// [low, high). Nil high means unbounded upper; nil r bounds are unbounded.
func DeleteRangeOverlapsSpan(r DeleteRange, low, high []byte) bool {
	if IsDeleteRangeNoop(r.Start, r.End) {
		return false
	}
	if r.End != nil && low != nil && bytes.Compare(r.End, low) <= 0 {
		return false
	}
	if high != nil && r.Start != nil && bytes.Compare(r.Start, high) >= 0 {
		return false
	}
	return true
}
