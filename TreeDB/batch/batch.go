package batch

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

var (
	ErrKeyEmpty    = errors.New("key cannot be empty")
	ErrBatchClosed = errors.New("batch closed")
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
	Flags    byte
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
	SetLastSeq(uint64)
}

// Batch accumulates writes and deletes before committing them.
type Batch struct {
	entries         []Entry
	slabManager     *slab.SlabManager
	byteSize        int
	slabWritten     int
	slabWrittenByID map[uint32]int64
	slabMaxEndByID  map[uint32]uint64
	inlineThreshold int
	sorted          bool
	assumeSorted    bool
	lastKey         []byte
	closed          bool
	largeIdxs       []int
	largeKeys       [][]byte
	largeVals       [][]byte
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
func New(sm *slab.SlabManager, threshold int) *Batch {
	return Acquire(sm, threshold)
}

// Acquire returns a reusable Batch from the pool.
func Acquire(sm *slab.SlabManager, threshold int) *Batch {
	if threshold < 0 {
		threshold = page.DefaultInlineThreshold
	}
	b := batchPool.Get().(*Batch)
	b.slabManager = sm
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
	if b.largeIdxs != nil {
		b.largeIdxs = b.largeIdxs[:0]
	}
	if b.largeKeys != nil {
		b.largeKeys = b.largeKeys[:0]
	}
	if b.largeVals != nil {
		b.largeVals = b.largeVals[:0]
	}
	b.byteSize = 0
	b.slabWritten = 0
	if b.slabWrittenByID != nil {
		for id := range b.slabWrittenByID {
			delete(b.slabWrittenByID, id)
		}
	}
	if b.slabMaxEndByID != nil {
		for id := range b.slabMaxEndByID {
			delete(b.slabMaxEndByID, id)
		}
	}
	b.sorted = true
	b.assumeSorted = false
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
	if b.largeIdxs != nil {
		if cap(b.largeIdxs) > maxBatchPoolCap {
			b.largeIdxs = nil
		} else {
			b.largeIdxs = b.largeIdxs[:0]
		}
	}
	if b.largeKeys != nil {
		clear(b.largeKeys)
		if cap(b.largeKeys) > maxBatchPoolCap {
			b.largeKeys = nil
		} else {
			b.largeKeys = b.largeKeys[:0]
		}
	}
	if b.largeVals != nil {
		clear(b.largeVals)
		if cap(b.largeVals) > maxBatchPoolCap {
			b.largeVals = nil
		} else {
			b.largeVals = b.largeVals[:0]
		}
	}
	b.byteSize = 0
	b.slabWritten = 0
	if b.slabWrittenByID != nil {
		for id := range b.slabWrittenByID {
			delete(b.slabWrittenByID, id)
		}
	}
	if b.slabMaxEndByID != nil {
		for id := range b.slabMaxEndByID {
			delete(b.slabMaxEndByID, id)
		}
	}
	b.sorted = true
	b.assumeSorted = false
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
	b.slabManager = nil
	b.inlineThreshold = 0
	b.slabWrittenByID = nil
	b.slabMaxEndByID = nil
	b.closed = true
	batchPool.Put(b)
}

func (b *Batch) noteKeyOrder(key []byte) {
	if !b.sorted {
		return
	}
	if b.assumeSorted {
		return
	}
	if b.lastKey != nil && bytes.Compare(b.lastKey, key) > 0 {
		b.sorted = false
		return
	}
	b.lastKey = key
}

func (b *Batch) recordSlabWrite(ptr page.ValuePtr) {
	b.slabWritten += int(ptr.Length)
	if b.slabWrittenByID == nil {
		b.slabWrittenByID = make(map[uint32]int64, 4)
	}
	b.slabWrittenByID[ptr.FileID] += int64(ptr.Length)
	if b.slabMaxEndByID == nil {
		b.slabMaxEndByID = make(map[uint32]uint64, 4)
	}
	end := ptr.Offset + uint64(page.ValuePtrRecordLength(ptr))
	if end > b.slabMaxEndByID[ptr.FileID] {
		b.slabMaxEndByID[ptr.FileID] = end
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
		ptr, err := b.slabManager.Append(key, value)
		if err != nil {
			return err
		}
		entry.ValuePtr = ptr
		entry.IsPtr = true
		b.recordSlabWrite(ptr)
	} else {
		entry.Value = value
	}

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
		// Write to slab
		ptr, err := b.slabManager.Append(k, value)
		if err != nil {
			return err
		}
		entry.ValuePtr = ptr
		entry.IsPtr = true
		b.recordSlabWrite(ptr)
	} else {
		// Store inline
		valCopy := make([]byte, len(value))
		copy(valCopy, value)
		entry.Value = valCopy
	}

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
	b.noteKeyOrder(entry.Key)
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
			val, err := b.slabManager.Read(entry.ValuePtr)
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

	// Convert large inline values to slab pointers in bulk to amortize syscalls.
	largeIdx := b.largeIdxs[:0]
	for i := range ops {
		op := &ops[i]
		if op.Type != OpPut || op.IsPtr {
			continue
		}
		if slab.MaxRecordSize > 0 {
			recordLen := int64(slab.HeaderSize) + int64(len(op.Key)) + int64(len(op.Value))
			if recordLen > slab.MaxRecordSize {
				return fmt.Errorf("record too large (key=%d val=%d record=%d max=%d)", len(op.Key), len(op.Value), recordLen, slab.MaxRecordSize)
			}
		}
		if len(op.Value) > b.inlineThreshold {
			largeIdx = append(largeIdx, i)
		}
	}
	b.largeIdxs = largeIdx

	if len(largeIdx) > 0 {
		keys := b.largeKeys
		if cap(keys) < len(largeIdx) {
			keys = make([][]byte, len(largeIdx))
		} else {
			keys = keys[:len(largeIdx)]
		}
		values := b.largeVals
		if cap(values) < len(largeIdx) {
			values = make([][]byte, len(largeIdx))
		} else {
			values = values[:len(largeIdx)]
		}
		for i, idx := range largeIdx {
			keys[i] = ops[idx].Key
			values[i] = ops[idx].Value
		}

		ptrs, err := b.slabManager.AppendMany(keys, values)
		if err != nil {
			if errors.Is(err, slab.ErrRecordTooLarge) {
				maxKey := 0
				maxVal := 0
				maxRecord := int64(0)
				for _, idx := range largeIdx {
					keyLen := len(ops[idx].Key)
					valLen := len(ops[idx].Value)
					if keyLen > maxKey {
						maxKey = keyLen
					}
					if valLen > maxVal {
						maxVal = valLen
					}
					recordLen := int64(slab.HeaderSize) + int64(keyLen) + int64(valLen)
					if recordLen > maxRecord {
						maxRecord = recordLen
					}
				}
				maxV2 := int64(slab.ZoneSize - slab.ZoneHeaderSize)
				log.Printf("treedb: appendmany record too large max_key=%d max_val=%d max_record=%d max_v2=%d count=%d",
					maxKey, maxVal, maxRecord, maxV2, len(largeIdx))
			}
			clear(keys)
			clear(values)
			b.largeKeys = keys[:0]
			b.largeVals = values[:0]
			b.largeIdxs = largeIdx[:0]
			return err
		}

		for i, idx := range largeIdx {
			ptr := ptrs[i]
			op := &ops[idx]
			op.ValuePtr = ptr
			op.IsPtr = true
			op.Value = nil

			b.recordSlabWrite(ptr)
		}

		clear(keys)
		clear(values)
		b.largeKeys = keys[:0]
		b.largeVals = values[:0]
		b.largeIdxs = largeIdx[:0]
	}

	// Just append them. Deduplication happens at Ops() time.
	for _, op := range ops {
		b.noteKeyOrder(op.Key)
		b.entries = append(b.entries, op)
		b.byteSize += len(op.Key) + len(op.Value) // Value is nil for slab pointers.
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
	if b.assumeSorted {
		b.sorted = true
	} else if !b.sorted {
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

// AssumeSorted marks the batch as already sorted, skipping internal sorting.
// Callers must only use this when entries are in non-decreasing key order.
func (b *Batch) AssumeSorted() {
	if b == nil || b.closed {
		return
	}
	if len(b.entries) > 1 {
		for i := 1; i < len(b.entries); i++ {
			if bytes.Compare(b.entries[i-1].Key, b.entries[i].Key) > 0 {
				b.assumeSorted = false
				b.sorted = false
				b.lastKey = nil
				return
			}
		}
	}
	b.assumeSorted = true
	b.sorted = true
	b.lastKey = nil
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

func (b *Batch) ByteSize() int {
	return b.byteSize
}

func (b *Batch) GetByteSize() (int, error) {
	return b.byteSize, nil
}

// SetLastSeq is a dummy implementation to satisfy the Interface.
func (b *Batch) SetLastSeq(uint64) {}

// SlabWriteBytes returns the number of bytes written to the slab file.
func (b *Batch) SlabWriteBytes() int {
	return b.slabWritten
}

// SlabWriteBytesByFile returns a per-slab breakdown of bytes appended during
// this batch. The returned map must be treated as read-only by callers.
func (b *Batch) SlabWriteBytesByFile() map[uint32]int64 {
	return b.slabWrittenByID
}

// SlabWriteMaxEndByFile returns a per-slab maximum end offset (exclusive) written.
// The returned map must be treated as read-only by callers.
func (b *Batch) SlabWriteMaxEndByFile() map[uint32]uint64 {
	return b.slabMaxEndByID
}
