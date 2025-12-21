package batch

import (
	"bytes"
	"errors"
	"sort"

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
	slabManager     *slab.SlabManager
	byteSize        int
	slabWritten     int
	slabWrittenByID map[uint32]int64
	inlineThreshold int
	sorted          bool
	lastKey         []byte
	closed          bool
}

// New creates a new Batch.
func New(sm *slab.SlabManager, threshold int) *Batch {
	if threshold <= 0 {
		threshold = page.DefaultInlineThreshold
	}
	return &Batch{
		entries:         make([]Entry, 0, 16),
		slabManager:     sm,
		inlineThreshold: threshold,
		sorted:          true,
	}
}

func (b *Batch) ensureOpen() error {
	if b.closed {
		return ErrBatchClosed
	}
	return nil
}

// Reset clears the batch for reuse without releasing its internal buffers.
func (b *Batch) Reset() {
	if b == nil {
		return
	}
	if b.closed {
		// Keep behavior simple: Reset is only valid on an open batch.
		return
	}
	if b.entries != nil {
		b.entries = b.entries[:0]
	}
	b.byteSize = 0
	b.slabWritten = 0
	if b.slabWrittenByID != nil {
		for id := range b.slabWrittenByID {
			delete(b.slabWrittenByID, id)
		}
	}
	b.sorted = true
	b.lastKey = nil
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
		b.slabWritten += int(ptr.Length)
		if b.slabWrittenByID == nil {
			b.slabWrittenByID = make(map[uint32]int64, 4)
		}
		b.slabWrittenByID[ptr.FileID] += int64(ptr.Length)
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
		b.slabWritten += int(ptr.Length)
		if b.slabWrittenByID == nil {
			b.slabWrittenByID = make(map[uint32]int64, 4)
		}
		b.slabWrittenByID[ptr.FileID] += int64(ptr.Length)
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
	if len(key) == 0 {
		return errors.New("key cannot be empty")
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

	// Just append them. Deduplication happens at Ops() time.
	for _, op := range ops {
		b.noteKeyOrder(op.Key)
		// Logic to handle large values if op came from raw map?
		// Assuming op is already processed or we need to check.

		if op.Type == OpDelete || op.IsPtr {
			b.entries = append(b.entries, op)
			b.byteSize += len(op.Key) + len(op.Value) // Value might be nil if Ptr
			continue
		}

		// Check threshold for Put with Inline value
		if len(op.Value) > b.inlineThreshold {
			ptr, err := b.slabManager.Append(op.Key, op.Value)
			if err != nil {
				return err
			}
			op.ValuePtr = ptr
			op.IsPtr = true
			op.Value = nil
			b.slabWritten += int(ptr.Length)
			if b.slabWrittenByID == nil {
				b.slabWrittenByID = make(map[uint32]int64, 4)
			}
			b.slabWrittenByID[ptr.FileID] += int64(ptr.Length)
		}
		// No need to copy op.Value if we assume ownership (which we do for SetOps)

		b.entries = append(b.entries, op)
		b.byteSize += len(op.Key) + len(op.Value)
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

// SlabWriteBytes returns the number of bytes written to the slab file.
func (b *Batch) SlabWriteBytes() int {
	return b.slabWritten
}

// SlabWriteBytesByFile returns a per-slab breakdown of bytes appended during
// this batch. The returned map must be treated as read-only by callers.
func (b *Batch) SlabWriteBytesByFile() map[uint32]int64 {
	return b.slabWrittenByID
}
