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

// Batch accumulates writes and deletes before committing them.
type Batch struct {
	ops             map[string]Entry
	slabManager     *slab.SlabManager
	byteSize        int
	inlineThreshold int
	closed          bool
}

// New creates a new Batch.
func New(sm *slab.SlabManager, threshold int) *Batch {
	if threshold <= 0 {
		threshold = page.DefaultInlineThreshold
	}
	return &Batch{
		ops:             make(map[string]Entry),
		slabManager:     sm,
		inlineThreshold: threshold,
	}
}

func (b *Batch) ensureOpen() error {
	if b.closed {
		return ErrBatchClosed
	}
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

	entry := Entry{
		Type: OpPut,
		Key:  key,
	}

	// Check threshold
	if len(value) > b.inlineThreshold {
		// Write to slab
		ptr, err := b.slabManager.Append(key, value)
		if err != nil {
			return err
		}
		entry.ValuePtr = ptr
		entry.IsPtr = true
	} else {
		// Store inline
		valCopy := make([]byte, len(value))
		copy(valCopy, value)
		entry.Value = valCopy
	}

	b.ops[string(key)] = entry
	// Approximate size tracking (optional for now)
	b.byteSize += len(key) + len(value)
	return nil
}

// SetPointer adds a pointer directly to the batch (used by Compaction).
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	entry := Entry{
		Type:     OpPut,
		Key:      key,
		ValuePtr: ptr,
		IsPtr:    true,
	}
	b.ops[string(key)] = entry
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

	b.ops[string(key)] = Entry{
		Type: OpDelete,
		Key:  key,
	}
	b.byteSize += len(key)
	return nil
}

// SetOps merges a map of operations into the batch.
func (b *Batch) SetOps(ops map[string]Entry) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	entries := make([]Entry, 0, len(ops))
	for _, op := range ops {
		entries = append(entries, op)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	for _, op := range entries {
		// If op is already populated with ValuePtr, we trust it?
		// But CachingDB doesn't populate ValuePtr.
		// So we must check value size again.

		if op.Type == OpDelete {
			b.ops[string(op.Key)] = op
			b.byteSize += len(op.Key)
			continue
		}

		// OpPut
		// If it came from CachingDB, it has Value but maybe not ValuePtr.
		if op.IsPtr {
			// Already has pointer (e.g. from compaction or advanced usage)
			b.ops[string(op.Key)] = op
			continue
		}

		key := op.Key
		value := op.Value

		// Check threshold
		if len(value) > b.inlineThreshold {
			// Write to slab
			ptr, err := b.slabManager.Append(key, value)
			if err != nil {
				return err
			}
			op.ValuePtr = ptr
			op.IsPtr = true
			op.Value = nil // Clear inline
		} else {
			// Ensure inline copy?
			// If ops came from another batch, value might be shared?
			// But CachingDB creates fresh batch.
			// Let's copy to be safe if we want independent batch lifecycle,
			// but for performance we might skip copy if we know ownership transfers.
			// Standard Set() does copy. Let's do copy.
			valCopy := make([]byte, len(value))
			copy(valCopy, value)
			op.Value = valCopy
		}
		b.ops[string(op.Key)] = op
		b.byteSize += len(key) + len(value)
	}
	return nil
}

// Ops returns the map of operations.
func (b *Batch) Ops() map[string]Entry {
	return b.ops
}

// ByteSize returns the approximate size of the batch.
func (b *Batch) ByteSize() int {
	return b.byteSize
}
