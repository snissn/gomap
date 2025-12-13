package batch

import (
	"errors"

	"github.com/snissn/gomap-gemini/TreeDB/page"
	"github.com/snissn/gomap-gemini/TreeDB/slab"
)

var (
	ErrKeyEmpty = errors.New("key cannot be empty")
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
	closed bool
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

// Ops returns the map of operations.
func (b *Batch) Ops() map[string]Entry {
	return b.ops
}

// ByteSize returns the approximate size of the batch.
func (b *Batch) ByteSize() int {
	return b.byteSize
}