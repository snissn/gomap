package treedb

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

		"github.com/snissn/gomap/TreeDB/internal/page"
		"github.com/snissn/gomap/TreeDB/internal/slab"
		"github.com/snissn/gomap/TreeDB/internal/tree"
	)
	
	var (
		// ErrBatchClosed is returned when the batch is closed.
		ErrBatchClosed = errors.New("batch has been written or closed")
		// ErrKeyEmpty is returned when the key is empty.
		ErrKeyEmpty = errors.New("key cannot be empty")
		// ErrValueNil is returned when the value is nil.
		ErrValueNil = errors.New("value cannot be nil")
	)
type batchState uint8

const (
	batchOpen batchState = iota
	batchWritten
	batchClosed
)

type batchOp struct {
	key    []byte
	value  []byte
	del    bool
	ptr    page.ValuePtr
	inline bool
}

// Batch implements the batch interface.
type Batch struct {
	db *DB

	ops map[string]*batchOp
	// stable ordering for merge.
	keys [][]byte

	byteSize int
	state    atomic.Uint32
}

func newBatch(db *DB, sizeHint int) *Batch {
	capHint := 0
	if sizeHint > 0 {
		capHint = sizeHint
	}
	return &Batch{
		db:  db,
		ops: make(map[string]*batchOp, capHint),
	}
}

func (b *Batch) ensureOpen() error {
	if b == nil {
		return ErrBatchClosed
	}
	st := batchState(b.state.Load())
	if st != batchOpen {
		return ErrBatchClosed
	}
	return nil
}

// Set adds or replaces a key/value operation.
func (b *Batch) Set(key, value []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil || len(key) == 0 {
		return ErrKeyEmpty
	}
	if value == nil {
		return ErrValueNil
	}
	k := string(key)
	if prev, ok := b.ops[k]; ok {
		b.byteSize -= len(prev.key)
		if !prev.del {
			b.byteSize -= len(prev.value)
		}
	}
	op := &batchOp{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		del:   false,
	}
	b.ops[k] = op
	b.byteSize += len(op.key) + len(op.value)
	return nil
}

// Delete adds or replaces a delete operation.
func (b *Batch) Delete(key []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil || len(key) == 0 {
		return ErrKeyEmpty
	}
	k := string(key)
	if prev, ok := b.ops[k]; ok {
		b.byteSize -= len(prev.key)
		if !prev.del {
			b.byteSize -= len(prev.value)
		}
	}
	op := &batchOp{
		key: append([]byte(nil), key...),
		del: true,
	}
	b.ops[k] = op
	b.byteSize += len(op.key)
	return nil
}

// Write commits the batch without durability guarantee.
func (b *Batch) Write() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if err := b.db.writeBatch(b, false); err != nil {
		return err
	}
	b.state.Store(uint32(batchWritten))
	return nil
}

// WriteSync commits the batch and flushes durability boundary.
func (b *Batch) WriteSync() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if err := b.db.writeBatch(b, true); err != nil {
		return err
	}
	b.state.Store(uint32(batchWritten))
	return nil
}

// Close closes the batch. It is idempotent.
func (b *Batch) Close() error {
	if b == nil {
		return nil
	}
	for {
		old := batchState(b.state.Load())
		if old == batchClosed {
			return nil
		}
		if b.state.CompareAndSwap(uint32(old), uint32(batchClosed)) {
			return nil
		}
	}
}

// GetByteSize returns the current batch size in bytes.
func (b *Batch) GetByteSize() (int, error) {
	if b == nil {
		return 0, ErrBatchClosed
	}
	st := batchState(b.state.Load())
	if st != batchOpen {
		return 0, ErrBatchClosed
	}
	return b.byteSize, nil
}

// prepare prewrites large values and returns sorted keys.
// observe, when non-nil, is called once per op with the threshold used.
func (b *Batch) prepare(inlineThreshold int, mgr *slab.SlabManager, observe func(int)) ([][]byte, map[uint32]struct{}, uint64, error) {
	if len(b.ops) == 0 {
		return nil, nil, 0, nil
	}
	keys := make([][]byte, 0, len(b.ops))
	modSlabs := make(map[uint32]struct{})
	var slabWriteBytes uint64
	for _, op := range b.ops {
		if op == nil {
			continue
		}
		if observe != nil {
			observe(inlineThreshold)
		}
		keys = append(keys, op.key)
		if op.del {
			continue
		}
		if len(op.value) > page.InlineHardMax || len(op.value) > inlineThreshold {
			ptr, err := mgr.AppendLarge(op.key, op.value)
			if err != nil {
				return nil, nil, 0, err
			}
			op.ptr = ptr
			op.inline = false
			op.value = nil // allow GC of large values
			modSlabs[ptr.FileID] = struct{}{}
			slabWriteBytes += uint64(4 + ptr.Length)
		} else {
			op.inline = true
		}
	}
	sort.Slice(keys, func(i, j int) bool { return compareKeys(keys[i], keys[j]) < 0 })
	b.keys = keys
	return keys, modSlabs, slabWriteBytes, nil
}

func (op *batchOp) leafEntry() tree.LeafEntry {
	if op.del {
		return tree.LeafEntry{Flags: page.LeafFlagTombstone}
	}
	if op.inline {
		return tree.LeafEntry{Flags: page.LeafFlagInline, InlineValue: op.value}
	}
	return tree.LeafEntry{Flags: page.LeafFlagPointer, Ptr: op.ptr}
}

func (b *Batch) String() string {
	if b == nil {
		return "<nil batch>"
	}
	return fmt.Sprintf("Batch{ops=%d,size=%d,state=%d}", len(b.ops), b.byteSize, b.state.Load())
}
