package pebblecompat

import (
	"context"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

// Batch is a Pebble batch wrapper that commits into TreeDB.
type Batch struct {
	db      *DB
	batch   *pebble.Batch
	indexed bool
	closed  bool
}

// NewBatch returns a new compatibility batch.
func (d *DB) NewBatch() *Batch {
	return &Batch{db: d, batch: &pebble.Batch{}}
}

// NewBatchWithSize returns a new compatibility batch.
// The size hint is ignored for now.
func (d *DB) NewBatchWithSize(_ int) *Batch {
	return d.NewBatch()
}

func (b *Batch) ensureOpen() error {
	if b == nil || b.closed || b.batch == nil {
		return ErrClosed
	}
	return nil
}

func (b *Batch) Set(key, value []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.Set(key, value, opts)
}

func (b *Batch) Merge(key, value []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.Merge(key, value, opts)
}

func (b *Batch) Delete(key []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.Delete(key, opts)
}

func (b *Batch) DeleteSized(key []byte, deletedValueSize uint32, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.DeleteSized(key, deletedValueSize, opts)
}

func (b *Batch) SingleDelete(key []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.SingleDelete(key, opts)
}

func (b *Batch) DeleteRange(start, end []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.DeleteRange(start, end, opts)
}

func (b *Batch) RangeKeySet(start, end, suffix, value []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.RangeKeySet(start, end, suffix, value, opts)
}

func (b *Batch) RangeKeyUnset(start, end, suffix []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.RangeKeyUnset(start, end, suffix, opts)
}

func (b *Batch) RangeKeyDelete(start, end []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.RangeKeyDelete(start, end, opts)
}

func (b *Batch) LogData(data []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.LogData(data, opts)
}

// Apply appends another batch's operations.
func (b *Batch) Apply(other *Batch, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if other == nil || other.batch == nil {
		return nil
	}
	return b.batch.Apply(other.batch, opts)
}

// Commit applies the batch to the underlying DB.
func (b *Batch) Commit(opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if b.db == nil {
		return ErrClosed
	}
	return b.db.Apply(b, opts)
}

// SyncWait is a compatibility no-op.
func (b *Batch) SyncWait() error {
	return nil
}

// CommitStats returns Pebble batch commit stats when available.
func (b *Batch) CommitStats() pebble.BatchCommitStats {
	if b == nil || b.batch == nil {
		return pebble.BatchCommitStats{}
	}
	return b.batch.CommitStats()
}

// AddInternalKey appends an internal-key record to the batch.
func (b *Batch) AddInternalKey(key *pebble.InternalKey, value []byte, opts *pebble.WriteOptions) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if key == nil {
		return errors.New("pebblecompat: nil internal key")
	}
	return b.batch.AddInternalKey(key, value, opts)
}

// Repr returns a stable copy of the Pebble batch representation.
func (b *Batch) Repr() []byte {
	if b == nil || b.batch == nil {
		return nil
	}
	repr := b.batch.Repr()
	return append([]byte(nil), repr...)
}

// SetRepr sets the batch representation.
func (b *Batch) SetRepr(repr []byte) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.batch.SetRepr(repr)
}

// Reader returns a Pebble batch reader over this batch.
func (b *Batch) Reader() pebble.BatchReader {
	if b == nil || b.batch == nil {
		return nil
	}
	return b.batch.Reader()
}

// Indexed returns whether this is an indexed batch.
func (b *Batch) Indexed() bool {
	if b == nil {
		return false
	}
	return b.indexed
}

// Get reads through the batch overlay when the batch is indexed.
func (b *Batch) Get(key []byte) ([]byte, io.Closer, error) {
	if err := b.ensureOpen(); err != nil {
		return nil, nil, err
	}
	if !b.indexed {
		return nil, nil, pebble.ErrNotIndexed
	}
	return b.batch.Get(key)
}

// NewIter returns an iterator over the batch view when indexed.
func (b *Batch) NewIter(o *pebble.IterOptions) (*pebble.Iterator, error) {
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}
	if !b.indexed {
		return nil, pebble.ErrNotIndexed
	}
	return b.batch.NewIter(o)
}

// NewIterWithContext returns an iterator over the batch view when indexed.
func (b *Batch) NewIterWithContext(ctx context.Context, o *pebble.IterOptions) *pebble.Iterator {
	if err := b.ensureOpen(); err != nil {
		return nil
	}
	return b.batch.NewIterWithContext(ctx, o)
}

func (b *Batch) Len() int {
	if b == nil || b.batch == nil {
		return 0
	}
	return b.batch.Len()
}

func (b *Batch) Count() uint32 {
	if b == nil || b.batch == nil {
		return 0
	}
	return b.batch.Count()
}

func (b *Batch) Empty() bool {
	if b == nil || b.batch == nil {
		return true
	}
	return b.batch.Empty()
}

func (b *Batch) Reset() {
	if b == nil || b.batch == nil {
		return
	}
	b.batch.Reset()
}

func (b *Batch) Close() error {
	if b == nil || b.closed {
		return nil
	}
	b.closed = true
	if b.batch == nil {
		return nil
	}
	return b.batch.Close()
}
