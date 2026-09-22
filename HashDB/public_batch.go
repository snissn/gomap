package hashdb

// Batch buffers mutations and applies them via ApplyBatch / ApplyBatchSync.
//
// Notes:
//   - For the sharded HashDB type, commits are atomic per shard but not across shards.
//   - Keys and values are copied into the batch so callers may reuse buffers safely.
type Batch struct {
	applier batchApplier
	ops     []BatchOp
	closed  bool
}

type batchApplier interface {
	ApplyBatch(ops []BatchOp) error
	ApplyBatchSync(ops []BatchOp) error
}

// NewBatch returns a new batch for the sharded HashDB.
func (h *HashDB) NewBatch() *Batch { return &Batch{applier: h} }

// NewBatch returns a new batch for the single-shard DB.
func (h *DB) NewBatch() *Batch { return &Batch{applier: h} }

// NewBatch returns a new batch for the cached DB wrapper.
func (c *CachedDB) NewBatch() *Batch { return &Batch{applier: c} }

// Set adds a put operation to the batch.
func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return nil
	}
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, PutOp(k, v))
	return nil
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key []byte) error {
	if b.closed {
		return nil
	}
	k := append([]byte(nil), key...)
	b.ops = append(b.ops, DeleteOp(k))
	return nil
}

// Commit applies the batch without forcing durability.
func (b *Batch) Commit() error {
	if b.closed {
		return nil
	}
	ops := b.ops
	b.ops = nil
	return b.applier.ApplyBatch(ops)
}

// CommitSync applies the batch and forces durability.
func (b *Batch) CommitSync() error {
	if b.closed {
		return nil
	}
	ops := b.ops
	b.ops = nil
	return b.applier.ApplyBatchSync(ops)
}

// Close releases batch resources and prevents further use.
func (b *Batch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}
