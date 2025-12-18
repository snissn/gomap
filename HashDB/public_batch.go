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

func (h *HashDB) NewBatch() *Batch   { return &Batch{applier: h} }
func (h *DB) NewBatch() *Batch       { return &Batch{applier: h} }
func (c *CachedDB) NewBatch() *Batch { return &Batch{applier: c} }

func (b *Batch) Set(key, value []byte) error {
	if b.closed {
		return nil
	}
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, PutOp(k, v))
	return nil
}

func (b *Batch) Delete(key []byte) error {
	if b.closed {
		return nil
	}
	k := append([]byte(nil), key...)
	b.ops = append(b.ops, DeleteOp(k))
	return nil
}

func (b *Batch) Commit() error {
	if b.closed {
		return nil
	}
	ops := b.ops
	b.ops = nil
	return b.applier.ApplyBatch(ops)
}

func (b *Batch) CommitSync() error {
	if b.closed {
		return nil
	}
	ops := b.ops
	b.ops = nil
	return b.applier.ApplyBatchSync(ops)
}

func (b *Batch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}
