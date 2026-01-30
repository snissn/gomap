package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"hash/maphash"

	"github.com/snissn/gomap/kvstore"
)

type shardedDB struct {
	name   string
	shards []kvstore.DB
	seed   maphash.Seed
}

func newShardedDB(name string, shards []kvstore.DB) *shardedDB {
	return &shardedDB{
		name:   name,
		shards: shards,
		seed:   maphash.MakeSeed(),
	}
}

func (s *shardedDB) Name() string {
	return fmt.Sprintf("%s[shards=%d]", s.name, len(s.shards))
}

func (s *shardedDB) Close() error {
	var firstErr error
	for _, shard := range s.shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *shardedDB) shardIndex(key []byte) int {
	if len(s.shards) == 0 {
		return 0
	}
	if len(s.shards) == 1 {
		return 0
	}
	h := maphash.Bytes(s.seed, key)
	return int(h % uint64(len(s.shards)))
}

func (s *shardedDB) shardForKey(key []byte) kvstore.DB {
	return s.shards[s.shardIndex(key)]
}

func (s *shardedDB) Get(key []byte) ([]byte, error) {
	return s.shardForKey(key).Get(key)
}

func (s *shardedDB) Set(key, value []byte) error {
	return s.shardForKey(key).Set(key, value)
}

func (s *shardedDB) Delete(key []byte) error {
	return s.shardForKey(key).Delete(key)
}

func (s *shardedDB) Has(key []byte) (bool, error) {
	shard := s.shardForKey(key)
	hs, ok := shard.(kvstore.Haser)
	if !ok {
		return false, kvstore.ErrUnsupported
	}
	return hs.Has(key)
}

func (s *shardedDB) SetSync(key, value []byte) error {
	shard := s.shardForKey(key)
	syncer, ok := shard.(kvstore.Syncer)
	if !ok {
		return kvstore.ErrUnsupported
	}
	return syncer.SetSync(key, value)
}

func (s *shardedDB) DeleteSync(key []byte) error {
	shard := s.shardForKey(key)
	syncer, ok := shard.(kvstore.Syncer)
	if !ok {
		return kvstore.ErrUnsupported
	}
	return syncer.DeleteSync(key)
}

func (s *shardedDB) Stats() map[string]string {
	stats := make(map[string]string)
	for i, shard := range s.shards {
		sp, ok := shard.(kvstore.StatsProvider)
		if !ok {
			continue
		}
		for k, v := range sp.Stats() {
			stats[fmt.Sprintf("shard.%d.%s", i, k)] = v
		}
	}
	if len(stats) == 0 {
		return nil
	}
	return stats
}

func (s *shardedDB) Print() error {
	var firstErr error
	for _, shard := range s.shards {
		pr, ok := shard.(kvstore.Printer)
		if !ok {
			return kvstore.ErrUnsupported
		}
		if err := pr.Print(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *shardedDB) NewBatch() (kvstore.Batch, error) {
	for _, shard := range s.shards {
		if _, ok := shard.(kvstore.Batcher); !ok {
			return nil, kvstore.ErrUnsupported
		}
	}
	return &shardedBatch{
		shards:   s.shards,
		shardFor: s.shardIndex,
		batches:  make([]kvstore.Batch, len(s.shards)),
	}, nil
}

func (s *shardedDB) ForEach(fn func(key, value []byte) error) error {
	for _, shard := range s.shards {
		if fe, ok := shard.(kvstore.ForEacher); ok {
			if err := fe.ForEach(fn); err != nil {
				return err
			}
			continue
		}
		if rs, ok := shard.(kvstore.RangeScanner); ok {
			iter, err := rs.Iterator(nil, nil)
			if err != nil {
				return err
			}
			for iter.Valid() {
				if err := fn(iter.Key(), iter.Value()); err != nil {
					_ = iter.Close()
					return err
				}
				iter.Next()
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				return err
			}
			if err := iter.Close(); err != nil {
				return err
			}
			continue
		}
		return kvstore.ErrUnsupported
	}
	return nil
}

func (s *shardedDB) Iterator(start, end []byte) (kvstore.Iterator, error) {
	iters := make([]kvstore.Iterator, 0, len(s.shards))
	h := &shardIterHeap{reverse: false}
	for _, shard := range s.shards {
		rs, ok := shard.(kvstore.RangeScanner)
		if !ok {
			return nil, kvstore.ErrUnsupported
		}
		iter, err := rs.Iterator(start, end)
		if err != nil {
			return nil, err
		}
		if iter.Valid() {
			keyCopy := iter.KeyCopy(nil)
			heap.Push(h, &shardIter{iter: iter, key: keyCopy})
		}
		iters = append(iters, iter)
	}
	return newShardedIterator(h, iters), nil
}

func (s *shardedDB) ReverseIterator(start, end []byte) (kvstore.Iterator, error) {
	iters := make([]kvstore.Iterator, 0, len(s.shards))
	h := &shardIterHeap{reverse: true}
	for _, shard := range s.shards {
		rs, ok := shard.(kvstore.RangeScanner)
		if !ok {
			return nil, kvstore.ErrUnsupported
		}
		iter, err := rs.ReverseIterator(start, end)
		if err != nil {
			return nil, err
		}
		if iter.Valid() {
			keyCopy := iter.KeyCopy(nil)
			heap.Push(h, &shardIter{iter: iter, key: keyCopy})
		}
		iters = append(iters, iter)
	}
	return newShardedIterator(h, iters), nil
}

func (s *shardedDB) Checkpoint() error {
	var firstErr error
	for _, shard := range s.shards {
		cp, ok := shard.(checkpointer)
		if !ok {
			return kvstore.ErrUnsupported
		}
		if err := cp.Checkpoint(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type shardedBatch struct {
	shards   []kvstore.DB
	batches  []kvstore.Batch
	shardFor func([]byte) int
}

func (b *shardedBatch) batchForKey(key []byte) (kvstore.Batch, error) {
	idx := b.shardFor(key)
	if b.batches[idx] != nil {
		return b.batches[idx], nil
	}
	batcher, ok := b.shards[idx].(kvstore.Batcher)
	if !ok {
		return nil, kvstore.ErrUnsupported
	}
	batch, err := batcher.NewBatch()
	if err != nil {
		return nil, err
	}
	b.batches[idx] = batch
	return batch, nil
}

func (b *shardedBatch) Set(key, value []byte) error {
	batch, err := b.batchForKey(key)
	if err != nil {
		return err
	}
	return batch.Set(key, value)
}

func (b *shardedBatch) Delete(key []byte) error {
	batch, err := b.batchForKey(key)
	if err != nil {
		return err
	}
	return batch.Delete(key)
}

func (b *shardedBatch) Commit() error {
	var firstErr error
	for _, batch := range b.batches {
		if batch == nil {
			continue
		}
		if err := batch.Commit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *shardedBatch) CommitSync() error {
	var firstErr error
	for _, batch := range b.batches {
		if batch == nil {
			continue
		}
		if err := batch.CommitSync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *shardedBatch) Close() error {
	var firstErr error
	for i, batch := range b.batches {
		if batch == nil {
			continue
		}
		if err := batch.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		b.batches[i] = nil
	}
	return firstErr
}

type shardIter struct {
	iter kvstore.Iterator
	key  []byte
}

type shardIterHeap struct {
	items   []*shardIter
	reverse bool
}

func (h shardIterHeap) Len() int { return len(h.items) }

func (h shardIterHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h.items[i].key, h.items[j].key)
	if h.reverse {
		return cmp > 0
	}
	return cmp < 0
}

func (h shardIterHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *shardIterHeap) Push(x any) {
	h.items = append(h.items, x.(*shardIter))
}

func (h *shardIterHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

type shardedIterator struct {
	heap    *shardIterHeap
	current *shardIter
	iters   []kvstore.Iterator
}

func newShardedIterator(h *shardIterHeap, iters []kvstore.Iterator) *shardedIterator {
	heap.Init(h)
	s := &shardedIterator{
		heap:  h,
		iters: iters,
	}
	s.advance()
	return s
}

func (s *shardedIterator) advance() {
	if s.current != nil {
		s.current.iter.Next()
		if s.current.iter.Valid() {
			s.current.key = s.current.iter.KeyCopy(s.current.key[:0])
			heap.Push(s.heap, s.current)
		}
		s.current = nil
	}
	if s.heap.Len() == 0 {
		return
	}
	s.current = heap.Pop(s.heap).(*shardIter)
}

func (s *shardedIterator) Valid() bool {
	return s.current != nil && s.current.iter.Valid()
}

func (s *shardedIterator) Next() {
	s.advance()
}

func (s *shardedIterator) Key() []byte {
	if s.current == nil {
		return nil
	}
	return s.current.iter.Key()
}

func (s *shardedIterator) Value() []byte {
	if s.current == nil {
		return nil
	}
	return s.current.iter.Value()
}

func (s *shardedIterator) KeyCopy(dst []byte) []byte {
	if s.current == nil {
		return nil
	}
	return s.current.iter.KeyCopy(dst)
}

func (s *shardedIterator) ValueCopy(dst []byte) []byte {
	if s.current == nil {
		return nil
	}
	return s.current.iter.ValueCopy(dst)
}

func (s *shardedIterator) Error() error {
	for _, iter := range s.iters {
		if err := iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (s *shardedIterator) Close() error {
	var firstErr error
	for _, iter := range s.iters {
		if err := iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
