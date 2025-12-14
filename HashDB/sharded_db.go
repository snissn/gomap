package hashdb

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// ShardedDB is a thread-safe, sharded key/value store implementation.
// It partitions keys across multiple underlying DB instances (shards) to
// maximize concurrency.
type ShardedDB struct {
	shards []*CachedDB
	locks  []sync.RWMutex
}

// HashmapDistributed is kept as a compatibility alias for older code.
// New code should use ShardedDB.
type HashmapDistributed = ShardedDB

// New initializes the sharded store with storage in the specified folder.
// It creates sub-directories for each partition.
func (h *ShardedDB) New(folder string) error {
	// 128 shards provides excellent balance for high concurrency
	return h.NewWithShards(folder, 128)
}

// Open is a compatibility wrapper for older code.
func (h *ShardedDB) Open(folder string) error {
	return h.New(folder)
}

// NewWithShards initializes the sharded store with a specific number of shards.
func (h *ShardedDB) NewWithShards(folder string, numShards int) error {
	if numShards <= 0 {
		numShards = runtime.NumCPU()
	}

	h.shards = make([]*CachedDB, numShards)
	h.locks = make([]sync.RWMutex, numShards)

	for i := 0; i < numShards; i++ {
		partitionFolder := fmt.Sprintf("%s/partition-%d", folder, i)
		if err := os.MkdirAll(partitionFolder, 0o755); err != nil {
			return fmt.Errorf("failed to create directory for partition: %w", err)
		}

		cached, err := NewCachedDB(partitionFolder, 4096, 4<<20, 2*time.Second)
		if err != nil {
			return err
		}
		h.shards[i] = cached
	}

	return nil
}

// OpenWithShards is a compatibility wrapper for older code.
func (h *ShardedDB) OpenWithShards(folder string, numShards int) error {
	return h.NewWithShards(folder, numShards)
}

// SetCompression enables or disables value compression on all shards.
// It should typically be called during initialization before serving traffic.
func (h *ShardedDB) SetCompression(enabled bool) {
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		h.shards[i].SetCompression(enabled)
		h.locks[i].Unlock()
	}
}

// Get retrieves the value for a given key.
// It returns nil if the key does not exist.
func (h *ShardedDB) Get(key []byte) ([]byte, error) {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].RLock()
	defer h.locks[shardIndex].RUnlock()
	return h.shards[shardIndex].Get(key)
}

// Put inserts or updates a key-value pair.
func (h *ShardedDB) Put(key []byte, value []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].Put(key, value)
}

// Add is a compatibility wrapper for older code.
func (h *ShardedDB) Add(key []byte, value []byte) error {
	return h.Put(key, value)
}

// Delete removes a key from the map.
func (h *ShardedDB) Delete(key []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].Delete(key)
}

// Update performs an atomic read-modify-write operation on a key.
func (h *ShardedDB) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].Update(key, callback)
}

// Clear wipes all data from all shards.
func (h *ShardedDB) Clear() error {
	var (
		errOnce   sync.Once
		errGlobal error
	)
	var wg sync.WaitGroup
	for i := 0; i < len(h.shards); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			h.locks[index].Lock()
			defer h.locks[index].Unlock()
			if err := h.shards[index].Clear(); err != nil {
				errOnce.Do(func() { errGlobal = err })
			}
		}(i)
	}
	wg.Wait()
	return errGlobal
}

// Stats collects and aggregates stats from all shards.
func (h *ShardedDB) Stats() Stats {
	var total Stats
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].RLock()
		s := h.shards[i].Stats()
		h.locks[i].RUnlock()

		total.KeyCount += s.KeyCount
		total.Capacity += s.Capacity
		total.DataSize += s.DataSize
		total.Segments += s.Segments
	}
	return total
}

// Compact triggers garbage collection on all shards.
func (h *ShardedDB) Compact() error {
	var (
		errOnce   sync.Once
		errGlobal error
	)
	var wg sync.WaitGroup
	for i := 0; i < len(h.shards); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			h.locks[index].Lock()
			defer h.locks[index].Unlock()
			if err := h.shards[index].Compact(); err != nil {
				errOnce.Do(func() { errGlobal = err })
			}
		}(i)
	}
	wg.Wait()
	return errGlobal
}

// Flush forces all shard-level write-back caches to flush pending writes.
// This is important before process exit or reopening the same on-disk store
// to ensure durability of recent writes.
func (h *ShardedDB) Flush() error {
	var errGlobal error
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		if h.shards[i] != nil {
			if err := h.shards[i].Flush(); err != nil && errGlobal == nil {
				errGlobal = err
			}
		}
		h.locks[i].Unlock()
	}
	return errGlobal
}

// Close flushes and closes all shards.
// It is not safe to call Close concurrently with other operations.
func (h *ShardedDB) Close() error {
	var firstErr error
	for i := range h.shards {
		if i < len(h.locks) {
			h.locks[i].Lock()
		}
		shard := h.shards[i]
		h.shards[i] = nil
		if i < len(h.locks) {
			h.locks[i].Unlock()
		}

		if shard == nil {
			continue
		}
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	h.shards = nil
	h.locks = nil
	return firstErr
}

// GetMany retrieves values for multiple keys efficiently by grouping them per shard.
// It returns a slice of values aligned with the input keys slice; missing keys map to nil.
// Errors are returned per key; nil error means the operation for that key succeeded (even if value is nil).
func (h *ShardedDB) GetMany(keys [][]byte) ([][]byte, []error) {
	numShards := len(h.shards)
	if numShards == 0 {
		return make([][]byte, len(keys)), make([]error, len(keys))
	}

	shardedIndexes := make([][]int, numShards)
	for i, key := range keys {
		hash := hash(key)
		mapIndex := hash % Hash(numShards)
		shardedIndexes[mapIndex] = append(shardedIndexes[mapIndex], i)
	}

	values := make([][]byte, len(keys))
	errs := make([]error, len(keys))

	var wg sync.WaitGroup
	for shardIdx, idxs := range shardedIndexes {
		if len(idxs) == 0 {
			continue
		}
		wg.Add(1)
		go func(shard int, idxs []int) {
			defer wg.Done()
			h.locks[shard].RLock()
			defer h.locks[shard].RUnlock()

			m := h.shards[shard]
			for _, keyIndex := range idxs {
				val, err := m.Get(keys[keyIndex])
				if err != nil {
					errs[keyIndex] = err
				} else {
					values[keyIndex] = val
				}
			}
		}(shardIdx, idxs)
	}
	wg.Wait()

	return values, errs
}

// PutMany inserts multiple key-value pairs efficiently.
// It buckets items by shard and performs parallel insertion.
func (h *ShardedDB) PutMany(items []Item) error {
	numShards := len(h.shards)
	shardedItems := make([][]Item, numShards)
	for i := 0; i < numShards; i++ {
		shardedItems[i] = make([]Item, 0, len(items)/numShards)
	}

	for _, item := range items {
		hash := hash(item.Key)
		mapIndex := hash % Hash(numShards)
		shardedItems[mapIndex] = append(shardedItems[mapIndex], item)
	}

	var wg sync.WaitGroup
	var errGlobal error
	var errOnce sync.Once

	for i := 0; i < numShards; i++ {
		if len(shardedItems[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(index int, items []Item) {
			defer wg.Done()
			h.locks[index].Lock()
			defer h.locks[index].Unlock()
			err := h.shards[index].PutMany(items)
			if err != nil {
				errOnce.Do(func() {
					errGlobal = err
				})
			}
		}(i, shardedItems[i])
	}
	wg.Wait()

	return errGlobal
}

// AddMany is a compatibility wrapper for older code.
func (h *ShardedDB) AddMany(items []Item) error {
	return h.PutMany(items)
}
