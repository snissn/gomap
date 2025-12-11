package gomap

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// HashmapDistributed is a thread-safe, sharded hash map implementation.
// It partitions keys across multiple underlying Hashmap instances (shards)
// based on the number of available CPU cores to maximize concurrency.
type HashmapDistributed struct {
	maps    []*CachedHashmap
	mutexes []sync.RWMutex
}

// New initializes the distributed hash map with storage in the specified folder.
// It creates sub-directories for each partition.
func (h *HashmapDistributed) New(folder string) error {
	// 128 shards provides excellent balance for high concurrency
	return h.NewWithShards(folder, 128)
}

// NewWithShards initializes the distributed hash map with a specific number of shards.
func (h *HashmapDistributed) NewWithShards(folder string, numShards int) error {
	if numShards <= 0 {
		numShards = runtime.NumCPU()
	}

	// Initialize the slice of Hashmap pointers and mutexes
	h.maps = make([]*CachedHashmap, numShards)
	h.mutexes = make([]sync.RWMutex, numShards)

	// Create a new Hashmap for each Shard
	for i := 0; i < numShards; i++ {
		partitionFolder := fmt.Sprintf("%s/partition-%d", folder, i)
		err := os.MkdirAll(partitionFolder, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory for partition: %w", err)
		}

		cached, err := NewCachedHashmap(partitionFolder, 4096, 4<<20, 2*time.Second)
		if err != nil {
			return err
		}
		h.maps[i] = cached
	}

	return nil
}

// SetCompression enables or disables value compression on all shards.
// It should typically be called during initialization before serving traffic.
func (h *HashmapDistributed) SetCompression(enabled bool) {
	for i := 0; i < len(h.maps); i++ {
		h.mutexes[i].Lock()
		h.maps[i].SetCompression(enabled)
		h.mutexes[i].Unlock()
	}
}

// Get retrieves the value for a given key.
// It returns nil if the key does not exist.
func (h *HashmapDistributed) Get(key []byte) ([]byte, error) {
	hash := hash(key)
	mapIndex := hash % Hash(len(h.maps))
	h.mutexes[mapIndex].RLock()         // lock for reading
	defer h.mutexes[mapIndex].RUnlock() // unlock after reading
	return h.maps[mapIndex].Get(key)
}

// Add inserts or updates a key-value pair.
func (h *HashmapDistributed) Add(key []byte, value []byte) error {
	hash := hash(key)
	mapIndex := hash % Hash(len(h.maps))
	h.mutexes[mapIndex].Lock()         // lock for writing
	defer h.mutexes[mapIndex].Unlock() // unlock after writing
	return h.maps[mapIndex].Add(key, value)
}

// Delete removes a key from the map.
func (h *HashmapDistributed) Delete(key []byte) error {
	hash := hash(key)
	mapIndex := hash % Hash(len(h.maps))
	h.mutexes[mapIndex].Lock()         // lock for writing
	defer h.mutexes[mapIndex].Unlock() // unlock after writing
	return h.maps[mapIndex].Delete(key)
}

// Update performs an atomic read-modify-write operation on a key.
func (h *HashmapDistributed) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	hash := hash(key)
	mapIndex := hash % Hash(len(h.maps))
	h.mutexes[mapIndex].Lock()         // lock for writing
	defer h.mutexes[mapIndex].Unlock() // unlock after writing
	return h.maps[mapIndex].Update(key, callback)
}

// Clear wipes all data from all shards.
func (h *HashmapDistributed) Clear() error {
	var errGlobal error
	// Iterate sequentially or parallel?
	// Sequentially is safer for file ops? Parallel is faster.
	// Since each shard has its own folder, parallel is fine.
	var wg sync.WaitGroup
	for i := 0; i < len(h.maps); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			h.mutexes[index].Lock()
			defer h.mutexes[index].Unlock()
			if err := h.maps[index].Clear(); err != nil {
				// Race on error assignment, but it's just "an error occurred"
				errGlobal = err
			}
		}(i)
	}
	wg.Wait()
	return errGlobal
}

// Stats collects and aggregates stats from all shards.
func (h *HashmapDistributed) Stats() Stats {
	var total Stats
	for i := 0; i < len(h.maps); i++ {
		h.mutexes[i].RLock()
		s := h.maps[i].Stats()
		h.mutexes[i].RUnlock()

		total.KeyCount += s.KeyCount
		total.Capacity += s.Capacity
		total.DataSize += s.DataSize
		total.Segments += s.Segments
	}
	return total
}

// Compact triggers garbage collection on all shards.
func (h *HashmapDistributed) Compact() error {
	var errGlobal error
	var wg sync.WaitGroup
	for i := 0; i < len(h.maps); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			h.mutexes[index].Lock()
			defer h.mutexes[index].Unlock()
			if err := h.maps[index].Compact(); err != nil {
				errGlobal = err
			}
		}(i)
	}
	wg.Wait()
	return errGlobal
}

// GetMany retrieves values for multiple keys efficiently by grouping them per shard.
// It returns a slice of values aligned with the input keys slice; missing keys map to nil.
// Errors are returned per key; nil error means the operation for that key succeeded (even if value is nil).
func (h *HashmapDistributed) GetMany(keys [][]byte) ([][]byte, []error) {
	numShards := len(h.maps)
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
			h.mutexes[shard].RLock()
			defer h.mutexes[shard].RUnlock()

			m := h.maps[shard]
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

// AddMany inserts multiple key-value pairs efficiently.
// It buckets items by shard and performs parallel insertion.
func (h *HashmapDistributed) AddMany(items []Item) error {
	numShards := len(h.maps)
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
			h.mutexes[index].Lock()
			defer h.mutexes[index].Unlock()
			err := h.maps[index].AddMany(items)
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
