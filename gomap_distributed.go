package gomap

import (
	"fmt"
	"os"
	"runtime"
	"sync"
)

// HashmapDistributed is a thread-safe, sharded hash map implementation.
// It partitions keys across multiple underlying Hashmap instances (shards)
// based on the number of available CPU cores to maximize concurrency.
type HashmapDistributed struct {
	maps    []*Hashmap
	mutexes []sync.RWMutex
}

// New initializes the distributed hash map with storage in the specified folder.
// It creates sub-directories for each partition.
func (h *HashmapDistributed) New(folder string) error {
	// Get the number of CPUs
	numCPU := runtime.NumCPU()

	// Initialize the slice of Hashmap pointers and mutexes
	h.maps = make([]*Hashmap, numCPU)
	h.mutexes = make([]sync.RWMutex, numCPU)

	// Create a new Hashmap for each CPU
	for i := 0; i < numCPU; i++ {
		partitionFolder := fmt.Sprintf("%s/partition-%d", folder, i)
		err := os.MkdirAll(partitionFolder, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory for partition: %w", err)
		}

		h.maps[i] = &Hashmap{}
		if err := h.maps[i].New(partitionFolder); err != nil {
			return err
		}
	}

	return nil
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
