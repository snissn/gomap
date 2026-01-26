package hashdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/snissn/gomap/HashDB/internal/lockfile"
)

// HashDB is the primary, thread-safe HashDB implementation.
//
// This is what older code called `gomap_distributed`: a sharded store backed by
// multiple underlying on-disk DB instances to maximize concurrency.
type HashDB struct {
	dir    string
	lock   *lockfile.Lock
	shards []*CachedDB
	locks  []sync.RWMutex
}

// HashDBOptions configures sharded HashDB behavior.
type HashDBOptions struct {
	CacheWAL CacheWALOptions

	// IndexMemoryPolicy controls memory pinning/advice for the swiss-table index
	// maps of each shard's backend DB.
	IndexMemoryPolicy    IndexMemoryPolicy
	IndexMemoryPolicySet bool
}

// ShardedDB is kept as a compatibility alias for older code.
type ShardedDB = HashDB

// HashmapDistributed is kept as a compatibility alias for older code.
type HashmapDistributed = HashDB

// New initializes the sharded store with storage in the specified folder.
// It creates sub-directories for each partition.
func (h *HashDB) New(folder string) error {
	// 128 shards provides excellent balance for high concurrency
	return h.NewWithShards(folder, 128)
}

// Open is a compatibility wrapper for older code.
func (h *HashDB) Open(folder string) error {
	return h.New(folder)
}

// NewWithShards initializes the sharded store with a specific number of shards.
func (h *HashDB) NewWithShards(folder string, numShards int) (err error) {
	return h.NewWithShardsAndOptions(folder, numShards, HashDBOptions{})
}

// NewWithShardsAndOptions initializes the sharded store with explicit options.
func (h *HashDB) NewWithShardsAndOptions(folder string, numShards int, opts HashDBOptions) (err error) {
	if folder == "" {
		return errors.New("db dir required")
	}
	if numShards <= 0 {
		numShards = runtime.NumCPU()
	}

	if err = os.MkdirAll(folder, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	var lock *lockfile.Lock
	lock, err = lockfile.Acquire(filepath.Join(folder, "LOCK"))
	if err != nil {
		return err
	}
	h.dir = folder
	h.lock = lock

	defer func() {
		// Release lock on error to avoid leaving the directory unusable.
		if err != nil {
			_ = h.Close()
		}
	}()

	h.shards = make([]*CachedDB, numShards)
	h.locks = make([]sync.RWMutex, numShards)

	for i := 0; i < numShards; i++ {
		partitionFolder := fmt.Sprintf("%s/partition-%d", folder, i)
		if err = os.MkdirAll(partitionFolder, 0o755); err != nil {
			return fmt.Errorf("failed to create directory for partition: %w", err)
		}

		var cached *CachedDB
		cached, err = NewCachedDBWithOptions(partitionFolder, 4096, 4<<20, 2*time.Second, CachedDBOptions{
			CacheWAL:             opts.CacheWAL,
			IndexMemoryPolicy:    opts.IndexMemoryPolicy,
			IndexMemoryPolicySet: opts.IndexMemoryPolicySet,
		})
		if err != nil {
			return err
		}
		h.shards[i] = cached
	}

	return nil
}

// OpenWithShards is a compatibility wrapper for older code.
func (h *HashDB) OpenWithShards(folder string, numShards int) error {
	return h.NewWithShards(folder, numShards)
}

// SetCompression enables or disables value compression on all shards.
// It should typically be called during initialization before serving traffic.
func (h *HashDB) SetCompression(enabled bool) {
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		h.shards[i].SetCompression(enabled)
		h.locks[i].Unlock()
	}
}

// SetMaxProbeGroupsBeforeResize sets a probe-length guard on all shards.
// If a new insert scans more than this many probe groups, the shard will
// trigger an incremental resize. Set to 0 to disable.
func (h *HashDB) SetMaxProbeGroupsBeforeResize(groups uint64) {
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		h.shards[i].SetMaxProbeGroupsBeforeResize(groups)
		h.locks[i].Unlock()
	}
}

// Get retrieves the value for a given key.
// It returns nil if the key does not exist.
func (h *HashDB) Get(key []byte) ([]byte, error) {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].RLock()
	defer h.locks[shardIndex].RUnlock()
	return h.shards[shardIndex].Get(key)
}

// Put inserts or updates a key-value pair.
func (h *HashDB) Put(key []byte, value []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].Put(key, value)
}

// PutNoCopyValue inserts or updates a key-value pair without copying the value.
// Caller must not mutate value after calling (it may be retained until flushed).
func (h *HashDB) PutNoCopyValue(key []byte, value []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].PutNoCopyValue(key, value)
}

// PutNoCopyKeyValueUnsafe inserts or updates a key-value pair without copying the key or value.
// Caller must not mutate key or value after calling (they may be retained until flushed).
func (h *HashDB) PutNoCopyKeyValueUnsafe(key []byte, value []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].PutNoCopyKeyValueUnsafe(key, value)
}

// PutNoCopy inserts or updates a key-value pair without copying the value.
//
// Deprecated: use PutNoCopyValue.
func (h *HashDB) PutNoCopy(key []byte, value []byte) error { return h.PutNoCopyValue(key, value) }

// PutNoCopyUnsafe inserts or updates a key-value pair without copying the key or value.
//
// Deprecated: use PutNoCopyKeyValueUnsafe.
func (h *HashDB) PutNoCopyUnsafe(key []byte, value []byte) error {
	return h.PutNoCopyKeyValueUnsafe(key, value)
}

// PutSync performs a durable write. See CachedDB.PutSync for details.
func (h *HashDB) PutSync(key []byte, value []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].PutSync(key, value)
}

// ApplyBatch applies a set of operations by grouping them per shard.
// It is atomic per shard, but not atomic across shards.
func (h *HashDB) ApplyBatch(ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	numShards := len(h.shards)
	sharded := make([][]BatchOp, numShards)
	for _, op := range ops {
		keyHash := hash(op.Key)
		shardIndex := keyHash % Hash(numShards)
		sharded[shardIndex] = append(sharded[shardIndex], op)
	}

	for shardIndex, shardOps := range sharded {
		if len(shardOps) == 0 {
			continue
		}
		h.locks[shardIndex].Lock()
		err := h.shards[shardIndex].ApplyBatch(shardOps)
		h.locks[shardIndex].Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyBatchSync applies a set of operations durably by grouping them per shard.
// It is atomic per shard, but not atomic across shards.
func (h *HashDB) ApplyBatchSync(ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	numShards := len(h.shards)
	sharded := make([][]BatchOp, numShards)
	for _, op := range ops {
		keyHash := hash(op.Key)
		shardIndex := keyHash % Hash(numShards)
		sharded[shardIndex] = append(sharded[shardIndex], op)
	}

	for shardIndex, shardOps := range sharded {
		if len(shardOps) == 0 {
			continue
		}
		h.locks[shardIndex].Lock()
		err := h.shards[shardIndex].ApplyBatchSync(shardOps)
		h.locks[shardIndex].Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// Add is a compatibility wrapper for older code.
func (h *HashDB) Add(key []byte, value []byte) error {
	return h.Put(key, value)
}

// Delete removes a key from the map.
func (h *HashDB) Delete(key []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].Delete(key)
}

// DeleteSync performs a durable delete. See CachedDB.DeleteSync for details.
func (h *HashDB) DeleteSync(key []byte) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].DeleteSync(key)
}

// Update performs an atomic read-modify-write operation on a key.
func (h *HashDB) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	hash := hash(key)
	shardIndex := hash % Hash(len(h.shards))
	h.locks[shardIndex].Lock()
	defer h.locks[shardIndex].Unlock()
	return h.shards[shardIndex].Update(key, callback)
}

// Clear wipes all data from all shards.
func (h *HashDB) Clear() error {
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		err := h.shards[i].Clear()
		h.locks[i].Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// Stats collects and aggregates stats from all shards.
func (h *HashDB) Stats() Stats {
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
func (h *HashDB) Compact() error {
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		err := h.shards[i].Compact()
		h.locks[i].Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush forces all shard-level write-back caches to flush pending writes.
// This is important before process exit or reopening the same on-disk store
// to ensure durability of recent writes.
func (h *HashDB) Flush() error {
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

// Sync flushes write-back caches and fsyncs shard-level storage.
// It is safe to call multiple times.
func (h *HashDB) Sync() error {
	var errGlobal error
	for i := 0; i < len(h.shards); i++ {
		h.locks[i].Lock()
		if h.shards[i] != nil {
			if err := h.shards[i].Sync(); err != nil && errGlobal == nil {
				errGlobal = err
			}
		}
		h.locks[i].Unlock()
	}
	return errGlobal
}

// Close flushes and closes all shards.
// It is not safe to call Close concurrently with other operations.
func (h *HashDB) Close() error {
	var errs []error
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
		if err := shard.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	h.shards = nil
	h.locks = nil

	if h.lock != nil {
		if err := h.lock.Close(); err != nil {
			errs = append(errs, err)
		}
		h.lock = nil
	}
	h.dir = ""

	return errors.Join(errs...)
}

// GetMany retrieves values for multiple keys efficiently by grouping them per shard.
// It returns a slice of values aligned with the input keys slice; missing keys map to nil.
// Errors are returned per key; nil error means the operation for that key succeeded (even if value is nil).
func (h *HashDB) GetMany(keys [][]byte) ([][]byte, []error) {
	numShards := len(h.shards)
	if numShards == 0 {
		return make([][]byte, len(keys)), make([]error, len(keys))
	}

	shardedIndexes := make([][]int, numShards)
	hashes := make([]Hash, len(keys))
	for i, key := range keys {
		keyHash := hash(key)
		hashes[i] = keyHash
		mapIndex := keyHash % Hash(numShards)
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
			shardKeys := make([][]byte, len(idxs))
			shardHashes := make([]Hash, len(idxs))
			for i, keyIndex := range idxs {
				shardKeys[i] = keys[keyIndex]
				shardHashes[i] = hashes[keyIndex]
			}

			shardVals, shardErrs := m.getManyWithHashes(shardKeys, shardHashes)
			for i, keyIndex := range idxs {
				values[keyIndex] = shardVals[i]
				errs[keyIndex] = shardErrs[i]
			}
		}(shardIdx, idxs)
	}
	wg.Wait()

	return values, errs
}

// PutMany inserts multiple key-value pairs efficiently.
// It buckets items by shard and performs parallel insertion.
func (h *HashDB) PutMany(items []Item) error {
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
func (h *HashDB) AddMany(items []Item) error {
	return h.PutMany(items)
}
