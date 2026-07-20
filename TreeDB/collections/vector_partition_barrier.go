package collections

import (
	"errors"
	"path/filepath"
	"sync"
)

// VectorPartitionStorageBarrierV1 serializes durable vector-partition
// namespace mutation with snapshot export for one DB root. It is deliberately
// root-scoped (not collection-scoped): snapshots copy column_assets and
// vector_partitions together.
type vectorPartitionStorageBarrierEntryV1 struct {
	mu   sync.Mutex
	refs int
}

var vectorPartitionStorageBarriersV1 = struct {
	sync.Mutex
	entries map[string]*vectorPartitionStorageBarrierEntryV1
}{entries: make(map[string]*vectorPartitionStorageBarrierEntryV1)}

func WithVectorPartitionStorageBarrierV1(root string, fn func() error) error {
	if root == "" {
		return errors.New("collections: empty vector partition barrier root")
	}
	canonical, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(canonical); resolveErr == nil {
		canonical = resolved
	}
	vectorPartitionStorageBarriersV1.Lock()
	entry := vectorPartitionStorageBarriersV1.entries[canonical]
	if entry == nil {
		entry = &vectorPartitionStorageBarrierEntryV1{}
		vectorPartitionStorageBarriersV1.entries[canonical] = entry
	}
	entry.refs++
	vectorPartitionStorageBarriersV1.Unlock()
	entry.mu.Lock()
	defer func() {
		entry.mu.Unlock()
		vectorPartitionStorageBarriersV1.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(vectorPartitionStorageBarriersV1.entries, canonical)
		}
		vectorPartitionStorageBarriersV1.Unlock()
	}()
	return fn()
}
