package collections

import (
	"errors"
	"sync"
)

// VectorPartitionStorageBarrierV1 serializes durable vector-partition
// namespace mutation with snapshot export for one DB root. It is deliberately
// root-scoped (not collection-scoped): snapshots copy column_assets and
// vector_partitions together.
var vectorPartitionStorageBarriersV1 sync.Map // map[string]*sync.Mutex

func WithVectorPartitionStorageBarrierV1(root string, fn func() error) error {
	if root == "" {
		return errors.New("collections: empty vector partition barrier root")
	}
	value, _ := vectorPartitionStorageBarriersV1.LoadOrStore(root, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	return fn()
}
