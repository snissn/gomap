package collections

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
)

// VectorPartitionStorageBarrierV1 serializes durable vector-partition
// namespace mutation with snapshot export for one DB root. It is deliberately
// root-scoped (not collection-scoped): snapshots copy column_assets and
// vector_partitions together.
type vectorPartitionStorageBarrierEntryV1 struct {
	gate chan struct{}
	refs int
}

var vectorPartitionStorageBarriersV1 = struct {
	sync.Mutex
	entries map[string]*vectorPartitionStorageBarrierEntryV1
}{entries: make(map[string]*vectorPartitionStorageBarrierEntryV1)}

// WithVectorPartitionStorageBarrierV1 is non-reentrant for a root: fn must
// not invoke it again for the same root, or it will wait on its own mutation.
func WithVectorPartitionStorageBarrierV1(root string, fn func() error) error {
	return WithVectorPartitionStorageBarrierWithContextV1(context.Background(), root, fn)
}

// WithVectorPartitionStorageBarrierWithContextV1 makes waiting for the
// root-scoped barrier cancellation-aware. Once fn starts it remains
// responsible for observing ctx itself.
func WithVectorPartitionStorageBarrierWithContextV1(ctx context.Context, root string, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical, err := canonicalVectorPartitionStorageRootV1(root)
	if err != nil {
		return err
	}
	vectorPartitionStorageBarriersV1.Lock()
	entry := vectorPartitionStorageBarriersV1.entries[canonical]
	if entry == nil {
		entry = &vectorPartitionStorageBarrierEntryV1{gate: make(chan struct{}, 1)}
		entry.gate <- struct{}{}
		vectorPartitionStorageBarriersV1.entries[canonical] = entry
	}
	entry.refs++
	vectorPartitionStorageBarriersV1.Unlock()
	select {
	case <-ctx.Done():
		vectorPartitionStorageBarriersV1.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(vectorPartitionStorageBarriersV1.entries, canonical)
		}
		vectorPartitionStorageBarriersV1.Unlock()
		return ctx.Err()
	case <-entry.gate:
		if err := ctx.Err(); err != nil {
			entry.gate <- struct{}{}
			vectorPartitionStorageBarriersV1.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(vectorPartitionStorageBarriersV1.entries, canonical)
			}
			vectorPartitionStorageBarriersV1.Unlock()
			return err
		}
	}
	defer func() {
		entry.gate <- struct{}{}
		vectorPartitionStorageBarriersV1.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(vectorPartitionStorageBarriersV1.entries, canonical)
		}
		vectorPartitionStorageBarriersV1.Unlock()
	}()
	return fn()
}

func canonicalVectorPartitionStorageRootV1(root string) (string, error) {
	if root == "" {
		return "", errors.New("collections: empty vector partition barrier root")
	}
	canonical, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(canonical); resolveErr == nil {
		canonical = resolved
	}
	return canonical, nil
}
