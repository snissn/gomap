package caching

import "github.com/snissn/gomap/TreeDB/internal/memtable"

func acquireMemtable(mode memtable.Mode, capacity int, indexer *memtable.HashSortedIndexer) (memtable.Table, error) {
	// Pooling disabled: reuse caused data corruption under concurrent flushes.
	return memtable.NewWithCapacityModeAndIndexer(capacity, mode, indexer)
}

func recycleMemtable(table memtable.Table) {
	_ = table
}
