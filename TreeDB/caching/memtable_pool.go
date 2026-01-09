package caching

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const maxPooledMemtableBytes = 64 << 20

var skiplistMemtablePool sync.Pool

func acquireMemtable(mode memtable.Mode, capacity int, indexer *memtable.HashSortedIndexer) (memtable.Table, error) {
	if mode != memtable.ModeSkiplist {
		return memtable.NewWithCapacityModeAndIndexer(capacity, mode, indexer)
	}
	if v := skiplistMemtablePool.Get(); v != nil {
		mt := v.(*memtable.Memtable)
		mt.Reset()
		return mt, nil
	}
	return memtable.NewWithCapacity(capacity), nil
}

func recycleMemtable(table memtable.Table) {
	mt, ok := table.(*memtable.Memtable)
	if !ok {
		return
	}
	if mt.AllocatedBytes() > maxPooledMemtableBytes {
		return
	}
	mt.Reset()
	skiplistMemtablePool.Put(mt)
}
