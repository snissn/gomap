package memtable

import (
	"sort"
)

const (
	hashSortedSealBytesThreshold = 1 << 20 // 1 MiB of new-key bytes
	hashSortedSealKeysThreshold  = 1 << 15 // 32768 new keys

	hashSortedPendingKeysInitCap          = 256
	hashSortedPendingKeysUpgradeThreshold = 1 << 12 // 4096 keys before preallocating to the seal size
	hashSortedSortedKeysInitCap           = 1024

	// Work items are chunk-granularity, so a fairly large buffer is cheap and
	// avoids backpressure on writers.
	hashSortedIndexerQueueSize = 256
)

type hashSortedIndexWork struct {
	mt   *HashSorted
	seq  uint64
	keys []string
}

type hashSortedIndexer struct {
	ch chan hashSortedIndexWork
}

func newHashSortedIndexer() *hashSortedIndexer {
	x := &hashSortedIndexer{
		ch: make(chan hashSortedIndexWork, hashSortedIndexerQueueSize),
	}
	go x.loop()
	return x
}

func (x *hashSortedIndexer) loop() {
	for work := range x.ch {
		if work.mt == nil || len(work.keys) == 0 {
			continue
		}
		sort.Strings(work.keys)
		work.mt.indexApplySortedChunk(work.seq, work.keys)
	}
}

func (x *hashSortedIndexer) enqueue(mt *HashSorted, seq uint64, keys []string) {
	x.ch <- hashSortedIndexWork{mt: mt, seq: seq, keys: keys}
}

var globalHashSortedIndexer = newHashSortedIndexer()
