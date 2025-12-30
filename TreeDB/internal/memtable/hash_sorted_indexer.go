package memtable

import (
	"sort"
	"sync"
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
	refs []arenaRef
	keys []string
}

// HashSortedIndexer processes sealed key chunks in the background.
type HashSortedIndexer struct {
	ch       chan hashSortedIndexWork
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewHashSortedIndexer() *HashSortedIndexer {
	x := &HashSortedIndexer{
		ch: make(chan hashSortedIndexWork, hashSortedIndexerQueueSize),
	}
	x.wg.Add(1)
	go x.loop()
	return x
}

func (x *HashSortedIndexer) loop() {
	defer x.wg.Done()
	for work := range x.ch {
		if work.mt == nil || len(work.refs) == 0 {
			continue
		}
		if len(work.keys) != len(work.refs) {
			work.mt.mu.RLock()
			keys := make([]string, len(work.refs))
			for i, ref := range work.refs {
				keys[i] = bytesToStringNoCopy(work.mt.arena.sliceRef(ref))
			}
			work.mt.mu.RUnlock()
			work.keys = keys
		}

		sort.Sort(hashSortedRunSorter{keys: work.keys, refs: work.refs})
		work.mt.indexApplySortedChunk(work.seq, work.refs)
	}
}

type hashSortedRunSorter struct {
	keys []string
	refs []arenaRef
}

func (s hashSortedRunSorter) Len() int { return len(s.refs) }

func (s hashSortedRunSorter) Less(i, j int) bool {
	return s.keys[i] < s.keys[j]
}

func (s hashSortedRunSorter) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.refs[i], s.refs[j] = s.refs[j], s.refs[i]
}

func (x *HashSortedIndexer) enqueue(mt *HashSorted, seq uint64, refs []arenaRef, keys []string) {
	x.ch <- hashSortedIndexWork{mt: mt, seq: seq, refs: refs, keys: keys}
}

// Close stops the indexer after draining queued work.
func (x *HashSortedIndexer) Close() {
	if x == nil {
		return
	}
	x.stopOnce.Do(func() {
		close(x.ch)
		x.wg.Wait()
	})
}

var globalHashSortedIndexer = NewHashSortedIndexer()
