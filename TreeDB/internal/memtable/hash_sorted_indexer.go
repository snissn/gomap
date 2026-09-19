package memtable

import (
	"runtime"
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
	mt     *HashSorted
	seq    uint64
	keys   []string
	sorted bool
}

// HashSortedIndexer processes sealed key chunks in the background.
type HashSortedIndexer struct {
	ch       chan hashSortedIndexWork
	workers  int
	stopOnce sync.Once
	wg       sync.WaitGroup
	worker   func()
}

func NewHashSortedIndexer() *HashSortedIndexer {
	x := &HashSortedIndexer{
		ch:      make(chan hashSortedIndexWork, hashSortedIndexerQueueSize),
		workers: hashSortedIndexerWorkerCount(),
	}
	x.worker = x.loop
	x.start()
	return x
}

func (x *HashSortedIndexer) start() {
	x.wg.Add(x.workers)
	for i := 0; i < x.workers; i++ {
		go x.worker()
	}
}

func hashSortedIndexerWorkerCount() int {
	procs := runtime.GOMAXPROCS(0)
	if procs <= 2 {
		return 1
	}
	workers := procs / 2
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	return workers
}

func (x *HashSortedIndexer) sharedLoop() { x.loop() }

func (x *HashSortedIndexer) loop() {
	defer x.wg.Done()
	for work := range x.ch {
		if work.mt == nil || len(work.keys) == 0 {
			continue
		}
		if !work.sorted {
			sort.Strings(work.keys)
		}
		work.mt.indexApplySortedChunk(work.seq, work.keys)
	}
}

func (x *HashSortedIndexer) enqueue(mt *HashSorted, seq uint64, keys []string, sorted bool) {
	x.ch <- hashSortedIndexWork{mt: mt, seq: seq, keys: keys, sorted: sorted}
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

var (
	globalHashSortedIndexerOnce sync.Once
	globalHashSortedIndexer     *HashSortedIndexer
)

// sharedHashSortedIndexer returns the process-global fallback indexer used by
// HashSorted memtables created without an explicit indexer. It starts on first
// use and intentionally runs for the lifetime of the process; callers that own
// a memtable lifecycle should construct and close their own indexer instead.
func sharedHashSortedIndexer() *HashSortedIndexer {
	globalHashSortedIndexerOnce.Do(func() {
		x := &HashSortedIndexer{
			ch:      make(chan hashSortedIndexWork, hashSortedIndexerQueueSize),
			workers: hashSortedIndexerWorkerCount(),
		}
		// sharedLoop gives the process-global workers a distinct stack top so
		// goroutine-leak checks can ignore the intentional global without
		// masking owned indexers that were never closed.
		x.worker = x.sharedLoop
		x.start()
		globalHashSortedIndexer = x
	})
	return globalHashSortedIndexer
}
