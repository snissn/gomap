package caching

import (
	"bytes"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	cowGetManySortMinKeys        = 16
	cowGetManyParallelMinKeys    = 128
	cowGetManyMinKeysPerWorker   = 256
	cowGetManyMaxWorkers         = 8
	cowGetManyViewMicroChunkSize = 128
	cowGetManyProbePoolMaxCap    = 16384
)

func cowGetManyWorkerCount(keyCount int) (workers int, parallel bool) {
	if keyCount < cowGetManyParallelMinKeys {
		return 1, false
	}
	workers = runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > cowGetManyMaxWorkers {
		workers = cowGetManyMaxWorkers
	}
	if byKeys := keyCount / cowGetManyMinKeysPerWorker; byKeys < workers {
		workers = byKeys
	}
	if workers <= 1 {
		return 1, false
	}
	return workers, true
}

func refundFreshOwners(owners []cowFreshReadOwner) bool {
	refunded := false
	for i := range owners {
		if owners[i].refund() {
			refunded = true
		}
	}
	return refunded
}

type cowGetManyProbe struct {
	origIdx int
	key     []byte
}

var cowGetManyProbePool sync.Pool // stores *[]cowGetManyProbe

func acquireCOWGetManyProbes(hint int) []cowGetManyProbe {
	if hint <= 0 {
		return nil
	}
	if hint <= cowGetManyProbePoolMaxCap {
		if raw := cowGetManyProbePool.Get(); raw != nil {
			if p, ok := raw.(*[]cowGetManyProbe); ok && p != nil {
				s := *p
				if cap(s) >= hint {
					return s[:0]
				}
			}
		}
	}
	return make([]cowGetManyProbe, 0, hint)
}

func releaseCOWGetManyProbes(s []cowGetManyProbe) {
	if cap(s) == 0 || cap(s) > cowGetManyProbePoolMaxCap {
		return
	}
	clear(s[:cap(s)])
	s = s[:0]
	cowGetManyProbePool.Put(&s)
}

func (db *DB) cowGetMany(keys [][]byte) ([][]byte, error) {
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		return nil, err
	}
	defer s.Close() // original panic unwind; synchronous outcomes join below
	if len(keys) == 0 {
		return make([][]byte, 0), s.Close()
	}
	values := make([][]byte, len(keys))
	var (
		misses      []cowGetManyProbe
		fallbackIdx []int
		inlineArena []byte
	)
	defer func() { releaseCOWGetManyProbes(misses) }()
	for i, rawKey := range keys {
		key := normalizeRawKVPointKey(rawKey)
		val, _, flags, _, found := s.lookupCachedRootDomainEntryWithRevision(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				continue
			}
			if flags&node.FlagPointer != 0 {
				fallbackIdx = append(fallbackIdx, i)
				continue
			}
			val = normalizeRawKVValue(val)
			if len(val) == 0 {
				values[i] = []byte{}
				continue
			}
			if cap(inlineArena)-len(inlineArena) < len(val) {
				alloc := max(len(val), 4096)
				inlineArena = make([]byte, 0, alloc)
			}
			start := len(inlineArena)
			inlineArena = append(inlineArena, val...)
			values[i] = inlineArena[start:len(inlineArena):len(inlineArena)]
			continue
		}
		if misses == nil {
			misses = acquireCOWGetManyProbes(len(keys) - i)
		}
		misses = append(misses, cowGetManyProbe{origIdx: i, key: key})
	}

	var workerFreshOwners []cowFreshReadOwner
	if len(misses) > 0 {
		if len(misses) >= cowGetManySortMinKeys {
			slices.SortFunc(misses, func(a, b cowGetManyProbe) int {
				return bytes.Compare(a.key, b.key)
			})
		}
		workers, _ := cowGetManyWorkerCount(len(misses))
		workerFallbacks := make([][]int, workers)
		workerErrs := make([]error, workers)
		workerFreshOwners = make([]cowFreshReadOwner, workers)
		basis := s.cowCut.basis
		independent := db.cowPhysicalDictionary(s.cowCache)

		runWorker := func(workerID, lo, hi int) {
			chunk := misses[lo:hi]
			stripe := uint32(workerID) & cowReadGateMask
			if err := s.beginReadStripe(stripe); err != nil {
				workerErrs[workerID] = err
				return
			}
			defer s.endReadStripe(stripe)
			mu, slotReader := basis.lockReadLane(uint32(workerID))
			fresh := *slotReader == nil
			w, werr := basis.workspaceLocked(slotReader, s.cowCache.budget, s.cowCache.readLimits())
			var point *cowPointScratch
			if werr == nil {
				point, werr = w.pointScratch()
			}
			if werr != nil {
				cowReadHandoffLocked(mu, slotReader, fresh, false, werr)
				mu.Unlock()
				if errors.Is(werr, memtable.ErrCOWCapacity) {
					fb := make([]int, len(chunk))
					for i, p := range chunk {
						fb[i] = p.origIdx
					}
					workerFallbacks[workerID] = fb
					return
				}
				workerErrs[workerID] = werr
				return
			}

			var (
				leafCache tree.FixedScratchLeafCache
				arena     []byte
				fb        []int
				readErr   error
			)
			readLeaf := func(ptr page.LeafLogPtr, dst []byte, record backenddb.PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error) {
				point.leafGen++
				return readInspectedPhysicalLeaf(s.cowCache, w, ptr, dst, false, true, independent, &record)
			}
			pinErr := basis.snapshot.WithPinnedCOWPointRead(w.limits, readLeaf, func(read func(key, keyScratch, leafScratch []byte, cache *tree.FixedScratchLeafCache) (node.LeafEntry, error)) error {
				for idx, p := range chunk {
					entry, err := read(p.key, point.key[:], point.leaf[:], &leafCache)
					if err == tree.ErrKeyNotFound {
						continue
					}
					if err != nil {
						_, dictCap := err.(*cowPhysicalDictionaryCapacity)
						if err == errCOWPhysicalDictionary || dictCap || errors.Is(err, memtable.ErrCOWCapacity) {
							fb = append(fb, p.origIdx)
							leafCache.Reset()
							continue
						}
						readErr = err
						return err
					}
					if entry.Flags&node.FlagTombstone != 0 {
						continue
					}
					if entry.Flags&node.FlagPointer != 0 {
						fb = append(fb, p.origIdx)
						continue
					}
					if len(entry.Value) == 0 {
						values[p.origIdx] = []byte{}
						continue
					}
					if cap(arena)-len(arena) < len(entry.Value) {
						const maxChunkArena = 256 << 10
						perKey := max(len(entry.Value), 64)
						chunkBytes := maxChunkArena
						if remaining := len(chunk) - idx; remaining < maxChunkArena/perKey {
							chunkBytes = remaining * perKey
						}
						alloc := max(len(entry.Value), chunkBytes)
						arena = make([]byte, 0, alloc)
					}
					start := len(arena)
					arena = append(arena, entry.Value...)
					values[p.origIdx] = arena[start:len(arena):len(arena)]
				}
				return nil
			})
			handoffErr := pinErr
			if handoffErr == nil {
				handoffErr = readErr
			}
			workerFreshOwners[workerID] = cowReadHandoffLocked(mu, slotReader, fresh, true, handoffErr)
			mu.Unlock()
			workerFallbacks[workerID] = fb
			if pinErr != nil {
				workerErrs[workerID] = pinErr
			}
		}

		if workers == 1 {
			runWorker(0, 0, len(misses))
		} else {
			func() {
				var wg sync.WaitGroup
				defer wg.Wait()
				perWorker := (len(misses) + workers - 1) / workers
				hi0 := min(perWorker, len(misses))
				for w := 1; w < workers; w++ {
					lo := w * perWorker
					if lo >= len(misses) {
						break
					}
					hi := min(lo+perWorker, len(misses))
					wg.Add(1)
					go func(id, start, end int) {
						defer wg.Done()
						runWorker(id, start, end)
					}(w, lo, hi)
				}
				runWorker(0, 0, hi0)
			}()
		}

		for _, werr := range workerErrs {
			if werr != nil {
				return nil, joinCOWCleanupError(werr, s.Close())
			}
		}
		for _, fb := range workerFallbacks {
			if len(fb) > 0 {
				fallbackIdx = append(fallbackIdx, fb...)
			}
		}
	}

	for _, origIdx := range fallbackIdx {
		v, gerr := s.Get(keys[origIdx])
		if errors.Is(gerr, memtable.ErrCOWCapacity) && refundFreshOwners(workerFreshOwners) {
			v, gerr = s.Get(keys[origIdx])
		}
		if gerr == tree.ErrKeyNotFound {
			continue
		}
		if gerr != nil {
			return nil, joinCOWCleanupError(gerr, s.Close())
		}
		values[origIdx] = v
	}
	return values, s.Close()
}

type cowGetManyViewStaged struct {
	probeIdx int
	start    int
	end      int
	found    bool
}

var cowGetManyViewStagedPool = sync.Pool{
	New: func() any {
		return new([cowGetManyViewMicroChunkSize]cowGetManyViewStaged)
	},
}

func (db *DB) cowGetManyView(keys [][]byte, fn tree.GetManyViewFunc) error {
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		return err
	}
	defer s.Close() // retain original panic unwind; actual outcome joins below
	if fn == nil {
		return joinCOWCleanupError(errors.New("cachingdb: GetManyView nil callback"), s.Close())
	}
	if len(keys) == 0 {
		return s.Close()
	}
	// Small batches retain strict 0..N-1 order and <=4 alloc reuse via Snapshot.GetManyView.
	if len(keys) < cowGetManyParallelMinKeys {
		err = s.GetManyView(keys, fn)
		return joinCOWCleanupError(err, s.Close())
	}
	err = s.cowGetManyViewParallel(keys, fn)
	return joinCOWCleanupError(err, s.Close())
}

func (s *Snapshot) cowGetManyViewParallel(keys [][]byte, fn tree.GetManyViewFunc) error {
	var (
		misses      []cowGetManyProbe
		fallbackIdx []int
		memResult   cowViewResult
	)
	defer memResult.close()
	defer func() { releaseCOWGetManyProbes(misses) }()

	for i, rawKey := range keys {
		key := normalizeRawKVPointKey(rawKey)
		val, _, flags, _, found := s.lookupCachedRootDomainEntryWithRevision(key)
		if found {
			if flags&node.FlagTombstone != 0 {
				if err := fn(i, key, nil, false); err != nil {
					return err
				}
				continue
			}
			if flags&node.FlagPointer != 0 {
				fallbackIdx = append(fallbackIdx, i)
				continue
			}
			val = normalizeRawKVValue(val)
			if len(val) == 0 {
				if err := fn(i, key, []byte{}, true); err != nil {
					return err
				}
				continue
			}
			if memResult.lease == nil || len(memResult.value) != len(val) {
				memResult.close()
				lease, err := s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(len(val))))
				if err != nil {
					if errors.Is(err, memtable.ErrCOWCapacity) {
						fallbackIdx = append(fallbackIdx, i)
						continue
					}
					return err
				}
				memResult.lease = lease
				memResult.value = make([]byte, len(val))
			}
			copy(memResult.value, val)
			if err := fn(i, key, memResult.value, true); err != nil {
				return err
			}
			if !memResult.reusable() {
				memResult.close()
			}
			continue
		}
		if misses == nil {
			misses = acquireCOWGetManyProbes(len(keys) - i)
		}
		misses = append(misses, cowGetManyProbe{origIdx: i, key: key})
	}
	memResult.close()

	var workerFreshOwners []cowFreshReadOwner
	if len(misses) > 0 {
		if len(misses) >= cowGetManySortMinKeys {
			slices.SortFunc(misses, func(a, b cowGetManyProbe) int {
				return bytes.Compare(a.key, b.key)
			})
		}
		workers, _ := cowGetManyWorkerCount(len(misses))
		workerFallbacks := make([][]int, workers)
		workerFreshOwners = make([]cowFreshReadOwner, workers)
		var (
			cancel   atomic.Bool
			errMu    sync.Mutex
			firstErr error
		)
		setErr := func(err error) {
			if err == nil {
				return
			}
			cancel.Store(true)
			errMu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			errMu.Unlock()
		}

		basis := s.cowCut.basis
		independent := s.db.cowPhysicalDictionary(s.cowCache)

		perWorker := (len(misses) + workers - 1) / workers
		activeWorkers := (len(misses) + perWorker - 1) / perWorker
		const workerSharedArenaMax = 16 << 10
		workerInitCap := min(workerSharedArenaMax, max(128, min(cowGetManyViewMicroChunkSize, perWorker)*128))
		var (
			sharedArena      []byte
			sharedArenaLease *memtable.COWExternalLease
		)
		if activeWorkers > 0 {
			totalShared := activeWorkers * workerInitCap
			if l, err := s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(totalShared))); err == nil {
				sharedArenaLease = l
				sharedArena = make([]byte, totalShared)
			}
		}
		defer func() {
			sharedArena = nil
			if sharedArenaLease != nil {
				sharedArenaLease.Close()
				sharedArenaLease = nil
			}
		}()

		runWorker := func(workerID, lo, hi int) {
			chunk := misses[lo:hi]
			staged := cowGetManyViewStagedPool.Get().(*[cowGetManyViewMicroChunkSize]cowGetManyViewStaged)
			defer cowGetManyViewStagedPool.Put(staged)
			var (
				fb          []int
				arena       []byte
				arenaLease  *memtable.COWExternalLease
				leafCache   tree.FixedScratchLeafCache
				cachedPoint *cowPointScratch
				cachedGen   uint64
			)
			if sharedArena != nil {
				base := workerID * workerInitCap
				arena = sharedArena[base : base : base+workerInitCap]
			}
			releaseArena := func() {
				arena = nil
				if arenaLease != nil {
					l := arenaLease
					arenaLease = nil
					l.Close()
				}
			}
			defer releaseArena()

			ensureArenaCap := func(need, subLen int) error {
				if cap(arena) >= need {
					return nil
				}
				if cap(arena) == 0 {
					const maxInitArena = 16 << 10
					perEntry := max(need, 128)
					initCap := maxInitArena
					if subLen > 0 && subLen < maxInitArena/perEntry {
						initCap = subLen * perEntry
					}
					initCap = max(need, initCap)
					releaseArena()
					l, err := s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(initCap)))
					if errors.Is(err, memtable.ErrCOWCapacity) && initCap > need {
						initCap = need
						l, err = s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(initCap)))
					}
					if err != nil {
						return err
					}
					arenaLease = l
					arena = make([]byte, 0, initCap)
					return nil
				}
				newCap := max(need, 8192)
				if curCap := cap(arena); curCap > newCap/2 && curCap <= int(^uint(0)>>1)/2 {
					newCap = curCap * 2
				}
				charge := memtable.COWAllocationCharge(uint64(newCap))
				if len(arena) == 0 {
					// Empty arena: release old backing before admitting/allocating replacement.
					releaseArena()
					l, err := s.cowCache.budget.AcquireExternal(charge)
					if errors.Is(err, memtable.ErrCOWCapacity) && newCap > need {
						newCap = need
						l, err = s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(newCap)))
					}
					if err != nil {
						return err
					}
					arenaLease = l
					arena = make([]byte, 0, newCap)
					return nil
				}
				// Non-empty arena: admit replacement while retaining old charge across copy.
				newLease, err := s.cowCache.budget.AcquireExternal(charge)
				if errors.Is(err, memtable.ErrCOWCapacity) && newCap > need {
					newCap = need
					newLease, err = s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(newCap)))
				}
				if err != nil {
					return err
				}
				grown := make([]byte, len(arena), newCap)
				copy(grown, arena)
				oldLease := arenaLease
				arena = grown
				arenaLease = newLease
				if oldLease != nil {
					oldLease.Close()
				}
				return nil
			}

			stripe := uint32(workerID) & cowReadGateMask
			for pos := 0; pos < len(chunk) && !cancel.Load(); {
				end := min(pos+cowGetManyViewMicroChunkSize, len(chunk))
				sub := chunk[pos:end]
				stagedN := 0
				if arena != nil {
					arena = arena[:0]
				}
				var capacityExhaustedAt = -1

				if err := s.beginReadStripe(stripe); err != nil {
					setErr(err)
					return
				}
				mu, slotReader := basis.lockReadLane(uint32(workerID))
				fresh := *slotReader == nil
				w, werr := basis.workspaceLocked(slotReader, s.cowCache.budget, s.cowCache.readLimits())
				var point *cowPointScratch
				if werr == nil {
					point, werr = w.pointScratch()
				}
				if werr != nil {
					cowReadHandoffLocked(mu, slotReader, fresh, false, werr)
					mu.Unlock()
					s.endReadStripe(stripe)
					if errors.Is(werr, memtable.ErrCOWCapacity) {
						for j := pos; j < len(chunk); j++ {
							fb = append(fb, chunk[j].origIdx)
						}
						workerFallbacks[workerID] = fb
						return
					}
					setErr(werr)
					return
				}

				if cachedPoint != point || cachedGen != point.leafGen {
					leafCache.Reset()
				}
				readLeaf := func(ptr page.LeafLogPtr, dst []byte, record backenddb.PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error) {
					point.leafGen++
					return readInspectedPhysicalLeaf(s.cowCache, w, ptr, dst, false, true, independent, &record)
				}
				var fatalErr error
				pinErr := basis.snapshot.WithPinnedCOWPointRead(w.limits, readLeaf, func(read func(key, keyScratch, leafScratch []byte, cache *tree.FixedScratchLeafCache) (node.LeafEntry, error)) error {
					for i, p := range sub {
						if cancel.Load() {
							return nil
						}
						entry, err := read(p.key, point.key[:], point.leaf[:], &leafCache)
						if err == tree.ErrKeyNotFound {
							staged[stagedN] = cowGetManyViewStaged{probeIdx: i, found: false}
							stagedN++
							continue
						}
						if err != nil {
							_, dictCap := err.(*cowPhysicalDictionaryCapacity)
							if err == errCOWPhysicalDictionary || dictCap || errors.Is(err, memtable.ErrCOWCapacity) {
								fb = append(fb, p.origIdx)
								leafCache.Reset()
								continue
							}
							fatalErr = err
							return err
						}
						if entry.Flags&node.FlagTombstone != 0 {
							staged[stagedN] = cowGetManyViewStaged{probeIdx: i, found: false}
							stagedN++
							continue
						}
						if entry.Flags&node.FlagPointer != 0 {
							fb = append(fb, p.origIdx)
							continue
						}
						if len(entry.Value) == 0 {
							staged[stagedN] = cowGetManyViewStaged{probeIdx: i, found: true}
							stagedN++
							continue
						}
						if err := ensureArenaCap(len(arena)+len(entry.Value), len(sub)); err != nil {
							if errors.Is(err, memtable.ErrCOWCapacity) {
								capacityExhaustedAt = pos + i
								return nil
							}
							fatalErr = err
							return err
						}
						vStart := len(arena)
						arena = append(arena, entry.Value...)
						staged[stagedN] = cowGetManyViewStaged{
							probeIdx: i,
							start:    vStart,
							end:      len(arena),
							found:    true,
						}
						stagedN++
					}
					return nil
				})
				cachedPoint = point
				cachedGen = point.leafGen
				handoffErr := pinErr
				if handoffErr == nil {
					handoffErr = fatalErr
				}
				if owner := cowReadHandoffLocked(mu, slotReader, fresh, true, handoffErr); owner.reader != nil {
					workerFreshOwners[workerID] = owner
				}
				mu.Unlock()
				s.endReadStripe(stripe)

				if pinErr != nil {
					setErr(pinErr)
					return
				}

				// Invoke callbacks strictly outside all basis/snapshot locks.
				for i := 0; i < stagedN; i++ {
					if cancel.Load() {
						return
					}
					st := staged[i]
					var val []byte
					if st.found {
						if st.end > st.start {
							val = arena[st.start:st.end:st.end]
						} else {
							val = []byte{}
						}
					}
					p := sub[st.probeIdx]
					if err := fn(p.origIdx, p.key, val, st.found); err != nil {
						setErr(err)
						return
					}
				}

				if capacityExhaustedAt >= 0 {
					// Append only this worker's remaining unvisited entries to fallbackIdx
					// so every key is visited at most once and never duplicated.
					releaseArena()
					for j := capacityExhaustedAt; j < len(chunk); j++ {
						fb = append(fb, chunk[j].origIdx)
					}
					workerFallbacks[workerID] = fb
					return
				}
				pos = end
			}
			workerFallbacks[workerID] = fb
		}

		if workers == 1 {
			runWorker(0, 0, len(misses))
		} else {
			func() {
				var wg sync.WaitGroup
				done := false
				defer func() {
					if !done {
						cancel.Store(true)
					}
					wg.Wait()
				}()
				perWorker := (len(misses) + workers - 1) / workers
				hi0 := min(perWorker, len(misses))
				for w := 1; w < workers; w++ {
					lo := w * perWorker
					if lo >= len(misses) {
						break
					}
					hi := min(lo+perWorker, len(misses))
					wg.Add(1)
					go func(id, start, end int) {
						defer wg.Done()
						runWorker(id, start, end)
					}(w, lo, hi)
				}
				runWorker(0, 0, hi0)
				done = true
			}()
		}

		if firstErr != nil {
			return firstErr
		}
		for _, fb := range workerFallbacks {
			if len(fb) > 0 {
				fallbackIdx = append(fallbackIdx, fb...)
			}
		}
	}

	if len(fallbackIdx) > 0 {
		var result cowViewResult
		defer result.close()
		for _, origIdx := range fallbackIdx {
			key := keys[origIdx]
			found, err := s.cowViewCopy(key, &result)
			if errors.Is(err, memtable.ErrCOWCapacity) {
				retried := false
				if result.lease != nil {
					result.close()
					retried = true
				}
				if refundFreshOwners(workerFreshOwners) {
					retried = true
				}
				if retried {
					found, err = s.cowViewCopy(key, &result)
				}
			}
			if err != nil {
				return err
			}
			var val []byte
			if found {
				val = result.value
			}
			if err := fn(origIdx, normalizeRawKVPointKey(key), val, found); err != nil {
				return err
			}
			if !result.reusable() {
				result.close()
			}
		}
	}
	return nil
}
