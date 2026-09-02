package caching

import (
	"bytes"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pointSuccessorCandidate struct {
	key      []byte
	value    []byte
	ptr      page.ValuePtr
	flags    byte
	revision page.EntryRevision
	priority int
	found    bool
	layer    pointSuccessorLayer
}

// pointSuccessorLayer identifies the source that supplied a visible result.
// Keep this separate from priority: priority expresses recency, while layer is
// attribution for the Dgraph-shaped read/write workload.
type pointSuccessorLayer uint8

const (
	pointSuccessorLayerUnknown pointSuccessorLayer = iota
	pointSuccessorLayerMutable
	pointSuccessorLayerQueue
	pointSuccessorLayerPublished
)

func choosePointSuccessor(best, candidate pointSuccessorCandidate) pointSuccessorCandidate {
	if !candidate.found {
		return best
	}
	if !best.found {
		return candidate
	}
	cmp := bytes.Compare(candidate.key, best.key)
	if cmp < 0 || (cmp == 0 && candidate.priority < best.priority) {
		return candidate
	}
	return best
}

func pointSuccessorAfter(key []byte) []byte {
	return append(append([]byte(nil), key...), 0)
}

func pointSuccessorMaskEnd(layers [][]batch.DeleteRange, firstLayer int, key []byte) (next []byte, masked bool) {
	for i := firstLayer; i < len(layers); i++ {
		for _, span := range layers[i] {
			if !batch.DeleteRangeContainsKey(span, key) {
				continue
			}
			masked = true
			if span.End == nil {
				return nil, true
			}
			if next == nil || bytes.Compare(span.End, next) > 0 {
				next = span.End
			}
		}
	}
	return next, masked
}

func seekPointSuccessorTable(mt memtable.Table, start, end []byte, spans [][]batch.DeleteRange, firstNewerLayer, priority int) pointSuccessorCandidate {
	for mt != nil && (end == nil || bytes.Compare(start, end) < 0) {
		seeker, ok := mt.(memtable.SuccessorTable)
		if !ok {
			return pointSuccessorCandidate{}
		}
		key, value, ptr, flags, revision, found := seeker.SeekGE(start, end)
		if !found {
			return pointSuccessorCandidate{}
		}
		next, masked := pointSuccessorMaskEnd(spans, firstNewerLayer, key)
		if !masked {
			return pointSuccessorCandidate{key: key, value: value, ptr: ptr, flags: flags, revision: revision, priority: priority, found: true}
		}
		if next == nil {
			return pointSuccessorCandidate{}
		}
		if bytes.Compare(next, key) <= 0 {
			next = pointSuccessorAfter(key)
		}
		start = next
	}
	return pointSuccessorCandidate{}
}

func seekPointSuccessorIterator(it iterator.UnsafeIterator, start, end []byte, spans [][]batch.DeleteRange, priority int) (pointSuccessorCandidate, error) {
	if it == nil {
		return pointSuccessorCandidate{}, nil
	}
	it.Seek(start)
	for it.Valid() {
		key := it.UnsafeKey()
		if end != nil && bytes.Compare(key, end) >= 0 {
			return pointSuccessorCandidate{}, nil
		}
		next, masked := pointSuccessorMaskEnd(spans, 0, key)
		if masked {
			if next == nil {
				return pointSuccessorCandidate{}, nil
			}
			if bytes.Compare(next, key) <= 0 {
				next = pointSuccessorAfter(key)
			}
			it.Seek(next)
			continue
		}
		value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)
		if err := it.Error(); err != nil {
			return pointSuccessorCandidate{}, err
		}
		return pointSuccessorCandidate{key: key, value: value, ptr: ptr, flags: flags, revision: revision, priority: priority, found: true}, nil
	}
	return pointSuccessorCandidate{}, it.Error()
}

func (db *DB) pointSuccessorDiskIterator(view *memtableView, start, end []byte) (iterator.UnsafeIterator, func(), error) {
	if snap, ok := liveIteratorRootDomainSnapshot(view); ok && rootDomainSnapshotHasPublishedState(snap) {
		it, exists, err := db.livePublishedRootIterator(snap, start, end, false)
		if err != nil || !exists || it == nil {
			return it, func() {}, err
		}
		return it, func() { _ = it.Close() }, nil
	}
	if err := db.ensureBackendRange(); err != nil {
		return nil, func() {}, err
	}
	db.mu.RLock()
	backendRangeKnown := db.backendRangeKnown
	backendRange := db.backendRange
	db.mu.RUnlock()
	if backendRangeKnown && !overlapsQuery(start, end, backendRange) {
		return nil, func() {}, nil
	}
	if provider, ok := db.backend.(backendSnapshotProvider); ok {
		snap := provider.AcquireSnapshot()
		if snap == nil {
			return nil, func() {}, backenddb.ErrClosed
		}
		lookup := backendSnapshotLookup{db: db, snapshot: snap}
		if state, ok := snap.StateToken(); ok {
			lookup.rootID = state.RootPageID
		}
		it, err := lookup.Iterator(start, end)
		if err != nil {
			_ = snap.Close()
			return nil, func() {}, err
		}
		return it, func() { _ = it.Close(); _ = snap.Close() }, nil
	}
	it, err := db.backend.Iterator(start, end)
	if err != nil {
		return nil, func() {}, err
	}
	return it, func() { _ = it.Close() }, nil
}

// SeekGE returns an owned copy of the first visible physical key/value in
// [start,end). It captures mutable and immutable state without rotating the
// mutable memtables and never constructs a general merging iterator.
func (db *DB) SeekGE(start, end []byte) (key, value []byte, found bool, err error) {
	if db == nil || db.backend == nil {
		return nil, nil, false, backenddb.ErrClosed
	}
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil, nil, false, nil
	}
	db.pointSuccessorCallsTotal.Add(1)
	debugTiming := pointSuccessorDebugEnabled.Load()
	var selectionStarted time.Time
	if debugTiming {
		selectionStarted = time.Now()
	}
	selectionRecorded := false
	recordSelection := func() {
		if debugTiming && !selectionRecorded {
			db.pointSuccessorSelectionNsTotal.Add(uint64(time.Since(selectionStarted).Nanoseconds()))
			db.pointSuccessorSelectionTimingSamplesTotal.Add(1)
			selectionRecorded = true
		}
	}
	defer recordSelection()
	db.beginForegroundRead()
	defer db.endForegroundRead()

	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if db.closing.Load() {
		return nil, nil, false, backenddb.ErrClosed
	}
	view := db.retainMemtableView()
	if view != nil {
		defer db.releaseMemtableView(view)
	}

	var mutables []memtable.Table
	var queue []memtable.Table
	var spans [][]batch.DeleteRange
	pointShard := -1
	if view != nil {
		mutables = view.mutables
		queue = view.queue
		spans = view.queueRangeSpans
		if snap, ok := liveIteratorRootDomainSnapshot(view); ok && len(snap.immutables) > 0 {
			queue = snap.immutables
		}
		if _, ok := mvcckey.ExactVersionRange(start, end); ok && !memtableViewHasRangeSpans(view) {
			pointShard = db.shardIndex(start)
			if pointShard >= 0 && pointShard < len(view.rootPointShards) {
				queue = view.rootPointShards[pointShard].immutables
				spans = nil
			}
		}
	}
	var callSources uint64
	defer func() {
		db.pointSuccessorSourcesTotal.Add(callSources)
		updateAtomicMaxUint64(&db.pointSuccessorSourcesMax, callSources)
	}()
	// CommitAt/GetAt normally asks for the exact physical version just written.
	// Its mutable shard has absolute precedence and no key can sort before an
	// exact lower-bound hit, so avoid acquiring a backend cursor in this case.
	initialTarget := -1
	initialCandidate := pointSuccessorCandidate{}
	if len(mutables) > 0 {
		initialTarget = db.shardIndex(start)
		if initialTarget >= 0 && initialTarget < len(mutables) {
			initialCandidate = seekPointSuccessorTable(mutables[initialTarget], start, end, nil, 0, 0)
			initialCandidate.layer = pointSuccessorLayerMutable
			db.pointSuccessorMutableProbesTotal.Add(1)
			callSources++
			if initialCandidate.found && bytes.Equal(initialCandidate.key, start) && initialCandidate.flags&node.FlagTombstone == 0 {
				recordSelection()
				return db.materializePointSuccessor(initialCandidate)
			}
		}
	}

	disk, closeDisk, err := db.pointSuccessorDiskIterator(view, start, end)
	if err != nil {
		return nil, nil, false, err
	}
	defer closeDisk()

	lower := start
	for end == nil || bytes.Compare(lower, end) < 0 {
		best := pointSuccessorCandidate{}
		priority := 0
		if len(mutables) > 0 {
			target := pointShard
			if target < 0 {
				target = db.shardIndex(lower)
			}
			if target >= 0 && target < len(mutables) {
				candidate := initialCandidate
				if target != initialTarget || !bytes.Equal(lower, start) {
					candidate = seekPointSuccessorTable(mutables[target], lower, end, nil, 0, priority)
					candidate.layer = pointSuccessorLayerMutable
					db.pointSuccessorMutableProbesTotal.Add(1)
					callSources++
				}
				best = choosePointSuccessor(best, candidate)
				priority++
				if candidate.found && bytes.Equal(candidate.key, lower) && candidate.flags&node.FlagTombstone == 0 {
					recordSelection()
					return db.materializePointSuccessor(candidate)
				}
			}
			for i, mt := range mutables {
				if pointShard >= 0 {
					break
				}
				if i == target {
					continue
				}
				candidate := seekPointSuccessorTable(mt, lower, end, nil, 0, priority)
				candidate.layer = pointSuccessorLayerMutable
				best = choosePointSuccessor(best, candidate)
				db.pointSuccessorMutableProbesTotal.Add(1)
				callSources++
				priority++
			}
		}
		for i := len(queue) - 1; i >= 0; i-- {
			candidate := seekPointSuccessorTable(queue[i], lower, end, spans, i+1, priority)
			candidate.layer = pointSuccessorLayerQueue
			best = choosePointSuccessor(best, candidate)
			db.pointSuccessorQueueProbesTotal.Add(1)
			callSources++
			priority++
		}
		if disk != nil {
			candidate, probeErr := seekPointSuccessorIterator(disk, lower, end, spans, priority)
			candidate.layer = pointSuccessorLayerPublished
			db.pointSuccessorBackendProbesTotal.Add(1)
			callSources++
			if probeErr != nil {
				return nil, nil, false, probeErr
			}
			best = choosePointSuccessor(best, candidate)
		}
		if !best.found {
			return nil, nil, false, nil
		}
		if best.flags&node.FlagTombstone == 0 {
			recordSelection()
			return db.materializePointSuccessor(best)
		}
		lower = pointSuccessorAfter(best.key)
	}
	return nil, nil, false, nil
}

func (db *DB) materializePointSuccessor(candidate pointSuccessorCandidate) ([]byte, []byte, bool, error) {
	debugTiming := pointSuccessorDebugEnabled.Load()
	var materializeStarted time.Time
	if debugTiming {
		materializeStarted = time.Now()
		defer func() {
			db.pointSuccessorMaterializeNsTotal.Add(uint64(time.Since(materializeStarted).Nanoseconds()))
			db.pointSuccessorMaterializeTimingSamplesTotal.Add(1)
		}()
	}
	if !candidate.found || candidate.flags&node.FlagTombstone != 0 {
		return nil, nil, false, nil
	}
	key := append([]byte(nil), candidate.key...)
	var value []byte
	if candidate.flags&node.FlagPointer != 0 && candidate.value == nil {
		var err error
		value, err = db.readValueLog(key, candidate.ptr)
		if err != nil {
			return nil, nil, false, err
		}
		value = append([]byte(nil), value...)
	} else {
		value = append([]byte(nil), candidate.value...)
	}
	db.pointSuccessorHitsTotal.Add(1)
	switch candidate.layer {
	case pointSuccessorLayerMutable:
		db.pointSuccessorMutableHitsTotal.Add(1)
	case pointSuccessorLayerQueue:
		db.pointSuccessorQueueHitsTotal.Add(1)
	case pointSuccessorLayerPublished:
		db.pointSuccessorBackendHitsTotal.Add(1)
	}
	return key, value, true, nil
}
