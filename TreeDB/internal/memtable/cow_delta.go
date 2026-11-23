package memtable

import (
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// Two-level immutable sorted front buffer layered in front of raw (non-ordered)
// COWWriter/COWRoot degree-32 B-trees:
//   - L0 (delta []cowDeltaEntry): tiny sorted slice (cap cowDeltaL0MaxEntries).
//     Single-op writes copy only L0 (~256B avg, 1 alloc) without touching L1 or
//     the main B-tree.
//   - L1 (*cowDeltaL1): immutable sorted slice of bounded chunks (each holding
//     1..cowDeltaChunkMax entries, up to cowDeltaL1MaxEntries total subject to
//     a deterministic budget gate). When L0 fills on a new key, L0+entry merge
//     into L1 copying only the outer chunk header slice and touched chunks,
//     dramatically reducing random leaf-copy amplification on the degree-32
//     main B-tree.
//   - Multi-key batches, physical Remove, resource-bearing preparations, or an
//     L1 flush merge L1 then L0 then entries into the main degree-32 B-tree and
//     reset both levels to nil so post-batch roots carry zero front-buffer cost.
const (
	cowDeltaL0MaxEntries = 16
	cowDeltaChunkMax     = 32
	cowDeltaL1MaxEntries = 512
)

type cowDeltaEntry struct {
	key   string
	value cowValue
}

type cowDeltaChunk struct {
	minKey  string
	entries []cowDeltaEntry
}

type cowDeltaL1 struct {
	chunks []cowDeltaChunk
	count  int
	maxKey string
}

func cowDeltaCharge(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return cowAllocation(uint64(n) * uint64(unsafe.Sizeof(cowDeltaEntry{})))
}

func cowDeltaL1HeaderCharge(chunks int) uint64 {
	return cowAllocation(uint64(unsafe.Sizeof(cowDeltaL1{}))) +
		cowAllocation(uint64(chunks)*uint64(unsafe.Sizeof(cowDeltaChunk{})))
}

// cowDeltaFind returns the index of key in sorted delta if found, or its sorted
// insertion position if absent. Uses cowKeyCompare for zero-alloc treedb_safe.
func cowDeltaFind(delta []cowDeltaEntry, key []byte) (int, bool) {
	lo, hi := 0, len(delta)
	for lo < hi {
		mid := lo + (hi-lo)/2
		c := cowKeyCompare(delta[mid].key, key)
		if c < 0 {
			lo = mid + 1
		} else if c > 0 {
			hi = mid
		} else {
			return mid, true
		}
	}
	return lo, false
}

func cowDeltaHasKey(delta []cowDeltaEntry, key []byte) bool {
	if len(delta) == 0 {
		return false
	}
	_, ok := cowDeltaFind(delta, key)
	return ok
}

func cowDeltaSeekGE(delta []cowDeltaEntry, key []byte) int {
	lo, hi := 0, len(delta)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cowKeyCompare(delta[mid].key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func cowDeltaSeekLE(delta []cowDeltaEntry, key []byte) int {
	lo, hi := 0, len(delta)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cowKeyCompare(delta[mid].key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

func cowDeltaSeekLT(delta []cowDeltaEntry, end []byte) int {
	if end == nil {
		return len(delta) - 1
	}
	lo, hi := 0, len(delta)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cowKeyCompare(delta[mid].key, end) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

func cowDeltaSeekLTString(delta []cowDeltaEntry, end string) int {
	lo, hi := 0, len(delta)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if delta[mid].key < end {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

// cowDeltaReplace allocates an exact-capacity immutable copy replacing idx.
func cowDeltaReplace(old []cowDeltaEntry, idx int, entry cowDeltaEntry) []cowDeltaEntry {
	next := make([]cowDeltaEntry, len(old))
	copy(next, old)
	next[idx] = entry
	return next
}

// cowDeltaInsert allocates an exact-capacity immutable copy inserting at idx.
func cowDeltaInsert(old []cowDeltaEntry, idx int, entry cowDeltaEntry) []cowDeltaEntry {
	next := make([]cowDeltaEntry, len(old)+1)
	copy(next[:idx], old[:idx])
	next[idx] = entry
	copy(next[idx+1:], old[idx:])
	return next
}

// --- L1 chunked sorted front buffer helpers ---

func (l1 *cowDeltaL1) len() int {
	if l1 == nil {
		return 0
	}
	return l1.count
}

// findChunk returns the index of the rightmost chunk whose minKey <= key,
// or -1 if key < chunks[0].minKey.
func (l1 *cowDeltaL1) findChunk(key []byte) int {
	lo, hi := 0, len(l1.chunks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cowKeyCompare(l1.chunks[mid].minKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

func (l1 *cowDeltaL1) findChunkString(key string) int {
	lo, hi := 0, len(l1.chunks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if l1.chunks[mid].minKey <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

func (l1 *cowDeltaL1) get(key []byte) (cowDeltaEntry, bool) {
	if l1 == nil || len(l1.chunks) == 0 {
		return cowDeltaEntry{}, false
	}
	if cowKeyCompare(l1.maxKey, key) < 0 || cowKeyCompare(l1.chunks[0].minKey, key) > 0 {
		return cowDeltaEntry{}, false
	}
	ci := l1.findChunk(key)
	if ci < 0 {
		return cowDeltaEntry{}, false
	}
	entries := l1.chunks[ci].entries
	if idx, ok := cowDeltaFind(entries, key); ok {
		return entries[idx], true
	}
	return cowDeltaEntry{}, false
}

func (l1 *cowDeltaL1) hasKey(key []byte) bool {
	_, ok := l1.get(key)
	return ok
}

// seekGE returns (chunkIdx, entryIdx, found) for the first entry >= key.
func (l1 *cowDeltaL1) seekGE(key []byte) (int, int, bool) {
	if l1 == nil || len(l1.chunks) == 0 {
		return 0, 0, false
	}
	if len(key) == 0 {
		return 0, 0, true
	}
	if cowKeyCompare(l1.maxKey, key) < 0 {
		return len(l1.chunks), 0, false
	}
	ci := l1.findChunk(key)
	if ci < 0 {
		return 0, 0, true
	}
	entries := l1.chunks[ci].entries
	ei := cowDeltaSeekGE(entries, key)
	if ei < len(entries) {
		return ci, ei, true
	}
	if ci+1 < len(l1.chunks) {
		return ci + 1, 0, true
	}
	return len(l1.chunks), 0, false
}

// seekLE returns (chunkIdx, entryIdx, found) for the last entry <= key.
func (l1 *cowDeltaL1) seekLE(key []byte) (int, int, bool) {
	if l1 == nil || len(l1.chunks) == 0 {
		return -1, -1, false
	}
	if cowKeyCompare(l1.chunks[0].minKey, key) > 0 {
		return -1, -1, false
	}
	ci := l1.findChunk(key)
	if ci < 0 {
		return -1, -1, false
	}
	entries := l1.chunks[ci].entries
	ei := cowDeltaSeekLE(entries, key)
	if ei >= 0 {
		return ci, ei, true
	}
	if ci > 0 {
		prev := l1.chunks[ci-1].entries
		return ci - 1, len(prev) - 1, true
	}
	return -1, -1, false
}

// seekLT returns (chunkIdx, entryIdx, found) for the last entry < end (nil = last).
func (l1 *cowDeltaL1) seekLT(end []byte) (int, int, bool) {
	if l1 == nil || len(l1.chunks) == 0 {
		return -1, -1, false
	}
	if end == nil {
		last := len(l1.chunks) - 1
		return last, len(l1.chunks[last].entries) - 1, true
	}
	if cowKeyCompare(l1.chunks[0].minKey, end) >= 0 {
		return -1, -1, false
	}
	ci := l1.findChunk(end)
	if ci < 0 {
		return -1, -1, false
	}
	entries := l1.chunks[ci].entries
	ei := cowDeltaSeekLT(entries, end)
	if ei >= 0 {
		return ci, ei, true
	}
	if ci > 0 {
		prev := l1.chunks[ci-1].entries
		return ci - 1, len(prev) - 1, true
	}
	return -1, -1, false
}

func (l1 *cowDeltaL1) seekLTString(end string) (int, int, bool) {
	if l1 == nil || len(l1.chunks) == 0 {
		return -1, -1, false
	}
	if l1.chunks[0].minKey >= end {
		return -1, -1, false
	}
	ci := l1.findChunkString(end)
	if ci < 0 {
		return -1, -1, false
	}
	entries := l1.chunks[ci].entries
	ei := cowDeltaSeekLTString(entries, end)
	if ei >= 0 {
		return ci, ei, true
	}
	if ci > 0 {
		prev := l1.chunks[ci-1].entries
		return ci - 1, len(prev) - 1, true
	}
	return -1, -1, false
}

func (l1 *cowDeltaL1) nextPos(ci, ei int) (int, int, bool) {
	if l1 == nil || ci < 0 || ci >= len(l1.chunks) {
		return 0, 0, false
	}
	if ei+1 < len(l1.chunks[ci].entries) {
		return ci, ei + 1, true
	}
	if ci+1 < len(l1.chunks) {
		return ci + 1, 0, true
	}
	return len(l1.chunks), 0, false
}

func (l1 *cowDeltaL1) prevPos(ci, ei int) (int, int, bool) {
	if l1 == nil || ci < 0 || ci >= len(l1.chunks) {
		return -1, -1, false
	}
	if ei > 0 {
		return ci, ei - 1, true
	}
	if ci > 0 {
		return ci - 1, len(l1.chunks[ci-1].entries) - 1, true
	}
	return -1, -1, false
}

// --- Zero-alloc 2-pass L0 + entry -> L1 chunk merge ---

// cowL0Stream presents the sorted sequence (l0[:insertIdx], newKey/newVal, l0[insertIdx:])
// without allocating a temporary merged slice.
type cowL0Stream struct {
	l0        []cowDeltaEntry
	insertIdx int
	newKey    []byte
	newEntry  cowDeltaEntry
	useBytes  bool
}

func (s *cowL0Stream) len() int { return len(s.l0) + 1 }

func (s *cowL0Stream) compareKey(i int, target string) int {
	if i == s.insertIdx && s.useBytes {
		return -cowKeyCompare(target, s.newKey)
	}
	var k string
	if i < s.insertIdx {
		k = s.l0[i].key
	} else if i == s.insertIdx {
		k = s.newEntry.key
	} else {
		k = s.l0[i-1].key
	}
	if k < target {
		return -1
	}
	if k > target {
		return 1
	}
	return 0
}

func (s *cowL0Stream) at(i int) cowDeltaEntry {
	if i < s.insertIdx {
		return s.l0[i]
	}
	if i == s.insertIdx {
		return s.newEntry
	}
	return s.l0[i-1]
}

type cowL1MergePlan struct {
	outChunks  int
	outCount   int
	nodeCharge uint64
}

func cowDeltaSplitParts(total int) (numParts, base, rem int) {
	if total <= cowDeltaChunkMax {
		return 1, total, 0
	}
	numParts = (total + cowDeltaChunkMax - 1) / cowDeltaChunkMax
	base = total / numParts
	rem = total % numParts
	return numParts, base, rem
}

func cowDeltaMergedSliceCharge(total int) (int, uint64) {
	numParts, base, rem := cowDeltaSplitParts(total)
	var charge uint64
	for p := 0; p < numParts; p++ {
		partLen := base
		if p < rem {
			partLen++
		}
		charge += cowDeltaCharge(partLen)
	}
	return numParts, charge
}

// cowDeltaL1PlanMerge computes the exact output chunk count, total entry count,
// and exact byte allocation charge of merging (l0, insertIdx, newKey) into l1
// without allocating.
func cowDeltaL1PlanMerge(l1 *cowDeltaL1, l0 []cowDeltaEntry, insertIdx int, newKey []byte) cowL1MergePlan {
	stream := cowL0Stream{l0: l0, insertIdx: insertIdx, newKey: newKey, useBytes: true}
	m := stream.len()
	if l1 == nil || len(l1.chunks) == 0 {
		numParts, sliceCharge := cowDeltaMergedSliceCharge(m)
		return cowL1MergePlan{
			outChunks:  numParts,
			outCount:   m,
			nodeCharge: cowDeltaL1HeaderCharge(numParts) + sliceCharge,
		}
	}
	chunks := l1.chunks
	sIdx := 0
	outChunks := 0
	outCount := 0
	var sliceCharge uint64
	for ci := range chunks {
		sStart := sIdx
		if ci+1 < len(chunks) {
			nextMin := chunks[ci+1].minKey
			for sIdx < m && stream.compareKey(sIdx, nextMin) < 0 {
				sIdx++
			}
		} else {
			sIdx = m
		}
		if sIdx == sStart {
			outChunks++
			outCount += len(chunks[ci].entries)
			continue
		}
		// Count merged length of chunks[ci].entries and stream[sStart:sIdx].
		ce := chunks[ci].entries
		ei, si := 0, sStart
		mergedLen := 0
		for ei < len(ce) && si < sIdx {
			cmp := stream.compareKey(si, ce[ei].key)
			if cmp < 0 {
				si++
			} else if cmp > 0 {
				ei++
			} else {
				si++
				ei++
			}
			mergedLen++
		}
		mergedLen += (len(ce) - ei) + (sIdx - si)
		parts, charge := cowDeltaMergedSliceCharge(mergedLen)
		outChunks += parts
		outCount += mergedLen
		sliceCharge += charge
	}
	return cowL1MergePlan{
		outChunks:  outChunks,
		outCount:   outCount,
		nodeCharge: cowDeltaL1HeaderCharge(outChunks) + sliceCharge,
	}
}

// cowDeltaL1BuildMerge executes the exact merge planned by cowDeltaL1PlanMerge,
// allocating exact-capacity slices so actual allocation charge equals plan.nodeCharge.
func cowDeltaL1BuildMerge(l1 *cowDeltaL1, l0 []cowDeltaEntry, insertIdx int, newEntry cowDeltaEntry, plan cowL1MergePlan) (*cowDeltaL1, uint64) {
	stream := cowL0Stream{l0: l0, insertIdx: insertIdx, newEntry: newEntry}
	m := stream.len()
	outChunks := make([]cowDeltaChunk, 0, plan.outChunks)
	var sliceCharge uint64

	if l1 == nil || len(l1.chunks) == 0 {
		numParts, base, rem := cowDeltaSplitParts(m)
		pos := 0
		for p := 0; p < numParts; p++ {
			partLen := base
			if p < rem {
				partLen++
			}
			entries := make([]cowDeltaEntry, partLen)
			for i := 0; i < partLen; i++ {
				entries[i] = stream.at(pos)
				pos++
			}
			outChunks = append(outChunks, cowDeltaChunk{minKey: entries[0].key, entries: entries})
			sliceCharge += cowDeltaCharge(partLen)
		}
		last := outChunks[len(outChunks)-1].entries
		return &cowDeltaL1{
			chunks: outChunks,
			count:  plan.outCount,
			maxKey: last[len(last)-1].key,
		}, cowDeltaL1HeaderCharge(len(outChunks)) + sliceCharge
	}

	chunks := l1.chunks
	sIdx := 0
	for ci := range chunks {
		sStart := sIdx
		if ci+1 < len(chunks) {
			nextMin := chunks[ci+1].minKey
			for sIdx < m && stream.compareKey(sIdx, nextMin) < 0 {
				sIdx++
			}
		} else {
			sIdx = m
		}
		if sIdx == sStart {
			outChunks = append(outChunks, chunks[ci])
			continue
		}
		ce := chunks[ci].entries
		// Count merged length first (exact match with plan).
		ei, si := 0, sStart
		mergedLen := 0
		for ei < len(ce) && si < sIdx {
			cmp := stream.compareKey(si, ce[ei].key)
			if cmp < 0 {
				si++
			} else if cmp > 0 {
				ei++
			} else {
				si++
				ei++
			}
			mergedLen++
		}
		mergedLen += (len(ce) - ei) + (sIdx - si)

		numParts, base, rem := cowDeltaSplitParts(mergedLen)
		ei, si = 0, sStart
		for p := 0; p < numParts; p++ {
			partLen := base
			if p < rem {
				partLen++
			}
			entries := make([]cowDeltaEntry, partLen)
			for out := 0; out < partLen; out++ {
				if ei < len(ce) && si < sIdx {
					se := stream.at(si)
					if se.key < ce[ei].key {
						entries[out] = se
						si++
					} else if se.key > ce[ei].key {
						entries[out] = ce[ei]
						ei++
					} else {
						// L0 shadows L1 on equal key.
						entries[out] = se
						si++
						ei++
					}
				} else if si < sIdx {
					entries[out] = stream.at(si)
					si++
				} else {
					entries[out] = ce[ei]
					ei++
				}
			}
			outChunks = append(outChunks, cowDeltaChunk{minKey: entries[0].key, entries: entries})
			sliceCharge += cowDeltaCharge(partLen)
		}
	}
	last := outChunks[len(outChunks)-1].entries
	return &cowDeltaL1{
		chunks: outChunks,
		count:  plan.outCount,
		maxKey: last[len(last)-1].key,
	}, cowDeltaL1HeaderCharge(len(outChunks)) + sliceCharge
}

// --- Zero-alloc 3-way ScanOwned merge (precedence L0 > L1 > tree) ---

func cowDeltaScanMerged(tree *btree.Map[string, cowValue], l0 []cowDeltaEntry, l1 *cowDeltaL1, iter func(COWRecord) bool) {
	di := 0
	ci, ei := 0, 0
	l1Valid := l1 != nil && len(l1.chunks) > 0
	stopped := false

	emitBufferBefore := func(limit string, hasLimit bool) (matchedLimit bool) {
		for {
			l0Ok := di < len(l0)
			if !l0Ok && !l1Valid {
				return false
			}
			var next cowDeltaEntry
			useL0, useL1 := false, false
			if l0Ok && l1Valid {
				e0 := l0[di]
				e1 := l1.chunks[ci].entries[ei]
				if e0.key < e1.key {
					next, useL0 = e0, true
				} else if e0.key > e1.key {
					next, useL1 = e1, true
				} else {
					next, useL0, useL1 = e0, true, true
				}
			} else if l0Ok {
				next, useL0 = l0[di], true
			} else {
				next, useL1 = l1.chunks[ci].entries[ei], true
			}
			if hasLimit && next.key > limit {
				return false
			}
			if !iter(next.value.record(next.key)) {
				stopped = true
				return false
			}
			if useL0 {
				di++
			}
			if useL1 {
				ci, ei, l1Valid = l1.nextPos(ci, ei)
			}
			if hasLimit && next.key == limit {
				return true
			}
		}
	}

	tree.Scan(func(key string, value cowValue) bool {
		shadowed := emitBufferBefore(key, true)
		if stopped {
			return false
		}
		if shadowed {
			return true
		}
		if !iter(value.record(key)) {
			stopped = true
			return false
		}
		return true
	})
	if stopped {
		return
	}
	emitBufferBefore("", false)
}

// --- Compact (< 256B) zero-alloc work-charged resumable (L0, L1) traversal ---

const (
	deltaPhaseDone      uint8 = 0
	deltaPhaseSearchL0  uint8 = 1
	deltaPhaseSearchL1C uint8 = 2
	deltaPhaseSearchL1E uint8 = 3
	deltaPhaseSkipEq    uint8 = 4
	deltaPhaseCompare   uint8 = 5
)

// cowDeltaTraversal is a compact, zero-allocation, byte-by-byte work-charged
// resumable cursor over the (L0, L1) front buffer (precedence L0 > L1).
// It keeps sizeof(cowDeltaTraversal) < 256B so tight work budgets succeed.
type cowDeltaTraversal struct {
	l0        []cowDeltaEntry
	l1        *cowDeltaL1
	l0Idx     int
	l1Chunk   int
	l1Entry   int
	lo, hi    int
	mid       int
	cmpOffset int
	key       string
	op        btree.Operation
	phase     uint8
	direction int8
	l0Ready   bool
	l1Ready   bool
	l0Win     bool
	equalL0L1 bool
	ready     bool
	started   bool
	atstart   bool
	atend     bool
	pending   bool
	cmpActive bool
	skipEqL0  bool
	skipEqL1  bool
	seekL0    bool
	seekL1    bool
	subOp     btree.Operation
}

func (d *cowDeltaTraversal) currentEntry() (cowDeltaEntry, bool) {
	if !d.ready {
		return cowDeltaEntry{}, false
	}
	if d.l0Win {
		return d.l0[d.l0Idx], true
	}
	return d.l1.chunks[d.l1Chunk].entries[d.l1Entry], true
}

func (d *cowDeltaTraversal) Begin(op btree.Operation, key string, work *COWTraversalWork) error {
	if d.pending && op != btree.Close {
		return btree.ErrPending
	}
	if !work.Reserve(1, uint64(unsafe.Sizeof(*d))) {
		return btree.ErrWork
	}
	d.op = op
	d.key = key
	d.pending = true
	d.cmpActive = false
	d.cmpOffset = 0
	d.skipEqL0 = false
	d.skipEqL1 = false
	d.seekL0 = false
	d.seekL1 = false
	switch op {
	case btree.Close:
		d.ready = false
		d.phase = deltaPhaseDone
	case btree.First, btree.Last, btree.Next, btree.Prev:
		d.phase = deltaPhaseDone
	case btree.SeekGE, btree.SeekLE:
		d.ready = false
		d.subOp = op
		d.seekL0 = len(d.l0) > 0
		d.seekL1 = d.l1 != nil && len(d.l1.chunks) > 0
		d.l0Ready = false
		d.l1Ready = false
		d.initNextSearchPhase()
	default:
		d.pending = false
		d.key = ""
		return btree.ErrOperation
	}
	return nil
}

func (d *cowDeltaTraversal) initNextSearchPhase() {
	if d.seekL0 {
		d.seekL0 = false
		d.lo = 0
		d.hi = len(d.l0)
		d.cmpActive = false
		d.cmpOffset = 0
		d.phase = deltaPhaseSearchL0
		return
	}
	if d.seekL1 {
		d.seekL1 = false
		d.lo = 0
		d.hi = len(d.l1.chunks)
		d.cmpActive = false
		d.cmpOffset = 0
		d.phase = deltaPhaseSearchL1C
		return
	}
	if d.skipEqL0 || d.skipEqL1 {
		d.cmpOffset = 0
		d.phase = deltaPhaseSkipEq
		return
	}
	d.cmpOffset = 0
	d.phase = deltaPhaseCompare
}

func (d *cowDeltaTraversal) stepCompareBytes(a, b string, work *COWTraversalWork) (cmp int, done bool) {
	for d.cmpOffset < len(a) && d.cmpOffset < len(b) {
		if !work.Reserve(0, 2) {
			return 0, false
		}
		x, y := a[d.cmpOffset], b[d.cmpOffset]
		d.cmpOffset++
		if x < y {
			return -1, true
		}
		if x > y {
			return 1, true
		}
	}
	if len(a) < len(b) {
		return -1, true
	}
	if len(a) > len(b) {
		return 1, true
	}
	return 0, true
}

func (d *cowDeltaTraversal) positionFirst() {
	d.started = true
	d.atstart = false
	d.direction = 1
	if len(d.l0) > 0 {
		d.l0Idx = 0
		d.l0Ready = true
	} else {
		d.l0Idx = 0
		d.l0Ready = false
	}
	if d.l1 != nil && len(d.l1.chunks) > 0 {
		d.l1Chunk = 0
		d.l1Entry = 0
		d.l1Ready = true
	} else {
		d.l1Chunk = 0
		d.l1Entry = 0
		d.l1Ready = false
	}
}

func (d *cowDeltaTraversal) positionLast() {
	d.started = true
	d.atend = false
	d.direction = -1
	if len(d.l0) > 0 {
		d.l0Idx = len(d.l0) - 1
		d.l0Ready = true
	} else {
		d.l0Idx = -1
		d.l0Ready = false
	}
	if d.l1 != nil && len(d.l1.chunks) > 0 {
		last := len(d.l1.chunks) - 1
		d.l1Chunk = last
		d.l1Entry = len(d.l1.chunks[last].entries) - 1
		d.l1Ready = true
	} else {
		d.l1Chunk = -1
		d.l1Entry = -1
		d.l1Ready = false
	}
}

func (d *cowDeltaTraversal) stepL0Forward() {
	if d.l0Ready && d.l0Idx+1 < len(d.l0) {
		d.l0Idx++
	} else {
		d.l0Idx = len(d.l0)
		d.l0Ready = false
	}
}

func (d *cowDeltaTraversal) stepL0Backward() {
	if d.l0Ready && d.l0Idx > 0 {
		d.l0Idx--
	} else {
		d.l0Idx = -1
		d.l0Ready = false
	}
}

func (d *cowDeltaTraversal) stepL1Forward() {
	if d.l1Ready {
		d.l1Chunk, d.l1Entry, d.l1Ready = d.l1.nextPos(d.l1Chunk, d.l1Entry)
	}
}

func (d *cowDeltaTraversal) stepL1Backward() {
	if d.l1Ready {
		d.l1Chunk, d.l1Entry, d.l1Ready = d.l1.prevPos(d.l1Chunk, d.l1Entry)
	}
}

func (d *cowDeltaTraversal) Step(work *COWTraversalWork) (bool, error) {
	if !d.pending {
		return true, nil
	}
	if d.op == btree.Close {
		if !work.Reserve(1, uint64(unsafe.Sizeof(*d))) {
			return false, nil
		}
		*d = cowDeltaTraversal{}
		return true, nil
	}

	// Kick off non-seek movement ops on first Step entry.
	if d.phase == deltaPhaseDone {
		if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
			return false, nil
		}
		switch d.op {
		case btree.First:
			d.positionFirst()
			d.cmpOffset = 0
			d.phase = deltaPhaseCompare
		case btree.Last:
			d.positionLast()
			d.cmpOffset = 0
			d.phase = deltaPhaseCompare
		case btree.Next:
			if !d.ready {
				if !d.started || d.atstart {
					d.positionFirst()
					d.cmpOffset = 0
					d.phase = deltaPhaseCompare
				} else {
					d.pending, d.key = false, ""
					return true, nil
				}
			} else if d.direction >= 0 {
				if d.l0Win || d.equalL0L1 {
					d.stepL0Forward()
				}
				if !d.l0Win || d.equalL0L1 {
					d.stepL1Forward()
				}
				d.direction = 1
				d.cmpOffset = 0
				d.phase = deltaPhaseCompare
			} else {
				// Direction reversal: Prev/Last/SeekLE -> Next.
				cur, _ := d.currentEntry()
				d.direction = 1
				if d.equalL0L1 {
					d.stepL0Forward()
					d.stepL1Forward()
					d.cmpOffset = 0
					d.phase = deltaPhaseCompare
				} else if d.l0Win {
					d.stepL0Forward()
					if d.l1 != nil && len(d.l1.chunks) > 0 {
						d.key = cur.key
						d.subOp = btree.SeekGE
						d.seekL1 = true
						d.skipEqL1 = true
						d.initNextSearchPhase()
					} else {
						d.cmpOffset = 0
						d.phase = deltaPhaseCompare
					}
				} else {
					d.stepL1Forward()
					if len(d.l0) > 0 {
						d.key = cur.key
						d.subOp = btree.SeekGE
						d.seekL0 = true
						d.skipEqL0 = true
						d.initNextSearchPhase()
					} else {
						d.cmpOffset = 0
						d.phase = deltaPhaseCompare
					}
				}
			}
		case btree.Prev:
			if !d.ready {
				if d.atend {
					d.positionLast()
					d.cmpOffset = 0
					d.phase = deltaPhaseCompare
				} else {
					d.pending, d.key = false, ""
					return true, nil
				}
			} else if d.direction <= 0 {
				if d.l0Win || d.equalL0L1 {
					d.stepL0Backward()
				}
				if !d.l0Win || d.equalL0L1 {
					d.stepL1Backward()
				}
				d.direction = -1
				d.cmpOffset = 0
				d.phase = deltaPhaseCompare
			} else {
				// Direction reversal: Next/First/SeekGE -> Prev.
				cur, _ := d.currentEntry()
				d.direction = -1
				if d.equalL0L1 {
					d.stepL0Backward()
					d.stepL1Backward()
					d.cmpOffset = 0
					d.phase = deltaPhaseCompare
				} else if d.l0Win {
					d.stepL0Backward()
					if d.l1 != nil && len(d.l1.chunks) > 0 {
						d.key = cur.key
						d.subOp = btree.SeekLE
						d.seekL1 = true
						d.skipEqL1 = true
						d.initNextSearchPhase()
					} else {
						d.cmpOffset = 0
						d.phase = deltaPhaseCompare
					}
				} else {
					d.stepL1Backward()
					if len(d.l0) > 0 {
						d.key = cur.key
						d.subOp = btree.SeekLE
						d.seekL0 = true
						d.skipEqL0 = true
						d.initNextSearchPhase()
					} else {
						d.cmpOffset = 0
						d.phase = deltaPhaseCompare
					}
				}
			}
		default:
			d.pending, d.key = false, ""
			return false, btree.ErrOperation
		}
	}

	for {
		switch d.phase {
		case deltaPhaseSearchL0:
			for d.lo < d.hi {
				if !d.cmpActive {
					if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
						return false, nil
					}
					d.mid = d.lo + (d.hi-d.lo)/2
					d.cmpOffset = 0
					d.cmpActive = true
				}
				cmp, done := d.stepCompareBytes(d.l0[d.mid].key, d.key, work)
				if !done {
					return false, nil
				}
				d.cmpActive = false
				if d.subOp == btree.SeekGE {
					if cmp < 0 {
						d.lo = d.mid + 1
					} else {
						d.hi = d.mid
					}
				} else {
					if cmp <= 0 {
						d.lo = d.mid + 1
					} else {
						d.hi = d.mid
					}
				}
			}
			if d.subOp == btree.SeekGE {
				d.l0Idx = d.lo
				d.l0Ready = d.l0Idx < len(d.l0)
			} else {
				d.l0Idx = d.lo - 1
				d.l0Ready = d.l0Idx >= 0 && d.l0Idx < len(d.l0)
			}
			d.initNextSearchPhase()

		case deltaPhaseSearchL1C:
			// Find rightmost chunk with minKey <= d.key.
			for d.lo < d.hi {
				if !d.cmpActive {
					if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaChunk{}))) {
						return false, nil
					}
					d.mid = d.lo + (d.hi-d.lo)/2
					d.cmpOffset = 0
					d.cmpActive = true
				}
				cmp, done := d.stepCompareBytes(d.l1.chunks[d.mid].minKey, d.key, work)
				if !done {
					return false, nil
				}
				d.cmpActive = false
				if cmp <= 0 {
					d.lo = d.mid + 1
				} else {
					d.hi = d.mid
				}
			}
			ci := d.lo - 1
			if ci < 0 {
				if d.subOp == btree.SeekGE {
					d.l1Chunk = 0
					d.l1Entry = 0
					d.l1Ready = true
				} else {
					d.l1Chunk = -1
					d.l1Entry = -1
					d.l1Ready = false
				}
				d.initNextSearchPhase()
			} else {
				d.l1Chunk = ci
				d.lo = 0
				d.hi = len(d.l1.chunks[ci].entries)
				d.cmpActive = false
				d.cmpOffset = 0
				d.phase = deltaPhaseSearchL1E
			}

		case deltaPhaseSearchL1E:
			entries := d.l1.chunks[d.l1Chunk].entries
			for d.lo < d.hi {
				if !d.cmpActive {
					if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
						return false, nil
					}
					d.mid = d.lo + (d.hi-d.lo)/2
					d.cmpOffset = 0
					d.cmpActive = true
				}
				cmp, done := d.stepCompareBytes(entries[d.mid].key, d.key, work)
				if !done {
					return false, nil
				}
				d.cmpActive = false
				if d.subOp == btree.SeekGE {
					if cmp < 0 {
						d.lo = d.mid + 1
					} else {
						d.hi = d.mid
					}
				} else {
					if cmp <= 0 {
						d.lo = d.mid + 1
					} else {
						d.hi = d.mid
					}
				}
			}
			if d.subOp == btree.SeekGE {
				if d.lo < len(entries) {
					d.l1Entry = d.lo
					d.l1Ready = true
				} else if d.l1Chunk+1 < len(d.l1.chunks) {
					d.l1Chunk++
					d.l1Entry = 0
					d.l1Ready = true
				} else {
					d.l1Chunk = len(d.l1.chunks)
					d.l1Entry = 0
					d.l1Ready = false
				}
			} else {
				ei := d.lo - 1
				if ei >= 0 {
					d.l1Entry = ei
					d.l1Ready = true
				} else if d.l1Chunk > 0 {
					d.l1Chunk--
					d.l1Entry = len(d.l1.chunks[d.l1Chunk].entries) - 1
					d.l1Ready = true
				} else {
					d.l1Chunk = -1
					d.l1Entry = -1
					d.l1Ready = false
				}
			}
			d.initNextSearchPhase()

		case deltaPhaseSkipEq:
			if d.skipEqL0 {
				if d.l0Ready {
					cmp, done := d.stepCompareBytes(d.l0[d.l0Idx].key, d.key, work)
					if !done {
						return false, nil
					}
					if cmp == 0 {
						if d.direction >= 0 {
							d.stepL0Forward()
						} else {
							d.stepL0Backward()
						}
					}
				}
				d.skipEqL0 = false
				d.cmpOffset = 0
			}
			if d.skipEqL1 {
				if d.l1Ready {
					cmp, done := d.stepCompareBytes(d.l1.chunks[d.l1Chunk].entries[d.l1Entry].key, d.key, work)
					if !done {
						return false, nil
					}
					if cmp == 0 {
						if d.direction >= 0 {
							d.stepL1Forward()
						} else {
							d.stepL1Backward()
						}
					}
				}
				d.skipEqL1 = false
				d.cmpOffset = 0
			}
			d.phase = deltaPhaseCompare

		case deltaPhaseCompare:
			reverse := d.op == btree.Prev || d.op == btree.Last || d.op == btree.SeekLE
			if reverse {
				d.direction = -1
			} else {
				d.direction = 1
			}
			d.equalL0L1 = false
			if d.l0Ready && d.l1Ready {
				cmp, done := d.stepCompareBytes(d.l0[d.l0Idx].key, d.l1.chunks[d.l1Chunk].entries[d.l1Entry].key, work)
				if !done {
					return false, nil
				}
				if cmp == 0 {
					d.l0Win = true
					d.equalL0L1 = true
				} else if (!reverse && cmp < 0) || (reverse && cmp > 0) {
					d.l0Win = true
				} else {
					d.l0Win = false
				}
				d.ready = true
			} else if d.l0Ready {
				d.l0Win = true
				d.ready = true
			} else if d.l1Ready {
				d.l0Win = false
				d.ready = true
			} else {
				d.ready = false
			}
			d.started = true
			if d.ready {
				d.atstart = false
				d.atend = false
			} else if reverse {
				d.atstart = true
				d.atend = false
			} else {
				d.atstart = false
				d.atend = true
			}
			d.phase = deltaPhaseDone
			d.pending = false
			d.key = ""
			return true, nil
		}
	}
}

func (d *cowDeltaTraversal) Ready() bool { return d.ready && !d.pending }

func (d *cowDeltaTraversal) Current() (COWRecord, bool) {
	if !d.Ready() {
		return COWRecord{}, false
	}
	e, ok := d.currentEntry()
	if !ok {
		return COWRecord{}, false
	}
	return e.value.record(e.key), true
}
