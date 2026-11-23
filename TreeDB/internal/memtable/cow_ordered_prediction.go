package memtable

import (
	"bytes"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/node"
)

// OrderedPredictionUpperBound is a positive-only shortcut for the host's
// pre-producer rollover prediction. It never admits storage or reports an input
// error. A false result requires the original exact Estimate path. Prepare must
// still validate and admit the final canonical operands under its original
// authority; this bound is not a preparation receipt.
//
// Caller keeps the input stable and the host writer gate held. The writer lock
// captures one basis. Fully qualified, monotone-prefix PUTs need no membership
// or timestamp-tree searches: every history is one run, and an original
// singleton adds at most one promotion call. All capacities below use the full
// degree-32 envelope, including sparse nodes retaining larger backing arrays.
func (w *COWWriter) OrderedPredictionUpperBound(entries []COWMutation) (COWCharge, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var c COWCharge
	if w.closed || w.pending != nil || w.ordered == nil || len(entries) == 0 {
		return c, false
	}
	t := w.ordered
	n := len(entries)
	maxInt := int(^uint(0) >> 1)
	if n > maxInt-w.private.Len() || n > maxInt-t.len() || n > maxInt-t.maxHistory || n > maxInt-t.keys.Len() {
		return c, false
	}
	ascending, descending := true, true
	var previous []byte
	for _, e := range entries {
		// Pointer production has unknown final resource identities. Keep it,
		// physical erasure and inherited raw qualification on the exact path.
		if !e.MVCC || e.Remove || (e.Flags != node.FlagInline && e.Flags != node.FlagTombstone) {
			return COWCharge{}, false
		}
		prefix, ts, valid := cowOrderedPrefix(e.Key)
		if !valid || ts != e.Timestamp {
			return COWCharge{}, false
		}
		if previous != nil {
			comparison := bytes.Compare(previous, prefix)
			ascending = ascending && comparison <= 0
			descending = descending && comparison >= 0
		}
		previous = prefix
		valueSize, valid := cowValueSize(len(e.Value))
		if !valid || !cowPredictionAdd(&c.Payload, cowAllocation(uint64(len(e.Key)))) ||
			!cowPredictionAdd(&c.Payload, cowAllocation(uint64(valueSize))) {
			return COWCharge{}, false
		}
	}
	if !ascending && !descending {
		return COWCharge{}, false
	}
	full := cowBTreeMaxItems
	rawHeight := max(cowMaximumHeight(w.private.Len()+n), w.private.Height())
	primaryHeight := cowMaximumHeight(t.keys.Len() + n)
	historyHeight := cowMaximumHeight(t.maxHistory + n)
	c.Height = max(rawHeight, primaryHeight, historyHeight)
	primary := cowOrderedNodeReserve(primaryHeight, false, false, full, uint64(unsafe.Sizeof(cowOrderedPairLayout{})))
	history := cowOrderedNodeReserve(historyHeight, false, false, full, uint64(unsafe.Sizeof(cowOrderedVersionPairLayout{})))
	if !cowPredictionProductAdd(&c.Nodes, uint64(n), primary) || !cowPredictionProductAdd(&c.Nodes, 2*uint64(n), history) {
		return COWCharge{}, false
	}
	// Each qualified version can migrate an existing raw incarnation. There
	// are no raw insertions, so independent worst-case deletes dominate every
	// original raw membership/cardinality optimization without looking it up.
	if w.private.Len() != 0 && !cowPredictionProductAdd(&c.Nodes, uint64(n), cowNodeReserveFor(rawHeight, false, true, full)) {
		return COWCharge{}, false
	}
	c.Wrappers = cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{}))) + 2*cowOrderedForkCharge()
	perMutation := cowAllocation(uint64(unsafe.Sizeof(cowOrderedHistory{}))) +
		2*cowAllocation(uint64(unsafe.Sizeof(btree.Map[uint64, *cowOrderedValue]{}))) +
		2*cowAllocation(uint64(unsafe.Sizeof(cowOrderedValue{})))
	if !cowPredictionProductAdd(&c.Wrappers, uint64(n), perMutation) {
		return COWCharge{}, false
	}
	c.Temporary = cowAllocation(uint64(unsafe.Sizeof(COWPrepared{})))
	if uint64(n) > ^uint64(0)/uint64(unsafe.Sizeof((*cowOrderedHistory)(nil))) ||
		!cowPredictionAdd(&c.Temporary, cowAllocation(uint64(n)*uint64(unsafe.Sizeof((*cowOrderedHistory)(nil))))) {
		return COWCharge{}, false
	}
	// Total/History are intentionally plain additions. Prove that neither
	// can overflow before exposing this positive bound to the host.
	var total uint64
	for _, part := range [...]uint64{c.Nodes, c.Payload, c.Wrappers, c.Temporary} {
		if !cowPredictionAdd(&total, part) {
			return COWCharge{}, false
		}
	}
	return c, true
}

// RawPointPredictionUpperBound provides an O(1) positive-only upper bound for a
// single raw (non-ordered) inline/tombstone point mutation without probing the
// B-tree. It covers all three raw paths in O(1):
//   - Path 1 (L0 insert): compact L0 delta slice charge.
//   - Path 2 (L0 -> L1 chunk merge): closed-form chunk-split upper bound.
//   - Path 3 (L1 + L0 -> main B-tree flush): conservative per-fork cardinality bound.
func (w *COWWriter) RawPointPredictionUpperBound(e COWMutation) (COWCharge, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var c COWCharge
	if w.closed || w.pending != nil || w.ordered != nil || e.Remove ||
		(e.Flags != node.FlagInline && e.Flags != node.FlagTombstone) {
		return c, false
	}
	logicalLen := w.private.Len() + w.deltaAdded
	if logicalLen >= int(^uint(0)>>1) {
		return c, false
	}
	valueSize, ok := cowValueSize(len(e.Value))
	if !ok || !cowPredictionAdd(&c.Payload, cowAllocation(uint64(len(e.Key)))) ||
		!cowPredictionAdd(&c.Payload, cowAllocation(uint64(valueSize))) {
		return COWCharge{}, false
	}
	c.Height = cowMaximumHeight(logicalLen + 1)
	if h := w.private.Height(); h > c.Height {
		c.Height = h
	}
	if len(w.delta) < cowDeltaL0MaxEntries {
		c.Nodes = cowDeltaCharge(len(w.delta) + 1)
		c.Wrappers = cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	} else if nextL1 := w.l1Count() + len(w.delta) + 1; w.l1AllowsMerge(nextL1) {
		// Path 2: merging M = len(L0)+1 <= cowDeltaChunkMax entries into L1.
		// At most t = min(oldChunks, M) existing chunks are touched; each
		// touched chunk splits into at most 2 parts of <= cowDeltaChunkMax.
		m := len(w.delta) + 1
		if w.l1 == nil || len(w.l1.chunks) == 0 {
			c.Nodes = cowDeltaL1HeaderCharge(1) + cowDeltaCharge(m)
		} else {
			oldChunks := len(w.l1.chunks)
			touched := min(oldChunks, m)
			maxChunks := oldChunks + touched
			var sliceBound uint64
			if !cowPredictionProductAdd(&sliceBound, uint64(2*touched), cowDeltaCharge(cowDeltaChunkMax)) {
				return COWCharge{}, false
			}
			c.Nodes = cowDeltaL1HeaderCharge(maxChunks)
			if !cowPredictionAdd(&c.Nodes, sliceBound) {
				return COWCharge{}, false
			}
		}
		c.Wrappers = cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	} else {
		// Path 3: full flush of L1 + L0 + entry into w.private.
		totalOps := uint64(w.l1Count() + len(w.delta) + 1)
		maxEntries := w.maxEntries
		if next := logicalLen + 1; next > maxEntries {
			maxEntries = next
		}
		perOp := cowNodeReserveFor(c.Height, false, false, maxEntries)
		var opBound uint64
		if !cowPredictionProductAdd(&opBound, totalOps, perOp) {
			return COWCharge{}, false
		}
		fullCapacity := cowNodeReserveFor(2, true, false, maxEntries) / 2
		oldNodes := uint64(w.private.Len()/cowBTreeMinItems + 1)
		if pathNodes := totalOps * uint64(c.Height); oldNodes > pathNodes {
			oldNodes = pathNodes
		}
		maxNodes := uint64((logicalLen+1)/cowBTreeMinItems + 1)
		var cardBound uint64
		if !cowPredictionProductAdd(&cardBound, oldNodes, fullCapacity) ||
			!cowPredictionProductAdd(&cardBound, 8*maxNodes, fullCapacity) {
			return COWCharge{}, false
		}
		c.Nodes = min(opBound, cardBound)
		c.Wrappers = cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	}
	var total uint64
	for _, part := range [...]uint64{c.Nodes, c.Payload, c.Wrappers} {
		if !cowPredictionAdd(&total, part) {
			return COWCharge{}, false
		}
	}
	return c, true
}

func cowPredictionAdd(dst *uint64, n uint64) bool {
	if n > ^uint64(0)-*dst {
		return false
	}
	*dst += n
	return true
}

func cowPredictionProductAdd(dst *uint64, n, each uint64) bool {
	return (each == 0 || n <= ^uint64(0)/each) && cowPredictionAdd(dst, n*each)
}
