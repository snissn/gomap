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
	full := 2*btreeDefaultDegree - 1
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
	c.Wrappers = 2*cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{}))) + 2*cowOrderedForkCharge()
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
