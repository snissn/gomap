package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
	"weak"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	cowBTreeDegree   = 32
	cowBTreeMinItems = cowBTreeDegree - 1
	cowBTreeMaxItems = 2*cowBTreeDegree - 1
)

// COWMutation borrows input only during Prepare. Remove erases the physical key;
// FlagTombstone stores a cached physical-key deletion marker and its revision.
// MVCC logical tombstones remain separately encoded in ordinary value bytes.
type COWMutation struct {
	Key, Value []byte
	Ptr        page.ValuePtr
	Revision   page.EntryRevision
	Flags      byte
	Remove     bool
	// MVCC qualifies a canonical physical version for the logical-history
	// representation. Timestamp is application time, never revision or LSN.
	MVCC      bool
	Timestamp uint64
}

// COWRecord exposes immutable owned strings. Byte conversions produce copies.
// Pointer resolution is permitted only while the enclosing cut/view is owned.
type COWRecord struct {
	Key, Value string
	Ptr        page.ValuePtr
	Revision   page.EntryRevision
	Flags      byte
}

type cowPairLayout struct {
	value cowValue
	key   string
}
type cowNodeLayout struct {
	ownership unsafe.Pointer // optional cowbtree owned descriptor node metadata
	isoid     uint64
	count     int
	items     []cowPairLayout
	children  *[]*cowNodeLayout
}
type cowInternalNodeLayout struct {
	node     cowNodeLayout
	children []*cowNodeLayout
}

// COWRetirement is a bounded ownership transfer, not a background queue.
// Drain runs outside publication/admission locks. Copies share a once-only
// cleanup claim; allocation/resource charges remain until callbacks complete.
type COWRetirement struct {
	generation *cowGeneration
	prepared   *COWPrepared
	viewLease  *cowViewReleaseLease
}

func (r *COWRetirement) Drain() {
	generation, prepared, lease := r.generation, r.prepared, r.viewLease
	r.generation, r.prepared, r.viewLease = nil, nil, nil
	if generation != nil {
		generation.drain()
	}
	generation = nil
	if prepared != nil {
		prepared.drain()
	}
	prepared = nil
	if lease != nil {
		lease.Close()
	}
}

// COWWriter is deliberately not Table: Reset, arena borrowing/stealing, mutable
// iterators and legacy mode dispatch cannot accidentally reach this capability.
// Only its private headers call Copy; published COWRoot headers never do.
type COWWriter struct {
	mu            sync.Mutex
	private       *btree.Map[string, cowValue]
	ordered       *cowOrderedTree
	delta         []cowDeltaEntry
	deltaTail     []cowDeltaEntry // writer-owned write-once append-tail backing for L0
	l1            *cowDeltaL1
	deltaAdded    int
	privateMaxKey string
	hasPrivateMax bool
	generation    *cowGeneration
	pending       *COWPrepared
	maxEntries    int
	closed        bool
}

func NewCOWWriter(b *COWBudget) (*COWWriter, error) {
	return newCOWWriterWithConstruction(b, nil)
}

func NewCOWOrderedWriter(b *COWBudget) (*COWWriter, error) {
	return newCOWWriterMode(b, nil, true)
}

// ForkCOWWriter prepares a private successor from an owned immutable source.
// It shares only the original source generation, budget and resource owners;
// no ancestor engine, view or published mutable header is retained. Closing one
// fork cannot retire a generation while another private writer remains active.
// All fork/header storage is admitted before allocation and remains in the
// original finite generation history until its last actual owner is released.
func ForkCOWWriter(root *COWRoot) (*COWWriter, error) {
	if !root.RetainOwned() {
		return nil, ErrCOWClosed
	}
	defer func() {
		retired := root.Release()
		retired.Drain()
	}()
	if root.tree == nil {
		return nil, ErrCOWClosed
	}
	g := root.generation
	b := g.budget
	charge := cowAllocation(uint64(unsafe.Sizeof(COWWriter{}))) + cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{})))
	if root.ordered != nil {
		charge += cowOrderedForkCharge()
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrCOWClosed
	}
	// Existing reservations may belong to another captured-root fork. Include
	// their full original charge conservatively; it contains every byte that
	// can become generation history, plus bounded preparation scratch.
	history := g.generationHistoryLocked()
	if g.frozen || !cowFits(b.limits.MaxGenerationBytes, history, g.pending) ||
		!cowFits(b.limits.MaxGenerationBytes, history+g.pending, charge) ||
		!b.retirementFitsLocked(charge) || !b.addLocked(charge) {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	if g.retired {
		b.stats.RetiredBytes -= g.history - g.reservedCharge
		g.retired = false
	}
	g.history += charge
	b.stats.HistoryBytes += charge
	g.refs++
	g.activeWriters++
	b.mu.Unlock()
	w := &COWWriter{private: root.tree.ForkImmutable().SealOwned(), delta: root.delta, l1: root.l1, deltaAdded: root.deltaAdded, generation: g, maxEntries: root.Len()}
	w.privateMaxKey, _, w.hasPrivateMax = w.private.Max()
	if root.ordered != nil {
		w.ordered = root.ordered.fork()
	}
	return w, nil
}

// NewCOWWriterWithMaintenanceConstruction admits only a fixed empty-source
// constructor from the original coarse reserve. Subsequent Prepare calls still
// use the producer budget, including payload, COW nodes, resources and scratch.
func NewCOWWriterWithMaintenanceConstruction(b, maintenance *COWBudget) (*COWWriter, error) {
	if maintenance == nil {
		return nil, ErrCOWCapacity
	}
	return newCOWWriterWithConstruction(b, maintenance)
}

func newCOWWriterWithConstruction(b, maintenance *COWBudget) (*COWWriter, error) {
	return newCOWWriterMode(b, maintenance, false)
}

func NewCOWOrderedWriterWithMaintenanceConstruction(b, maintenance *COWBudget) (*COWWriter, error) {
	if maintenance == nil {
		return nil, ErrCOWCapacity
	}
	return newCOWWriterMode(b, maintenance, true)
}

func newCOWWriterMode(b, maintenance *COWBudget, ordered bool) (*COWWriter, error) {
	if b == nil {
		return nil, fmt.Errorf("nil COW budget")
	}
	base := cowWriterConstructionCharge(b.limits.MaxResources)
	if maintenance != nil {
		// Same immutable empty-root/map wrapper envelope as Prepare(nil), with
		// no mutable payload or resource-growth allowance hidden in the reserve.
		base += cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	}
	if ordered {
		base += cowOrderedConstructionCharge(maintenance != nil)
	}
	g, err := newCOWGenerationWithConstruction(b, base, maintenance)
	if err != nil {
		return nil, err
	}
	w := &COWWriter{private: btree.NewMap[string, cowValue](cowBTreeDegree).SealOwned(), generation: g}
	if ordered {
		w.ordered = newCOWOrderedTree()
	}
	return w, nil
}

// cowWriterConstructionCharge is the ordinary empty writer's generation history.
// The native source's original lease is accounted by its separate constructor.
func cowWriterConstructionCharge(resourceLimit int) uint64 {
	return cowAllocation(uint64(unsafe.Sizeof(COWWriter{}))) + cowAllocation(uint64(unsafe.Sizeof(cowGeneration{}))) +
		cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) +
		cowAllocation(uint64(resourceLimit)*uint64(unsafe.Sizeof(COWResourceID{}))) +
		cowAllocation(uint64(resourceLimit)*uint64(unsafe.Sizeof(cowResourceOwner{})))
}

// BindReservedEmptyRoot installs the one already-admitted generation resolver
// before this empty source can become visible. Original generation release owns
// both its callback and construction lease; no alternate finalizer is created.
func (w *COWWriter) BindReservedEmptyRoot(id COWResourceID, release func()) (*COWRoot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.pending != nil || w.private.Len() != 0 || len(w.delta) != 0 || w.l1 != nil || (w.ordered != nil && w.ordered.len() != 0) || release == nil {
		return nil, ErrCOWPending
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	if b.closed || g.constructionLease == nil || g.frozen || len(g.ids) != 0 || cap(g.ids) == 0 || cap(g.owners) == 0 {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	g.refs++
	g.ids = append(g.ids, id)
	g.owners = append(g.owners, cowResourceOwner{release: release})
	b.mu.Unlock()
	r := &COWRoot{tree: w.private, generation: g}
	r.refs.Store(1)
	if w.ordered != nil {
		r.ordered = w.ordered.immutableCopy()
	}
	return r, nil
}

// COWPrepareOptions reserves the later pre-frame resource binding and C2's
// publication vector/cut/backend/retirement wrappers before allocating them.
// ResourceBytes includes caller-owned closure/lease capacities, not file size.
type COWPrepareOptions struct {
	ResourceSlots             int
	ResourceBytes, ExtraBytes uint64
}

type COWCharge struct {
	Nodes, Payload, Wrappers, Resources, Extra, Temporary uint64
	Height                                                int
}

func (c COWCharge) History() uint64 { return c.Nodes + c.Payload + c.Wrappers + c.Resources + c.Extra }
func (c COWCharge) Total() uint64   { return c.History() + c.Temporary }

// The minimum cardinality of a degree-d height-h tree is 2*d^(h-1)-1.
// This bounds height across the entire batch, including its transient inserts.
func cowMaximumHeight(n int) int {
	if n <= 0 {
		return 0
	}
	h := 1
	min := uint64(1)
	for min <= uint64(n) {
		if min > (^uint64(0)-cowBTreeMinItems)/cowBTreeDegree {
			break
		}
		min = min*cowBTreeDegree + cowBTreeMinItems
		if min > uint64(n) {
			break
		}
		h++
	}
	return h
}

// cowNonDeleteNodeBound returns a proven worst-case B-tree node+slice charge for
// non-delete batches flushing totalOps entries into w.private (initially
// holding oldLen keys, reaching at most logicalAfter keys and height levels).
func cowNonDeleteNodeBound(oldLen, logicalAfter, totalOps uint64, height, maxEntries int, allReplace bool) (uint64, bool) {
	if totalOps == 0 {
		return 0, true
	}
	if height < 1 {
		height = 1
	}
	maxItemsLimit := uint64(cowBTreeMaxItems)
	if maxEntries > 0 {
		// Doubling a private node's capacity can reach 2*maxEntries (clamped at 63).
		if doubled := uint64(maxEntries) * 2; doubled < maxItemsLimit {
			maxItemsLimit = doubled
		}
	}
	leafHeader := cowAllocation(uint64(unsafe.Sizeof(cowNodeLayout{})))
	intHeader := cowAllocation(uint64(unsafe.Sizeof(cowInternalNodeLayout{})))
	maxItems := cowAllocation(maxItemsLimit * uint64(unsafe.Sizeof(cowPairLayout{})))
	var maxCh uint64
	if height > 1 {
		maxCh = cowAllocation(uint64(2*cowBTreeDegree) * uint64(unsafe.Sizeof(uintptr(0))))
	}

	var oldLeaves uint64
	if oldLen > 0 {
		oldLeaves = oldLen/cowBTreeMinItems + 1
	}
	tLeaf := min(oldLeaves, totalOps)

	var tInt uint64
	if height > 1 && oldLen > 0 {
		oldInts := oldLeaves/cowBTreeMinItems + uint64(height)
		tInt = oldInts
		var pathInts uint64
		if cowPredictionProductAdd(&pathInts, totalOps, uint64(height-1)) && pathInts < tInt {
			tInt = pathInts
		}
	}

	if allReplace {
		var leafCopyCharge, intCopyCharge uint64
		if !cowPredictionProductAdd(&leafCopyCharge, tLeaf, leafHeader+maxItems) ||
			!cowPredictionProductAdd(&intCopyCharge, tInt, intHeader+maxItems+maxCh) {
			return 0, false
		}
		var total uint64
		if !cowPredictionAdd(&total, leafCopyCharge) || !cowPredictionAdd(&total, intCopyCharge) {
			return 0, false
		}
		return total, true
	}

	// Excess items above min=31 across all initial leaves: sum(max(0,|L|-31)) <=
	// (oldLen/63)*32 + ceil(((oldLen%63)*32)/63). Each split consumes 32 excess
	// items while each insertion adds at most 1.
	initLeafExcess := (oldLen/cowBTreeMaxItems)*32 + ((oldLen%cowBTreeMaxItems)*32+62)/63
	sLeaf := min(totalOps, (initLeafExcess+totalOps)/32+1)

	var leafHdrCount, leafHeaders uint64
	if !cowPredictionAdd(&leafHdrCount, tLeaf) ||
		!cowPredictionAdd(&leafHdrCount, sLeaf) ||
		!cowPredictionAdd(&leafHdrCount, 1) ||
		!cowPredictionProductAdd(&leafHeaders, leafHdrCount, leafHeader) {
		return 0, false
	}

	// Either at most (totalOps + cFull) leaf slice allocations occur (1 per op,
	// plus 1 growItems after splitting a full shared leaf), or each copied/new
	// leaf doubles at most <= 3*maxItems and each split side grows <= 2*maxItems each.
	cFull := min(oldLen/cowBTreeMaxItems, totalOps)
	var perOpLeafSlices, opLeafBytes uint64
	if !cowPredictionAdd(&perOpLeafSlices, totalOps) ||
		!cowPredictionAdd(&perOpLeafSlices, cFull) ||
		!cowPredictionProductAdd(&opLeafBytes, perOpLeafSlices, maxItems) {
		return 0, false
	}
	leafSlices := opLeafBytes
	var growthLeafBytes, splitLeafBytes uint64
	if cowPredictionProductAdd(&growthLeafBytes, tLeaf+1, 3*maxItems) &&
		cowPredictionProductAdd(&splitLeafBytes, sLeaf, 4*maxItems) &&
		cowPredictionAdd(&growthLeafBytes, splitLeafBytes) &&
		growthLeafBytes < leafSlices {
		leafSlices = growthLeafBytes
	}

	var total uint64
	if !cowPredictionAdd(&total, leafHeaders) || !cowPredictionAdd(&total, leafSlices) {
		return 0, false
	}
	if height <= 1 {
		return total, true
	}

	sInt := (oldLeaves+sLeaf)/cowBTreeMinItems + uint64(height)
	var maxCascadeSplits uint64
	if cowPredictionProductAdd(&maxCascadeSplits, totalOps, uint64(height)) && maxCascadeSplits < sInt {
		sInt = maxCascadeSplits
	}

	var intHdrCount, intHeaders uint64
	if !cowPredictionAdd(&intHdrCount, tInt) ||
		!cowPredictionAdd(&intHdrCount, sInt) ||
		!cowPredictionAdd(&intHdrCount, uint64(height)) ||
		!cowPredictionProductAdd(&intHeaders, intHdrCount, intHeader) {
		return 0, false
	}

	var intCopyCount, intCopies uint64
	if !cowPredictionAdd(&intCopyCount, tInt) ||
		!cowPredictionAdd(&intCopyCount, uint64(height)) ||
		!cowPredictionProductAdd(&intCopies, intCopyCount, maxItems+maxCh) {
		return 0, false
	}

	var splitTotal, perSplitGrow uint64
	if !cowPredictionAdd(&splitTotal, sLeaf) ||
		!cowPredictionAdd(&splitTotal, sInt) ||
		!cowPredictionProductAdd(&perSplitGrow, splitTotal, maxItems+maxCh) {
		return 0, false
	}
	intGrows := perSplitGrow
	var amortBase, amortSplits uint64
	if cowPredictionProductAdd(&amortBase, intCopyCount, 3*(maxItems+maxCh)) &&
		cowPredictionProductAdd(&amortSplits, sInt, 4*maxItems+2*maxCh) &&
		cowPredictionAdd(&amortBase, amortSplits) &&
		amortBase < intGrows {
		intGrows = amortBase
	}

	if !cowPredictionAdd(&total, intHeaders) ||
		!cowPredictionAdd(&total, intCopies) ||
		!cowPredictionAdd(&total, intGrows) {
		return 0, false
	}
	return total, true
}

// Bound tidwall v1.8.1 Set/Delete allocation paths. Copies use existing capacity;
// growth occurs only at cap<=max, at most doubles, then allocator rounding.
// Splits share arrays; retries touch private nodes. Delete copies at most one
// extra sibling per level and a merge can grow arrays twice. Replacement alone
// neither splits nor grows and needs just the original path copies.
func cowNodeReserve(height int, replacement, remove bool) uint64 {
	return cowNodeReserveFor(height, replacement, remove, cowBTreeMaxItems)
}

func cowNodeReserveFor(height int, replacement, remove bool, maxEntries int) uint64 {
	if height < 1 {
		height = 1
	}
	items := cowBTreeMaxItems
	if maxEntries < items {
		items = maxEntries
	}
	if items < 1 {
		items = 1
	}
	pairs := cowAllocation(uint64(2*items) * uint64(unsafe.Sizeof(cowPairLayout{})))
	children := cowAllocation(uint64(4*cowBTreeDegree) * uint64(unsafe.Sizeof(uintptr(0))))
	header := cowAllocation(uint64(unsafe.Sizeof(cowInternalNodeLayout{})))
	if height == 1 {
		children = 0
		header = cowAllocation(uint64(unsafe.Sizeof(cowNodeLayout{})))
	}
	full := header + pairs + children
	h := uint64(height)
	if replacement {
		return h * full
	}
	if remove {
		return 2*h*full + 2*h*(pairs+children)
	}
	return h*full + (h+1)*header + (h+1)*(pairs+children)
}

func (w *COWWriter) l1Count() int {
	if w.l1 == nil {
		return 0
	}
	return w.l1.count
}

func (w *COWWriter) l1AllowsMerge(nextL1Count int) bool {
	if nextL1Count > cowDeltaL1MaxEntries {
		return false
	}
	limits := w.generation.budget.limits
	budgetCap := min(limits.MaxInFlightBytes, limits.MaxGenerationBytes) / 4
	// Account for up to cowDeltaL0MaxEntries + 1 more keys arriving before L1
	// is next flushed to the B-tree, so the eventual Path-3 flush cannot spike
	// past budgetCap.
	flushOps := nextL1Count + cowDeltaL0MaxEntries + 1
	logicalAfter := w.private.Len() + w.deltaAdded + 1 + cowDeltaL0MaxEntries
	h := cowMaximumHeight(logicalAfter)
	if ph := w.private.Height(); ph > h {
		h = ph
	}
	maxEntries := w.maxEntries
	if logicalAfter > maxEntries {
		maxEntries = logicalAfter
	}
	totalOps := uint64(flushOps)
	perOp := cowNodeReserveFor(h, false, false, maxEntries)
	var opBound uint64
	if !cowPredictionProductAdd(&opBound, totalOps, perOp) {
		return false
	}
	bound := opBound
	if nonDel, ok := cowNonDeleteNodeBound(uint64(w.private.Len()), uint64(logicalAfter), totalOps, h, maxEntries, false); ok && nonDel < bound {
		bound = nonDel
	}
	return bound <= budgetCap
}

func (w *COWWriter) canUseL0(entries []COWMutation, opts COWPrepareOptions) (int, bool, bool) {
	if w.ordered != nil || len(entries) != 1 || entries[0].Remove || opts.ResourceSlots != 0 {
		return 0, false, false
	}
	idx, inL0 := cowDeltaFind(w.delta, entries[0].Key)
	if !inL0 && len(w.delta) >= cowDeltaL0MaxEntries {
		return 0, false, false
	}
	return idx, inL0, true
}

func (w *COWWriter) canMergeToL1(entries []COWMutation, opts COWPrepareOptions) (int, cowL1MergePlan, bool) {
	if w.ordered != nil || len(entries) != 1 || entries[0].Remove || opts.ResourceSlots != 0 {
		return 0, cowL1MergePlan{}, false
	}
	idx, inL0 := cowDeltaFind(w.delta, entries[0].Key)
	if inL0 {
		return 0, cowL1MergePlan{}, false
	}
	plan := cowDeltaL1PlanMerge(w.l1, w.delta, idx, entries[0].Key)
	if !w.l1AllowsMerge(plan.outCount) {
		return 0, cowL1MergePlan{}, false
	}
	return idx, plan, true
}

func (w *COWWriter) canAppendDeltaTail(inL0 bool, deltaIdx int) bool {
	return !inL0 && deltaIdx == len(w.delta) && len(w.delta) > 0 &&
		len(w.delta) < cap(w.deltaTail) && len(w.deltaTail) == len(w.delta) &&
		&w.delta[0] == &w.deltaTail[0]
}

func (w *COWWriter) estimateSingleDelta(e COWMutation, opts COWPrepareOptions, deltaIdx int, inL0, mergeToL1 bool, plan cowL1MergePlan, reusePrepared bool) (COWCharge, string, cowValue, bool, bool, error) {
	var c COWCharge
	if e.Flags&node.FlagPointer != 0 && e.Flags&node.FlagTombstone != 0 {
		return c, "", "", false, false, fmt.Errorf("COW entry has pointer and tombstone flags")
	}
	valueSize, ok := cowValueSize(len(e.Value))
	if !ok {
		return c, "", "", false, false, ErrCOWCapacity
	}
	var ownedKey string
	var prevVal cowValue
	var keyExists, isBrandNew bool
	nextLogicalLen := w.private.Len() + w.deltaAdded
	if inL0 {
		ownedKey = w.delta[deltaIdx].key
		prevVal = w.delta[deltaIdx].value
		keyExists = true
	} else if l1Entry, ok := w.l1.get(e.Key); ok {
		ownedKey = l1Entry.key
		prevVal = l1Entry.value
		keyExists = true
	} else {
		var inTree bool
		if w.hasPrivateMax && cowKeyCompare(w.privateMaxKey, e.Key) >= 0 {
			treeKey, treeVal, found := cowLookupGE(w.private, e.Key)
			if found && cowKeyCompare(treeKey, e.Key) == 0 {
				inTree = true
				ownedKey = treeKey
				prevVal = treeVal
				keyExists = true
			}
		}
		if !inTree {
			if nextLogicalLen == int(^uint(0)>>1) {
				return c, "", "", false, false, ErrCOWCapacity
			}
			isBrandNew = true
			nextLogicalLen++
		}
	}
	c.Height = cowMaximumHeight(nextLogicalLen)
	if h := w.private.Height(); h > c.Height {
		c.Height = h
	}
	if mergeToL1 {
		c.Nodes = plan.nodeCharge
	} else if len(w.delta) == 0 {
		// Pre-charge one full write-once append-tail L0 backing once per run.
		c.Nodes = cowDeltaCharge(cowDeltaL0MaxEntries)
	} else if w.canAppendDeltaTail(inL0, deltaIdx) {
		c.Nodes = 0
	} else {
		nextDeltaLen := len(w.delta)
		if !inL0 {
			nextDeltaLen++
		}
		c.Nodes = cowDeltaCharge(nextDeltaLen)
	}
	if !keyExists {
		keyCharge := cowAllocation(uint64(len(e.Key)))
		valCharge := cowAllocation(uint64(valueSize))
		payload, ok := cowAdd(keyCharge, valCharge)
		if !ok {
			return c, "", "", false, false, ErrCOWCapacity
		}
		c.Payload = payload
	} else {
		c.Payload = cowAllocation(uint64(valueSize))
	}
	c.Wrappers = cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	if !reusePrepared {
		c.Temporary = cowAllocation(uint64(unsafe.Sizeof(COWPrepared{})))
	}
	c.Resources = opts.ResourceBytes
	c.Extra = opts.ExtraBytes
	var total uint64
	for _, n := range [...]uint64{c.Nodes, c.Payload, c.Wrappers, c.Temporary, c.Resources, c.Extra} {
		var ok bool
		total, ok = cowAdd(total, n)
		if !ok {
			return c, "", "", false, false, ErrCOWCapacity
		}
	}
	return c, ownedKey, prevVal, keyExists, isBrandNew, nil
}

func (w *COWWriter) estimate(entries []COWMutation, opts COWPrepareOptions) (COWCharge, error) {
	var c COWCharge
	routing := w.ordered.routing(entries)
	if opts.ResourceSlots < 0 || opts.ResourceSlots > w.generation.budget.limits.MaxResources {
		return c, ErrCOWCapacity
	}
	bufLen := w.l1Count() + len(w.delta)
	if bufLen > int(^uint(0)>>1)-w.private.Len() || len(entries) > int(^uint(0)>>1)-w.private.Len()-bufLen {
		return c, ErrCOWCapacity
	}
	logicalAfter := w.private.Len() + w.deltaAdded + len(entries)
	c.Height = cowMaximumHeight(logicalAfter)
	if h := w.private.Height(); h > c.Height {
		c.Height = h
	}
	// A physical delete can invalidate a later replacement's membership proof.
	deletes := false
	allReplace := w.deltaAdded == 0
	allRemove := len(entries) > 0 && bufLen == 0
	maxEntries := w.maxEntries
	if logicalAfter > maxEntries {
		maxEntries = logicalAfter
	}
	for index, e := range entries {
		routed := routing.routes(index)
		rawRemove := e.Remove
		if routed {
			if !cowHasKey(w.private, e.Key) {
				continue
			}
			rawRemove = true
		}
		if !rawRemove {
			allRemove = false
		}
		if rawRemove {
			deletes = true
			allReplace = false
		}
	}
	add := func(dst *uint64, n uint64) bool { v, ok := cowAdd(*dst, n); *dst = v; return ok }
	var growth uint64
	fullCapacity := cowNodeReserveFor(2, true, false, maxEntries) / 2
	if bufLen > 0 {
		copies := cowNodeReserveFor(c.Height, true, false, maxEntries)
		if deletes {
			reserve := cowNodeReserveFor(c.Height, false, false, maxEntries)
			for range bufLen {
				if !add(&c.Nodes, reserve) || !add(&growth, reserve-copies) {
					return c, ErrCOWCapacity
				}
			}
		} else {
			bufReplaces := bufLen - w.deltaAdded
			for range bufReplaces {
				if !add(&c.Nodes, copies) {
					return c, ErrCOWCapacity
				}
			}
			if w.deltaAdded > 0 {
				reserve := cowNodeReserveFor(c.Height, false, false, maxEntries)
				for range w.deltaAdded {
					if !add(&c.Nodes, reserve) || !add(&growth, reserve-copies) {
						return c, ErrCOWCapacity
					}
				}
			}
		}
	}
	skipRawBatchProbes := false
	if w.ordered == nil && len(entries) > 1 && !deletes {
		totalOpsEstimate := bufLen + len(entries)
		if w.private.Len() == 0 && len(w.delta) == 0 && w.l1 == nil {
			skipRawBatchProbes = true
		} else if len(entries) >= 32 && w.generation != nil && w.generation.budget != nil {
			limits := w.generation.budget.limits
			budgetCap := min(limits.MaxInFlightBytes, limits.MaxGenerationBytes) / 4
			if bound, ok := cowNonDeleteNodeBound(uint64(w.private.Len()), uint64(logicalAfter), uint64(totalOpsEstimate), c.Height, maxEntries, false); ok && bound <= budgetCap {
				skipRawBatchProbes = true
			}
		}
	}
	for index, e := range entries {
		if !e.Remove && e.Flags&node.FlagPointer != 0 && e.Flags&node.FlagTombstone != 0 {
			return c, fmt.Errorf("COW entry has pointer and tombstone flags")
		}
		if routing.routes(index) {
			if cowHasKey(w.private, e.Key) {
				reserve := cowNodeReserveFor(c.Height, false, true, maxEntries)
				if !add(&c.Nodes, reserve) {
					return c, ErrCOWCapacity
				}
				copies := 2 * cowNodeReserveFor(c.Height, true, false, maxEntries)
				if !add(&growth, reserve-copies) {
					return c, ErrCOWCapacity
				}
			}
			continue
		}
		var exists bool
		if !skipRawBatchProbes {
			inTree := false
			if w.private.Len() != 0 && (!w.hasPrivateMax || cowKeyCompare(w.privateMaxKey, e.Key) >= 0) {
				inTree = cowHasKey(w.private, e.Key)
			}
			exists = inTree || cowDeltaHasKey(w.delta, e.Key) || w.l1.hasKey(e.Key)
		}
		if !exists || e.Remove {
			allReplace = false
		}
		replacement := exists && !deletes && !e.Remove
		reserve := cowNodeReserveFor(c.Height, replacement, e.Remove, maxEntries)
		if !add(&c.Nodes, reserve) {
			return c, ErrCOWCapacity
		}
		if !replacement {
			copies := cowNodeReserveFor(c.Height, true, false, maxEntries)
			if e.Remove {
				copies *= 2
			}
			if !add(&growth, reserve-copies) {
				return c, ErrCOWCapacity
			}
		}
		if !e.Remove {
			valueSize, ok := cowValueSize(len(e.Value))
			if !ok {
				return c, ErrCOWCapacity
			}
			keyCharge := uint64(0)
			if !replacement {
				keyCharge = cowAllocation(uint64(len(e.Key)))
			}
			if !add(&c.Payload, keyCharge) || !add(&c.Payload, cowAllocation(uint64(valueSize))) {
				return c, ErrCOWCapacity
			}
		}
	}
	// Across any one fork, each original node can be copied only once, even
	// when deletes and inserts are interleaved. Fresh split nodes are private.
	totalOps := bufLen + len(entries)
	if totalOps > 0 {
		bound := uint64(w.private.Len()/cowBTreeMinItems+1)*fullCapacity + growth
		if bound < c.Nodes {
			c.Nodes = bound
		}
	}
	// One batch forks only once. A shared original node is copied at most once;
	// without physical deletes nodes never disappear. This cardinality bound
	// avoids charging every operation as an independent full-height fork.
	if totalOps > 0 && !deletes {
		if bound, ok := cowNonDeleteNodeBound(uint64(w.private.Len()), uint64(logicalAfter), uint64(totalOps), c.Height, maxEntries, allReplace); ok && bound < c.Nodes {
			c.Nodes = bound
		}
	}
	if allRemove {
		// Delete-only batches copy each original node at most once. Each
		// merge removes one sibling and can allocate two array growths;
		// rotations add at most one per surviving node capacity progression.
		bound := 5 * uint64(w.private.Len()/cowBTreeMinItems+1) * fullCapacity
		if bound < c.Nodes {
			c.Nodes = bound
		}
	}
	c.Wrappers = cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	c.Temporary = cowAllocation(uint64(unsafe.Sizeof(COWPrepared{}))) + cowAllocation(uint64(opts.ResourceSlots)*uint64(unsafe.Sizeof(COWResourceID{}))) +
		cowAllocation(uint64(opts.ResourceSlots)*uint64(unsafe.Sizeof(cowResourceOwner{})))
	c.Resources = opts.ResourceBytes
	c.Extra = opts.ExtraBytes
	if w.ordered != nil {
		ordered, err := w.ordered.estimate(entries)
		if err != nil {
			return c, err
		}
		for _, pair := range []struct {
			dst *uint64
			n   uint64
		}{
			{&c.Nodes, ordered.Nodes}, {&c.Payload, ordered.Payload},
			{&c.Wrappers, ordered.Wrappers}, {&c.Temporary, ordered.Temporary},
		} {
			if !add(pair.dst, pair.n) {
				return c, ErrCOWCapacity
			}
		}
		if ordered.Height > c.Height {
			c.Height = ordered.Height
		}
	}
	total := uint64(0)
	for _, n := range []uint64{c.Nodes, c.Payload, c.Wrappers, c.Temporary, c.Resources, c.Extra} {
		if !add(&total, n) {
			return c, ErrCOWCapacity
		}
	}
	return c, nil
}

// Estimate performs no node walk, payload copy or mutation. Caller input must
// remain stable until Prepare returns. Estimates include final batch height.
func (w *COWWriter) Estimate(entries []COWMutation, opts COWPrepareOptions) (COWCharge, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return COWCharge{}, ErrCOWClosed
	}
	if deltaIdx, inL0, ok := w.canUseL0(entries, opts); ok {
		c, _, _, _, _, err := w.estimateSingleDelta(entries[0], opts, deltaIdx, inL0, false, cowL1MergePlan{}, false)
		return c, err
	}
	if deltaIdx, plan, ok := w.canMergeToL1(entries, opts); ok {
		c, _, _, _, _, err := w.estimateSingleDelta(entries[0], opts, deltaIdx, false, true, plan, false)
		return c, err
	}
	return w.estimate(entries, opts)
}

type COWPrepared struct {
	writer            *COWWriter
	private           *btree.Map[string, cowValue]
	ordered           *cowOrderedTree
	delta             []cowDeltaEntry
	deltaTail         []cowDeltaEntry
	deltaTailAppended bool
	l1                *cowDeltaL1
	deltaAdded        int
	privateMaxKey     string
	hasPrivateMax     bool
	root              *COWRoot
	charge            COWCharge
	ids               []COWResourceID
	owners            []cowResourceOwner
	maxEntries        int
	deferredBudget    *COWBudget
	deferredCharge    uint64
	logicalBytes      int64
	logicalOverflow   bool
	drainClaimed      bool
}

func (p *COWPrepared) Charge() COWCharge { return p.charge }

// Root is the already allocated staged header for constructing C2's successor
// cut before a frame. It acquires its first source reference only at Publish.
// Before then it must not be exposed, retained, or used for pointer resolution.
func (p *COWPrepared) Root() *COWRoot { return p.root }

// COWProjectedGenerationRefusal describes a single denied history projection
// with a fitting estimate on an empty native generation. Its weak identity
// cannot keep alive the old root, generation or any of their resources.
type COWProjectedGenerationRefusal struct {
	generation                                                 weak.Pointer[cowGeneration]
	HistoryBytes, PendingBytes, DemandBytes, FreshHistoryBytes uint64
}

func (r *COWProjectedGenerationRefusal) Error() string { return ErrCOWCapacity.Error() }
func (r *COWProjectedGenerationRefusal) Unwrap() error { return ErrCOWCapacity }

// NeedsProjectedGenerationRollover rechecks the old owner's exact prospective demand.
// Caller holds this root's original edge throughout.
func (r *COWRoot) NeedsProjectedGenerationRollover(refusal *COWProjectedGenerationRefusal) bool {
	if !r.RetainOwned() {
		return false
	}
	defer func() {
		retired := r.Release()
		retired.Drain()
	}()
	g := r.generation
	if g == nil || g != refusal.generation.Value() {
		return false
	}
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	return !cowGenerationFits(g, refusal.DemandBytes)
}

// The subtraction guards ensure the intermediate sum cannot overflow.
func cowGenerationFits(g *cowGeneration, demand uint64) bool {
	history := g.generationHistoryLocked()
	limit := g.budget.limits.MaxGenerationBytes
	return cowFits(limit, history, g.pending) && cowFits(limit, history+g.pending, demand)
}

func (w *COWWriter) Prepare(entries []COWMutation, opts COWPrepareOptions) (*COWPrepared, error) {
	return w.prepare(nil, entries, opts, false)
}

// PrepareInto stages a resource-free publication into caller-owned storage dst
// so single-point writers can reuse one pre-admitted COWPrepared wrapper without
// per-op heap allocation. When dst is nil or opts.ResourceSlots != 0, it falls
// back to allocating a fresh COWPrepared.
func (w *COWWriter) PrepareInto(dst *COWPrepared, entries []COWMutation, opts COWPrepareOptions) (*COWPrepared, error) {
	if opts.ResourceSlots != 0 {
		dst = nil
	}
	return w.prepare(dst, entries, opts, false)
}

// PrepareNativeProjected reports only a pre-reservation generation projection
// on a nonempty native tail. All capacities and actual reservations are shared
// with Prepare.
func (w *COWWriter) PrepareNativeProjected(entries []COWMutation) (*COWPrepared, error) {
	return w.prepare(nil, entries, COWPrepareOptions{}, true)
}

func (w *COWWriter) prepare(dst *COWPrepared, entries []COWMutation, opts COWPrepareOptions, observeProjected bool) (*COWPrepared, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrCOWClosed
	}
	if w.pending != nil {
		return nil, ErrCOWPending
	}
	var singleOwnedKey string
	var singlePrevVal cowValue
	var singleKeyExists, singleBrandNew bool
	var c COWCharge
	var err error
	var mergePlan cowL1MergePlan
	deltaIdx, inL0, useL0 := w.canUseL0(entries, opts)
	mergeToL1 := false
	if useL0 {
		c, singleOwnedKey, singlePrevVal, singleKeyExists, singleBrandNew, err = w.estimateSingleDelta(entries[0], opts, deltaIdx, inL0, false, cowL1MergePlan{}, dst != nil)
	} else if idx, plan, ok := w.canMergeToL1(entries, opts); ok {
		deltaIdx, mergePlan, mergeToL1 = idx, plan, true
		c, singleOwnedKey, singlePrevVal, singleKeyExists, singleBrandNew, err = w.estimateSingleDelta(entries[0], opts, deltaIdx, false, true, mergePlan, dst != nil)
	} else {
		c, err = w.estimate(entries, opts)
		if err == nil && dst != nil && opts.ResourceSlots == 0 {
			temp := cowAllocation(uint64(unsafe.Sizeof(COWPrepared{})))
			if c.Temporary >= temp {
				c.Temporary -= temp
			}
		}
	}
	if err != nil {
		return nil, err
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrCOWClosed
	}
	// Distinct private forks may prepare ordinary rows concurrently. New
	// resource identities still use the original single-writer reservation
	// protocol; do not race two private resource ledgers in this generation.
	if opts.ResourceSlots != 0 && g.activeWriters != 1 {
		b.mu.Unlock()
		return nil, ErrCOWPending
	}
	total := c.Total()
	if g.frozen || !b.inFlightFitsLocked(total) {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	if !cowGenerationFits(g, c.History()) {
		// Preserve the original short circuit. This diagnostic proof reads
		// other dimensions without reclaiming scratch or reserving bytes.
		historyAndReserved, ok := cowAdd(b.stats.HistoryBytes, b.stats.ReservedBytes)
		retirement, ok2 := cowAdd(historyAndReserved, b.stats.DeferredBytes)
		if !observeProjected || g.nativeSource == nil || w.private.Len()+w.deltaAdded == 0 ||
			!ok || !ok2 || !cowFits(b.limits.MaxRetiredBytes, retirement, total) ||
			!cowFits(b.limits.MaxTotalBytes, b.stats.TotalBytes, total) {
			b.mu.Unlock()
			return nil, ErrCOWCapacity
		}
		history, pending := g.generationHistoryLocked(), g.pending
		freshBase := cowWriterConstructionCharge(b.limits.MaxResources)
		sourceCharge, limit := g.nativeSource.lease.charge, b.limits.MaxGenerationBytes
		b.mu.Unlock()

		// Rollover moves the active tail to its original captured source.
		// The next writer is the same ordinary, empty native constructor.
		// Reuse the unchanged reservation estimator over that shape.
		empty := COWWriter{private: new(btree.Map[string, cowValue]), generation: g}
		fresh, err := empty.estimate(entries, opts)
		if err != nil || !cowFits(limit, freshBase, sourceCharge) ||
			!cowFits(limit, freshBase+sourceCharge, fresh.History()) {
			return nil, ErrCOWCapacity
		}
		return nil, &COWProjectedGenerationRefusal{
			generation: weak.Make(g), HistoryBytes: history, PendingBytes: pending,
			DemandBytes: c.History(), FreshHistoryBytes: freshBase + sourceCharge + fresh.History(),
		}
	}
	if !b.retirementFitsLocked(total) ||
		opts.ResourceSlots > b.limits.MaxResources-len(g.ids) || !b.addLocked(total) {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	g.pending += total
	b.stats.ReservedBytes += total
	b.mu.Unlock()
	var p *COWPrepared
	var logicalDelta cowLogicalDelta
	var actualNodes uint64
	if useL0 || mergeToL1 {
		nextDeltaAdded := w.deltaAdded
		if singleBrandNew {
			nextDeltaAdded++
		}
		if dst != nil {
			*dst = COWPrepared{
				writer:        w,
				private:       w.private,
				privateMaxKey: w.privateMaxKey,
				hasPrivateMax: w.hasPrivateMax,
				deltaAdded:    nextDeltaAdded,
				charge:        c,
				maxEntries:    w.private.Len() + nextDeltaAdded,
			}
			p = dst
		} else {
			p = &COWPrepared{
				writer:        w,
				private:       w.private,
				privateMaxKey: w.privateMaxKey,
				hasPrivateMax: w.hasPrivateMax,
				deltaAdded:    nextDeltaAdded,
				charge:        c,
				maxEntries:    w.private.Len() + nextDeltaAdded,
			}
		}
		e := entries[0]
		ownedKey := singleOwnedKey
		if !singleKeyExists {
			ownedKey = string(e.Key)
		}
		value := cowEncodeValue(e)
		entry := cowDeltaEntry{key: ownedKey, value: value}
		if useL0 {
			p.l1 = w.l1
			if len(w.delta) == 0 {
				buf := make([]cowDeltaEntry, 1, cowDeltaL0MaxEntries)
				buf[0] = entry
				p.delta = buf[:1:1]
				p.deltaTail = buf[:1:cowDeltaL0MaxEntries]
				actualNodes = cowDeltaCharge(cowDeltaL0MaxEntries)
			} else if w.canAppendDeltaTail(inL0, deltaIdx) {
				tail := w.deltaTail[:len(w.delta)+1]
				tail[len(w.delta)] = entry
				p.delta = tail[:len(tail):len(tail)]
				p.deltaTail = tail
				p.deltaTailAppended = true
				actualNodes = 0
			} else if inL0 {
				p.delta = cowDeltaReplace(w.delta, deltaIdx, entry)
				actualNodes = cowDeltaCharge(len(p.delta))
			} else {
				p.delta = cowDeltaInsert(w.delta, deltaIdx, entry)
				actualNodes = cowDeltaCharge(len(p.delta))
			}
		} else {
			p.l1, actualNodes = cowDeltaL1BuildMerge(w.l1, w.delta, deltaIdx, entry, mergePlan)
			p.delta = nil
		}
		logicalDelta.replace(len(e.Key), value, singlePrevVal)
		p.logicalBytes, p.logicalOverflow = logicalDelta.bytes, logicalDelta.overflow
		p.root = &COWRoot{tree: p.private, delta: p.delta, l1: p.l1, deltaAdded: p.deltaAdded, generation: g}
	} else {
		if dst != nil {
			*dst = COWPrepared{writer: w, private: w.private.ForkImmutable(), charge: c, maxEntries: w.private.Len() + w.deltaAdded + len(entries)}
			p = dst
		} else {
			p = &COWPrepared{writer: w, private: w.private.ForkImmutable(), charge: c,
				ids: make([]COWResourceID, 0, opts.ResourceSlots), owners: make([]cowResourceOwner, 0, opts.ResourceSlots), maxEntries: w.private.Len() + w.deltaAdded + len(entries)}
		}
		// Flush buffered front layers into the forked private tree in precedence
		// order (L1 first, then L0 so L0 shadows L1). The opening root already
		// reflects L1+L0, so do not adjust logicalDelta here.
		if w.l1 != nil {
			for _, ch := range w.l1.chunks {
				for _, de := range ch.entries {
					p.private.Set(de.key, de.value)
				}
			}
		}
		for _, de := range w.delta {
			p.private.Set(de.key, de.value)
		}
		if w.ordered != nil {
			p.ordered = w.ordered.fork()
			logicalDelta = p.ordered.applyValidatedFrom(w.ordered, entries)
		}
		routing := w.ordered.routing(entries)
		if w.ordered == nil && len(entries) >= 2 && cowEntriesStrictlySortedNonDelete(entries) {
			logicalDelta = applySortedNonDeleteCOWMutations(p.private, entries, c.Payload)
		} else {
			for index, e := range entries {
				if routing.routes(index) {
					// A newly qualified version replaces any prior raw incarnation.
					// Deleting a missing key can still copy its raw search path. Only
					// real migration work is included in the original reservation; use
					// the staged tree because earlier operations may insert this key.
					if p.private.Len() != 0 && cowHasKey(p.private, e.Key) {
						if previous, removed := cowDeleteKey(p.private, e.Key); removed {
							logicalDelta.replace(len(e.Key), "", previous)
						}
					}
					continue
				}
				// Lookup returns an existing owned key; safe builds compare bytes
				// directly, without temporary key conversions before or after admission.
				if e.Remove {
					if previous, removed := cowDeleteKey(p.private, e.Key); removed {
						logicalDelta.replace(len(e.Key), "", previous)
					}
					continue
				}
				value := cowEncodeValue(e)
				previous, _ := btree.SetBytes(p.private, e.Key, value)
				logicalDelta.replace(len(e.Key), value, previous)
			}
		}
		p.logicalBytes, p.logicalOverflow = logicalDelta.bytes, logicalDelta.overflow
		p.private = p.private.SealOwned()
		p.privateMaxKey, _, p.hasPrivateMax = p.private.Max()
		p.root = &COWRoot{tree: p.private, generation: g}
		if p.ordered != nil {
			p.root.ordered = p.ordered.immutableCopy()
		}
		actualNodes = p.private.AllocationCharge()
		if p.ordered != nil {
			orderedNodes := p.ordered.nodeAllocationCharge()
			if orderedNodes > ^uint64(0)-actualNodes {
				actualNodes = ^uint64(0)
			} else {
				actualNodes += orderedNodes
			}
		}
	}
	if err := p.reconcileNodes(actualNodes); err != nil {
		// No resource callback or published source edge exists yet. End every
		// private postimage alias before returning the original reservation.
		if p.deltaTailAppended {
			p.deltaTail[len(p.deltaTail)-1] = cowDeltaEntry{}
			w.deltaTail = nil
		}
		p.private, p.ordered, p.delta, p.deltaTail, p.deltaTailAppended, p.l1, p.deltaAdded, p.privateMaxKey, p.hasPrivateMax, p.root, p.ids, p.owners, p.writer = nil, nil, nil, nil, false, nil, 0, "", false, nil, nil, nil, nil
		b.mu.Lock()
		g.pending -= total
		b.stats.ReservedBytes -= total
		b.stats.TotalBytes -= total
		b.mu.Unlock()
		return nil, err
	}
	w.pending = p
	return p, nil
}

func cowEntriesStrictlySortedNonDelete(entries []COWMutation) bool {
	if len(entries) < 2 {
		return false
	}
	if entries[0].Remove {
		return false
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Remove || bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			return false
		}
	}
	return true
}

func applySortedNonDeleteCOWMutations(tr *btree.Map[string, cowValue], entries []COWMutation, admittedPayload uint64) cowLogicalDelta {
	var totalKeyBytes, totalValBytes uint64
	for _, e := range entries {
		totalKeyBytes += uint64(len(e.Key))
		if _, isConst := cowIsEmptyConstantValue(e); !isConst {
			totalValBytes += uint64(cowValueMetadataBytes + len(e.Value))
		}
	}
	var valArena, keyArena []byte
	valCharge := uint64(0)
	if totalValBytes > 0 {
		valCharge = cowAllocation(totalValBytes)
		if valCharge <= admittedPayload {
			valArena = make([]byte, totalValBytes)
		}
	}
	if totalKeyBytes > 0 {
		keyCharge := cowAllocation(totalKeyBytes)
		if keyCharge <= admittedPayload-valCharge {
			keyArena = make([]byte, totalKeyBytes)
		}
	}
	valOff := 0
	keyOff := 0
	var logicalDelta cowLogicalDelta
	var pendingPrev cowValue
	pendingPrevIdx := -1

	keyAt := func(i int) []byte { return entries[i].Key }
	onReplace := func(i int, prev cowValue) {
		pendingPrev = prev
		pendingPrevIdx = i
	}
	valAt := func(i int) cowValue {
		var val cowValue
		if valArena != nil {
			val, valOff = cowEncodeValueInto(valArena, valOff, entries[i])
		} else {
			val = cowEncodeValue(entries[i])
		}
		var prev cowValue
		if pendingPrevIdx == i {
			prev = pendingPrev
			pendingPrev = ""
			pendingPrevIdx = -1
		}
		logicalDelta.replace(len(entries[i].Key), val, prev)
		return val
	}
	ownKey := func(i int) string {
		k := entries[i].Key
		if keyArena != nil && len(k) > 0 {
			dst := keyArena[keyOff : keyOff+len(k)]
			copy(dst, k)
			keyOff += len(k)
			return bytesToStringNoCopy(dst)
		}
		return string(k)
	}
	btree.SetSortedBytes(tr, len(entries), keyAt, valAt, ownKey, onReplace)
	return logicalDelta
}

// reconcileNodes returns only an unspent part of the original proven node
// reservation. The counter includes every actual copied/grown backing, even
// ones discarded while preparing. Payload, wrappers and resource envelopes
// retain their original charge. Caller holds the writer lock before publication.
func (p *COWPrepared) reconcileNodes(actual uint64) error {
	if actual > p.charge.Nodes {
		return fmt.Errorf("COW node allocation charge %d exceeds reservation %d: %w", actual, p.charge.Nodes, ErrCOWCapacity)
	}
	unused := p.charge.Nodes - actual
	if unused == 0 {
		return nil
	}
	g := p.writer.generation
	b := g.budget
	b.mu.Lock()
	g.pending -= unused
	b.stats.ReservedBytes -= unused
	b.stats.TotalBytes -= unused
	p.charge.Nodes = actual
	b.mu.Unlock()
	return nil
}

func (w *COWWriter) HasResource(id COWResourceID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	b := w.generation.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, got := range w.generation.ids {
		if got == id {
			return true
		}
	}
	return false
}

// AttachResources transfers one independently retained existing physical owner.
// IDs must all be newly introduced, distinct identities. Capacity was reserved
// by Prepare; no append here can allocate. On refusal ownership stays with caller.
// The callback is executed once on cancellation or last generation release.
func (p *COWPrepared) AttachResources(ids []COWResourceID, release func()) error {
	w := p.writer
	if w == nil {
		return ErrCOWClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending != p {
		return ErrCOWClosed
	}
	if len(ids) == 0 || release == nil {
		return fmt.Errorf("COW resource owner requires distinct identities and release")
	}
	if len(ids) > cap(p.ids)-len(p.ids) || len(p.owners) == cap(p.owners) {
		return ErrCOWCapacity
	}
	b := w.generation.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, id := range ids {
		for _, old := range w.generation.ids {
			if old == id {
				return fmt.Errorf("COW resource already retained")
			}
		}
		for _, old := range p.ids {
			if old == id {
				return fmt.Errorf("COW resource already staged")
			}
		}
		for _, old := range ids[:i] {
			if old == id {
				return fmt.Errorf("duplicate COW resource identity")
			}
		}
	}
	p.ids = append(p.ids, ids...)
	p.owners = append(p.owners, cowResourceOwner{release: release})
	return nil
}

// Publish only transfers already allocated state. The external writer sequencer
// must keep the prepared basis exclusive through its WAL/ACK and complete-cut
// swap. This function has no fallible allocation, callback, IO or rebase.
func (p *COWPrepared) Publish() *COWRoot {
	w := p.writer
	if w == nil {
		panic("COW preparation consumed")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending != p {
		panic("COW preparation is not pending")
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	g.pending -= p.charge.Total()
	b.stats.ReservedBytes -= p.charge.Total()
	b.stats.TotalBytes -= p.charge.Temporary
	g.history += p.charge.History()
	b.stats.HistoryBytes += p.charge.History()
	g.refs++
	g.ids = append(g.ids, p.ids...)
	g.owners = append(g.owners, p.owners...)
	b.mu.Unlock()
	w.private = p.private
	w.ordered = p.ordered
	w.delta = p.delta
	w.deltaTail = p.deltaTail
	w.l1 = p.l1
	w.deltaAdded = p.deltaAdded
	w.privateMaxKey = p.privateMaxKey
	w.hasPrivateMax = p.hasPrivateMax
	if n := p.maxEntries; n > w.maxEntries {
		w.maxEntries = n
	}
	w.pending = nil
	root := p.root
	root.refs.Store(1)
	p.writer = nil
	p.private = nil
	p.ordered = nil
	p.delta = nil
	p.deltaTail = nil
	p.deltaTailAppended = false
	p.l1 = nil
	p.deltaAdded = 0
	p.privateMaxKey = ""
	p.hasPrivateMax = false
	p.root = nil
	p.ids = nil
	p.owners = nil
	return root
}

func (p *COWPrepared) Cancel() COWRetirement {
	w := p.writer
	if w == nil {
		return COWRetirement{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending != p {
		return COWRetirement{}
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	g.pending -= p.charge.Total()
	b.stats.ReservedBytes -= p.charge.Total()
	deferred := uint64(0)
	if len(p.owners) != 0 {
		// The prepared wrapper is also the shared cleanup claim. Retain its
		// full staging charge and resource owners until callbacks finish.
		deferred = p.charge.Temporary + p.charge.Resources
		p.deferredBudget = b
		p.deferredCharge = deferred
		b.stats.DeferredBytes += deferred
		b.stats.RetiredBytes += deferred
	}
	b.stats.TotalBytes -= p.charge.Total() - deferred
	b.mu.Unlock()
	if p.deltaTailAppended {
		p.deltaTail[len(p.deltaTail)-1] = cowDeltaEntry{}
		w.deltaTail = nil
	}
	p.deltaTail = nil
	p.deltaTailAppended = false
	w.pending = nil
	r := COWRetirement{}
	if deferred != 0 {
		r.prepared = p
	} else {
		p.owners = nil
	}
	p.writer = nil
	p.private = nil
	p.ordered = nil
	p.delta = nil
	p.l1 = nil
	p.deltaAdded = 0
	p.privateMaxKey = ""
	p.hasPrivateMax = false
	p.root = nil
	p.ids = nil
	return r
}

func (p *COWPrepared) drain() {
	b := p.deferredBudget
	b.mu.Lock()
	if p.drainClaimed {
		b.mu.Unlock()
		return
	}
	p.drainClaimed = true
	b.mu.Unlock()
	cowReleaseOwners(p.owners)
	b.mu.Lock()
	charge := p.deferredCharge
	p.owners, p.deferredCharge = nil, 0
	p = nil
	b.stats.TotalBytes -= charge
	b.stats.DeferredBytes -= charge
	b.stats.RetiredBytes -= charge
	b.releaseControlLocked()
	b.mu.Unlock()
	b.releaseMaintenanceReservation()
}

// ReserveRawRolloverSource reserves the cache generation's source before its
// first dirty acceptance. Standalone writers retain Freeze-time admission.
// The original generation owns the slot through all actual root/view edges.
func (w *COWWriter) ReserveRawRolloverSource() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.pending != nil {
		return ErrCOWPending
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrCOWClosed
	}
	if g.frozen || g.rawSourceReserved || b.stats.Sources >= b.limits.MaxSources {
		return ErrCOWCapacity
	}
	g.rawSourceReserved = true
	b.stats.Sources++
	return nil
}

// CancelRawRolloverSource unwinds only an unpublished first-dirty preparation.
// The cache serializes this operation with all writes and rollover.
func (w *COWWriter) CancelRawRolloverSource() {
	w.mu.Lock()
	defer w.mu.Unlock()
	g := w.generation
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if g.frozen || !g.rawSourceReserved || w.pending != nil {
		panic("invalid raw source cancellation")
	}
	g.rawSourceReserved = false
	b.stats.Sources--
}

// Freeze is pre-frame admission for a write-side source rollover. C2 reserves
// its complete successor cut and fresh writer before making this transition.
// Freeze cannot run while a preparation is pending. It never walks records.
func (w *COWWriter) Freeze() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrCOWClosed
	}
	if w.pending != nil {
		return ErrCOWPending
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if g.frozen {
		return nil
	}
	if g.activeWriters != 1 || g.pending != 0 {
		return ErrCOWPending
	}
	if b.closed {
		return ErrCOWClosed
	}
	if (!g.rawSourceReserved && b.stats.Sources >= b.limits.MaxSources) || !cowFits(b.limits.MaxRetiredBytes, b.stats.RetiredBytes, g.history-g.reservedCharge) {
		return ErrCOWCapacity
	}
	g.frozen = true
	g.retired = true
	if !g.rawSourceReserved {
		b.stats.Sources++
	}
	b.stats.RetiredBytes += g.history - g.reservedCharge
	return nil
}

func (w *COWWriter) Close() COWRetirement {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return COWRetirement{}
	}
	if w.pending != nil {
		panic("cancel COW preparation before writer Close")
	}
	w.closed = true
	w.private = nil
	w.ordered = nil
	w.delta = nil
	w.deltaTail = nil
	w.l1 = nil
	w.deltaAdded = 0
	w.privateMaxKey = ""
	w.hasPrivateMax = false
	g := w.generation
	b := g.budget
	b.mu.Lock()
	g.activeWriters--
	if g.activeWriters < 0 {
		panic("COW generation writer released twice")
	}
	if g.activeWriters == 0 && !g.retired {
		g.retired = true
		b.stats.RetiredBytes += g.history - g.reservedCharge
	}
	b.mu.Unlock()
	w.generation = nil
	return g.release()
}

// COWRoot is one immutable read header. Retain/Release are source/cut ownership,
// not public independently closable views. Release returns deferred IO work.
// refs is managed atomically: RetainOwned uses a lock-free CAS (refusing <=0),
// and non-final Release is a single atomic decrement. All fields other than refs
// are immutable after Publish stores refs=1 until the final Release observes 0.
type COWRoot struct {
	tree       *btree.Map[string, cowValue]
	ordered    *cowOrderedTree
	delta      []cowDeltaEntry
	l1         *cowDeltaL1
	deltaAdded int
	generation *cowGeneration
	refs       atomic.Int64
}

func (r *COWRoot) hasDelta() bool {
	return len(r.delta) > 0 || r.l1 != nil
}

// AcquireCapturedSource claims one already admitted original source cohort AND
// one exact root edge against this root's SAME budget. The caller owns both on
// success. It admits no producer bytes after accepted mutation. It does not
// freeze shared generations or alter private fork publication/cancellation.
// The caller attaches this receipt to the original source, shares that owner
// across views, and closes it only after detaching its last root/resource edge.
func (r *COWRoot) AcquireCapturedSource(bytes uint64) (*COWNativeSourceReservation, error) {
	if !r.RetainOwned() {
		return nil, ErrCOWClosed
	}
	if r.tree == nil {
		retired := r.Release()
		retired.Drain()
		return nil, ErrCOWClosed
	}
	cell := r.generation.nativeSource
	if cell == nil {
		retired := r.Release()
		retired.Drain()
		return nil, ErrCOWCapacity
	}
	b := r.generation.budget
	b.mu.Lock()
	err := cell.claimLocked(bytes)
	b.mu.Unlock()
	if err != nil {
		retired := r.Release()
		retired.Drain()
		return nil, err
	}
	// The retained reference from RetainOwned belongs to this admitted source.
	return cell, nil
}

func (r *COWRoot) Retain() bool {
	if !r.RetainOwned() {
		return false
	}
	b := r.generation.budget
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		retired := r.Release()
		retired.Drain()
		return false
	}
	return true
}

// RetainOwned shares an immutable root already owned by the caller. Native
// maintenance uses it to transfer an existing source edge into its preadmitted
// successor, including after budget Close. It acquires no generation, source
// slot, pin, allocation, or admission authority. The caller must keep its own
// root edge through this call; a released root cannot be revived.
func (r *COWRoot) RetainOwned() bool {
	if r == nil {
		return false
	}
	for {
		cur := r.refs.Load()
		if cur <= 0 {
			return false
		}
		if r.refs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// COWGenerationUsage is a point-in-time observation, not a reservation or an
// admission decision. PendingBytes includes other preparations sharing the
// original generation. HistoryBytes includes native source metadata.
type COWGenerationUsage struct {
	HistoryBytes, PendingBytes, MaxGenerationBytes uint64
}

// GenerationUsage borrows the caller's original root edge without retaining it
// or changing capacity.
func (r *COWRoot) GenerationUsage() (COWGenerationUsage, error) {
	if !r.RetainOwned() {
		return COWGenerationUsage{}, ErrCOWClosed
	}
	defer func() {
		retired := r.Release()
		retired.Drain()
	}()
	g := r.generation
	b := g.budget
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return COWGenerationUsage{}, ErrCOWClosed
	}
	usage := COWGenerationUsage{HistoryBytes: g.generationHistoryLocked(), PendingBytes: g.pending, MaxGenerationBytes: b.limits.MaxGenerationBytes}
	b.mu.Unlock()
	return usage, nil
}

func (r *COWRoot) Release() COWRetirement {
	next := r.refs.Add(-1)
	if next < 0 {
		panic("COW root released twice")
	}
	if next != 0 {
		return COWRetirement{}
	}
	generation := r.generation
	r.tree, r.ordered, r.delta, r.l1, r.deltaAdded, r.generation = nil, nil, nil, nil, 0, nil
	return generation.release()
}
func (r *COWRoot) Len() int {
	n := r.tree.Len() + r.deltaAdded
	if r.ordered != nil {
		n += r.ordered.len()
	}
	return n
}

// MaxKey returns the maximum key currently in this immutable root when it uses
// the standard non-ordered representation. Callers must hold the enclosing cut.
func (r *COWRoot) MaxKey() (string, bool) {
	if r == nil || r.ordered != nil {
		return "", false
	}
	maxKey, _, ok := r.tree.Max()
	if r.l1 != nil && len(r.l1.chunks) > 0 {
		lastChunk := r.l1.chunks[len(r.l1.chunks)-1]
		if len(lastChunk.entries) > 0 {
			k := lastChunk.entries[len(lastChunk.entries)-1].key
			if !ok || k > maxKey {
				maxKey, ok = k, true
			}
		}
	}
	if len(r.delta) > 0 {
		k := r.delta[len(r.delta)-1].key
		if !ok || k > maxKey {
			maxKey, ok = k, true
		}
	}
	return maxKey, ok
}
func (r *COWRoot) Height() int {
	h := r.tree.Height()
	if r.ordered != nil && r.ordered.height() > h {
		h = r.ordered.height()
	}
	return h
}

// OrderedCounts reports representation activation while the caller owns this
// immutable root. It creates no cursor or new ownership edge.
func (r *COWRoot) OrderedCounts() (logicalKeys, physicalVersions int) {
	if r == nil || r.ordered == nil {
		return 0, 0
	}
	return r.ordered.keys.Len(), r.ordered.len()
}
func (r *COWRoot) Get(key []byte) (COWRecord, bool) {
	if len(r.delta) > 0 {
		if idx, ok := cowDeltaFind(r.delta, key); ok {
			e := r.delta[idx]
			return e.value.record(e.key), true
		}
	}
	if r.l1 != nil {
		if e, ok := r.l1.get(key); ok {
			return e.value.record(e.key), true
		}
	}
	if r.ordered == nil {
		k, v, found := btree.GetEntryBytes(r.tree, key)
		if !found {
			return COWRecord{}, false
		}
		return v.record(k), true
	}
	record, ok := r.SeekGE(key, nil)
	if !ok || cowKeyCompare(record.Key, key) != 0 {
		return COWRecord{}, false
	}
	return record, true
}

// ScanOwned scans this immutable root in key order using the caller's existing
// source/cut edge. It admits no view or cursor and retains no callback or record.
// The caller must keep that edge until every borrowed key/value alias has been
// cleared. Callbacks run outside publication/cut gates and must not release the
// enclosing source while scanning. Returning false stops the scan immediately.
func (r *COWRoot) ScanOwned(iter func(COWRecord) bool) {
	if r.ordered != nil && r.ordered.len() != 0 {
		// Immutable projections share the original generation owner.
		r.scanOrderedMerged(iter)
		return
	}
	if r.hasDelta() {
		cowDeltaScanMerged(r.tree, r.delta, r.l1, iter)
		return
	}
	r.tree.Scan(func(key string, value cowValue) bool {
		return iter(value.record(key))
	})
}

// SeekGE uses the already owned cut/source reference and allocates no cursor,
// view or key conversion. Default builds search one path; treedb_safe uses
// read-only rank binary search, O(log N * height), to avoid unsafe conversions.
// The returned strings are immutable generation-owned data.
func (r *COWRoot) SeekGE(start, end []byte) (COWRecord, bool) {
	key, v, found := cowLookupGE(r.tree, start)
	if r.hasDelta() {
		bestKey, bestVal, bestFound := key, v, found
		if r.l1 != nil {
			if ci, ei, ok := r.l1.seekGE(start); ok {
				de := r.l1.chunks[ci].entries[ei]
				if !bestFound || de.key <= bestKey {
					bestKey, bestVal, bestFound = de.key, de.value, true
				}
			}
		}
		if len(r.delta) > 0 {
			if di := cowDeltaSeekGE(r.delta, start); di < len(r.delta) {
				de := r.delta[di]
				if !bestFound || de.key <= bestKey {
					bestKey, bestVal, bestFound = de.key, de.value, true
				}
			}
		}
		if !bestFound || (end != nil && cowKeyCompare(bestKey, end) >= 0) {
			return COWRecord{}, false
		}
		return bestVal.record(bestKey), true
	}
	if r.ordered != nil {
		if candidate, ok := r.ordered.seekGE(start, end); ok && (!found || candidate.Key < key) {
			return candidate, true
		}
	}
	if !found || (end != nil && cowKeyCompare(key, end) >= 0) {
		return COWRecord{}, false
	}
	return v.record(key), true
}

// SeekLT is the borrowed strict predecessor in [start,end); nil end means last.
// It has the same original source lifetime and allocation contract as SeekGE.
func (r *COWRoot) SeekLT(start, end []byte) (COWRecord, bool) {
	key, v, found := cowLookupLT(r.tree, end)
	if r.hasDelta() {
		bestKey, bestVal, bestFound := key, v, found
		if r.l1 != nil {
			if ci, ei, ok := r.l1.seekLT(end); ok {
				de := r.l1.chunks[ci].entries[ei]
				if !bestFound || de.key >= bestKey {
					bestKey, bestVal, bestFound = de.key, de.value, true
				}
			}
		}
		if len(r.delta) > 0 {
			if di := cowDeltaSeekLT(r.delta, end); di >= 0 {
				de := r.delta[di]
				if !bestFound || de.key >= bestKey {
					bestKey, bestVal, bestFound = de.key, de.value, true
				}
			}
		}
		if !bestFound || (start != nil && cowKeyCompare(bestKey, start) < 0) {
			return COWRecord{}, false
		}
		return bestVal.record(bestKey), true
	}
	if r.ordered != nil {
		if candidate, ok := r.ordered.seekLT(start, end); ok && (!found || candidate.Key > key) {
			return candidate, true
		}
	}
	if !found || (start != nil && cowKeyCompare(key, start) < 0) {
		return COWRecord{}, false
	}
	return v.record(key), true
}
