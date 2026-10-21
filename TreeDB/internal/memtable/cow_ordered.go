package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

// cowOrderedTree has one primary entry per canonical logical-key prefix. Its
// values are immutable timestamp postimages; no mutable writer or root is
// reachable from a published history. The physical key is retained once per
// version to preserve COWRecord's generation-owned alias contract.
type cowOrderedTree struct {
	keys       *btree.Map[string, *cowOrderedHistory]
	versions   int
	maxHistory int
	sealed     bool
	// Preparation-local receipt for numeric trees, including allocations made
	// by trees discarded during erase/reinsert cycles. Published readers do
	// not need or retain this accounting state.
	historyNodeCharge uint64
}

type cowOrderedValue struct {
	key   string
	value cowValue
}

// A singleton needs no timestamp tree. maximum/latest are also the latest-read
// shortcut for a multi-version history. private is set only on a newly allocated
// preparation-owned object and cleared before publication.
type cowOrderedHistory struct {
	versions *btree.Map[uint64, cowOrderedValue]
	maximum  uint64
	latest   cowOrderedValue
	count    int
	private  bool
}

type cowOrderedPairLayout struct {
	value *cowOrderedHistory
	key   string
}
type cowOrderedVersionPairLayout struct {
	value cowOrderedValue
	key   uint64
}

func newCOWOrderedTree() *cowOrderedTree {
	return &cowOrderedTree{keys: btree.NewMap[string, *cowOrderedHistory](btreeDefaultDegree)}
}

func cowOrderedForkCharge() uint64 {
	return cowAllocation(uint64(unsafe.Sizeof(cowOrderedTree{}))) +
		cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, *cowOrderedHistory]{})))
}

func cowOrderedConstructionCharge(withReservedRoot bool) uint64 {
	charge := cowOrderedForkCharge()
	if withReservedRoot {
		charge += cowOrderedForkCharge()
	}
	return charge
}

func (t *cowOrderedTree) fork() *cowOrderedTree {
	if t == nil {
		return newCOWOrderedTree()
	}
	var keys *btree.Map[string, *cowOrderedHistory]
	if t.sealed {
		keys = t.keys.ForkImmutable()
	} else {
		keys = t.keys.Copy()
	}
	return &cowOrderedTree{keys: keys, versions: t.versions, maxHistory: t.maxHistory}
}

func (t *cowOrderedTree) immutableCopy() *cowOrderedTree {
	return &cowOrderedTree{keys: t.keys.ImmutableCopy(), versions: t.versions, maxHistory: t.maxHistory, sealed: true}
}

// nodeAllocationCharge is the actual allocation receipt of this preparation,
// not a reachable-size estimate. In particular, discarded arrays and numeric
// trees still consumed part of its original admitted node reservation.
func (t *cowOrderedTree) nodeAllocationCharge() uint64 {
	if t == nil {
		return 0
	}
	charge, ok := cowAdd(t.keys.AllocationCharge(), t.historyNodeCharge)
	if !ok {
		return ^uint64(0)
	}
	return charge
}

func (t *cowOrderedTree) addHistoryNodeCharge(charge uint64) {
	if sum, ok := cowAdd(t.historyNodeCharge, charge); ok {
		t.historyNodeCharge = sum
	} else {
		t.historyNodeCharge = ^uint64(0)
	}
}

// cowOrderedRouting makes classification identical for admission, ordered
// preparation and the raw fallback. Sorted groups use only adjacent duplicate
// inspection; an unqualified raw-only batch never takes a quadratic fallback.
type cowOrderedRouting struct {
	basis          *cowOrderedTree
	entries        []COWMutation
	firstQualified int
	sorted         bool
}

func (t *cowOrderedTree) routing(entries []COWMutation) cowOrderedRouting {
	p := cowOrderedRouting{basis: t, entries: entries, firstQualified: len(entries), sorted: true}
	if t == nil {
		return p
	}
	for i := range entries {
		if entries[i].MVCC && p.firstQualified == len(entries) {
			p.firstQualified = i
		}
		if i > 0 && bytes.Compare(entries[i-1].Key, entries[i].Key) > 0 {
			p.sorted = false
		}
	}
	return p
}

func (p cowOrderedRouting) routes(index int) bool {
	if p.basis == nil {
		return false
	}
	e := p.entries[index]
	if e.MVCC || p.basis.contains(e.Key) {
		return true
	}
	if index <= p.firstQualified {
		return false
	}
	if _, _, ok := cowOrderedPrefix(e.Key); !ok {
		return false
	}
	for j := index - 1; j >= p.firstQualified; j-- {
		if p.sorted && !bytes.Equal(p.entries[j].Key, e.Key) {
			break
		}
		if p.entries[j].MVCC && bytes.Equal(p.entries[j].Key, e.Key) {
			return true
		}
	}
	return false
}

func (t *cowOrderedTree) len() int {
	if t == nil {
		return 0
	}
	return t.versions
}

func (t *cowOrderedTree) height() int {
	if t == nil {
		return 0
	}
	return t.keys.Height()
}

// Canonical validation is intentional: a raw malformed reserved-namespace key
// remains in the raw tree and is observed by the existing fail-closed readers.
func cowOrderedPrefix(key []byte) ([]byte, uint64, bool) {
	prefix, ok := mvcckey.VersionPrefix(key)
	if !ok {
		return nil, 0, false
	}
	return prefix, ^binary.BigEndian.Uint64(key[len(key)-mvcckey.TimestampSize:]), true
}

func (t *cowOrderedTree) contains(key []byte) bool {
	_, ok := t.get(key)
	return ok
}

func (t *cowOrderedTree) get(key []byte) (COWRecord, bool) {
	if t == nil {
		return COWRecord{}, false
	}
	prefix, ts, ok := cowOrderedPrefix(key)
	if !ok {
		return COWRecord{}, false
	}
	h, ok := cowOrderedPrimaryGet(t.keys, prefix)
	if !ok {
		return COWRecord{}, false
	}
	v, ok := h.exact(ts)
	if !ok {
		return COWRecord{}, false
	}
	return v.value.record(v.key), true
}

func (h *cowOrderedHistory) exact(ts uint64) (cowOrderedValue, bool) {
	if ts == h.maximum && h.count > 0 {
		return h.latest, true
	}
	if h.versions == nil {
		return cowOrderedValue{}, false
	}
	return h.versions.Get(ts)
}

func (h *cowOrderedHistory) predecessor(ts uint64) (uint64, cowOrderedValue, bool) {
	if h.count == 0 {
		return 0, cowOrderedValue{}, false
	}
	if ts >= h.maximum {
		return h.maximum, h.latest, true
	}
	if h.versions == nil {
		return 0, cowOrderedValue{}, false
	}
	var actual uint64
	var value cowOrderedValue
	found := false
	h.versions.Descend(ts, func(k uint64, v cowOrderedValue) bool {
		actual, value, found = k, v, true
		return false
	})
	return actual, value, found
}

func (h *cowOrderedHistory) successor(ts uint64) (uint64, cowOrderedValue, bool) {
	if h.count == 0 || ts > h.maximum {
		return 0, cowOrderedValue{}, false
	}
	if h.versions == nil || ts == h.maximum {
		return h.maximum, h.latest, true
	}
	var actual uint64
	var value cowOrderedValue
	found := false
	h.versions.Ascend(ts, func(k uint64, v cowOrderedValue) bool {
		actual, value, found = k, v, true
		return false
	})
	return actual, value, found
}

func (h *cowOrderedHistory) firstPhysical() cowOrderedValue { return h.latest }
func (h *cowOrderedHistory) lastPhysical() cowOrderedValue {
	if h.versions == nil {
		return h.latest
	}
	_, value, _ := h.versions.Min()
	return value
}

// suffixLowerTimestamp converts an arbitrary physical bound to an inclusive
// numeric timestamp bound. Short, long and malformed suffixes still have their
// exact byte ordering; the raw stream is responsible for returning those keys.
func cowOrderedSuffix(prefix string, bound []byte) (uint64, int, bool) {
	if len(bound) < len(prefix) || cowKeyCompare(prefix, bound[:len(prefix)]) != 0 {
		return 0, 0, false
	}
	suffix := bound[len(prefix):]
	n := len(suffix)
	if n > 8 {
		n = 8
	}
	var encoded uint64
	for i := 0; i < 8; i++ {
		encoded <<= 8
		if i < n {
			encoded |= uint64(suffix[i])
		}
	}
	return encoded, len(suffix), true
}

func (h *cowOrderedHistory) physicalGE(prefix string, bound []byte) (cowOrderedValue, bool) {
	encoded, suffixLen, same := cowOrderedSuffix(prefix, bound)
	if !same {
		return cowOrderedValue{}, false
	}
	if suffixLen > 8 {
		if encoded == ^uint64(0) {
			return cowOrderedValue{}, false
		}
		encoded++ // an eight-byte key is less than its longer extension
	}
	_, value, ok := h.predecessor(^encoded)
	return value, ok
}

func (h *cowOrderedHistory) physicalLT(prefix string, bound []byte) (cowOrderedValue, bool) {
	encoded, suffixLen, same := cowOrderedSuffix(prefix, bound)
	if !same {
		return h.lastPhysical(), true // the complete prefix is below bound
	}
	if suffixLen <= 8 {
		if encoded == 0 {
			return cowOrderedValue{}, false
		}
		encoded--
	}
	_, value, ok := h.successor(^encoded)
	return value, ok
}

func (t *cowOrderedTree) seekGE(start, end []byte) (COWRecord, bool) {
	if t == nil || t.versions == 0 {
		return COWRecord{}, false
	}
	var value cowOrderedValue
	found := false
	// Only the nearest lower prefix can contain a physical key at or above
	// start. Other candidates begin at a primary prefix at or above start.
	cowOrderedPrimaryDescend(t.keys, start, func(prefix string, h *cowOrderedHistory) bool {
		value, found = h.physicalGE(prefix, start)
		return false
	})
	if !found {
		cowOrderedPrimaryAscend(t.keys, start, func(_ string, h *cowOrderedHistory) bool {
			value, found = h.firstPhysical(), true
			return false
		})
	}
	if !found || (end != nil && cowKeyCompare(value.key, end) >= 0) {
		return COWRecord{}, false
	}
	return value.value.record(value.key), true
}

func (t *cowOrderedTree) seekLT(start, end []byte) (COWRecord, bool) {
	if t == nil || t.versions == 0 {
		return COWRecord{}, false
	}
	var value cowOrderedValue
	found := false
	if end == nil {
		_, h, ok := t.keys.Max()
		if ok {
			value, found = h.lastPhysical(), true
		}
	} else {
		cowOrderedPrimaryDescend(t.keys, end, func(prefix string, h *cowOrderedHistory) bool {
			value, found = h.physicalLT(prefix, end)
			return !found
		})
	}
	if !found || (start != nil && cowKeyCompare(value.key, start) < 0) {
		return COWRecord{}, false
	}
	return value.value.record(value.key), true
}

func (t *cowOrderedTree) scanOwned(iter func(COWRecord) bool) {
	if t == nil {
		return
	}
	t.keys.Scan(func(_ string, h *cowOrderedHistory) bool {
		if h.versions == nil {
			return iter(h.latest.value.record(h.latest.key))
		}
		keep := true
		h.versions.Reverse(func(_ uint64, v cowOrderedValue) bool {
			keep = iter(v.value.record(v.key))
			return keep
		})
		return keep
	})
}

// scanRangeOwned performs one lower-bound descent and then a linear borrowed
// walk. It allocates no iterator stack. It is used only by the mixed raw stream
// compatibility scan; pure ordered flushes use scanOwned directly.
func (t *cowOrderedTree) scanRangeOwned(start, end []byte, iter func(COWRecord) bool) bool {
	if t == nil || t.versions == 0 {
		return true
	}
	keep := true
	visit := func(v cowOrderedValue) bool {
		if end != nil && cowKeyCompare(v.key, end) >= 0 {
			return false
		}
		keep = iter(v.value.record(v.key))
		return keep
	}
	var floor string
	cowOrderedPrimaryDescend(t.keys, start, func(prefix string, h *cowOrderedHistory) bool {
		floor = prefix
		encoded, suffixLen, same := cowOrderedSuffix(prefix, start)
		if !same || (suffixLen > 8 && encoded == ^uint64(0)) {
			return false
		}
		if suffixLen > 8 {
			encoded++
		}
		if h.versions == nil {
			if h.maximum <= ^encoded {
				visit(h.latest)
			}
		} else {
			h.versions.Descend(^encoded, func(_ uint64, value cowOrderedValue) bool { return visit(value) })
		}
		return false
	})
	if !keep {
		return false
	}
	cowOrderedPrimaryAscend(t.keys, start, func(prefix string, h *cowOrderedHistory) bool {
		if prefix == floor {
			return true
		}
		if end != nil && cowKeyCompare(prefix, end) >= 0 {
			return false
		}
		if h.versions == nil {
			visit(h.latest)
		} else {
			h.versions.Reverse(func(_ uint64, value cowOrderedValue) bool { return visit(value) })
		}
		return keep
	})
	return keep
}

// The uncommon mixed-key compatibility path uses one ordered range walk per
// raw entry. This keeps ScanOwned allocation-free: O(V + R log K), with R raw
// entries, rather than creating an unadmitted pair of heap iterator stacks.
func (r *COWRoot) scanOrderedMerged(iter func(COWRecord) bool) {
	if r.tree.Len() == 0 {
		r.ordered.scanOwned(iter)
		return
	}
	var after string
	hasAfter, keep := false, true
	ordered := func(record COWRecord) bool {
		if hasAfter && record.Key == after {
			return true // the raw representation has precedence on an equal key
		}
		return iter(record)
	}
	r.tree.Scan(func(key string, value cowValue) bool {
		var start []byte
		if hasAfter {
			start = []byte(after)
		}
		if !r.ordered.scanRangeOwned(start, []byte(key), ordered) {
			keep = false
			return false
		}
		if !iter(value.record(key)) {
			keep = false
			return false
		}
		after, hasAfter = key, true
		return true
	})
	if keep {
		r.ordered.scanRangeOwned([]byte(after), nil, ordered)
	}
}

func (h *cowOrderedHistory) fork() *cowOrderedHistory {
	copy := *h
	copy.private = true
	if h.versions != nil {
		copy.versions = h.versions.ForkImmutable()
	}
	return &copy
}

func (h *cowOrderedHistory) set(ts uint64, value cowOrderedValue) bool {
	if h.count == 0 {
		h.maximum, h.latest, h.count = ts, value, 1
		return false
	}
	if h.versions == nil {
		if ts == h.maximum {
			h.latest = value
			return true
		}
		h.versions = btree.NewMap[uint64, cowOrderedValue](btreeDefaultDegree)
		if ts > h.maximum {
			h.versions.Load(h.maximum, h.latest)
			h.versions.Load(ts, value)
		} else {
			h.versions.Load(ts, value)
			h.versions.Load(h.maximum, h.latest)
		}
		h.count = 2
		if ts > h.maximum {
			h.maximum, h.latest = ts, value
		}
		return false
	}
	var replaced bool
	if ts > h.maximum {
		_, replaced = h.versions.Load(ts, value)
	} else {
		_, replaced = h.versions.Set(ts, value)
	}
	if !replaced {
		h.count++
	}
	if ts >= h.maximum {
		h.maximum, h.latest = ts, value
	}
	return replaced
}

func (h *cowOrderedHistory) remove(ts uint64) bool {
	if h.count == 0 {
		return false
	}
	if h.versions == nil {
		if ts != h.maximum {
			return false
		}
		h.maximum, h.latest, h.count = 0, cowOrderedValue{}, 0
		return true
	}
	if _, ok := h.versions.Delete(ts); !ok {
		return false
	}
	h.count--
	if ts == h.maximum {
		h.maximum, h.latest, _ = h.versions.Max()
	}
	if h.count == 1 {
		h.versions = nil
	}
	return true
}

// apply runs only after the caller has admitted estimate's complete promise.
// Each touched history gets one private fork for the entire atomic group.
// Newly allocated numeric trees and postimages are sealed before returning.
func (t *cowOrderedTree) applyFrom(basis *cowOrderedTree, entries []COWMutation) {
	routing := basis.routing(entries)
	dirty := make([]*cowOrderedHistory, 0, len(entries))
	for i, e := range entries {
		prefix, ts, canonical := cowOrderedPrefix(e.Key)
		if !canonical {
			continue // estimate rejects explicitly qualified malformed input
		}
		h, exists := cowOrderedPrimaryGet(t.keys, prefix)
		member := false
		if exists {
			_, member = h.exact(ts)
		}
		if !routing.routes(i) {
			continue
		}
		if e.Remove && !member {
			continue
		}
		if !exists {
			h = &cowOrderedHistory{private: true}
			dirty = append(dirty, h)
		} else if !h.private {
			h = h.fork()
			dirty = append(dirty, h)
		}
		if e.Remove {
			numeric := h.versions
			if h.remove(ts) {
				t.versions--
			}
			if numeric != nil && h.versions == nil {
				// Removing the penultimate version drops the private map. Its
				// allocation receipt must survive subsequent singleton writes
				// or a new promotion of this same history.
				t.addHistoryNodeCharge(numeric.AllocationCharge())
			}
		} else {
			value := cowOrderedValue{key: string(e.Key), value: cowEncodeValue(e)}
			if !h.set(ts, value) {
				t.versions++
			}
		}
		if h.count == 0 {
			cowOrderedPrimaryDelete(t.keys, prefix)
		} else {
			// Reuse the first retained physical key's prefix backing. Existing
			// Set replacements preserve the old primary key string anyway.
			t.keys.Set(h.latest.key[:len(h.latest.key)-mvcckey.TimestampSize], h)
			if h.count > t.maxHistory {
				t.maxHistory = h.count
			}
		}
	}
	for _, h := range dirty {
		if h.versions != nil {
			t.addHistoryNodeCharge(h.versions.AllocationCharge())
			h.versions = h.versions.ImmutableCopy()
		}
		h.private = false
	}
}

// This variant of the original reserve uses the concrete generic pair size.
// It deliberately includes discarded growth/split arrays as well as retained
// nodes. The original COW budget remains the sole admission authority.
func cowOrderedNodeReserve(height int, replacement, remove bool, maxEntries int, pairBytes uint64) uint64 {
	if height < 1 {
		height = 1
	}
	items := 2*btreeDefaultDegree - 1
	if maxEntries < items {
		items = maxEntries
	}
	if items < 1 {
		items = 1
	}
	pairs := cowAllocation(uint64(2*items) * pairBytes)
	children := cowAllocation(uint64(4*btreeDefaultDegree) * uint64(unsafe.Sizeof(uintptr(0))))
	header := cowAllocation(uint64(unsafe.Sizeof(cowNodeLayout{}))) + cowAllocation(uint64(unsafe.Sizeof([]uintptr{})))
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

func (t *cowOrderedTree) estimate(entries []COWMutation) (COWCharge, error) {
	var c COWCharge
	if len(entries) > int(^uint(0)>>1)-t.len() || len(entries) > int(^uint(0)>>1)-t.maxHistory {
		return c, ErrCOWCapacity
	}
	add := func(dst *uint64, n uint64) bool { v, ok := cowAdd(*dst, n); *dst = v; return ok }
	routing := t.routing(entries)
	maxKeys := t.keys.Len() + len(entries)
	c.Height = cowMaximumHeight(maxKeys)
	primaryPair := uint64(unsafe.Sizeof(cowOrderedPairLayout{}))
	historyPair := uint64(unsafe.Sizeof(cowOrderedVersionPairLayout{}))
	// Monotone prefix runs prove that each history occurs in a single run,
	// including descending-timestamp groups whose physical keys are unsorted.
	ascending, descending := true, true
	var previous []byte
	for i, e := range entries {
		prefix, ts, canonical := cowOrderedPrefix(e.Key)
		if e.MVCC && (!canonical || e.Timestamp != ts) {
			return c, fmt.Errorf("COW ordered mutation has invalid canonical key or timestamp")
		}
		if !routing.routes(i) {
			continue
		}
		if !canonical {
			return c, fmt.Errorf("COW ordered mutation is not canonical")
		}
		if previous != nil {
			comparison := bytes.Compare(previous, prefix)
			ascending = ascending && comparison <= 0
			descending = descending && comparison >= 0
		}
		previous = prefix
		if !e.Remove {
			if e.Flags&node.FlagPointer != 0 && e.Flags&node.FlagTombstone != 0 {
				return c, fmt.Errorf("COW entry has pointer and tombstone flags")
			}
			valueBytes, ok := cowValueSize(len(e.Value))
			if !ok || !add(&c.Payload, cowAllocation(uint64(len(e.Key)))) || !add(&c.Payload, cowAllocation(uint64(valueBytes))) {
				return c, ErrCOWCapacity
			}
		}
	}
	// Runs below skip raw entries, which cannot change an ordered history.
	// Monotone routed prefixes therefore prove one run per history even when
	// unqualified records interrupt that history's mutations.
	grouped := ascending || descending
	var primaryCopies, primaryGrowth uint64
	allPrimaryReplace, anyRemove := true, false
	for i := 0; i < len(entries); {
		if !routing.routes(i) {
			i++
			continue
		}
		prefix, _, _ := cowOrderedPrefix(entries[i].Key)
		j := i + 1
		operations := 1
		for j < len(entries) {
			if !routing.routes(j) {
				j++
				continue
			}
			next, _, canonical := cowOrderedPrefix(entries[j].Key)
			if !canonical || !bytes.Equal(prefix, next) {
				break
			}
			operations++
			j++
		}
		h, exists := cowOrderedPrimaryGet(t.keys, prefix)
		oldCount := 0
		if exists {
			oldCount = h.count
		}
		maximum := oldCount + operations
		if !grouped {
			maximum = oldCount + len(entries)
		}
		removes, allRemove, allReplace := false, true, exists
		for k := i; k < j; k++ {
			if !routing.routes(k) {
				continue
			}
			e := entries[k]
			removes = removes || e.Remove
			allRemove = allRemove && e.Remove
			_, ts, _ := cowOrderedPrefix(e.Key)
			member := false
			if exists {
				_, member = h.exact(ts)
			}
			allReplace = allReplace && member && !e.Remove
		}
		anyRemove = anyRemove || removes
		allPrimaryReplace = allPrimaryReplace && exists && !removes
		primaryReserve := cowOrderedNodeReserve(c.Height, exists && !removes, removes, maxKeys, primaryPair)
		if !add(&c.Nodes, primaryReserve) {
			return c, ErrCOWCapacity
		}
		copies := cowOrderedNodeReserve(c.Height, true, false, maxKeys, primaryPair)
		if removes {
			copies *= 2
		}
		if !add(&primaryCopies, copies) {
			return c, ErrCOWCapacity
		}
		if primaryReserve > copies && !add(&primaryGrowth, primaryReserve-copies) {
			return c, ErrCOWCapacity
		}
		// Remove/reinsert cycles can create several history wrappers; ordinary
		// groups fork one history and one numeric map exactly once.
		headers := 1
		if removes {
			headers = operations
		}
		if !add(&c.Wrappers, uint64(headers)*cowAllocation(uint64(unsafe.Sizeof(cowOrderedHistory{})))) {
			return c, ErrCOWCapacity
		}
		if maximum > 1 {
			height := cowMaximumHeight(maximum)
			full := cowOrderedNodeReserve(2, true, false, maximum, historyPair) / 2
			var reserve uint64
			if allReplace {
				reserve = uint64(operations) * cowOrderedNodeReserve(height, true, false, maximum, historyPair)
				bound := uint64(oldCount/31+1) * full
				if bound < reserve {
					reserve = bound
				}
			} else if allRemove {
				reserve = 5 * uint64(oldCount/31+1) * full
			} else if !removes {
				oldNodes := uint64(oldCount/31 + 1)
				if oldCount <= 1 {
					oldNodes = 0
				}
				pathNodes := uint64(operations) * uint64(height)
				if oldNodes > pathNodes {
					oldNodes = pathNodes
				}
				reserve = (oldNodes + 8*uint64(maximum/31+1)) * full
				if grouped {
					// Independently bound each mutation as the original raw COW
					// estimator does, then take the tighter valid envelope. The
					// bulk bound alone charges an entire history's growth for a
					// single insert. Promoting an existing singleton executes two
					// initial Loads; a fresh history's first operation needs none.
					calls := uint64(operations)
					if oldCount == 1 {
						calls++
					}
					perCall := cowOrderedNodeReserve(height, false, false, maximum, historyPair)
					// Compare before multiplying so the cap cannot overflow.
					if calls <= reserve/perCall {
						reserve = calls * perCall
					}
				}
			} else {
				reserve = uint64(operations) * cowOrderedNodeReserve(height, false, true, maximum, historyPair)
			}
			if !add(&c.Nodes, reserve) || !add(&c.Wrappers, 2*uint64(headers)*cowAllocation(uint64(unsafe.Sizeof(btree.Map[uint64, cowOrderedValue]{})))) {
				return c, ErrCOWCapacity
			}
		}
		i = j
	}
	// Primary paths are copied at most once per private fork. Preserve the
	// original COW cardinality/growth envelope rather than charging one full
	// root descent for every independent logical key.
	fullPrimary := cowOrderedNodeReserve(2, true, false, maxKeys, primaryPair) / 2
	bound := uint64(t.keys.Len()/31+1)*fullPrimary + primaryGrowth
	if !anyRemove {
		oldNodes := uint64(t.keys.Len()/31 + 1)
		pathNodes := uint64(len(entries)) * uint64(c.Height)
		if oldNodes > pathNodes {
			oldNodes = pathNodes
		}
		bound = oldNodes * fullPrimary
		if !allPrimaryReplace {
			bound += 8 * uint64(maxKeys/31+1) * fullPrimary
		}
	}
	// c.Nodes also contains numeric histories: cap only the primary component.
	primaryTotal := primaryCopies + primaryGrowth
	if bound < primaryTotal {
		c.Nodes -= primaryTotal - bound
	}
	if !add(&c.Wrappers, 2*cowOrderedForkCharge()) || !add(&c.Temporary, cowAllocation(uint64(len(entries))*uint64(unsafe.Sizeof((*cowOrderedHistory)(nil))))) {
		return c, ErrCOWCapacity
	}
	var total uint64
	for _, n := range []uint64{c.Nodes, c.Payload, c.Wrappers, c.Temporary} {
		if !add(&total, n) {
			return c, ErrCOWCapacity
		}
	}
	return c, nil
}

// cowOrderedCursor borrows an already owned immutable tree. Timestamp direction
// is reversed because physical MVCC suffixes sort by descending timestamp.
type cowOrderedCursor struct {
	tree    *cowOrderedTree
	primary btree.MapIter[string, *cowOrderedHistory]
	history btree.MapIter[uint64, cowOrderedValue]
	h       *cowOrderedHistory
	value   cowOrderedValue
	valid   bool
}

func (t *cowOrderedTree) cursor() cowOrderedCursor {
	if t == nil {
		return cowOrderedCursor{}
	}
	return cowOrderedCursor{tree: t, primary: t.keys.Iter()}
}

func (c *cowOrderedCursor) position(record COWRecord) bool {
	if c.tree == nil {
		return false
	}
	prefix := record.Key[:len(record.Key)-mvcckey.TimestampSize]
	c.valid = c.primary.Seek(prefix)
	if !c.valid {
		return false
	}
	c.h = c.primary.Value()
	if c.h.versions == nil {
		c.history.ResetTree(nil)
		c.value = c.h.latest
		return true
	}
	c.history.ResetTree(c.h.versions)
	ts := ^binary.BigEndian.Uint64([]byte(record.Key[len(record.Key)-mvcckey.TimestampSize:]))
	c.valid = c.history.Seek(ts)
	if c.valid {
		c.value = c.history.Value()
	}
	return c.valid
}

func (c *cowOrderedCursor) seekGE(key []byte) bool {
	if c.tree == nil {
		return false
	}
	// The primary floor is the only prefix whose versions can straddle key.
	// Retain that search's stack instead of looking up its resulting record a
	// second time. At most one adjacent primary key can supply the fallback.
	if cowOrderedPrimaryCursorFloor(&c.primary, c.tree.keys, key) {
		c.h = c.primary.Value()
		encoded, suffixLen, same := cowOrderedSuffix(c.primary.Key(), key)
		if same && (suffixLen <= mvcckey.TimestampSize || encoded != ^uint64(0)) {
			if suffixLen > mvcckey.TimestampSize {
				encoded++
			}
			if c.historyFloor(^encoded) {
				c.valid = true
				return true
			}
		}
		c.valid = c.primary.Next()
	} else {
		c.valid = c.primary.First()
	}
	return c.valid && c.enter(false)
}

func (c *cowOrderedCursor) seekLE(key []byte) bool {
	if c.tree == nil {
		return false
	}
	// Nil is the finite empty physical bound here; last is the distinct
	// unbounded reverse operation. The numeric order is opposite the physical
	// suffix order, so this direction needs a timestamp ceiling search.
	c.valid = cowOrderedPrimaryCursorFloor(&c.primary, c.tree.keys, key)
	if !c.valid {
		return false
	}
	c.h = c.primary.Value()
	encoded, suffixLen, same := cowOrderedSuffix(c.primary.Key(), key)
	if !same {
		return c.enter(true)
	}
	// A shorter suffix sorts before its zero-padded eight-byte extension;
	// an equal or longer suffix admits that complete eight-byte version.
	if suffixLen >= mvcckey.TimestampSize || encoded != 0 {
		if suffixLen < mvcckey.TimestampSize {
			encoded--
		}
		c.history.ResetTree(c.h.versions)
		if c.h.versions == nil {
			if c.h.maximum >= ^encoded {
				c.value = c.h.latest
				return true
			}
		} else if c.history.Seek(^encoded) {
			c.value = c.history.Value()
			return true
		}
	}
	c.valid = c.primary.Prev()
	return c.valid && c.enter(true)
}

// historyFloor positions the reusable numeric cursor at the greatest timestamp
// <= timestamp. The caller has already retained the owning primary history.
func (c *cowOrderedCursor) historyFloor(timestamp uint64) bool {
	c.history.ResetTree(c.h.versions)
	if c.h.versions == nil {
		if c.h.maximum > timestamp {
			return false
		}
		c.value = c.h.latest
		return true
	}
	found := c.history.Seek(timestamp)
	if !found {
		found = c.history.Last()
	} else if c.history.Key() > timestamp {
		found = c.history.Prev()
	}
	if found {
		c.value = c.history.Value()
	}
	return found
}

func (c *cowOrderedCursor) enter(last bool) bool {
	c.h = c.primary.Value()
	if c.h.versions == nil {
		c.history.ResetTree(nil)
		c.value = c.h.latest
		return true
	}
	c.history.ResetTree(c.h.versions)
	if last {
		c.valid = c.history.First()
	} else {
		c.valid = c.history.Last()
	}
	if c.valid {
		c.value = c.history.Value()
	}
	return c.valid
}

func (c *cowOrderedCursor) first() bool {
	c.valid = c.tree != nil && c.primary.First()
	return c.valid && c.enter(false)
}
func (c *cowOrderedCursor) last() bool {
	c.valid = c.tree != nil && c.primary.Last()
	return c.valid && c.enter(true)
}
func (c *cowOrderedCursor) next() bool {
	if !c.valid {
		return false
	}
	if c.h.versions != nil && c.history.Prev() {
		c.value = c.history.Value()
		return true
	}
	c.valid = c.primary.Next()
	return c.valid && c.enter(false)
}
func (c *cowOrderedCursor) prev() bool {
	if !c.valid {
		return false
	}
	if c.h.versions != nil && c.history.Next() {
		c.value = c.history.Value()
		return true
	}
	c.valid = c.primary.Prev()
	return c.valid && c.enter(true)
}
func (c *cowOrderedCursor) current() (COWRecord, bool) {
	if !c.valid {
		return COWRecord{}, false
	}
	return c.value.value.record(c.value.key), true
}
func (c *cowOrderedCursor) clear() { *c = cowOrderedCursor{} }

func cowOrderedCursorStackCharge(t *cowOrderedTree) (uint64, bool) {
	if t == nil {
		return 0, true
	}
	a, ok := cowCursorStackCharge(t.height())
	if !ok {
		return 0, false
	}
	b, ok := cowCursorStackCharge(cowMaximumHeight(t.maxHistory))
	if !ok {
		return 0, false
	}
	return cowAdd(a, b)
}
