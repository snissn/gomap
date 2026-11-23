package memtable

import (
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// cowDeltaMaxEntries bounds the immutable sorted front delta layered in front
// of raw (non-ordered) COWWriter/COWRoot B-trees. Single-op inline writes copy
// only this small sorted slice instead of path-copying degree-32 B-tree nodes,
// amortizing B-tree mutations across up to cowDeltaMaxEntries single ops.
// Multi-key batches, physical removes, and resource-bearing preparations flush
// the delta immediately so post-batch roots have delta == nil.
const cowDeltaMaxEntries = 32

type cowDeltaEntry struct {
	key   string
	value cowValue
}

func cowDeltaCharge(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return cowAllocation(uint64(n) * uint64(unsafe.Sizeof(cowDeltaEntry{})))
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

// cowDeltaScanMerged performs a zero-allocation ascending merge of tree and
// sorted delta, letting delta shadow any equal key in tree.
func cowDeltaScanMerged(tree *btree.Map[string, cowValue], delta []cowDeltaEntry, iter func(COWRecord) bool) {
	di := 0
	stopped := false
	tree.Scan(func(key string, value cowValue) bool {
		for di < len(delta) && delta[di].key < key {
			if !iter(delta[di].value.record(delta[di].key)) {
				stopped = true
				return false
			}
			di++
		}
		if di < len(delta) && delta[di].key == key {
			if !iter(delta[di].value.record(delta[di].key)) {
				stopped = true
				return false
			}
			di++
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
	for di < len(delta) {
		if !iter(delta[di].value.record(delta[di].key)) {
			return
		}
		di++
	}
}

// cowDeltaTraversal is a zero-allocation, byte-by-byte work-charged resumable
// cursor over an immutable sorted cowDeltaEntry slice for COWTraversal.
type cowDeltaTraversal struct {
	entries   []cowDeltaEntry
	idx       int
	ready     bool
	started   bool
	atstart   bool
	atend     bool
	pending   bool
	op        btree.Operation
	key       string
	lo, hi    int
	mid       int
	cmpOffset int
	cmpActive bool
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
	switch op {
	case btree.Close:
		d.ready = false
	case btree.First, btree.Last, btree.Next, btree.Prev:
	case btree.SeekGE, btree.SeekLE:
		d.lo = 0
		d.hi = len(d.entries)
		d.ready = false
	default:
		d.pending = false
		d.key = ""
		return btree.ErrOperation
	}
	return nil
}

func (d *cowDeltaTraversal) Step(work *COWTraversalWork) (bool, error) {
	if !d.pending {
		return true, nil
	}
	switch d.op {
	case btree.Close:
		if !work.Reserve(1, uint64(unsafe.Sizeof(*d))) {
			return false, nil
		}
		*d = cowDeltaTraversal{}
		return true, nil
	case btree.First:
		if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
			return false, nil
		}
		d.started = true
		d.atstart = false
		if len(d.entries) == 0 {
			d.idx = 0
			d.ready = false
			d.atend = true
		} else {
			d.idx = 0
			d.ready = true
			d.atend = false
		}
		d.pending, d.key = false, ""
		return true, nil
	case btree.Last:
		if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
			return false, nil
		}
		d.started = true
		d.atend = false
		if len(d.entries) == 0 {
			d.idx = -1
			d.ready = false
			d.atstart = true
		} else {
			d.idx = len(d.entries) - 1
			d.ready = true
			d.atstart = false
		}
		d.pending, d.key = false, ""
		return true, nil
	case btree.Next:
		if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
			return false, nil
		}
		if !d.ready {
			if !d.started || d.atstart {
				d.started = true
				d.atstart = false
				if len(d.entries) == 0 {
					d.idx = 0
					d.ready = false
					d.atend = true
				} else {
					d.idx = 0
					d.ready = true
					d.atend = false
				}
			}
		} else if d.idx+1 < len(d.entries) {
			d.idx++
		} else {
			d.idx = len(d.entries)
			d.ready = false
			d.atstart = false
			d.atend = true
		}
		d.pending, d.key = false, ""
		return true, nil
	case btree.Prev:
		if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
			return false, nil
		}
		if !d.ready {
			if d.atend {
				d.started = true
				d.atend = false
				if len(d.entries) == 0 {
					d.idx = -1
					d.ready = false
					d.atstart = true
				} else {
					d.idx = len(d.entries) - 1
					d.ready = true
					d.atstart = false
				}
			}
		} else if d.idx > 0 {
			d.idx--
		} else {
			d.idx = -1
			d.ready = false
			d.atstart = true
			d.atend = false
		}
		d.pending, d.key = false, ""
		return true, nil
	case btree.SeekGE, btree.SeekLE:
		for d.lo < d.hi {
			if !d.cmpActive {
				if !work.Reserve(1, uint64(unsafe.Sizeof(cowDeltaEntry{}))) {
					return false, nil
				}
				d.mid = d.lo + (d.hi-d.lo)/2
				d.cmpOffset = 0
				d.cmpActive = true
			}
			a, b := d.entries[d.mid].key, d.key
			cmp := 0
			for d.cmpOffset < len(a) && d.cmpOffset < len(b) {
				if !work.Reserve(0, 2) {
					return false, nil
				}
				x, y := a[d.cmpOffset], b[d.cmpOffset]
				d.cmpOffset++
				if x < y {
					cmp = -1
					break
				}
				if x > y {
					cmp = 1
					break
				}
			}
			if cmp == 0 {
				if len(a) < len(b) {
					cmp = -1
				} else if len(a) > len(b) {
					cmp = 1
				}
			}
			d.cmpActive = false
			if d.op == btree.SeekGE {
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
		d.started = true
		if d.op == btree.SeekGE {
			d.idx = d.lo
			d.ready = d.idx < len(d.entries)
			d.atstart = false
			d.atend = !d.ready
		} else {
			d.idx = d.lo - 1
			d.ready = d.idx >= 0 && d.idx < len(d.entries)
			d.atstart = !d.ready
			d.atend = false
		}
		d.pending, d.key = false, ""
		return true, nil
	default:
		d.pending, d.key = false, ""
		return false, btree.ErrOperation
	}
}

func (d *cowDeltaTraversal) Ready() bool { return d.ready && !d.pending }

func (d *cowDeltaTraversal) Current() (COWRecord, bool) {
	if !d.Ready() || d.idx < 0 || d.idx >= len(d.entries) {
		return COWRecord{}, false
	}
	e := d.entries[d.idx]
	return e.value.record(e.key), true
}
