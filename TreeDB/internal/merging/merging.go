package merging

import (
	"bytes"
	"container/heap"
	"github.com/snissn/gomap/TreeDB/internal/iterator" // For UnsafeIterator
)

// Iterator represents a generic iterator that yields key-value pairs.
// It matches the public Cosmos-DB interface contract (returns copies).
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Close() error
	Error() error
	Domain() (start, end []byte)
}

// IteratorSource wraps an internal UnsafeIterator with a priority level.
type IteratorSource struct {
	Iter     iterator.UnsafeIterator
	Priority int
}

// heapItem is an internal wrapper for the min-heap.
type heapItem struct {
	iter     iterator.UnsafeIterator
	priority int
	key      []byte // Cached key view from iter.UnsafeKey() (valid until iter.Next/Seek)
}

type iteratorHeap []*heapItem

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	// 1. Primary sort key: Key (lexicographical)
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// 2. Secondary sort key: Priority (Lower is better/newer)
	// We want the newest version (lowest priority number) to come first
	// so we can deduplicate.
	return h[i].priority < h[j].priority
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergingIterator merges multiple sorted iterators.
type MergingIterator struct {
	h         *iteratorHeap
	iters     []iterator.UnsafeIterator
	curr      *heapItem
	valid     bool
	key       []byte // Current Key (copy; populated lazily)
	keyLoaded bool
	val       []byte // Current Value (copy; populated lazily)
	valLoaded bool
	err       error
	start     []byte
	end       []byte
}

func NewMergingIterator(sources []IteratorSource, start, end []byte) Iterator {
	// Optimization for the common two-source case
	if len(sources) == 2 {
		// Assume sources[0] has higher priority (lower number)
		return NewTwoWayMerger(sources[0].Iter, sources[1].Iter, start, end)
	}

	h := &iteratorHeap{}
	heap.Init(h)

	iters := make([]iterator.UnsafeIterator, 0, len(sources))
	for _, src := range sources {
		iters = append(iters, src.Iter)
		if src.Iter.Valid() {
			heap.Push(h, &heapItem{
				iter:     src.Iter,
				priority: src.Priority,
				key:      src.Iter.UnsafeKey(),
			})
		}
	}

	mi := &MergingIterator{h: h, iters: iters, start: start, end: end}
	mi.advance()
	return mi
}

func (mi *MergingIterator) Next() {
	if !mi.valid {
		panic("merging iterator invalid")
	}

	// Advance the iterator that produced the current key, then re-select.
	mi.curr.iter.Next()
	if mi.curr.iter.Valid() {
		mi.curr.key = mi.curr.iter.UnsafeKey()
		heap.Push(mi.h, mi.curr)
	}
	mi.curr = nil
	mi.advance()
}

func (mi *MergingIterator) advance() {
	mi.valid = false
	mi.key = nil
	mi.keyLoaded = false
	mi.val = nil
	mi.valLoaded = false
	mi.curr = nil

	for mi.h.Len() > 0 {
		top := heap.Pop(mi.h).(*heapItem)

		// Respect end bound (exclusive).
		if mi.end != nil && bytes.Compare(top.key, mi.end) >= 0 {
			return
		}

		// Consume shadows for this key (advance and requeue them), keeping the best-precedence
		// iterator (top) positioned at the current key until the caller calls Next().
		for mi.h.Len() > 0 {
			next := (*mi.h)[0]
			if bytes.Equal(next.key, top.key) {
				shadowed := heap.Pop(mi.h).(*heapItem)
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.UnsafeKey()
					heap.Push(mi.h, shadowed)
				}
			} else {
				break
			}
		}

		// Handle range bounds (inclusive start). This should already be enforced by Seek, but
		// keep it for safety if a source iterator doesn't implement Seek correctly.
		if mi.start != nil && bytes.Compare(top.key, mi.start) < 0 {
			top.iter.Next()
			if top.iter.Valid() {
				top.key = top.iter.UnsafeKey()
				heap.Push(mi.h, top)
			}
			continue
		}

		// Skip tombstones.
		if top.iter.IsDeleted() {
			top.iter.Next()
			if top.iter.Valid() {
				top.key = top.iter.UnsafeKey()
				heap.Push(mi.h, top)
			}
			continue
		}

		mi.curr = top
		mi.valid = true
		return
	}
}

func (mi *MergingIterator) Valid() bool {
	return mi.valid
}

func (mi *MergingIterator) Key() []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	if !mi.keyLoaded {
		mi.key = append([]byte(nil), mi.curr.key...)
		mi.keyLoaded = true
	}
	return mi.key
}

func (mi *MergingIterator) Value() []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	if !mi.valLoaded {
		mi.val = append([]byte(nil), mi.curr.iter.UnsafeValue()...)
		mi.valLoaded = true
	}
	return mi.val
}

func (mi *MergingIterator) Error() error {
	return mi.err
}

func (mi *MergingIterator) Close() error {
	var firstErr error
	for _, it := range mi.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	mi.iters = nil
	mi.curr = nil
	mi.valid = false
	return firstErr
}

func (mi *MergingIterator) Domain() (start, end []byte) {
	return mi.start, mi.end
}
