package merging

import (
	"bytes"
	"container/heap"
	"github.com/snissn/gomap-gemini/TreeDB/internal/iterator"
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
	key      []byte // Cached Key from iter.UnsafeKey()
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
	h     *iteratorHeap
	valid bool
	key   []byte // Current Key (copy)
	val   []byte // Current Value (copy)
	err   error
	start []byte
	end   []byte
}

func NewMergingIterator(sources []IteratorSource, start, end []byte) Iterator {
	// Optimization for the common two-source case
	if len(sources) == 2 {
		// Assume sources[0] has higher priority (lower number)
		return NewTwoWayMerger(sources[0].Iter, sources[1].Iter, start, end)
	}

	h := &iteratorHeap{}
	heap.Init(h)

	for _, src := range sources {
		if src.Iter.Valid() {
			heap.Push(h, &heapItem{
				iter:     src.Iter,
				priority: src.Priority,
				key:      append([]byte(nil), src.Iter.UnsafeKey()...), // Copy once per source
			})
		}
	}

	mi := &MergingIterator{h: h, start: start, end: end}
	mi.next()
	return mi
}

func (mi *MergingIterator) Next() {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	mi.next()
}

func (mi *MergingIterator) next() {
	mi.valid = false

	for mi.h.Len() > 0 {
		top := heap.Pop(mi.h).(*heapItem)
		currentUnsafeKey := top.iter.UnsafeKey()
		currentUnsafeVal := top.iter.UnsafeValue() // Value might be lazy-loaded here

		isDeleted := top.iter.IsDeleted() // Use IsDeleted from UnsafeIterator

		// Advance the winner (UnsafeIterator.Next)
		top.iter.Next()
		if top.iter.Valid() {
			top.key = append(top.key[:0], top.iter.UnsafeKey()...) // Reuse buffer
			heap.Push(mi.h, top)
		}

		// Consume Shadows
		for mi.h.Len() > 0 {
			next := (*mi.h)[0]
			if bytes.Equal(next.key, currentUnsafeKey) { // Compare against unsafe key
				shadowed := heap.Pop(mi.h).(*heapItem)
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = append(shadowed.key[:0], shadowed.iter.UnsafeKey()...)
					heap.Push(mi.h, shadowed)
				}
			} else {
				break
			}
		}

		// If tombstone, continue loop
		if isDeleted {
			continue
		}

		// Found valid data: perform final copy for public API
		mi.key = append(mi.key[:0], currentUnsafeKey...)
		mi.val = append(mi.val[:0], currentUnsafeVal...)
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
	return mi.key
}

func (mi *MergingIterator) Value() []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	return mi.val
}

func (mi *MergingIterator) Error() error {
	return mi.err
}

func (mi *MergingIterator) Close() error {
	for _, item := range *mi.h {
		if err := item.iter.Close(); err != nil { // Call Close() on UnsafeIterator
			return err
		}
	}
	return nil
}

func (mi *MergingIterator) Domain() (start, end []byte) {
	return mi.start, mi.end
}
