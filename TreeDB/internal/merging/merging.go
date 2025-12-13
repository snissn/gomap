package merging

import (
	"bytes"
	"container/heap"
)

// Iterator represents a generic iterator that yields key-value pairs.
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Close() error
	Error() error
	Domain() (start, end []byte)
}

// IteratorSource wraps an iterator with a priority level (0 = highest).
type IteratorSource struct {
	Iter     Iterator
	Priority int
}

// heapItem is an internal wrapper for the min-heap.
type heapItem struct {
	iter     Iterator
	priority int
	key      []byte
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
	key   []byte
	val   []byte
	err   error
	start []byte
	end   []byte
}

func NewMergingIterator(sources []IteratorSource, start, end []byte) *MergingIterator {
	h := &iteratorHeap{}
	heap.Init(h)

	for _, src := range sources {
		if src.Iter.Valid() {
			heap.Push(h, &heapItem{
				iter:     src.Iter,
				priority: src.Priority,
				key:      append([]byte(nil), src.Iter.Key()...),
			})
		}
	}

	mi := &MergingIterator{h: h, start: start, end: end}
	// Position at first valid item
	mi.next()
	return mi
}

func (mi *MergingIterator) Domain() (start, end []byte) {
	return mi.start, mi.end
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
		// 1. Pop the smallest item (Key + Priority)
		top := heap.Pop(mi.h).(*heapItem)
		currentKey := top.key
		currentVal := append([]byte(nil), top.iter.Value()...) // defensive copy? Iter contract says copy.

		// Determine if this is a tombstone
		// How do we know if it's a tombstone?
		// Value() usually returns nil for tombstone? Or empty?
		// treedb Iterators skip tombstones.
		// Memtable iterators need to expose tombstone status.
		// Issue: The Iterator interface here is generic.
		// Memtable Get returns `bool isDeleted`.
		// But Iterator usually hides deleted items.
		// For MergingIterator to work, underlying iterators MUST return Tombstones so we can mask older versions.
		// If underlying iterators skip tombstones, we might see an older version!
		// 
		// Fix: We need `IsTombstone()` on Iterator interface or convention.
		// Let's assume a nil value means tombstone? Or specific interface?
		// cosmos-db Iterator doesn't have IsTombstone.
		// But Memtable needs to return them.
		
		// Let's defer tombstone logic. Assume Value() == nil means tombstone?
		// No, empty value != nil.
		// Let's add `IsDeleted() bool` to our internal Iterator interface if possible.
		// But treedb.Iterator doesn't have it.
		// Wait, treedb.Iterator skips tombstones internally.
		// So disk layer never returns tombstones.
		// Memtable MUST return tombstones.
		
		// Actually, if we define our own Iterator interface for this package, we can add it.
		// But we wrap treedb.Iterator.
		
		// Let's assume we can cast to an interface with IsDeleted.
		isDeleted := false
		if delIter, ok := top.iter.(interface{ IsDeleted() bool }); ok {
			isDeleted = delIter.IsDeleted()
		}

		// Advance the winner
		top.iter.Next()
		if top.iter.Valid() {
			top.key = append([]byte(nil), top.iter.Key()...)
			heap.Push(mi.h, top)
		}

		// 2. Consume Shadows
		for mi.h.Len() > 0 {
			next := (*mi.h)[0]
			if bytes.Equal(next.key, currentKey) {
				// Same key, lower priority (older version). Discard.
				shadowed := heap.Pop(mi.h).(*heapItem)
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = append([]byte(nil), shadowed.iter.Key()...)
					heap.Push(mi.h, shadowed)
				}
			} else {
				break
			}
		}

		// 3. If tombstone, continue loop (don't return this key)
		if isDeleted {
			continue
		}

		// 4. Valid item found
		mi.key = currentKey
		mi.val = currentVal
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
		item.iter.Close()
	}
	return nil
}
