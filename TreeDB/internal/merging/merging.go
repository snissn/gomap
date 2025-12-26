package merging

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// Iterator represents a generic iterator that yields key-value pairs.
// Key/Value are views, valid until the next Next()/Close().
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
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

type iteratorHeap []heapItem

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

func (h *iteratorHeap) push(x heapItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *iteratorHeap) pop() heapItem {
	old := *h
	n := len(old)
	if n == 0 {
		return heapItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h iteratorHeap) peek() *heapItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *iteratorHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *iteratorHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

// MergingIterator merges multiple sorted iterators.
type MergingIterator struct {
	h      iteratorHeap
	cur    heapItem
	hasCur bool
	valid  bool
	err    error
	start  []byte
	end    []byte
}

func NewMergingIterator(sources []IteratorSource, start, end []byte) Iterator {
	// Optimization for the common two-source case
	if len(sources) == 2 {
		// Assume sources[0] has higher priority (lower number)
		return NewTwoWayMerger(sources[0].Iter, sources[1].Iter, start, end)
	}

	h := make(iteratorHeap, 0, len(sources))
	for _, src := range sources {
		if src.Iter.Valid() {
			h = append(h, heapItem{
				iter:     src.Iter,
				priority: src.Priority,
				key:      src.Iter.UnsafeKey(), // View valid until src.Iter.Next()
			})
		} else {
			_ = src.Iter.Close()
		}
	}
	// Heapify.
	for i := len(h)/2 - 1; i >= 0; i-- {
		(&h).down(i, len(h))
	}

	mi := &MergingIterator{h: h, start: start, end: end}
	mi.advance()
	return mi
}

func (mi *MergingIterator) Next() {
	if !mi.valid {
		panic("merging iterator invalid")
	}

	if mi.hasCur {
		mi.cur.iter.Next()
		if mi.cur.iter.Valid() {
			mi.cur.key = mi.cur.iter.UnsafeKey()
			mi.h.push(mi.cur)
		} else {
			_ = mi.cur.iter.Close()
		}
		mi.hasCur = false
	}

	mi.advance()
}

func (mi *MergingIterator) advance() {
	mi.valid = false
	mi.hasCur = false

	for len(mi.h) > 0 {
		top := mi.h.pop()
		currentKey := top.key

		if mi.end != nil && bytes.Compare(currentKey, mi.end) >= 0 {
			// Put it back so Close() can close everything.
			mi.h.push(top)
			return
		}

		// Consume shadows (same key, lower precedence).
		for len(mi.h) > 0 {
			next := mi.h.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := mi.h.pop()
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.UnsafeKey()
					mi.h.push(shadowed)
				} else {
					_ = shadowed.iter.Close()
				}
			} else {
				break
			}
		}

		// If tombstone, advance winner and continue.
		if top.iter.IsDeleted() {
			top.iter.Next()
			if top.iter.Valid() {
				top.key = top.iter.UnsafeKey()
				mi.h.push(top)
			} else {
				_ = top.iter.Close()
			}
			continue
		}

		// Found current item. Keep the winner positioned here; Value() loads lazily.
		mi.cur = top
		mi.hasCur = true
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
	return mi.cur.key
}

func (mi *MergingIterator) Value() []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	if !mi.hasCur {
		return nil
	}
	return mi.cur.iter.UnsafeValue()
}

func (mi *MergingIterator) KeyCopy(dst []byte) []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	return append(dst[:0], mi.Key()...)
}

func (mi *MergingIterator) ValueCopy(dst []byte) []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	return append(dst[:0], mi.Value()...)
}

func (mi *MergingIterator) Error() error {
	return mi.err
}

func (mi *MergingIterator) Close() error {
	var firstErr error

	if mi.hasCur {
		if err := mi.cur.iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		mi.hasCur = false
	}

	for _, item := range mi.h {
		if err := item.iter.Close(); err != nil && firstErr == nil { // Call Close() on UnsafeIterator
			firstErr = err
		}
	}

	return firstErr
}

func (mi *MergingIterator) Domain() (start, end []byte) {
	return mi.start, mi.end
}
