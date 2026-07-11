package merging

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// Iterator represents a generic iterator that yields key-value pairs.
// Key/Value are views, valid until the next Next()/Close().
type Iterator interface {
	Next()
	Seek(key []byte)
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

func joinIteratorError(current, next error) error {
	if next == nil {
		return current
	}
	if current == nil {
		return next
	}
	return errors.Join(current, next)
}

func twoIteratorErrors(first, second iterator.UnsafeIterator) error {
	var err error
	if first != nil {
		err = joinIteratorError(err, first.Error())
	}
	if second != nil {
		err = joinIteratorError(err, second.Error())
	}
	return err
}

func sourceIteratorErrors(sources []IteratorSource) error {
	var err error
	for _, src := range sources {
		if src.Iter != nil {
			err = joinIteratorError(err, src.Iter.Error())
		}
	}
	return err
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
	sources []IteratorSource
	h       iteratorHeap
	cur     heapItem
	hasCur  bool
	valid   bool
	err     error
	start   []byte
	end     []byte
}

func NewMergingIterator(sources []IteratorSource, start, end []byte) Iterator {
	// Optimization for the common two-source case
	if len(sources) == 2 {
		// Assume sources[0] has higher priority (lower number)
		return NewTwoWayMerger(sources[0].Iter, sources[1].Iter, start, end)
	}

	mi := &MergingIterator{sources: sources, start: start, end: end}
	mi.rebuildHeap()
	mi.advance()
	return mi
}

func (mi *MergingIterator) rebuildHeap() {
	mi.h = mi.h[:0]
	for _, src := range mi.sources {
		if src.Iter.Valid() {
			mi.h = append(mi.h, heapItem{
				iter:     src.Iter,
				priority: src.Priority,
				key:      src.Iter.UnsafeKey(),
			})
		}
	}
	for i := len(mi.h)/2 - 1; i >= 0; i-- {
		(&mi.h).down(i, len(mi.h))
	}
}

// Seek positions the iterator at the first visible key greater than or equal
// to key, restricted to the iterator's original [start, end) domain.
func (mi *MergingIterator) Seek(key []byte) {
	mi.err = nil
	mi.hasCur = false
	mi.valid = false
	if mi.start != nil && (key == nil || bytes.Compare(key, mi.start) < 0) {
		key = mi.start
	}
	for _, src := range mi.sources {
		src.Iter.Seek(key)
	}
	mi.err = sourceIteratorErrors(mi.sources)
	if mi.err != nil {
		mi.h = mi.h[:0]
		return
	}
	mi.rebuildHeap()
	mi.advance()
}

func (mi *MergingIterator) Next() {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	if mi.err != nil {
		mi.valid = false
		return
	}

	if mi.hasCur {
		mi.cur.iter.Next()
		if mi.captureIteratorError(mi.cur.iter) {
			return
		}
		if mi.cur.iter.Valid() {
			mi.cur.key = mi.cur.iter.UnsafeKey()
			mi.h.push(mi.cur)
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
			// Preserve the source position; Seek may reactivate it later.
			mi.h.push(top)
			return
		}

		// Consume shadows (same key, lower precedence).
		for len(mi.h) > 0 {
			next := mi.h.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := mi.h.pop()
				shadowed.iter.Next()
				if mi.captureIteratorError(shadowed.iter) {
					return
				}
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.UnsafeKey()
					mi.h.push(shadowed)
				}
			} else {
				break
			}
		}

		// If tombstone, advance winner and continue.
		if top.iter.IsDeleted() {
			top.iter.Next()
			if mi.captureIteratorError(top.iter) {
				return
			}
			if top.iter.Valid() {
				top.key = top.iter.UnsafeKey()
				mi.h.push(top)
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

func (mi *MergingIterator) captureIteratorError(iter iterator.UnsafeIterator) bool {
	if iter == nil {
		return false
	}
	err := iter.Error()
	if err == nil {
		return false
	}
	mi.err = joinIteratorError(mi.err, err)
	mi.cur = heapItem{}
	mi.hasCur = false
	mi.valid = false
	return true
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
	value := mi.cur.iter.UnsafeValue()
	if err := mi.cur.iter.Error(); err != nil {
		mi.err = err
	}
	return value
}

func (mi *MergingIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !mi.valid || !mi.hasCur {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(mi.cur.iter)
	if err := mi.cur.iter.Error(); err != nil {
		mi.err = err
	}
	return value, ptr, flags, revision
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
	if mi.err != nil {
		return mi.err
	}
	if mi.hasCur {
		return mi.cur.iter.Error()
	}
	return mi.err
}

func (mi *MergingIterator) Close() error {
	var firstErr error
	for _, src := range mi.sources {
		if err := src.Iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	mi.sources = nil
	mi.h = nil
	mi.hasCur = false
	mi.valid = false
	return firstErr
}

func (mi *MergingIterator) Domain() (start, end []byte) {
	return mi.start, mi.end
}
