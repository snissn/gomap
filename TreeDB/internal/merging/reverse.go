package merging

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type reverseHeapItem struct {
	iter     iterator.UnsafeIterator
	priority int
	key      []byte
}

type reverseIteratorHeap []reverseHeapItem

func (h reverseIteratorHeap) Len() int { return len(h) }

func (h reverseIteratorHeap) Less(i, j int) bool {
	// 1. Primary sort key: Key (lexicographical, reverse order)
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp > 0
	}
	// 2. Secondary sort key: Priority (Lower is better/newer)
	return h[i].priority < h[j].priority
}

func (h reverseIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *reverseIteratorHeap) push(x reverseHeapItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *reverseIteratorHeap) pop() reverseHeapItem {
	old := *h
	n := len(old)
	if n == 0 {
		return reverseHeapItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h reverseIteratorHeap) peek() *reverseHeapItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *reverseIteratorHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *reverseIteratorHeap) down(i0, n int) bool {
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

// ReverseMergingIterator merges multiple reverse-sorted iterators.
type ReverseMergingIterator struct {
	sources []IteratorSource
	h       reverseIteratorHeap
	cur     reverseHeapItem
	hasCur  bool
	valid   bool
	err     error
	start   []byte
	end     []byte
}

func NewReverseMergingIterator(sources []IteratorSource, start, end []byte) Iterator {
	if len(sources) == 2 {
		// Assume sources[0] has higher priority (lower number).
		return NewReverseTwoWayMerger(sources[0].Iter, sources[1].Iter, start, end)
	}

	mi := &ReverseMergingIterator{sources: sources, start: start, end: end}
	mi.rebuildHeap()
	mi.advance()
	return mi
}

func (mi *ReverseMergingIterator) rebuildHeap() {
	mi.h = mi.h[:0]
	for _, src := range mi.sources {
		if src.Iter.Valid() {
			mi.h = append(mi.h, reverseHeapItem{
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

// Seek positions the iterator at the first visible key less than or equal to
// key, restricted to the iterator's original [start, end) domain. A nil key
// seeks to the greatest key in the domain.
func (mi *ReverseMergingIterator) Seek(key []byte) {
	mi.err = nil
	mi.hasCur = false
	mi.valid = false
	if key != nil && mi.start != nil && bytes.Compare(key, mi.start) < 0 {
		return
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

func (mi *ReverseMergingIterator) Next() {
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

func (mi *ReverseMergingIterator) advance() {
	mi.valid = false
	mi.hasCur = false

	for len(mi.h) > 0 {
		top := mi.h.pop()
		currentKey := top.key

		// Stop once we've gone below the start bound (inclusive start).
		if mi.start != nil && bytes.Compare(currentKey, mi.start) < 0 {
			mi.h.push(top)
			return
		}

		// Upper bound is exclusive; if a source yields >= end, advance it and continue.
		if mi.end != nil && bytes.Compare(currentKey, mi.end) >= 0 {
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

		mi.cur = top
		mi.hasCur = true
		mi.valid = true
		return
	}
}

func (mi *ReverseMergingIterator) captureIteratorError(iter iterator.UnsafeIterator) bool {
	if iter == nil {
		return false
	}
	err := iter.Error()
	if err == nil {
		return false
	}
	mi.err = joinIteratorError(mi.err, err)
	mi.cur = reverseHeapItem{}
	mi.hasCur = false
	mi.valid = false
	return true
}

func (mi *ReverseMergingIterator) Valid() bool { return mi.valid }

func (mi *ReverseMergingIterator) Key() []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	return mi.cur.key
}

func (mi *ReverseMergingIterator) Value() []byte {
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

func (mi *ReverseMergingIterator) KeyCopy(dst []byte) []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	return append(dst[:0], mi.Key()...)
}

func (mi *ReverseMergingIterator) ValueCopy(dst []byte) []byte {
	if !mi.valid {
		panic("merging iterator invalid")
	}
	return append(dst[:0], mi.Value()...)
}

func (mi *ReverseMergingIterator) Error() error {
	if mi.err != nil {
		return mi.err
	}
	if mi.hasCur {
		return mi.cur.iter.Error()
	}
	return mi.err
}

func (mi *ReverseMergingIterator) Close() error {
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

func (mi *ReverseMergingIterator) Domain() (start, end []byte) { return mi.start, mi.end }

// ReverseTwoWayMerger implements ReverseMergingIterator for two sources.
type ReverseTwoWayMerger struct {
	src1  iterator.UnsafeIterator
	src2  iterator.UnsafeIterator
	cur   iterator.UnsafeIterator
	valid bool
	err   error
	start []byte
	end   []byte
}

func NewReverseTwoWayMerger(src1, src2 iterator.UnsafeIterator, start, end []byte) *ReverseTwoWayMerger {
	m := &ReverseTwoWayMerger{
		src1:  src1,
		src2:  src2,
		start: start,
		end:   end,
	}
	m.advance()
	return m
}

func (m *ReverseTwoWayMerger) Next() {
	if !m.valid {
		panic("merging iterator invalid")
	}
	if m.err != nil {
		m.valid = false
		return
	}
	if m.cur != nil {
		m.cur.Next()
		if m.captureIteratorError(m.cur) {
			return
		}
	}
	m.advance()
}

// Seek positions the iterator at the first visible key less than or equal to
// key, restricted to the iterator's original [start, end) domain. A nil key
// seeks to the greatest key in the domain.
func (m *ReverseTwoWayMerger) Seek(key []byte) {
	m.err = nil
	m.cur = nil
	m.valid = false
	if key != nil && m.start != nil && bytes.Compare(key, m.start) < 0 {
		return
	}
	m.src1.Seek(key)
	m.src2.Seek(key)
	m.err = twoIteratorErrors(m.src1, m.src2)
	if m.err != nil {
		return
	}
	m.advance()
}

func (m *ReverseTwoWayMerger) advance() {
	m.valid = false
	m.cur = nil

	for m.src1.Valid() || m.src2.Valid() {
		var winner iterator.UnsafeIterator

		switch {
		case m.src1.Valid() && m.src2.Valid():
			cmp := bytes.Compare(m.src1.UnsafeKey(), m.src2.UnsafeKey())
			if cmp > 0 {
				winner = m.src1
			} else if cmp < 0 {
				winner = m.src2
			} else {
				// Keys equal: src1 wins, src2 is shadowed.
				winner = m.src1
				m.src2.Next()
				if m.captureIteratorError(m.src2) {
					return
				}
			}
		case m.src1.Valid():
			winner = m.src1
		default:
			winner = m.src2
		}

		k := winner.UnsafeKey()

		// Exclusive end (upper bound): skip keys >= end.
		if m.end != nil && bytes.Compare(k, m.end) >= 0 {
			winner.Next()
			if m.captureIteratorError(winner) {
				return
			}
			continue
		}

		// Inclusive start (lower bound): stop once we fall below it.
		if m.start != nil && bytes.Compare(k, m.start) < 0 {
			return
		}

		if winner.IsDeleted() {
			winner.Next()
			if m.captureIteratorError(winner) {
				return
			}
			continue
		}

		m.cur = winner
		m.valid = true
		return
	}
}

func (m *ReverseTwoWayMerger) captureIteratorError(iter iterator.UnsafeIterator) bool {
	if iter == nil {
		return false
	}
	err := iter.Error()
	if err == nil {
		return false
	}
	m.err = joinIteratorError(m.err, err)
	m.valid = false
	m.cur = nil
	return true
}

func (m *ReverseTwoWayMerger) Valid() bool { return m.valid }

func (m *ReverseTwoWayMerger) Key() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	return m.cur.UnsafeKey()
}

func (m *ReverseTwoWayMerger) Value() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	if m.cur == nil {
		return nil
	}
	value := m.cur.UnsafeValue()
	if err := m.cur.Error(); err != nil {
		m.err = err
	}
	return value
}

func (m *ReverseTwoWayMerger) KeyCopy(dst []byte) []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	return append(dst[:0], m.Key()...)
}

func (m *ReverseTwoWayMerger) ValueCopy(dst []byte) []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	return append(dst[:0], m.Value()...)
}

func (m *ReverseTwoWayMerger) Error() error {
	if m.err != nil {
		return m.err
	}
	if m.cur != nil {
		return m.cur.Error()
	}
	return m.err
}

func (m *ReverseTwoWayMerger) Close() error {
	var firstErr error
	if err := m.src1.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := m.src2.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (m *ReverseTwoWayMerger) Domain() (start, end []byte) { return m.start, m.end }
