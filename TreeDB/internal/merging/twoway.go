package merging

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// TwoWayMerger implements MergingIterator for two sources (Memtable and Disk).
// It avoids heap overhead for the common case.
type TwoWayMerger struct {
	// Source 1: Memtable (always higher precedence / lower priority)
	src1 iterator.UnsafeIterator
	// Source 2: Disk
	src2 iterator.UnsafeIterator

	cur iterator.UnsafeIterator

	valid bool
	err   error
	start []byte
	end   []byte
}

func NewTwoWayMerger(src1, src2 iterator.UnsafeIterator, start, end []byte) *TwoWayMerger {
	m := &TwoWayMerger{
		src1:  src1,
		src2:  src2,
		start: start,
		end:   end,
	}
	m.advance() // Position at first element
	return m
}

func (m *TwoWayMerger) Next() {
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

// Seek positions the iterator at the first visible key greater than or equal
// to key, restricted to the iterator's original [start, end) domain.
func (m *TwoWayMerger) Seek(key []byte) {
	if m.start != nil && (key == nil || bytes.Compare(key, m.start) < 0) {
		key = m.start
	}
	m.err = nil
	m.cur = nil
	m.valid = false
	m.src1.Seek(key)
	m.src2.Seek(key)
	m.err = twoIteratorErrors(m.src1, m.src2)
	if m.err != nil {
		return
	}
	m.advance()
}

func (m *TwoWayMerger) advance() {
	m.valid = false // Assume invalid until an item is found
	m.cur = nil

	for m.src1.Valid() || m.src2.Valid() {
		var winner iterator.UnsafeIterator

		switch {
		case m.src1.Valid() && m.src2.Valid():
			cmp := bytes.Compare(m.src1.UnsafeKey(), m.src2.UnsafeKey())
			if cmp < 0 {
				winner = m.src1
			} else if cmp > 0 {
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

		// Handle range bounds (exclusive end)
		if m.end != nil && bytes.Compare(k, m.end) >= 0 {
			return
		}
		// Handle range bounds (inclusive start) - only needed if Seek doesn't handle it
		if m.start != nil && bytes.Compare(k, m.start) < 0 {
			winner.Next()
			if m.captureIteratorError(winner) {
				return
			}
			continue
		}

		// If tombstone, skip it (and keep searching).
		if winner.IsDeleted() {
			winner.Next()
			if m.captureIteratorError(winner) {
				return
			}
			continue
		}

		// Found current item. Keep the winner positioned here; Value() will load
		// lazily if needed.
		m.cur = winner
		m.valid = true
		return
	}
}

func (m *TwoWayMerger) captureIteratorError(iter iterator.UnsafeIterator) bool {
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

func (m *TwoWayMerger) Valid() bool {
	return m.valid
}

func (m *TwoWayMerger) Key() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	// Zero-copy: UnsafeKey is valid until Next()
	return m.cur.UnsafeKey()
}

func (m *TwoWayMerger) Value() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	// Zero-copy: UnsafeValue is valid until Next()
	if m.cur == nil {
		return nil
	}
	value := m.cur.UnsafeValue()
	if err := m.cur.Error(); err != nil {
		m.err = err
	}
	return value
}

func (m *TwoWayMerger) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !m.valid || m.cur == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(m.cur)
	if err := m.cur.Error(); err != nil {
		m.err = err
	}
	return value, ptr, flags, revision
}

func (m *TwoWayMerger) KeyCopy(dst []byte) []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	return append(dst[:0], m.Key()...)
}

func (m *TwoWayMerger) ValueCopy(dst []byte) []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	return append(dst[:0], m.Value()...)
}

func (m *TwoWayMerger) Error() error {
	if m.err != nil {
		return m.err
	}
	if m.cur != nil {
		return m.cur.Error()
	}
	return m.err
}

func (m *TwoWayMerger) Close() error {
	var firstErr error
	if err := m.src1.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := m.src2.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (m *TwoWayMerger) Domain() (start, end []byte) {
	return m.start, m.end
}
