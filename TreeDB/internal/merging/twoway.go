package merging

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
	key   []byte
	val   []byte
	valOK bool
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
	if m.cur != nil {
		m.cur.Next()
	}
	m.advance()
}

func (m *TwoWayMerger) advance() {
	m.valid = false // Assume invalid until an item is found
	m.cur = nil
	m.valOK = false

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
			continue
		}

		// If tombstone, skip it (and keep searching).
		if winner.IsDeleted() {
			winner.Next()
			continue
		}

		// Found current item. Keep the winner positioned here; Value() will load
		// lazily if needed.
		m.cur = winner
		m.key = append(m.key[:0], k...)
		m.valid = true
		return
	}
}

func (m *TwoWayMerger) Valid() bool {
	return m.valid
}

func (m *TwoWayMerger) Key() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	return m.key
}

func (m *TwoWayMerger) Value() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	if m.valOK {
		return m.val
	}

	if m.cur == nil {
		return nil
	}
	m.val = append(m.val[:0], m.cur.UnsafeValue()...)
	m.valOK = true
	return m.val
}

func (m *TwoWayMerger) Error() error {
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
