package merging

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator" // For UnsafeIterator
)

// TwoWayMerger implements MergingIterator for two sources (Memtable and Disk).
// It avoids heap overhead for the common case.
type TwoWayMerger struct {
	// Source 1: Memtable (always higher precedence / lower priority)
	src1     iterator.UnsafeIterator 
	// Source 2: Disk
	src2     iterator.UnsafeIterator 

	curr      iterator.UnsafeIterator
	valid     bool
	key       []byte
	keyLoaded bool
	val       []byte
	valLoaded bool
	err       error
	start     []byte
	end       []byte
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
	m.curr.Next()
	m.advance()
}

func (m *TwoWayMerger) advance() {
	m.valid = false // Assume invalid until an item is found
	m.key = nil
	m.keyLoaded = false
	m.val = nil
	m.valLoaded = false
	m.curr = nil

	// Find the smaller key, without forcing value loads unless Value() is called.
	for m.src1.Valid() || m.src2.Valid() {
		cmp := 0
		if m.src1.Valid() && m.src2.Valid() {
			cmp = bytes.Compare(m.src1.UnsafeKey(), m.src2.UnsafeKey())
		} else if m.src1.Valid() {
			cmp = -1 // src1 is smaller (src2 exhausted)
		} else if m.src2.Valid() {
			cmp = 1 // src2 is smaller (src1 exhausted)
		} else {
			break // Both exhausted
		}

		var (
			currentKey []byte
			isDeleted  bool
		)

		switch {
		case cmp < 0: // src1 is smaller or src2 exhausted
			m.curr = m.src1
			currentKey = m.src1.UnsafeKey()
			isDeleted = m.src1.IsDeleted()
		case cmp > 0: // src2 is smaller or src1 exhausted
			m.curr = m.src2
			currentKey = m.src2.UnsafeKey()
			isDeleted = m.src2.IsDeleted()
		default: // Keys are equal (shadowing): src1 wins, src2 is advanced now.
			m.curr = m.src1
			currentKey = m.src1.UnsafeKey()
			isDeleted = m.src1.IsDeleted()
			m.src2.Next()
		}

		// Handle range bounds (exclusive end).
		if m.end != nil && bytes.Compare(currentKey, m.end) >= 0 {
			m.curr = nil
			return
		}
		// Handle range bounds (inclusive start).
		if m.start != nil && bytes.Compare(currentKey, m.start) < 0 {
			m.curr.Next()
			continue
		}

		// Skip tombstones.
		if isDeleted {
			m.curr.Next()
			continue
		}

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
	if !m.keyLoaded {
		m.key = append([]byte(nil), m.curr.UnsafeKey()...)
		m.keyLoaded = true
	}
	return m.key
}

func (m *TwoWayMerger) Value() []byte {
	if !m.valid {
		panic("iterator invalid")
	}
	if !m.valLoaded {
		m.val = append([]byte(nil), m.curr.UnsafeValue()...)
		m.valLoaded = true
	}
	return m.val
}

func (m *TwoWayMerger) Error() error {
	return m.err
}

func (m *TwoWayMerger) Close() error {
	var firstErr error
	if err := m.src1.Close(); err != nil && firstErr == nil { firstErr = err }
	if err := m.src2.Close(); err != nil && firstErr == nil { firstErr = err }
	return firstErr
}

func (m *TwoWayMerger) Domain() (start, end []byte) {
	return m.start, m.end
}
