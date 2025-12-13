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

	valid bool
	key   []byte
	val   []byte
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
	m.next() // Position at first element
	return m
}

func (m *TwoWayMerger) Next() {
	if !m.valid {
		panic("merging iterator invalid")
	}
	m.next()
}

func (m *TwoWayMerger) next() {
	m.valid = false // Assume invalid until an item is found

	// Advance any exhausted iterators
	// Logic here is tricky: next() should find the *next* item.
	// Initial call to next() positions it. Subsequent calls advance.
	// So we don't call Next() on exhausted iterators here.
	
	// Find the smaller key
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
			currentKey   []byte
			currentValue []byte
			isDeleted    bool
		)

		if cmp < 0 { // src1 is smaller or src2 exhausted
			currentKey = m.src1.UnsafeKey()
			currentValue = m.src1.UnsafeValue()
			isDeleted = m.src1.IsDeleted()
			m.src1.Next()
		} else if cmp > 0 { // src2 is smaller or src1 exhausted
			currentKey = m.src2.UnsafeKey()
			currentValue = m.src2.UnsafeValue()
			isDeleted = m.src2.IsDeleted()
			m.src2.Next()
		} else { // Keys are equal (shadowing)
			currentKey = m.src1.UnsafeKey() // Take from src1 (higher precedence)
			currentValue = m.src1.UnsafeValue()
			isDeleted = m.src1.IsDeleted()
			m.src1.Next() // Advance both
			m.src2.Next()
		}

		// Handle range bounds (exclusive end)
		if m.end != nil && bytes.Compare(currentKey, m.end) >= 0 {
			break // Reached or passed end boundary
		}
		// Handle range bounds (inclusive start) - only needed if Seek doesn't handle it
		if m.start != nil && bytes.Compare(currentKey, m.start) < 0 {
			continue // Skip if before start boundary
		}

		// If tombstone, continue loop
		if isDeleted {
			continue
		}

		// Found valid data: perform final copy for public API
		m.key = append([]byte(nil), currentKey...)
		m.val = append([]byte(nil), currentValue...)
		m.valid = true
		return
	}
	m.valid = false // Exhausted
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