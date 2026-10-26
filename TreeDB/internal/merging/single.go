package merging

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// SingleSourceMerger filters one original source without heap arbitration.
// It retains the original source backing, including its independently admitted
// allocation, until Close. Keys and values retain the child's borrowed lifetime.
type SingleSourceMerger struct {
	sources        []IteratorSource
	key            []byte
	start          []byte
	end            []byte
	err            error
	valid          bool
	forwardAligned bool
}

func newSingleSourceMerger(sources []IteratorSource, start, end []byte) *SingleSourceMerger {
	m := &SingleSourceMerger{sources: sources, start: start, end: end}
	// The child already owns its original domain and initial position. Do not
	// seek it again or materialize a value to expose its metadata head.
	m.advance()
	return m
}

func (m *SingleSourceMerger) captureError() bool {
	if err := m.sources[0].Iter.Error(); err != nil {
		m.err = err
		m.valid, m.forwardAligned = false, false
		m.key = nil
		return true
	}
	return false
}

func (m *SingleSourceMerger) advance() {
	m.valid = false
	m.key = nil
	if m.captureError() {
		return
	}
	key, valid := iteratorHead(m.sources[0].Iter)
	m.advanceHead(key, valid)
}

func (m *SingleSourceMerger) advanceHead(key []byte, valid bool) {
	m.valid = false
	m.key = nil
	source := m.sources[0].Iter
	for valid {
		if m.end != nil && bytes.Compare(key, m.end) >= 0 {
			return // Keep the child positioned so an ordinary Seek can reactivate it.
		}
		if !source.IsDeleted() {
			m.key, m.valid = key, true
			return
		}
		source.Next()
		if m.captureError() {
			return
		}
		key, valid = iteratorHead(source)
	}
}

func (m *SingleSourceMerger) Next() {
	if !m.valid {
		panic("merging iterator invalid")
	}
	if m.err != nil {
		m.valid = false
		return
	}
	m.sources[0].Iter.Next()
	m.advance()
}

func (m *SingleSourceMerger) Seek(key []byte) {
	m.valid, m.forwardAligned = false, false
	m.key, m.err = nil, nil
	if len(m.sources) == 0 {
		return
	}
	if m.start != nil && (key == nil || bytes.Compare(key, m.start) < 0) {
		key = m.start
	}
	m.sources[0].Iter.Seek(key)
	m.advance()
	m.forwardAligned = m.err == nil
}

func (m *SingleSourceMerger) SeekForward(key []byte) {
	if !m.forwardAligned || !m.valid || m.err != nil || bytes.Compare(key, m.key) <= 0 || m.sources[0].Iter.Error() != nil {
		m.Seek(key)
		return
	}
	// Reuse the maintained source-private movement policy. Initial constructor
	// positions alone never authorize the forward shortcut.
	head, valid, err := seekForwardHead(m.sources[0].Iter, key)
	if err != nil {
		m.err = err
		m.valid, m.forwardAligned = false, false
		m.key = nil
		return
	}
	m.advanceHead(head, valid)
}

func (m *SingleSourceMerger) Valid() bool { return m.valid }

func (m *SingleSourceMerger) Key() []byte {
	if !m.valid {
		panic("merging iterator invalid")
	}
	return m.key
}

func (m *SingleSourceMerger) Value() []byte {
	if !m.valid {
		panic("merging iterator invalid")
	}
	value := m.sources[0].Iter.UnsafeValue()
	if err := m.sources[0].Iter.Error(); err != nil {
		m.err = err
	}
	return value
}

func (m *SingleSourceMerger) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !m.valid {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(m.sources[0].Iter)
	if err := m.sources[0].Iter.Error(); err != nil {
		m.err = err
	}
	return value, ptr, flags, revision
}

func (m *SingleSourceMerger) KeyCopy(dst []byte) []byte {
	return append(dst[:0], m.Key()...)
}

func (m *SingleSourceMerger) ValueCopy(dst []byte) []byte {
	return append(dst[:0], m.Value()...)
}

func (m *SingleSourceMerger) Error() error {
	if m.err != nil {
		return m.err
	}
	if len(m.sources) != 0 {
		return m.sources[0].Iter.Error()
	}
	return nil
}

func (m *SingleSourceMerger) Close() error {
	if len(m.sources) == 0 {
		return nil
	}
	source := m.sources[0].Iter
	m.sources, m.key = nil, nil
	m.valid, m.forwardAligned = false, false
	return source.Close()
}

func (m *SingleSourceMerger) Domain() ([]byte, []byte) { return m.start, m.end }
