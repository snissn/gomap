package collections

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// textConstructorValues is private typed storage over the maintained ordinal
// index. It has no owner or refund. Keys borrow the original admitted analysis
// backing, which outlives every lookup. Replacement backing stays cumulatively
// charged on that SAME phase callback.
type textConstructorEntry[V any] struct {
	key   string
	value V
}
type textConstructorValues[V any] struct {
	index   *localIDIndex
	entries []textConstructorEntry[V]
	admit   func(uint64) error
}

func newTextConstructorValues[V any](admit func(uint64) error) (*textConstructorValues[V], error) {
	if admit == nil {
		return nil, memtable.ErrCOWClosed
	}
	if err := admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textConstructorValues[V]{})))); err != nil {
		return nil, err
	}
	index, err := newLocalIDIndex(0, admit)
	if err != nil {
		return nil, err
	}
	return &textConstructorValues[V]{index: index, admit: admit}, nil
}
func (m *textConstructorValues[V]) get(key string) (V, bool) {
	var zero V
	if m == nil {
		return zero, false
	}
	i, ok := m.index.lookup(key)
	if !ok {
		return zero, false
	}
	return m.entries[i].value, true
}
func (m *textConstructorValues[V]) set(key string, value V) error {
	if i, ok := m.index.lookup(key); ok {
		m.entries[i].value = value
		return nil
	}
	capacity, err := textTermGrowthCapacity(len(m.entries), cap(m.entries))
	if err != nil {
		return err
	}
	payload := uint64(0)
	if capacity != cap(m.entries) {
		width := uint64(unsafe.Sizeof(textConstructorEntry[V]{}))
		if uint64(capacity) > uint64(int(^uint(0)>>1))/width {
			return memtable.ErrCOWCapacity
		}
		payload = memtable.COWAllocationCharge(uint64(capacity) * width)
	}
	// Admit the cell replacement and typed lane together, before either changes.
	if err = m.index.prepare(payload); err != nil {
		return err
	}
	if capacity != cap(m.entries) {
		next := make([]textConstructorEntry[V], len(m.entries), capacity)
		copy(next, m.entries)
		m.entries = next
	}
	ordinal := len(m.entries)
	m.entries = append(m.entries, textConstructorEntry[V]{key: key, value: value})
	m.index.insert(key, ordinal)
	return nil
}

// textValues keeps legacy map lookup where no native receipt is claimed.
// Native callers select the same typed lane for the complete private phase.
type textValues[V any] struct {
	legacy map[string]V
	native *textConstructorValues[V]
	admit  func(uint64) error
}

func (m *textValues[V]) get(key string) V { v, _ := m.find(key); return v }
func (m *textValues[V]) find(key string) (V, bool) {
	if m.native != nil {
		return m.native.get(key)
	}
	v, ok := m.legacy[key]
	return v, ok
}
func (m *textValues[V]) set(key string, value V) error {
	if m.admit == nil {
		if m.legacy == nil {
			m.legacy = make(map[string]V)
		}
		m.legacy[key] = value
		return nil
	}
	if m.native == nil {
		var err error
		m.native, err = newTextConstructorValues[V](m.admit)
		if err != nil {
			return err
		}
	}
	return m.native.set(key, value)
}
func (m *textValues[V]) len() int {
	if m.native != nil {
		return len(m.native.entries)
	}
	return len(m.legacy)
}

// all is synchronous and retains no callback. Legacy iteration keeps its
// original direct map walk; native iteration borrows the admitted value lane.
func (m *textValues[V]) all(yield func(string, V) bool) {
	if m.native != nil {
		for _, entry := range m.native.entries {
			if !yield(entry.key, entry.value) {
				return
			}
		}
		return
	}
	for key, value := range m.legacy {
		if !yield(key, value) {
			return
		}
	}
}
func (m *textValues[V]) keys() ([]string, error) {
	n := m.len()
	if err := admitLocalIDArray(m.admit, n, uint64(unsafe.Sizeof(string("")))); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	if m.native != nil {
		for _, entry := range m.native.entries {
			out = append(out, entry.key)
		}
	} else {
		for k := range m.legacy {
			out = append(out, k)
		}
	}
	return out, nil
}

type textNumberEntry[V any] struct {
	key   uint64
	value V
}
type textNumberValues[V any] struct {
	entries []textNumberEntry[V]
	admit   func(uint64) error
}

func newTextNumberValues[V any](admit func(uint64) error) (*textNumberValues[V], error) {
	if err := admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textNumberValues[V]{})))); err != nil {
		return nil, err
	}
	return &textNumberValues[V]{admit: admit}, nil
}
func (m *textNumberValues[V]) position(key uint64) int {
	lo, hi := 0, len(m.entries)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if m.entries[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
func (m *textNumberValues[V]) get(key uint64) V {
	i := m.position(key)
	if i < len(m.entries) && m.entries[i].key == key {
		return m.entries[i].value
	}
	var zero V
	return zero
}
func (m *textNumberValues[V]) set(key uint64, value V) error {
	i := m.position(key)
	if i < len(m.entries) && m.entries[i].key == key {
		m.entries[i].value = value
		return nil
	}
	capacity, err := textTermGrowthCapacity(len(m.entries), cap(m.entries))
	if err != nil {
		return err
	}
	if capacity != cap(m.entries) {
		if err = admitLocalIDArray(m.admit, capacity, uint64(unsafe.Sizeof(textNumberEntry[V]{}))); err != nil {
			return err
		}
		next := make([]textNumberEntry[V], len(m.entries), capacity)
		copy(next, m.entries)
		m.entries = next
	}
	m.entries = append(m.entries, textNumberEntry[V]{})
	copy(m.entries[i+1:], m.entries[i:len(m.entries)-1])
	m.entries[i] = textNumberEntry[V]{key: key, value: value}
	return nil
}

func appendTextConstructor[V any](values []V, value V, admit func(uint64) error) ([]V, error) {
	if admit == nil {
		return append(values, value), nil
	}
	capacity, err := textTermGrowthCapacity(len(values), cap(values))
	if err != nil {
		return nil, err
	}
	if capacity != cap(values) {
		if err = admitLocalIDArray(admit, capacity, uint64(unsafe.Sizeof(value))); err != nil {
			return nil, err
		}
		next := make([]V, len(values), capacity)
		copy(next, values)
		values = next
	}
	return append(values, value), nil
}
