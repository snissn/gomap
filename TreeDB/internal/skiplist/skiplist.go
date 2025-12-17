package skiplist

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	maxHeight      = 20
	nodeHeaderBase = 1 + 2 + 4 + 1 // Height + KeyLen + ValLen + Flags
	flagDeleted    = 1
)

// SkipList is an arena-backed skiplist.
type SkipList struct {
	data  []byte
	head  uint32
	rnd   *rand.Rand
	size  int64 // Logical size (approx)
	count int   // Number of items
}

// New creates a new SkipList with pre-allocated capacity.
func New(capacity int) *SkipList {
	s := &SkipList{
		data: make([]byte, 0, capacity),
		rnd:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	// Allocate dummy head (height=maxHeight, key=empty, val=empty)
	s.head = s.allocNode(0, 0, maxHeight)
	return s
}

func (s *SkipList) alloc(n int) uint32 {
	off := uint32(len(s.data))

	needed := len(s.data) + n
	if cap(s.data) < needed {
		newCap := cap(s.data)
		if newCap == 0 {
			newCap = 64 * 1024
		}
		for newCap < needed {
			newCap *= 2
		}
		newData := make([]byte, len(s.data), newCap)
		copy(newData, s.data)
		s.data = newData
	}

	// We intentionally do not zero the newly exposed bytes here. Every field that
	// is later read is explicitly written before use (header, next pointers for
	// all levels < height, key/value bytes, flags).
	s.data = s.data[:needed]
	return off
}

func (s *SkipList) allocNode(keyLen, valLen, height int) uint32 {
	size := nodeHeaderBase + (4 * height) + keyLen + valLen
	off := s.alloc(size)

	s.data[off] = uint8(height)
	binary.LittleEndian.PutUint16(s.data[off+1:], uint16(keyLen))
	binary.LittleEndian.PutUint32(s.data[off+3:], uint32(valLen))
	return off
}

func (s *SkipList) getKey(node uint32) []byte {
	h := int(s.data[node])
	kLen := int(binary.LittleEndian.Uint16(s.data[node+1:]))
	offset := node + uint32(nodeHeaderBase) + uint32(4*h)
	return s.data[offset : offset+uint32(kLen)]
}

func (s *SkipList) getValue(node uint32) []byte {
	h := int(s.data[node])
	kLen := int(binary.LittleEndian.Uint16(s.data[node+1:]))
	vLen := int(binary.LittleEndian.Uint32(s.data[node+3:]))
	offset := node + uint32(nodeHeaderBase) + uint32(4*h) + uint32(kLen)
	return s.data[offset : offset+uint32(vLen)]
}

func (s *SkipList) getFlags(node uint32) uint8 {
	return s.data[node+7]
}

func (s *SkipList) setFlags(node uint32, f uint8) {
	s.data[node+7] = f
}

func (s *SkipList) getNext(node uint32, level int) uint32 {
	offset := node + uint32(nodeHeaderBase) + uint32(4*level)
	return binary.LittleEndian.Uint32(s.data[offset:])
}

func (s *SkipList) setNext(node uint32, level int, next uint32) {
	offset := node + uint32(nodeHeaderBase) + uint32(4*level)
	binary.LittleEndian.PutUint32(s.data[offset:], next)
}

// Put inserts or updates a key.
func (s *SkipList) Put(key, value []byte) {
	s.put(key, value, 0, nil)
}

// PutWithCallback inserts key/value, calling cb with views into the arena before linking.
func (s *SkipList) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	return s.put(key, value, 0, cb)
}

// Delete marks a key as deleted (tombstone).
func (s *SkipList) Delete(key []byte) {
	s.put(key, nil, flagDeleted, nil)
}

// DeleteWithCallback marks deleted with callback.
func (s *SkipList) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return s.put(key, nil, flagDeleted, cb)
}

func (s *SkipList) put(key, value []byte, flags uint8, cb func(k, v []byte) error) error {
	var prev [maxHeight]uint32
	x := s.head
	for i := maxHeight - 1; i >= 0; i-- {
		for {
			next := s.getNext(x, i)
			if next == 0 {
				break
			}
			k := s.getKey(next)
			cmp := bytes.Compare(k, key)
			if cmp > 0 {
				break
			}
			if cmp == 0 {
				// Update existing node
				// If fits, inplace. Else replace.
				// Exception: If cb != nil, we MUST alloc new to allow rollback.
				oldValLen := int(binary.LittleEndian.Uint32(s.data[next+3:]))
				if cb == nil && len(value) <= oldValLen {
					// Inplace update
					// Write new value len
					binary.LittleEndian.PutUint32(s.data[next+3:], uint32(len(value)))
					// Write flags
					s.setFlags(next, flags)
					// Copy value
					vOffset := next + uint32(nodeHeaderBase) + uint32(4*int(s.data[next])) + uint32(len(key))
					copy(s.data[vOffset:], value)
					return nil
				}

				// Must replace node.
				break
			}
			x = next
		}
		prev[i] = x
	}

	// Check if x.next is target (at level 0)
	next := s.getNext(x, 0)
	if next != 0 && bytes.Equal(s.getKey(next), key) {
		oldValLen := int(binary.LittleEndian.Uint32(s.data[next+3:]))
		// Check inplace again (for level 0 case)
		if cb == nil && len(value) <= oldValLen {
			binary.LittleEndian.PutUint32(s.data[next+3:], uint32(len(value)))
			s.setFlags(next, flags)
			vOffset := next + uint32(nodeHeaderBase) + uint32(4*int(s.data[next])) + uint32(len(key))
			copy(s.data[vOffset:], value)
			return nil
		}

		// Unlink logic:
		oldHeight := int(s.data[next])
		for i := 0; i < oldHeight; i++ {
			if s.getNext(prev[i], i) == next {
				s.setNext(prev[i], i, s.getNext(next, i))
			}
		}
		// Now OldNode is gone.
	}

	// Insert New Node
	h := s.randomHeight()
	newNode := s.allocNode(len(key), len(value), h)
	copy(s.getKey(newNode), key)
	copy(s.getValue(newNode), value)
	s.setFlags(newNode, flags)

	// Callback (Before linking)
	if cb != nil {
		kView := s.getKey(newNode)
		vView := s.getValue(newNode)
		if err := cb(kView, vView); err != nil {
			return err // Abort: New node allocated but not linked.
		}
	}

	for i := 0; i < h; i++ {
		s.setNext(newNode, i, s.getNext(prev[i], i))
		s.setNext(prev[i], i, newNode)
	}

	s.size += int64(len(key) + len(value))
	s.count++
	return nil
}

func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && s.rnd.Float32() < 0.25 {
		h++
	}
	return h
}

// Get returns value, isDeleted, exists
func (s *SkipList) Get(key []byte) ([]byte, bool, bool) {
	x := s.head
	for i := maxHeight - 1; i >= 0; i-- {
		for {
			next := s.getNext(x, i)
			if next == 0 {
				break
			}
			k := s.getKey(next)
			cmp := bytes.Compare(k, key)
			if cmp > 0 {
				break
			}
			if cmp == 0 {
				// Found
				// If we are at level > 0, we can return immediately?
				// Yes, key matches.
				flags := s.getFlags(next)
				return s.getValue(next), flags&flagDeleted != 0, true
			}
			x = next
		}
	}
	return nil, false, false
}

// Size returns allocated bytes
func (s *SkipList) Size() int64 {
	return int64(len(s.data))
}

// Count returns the number of items
func (s *SkipList) Count() int {
	return s.count
}

// Iterator support
type Iterator struct {
	sl    *SkipList
	curr  uint32
	valid bool
}

func (s *SkipList) NewIterator(start, end []byte) *Iterator {
	it := &Iterator{sl: s}
	it.Seek(start)
	// Apply end bound? The caller (DB) usually handles bounds or we can check Valid?
	// The interface `iterator.UnsafeIterator` doesn't enforce bounds check in `Next`, caller logic does?
	// Or we can store end.
	return it
}

func (it *Iterator) Seek(key []byte) {
	if key == nil {
		it.curr = it.sl.getNext(it.sl.head, 0)
		it.valid = it.curr != 0
		return
	}

	x := it.sl.head
	for i := maxHeight - 1; i >= 0; i-- {
		for {
			next := it.sl.getNext(x, i)
			if next == 0 {
				break
			}
			k := it.sl.getKey(next)
			if bytes.Compare(k, key) >= 0 {
				break
			}
			x = next
		}
	}
	it.curr = it.sl.getNext(x, 0)
	it.valid = it.curr != 0
}

func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	it.curr = it.sl.getNext(it.curr, 0)
	it.valid = it.curr != 0
}

func (it *Iterator) Valid() bool {
	return it.valid
}

func (it *Iterator) UnsafeKey() []byte {
	return it.sl.getKey(it.curr)
}

func (it *Iterator) UnsafeValue() []byte {
	return it.sl.getValue(it.curr)
}

func (it *Iterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val := it.sl.getValue(it.curr)
	flags := it.sl.getFlags(it.curr)
	// SkipList doesn't use Pointers, so ptr is empty.
	return val, page.ValuePtr{}, flags
}

func (it *Iterator) IsDeleted() bool {
	return it.sl.getFlags(it.curr)&flagDeleted != 0
}

func (it *Iterator) Close() error {
	return nil
}

func (it *Iterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *Iterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *Iterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *Iterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *Iterator) Error() error {
	return nil
}

func (it *Iterator) Domain() (start, end []byte) {
	return nil, nil
}
