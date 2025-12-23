package skiplist

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	maxHeight = 20

	nodeHeightOff  = 0
	nodeKeyLenOff  = 1
	nodeValLenOff  = 3
	nodeFlagsOff   = 7
	nodeHeaderBase = 8 // Height + KeyLen + ValLen + Flags

	flagDeleted = 1

	// Arena Constants
	// We use 64KB chunks. With uint32 pointers, this allows for 65,536 chunks,
	// providing a total addressable arena of 4GB.
	chunkShift = 16
	chunkSize  = 1 << chunkShift // 65536 bytes
	chunkMask  = chunkSize - 1   // 0xFFFF
)

// SkipList is an arena-backed skiplist using chunked memory to eliminate resizing copy costs.
type SkipList struct {
	// chunks maps virtual 64KB pages to physical memory.
	// chunks[i] is a slice starting at the beginning of the physical chunk.
	chunks [][]byte

	// allocated holds the underlying memory allocations to allow reuse on Reset.
	allocated [][]byte

	head   uint32
	tail   [maxHeight]uint32
	rnd    *rand.Rand
	height int
	size   int64 // Logical size (approx)
	count  int   // Number of items

	// Allocator state
	curChunkIdx int
	curChunkOff int
}

// New creates a new SkipList.
func New(capacity int) *SkipList {
	s := &SkipList{
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
		height: 1,
	}
	// Pre-allocate first chunk if capacity suggested
	if capacity > 0 {
		s.ensureSpace(0, chunkSize)
	}
	// Allocate dummy head (height=maxHeight, key=empty, val=empty)
	s.head = s.allocNode(0, 0, maxHeight)
	for i := range s.tail {
		s.tail[i] = s.head
	}
	return s
}

// Reset clears all entries while retaining the allocated arena capacity.
// This allows the skiplist to be reused without incurring new allocations.
func (s *SkipList) Reset() {
	// Reset pointers in the head node
	for i := 0; i < maxHeight; i++ {
		s.setNext(s.head, i, 0)
		s.tail[i] = s.head
	}
	s.height = 1
	s.size = 0
	s.count = 0

	// Determine size of the head node to skip over it.
	// We assume the head node is at the start of the first chunk.
	h := int(s.valAt(s.head, nodeHeightOff))
	kLen := int(binary.LittleEndian.Uint16(s.bytesAt(s.head+nodeKeyLenOff, 2)))
	vLen := int(binary.LittleEndian.Uint32(s.bytesAt(s.head+nodeValLenOff, 4)))
	headSize := nodeHeaderBase + (4 * h) + kLen + vLen

	// Reset allocator to just after the head
	s.curChunkIdx = 0
	s.curChunkOff = headSize
}

// alloc allocates n bytes in the arena and returns the virtual offset.
// Pointer format: (ChunkIndex << 16) | Offset
func (s *SkipList) alloc(n int) uint32 {
	// 1. Handle Huge Allocations (> 64KB)
	// We allocate a contiguous block and consume enough virtual chunks to cover it.
	if n > chunkSize {
		// Align to the next chunk boundary
		if s.curChunkOff > 0 {
			s.curChunkIdx++
			s.curChunkOff = 0
		}
		startIdx := s.curChunkIdx
		s.ensureSpace(startIdx, n)

		chunksNeeded := (n + chunkSize - 1) / chunkSize
		s.curChunkIdx += chunksNeeded
		s.curChunkOff = 0

		return uint32(startIdx) << chunkShift
	}

	// 2. Handle Standard Allocations
	// If it doesn't fit in the current chunk, move to the next one.
	if s.curChunkOff+n > chunkSize {
		s.curChunkIdx++
		s.curChunkOff = 0
	}

	s.ensureSpace(s.curChunkIdx, chunkSize)

	off := (uint32(s.curChunkIdx) << chunkShift) | uint32(s.curChunkOff)
	s.curChunkOff += n
	return off
}

// ensureSpace guarantees that s.chunks[idx] exists and has sufficient capacity.
func (s *SkipList) ensureSpace(idx, size int) {
	// Reuse existing chunk if possible (fast path for Reset/Reuse)
	if idx < len(s.chunks) {
		if cap(s.chunks[idx]) >= size {
			s.chunks[idx] = s.chunks[idx][:size]
			return
		}
	}

	allocSize := size
	if allocSize < chunkSize {
		allocSize = chunkSize
	}

	// Allocate new backing array
	buf := make([]byte, allocSize)
	s.allocated = append(s.allocated, buf)

	chunksNeeded := (allocSize + chunkSize - 1) / chunkSize

	// Grow chunks directory if needed
	if idx+chunksNeeded > len(s.chunks) {
		grow := (idx + chunksNeeded) - len(s.chunks)
		if grow < 16 {
			grow = 16
		}
		s.chunks = append(s.chunks, make([][]byte, grow)...)
	}

	// Map virtual chunks to the physical buffer.
	// For huge allocations, multiple virtual chunks point to slices of the same large buffer.
	for i := 0; i < chunksNeeded; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > allocSize {
			end = allocSize
		}
		s.chunks[idx+i] = buf[start:end:allocSize]
	}
}

func (s *SkipList) allocNode(keyLen, valLen, height int) uint32 {
	size := nodeHeaderBase + (4 * height) + keyLen + valLen
	off := s.alloc(size)

	s.setValAt(off, nodeHeightOff, uint8(height))
	binary.LittleEndian.PutUint16(s.bytesAt(off+nodeKeyLenOff, 2), uint16(keyLen))
	binary.LittleEndian.PutUint32(s.bytesAt(off+nodeValLenOff, 4), uint32(valLen))
	return off
}

// --- Accessors ---

func (s *SkipList) valAt(ptr uint32, off int) byte {
	p := ptr + uint32(off)
	return s.chunks[p>>chunkShift][p&chunkMask]
}

func (s *SkipList) setValAt(ptr uint32, off int, val byte) {
	p := ptr + uint32(off)
	s.chunks[p>>chunkShift][p&chunkMask] = val
}

func (s *SkipList) bytesAt(ptr uint32, len int) []byte {
	idx := ptr >> chunkShift
	off := ptr & chunkMask
	// Note: For huge items, this slice extends beyond the standard 64KB chunk boundary.
	// This works because ensureSpace sets the capacity of the slice to the full huge buffer.
	return s.chunks[idx][off : off+uint32(len)]
}

func (s *SkipList) getKey(node uint32) []byte {
	h := int(s.valAt(node, nodeHeightOff))
	kLen := int(binary.LittleEndian.Uint16(s.bytesAt(node+nodeKeyLenOff, 2)))
	offset := node + uint32(nodeHeaderBase) + uint32(4*h)
	return s.bytesAt(offset, kLen)
}

func (s *SkipList) getValue(node uint32) []byte {
	h := int(s.valAt(node, nodeHeightOff))
	kLen := int(binary.LittleEndian.Uint16(s.bytesAt(node+nodeKeyLenOff, 2)))
	vLen := int(binary.LittleEndian.Uint32(s.bytesAt(node+nodeValLenOff, 4)))
	offset := node + uint32(nodeHeaderBase) + uint32(4*h) + uint32(kLen)
	return s.bytesAt(offset, vLen)
}

func (s *SkipList) getFlags(node uint32) uint8 {
	return s.valAt(node, nodeFlagsOff)
}

func (s *SkipList) setFlags(node uint32, f uint8) {
	s.setValAt(node, nodeFlagsOff, f)
}

func (s *SkipList) getNext(node uint32, level int) uint32 {
	offset := node + uint32(nodeHeaderBase) + uint32(4*level)
	return binary.LittleEndian.Uint32(s.bytesAt(offset, 4))
}

func (s *SkipList) setNext(node uint32, level int, next uint32) {
	offset := node + uint32(nodeHeaderBase) + uint32(4*level)
	binary.LittleEndian.PutUint32(s.bytesAt(offset, 4), next)
}

// --- Standard SkipList Logic ---

// Put inserts or updates a key.
func (s *SkipList) Put(key, value []byte) {
	s.put(key, value, 0, nil)
}

// PutWithCallback inserts key/value, calling cb with views into the arena before linking.
func (s *SkipList) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	return s.put(key, value, 0, cb)
}

// LastKey returns the largest key currently in the skiplist, or nil if empty.
func (s *SkipList) LastKey() []byte {
	last := s.tail[0]
	if last == 0 || last == s.head {
		return nil
	}
	return s.getKey(last)
}

// AppendWithCallback inserts a new entry assuming the key is strictly greater.
func (s *SkipList) AppendWithCallback(key, value []byte, flags uint8, cb func(k, v []byte) error) error {
	var prev [maxHeight]uint32
	for i := 0; i < maxHeight; i++ {
		prev[i] = s.tail[i]
	}
	return s.insertNew(key, value, flags, cb, &prev, true)
}

func (s *SkipList) Append(key, value []byte) {
	_ = s.AppendWithCallback(key, value, 0, nil)
}

func (s *SkipList) AppendDelete(key []byte) {
	_ = s.AppendWithCallback(key, nil, flagDeleted, nil)
}

func (s *SkipList) Delete(key []byte) {
	s.put(key, nil, flagDeleted, nil)
}

func (s *SkipList) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return s.put(key, nil, flagDeleted, cb)
}

func (s *SkipList) insertNew(key, value []byte, flags uint8, cb func(k, v []byte) error, prev *[maxHeight]uint32, updateTail bool) error {
	h := s.randomHeight()
	if h > s.height {
		s.height = h
	}
	newNode := s.allocNode(len(key), len(value), h)
	copy(s.getKey(newNode), key)
	copy(s.getValue(newNode), value)
	s.setFlags(newNode, flags)

	if cb != nil {
		if err := cb(s.getKey(newNode), s.getValue(newNode)); err != nil {
			return err
		}
	}

	for i := 0; i < h; i++ {
		s.setNext(newNode, i, s.getNext(prev[i], i))
		s.setNext(prev[i], i, newNode)
		if updateTail {
			s.tail[i] = newNode
		}
	}

	s.size += int64(len(key) + len(value))
	s.count++
	return nil
}

func (s *SkipList) put(key, value []byte, flags uint8, cb func(k, v []byte) error) error {
	if s.count == 0 {
		var prev [maxHeight]uint32
		for i := 0; i < maxHeight; i++ {
			prev[i] = s.tail[i]
		}
		return s.insertNew(key, value, flags, cb, &prev, true)
	}

	last := s.tail[0]
	if last != s.head {
		if bytes.Compare(s.getKey(last), key) < 0 {
			var prev [maxHeight]uint32
			for i := 0; i < maxHeight; i++ {
				prev[i] = s.tail[i]
			}
			return s.insertNew(key, value, flags, cb, &prev, true)
		}
	}

	var prev [maxHeight]uint32
	for i := range prev {
		prev[i] = s.head
	}
	x := s.head
	top := s.height
	if top < 1 {
		top = 1
	}
	for i := top - 1; i >= 0; i-- {
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
				// Update existing node (inplace if fits, else replace)
				oldValLen := int(binary.LittleEndian.Uint32(s.bytesAt(next+nodeValLenOff, 4)))
				if cb == nil && len(value) <= oldValLen {
					binary.LittleEndian.PutUint32(s.bytesAt(next+nodeValLenOff, 4), uint32(len(value)))
					s.setFlags(next, flags)
					kLen := int(binary.LittleEndian.Uint16(s.bytesAt(next+nodeKeyLenOff, 2)))
					vOffset := next + uint32(nodeHeaderBase) + uint32(4*int(s.valAt(next, nodeHeightOff))) + uint32(kLen)
					copy(s.bytesAt(vOffset, len(value)), value)
					return nil
				}
				break
			}
			x = next
		}
		prev[i] = x
	}

	// Check level 0
	next := s.getNext(x, 0)
	replaceTail := false
	if next != 0 && bytes.Equal(s.getKey(next), key) {
		replaceTail = next == s.tail[0]
		oldValLen := int(binary.LittleEndian.Uint32(s.bytesAt(next+nodeValLenOff, 4)))
		if cb == nil && len(value) <= oldValLen {
			binary.LittleEndian.PutUint32(s.bytesAt(next+nodeValLenOff, 4), uint32(len(value)))
			s.setFlags(next, flags)
			kLen := int(binary.LittleEndian.Uint16(s.bytesAt(next+nodeKeyLenOff, 2)))
			vOffset := next + uint32(nodeHeaderBase) + uint32(4*int(s.valAt(next, nodeHeightOff))) + uint32(kLen)
			copy(s.bytesAt(vOffset, len(value)), value)
			return nil
		}

		oldHeight := int(s.valAt(next, nodeHeightOff))
		for i := 0; i < oldHeight; i++ {
			if s.getNext(prev[i], i) == next {
				s.setNext(prev[i], i, s.getNext(next, i))
			}
		}
		if replaceTail {
			for i := 0; i < oldHeight; i++ {
				if s.tail[i] == next {
					s.tail[i] = prev[i]
				}
			}
		}
	}

	return s.insertNew(key, value, flags, cb, &prev, replaceTail)
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
				flags := s.getFlags(next)
				return s.getValue(next), flags&flagDeleted != 0, true
			}
			x = next
		}
	}
	return nil, false, false
}

func (s *SkipList) Size() int64 {
	// Approximation based on allocated chunks
	return int64(len(s.allocated)) * chunkSize
}

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
