package skiplist

import (
	"bytes"
	"encoding/binary"
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

	headNodeSize = nodeHeaderBase + (4 * maxHeight)

	// Arena Constants
	// We use 4MiB chunks. With uint32 pointers, this provides a total addressable
	// arena of 4GiB.
	chunkShift = 22
	chunkSize  = 1 << chunkShift // 4 MiB
	chunkMask  = chunkSize - 1
)

// SkipList is an arena-backed skiplist using chunked memory to eliminate resizing copy costs.
type SkipList struct {
	// chunks maps virtual chunk IDs to physical memory slices.
	chunks [][]byte

	// allocated holds the underlying memory allocations to allow reuse on Reset.
	allocated [][]byte

	head   uint32
	tail   [maxHeight]uint32
	rng    uint64
	height int
	size   int64 // Logical size (approx)
	count  int   // Number of items

	// Allocator state
	curChunkIdx int
	curChunkOff int
}

func maxChunkIndex() int {
	// Upper bits store chunk index, lower bits store offset within chunk.
	// maxIndex = 2^(32-chunkShift) - 1
	return (1 << (32 - chunkShift)) - 1
}

// New creates a new SkipList.
func New(capacity int) *SkipList {
	s := &SkipList{
		height: 1,
	}
	s.rng = uint64(time.Now().UnixNano())
	if s.rng == 0 {
		s.rng = 0xdeadbeefcafebabe
	}
	if capacity < headNodeSize {
		capacity = headNodeSize
	}
	if capacity > chunkSize {
		capacity = chunkSize
	}
	s.ensureSpace(0, capacity)
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

/*
### The 4GB Limit & Panic Explanation

#### 1. Why 4GB? (The Technical Constraint)
The limit comes from how the SkipList addresses memory. To save RAM, it uses **32-bit integers (`uint32`)** as "pointers" instead of standard 64-bit pointers.

-   **Standard 64-bit pointer:** 8 bytes per link.
-   **This implementation:** 4 bytes per link.

Since a SkipList has many links (up to 20 per node), saving 4 bytes per link is significant.

The 32 bits are split:
-   **High (32 - chunkShift) bits:** Chunk Index
-   **Low chunkShift bits:** Offset within Chunk (0 to (chunkSize - 1) bytes)

Math: `2^(32-chunkShift) chunks * 2^chunkShift bytes/chunk = 2^32 bytes` (**4GB**).

#### 2. When can this panic?
It panics if the **total active data** in a single Memtable exceeds 4GB.
This happens when `s.curChunkIdx > 0xFFFF`.

#### 3. Will it ever happen?
**In normal operation: No.**
In an LSM-tree (like TreeDB), Memtables are temporary buffers. They are typically flushed to disk when they reach **4MB to 128MB**.
-   **Scenario A (Normal):** User writes 64MB -> Memtable flushes -> Reset -> Empty.
-   **Scenario B (The Panic):** User configures `FlushThreshold` to 5GB, or the flush process hangs indefinitely while the user continues to pump data into the *same* in-memory table.

#### 4. Alternatives to Panic
The panic is a "fail-fast" mechanism for an impossible state, but there are alternatives:

**A. Return an Error (Safe but intrusive)**
Change `alloc()` to return `(uint32, error)`.
-   **Pros:** The application doesn't crash; it just gets an error like `errMemtableFull`.
-   **Cons:** Requires refactoring every internal method (`put`, `insertNew`, `allocNode`) to handle errors, which complicates the code significantly for a scenario that "should never happen."

**B. Use 64-bit Pointers (Unlimited size)**
Change the pointer type to `uint64`.
-   **Pros:** Limit becomes 18 exabytes.
-   **Cons:** Doubles the memory overhead for pointers. A node with height 20 would grow from ~80 bytes of links to ~160 bytes.

**C. Early Hard Cap (Best Practice)**
Instead of panicking *during* allocation, enforce a configuration limit.
-   **Constraint:** `if Config.FlushThreshold > 3GB { return Error }`.
-   **Runtime Check:** In `Put`, check `if s.Size() > 3GB { return ErrMemtableFull }` *before* attempting to allocate. This is cleaner than a panic deep in the allocator.

### Summary
The panic exists because **addressing more than 4GB is mathematically impossible** with the current 32-bit optimized design. It acts as a final guard rail against memory corruption.

Given that Memtables are meant to be small (MBs, not GBs), this design tradeoff (half the pointer memory usage vs. 4GB max size) is usually considered the right choice for high-performance databases.
*/

// alloc allocates n bytes in the arena and returns the virtual offset.
// Pointer format: (ChunkIndex << chunkShift) | Offset
func (s *SkipList) alloc(n int) uint32 {
	// 1. Handle Huge Allocations (> chunkSize)
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

		if s.curChunkIdx > maxChunkIndex() {
			panic("skiplist arena size exceeded 4GB")
		}
		return uint32(startIdx) << chunkShift
	}

	// 2. Handle Standard Allocations
	// If it doesn't fit in the current chunk, move to the next one.
	if s.curChunkIdx == 0 && len(s.chunks) > 0 {
		// Chunk 0 may be sized based on the provided capacity hint (e.g. small
		// "iterator rotation" memtables). Once it's full, start allocating from
		// chunk 1 rather than growing chunk 0 and copying existing content.
		c0cap := cap(s.chunks[0])
		if c0cap > 0 && c0cap < chunkSize && s.curChunkOff+n > c0cap {
			s.curChunkIdx++
			s.curChunkOff = 0
		}
	}
	if s.curChunkOff+n > chunkSize {
		s.curChunkIdx++
		s.curChunkOff = 0
	}

	ensureSize := s.curChunkOff + n
	if s.curChunkIdx > 0 && ensureSize < chunkSize {
		ensureSize = chunkSize
	}
	s.ensureSpace(s.curChunkIdx, ensureSize)

	if s.curChunkIdx > maxChunkIndex() {
		panic("skiplist arena size exceeded 4GB")
	}

	off := (uint32(s.curChunkIdx) << chunkShift) | uint32(s.curChunkOff)
	s.curChunkOff += n
	return off
}

// ensureSpace guarantees that s.chunks[idx] exists and has sufficient capacity.
func (s *SkipList) ensureSpace(idx, size int) {
	if size <= 0 {
		size = 1
	}

	// Reuse existing chunk if possible (fast path for Reset/Reuse)
	var old []byte
	if idx < len(s.chunks) {
		old = s.chunks[idx]
		if cap(old) >= size {
			s.chunks[idx] = old[:size]
			return
		}
	}

	allocSize := size
	if idx > 0 && allocSize < chunkSize {
		allocSize = chunkSize
	}

	if len(old) > 0 {
		growCap := cap(old) * 2
		if growCap < allocSize {
			growCap = allocSize
		}
		if growCap < size {
			growCap = size
		}
		if idx == 0 && growCap > chunkSize {
			growCap = chunkSize
		}
		allocSize = growCap
	}

	// Allocate new backing array and preserve any existing content for this chunk.
	buf := make([]byte, allocSize)
	if len(old) > 0 {
		copy(buf, old)
	}
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

	if chunksNeeded == 1 {
		s.chunks[idx] = buf[:size:allocSize]
		return
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
	// Note: For huge items, this slice extends beyond the standard 64KB chunk boundary
	// into the capacity of the backing buffer.
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
	p := node + uint32(nodeHeaderBase) + uint32(4*level)
	idx := int(p >> chunkShift)
	off := int(p & chunkMask)
	b := s.chunks[idx]
	return uint32(b[off]) | uint32(b[off+1])<<8 | uint32(b[off+2])<<16 | uint32(b[off+3])<<24
}

func (s *SkipList) setNext(node uint32, level int, next uint32) {
	p := node + uint32(nodeHeaderBase) + uint32(4*level)
	idx := int(p >> chunkShift)
	off := int(p & chunkMask)
	b := s.chunks[idx]
	b[off] = byte(next)
	b[off+1] = byte(next >> 8)
	b[off+2] = byte(next >> 16)
	b[off+3] = byte(next >> 24)
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

// AppendWithCallback inserts a new entry assuming the key is strictly greater
// than the current maximum key in the skiplist.
func (s *SkipList) AppendWithCallback(key, value []byte, flags uint8, cb func(k, v []byte) error) error {
	var prev [maxHeight]uint32
	for i := 0; i < maxHeight; i++ {
		prev[i] = s.tail[i]
	}
	return s.insertNew(key, value, flags, cb, &prev)
}

// Append inserts a new key/value entry assuming the key is strictly greater
// than the current maximum key in the skiplist.
func (s *SkipList) Append(key, value []byte) {
	_ = s.AppendWithCallback(key, value, 0, nil)
}

// AppendDelete inserts a tombstone for key assuming the key is strictly greater
// than the current maximum key in the skiplist.
func (s *SkipList) AppendDelete(key []byte) {
	_ = s.AppendWithCallback(key, nil, flagDeleted, nil)
}

// Delete marks a key as deleted (tombstone).
func (s *SkipList) Delete(key []byte) {
	s.put(key, nil, flagDeleted, nil)
}

// DeleteWithCallback marks deleted with callback.
func (s *SkipList) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return s.put(key, nil, flagDeleted, cb)
}

func (s *SkipList) insertNew(key, value []byte, flags uint8, cb func(k, v []byte) error, prev *[maxHeight]uint32) error {
	h := s.randomHeight()
	if h > s.height {
		s.height = h
	}
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
		next := s.getNext(prev[i], i)
		s.setNext(newNode, i, next)
		s.setNext(prev[i], i, newNode)
		if next == 0 {
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
		return s.insertNew(key, value, flags, cb, &prev)
	}

	last := s.tail[0]
	if last != s.head {
		if bytes.Compare(s.getKey(last), key) < 0 {
			var prev [maxHeight]uint32
			for i := 0; i < maxHeight; i++ {
				prev[i] = s.tail[i]
			}
			return s.insertNew(key, value, flags, cb, &prev)
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
				// Update existing node
				// Always replace nodes on update. This avoids mutating existing
				// key/value bytes in-place, which can race with readers that may hold
				// views beyond the SkipList mutex critical section (e.g. GetAppend).
				break
			}
			x = next
		}
		prev[i] = x
	}

	// Check level 0
	next := s.getNext(x, 0)
	if next != 0 && bytes.Equal(s.getKey(next), key) {
		// Unlink logic:
		oldHeight := int(s.valAt(next, nodeHeightOff))
		for i := 0; i < oldHeight; i++ {
			if s.getNext(prev[i], i) == next {
				s.setNext(prev[i], i, s.getNext(next, i))
			}
			if s.tail[i] == next {
				s.tail[i] = prev[i]
			}
		}
		// Now OldNode is gone.
	}

	return s.insertNew(key, value, flags, cb, &prev)
}

func (s *SkipList) randomHeight() int {
	const mask = uint64(0x3) // 2 bits => p=0.25
	h := 1
	r := s.rand64()
	for h < maxHeight && (r&mask) == 0 {
		h++
		r >>= 2
	}
	return h
}

// rand64 returns pseudo-random bits for level selection.
// Distribution quality is not critical; it must be fast and non-zero.
func (s *SkipList) rand64() uint64 {
	// xorshift64*
	x := s.rng
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	s.rng = x
	return x * 2685821657736338717
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
	// Approximation based on the high-water mark within the virtual arena.
	// This intentionally does not account for spare capacity in the current chunk.
	return int64(s.curChunkIdx)*chunkSize + int64(s.curChunkOff)
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
