package skiplist

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	maxHeight = 20

	nodeHeightOff   = 0
	nodeKeyLenOff   = 1
	nodeValLenOff   = 3
	nodeFlagsOff    = 7
	nodeRevisionOff = 8
	nodeHeaderBase  = 16 // Height + KeyLen + ValLen + Flags + Revision

	flagPointer = node.FlagPointer
	flagDeleted = node.FlagTombstone

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
	rnd    *rand.Rand
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
	if s.curChunkOff+n > chunkSize {
		s.curChunkIdx++
		s.curChunkOff = 0
	}

	s.ensureSpace(s.curChunkIdx, chunkSize)

	if s.curChunkIdx > maxChunkIndex() {
		panic("skiplist arena size exceeded 4GB")
	}

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
		// slice[i:j:k] sets capacity to k-i. This ensures that for huge allocations,
		// the slice has enough capacity to allow bytesAt to read past the 64KB boundary.
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

func (s *SkipList) getRevision(node uint32) page.EntryRevision {
	return page.EntryRevision(binary.LittleEndian.Uint64(s.bytesAt(node+nodeRevisionOff, page.EntryRevisionSize)))
}

func (s *SkipList) setRevision(node uint32, revision page.EntryRevision) {
	binary.LittleEndian.PutUint64(s.bytesAt(node+nodeRevisionOff, page.EntryRevisionSize), uint64(revision))
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
	s.put(key, value, 0, page.LegacyEntryRevision, nil)
}

// PutWithCallback inserts key/value, calling cb with views into the arena before linking.
func (s *SkipList) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	return s.put(key, value, 0, page.LegacyEntryRevision, cb)
}

// PutEntry inserts key/value with explicit flags and optional value pointer.
// When flags include FlagPointer, ptr is encoded into the value area; if value is
// non-nil, it is appended after the pointer bytes.
func (s *SkipList) PutEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	s.PutEntryWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

// PutEntryWithRevision inserts key/value with explicit flags, pointer metadata,
// and native entry revision.
func (s *SkipList) PutEntryWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	if flags&flagPointer != 0 {
		if len(value) > 0 {
			buf := make([]byte, page.ValuePtrSize+len(value))
			ptr.Encode(buf[:page.ValuePtrSize])
			copy(buf[page.ValuePtrSize:], value)
			_ = s.put(key, buf, flags, revision, nil)
			return
		}
		var buf [page.ValuePtrSize]byte
		ptr.Encode(buf[:])
		_ = s.put(key, buf[:], flags, revision, nil)
		return
	}
	if flags&flagDeleted != 0 {
		_ = s.put(key, nil, flags, revision, nil)
		return
	}
	_ = s.put(key, value, flags, revision, nil)
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
	return s.AppendWithCallbackRevision(key, value, flags, page.LegacyEntryRevision, cb)
}

// AppendWithCallbackRevision inserts a new strictly-increasing entry with a
// native revision token.
func (s *SkipList) AppendWithCallbackRevision(key, value []byte, flags uint8, revision page.EntryRevision, cb func(k, v []byte) error) error {
	var prev [maxHeight]uint32
	for i := 0; i < maxHeight; i++ {
		prev[i] = s.tail[i]
	}
	return s.insertNew(key, value, flags, revision, cb, &prev)
}

// Append inserts a new key/value entry assuming the key is strictly greater
// than the current maximum key in the skiplist.
func (s *SkipList) Append(key, value []byte) {
	_ = s.AppendWithCallback(key, value, 0, nil)
}

// AppendDelete inserts a tombstone for key assuming the key is strictly greater
// than the current maximum key in the skiplist.
func (s *SkipList) AppendDelete(key []byte) {
	_ = s.AppendDeleteWithRevision(key, page.LegacyEntryRevision)
}

// AppendDeleteWithRevision appends a tombstone with a native revision token.
func (s *SkipList) AppendDeleteWithRevision(key []byte, revision page.EntryRevision) error {
	return s.AppendWithCallbackRevision(key, nil, flagDeleted, revision, nil)
}

// Delete marks a key as deleted (tombstone).
func (s *SkipList) Delete(key []byte) {
	s.put(key, nil, flagDeleted, page.LegacyEntryRevision, nil)
}

// DeleteWithCallback marks deleted with callback.
func (s *SkipList) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return s.put(key, nil, flagDeleted, page.LegacyEntryRevision, cb)
}

func (s *SkipList) insertNew(key, value []byte, flags uint8, revision page.EntryRevision, cb func(k, v []byte) error, prev *[maxHeight]uint32) error {
	h := s.randomHeight()
	if h > s.height {
		s.height = h
	}
	newNode := s.allocNode(len(key), len(value), h)
	copy(s.getKey(newNode), key)
	copy(s.getValue(newNode), value)
	s.setFlags(newNode, flags)
	s.setRevision(newNode, revision)

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

func (s *SkipList) put(key, value []byte, flags uint8, revision page.EntryRevision, cb func(k, v []byte) error) error {
	if s.count == 0 {
		var prev [maxHeight]uint32
		for i := 0; i < maxHeight; i++ {
			prev[i] = s.tail[i]
		}
		return s.insertNew(key, value, flags, revision, cb, &prev)
	}

	last := s.tail[0]
	if last != s.head {
		if bytes.Compare(s.getKey(last), key) < 0 {
			var prev [maxHeight]uint32
			for i := 0; i < maxHeight; i++ {
				prev[i] = s.tail[i]
			}
			return s.insertNew(key, value, flags, revision, cb, &prev)
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

	return s.insertNew(key, value, flags, revision, cb, &prev)
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
				if flags&flagPointer != 0 {
					val := s.getValue(next)
					if len(val) > page.ValuePtrSize {
						return val[page.ValuePtrSize:], flags&flagDeleted != 0, true
					}
					return nil, flags&flagDeleted != 0, true
				}
				return s.getValue(next), flags&flagDeleted != 0, true
			}
			x = next
		}
	}
	return nil, false, false
}

// GetEntry returns the raw entry, including pointer and flags, if present.
func (s *SkipList) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	val, ptr, flags, _, found := s.GetEntryWithRevision(key)
	return val, ptr, flags, found
}

// GetEntryWithRevision returns the raw entry plus native revision metadata.
func (s *SkipList) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
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
				revision := s.getRevision(next)
				val := s.getValue(next)
				if flags&flagPointer != 0 {
					if len(val) >= page.ValuePtrSize {
						inline := []byte(nil)
						if len(val) > page.ValuePtrSize {
							inline = val[page.ValuePtrSize:]
						}
						return inline, page.DecodeValuePtr(val[:page.ValuePtrSize]), flags, revision, true
					}
					return nil, page.ValuePtr{}, flags, revision, true
				}
				return val, page.ValuePtr{}, flags, revision, true
			}
			x = next
		}
	}
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
}

func (s *SkipList) Size() int64 {
	// Approximation based on the high-water mark within the virtual arena.
	// This intentionally does not account for spare capacity in the current chunk.
	return int64(s.curChunkIdx)*chunkSize + int64(s.curChunkOff)
}

func (s *SkipList) Count() int {
	return s.count
}

func (s *SkipList) seekGE(key []byte) uint32 {
	if key == nil {
		return s.getNext(s.head, 0)
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
			if bytes.Compare(k, key) >= 0 {
				break
			}
			x = next
		}
	}
	return s.getNext(x, 0)
}

// SeekGEEntryWithRevision returns the first physical entry whose key is greater
// than or equal to key. Returned slices alias skiplist storage.
func (s *SkipList) SeekGEEntryWithRevision(key []byte) (entryKey, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	curr := s.seekGE(key)
	if curr == 0 {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	entryKey = s.getKey(curr)
	value = s.getValue(curr)
	flags = s.getFlags(curr)
	revision = s.getRevision(curr)
	if flags&flagPointer != 0 {
		if len(value) >= page.ValuePtrSize {
			ptr = page.DecodeValuePtr(value[:page.ValuePtrSize])
			value = value[page.ValuePtrSize:]
		}
	} else if flags&flagDeleted != 0 {
		value = nil
	}
	return entryKey, value, ptr, flags, revision, true
}

func (s *SkipList) findLessThan(key []byte) uint32 {
	if key == nil {
		last := s.tail[0]
		if last == 0 || last == s.head {
			return 0
		}
		return last
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
			if bytes.Compare(k, key) >= 0 {
				break
			}
			x = next
		}
	}
	if x == s.head {
		return 0
	}
	return x
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
	it.curr = it.sl.seekGE(key)
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
	flags := it.sl.getFlags(it.curr)
	if flags&flagPointer != 0 {
		val := it.sl.getValue(it.curr)
		if len(val) > page.ValuePtrSize {
			return val[page.ValuePtrSize:]
		}
		return nil
	}
	return it.sl.getValue(it.curr)
}

func (it *Iterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

// UnsafeEntryWithRevision returns raw entry details plus native revision.
func (it *Iterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	val := it.sl.getValue(it.curr)
	flags := it.sl.getFlags(it.curr)
	revision := it.sl.getRevision(it.curr)
	if flags&flagPointer != 0 {
		if len(val) >= page.ValuePtrSize {
			inline := []byte(nil)
			if len(val) > page.ValuePtrSize {
				inline = val[page.ValuePtrSize:]
			}
			return inline, page.DecodeValuePtr(val[:page.ValuePtrSize]), flags, revision
		}
		return nil, page.ValuePtr{}, flags, revision
	}
	return val, page.ValuePtr{}, flags, revision
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

type ReverseIterator struct {
	sl    *SkipList
	start []byte
	end   []byte
	curr  uint32
	valid bool
}

func (s *SkipList) NewReverseIterator(start, end []byte) *ReverseIterator {
	it := &ReverseIterator{sl: s, start: start, end: end}
	it.Seek(nil)
	return it
}

func (it *ReverseIterator) Seek(key []byte) {
	if it == nil || it.sl == nil {
		return
	}
	if key == nil || (it.end != nil && bytes.Compare(key, it.end) >= 0) {
		if it.end == nil {
			it.curr = it.sl.findLessThan(nil)
		} else {
			it.curr = it.sl.findLessThan(it.end)
		}
		it.valid = it.curr != 0
		it.clampStart()
		return
	}
	pos := it.sl.seekGE(key)
	if pos == 0 {
		it.curr = it.sl.findLessThan(nil)
		it.valid = it.curr != 0
		it.clampStart()
		return
	}
	k := it.sl.getKey(pos)
	if bytes.Equal(k, key) {
		it.curr = pos
		it.valid = true
		it.clampStart()
		return
	}
	it.curr = it.sl.findLessThan(key)
	it.valid = it.curr != 0
	it.clampStart()
}

func (it *ReverseIterator) Next() {
	if !it.valid {
		return
	}
	key := it.sl.getKey(it.curr)
	it.curr = it.sl.findLessThan(key)
	it.valid = it.curr != 0
	it.clampStart()
}

func (it *ReverseIterator) Valid() bool { return it.valid }

func (it *ReverseIterator) UnsafeKey() []byte {
	if !it.valid || it.curr == 0 {
		return nil
	}
	return it.sl.getKey(it.curr)
}

func (it *ReverseIterator) UnsafeValue() []byte {
	if !it.valid || it.curr == 0 {
		return nil
	}
	flags := it.sl.getFlags(it.curr)
	if flags&flagPointer != 0 {
		val := it.sl.getValue(it.curr)
		if len(val) > page.ValuePtrSize {
			return val[page.ValuePtrSize:]
		}
		return nil
	}
	return it.sl.getValue(it.curr)
}

func (it *ReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

// UnsafeEntryWithRevision returns raw entry details plus native revision.
func (it *ReverseIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.valid || it.curr == 0 {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	val := it.sl.getValue(it.curr)
	flags := it.sl.getFlags(it.curr)
	revision := it.sl.getRevision(it.curr)
	if flags&flagPointer != 0 {
		if len(val) >= page.ValuePtrSize {
			inline := []byte(nil)
			if len(val) > page.ValuePtrSize {
				inline = val[page.ValuePtrSize:]
			}
			return inline, page.DecodeValuePtr(val[:page.ValuePtrSize]), flags, revision
		}
		return nil, page.ValuePtr{}, flags, revision
	}
	return val, page.ValuePtr{}, flags, revision
}

func (it *ReverseIterator) IsDeleted() bool {
	if !it.valid || it.curr == 0 {
		return false
	}
	return it.sl.getFlags(it.curr)&flagDeleted != 0
}

func (it *ReverseIterator) Close() error { return nil }

func (it *ReverseIterator) Key() []byte { return it.UnsafeKey() }

func (it *ReverseIterator) Value() []byte { return it.UnsafeValue() }

func (it *ReverseIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *ReverseIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *ReverseIterator) Error() error { return nil }

func (it *ReverseIterator) Domain() (start, end []byte) { return it.start, it.end }

func (it *ReverseIterator) clampStart() {
	if !it.valid || it.start == nil {
		return
	}
	if key := it.sl.getKey(it.curr); bytes.Compare(key, it.start) < 0 {
		it.curr = 0
		it.valid = false
	}
}
