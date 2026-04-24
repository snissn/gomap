package memtable

import (
	"bytes"
	"strings"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	btree "github.com/tidwall/btree"
)

const btreeDefaultDegree = 32
const btreeUseLoadFastPath = false
const btreeArenaChunkSize = 1 << 20
const btreeInlineValueDedupeMax = 1 << 20

type btreeEntry struct {
	value []byte // inline bytes (if pointer, may store inline tail bytes)
	flags byte
}

func btreeEntryValue(entry btreeEntry) []byte {
	if entry.flags&node.FlagPointer != 0 {
		return btreePointerInlineValue(entry.value)
	}
	if entry.flags&node.FlagTombstone != 0 {
		return nil
	}
	return entry.value
}

func btreePointerInlineValue(value []byte) []byte {
	if len(value) <= page.ValuePtrSize {
		return nil
	}
	return value[page.ValuePtrSize:]
}

func btreeEntryValuePtr(entry btreeEntry) page.ValuePtr {
	if entry.flags&node.FlagPointer == 0 || len(entry.value) < page.ValuePtrSize {
		return page.ValuePtr{}
	}
	return page.DecodeValuePtr(entry.value[:page.ValuePtrSize])
}

func canonicalizeBTreeInlineValue(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return value
}

// btreeEntryPayloadSize tracks the logical value payload bytes contributing to
// memtable flush thresholds, not the fixed in-memory struct footprint.
func btreeEntryPayloadSize(flags byte, value []byte) int {
	if flags&node.FlagPointer != 0 {
		return len(value)
	}
	if flags&node.FlagTombstone != 0 {
		return 0
	}
	return len(value)
}

func normalizeBTreeEntryFlags(flags byte) byte {
	if flags&node.FlagTombstone != 0 {
		return flags &^ node.FlagPointer
	}
	if flags&node.FlagPointer != 0 {
		return flags &^ node.FlagTombstone
	}
	return flags &^ (node.FlagPointer | node.FlagTombstone)
}

type BTree struct {
	mu         sync.RWMutex
	tree       *btree.Map[string, btreeEntry]
	sizeBytes  int64
	arena      *btreeArena
	lastKey    string
	hasLast    bool
	degree     int
	lastInline []byte
}

func (*BTree) StableUnsafeIteratorSlices() bool { return true }

func NewBTree() *BTree {
	return NewBTreeWithDegree(btreeDefaultDegree)
}

func (m *BTree) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, nil, onKey)
}

func (m *BTree) ApplyStealSortedBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, nil, onKey)
}

func (m *BTree) ApplyStealSortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, idxs, onKey)
}

func (m *BTree) ApplyCopySortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, idx := range idxs {
		op := entries[idx]
		if op.Key == nil {
			continue
		}
		keyCopy := m.arena.Copy(op.Key)
		keyStr := bytesToStringNoCopy(keyCopy)
		entry := m.btreeEntryCopyFromBatchOpLocked(op, storeInlinePtrValues)
		prev, replaced := m.setMaybeSortedLoadLocked(keyStr, entry)
		m.recordSetLocked(keyStr, len(keyCopy), entry, prev, replaced)
		if onKey != nil {
			onKey(op.Key)
		}
	}
}

func (m *BTree) applyStealSortedBatch(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	apply := func(op batchpkg.Entry) {
		if op.Key == nil {
			return
		}
		keyStr := bytesToStringNoCopy(op.Key)
		entry := m.btreeEntryFromBatchOpLocked(op)
		prev, replaced := m.setMaybeSortedLoadLocked(keyStr, entry)
		m.recordSetLocked(keyStr, len(op.Key), entry, prev, replaced)
		if onKey != nil {
			onKey(op.Key)
		}
	}

	if idxs == nil {
		for _, op := range entries {
			apply(op)
		}
		return
	}
	for _, idx := range idxs {
		apply(entries[idx])
	}
}

func (m *BTree) btreeEntryCopyFromBatchOpLocked(op batchpkg.Entry, storeInlinePtrValues bool) btreeEntry {
	switch {
	case op.Type == batchpkg.OpDelete:
		return btreeEntry{flags: node.FlagTombstone}
	case op.IsPtr:
		value := op.Value
		if !storeInlinePtrValues {
			value = nil
		}
		return btreeEntry{
			value: m.copyPointerValueLocked(op.ValuePtr, value),
			flags: node.FlagPointer,
		}
	default:
		value := canonicalizeBTreeInlineValue(op.Value)
		return btreeEntry{
			value: m.copyInlineValueLocked(value),
			flags: node.FlagInline,
		}
	}
}

func NewBTreeWithDegree(degree int) *BTree {
	if degree <= 0 {
		degree = btreeDefaultDegree
	}
	return &BTree{
		tree:   btree.NewMap[string, btreeEntry](degree),
		degree: degree,
		arena: &btreeArena{
			chunkSize: btreeArenaChunkSize,
		},
	}
}

// Reset clears all entries while retaining internal allocations.
func (m *BTree) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tree = btree.NewMap[string, btreeEntry](m.degree)
	m.sizeBytes = 0
	m.lastKey = ""
	m.hasLast = false
	m.lastInline = nil
	if m.arena != nil {
		m.arena.resetKeepFirstChunk()
	}
}

func (m *BTree) Set(key, value []byte) {
	m.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *BTree) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	if cb != nil {
		keyCopy := append([]byte(nil), key...)
		valCopy := append([]byte(nil), value...)
		if err := cb(keyCopy, valCopy); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStored := m.arena.Copy(key)
	valStored := m.copyInlineValueLocked(canonicalizeBTreeInlineValue(value))
	keyStr := bytesToStringNoCopy(keyStored)
	entry := btreeEntry{value: valStored, flags: node.FlagInline}
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(keyStored), entry, prev, replaced)
	return nil
}

func (m *BTree) SetSteal(key, value []byte) {
	m.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *BTree) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	if key == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keyCopy := m.arena.Copy(key)
	keyStr := bytesToStringNoCopy(keyCopy)
	entry := btreeEntry{flags: normalizeBTreeEntryFlags(flags)}
	switch {
	case entry.flags&node.FlagTombstone != 0:
		entry.value = nil
	case entry.flags&node.FlagPointer != 0:
		entry.value = m.copyPointerValueLocked(ptr, value)
	default:
		value = canonicalizeBTreeInlineValue(value)
		entry.value = m.copyInlineValueLocked(value)
	}
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(keyCopy), entry, prev, replaced)
}

func (m *BTree) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	if key == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStr := bytesToStringNoCopy(key)
	entry := btreeEntry{flags: normalizeBTreeEntryFlags(flags)}
	switch {
	case entry.flags&node.FlagTombstone != 0:
		entry.value = nil
	case entry.flags&node.FlagPointer != 0:
		entry.value = m.copyPointerValueLocked(ptr, value)
	default:
		entry.value = canonicalizeBTreeInlineValue(value)
	}
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(key), entry, prev, replaced)
}

func (m *BTree) Delete(key []byte) {
	m.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *BTree) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	if cb != nil {
		keyCopy := append([]byte(nil), key...)
		if err := cb(keyCopy, nil); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStored := m.arena.Copy(key)
	keyStr := bytesToStringNoCopy(keyStored)
	entry := btreeEntry{flags: node.FlagTombstone}
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(keyStored), entry, prev, replaced)
	return nil
}

func (m *BTree) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *BTree) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, false, false
	}
	return btreeEntryValue(val), val.flags&node.FlagTombstone != 0, true
}

func (m *BTree) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, page.ValuePtr{}, 0, false
	}
	return btreeEntryValue(val), btreeEntryValuePtr(val), val.flags, true
}

func (m *BTree) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *BTree) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Len()
}

func (m *BTree) Freeze() {}

func (m *BTree) NewIterator(start, end []byte) iterator.UnsafeIterator {
	startKey := ""
	if start != nil {
		startKey = bytesToStringNoCopy(start)
	}
	endKey := ""
	hasEnd := false
	if end != nil {
		endKey = bytesToStringNoCopy(end)
		hasEnd = true
	}

	m.mu.RLock()
	iter := m.tree.Iter()

	valid := false
	if startKey == "" {
		valid = iter.First()
	} else {
		valid = iter.Seek(startKey)
	}

	it := &btreeIterator{
		iter:   iter,
		end:    endKey,
		hasEnd: hasEnd,
		valid:  valid,
		mu:     &m.mu,
	}
	it.refresh()
	return it
}

func (m *BTree) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	startKey := ""
	hasStart := false
	if start != nil {
		startKey = bytesToStringNoCopy(start)
		hasStart = true
	}
	endKey := ""
	hasEnd := false
	if end != nil {
		endKey = bytesToStringNoCopy(end)
		hasEnd = true
	}

	m.mu.RLock()
	iter := m.tree.Iter()

	valid := false
	if !hasEnd {
		valid = iter.Last()
	} else {
		valid = iter.Seek(endKey)
		if valid {
			// Seek positions at the first key >= end. Reverse iteration is over
			// [start, end), so step back to the last key < end.
			valid = iter.Prev()
		} else {
			// seek(end) fell off the end.
			valid = iter.Last()
		}
	}

	it := &btreeReverseIterator{
		iter:     iter,
		start:    startKey,
		hasStart: hasStart,
		end:      endKey,
		hasEnd:   hasEnd,
		valid:    valid,
		mu:       &m.mu,
	}
	it.refresh()
	return it
}

type btreeIterator struct {
	iter   btree.MapIter[string, btreeEntry]
	end    string
	hasEnd bool
	valid  bool
	cur    btreeEntry
	hasCur bool
	mu     *sync.RWMutex
}

func (it *btreeIterator) Seek(key []byte) {
	it.valid = it.iter.Seek(bytesToStringNoCopy(key))
	it.refresh()
}

func (it *btreeIterator) Next() {
	if !it.valid {
		return
	}
	it.valid = it.iter.Next()
	it.refresh()
}

func (it *btreeIterator) Valid() bool {
	if !it.valid || !it.hasCur {
		return false
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		return false
	}
	return true
}

func (it *btreeIterator) UnsafeKey() []byte {
	if !it.hasCur {
		return nil
	}
	return stringToBytesNoCopy(it.iter.Key())
}

func (it *btreeIterator) UnsafeValue() []byte {
	if !it.hasCur {
		return nil
	}
	return btreeEntryValue(it.cur)
}

func (it *btreeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return btreeEntryValue(it.cur), btreeEntryValuePtr(it.cur), it.cur.flags
}

func (it *btreeIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *btreeIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *btreeIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *btreeIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *btreeIterator) IsDeleted() bool {
	if !it.hasCur {
		return false
	}
	return it.cur.flags&node.FlagTombstone != 0
}

func (it *btreeIterator) Error() error {
	return nil
}

func (it *btreeIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	return nil
}

func (it *btreeIterator) Domain() (start, end []byte) {
	if !it.hasEnd {
		return nil, nil
	}
	return nil, []byte(it.end)
}

func (it *btreeIterator) refresh() {
	it.hasCur = false
	if !it.valid {
		return
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		it.valid = false
		return
	}
	it.cur = it.iter.Value()
	it.hasCur = true
}

type btreeReverseIterator struct {
	iter     btree.MapIter[string, btreeEntry]
	start    string
	hasStart bool
	end      string
	hasEnd   bool
	valid    bool
	cur      btreeEntry
	hasCur   bool
	mu       *sync.RWMutex
}

func (it *btreeReverseIterator) Seek(key []byte) {
	seekToReverseEnd := func() {
		if !it.hasEnd {
			it.valid = it.iter.Last()
			return
		}
		it.valid = it.iter.Seek(it.end)
		if it.valid {
			// Seek positions at the first key >= end. Reverse iteration is over
			// [start, end), so step back to the last key < end.
			it.valid = it.iter.Prev()
			return
		}
		it.valid = it.iter.Last()
	}

	if key == nil {
		seekToReverseEnd()
		it.refresh()
		return
	}
	keyStr := bytesToStringNoCopy(key)
	if it.hasEnd && strings.Compare(keyStr, it.end) >= 0 {
		seekToReverseEnd()
		it.refresh()
		return
	}
	found := it.iter.Seek(keyStr)
	if !found {
		it.valid = it.iter.Last()
		it.refresh()
		return
	}
	it.valid = true
	if strings.Compare(it.iter.Key(), keyStr) > 0 {
		it.valid = it.iter.Prev()
	}
	it.refresh()
}

func (it *btreeReverseIterator) Next() {
	if !it.valid {
		return
	}
	it.valid = it.iter.Prev()
	it.refresh()
}

func (it *btreeReverseIterator) Valid() bool {
	if !it.valid || !it.hasCur {
		return false
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		return false
	}
	if it.hasStart && strings.Compare(it.iter.Key(), it.start) < 0 {
		return false
	}
	return true
}

func (it *btreeReverseIterator) UnsafeKey() []byte {
	if !it.hasCur {
		return nil
	}
	return stringToBytesNoCopy(it.iter.Key())
}

func (it *btreeReverseIterator) UnsafeValue() []byte {
	if !it.hasCur {
		return nil
	}
	return btreeEntryValue(it.cur)
}

func (it *btreeReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return btreeEntryValue(it.cur), btreeEntryValuePtr(it.cur), it.cur.flags
}

func (it *btreeReverseIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *btreeReverseIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *btreeReverseIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *btreeReverseIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *btreeReverseIterator) IsDeleted() bool {
	if !it.hasCur {
		return false
	}
	return it.cur.flags&node.FlagTombstone != 0
}

func (it *btreeReverseIterator) Error() error {
	return nil
}

func (it *btreeReverseIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	return nil
}

func (it *btreeReverseIterator) Domain() (start, end []byte) {
	if !it.hasEnd && !it.hasStart {
		return nil, nil
	}
	var s []byte
	if it.hasStart {
		s = []byte(it.start)
	}
	var e []byte
	if it.hasEnd {
		e = []byte(it.end)
	}
	return s, e
}

func (it *btreeReverseIterator) refresh() {
	it.hasCur = false
	if !it.valid {
		return
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		it.valid = false
		return
	}
	if it.hasStart && strings.Compare(it.iter.Key(), it.start) < 0 {
		it.valid = false
		return
	}
	it.cur = it.iter.Value()
	it.hasCur = true
}

func (m *BTree) setMaybeLoadLocked(key string, entry btreeEntry) (btreeEntry, bool) {
	if btreeUseLoadFastPath && m.hasLast && key > m.lastKey {
		return m.tree.Load(key, entry)
	}
	return m.tree.Set(key, entry)
}

func (m *BTree) setMaybeSortedLoadLocked(key string, entry btreeEntry) (btreeEntry, bool) {
	if m.hasLast && key > m.lastKey {
		return m.tree.Load(key, entry)
	}
	return m.tree.Set(key, entry)
}

func (m *BTree) copyInlineValueLocked(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	if len(value) <= btreeInlineValueDedupeMax && bytes.Equal(value, m.lastInline) {
		return m.lastInline
	}
	stored := m.arena.Copy(value)
	if len(stored) <= btreeInlineValueDedupeMax {
		m.lastInline = stored
	} else {
		m.lastInline = nil
	}
	return stored
}

func (m *BTree) copyPointerValueLocked(ptr page.ValuePtr, value []byte) []byte {
	value = canonicalizeBTreeInlineValue(value)
	stored := m.arena.alloc(page.ValuePtrSize + len(value))
	ptr.Encode(stored[:page.ValuePtrSize])
	copy(stored[page.ValuePtrSize:], value)
	return stored
}

func (m *BTree) btreeEntryFromBatchOpLocked(op batchpkg.Entry) btreeEntry {
	switch {
	case op.Type == batchpkg.OpDelete:
		return btreeEntry{flags: node.FlagTombstone}
	case op.IsPtr:
		return btreeEntry{
			value: m.copyPointerValueLocked(op.ValuePtr, op.Value),
			flags: node.FlagPointer,
		}
	default:
		return btreeEntry{
			value: canonicalizeBTreeInlineValue(op.Value),
			flags: node.FlagInline,
		}
	}
}

func (m *BTree) recordSetLocked(key string, keyLen int, entry, prev btreeEntry, replaced bool) {
	newSize := btreeEntryPayloadSize(entry.flags, entry.value)
	if replaced {
		oldSize := btreeEntryPayloadSize(prev.flags, prev.value)
		m.sizeBytes += int64(newSize - oldSize)
	} else {
		m.sizeBytes += int64(keyLen + newSize)
	}
	if !m.hasLast || key > m.lastKey {
		m.lastKey = key
		m.hasLast = true
	}
}

type btreeArena struct {
	chunkSize int
	chunks    [][]byte
	offset    int
}

func (a *btreeArena) resetKeepFirstChunk() {
	if a == nil {
		return
	}
	if len(a.chunks) == 0 {
		a.offset = 0
		return
	}
	first := a.chunks[0]
	first = first[:len(first)]
	a.chunks = a.chunks[:1]
	a.chunks[0] = first
	a.offset = 0
}

func (a *btreeArena) Copy(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.alloc(len(src))
	copy(dst, src)
	return dst
}

func (a *btreeArena) alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	chunk := a.currentChunk(n)
	start := a.offset
	a.offset += n
	return chunk[start:a.offset]
}

func (a *btreeArena) currentChunk(n int) []byte {
	if a == nil {
		return make([]byte, n)
	}
	if len(a.chunks) == 0 {
		size := a.chunkSize
		if size < n {
			size = n
		}
		a.chunks = append(a.chunks, make([]byte, size))
		a.offset = 0
		return a.chunks[len(a.chunks)-1]
	}
	chunk := a.chunks[len(a.chunks)-1]
	if a.offset+n <= len(chunk) {
		return chunk
	}
	size := a.chunkSize
	if size < n {
		size = n
	}
	a.chunks = append(a.chunks, make([]byte, size))
	a.offset = 0
	return a.chunks[len(a.chunks)-1]
}
