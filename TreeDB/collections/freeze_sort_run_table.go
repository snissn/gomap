package collections

import (
	"bytes"
	"errors"
	"sort"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type freezeSortRunEntry struct {
	key      []byte
	value    []byte
	ptr      page.ValuePtr
	seq      uint64
	flags    byte
	revision page.EntryRevision
}

type freezeSortRunTable struct {
	mu              sync.RWMutex
	entries         []freezeSortRunEntry
	latest          map[string]int
	latestDirty     bool
	frozen          bool
	released        bool
	sizeBytes       int64
	nextSeq         uint64
	ownerGeneration uint64
}

type freezeSortRunTableHandle struct {
	table           *freezeSortRunTable
	ownerGeneration uint64
}

const (
	freezeSortRunTableMaxPooledEntries      = 512 << 10
	freezeSortRunTableMaxPooledTotalEntries = 1 << 20
	freezeSortRunTableMaxPooledTables       = 32
)

var freezeSortRunTablePool struct {
	mu            sync.Mutex
	tables        []*freezeSortRunTable
	entryCapacity int
}

// freezeSortRunTable is a collection-write-domain run table optimized for
// root-local accumulation: writes append cheaply while mutable, and rotation to
// an immutable flush unit pays the sort/coalesce cost once.
func newFreezeSortRunTable() memtable.Table {
	freezeSortRunTablePool.mu.Lock()
	n := len(freezeSortRunTablePool.tables)
	if n > 0 {
		t := freezeSortRunTablePool.tables[n-1]
		freezeSortRunTablePool.tables[n-1] = nil
		freezeSortRunTablePool.tables = freezeSortRunTablePool.tables[:n-1]
		freezeSortRunTablePool.entryCapacity -= cap(t.entries)
		freezeSortRunTablePool.mu.Unlock()
		t.mu.Lock()
		t.ownerGeneration++
		t.released = false
		ownerGeneration := t.ownerGeneration
		t.mu.Unlock()
		return freezeSortRunTableHandle{table: t, ownerGeneration: ownerGeneration}
	}
	freezeSortRunTablePool.mu.Unlock()
	return freezeSortRunTableHandle{table: &freezeSortRunTable{ownerGeneration: 1}, ownerGeneration: 1}
}

func (h freezeSortRunTableHandle) StableUnsafeIteratorSlices() bool { return true }

func (h freezeSortRunTableHandle) Set(key, value []byte) {
	h.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (h freezeSortRunTableHandle) SetSteal(key, value []byte) {
	h.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (h freezeSortRunTableHandle) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	h.SetEntryWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (h freezeSortRunTableHandle) SetEntryWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	h.SetEntryStealWithRevision(bytes.Clone(key), bytes.Clone(value), ptr, flags, revision)
}

func (h freezeSortRunTableHandle) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	keyCopy := bytes.Clone(key)
	valueCopy := bytes.Clone(value)
	if cb != nil {
		if err := cb(keyCopy, valueCopy); err != nil {
			return err
		}
	}
	h.SetEntrySteal(keyCopy, valueCopy, page.ValuePtr{}, node.FlagInline)
	return nil
}

func (h freezeSortRunTableHandle) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	h.SetEntryStealWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (h freezeSortRunTableHandle) SetEntryStealWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	if h.table == nil {
		return
	}
	h.table.setEntryStealOwned(h.ownerGeneration, key, value, ptr, flags, revision)
}

func (h freezeSortRunTableHandle) ApplyStealEntryFunc(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error)) error {
	if h.table == nil {
		return nil
	}
	return h.table.applyStealEntryFuncOwned(h.ownerGeneration, count, emit)
}

func (h freezeSortRunTableHandle) ApplyStealEntryFuncWithRevision(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, err error)) error {
	if h.table == nil {
		return nil
	}
	return h.table.applyStealEntryFuncWithRevisionOwned(h.ownerGeneration, count, emit)
}

func (h freezeSortRunTableHandle) Delete(key []byte) {
	h.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (h freezeSortRunTableHandle) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	keyCopy := bytes.Clone(key)
	if cb != nil {
		if err := cb(keyCopy, nil); err != nil {
			return err
		}
	}
	h.SetEntrySteal(keyCopy, nil, page.ValuePtr{}, node.FlagTombstone)
	return nil
}

func (h freezeSortRunTableHandle) DeleteSteal(key []byte) {
	h.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (h freezeSortRunTableHandle) SetInlineNilKeyParts(first, second []byte) {
	key := make([]byte, len(first)+len(second))
	n := copy(key, first)
	copy(key[n:], second)
	h.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagInline)
}

func (h freezeSortRunTableHandle) DeleteKeyParts(first, second []byte) {
	key := make([]byte, len(first)+len(second))
	n := copy(key, first)
	copy(key[n:], second)
	h.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (h freezeSortRunTableHandle) Get(key []byte) ([]byte, bool, bool) {
	if h.table == nil {
		return nil, false, false
	}
	return h.table.getOwned(h.ownerGeneration, key)
}

func (h freezeSortRunTableHandle) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	if h.table == nil {
		return nil, page.ValuePtr{}, 0, false
	}
	return h.table.getEntryOwned(h.ownerGeneration, key)
}

func (h freezeSortRunTableHandle) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if h.table == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	return h.table.getEntryWithRevisionOwned(h.ownerGeneration, key)
}

func (h freezeSortRunTableHandle) Size() int64 {
	if h.table == nil {
		return 0
	}
	return h.table.sizeOwned(h.ownerGeneration)
}

func (h freezeSortRunTableHandle) Len() int {
	if h.table == nil {
		return 0
	}
	return h.table.lenOwned(h.ownerGeneration)
}

func (h freezeSortRunTableHandle) Freeze() {
	if h.table != nil {
		h.table.freezeOwned(h.ownerGeneration)
	}
}

func (h freezeSortRunTableHandle) Reset() {
	if h.table != nil {
		h.table.resetOwned(h.ownerGeneration)
	}
}

func (h freezeSortRunTableHandle) Release() {
	if h.table != nil {
		h.table.releaseOwned(h.ownerGeneration)
	}
}

func (h freezeSortRunTableHandle) NewIterator(start, end []byte) iterator.UnsafeIterator {
	if h.table == nil {
		return &freezeSortRunIterator{idx: -1}
	}
	return h.table.newIteratorOwned(h.ownerGeneration, start, end, false)
}

func (h freezeSortRunTableHandle) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	if h.table == nil {
		return &freezeSortRunIterator{reverse: true, idx: -1}
	}
	return h.table.newIteratorOwned(h.ownerGeneration, start, end, true)
}

func (*freezeSortRunTable) StableUnsafeIteratorSlices() bool { return true }

func (t *freezeSortRunTable) Set(key, value []byte) {
	t.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (t *freezeSortRunTable) SetSteal(key, value []byte) {
	t.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (t *freezeSortRunTable) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	t.SetEntryWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (t *freezeSortRunTable) SetEntryWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	t.SetEntryStealWithRevision(bytes.Clone(key), bytes.Clone(value), ptr, flags, revision)
}

func (t *freezeSortRunTable) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	keyCopy := bytes.Clone(key)
	valueCopy := bytes.Clone(value)
	if cb != nil {
		if err := cb(keyCopy, valueCopy); err != nil {
			return err
		}
	}
	t.SetEntrySteal(keyCopy, valueCopy, page.ValuePtr{}, node.FlagInline)
	return nil
}

func (t *freezeSortRunTable) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	t.SetEntryStealWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (t *freezeSortRunTable) SetEntryStealWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	t.setEntryStealOwned(0, key, value, ptr, flags, revision)
}

func (t *freezeSortRunTable) setEntryStealOwned(ownerGeneration uint64, key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	if t == nil || len(key) == 0 {
		return
	}
	if flags&node.FlagTombstone != 0 {
		value = nil
		ptr = page.ValuePtr{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mustOwnedLocked(ownerGeneration)
	idx := len(t.entries)
	t.entries = append(t.entries, freezeSortRunEntry{
		key:      key,
		value:    value,
		ptr:      ptr,
		seq:      t.nextSeq,
		flags:    flags,
		revision: revision,
	})
	t.nextSeq++
	t.sizeBytes += int64(len(key) + entryLogicalValueSize(flags, value))
	if t.latest != nil && !t.latestDirty {
		t.latest[unsafe.String(&key[0], len(key))] = idx
	}
}

func (t *freezeSortRunTable) ApplyStealEntryFunc(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error)) error {
	return t.applyStealEntryFuncOwned(0, count, emit)
}

func (t *freezeSortRunTable) applyStealEntryFuncOwned(ownerGeneration uint64, count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error)) error {
	if emit == nil {
		return errors.New("collections: nil freeze-sort entry emitter")
	}
	return t.applyStealEntryFuncWithRevisionOwned(ownerGeneration, count, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, err error) {
		key, value, ptr, flags, err = emit(i)
		return key, value, ptr, flags, page.LegacyEntryRevision, err
	})
}

func (t *freezeSortRunTable) ApplyStealEntryFuncWithRevision(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, err error)) error {
	return t.applyStealEntryFuncWithRevisionOwned(0, count, emit)
}

func (t *freezeSortRunTable) applyStealEntryFuncWithRevisionOwned(ownerGeneration uint64, count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, err error)) error {
	if count <= 0 {
		return nil
	}
	if emit == nil {
		return errors.New("collections: nil freeze-sort entry emitter")
	}
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mustOwnedLocked(ownerGeneration)
	t.entries = growFreezeSortRunEntries(t.entries, count)
	for i := 0; i < count; i++ {
		key, value, ptr, flags, revision, err := emit(i)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			continue
		}
		if flags&node.FlagTombstone != 0 {
			value = nil
			ptr = page.ValuePtr{}
		}
		idx := len(t.entries)
		t.entries = append(t.entries, freezeSortRunEntry{
			key:      key,
			value:    value,
			ptr:      ptr,
			seq:      t.nextSeq,
			flags:    flags,
			revision: revision,
		})
		t.nextSeq++
		t.sizeBytes += int64(len(key) + entryLogicalValueSize(flags, value))
		if t.latest != nil && !t.latestDirty {
			t.latest[unsafe.String(&key[0], len(key))] = idx
		}
	}
	return nil
}

func (t *freezeSortRunTable) Delete(key []byte) {
	t.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (t *freezeSortRunTable) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	keyCopy := bytes.Clone(key)
	if cb != nil {
		if err := cb(keyCopy, nil); err != nil {
			return err
		}
	}
	t.SetEntrySteal(keyCopy, nil, page.ValuePtr{}, node.FlagTombstone)
	return nil
}

func (t *freezeSortRunTable) DeleteSteal(key []byte) {
	t.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (t *freezeSortRunTable) SetInlineNilKeyParts(first, second []byte) {
	key := make([]byte, len(first)+len(second))
	n := copy(key, first)
	copy(key[n:], second)
	t.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagInline)
}

func (t *freezeSortRunTable) DeleteKeyParts(first, second []byte) {
	key := make([]byte, len(first)+len(second))
	n := copy(key, first)
	copy(key[n:], second)
	t.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (t *freezeSortRunTable) Get(key []byte) ([]byte, bool, bool) {
	return t.getOwned(0, key)
}

func (t *freezeSortRunTable) getOwned(ownerGeneration uint64, key []byte) ([]byte, bool, bool) {
	value, _, flags, found := t.getEntryOwned(ownerGeneration, key)
	if !found {
		return nil, false, false
	}
	return value, flags&node.FlagTombstone != 0, true
}

func (t *freezeSortRunTable) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	return t.getEntryOwned(0, key)
}

func (t *freezeSortRunTable) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	return t.getEntryWithRevisionOwned(0, key)
}

func (t *freezeSortRunTable) getEntryOwned(ownerGeneration uint64, key []byte) ([]byte, page.ValuePtr, byte, bool) {
	value, ptr, flags, _, found := t.getEntryWithRevisionOwned(ownerGeneration, key)
	return value, ptr, flags, found
}

func (t *freezeSortRunTable) getEntryWithRevisionOwned(ownerGeneration uint64, key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if t == nil || len(key) == 0 {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	t.mu.RLock()
	if !t.ownedLocked(ownerGeneration) {
		t.mu.RUnlock()
		panicFreezeSortRunTableNotOwned()
	}
	if t.frozen {
		entry, found := t.getFrozenEntryLocked(key)
		t.mu.RUnlock()
		if !found {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
		}
		return entry.value, entry.ptr, entry.flags, entry.revision, true
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.mustOwnedLocked(ownerGeneration)
	if t.frozen {
		entry, found := t.getFrozenEntryLocked(key)
		if !found {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
		}
		return entry.value, entry.ptr, entry.flags, entry.revision, true
	}
	t.rebuildLatestLocked()
	idx, ok := t.latest[unsafe.String(&key[0], len(key))]
	if !ok || idx < 0 || idx >= len(t.entries) {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	entry := t.entries[idx]
	if !bytes.Equal(entry.key, key) {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	return entry.value, entry.ptr, entry.flags, entry.revision, true
}

func (t *freezeSortRunTable) getFrozenEntryLocked(key []byte) (freezeSortRunEntry, bool) {
	idx := sort.Search(len(t.entries), func(i int) bool {
		return bytes.Compare(t.entries[i].key, key) >= 0
	})
	if idx >= len(t.entries) || !bytes.Equal(t.entries[idx].key, key) {
		return freezeSortRunEntry{}, false
	}
	return t.entries[idx], true
}

func (t *freezeSortRunTable) Size() int64 {
	return t.sizeOwned(0)
}

func (t *freezeSortRunTable) sizeOwned(ownerGeneration uint64) int64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.mustOwnedLocked(ownerGeneration)
	return t.sizeBytes
}

func (t *freezeSortRunTable) Len() int {
	return t.lenOwned(0)
}

func (t *freezeSortRunTable) lenOwned(ownerGeneration uint64) int {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.mustOwnedLocked(ownerGeneration)
	return len(t.entries)
}

func (t *freezeSortRunTable) Freeze() {
	t.freezeOwned(0)
}

func (t *freezeSortRunTable) freezeOwned(ownerGeneration uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mustOwnedLocked(ownerGeneration)
	if t.frozen {
		return
	}
	t.sortAndCoalesceLocked()
	t.latest = nil
	t.latestDirty = false
	t.frozen = true
}

func (t *freezeSortRunTable) Reset() {
	t.resetOwned(0)
}

func (t *freezeSortRunTable) resetOwned(ownerGeneration uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mustOwnedLocked(ownerGeneration)
	t.resetLocked(false)
}

func (t *freezeSortRunTable) Release() {
	t.releaseOwned(0)
}

func (t *freezeSortRunTable) releaseOwned(ownerGeneration uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if ownerGeneration != 0 && ownerGeneration != t.ownerGeneration {
		t.mu.Unlock()
		return
	}
	if t.released {
		t.mu.Unlock()
		return
	}
	t.resetLocked(true)
	t.released = true
	entryCapacity := cap(t.entries)
	t.mu.Unlock()

	if entryCapacity == 0 || entryCapacity > freezeSortRunTableMaxPooledEntries {
		return
	}
	freezeSortRunTablePool.mu.Lock()
	if len(freezeSortRunTablePool.tables) >= freezeSortRunTableMaxPooledTables ||
		freezeSortRunTablePool.entryCapacity+entryCapacity > freezeSortRunTableMaxPooledTotalEntries {
		freezeSortRunTablePool.mu.Unlock()
		t.mu.Lock()
		t.entries = nil
		t.mu.Unlock()
		return
	}
	freezeSortRunTablePool.tables = append(freezeSortRunTablePool.tables, t)
	freezeSortRunTablePool.entryCapacity += entryCapacity
	freezeSortRunTablePool.mu.Unlock()
}

func (t *freezeSortRunTable) resetLocked(dropOversizedEntries bool) {
	if dropOversizedEntries && cap(t.entries) > freezeSortRunTableMaxPooledEntries {
		t.entries = nil
	} else {
		clear(t.entries)
		t.entries = t.entries[:0]
	}
	t.latest = nil
	t.latestDirty = false
	t.frozen = false
	t.sizeBytes = 0
	t.nextSeq = 0
}

func (t *freezeSortRunTable) mustNotReleasedLocked() {
	t.mustOwnedLocked(0)
}

func (t *freezeSortRunTable) mustOwnedLocked(ownerGeneration uint64) {
	if !t.ownedLocked(ownerGeneration) {
		panicFreezeSortRunTableNotOwned()
	}
}

func (t *freezeSortRunTable) ownedLocked(ownerGeneration uint64) bool {
	return !t.released && (ownerGeneration == 0 || ownerGeneration == t.ownerGeneration)
}

func panicFreezeSortRunTableNotOwned() {
	panic("collections: freeze-sort run table used after Release or after reuse by a new owner")
}

func (t *freezeSortRunTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return t.newIteratorOwned(0, start, end, false)
}

func (t *freezeSortRunTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return t.newIteratorOwned(0, start, end, true)
}

func (t *freezeSortRunTable) newIterator(start, end []byte, reverse bool) iterator.UnsafeIterator {
	return t.newIteratorOwned(0, start, end, reverse)
}

func (t *freezeSortRunTable) newIteratorOwned(ownerGeneration uint64, start, end []byte, reverse bool) iterator.UnsafeIterator {
	if t == nil {
		return &freezeSortRunIterator{reverse: reverse, idx: -1}
	}
	t.mu.RLock()
	if !t.ownedLocked(ownerGeneration) {
		t.mu.RUnlock()
		panicFreezeSortRunTableNotOwned()
	}
	if t.frozen {
		entries := t.entries
		it := &freezeSortRunIterator{entries: entries, start: start, end: end, reverse: reverse, table: t}
		it.seekInitial()
		return it
	}
	t.mu.RUnlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mustOwnedLocked(ownerGeneration)
	entries := t.sortedLatestCopyLocked()
	it := &freezeSortRunIterator{entries: entries, start: start, end: end, reverse: reverse}
	it.seekInitial()
	return it
}

func (t *freezeSortRunTable) rebuildLatestLocked() {
	if t.latest != nil && !t.latestDirty {
		return
	}
	if len(t.entries) == 0 {
		t.latest = nil
		t.latestDirty = false
		return
	}
	latest := t.latest
	if latest == nil {
		latest = make(map[string]int, len(t.entries))
	} else {
		clear(latest)
	}
	for i := range t.entries {
		key := t.entries[i].key
		latest[unsafe.String(&key[0], len(key))] = i
	}
	t.latest = latest
	t.latestDirty = false
}

func (t *freezeSortRunTable) sortedLatestCopyLocked() []freezeSortRunEntry {
	if len(t.entries) == 0 {
		return nil
	}
	entries := make([]freezeSortRunEntry, len(t.entries))
	copy(entries, t.entries)
	return sortAndCoalesceFreezeSortEntries(entries)
}

func (t *freezeSortRunTable) sortAndCoalesceLocked() {
	t.entries = sortAndCoalesceFreezeSortEntries(t.entries)
	t.sizeBytes = freezeSortEntriesSize(t.entries)
}

func growFreezeSortRunEntries(entries []freezeSortRunEntry, count int) []freezeSortRunEntry {
	// Indexed insert accumulators append many similarly-sized batches into the
	// same mutable root table before publishing. Doubling here keeps append-copy
	// work bounded for that steady-state path; slices.Grow follows the runtime's
	// post-threshold growth curve and was visible in batch-insert allocation
	// profiles as repeated root-run entry copies.
	if count <= 0 || len(entries)+count <= cap(entries) {
		return entries
	}
	needed := len(entries) + count
	newCap := cap(entries) * 2
	if newCap < needed {
		newCap = needed
	}
	if newCap == 0 {
		newCap = count
	}
	grown := make([]freezeSortRunEntry, len(entries), newCap)
	copy(grown, entries)
	return grown
}

func sortAndCoalesceFreezeSortEntries(entries []freezeSortRunEntry) []freezeSortRunEntry {
	if len(entries) <= 1 {
		return entries
	}
	sort.Slice(entries, func(i, j int) bool {
		if cmp := bytes.Compare(entries[i].key, entries[j].key); cmp != 0 {
			return cmp < 0
		}
		return entries[i].seq < entries[j].seq
	})
	out := entries[:0]
	for i := 0; i < len(entries); {
		j := i + 1
		for j < len(entries) && bytes.Equal(entries[i].key, entries[j].key) {
			j++
		}
		out = append(out, entries[j-1])
		i = j
	}
	clear(entries[len(out):])
	return out
}

func freezeSortEntriesSize(entries []freezeSortRunEntry) int64 {
	var total int64
	for i := range entries {
		total += int64(len(entries[i].key) + entryLogicalValueSize(entries[i].flags, entries[i].value))
	}
	return total
}

func entryLogicalValueSize(flags byte, value []byte) int {
	if flags&node.FlagPointer != 0 {
		return page.ValuePtrSize + len(value)
	}
	if flags&node.FlagTombstone != 0 {
		return 0
	}
	return len(value)
}

type freezeSortRunIterator struct {
	entries []freezeSortRunEntry
	start   []byte
	end     []byte
	table   *freezeSortRunTable
	idx     int
	reverse bool
	closed  bool
}

func (it *freezeSortRunIterator) seekInitial() {
	if it.reverse {
		it.seekReverseEnd()
		return
	}
	it.idx = 0
	if it.start != nil {
		it.Seek(it.start)
	}
	if !it.inBounds() {
		it.idx = len(it.entries)
	}
}

func (it *freezeSortRunIterator) seekReverseEnd() {
	n := len(it.entries)
	if n == 0 {
		it.idx = -1
		return
	}
	if it.end == nil {
		it.idx = n - 1
	} else {
		pos := sort.Search(n, func(i int) bool {
			return bytes.Compare(it.entries[i].key, it.end) >= 0
		})
		it.idx = pos - 1
	}
	if !it.inBounds() {
		it.idx = -1
	}
}

func (it *freezeSortRunIterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.entries) && it.inBounds()
}

func (it *freezeSortRunIterator) Next() {
	if it == nil {
		return
	}
	if it.reverse {
		it.idx--
	} else {
		it.idx++
	}
}

func (it *freezeSortRunIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	pos := sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
	if it.reverse {
		if pos < len(it.entries) && bytes.Equal(it.entries[pos].key, key) {
			it.idx = pos
		} else {
			it.idx = pos - 1
		}
	} else {
		it.idx = pos
	}
	if !it.inBounds() {
		if it.reverse {
			it.idx = -1
		} else {
			it.idx = len(it.entries)
		}
	}
}

func (it *freezeSortRunIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].key
}

func (it *freezeSortRunIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *freezeSortRunIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	value, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return value, ptr, flags
}

func (it *freezeSortRunIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	entry := it.entries[it.idx]
	return entry.value, entry.ptr, entry.flags, entry.revision
}

func (it *freezeSortRunIterator) Key() []byte { return it.UnsafeKey() }

func (it *freezeSortRunIterator) Value() []byte { return it.UnsafeValue() }

func (it *freezeSortRunIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].key...)
}

func (it *freezeSortRunIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].value...)
}

func (it *freezeSortRunIterator) IsDeleted() bool {
	if !it.Valid() {
		return false
	}
	return it.entries[it.idx].flags&node.FlagTombstone != 0
}

func (it *freezeSortRunIterator) Error() error { return nil }

func (it *freezeSortRunIterator) Close() error {
	if it == nil || it.closed {
		return nil
	}
	it.closed = true
	if it.table != nil {
		it.table.mu.RUnlock()
		it.table = nil
	}
	return nil
}

func (it *freezeSortRunIterator) Domain() ([]byte, []byte) {
	if it == nil {
		return nil, nil
	}
	return it.start, it.end
}

func (it *freezeSortRunIterator) StableUnsafeIteratorSlices() bool {
	return true
}

func (it *freezeSortRunIterator) OrderedUniqueUnsafeIterator() bool {
	return true
}

func (it *freezeSortRunIterator) Len() int {
	if it == nil || !it.Valid() {
		return 0
	}
	if it.reverse {
		lower := 0
		if it.start != nil {
			lower = sort.Search(len(it.entries), func(i int) bool {
				return bytes.Compare(it.entries[i].key, it.start) >= 0
			})
		}
		if it.idx < lower {
			return 0
		}
		return it.idx - lower + 1
	}
	upper := len(it.entries)
	if it.end != nil {
		upper = sort.Search(len(it.entries), func(i int) bool {
			return bytes.Compare(it.entries[i].key, it.end) >= 0
		})
	}
	if it.idx >= upper {
		return 0
	}
	return upper - it.idx
}

func (it *freezeSortRunIterator) inBounds() bool {
	if it == nil || it.idx < 0 || it.idx >= len(it.entries) {
		return false
	}
	key := it.entries[it.idx].key
	if it.start != nil && bytes.Compare(key, it.start) < 0 {
		return false
	}
	if it.end != nil && bytes.Compare(key, it.end) >= 0 {
		return false
	}
	return true
}
