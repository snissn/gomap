package memtable

import (
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPutAppendOnlyEntriesClearsReferences(t *testing.T) {
	entries := make([]appendOnlyEntry, 2)
	appendOnlyEntrySetKeyIndex(&entries[0], 1)
	appendOnlyEntrySetPayloadIndex(&entries[0], 1)
	appendOnlyEntrySetKeyIndex(&entries[1], 2)
	appendOnlyEntrySetPayloadIndex(&entries[1], 2)

	putAppendOnlyEntries(entries)

	for i := range entries {
		if appendOnlyEntryKeyIndex(&entries[i]) != 0 || appendOnlyEntryPayloadIndex(&entries[i]) != 0 {
			t.Fatalf("entry %d still retains references after put: %+v", i, entries[i])
		}
	}
}

func TestPutAppendOnlyKeysClearsReferences(t *testing.T) {
	keys := []string{
		appendOnlyStringFromBytes([]byte("k0")),
		appendOnlyStringFromBytes([]byte("k1")),
	}

	putAppendOnlyKeys(keys)

	for i := range keys {
		if keys[i] != "" {
			t.Fatalf("key %d still retains references after put: %q", i, keys[i])
		}
	}
}

func TestPutAppendOnlyValuesClearsReferences(t *testing.T) {
	values := []string{
		appendOnlyStringFromBytes([]byte("v0")),
		appendOnlyStringFromBytes([]byte("v1")),
	}

	putAppendOnlyValues(values)

	for i := range values {
		if values[i] != "" {
			t.Fatalf("value %d still retains references after put: %q", i, values[i])
		}
	}
}

func TestPutAppendOnlyPtrPayloadsClearsReferences(t *testing.T) {
	payloads := make([]appendOnlyPointerPayload, 2)
	payloads[0].value = appendOnlyStringFromBytes([]byte("v0"))
	payloads[0].ptr = page.ValuePtr{Offset: 1, Length: 2, FileID: 3}
	payloads[1].value = appendOnlyStringFromBytes([]byte("v1"))
	payloads[1].ptr = page.ValuePtr{Offset: 4, Length: 5, FileID: 6}

	putAppendOnlyPtrPayloads(payloads)

	for i := range payloads {
		if payloads[i].value != "" || payloads[i].ptr != (page.ValuePtr{}) {
			t.Fatalf("ptr payload %d still retains references after put: %+v", i, payloads[i])
		}
	}
}

func TestGetAppendOnlyKeysFromPoolClearsReturnedSlice(t *testing.T) {
	var pool sync.Pool
	pool.New = func() any {
		return []string{appendOnlyStringFromBytes([]byte("stale"))}
	}

	keys := getAppendOnlyKeysFromPool(1, &pool, 1)
	if got := keys[0]; got != "" {
		t.Fatalf("pooled key was not cleared: %q", got)
	}
}

func TestGetAppendOnlyValuesFromPoolClearsReturnedSlice(t *testing.T) {
	var pool sync.Pool
	pool.New = func() any {
		return []string{appendOnlyStringFromBytes([]byte("stale"))}
	}

	values := getAppendOnlyValuesFromPool(1, &pool, 1)
	if got := values[0]; got != "" {
		t.Fatalf("pooled value was not cleared: %q", got)
	}
}

func TestGetAppendOnlyPtrPayloadsFromPoolClearsReturnedSlice(t *testing.T) {
	var pool sync.Pool
	pool.New = func() any {
		return []appendOnlyPointerPayload{{
			value: appendOnlyStringFromBytes([]byte("stale")),
			ptr:   page.ValuePtr{Offset: 1, Length: 2, FileID: 3},
		}}
	}

	payloads := getAppendOnlyPtrPayloadsFromPool(1, &pool, 1)
	if got := payloads[0]; got.value != "" || got.ptr != (page.ValuePtr{}) {
		t.Fatalf("pooled ptr payload was not cleared: %+v", got)
	}
}

func TestGetAppendOnlyKeysFromPoolRespectsMaxCap(t *testing.T) {
	var pool sync.Pool
	gets := 0
	pool.New = func() any {
		gets++
		return make([]string, 0, 4)
	}

	keys := getAppendOnlyKeysFromPool(2, &pool, 1)
	if gets != 0 {
		t.Fatalf("pool used despite max cap, gets=%d", gets)
	}
	if len(keys) != 2 {
		t.Fatalf("len(keys)=%d want=2", len(keys))
	}
}

func TestGetAppendOnlyKeysFromPoolRejectsOversizedReuse(t *testing.T) {
	var pool sync.Pool
	pool.New = func() any {
		return make([]string, 0, appendOnlyMaxReuseEntries(1)+1)
	}

	keys := getAppendOnlyKeysFromPool(1, &pool, appendOnlyKeyPoolMaxCap)
	if cap(keys) > appendOnlyMaxReuseEntries(1) {
		t.Fatalf("cap(keys)=%d want <=%d", cap(keys), appendOnlyMaxReuseEntries(1))
	}
}

func TestGetAppendOnlyValuesFromPoolRespectsMaxCap(t *testing.T) {
	var pool sync.Pool
	gets := 0
	pool.New = func() any {
		gets++
		return make([]string, 0, 4)
	}

	values := getAppendOnlyValuesFromPool(2, &pool, 1)
	if gets != 0 {
		t.Fatalf("pool used despite max cap, gets=%d", gets)
	}
	if len(values) != 2 {
		t.Fatalf("len(values)=%d want=2", len(values))
	}
}

func TestGetAppendOnlyValuesFromPoolRejectsOversizedReuse(t *testing.T) {
	var pool sync.Pool
	pool.New = func() any {
		return make([]string, 0, appendOnlyMaxReuseEntries(1)+1)
	}

	values := getAppendOnlyValuesFromPool(1, &pool, appendOnlyValuePoolMaxCap)
	if cap(values) > appendOnlyMaxReuseEntries(1) {
		t.Fatalf("cap(values)=%d want <=%d", cap(values), appendOnlyMaxReuseEntries(1))
	}
}

func TestGetAppendOnlyPtrPayloadsFromPoolRespectsMaxCap(t *testing.T) {
	var pool sync.Pool
	gets := 0
	pool.New = func() any {
		gets++
		return make([]appendOnlyPointerPayload, 0, 4)
	}

	payloads := getAppendOnlyPtrPayloadsFromPool(2, &pool, 1)
	if gets != 0 {
		t.Fatalf("pool used despite max cap, gets=%d", gets)
	}
	if len(payloads) != 2 {
		t.Fatalf("len(ptrPayloads)=%d want=2", len(payloads))
	}
}

func TestGetAppendOnlyPtrPayloadsFromPoolRejectsOversizedReuse(t *testing.T) {
	var pool sync.Pool
	pool.New = func() any {
		return make([]appendOnlyPointerPayload, 0, appendOnlyMaxReuseEntries(1)+1)
	}

	payloads := getAppendOnlyPtrPayloadsFromPool(1, &pool, appendOnlyPtrPayloadPoolMaxCap)
	if cap(payloads) > appendOnlyMaxReuseEntries(1) {
		t.Fatalf("cap(payloads)=%d want <=%d", cap(payloads), appendOnlyMaxReuseEntries(1))
	}
}

func TestAppendOnlyIteratorCloseClearsPooledEntries(t *testing.T) {
	entries := make([]appendOnlyEntry, 2)
	appendOnlyEntrySetKeyIndex(&entries[0], 1)
	appendOnlyEntrySetPayloadIndex(&entries[0], 1)
	appendOnlyEntrySetKeyIndex(&entries[1], 2)
	appendOnlyEntrySetPayloadIndex(&entries[1], 2)
	keys := []string{
		appendOnlyStringFromBytes([]byte("k0")),
		appendOnlyStringFromBytes([]byte("k1")),
	}
	values := []string{
		appendOnlyStringFromBytes([]byte("v0")),
		appendOnlyStringFromBytes([]byte("v1")),
	}
	ptrPayloads := []appendOnlyPointerPayload{
		{value: appendOnlyStringFromBytes([]byte("pv0")), ptr: page.ValuePtr{Offset: 7, Length: 8, FileID: 9}},
	}

	it := &appendOnlyIterator{
		entries:       entries,
		keys:          keys,
		values:        values,
		ptrPayloads:   ptrPayloads,
		pooledEntries: true,
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	for i := range entries {
		if appendOnlyEntryKeyIndex(&entries[i]) != 0 || appendOnlyEntryPayloadIndex(&entries[i]) != 0 {
			t.Fatalf("entry %d still retains references after close: %+v", i, entries[i])
		}
	}
	for i := range keys {
		if keys[i] != "" {
			t.Fatalf("key %d still retains references after close: %q", i, keys[i])
		}
	}
	for i := range values {
		if values[i] != "" {
			t.Fatalf("value %d still retains references after close: %q", i, values[i])
		}
	}
	for i := range ptrPayloads {
		if ptrPayloads[i].value != "" || ptrPayloads[i].ptr != (page.ValuePtr{}) {
			t.Fatalf("ptr payload %d still retains references after close: %+v", i, ptrPayloads[i])
		}
	}
	if it.entries != nil {
		t.Fatalf("iterator entries not cleared on close")
	}
	if it.keys != nil {
		t.Fatalf("iterator keys not cleared on close")
	}
	if it.values != nil {
		t.Fatalf("iterator values not cleared on close")
	}
	if it.ptrPayloads != nil {
		t.Fatalf("iterator ptr payloads not cleared on close")
	}
	if it.pooledEntries {
		t.Fatalf("pooledEntries flag not cleared on close")
	}
}

func TestAppendOnlyIteratorCloseClearsPooledPointerEntries(t *testing.T) {
	entry0 := &appendOnlyEntry{}
	appendOnlyEntrySetKeyIndex(entry0, 1)
	appendOnlyEntrySetPayloadIndex(entry0, 1)
	appendOnlyEntrySetFlags(entry0, node.FlagPointer)
	entry1 := &appendOnlyEntry{}
	appendOnlyEntrySetKeyIndex(entry1, 2)
	appendOnlyEntrySetPayloadIndex(entry1, 2)
	appendOnlyEntrySetFlags(entry1, node.FlagTombstone)
	entries := []*appendOnlyEntry{
		entry0,
		entry1,
	}
	it := &appendOnlyIterator{
		entryPtrs:       entries,
		pooledEntryPtrs: true,
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	for i := range entries {
		if entries[i] != nil {
			t.Fatalf("entry ptr %d not cleared on close", i)
		}
	}
	if it.entryPtrs != nil {
		t.Fatalf("iterator pointer entries not cleared on close")
	}
	if it.pooledEntryPtrs {
		t.Fatalf("pooledEntryPtrs flag not cleared on close")
	}
}

func TestGetAppendOnlyEntriesSkipsOversizedPooledSliceForEmptyRequest(t *testing.T) {
	var pool sync.Pool
	oversized := make([]appendOnlyEntry, 0, appendOnlyMaxReuseEntries(0)+1)
	gets := 0
	pool.New = func() any {
		gets++
		return oversized
	}

	got := getAppendOnlyEntriesFromPool(0, &pool)
	if gets != 1 {
		t.Fatalf("expected pool.New to supply one oversized slice, got %d calls", gets)
	}
	if got == nil {
		t.Fatalf("getAppendOnlyEntries(0) returned nil slice")
	}
	if cap(got) > appendOnlyMaxReuseEntries(0) {
		t.Fatalf("cap=%d want <= %d", cap(got), appendOnlyMaxReuseEntries(0))
	}
}

func TestAppendOnlyUnorderedIteratorUsesPooledEntries(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered iterator path

	rawIt := m.NewIterator(nil, nil)
	it, ok := rawIt.(*appendOnlyIterator)
	if !ok {
		t.Fatalf("unexpected iterator type %T", rawIt)
	}
	if !it.pooledEntries {
		t.Fatalf("unordered iterator should use pooled entry copy buffer")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestAppendOnlyFrozenUnorderedIteratorUsesPointerSnapshot(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered iterator path
	m.Freeze()

	rawIt := m.NewIterator(nil, nil)
	it, ok := rawIt.(*appendOnlyIterator)
	if !ok {
		t.Fatalf("unexpected iterator type %T", rawIt)
	}
	if it.pooledEntries {
		t.Fatalf("frozen unordered iterator should avoid entry-copy pool")
	}
	if it.pooledEntryPtrs {
		t.Fatalf("frozen unordered iterator should avoid per-iterator pointer snapshot copies")
	}
	if it.entryPtrs == nil || len(it.entryPtrs) != 2 {
		t.Fatalf("unexpected pointer snapshot len=%d", len(it.entryPtrs))
	}
	if !it.leaseHeld || it.leaseOwner != m {
		t.Fatalf("frozen unordered iterator should hold memtable lease until close")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestAppendOnlyFrozenUnorderedIteratorBlocksResetUntilClose(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered iterator path
	m.Freeze()

	rawIt := m.NewIterator(nil, nil)
	it, ok := rawIt.(*appendOnlyIterator)
	if !ok {
		t.Fatalf("unexpected iterator type %T", rawIt)
	}
	if !it.leaseHeld || it.leaseOwner != m {
		t.Fatalf("expected frozen iterator to hold a memtable lease")
	}

	resetDone := make(chan struct{})
	go func() {
		m.Reset()
		close(resetDone)
	}()

	select {
	case <-resetDone:
		_ = it.Close()
		t.Fatalf("Reset completed while frozen iterator was still open")
	case <-time.After(200 * time.Millisecond):
	}

	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close() second: %v", err)
	}

	select {
	case <-resetDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Reset did not proceed after iterator close")
	}
}

func TestAppendOnlyEntryStructSizeBound(t *testing.T) {
	if got := unsafe.Sizeof(appendOnlyEntry{}); got > 16 {
		t.Fatalf("appendOnlyEntry size=%d want <= 16", got)
	}
}

func TestAppendOnlySetCopiesNonInlineKeyIntoArena(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key1 := []byte("long-key-01")
	lookupKey := cloneBytes(key1)
	val1 := []byte("value-aaaaaa")
	m.Set(key1, val1)
	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	storedKey := m.appendOnlyEntryKey(&m.entries[0])
	if string(storedKey) != string(lookupKey) {
		t.Fatalf("stored key=%q want=%q", storedKey, lookupKey)
	}
	if unsafe.SliceData(storedKey) == unsafe.SliceData(key1) {
		t.Fatalf("stored key unexpectedly aliases caller buffer")
	}
	key1[0] = 'X'

	got, tombstone, ok := m.Get(lookupKey)
	if !ok || tombstone {
		t.Fatalf("Get(%q) = ok=%v tombstone=%v; want ok=true tombstone=false", lookupKey, ok, tombstone)
	}
	if string(got) != string(val1) {
		t.Fatalf("stored value changed after source key mutation: got=%q want=%q", got, val1)
	}
}

func TestAppendOnlySetInlinesShortKey(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("short")
	lookupKey := cloneBytes(key)
	m.Set(key, []byte("v"))

	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	ent := &m.entries[0]
	if appendOnlyEntryInlineKeyLen(ent) == 0 {
		t.Fatalf("expected short key to be stored inline")
	}
	if got := appendOnlyEntryInlineKeyLen(ent); got != len(lookupKey) {
		t.Fatalf("inline key len=%d want=%d", got, len(lookupKey))
	}
	if got := appendOnlyEntryKeyIndex(ent); got != 0 {
		t.Fatalf("short inline key should not allocate a key slot; keyIndex=%d", got)
	}
	storedKey := m.appendOnlyEntryKey(ent)
	if string(storedKey) != string(lookupKey) {
		t.Fatalf("stored key=%q want=%q", storedKey, lookupKey)
	}
	key[0] = 'X'
	if string(m.appendOnlyEntryKey(ent)) != string(lookupKey) {
		t.Fatalf("inline key changed after caller mutation: got=%q want=%q", m.appendOnlyEntryKey(ent), lookupKey)
	}
}

func TestAppendOnlySetCopiesIntoArenaForNonSteal(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("long-key-arena")
	src := []byte("value-arena-copy")
	m.Set(key, src)

	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	ent := &m.entries[0]
	val := m.appendOnlyEntryValue(ent)
	if len(val) != len(src) {
		t.Fatalf("entry value len=%d want=%d", len(val), len(src))
	}
	if &val[0] == &src[0] {
		t.Fatalf("entry value unexpectedly aliases caller buffer")
	}
	src[0] = 'X'

	got, tombstone, ok := m.Get(key)
	if !ok || tombstone {
		t.Fatalf("Get(%q) = ok=%v tombstone=%v; want ok=true tombstone=false", key, ok, tombstone)
	}
	if string(got) != "value-arena-copy" {
		t.Fatalf("stored value changed via source mutation: got=%q", got)
	}
}

func TestAppendOnlyResetClearsValueArenaState(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("k")
	val := make([]byte, 2048)
	for i := 0; i < 8; i++ {
		key[0] = byte('a' + i)
		val[0] = byte(i)
		m.Set(key, val)
	}
	if len(m.valueArena.chunks) == 0 {
		t.Fatalf("expected value arena chunks to be populated")
	}

	m.Reset()
	if len(m.valueArena.chunks) != 0 {
		t.Fatalf("expected value arena chunks to be released on reset; got=%d", len(m.valueArena.chunks))
	}
	if m.valueArena.cur != nil {
		t.Fatalf("expected value arena current chunk to be cleared")
	}
	if m.valueArena.curPos != 0 {
		t.Fatalf("expected value arena position reset; got=%d", m.valueArena.curPos)
	}
}

func TestAppendOnlyResetRetainsArenaChunksForReuse(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("k")
	val := make([]byte, 2048)
	for i := 0; i < 64; i++ {
		key[0] = byte('a' + byte(i%26))
		val[0] = byte(i)
		m.Set(key, val)
	}
	if len(m.valueArena.chunks) == 0 {
		t.Fatalf("expected populated arena chunks before reset")
	}
	type chunkSpan struct {
		start uintptr
		end   uintptr
	}
	seen := make([]chunkSpan, 0, len(m.valueArena.chunks))
	for _, chunk := range m.valueArena.chunks {
		full := chunk[:cap(chunk)]
		start := uintptr(unsafe.Pointer(&full[0]))
		seen = append(seen, chunkSpan{
			start: start,
			end:   start + uintptr(len(full)),
		})
	}
	inRetainedChunk := func(ptr uintptr) bool {
		for _, span := range seen {
			if ptr >= span.start && ptr < span.end {
				return true
			}
		}
		return false
	}

	m.Reset()
	if len(m.valueArena.retained) == 0 {
		t.Fatalf("expected retained arena chunks after reset")
	}
	if m.valueArena.retainedB <= 0 {
		t.Fatalf("expected retained arena bytes > 0")
	}

	postResetKey := []byte("post-reset-long-key")
	m.Set(postResetKey, make([]byte, 1024))
	if m.count != 1 || len(m.appendOnlyEntryValue(&m.entries[0])) == 0 {
		t.Fatalf("expected first post-reset set to allocate value from arena")
	}
	postResetKeyBuf := m.appendOnlyEntryKey(&m.entries[0])
	postResetValue := m.appendOnlyEntryValue(&m.entries[0])
	if len(postResetKeyBuf) == 0 {
		t.Fatalf("expected non-inline key to be retained after reset")
	}
	keyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(postResetKeyBuf)))
	ptr := uintptr(unsafe.Pointer(&postResetValue[0]))
	if !inRetainedChunk(keyPtr) {
		t.Fatalf("post-reset key buffer did not reuse retained arena chunk")
	}
	if !inRetainedChunk(ptr) {
		t.Fatalf("post-reset value buffer did not reuse retained arena chunk")
	}
}

func TestAppendOnlyResetRetainedArenaBounded(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("k")
	val := make([]byte, 2048)
	for i := 0; i < 5000; i++ {
		key[0] = byte('a' + byte(i%26))
		val[0] = byte(i)
		m.Set(key, val)
	}
	m.Reset()
	if len(m.valueArena.retained) > appendOnlyValueArenaRetainChunks {
		t.Fatalf("retained chunk count=%d exceeds bound=%d", len(m.valueArena.retained), appendOnlyValueArenaRetainChunks)
	}
	if m.valueArena.retainedB > appendOnlyValueArenaRetainMaxCap {
		t.Fatalf("retained bytes=%d exceed bound=%d", m.valueArena.retainedB, appendOnlyValueArenaRetainMaxCap)
	}
}

func TestAppendOnlyValueArenaChunksGrowBounded(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("k")
	val := make([]byte, 2048)
	for i := 0; i < 128; i++ {
		key[0] = byte('a' + byte(i%26))
		val[0] = byte(i)
		m.Set(key, val)
	}
	if len(m.valueArena.chunks) < 2 {
		t.Fatalf("expected multiple arena chunks, got=%d", len(m.valueArena.chunks))
	}
	prevCap := 0
	for i, chunk := range m.valueArena.chunks {
		c := cap(chunk)
		if c < appendOnlyValueArenaDefaultChunk {
			t.Fatalf("chunk %d cap=%d want >= %d", i, c, appendOnlyValueArenaDefaultChunk)
		}
		if c > appendOnlyValueArenaGrowthMaxChunk {
			t.Fatalf("chunk %d cap=%d want <= %d", i, c, appendOnlyValueArenaGrowthMaxChunk)
		}
		if prevCap > c {
			t.Fatalf("chunk %d cap=%d regressed from %d", i, c, prevCap)
		}
		prevCap = c
	}
}

func TestAppendOnlyUnorderedAppendBuildsLatestIndexLazily(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("b"), []byte("v1"))
	m.Set([]byte("a"), []byte("v2")) // force unordered path
	if m.ordered {
		t.Fatalf("expected unordered memtable")
	}
	if !m.latestDirty {
		t.Fatalf("expected latest index to be dirty after order break")
	}

	it := m.NewIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected iterator snapshot build to refresh latest index")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered iterator to avoid caching snapshot; got snapCount=%d", m.snapCount)
	}
	if m.snapshot != nil {
		t.Fatalf("expected mutable unordered iterator to leave shared snapshot nil")
	}

	m.Set([]byte("b"), []byte("v3"))
	if m.latestDirty {
		t.Fatalf("expected unordered append to keep latest index clean after iterator rebuild")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected snapshot invalidation after append; got snapCount=%d", m.snapCount)
	}

	got, tombstone, found := m.Get([]byte("b"))
	if !found || tombstone {
		t.Fatalf("Get(b) = found=%v tombstone=%v; want found=true tombstone=false", found, tombstone)
	}
	if string(got) != "v3" {
		t.Fatalf("Get(b)=%q want=v3", got)
	}

	it = m.NewIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close after append: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected latest index rebuild on second iterator snapshot")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered iterator to keep shared snapshot uncached after rebuild; got snapCount=%d", m.snapCount)
	}
	inlineKey, ok := appendOnlyInlineMapKeyFromBytes([]byte("b"))
	if !ok {
		t.Fatalf("test key did not fit inline latest index")
	}
	idx, ok := m.latestInline[inlineKey]
	if !ok {
		t.Fatalf("missing latest index entry for key b")
	}
	if idx != 2 {
		t.Fatalf("latest index for key b=%d want=2", idx)
	}
}

func TestAppendOnlyUnorderedReverseIteratorDoesNotCacheSharedSnapshot(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("b"), []byte("v1"))
	m.Set([]byte("a"), []byte("v2")) // force unordered path
	if m.ordered {
		t.Fatalf("expected unordered memtable")
	}
	if !m.latestDirty {
		t.Fatalf("expected latest index to be dirty after order break")
	}

	it := m.NewReverseIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("reverse iterator close: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected reverse iterator snapshot build to refresh latest index")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered reverse iterator to avoid caching snapshot; got snapCount=%d", m.snapCount)
	}
	if m.snapshot != nil {
		t.Fatalf("expected mutable unordered reverse iterator to leave shared snapshot nil")
	}

	m.Set([]byte("b"), []byte("v3"))
	if m.latestDirty {
		t.Fatalf("expected unordered append to keep latest index clean after reverse iterator rebuild")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected snapshot invalidation after append; got snapCount=%d", m.snapCount)
	}

	it = m.NewReverseIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("reverse iterator close after append: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected latest index rebuild on second reverse iterator snapshot")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered reverse iterator to keep shared snapshot uncached after rebuild; got snapCount=%d", m.snapCount)
	}
	if m.snapshot != nil {
		t.Fatalf("expected mutable unordered reverse iterator to keep shared snapshot nil after rebuild")
	}
}

func TestAppendOnlyResetDoesNotPoolStolenValueSlices(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	external := []byte("external-immutable")
	m.SetEntrySteal([]byte("k-steal"), external, page.ValuePtr{}, node.FlagInline)
	m.Reset()

	newVal := []byte("replacement-value")
	m.Set([]byte("k-new"), newVal)
	if string(external) != "external-immutable" {
		t.Fatalf("stolen caller value was mutated via pooled reuse: got %q", external)
	}
}

func TestAppendOnlyResetDoesNotPoolBorrowedValueSlices(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	external := []byte("borrowed-immutable")
	m.SetEntryBorrowValue([]byte("k-borrow"), external, page.ValuePtr{}, node.FlagInline)
	m.Reset()

	newVal := []byte("replacement-value")
	m.Set([]byte("k-new"), newVal)
	if string(external) != "borrowed-immutable" {
		t.Fatalf("borrowed caller value was mutated via pooled reuse: got %q", external)
	}
}

func TestAppendOnlyResetKeepsSnapshotBuffersWarm(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	// Force unordered mode and a non-empty latest snapshot/index buffer.
	for i := 9; i >= 0; i-- {
		m.Set([]byte{byte('a' + i)}, []byte("v"))
	}
	m.Freeze()
	it := m.NewIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	if cap(m.snapshot) == 0 {
		t.Fatalf("expected snapshot capacity to be initialized")
	}
	snapshotCap := cap(m.snapshot)
	indexCap := cap(m.indexBuf)

	m.Reset()
	if len(m.snapshot) != 0 {
		t.Fatalf("snapshot len after reset=%d want=0", len(m.snapshot))
	}
	if len(m.indexBuf) != 0 {
		t.Fatalf("index buf len after reset=%d want=0", len(m.indexBuf))
	}
	if cap(m.snapshot) < snapshotCap {
		t.Fatalf("snapshot cap shrank after reset: got=%d want>=%d", cap(m.snapshot), snapshotCap)
	}
	if cap(m.indexBuf) < indexCap {
		t.Fatalf("index cap shrank after reset: got=%d want>=%d", cap(m.indexBuf), indexCap)
	}
}

func TestBuildSortedLatestIndicesLockedRetainsGrownIndexBuf(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered latest-index path
	m.indexBuf = make([]int, 0, 1)

	m.mu.Lock()
	defer m.mu.Unlock()

	indices := m.buildSortedLatestIndicesLocked()
	if len(indices) != 2 {
		t.Fatalf("indices len=%d want=2", len(indices))
	}
	if cap(m.indexBuf) < len(indices) {
		t.Fatalf("indexBuf cap=%d want >= %d", cap(m.indexBuf), len(indices))
	}
	if cap(m.indexBuf) == 0 {
		t.Fatal("indexBuf cap=0 want retained grown buffer")
	}
	retained := m.indexBuf[:len(indices)]
	if &retained[0] != &indices[0] {
		t.Fatal("indexBuf does not retain grown indices backing array")
	}
}
