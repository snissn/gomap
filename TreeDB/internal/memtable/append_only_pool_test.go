package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPutAppendOnlyEntriesClearsReferences(t *testing.T) {
	entries := make([]appendOnlyEntry, 2)
	entries[0].key = []byte("k0")
	entries[0].value = []byte("v0")
	entries[1].key = []byte("k1")
	entries[1].value = []byte("v1")

	putAppendOnlyEntries(entries)

	for i := range entries {
		if entries[i].key != nil || entries[i].value != nil {
			t.Fatalf("entry %d still retains references after put: %+v", i, entries[i])
		}
	}
}

func TestAppendOnlyEntryPoolClassForLength(t *testing.T) {
	tests := []struct {
		name     string
		length   int
		wantCap  int
		wantPool bool
	}{
		{name: "negative", length: -1, wantCap: appendOnlyMinInitialEntries, wantPool: true},
		{name: "zero", length: 0, wantCap: appendOnlyMinInitialEntries, wantPool: true},
		{name: "minimum", length: appendOnlyMinInitialEntries, wantCap: appendOnlyMinInitialEntries, wantPool: true},
		{name: "round-up", length: appendOnlyMinInitialEntries + 1, wantCap: appendOnlyMinInitialEntries * 2, wantPool: true},
		{name: "bench-sized", length: 16000, wantCap: 1 << 14, wantPool: true},
		{name: "retention-maximum", length: appendOnlyEntryPoolRetainMaxCap, wantCap: appendOnlyEntryPoolRetainMaxCap, wantPool: true},
		{name: "above-retention-maximum", length: appendOnlyEntryPoolRetainMaxCap + 1, wantPool: false},
		{name: "active-maximum-not-retained", length: appendOnlyEntryPoolMaxCap, wantPool: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, gotCap, gotPool := appendOnlyEntryPoolClassForLength(tc.length)
			if gotPool != tc.wantPool {
				t.Fatalf("pooled=%v want %v", gotPool, tc.wantPool)
			}
			if gotPool && gotCap != tc.wantCap {
				t.Fatalf("cap=%d want %d", gotCap, tc.wantCap)
			}
		})
	}
}

func TestAppendOnlyEntryPoolClassForReusableCapacity(t *testing.T) {
	if _, ok := appendOnlyEntryPoolClassForReusableCapacity(appendOnlyMinInitialEntries - 1); ok {
		t.Fatalf("sub-minimum capacity should not be pooled")
	}
	if _, ok := appendOnlyEntryPoolClassForReusableCapacity(16000); !ok {
		t.Fatalf("bench-sized capacity should be pooled")
	}
	if _, ok := appendOnlyEntryPoolClassForReusableCapacity(appendOnlyEntryPoolRetainMaxCap); !ok {
		t.Fatalf("retention maximum capacity should be pooled")
	}
	if _, ok := appendOnlyEntryPoolClassForReusableCapacity(appendOnlyEntryPoolRetainMaxCap + 1); ok {
		t.Fatalf("above-retention capacity should not be pooled")
	}
	if _, ok := appendOnlyEntryPoolClassForReusableCapacity(appendOnlyEntryPoolMaxCap + 1); ok {
		t.Fatalf("oversized capacity should not be pooled")
	}
}

func TestDropAppendOnlyEntryPoolsReplacesPools(t *testing.T) {
	class, _, ok := appendOnlyEntryPoolClassForLength(1024)
	if !ok {
		t.Fatal("test setup failed to map entry length to pool class")
	}
	before := appendOnlyEntryPoolForClass(class)
	if before == nil {
		t.Fatal("expected initial append-only entry pool")
	}
	beforeDrop := AppendOnlyEntryPoolDropTotal()

	DropAppendOnlyEntryPools()

	after := appendOnlyEntryPoolForClass(class)
	if after == nil {
		t.Fatal("expected replacement append-only entry pool")
	}
	if before == after {
		t.Fatal("drop did not replace append-only entry pool")
	}
	if got := AppendOnlyEntryPoolDropTotal(); got != beforeDrop+1 {
		t.Fatalf("entry pool drop total=%d want %d", got, beforeDrop+1)
	}
}

func TestAppendOnlyEntryPoolStatsTrackRetainedBacking(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	before := AppendOnlyEntryPoolStatsSnapshot()
	entries := make([]appendOnlyEntry, 0, appendOnlyMinInitialEntries)
	wantBytes := appendOnlyEntryPoolBytes(cap(entries))

	putAppendOnlyEntries(entries)
	afterPut := AppendOnlyEntryPoolStatsSnapshot()
	if got := afterPut.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes after put=%d want %d", got, wantBytes)
	}
	if got := afterPut.PutsTotal; got != before.PutsTotal+1 {
		t.Fatalf("puts total=%d want %d", got, before.PutsTotal+1)
	}

	got := getAppendOnlyEntries(appendOnlyMinInitialEntries)
	afterGet := AppendOnlyEntryPoolStatsSnapshot()
	if cap(got) != cap(entries) {
		t.Fatalf("pooled cap=%d want %d", cap(got), cap(entries))
	}
	switch gotGets := afterGet.GetsTotal; gotGets {
	case before.GetsTotal + 1:
		if gotRetained := afterGet.RetainedBytesEstimate; gotRetained != 0 {
			t.Fatalf("retained bytes after pooled get=%d want 0", gotRetained)
		}
	case before.GetsTotal:
		if gotRetained := afterGet.RetainedBytesEstimate; gotRetained != wantBytes {
			t.Fatalf("retained bytes after sync.Pool miss=%d want estimate %d", gotRetained, wantBytes)
		}
	default:
		t.Fatalf("gets total=%d want %d or %d", gotGets, before.GetsTotal, before.GetsTotal+1)
	}

	putAppendOnlyEntries(got)
	beforeDrop := AppendOnlyEntryPoolStatsSnapshot()
	DropAppendOnlyEntryPools()
	afterDrop := AppendOnlyEntryPoolStatsSnapshot()
	if gotRetained := afterDrop.RetainedBytesEstimate; gotRetained != 0 {
		t.Fatalf("retained bytes after drop=%d want 0", gotRetained)
	}
	if gotDrops := afterDrop.DropsTotal; gotDrops != beforeDrop.DropsTotal+1 {
		t.Fatalf("drops total=%d want %d", gotDrops, beforeDrop.DropsTotal+1)
	}
	if gotDropBytes := afterDrop.DropBytesTotal; gotDropBytes < beforeDrop.DropBytesTotal+beforeDrop.RetainedBytesEstimate {
		t.Fatalf("drop bytes total=%d want at least %d", gotDropBytes, beforeDrop.DropBytesTotal+beforeDrop.RetainedBytesEstimate)
	}
}

func TestGetAppendOnlyEntriesKeepsSmallerSameClassBacking(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	smallLen := appendOnlyMinInitialEntries + 1
	largeLen := appendOnlyMinInitialEntries * 2
	smallClass, _, ok := appendOnlyEntryPoolClassForLength(smallLen)
	if !ok {
		t.Fatalf("small length %d should map to a retained pool class", smallLen)
	}
	largeClass, _, ok := appendOnlyEntryPoolClassForLength(largeLen)
	if !ok {
		t.Fatalf("large length %d should map to a retained pool class", largeLen)
	}
	if smallClass != largeClass {
		t.Fatalf("test setup needs one pool class: small=%d large=%d", smallClass, largeClass)
	}

	large := make([]appendOnlyEntry, 0, largeLen)
	small := make([]appendOnlyEntry, 0, smallLen)
	putAppendOnlyEntries(large)
	putAppendOnlyEntries(small)

	gotLarge := getAppendOnlyEntries(largeLen)
	if cap(gotLarge) != largeLen {
		t.Fatalf("large cap=%d want %d", cap(gotLarge), largeLen)
	}
	if gotRetained, want := AppendOnlyEntryPoolStatsSnapshot().RetainedBytesEstimate, appendOnlyEntryPoolBytes(smallLen); gotRetained != want {
		t.Fatalf("retained bytes after large get=%d want smaller backing retained %d", gotRetained, want)
	}

	gotSmall := getAppendOnlyEntries(smallLen)
	if cap(gotSmall) != smallLen {
		t.Fatalf("small cap=%d want %d", cap(gotSmall), smallLen)
	}
	if gotRetained := AppendOnlyEntryPoolStatsSnapshot().RetainedBytesEstimate; gotRetained != 0 {
		t.Fatalf("retained bytes after small get=%d want 0", gotRetained)
	}
	putAppendOnlyEntries(gotLarge[:0])
	putAppendOnlyEntries(gotSmall[:0])
}

func TestAppendOnlyEntryPoolRetainsBackingAcrossGC(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	entries := make([]appendOnlyEntry, 0, appendOnlyMinInitialEntries)
	wantBytes := appendOnlyEntryPoolBytes(cap(entries))
	putAppendOnlyEntries(entries)

	runtime.GC()

	afterGC := AppendOnlyEntryPoolStatsSnapshot()
	if got := afterGC.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes after GC=%d want %d", got, wantBytes)
	}

	reused := getAppendOnlyEntries(appendOnlyMinInitialEntries)
	if cap(reused) != cap(entries) {
		t.Fatalf("reused cap after GC=%d want %d", cap(reused), cap(entries))
	}
	afterGet := AppendOnlyEntryPoolStatsSnapshot()
	if got := afterGet.RetainedBytesEstimate; got != 0 {
		t.Fatalf("retained bytes after get=%d want 0", got)
	}
	putAppendOnlyEntries(reused[:0])
}

func TestPutAppendOnlyEntriesDropsOversizedRetainedSlice(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	before := AppendOnlyEntryPoolStatsSnapshot()
	entries := make([]appendOnlyEntry, 0, appendOnlyEntryPoolRetainMaxCap+1)
	wantBytes := appendOnlyEntryPoolBytes(cap(entries))

	putAppendOnlyEntries(entries)

	after := AppendOnlyEntryPoolStatsSnapshot()
	if got := after.RetainedBytesEstimate; got != 0 {
		t.Fatalf("retained bytes after oversized put=%d want 0", got)
	}
	if got := after.PutsTotal - before.PutsTotal; got != 0 {
		t.Fatalf("puts delta=%d want 0", got)
	}
	if got := after.AdmissionDropsTotal - before.AdmissionDropsTotal; got != 1 {
		t.Fatalf("admission drops delta=%d want 1", got)
	}
	if got := after.AdmissionDropBytesTotal - before.AdmissionDropBytesTotal; got != wantBytes {
		t.Fatalf("admission drop bytes delta=%d want %d", got, wantBytes)
	}
}

func TestAppendOnlyEntryPoolRetainsLargeBatchHintClass(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	const entries = 1 << 17
	_, classCap, ok := appendOnlyEntryPoolClassForLength(entries)
	if !ok {
		t.Fatalf("entry count %d should map to a retained pool class", entries)
	}
	if classCap != entries {
		t.Fatalf("class cap=%d want %d", classCap, entries)
	}

	before := AppendOnlyEntryPoolStatsSnapshot()
	putAppendOnlyEntries(make([]appendOnlyEntry, 0, entries))
	afterPut := AppendOnlyEntryPoolStatsSnapshot()
	if got := afterPut.AdmissionDropsTotal - before.AdmissionDropsTotal; got != 0 {
		t.Fatalf("admission drops delta=%d want 0", got)
	}
	if got := afterPut.PutsTotal - before.PutsTotal; got != 1 {
		t.Fatalf("puts delta=%d want 1", got)
	}

	reused := getAppendOnlyEntries(entries)
	if cap(reused) != entries {
		t.Fatalf("reused cap=%d want %d", cap(reused), entries)
	}
	putAppendOnlyEntries(reused[:0])
}

func TestPutAppendOnlyEntriesRecyclesPoolsOnRetainBudgetPressure(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	savedBudget := appendOnlyEntryPoolRetainBudgetBytes
	appendOnlyEntryPoolRetainBudgetBytes = appendOnlyEntryPoolBytes(appendOnlyMinInitialEntries)
	t.Cleanup(func() {
		appendOnlyEntryPoolRetainBudgetBytes = savedBudget
	})

	before := AppendOnlyEntryPoolStatsSnapshot()
	first := make([]appendOnlyEntry, 0, appendOnlyMinInitialEntries)
	second := make([]appendOnlyEntry, 0, appendOnlyMinInitialEntries)
	wantBytes := appendOnlyEntryPoolBytes(appendOnlyMinInitialEntries)

	putAppendOnlyEntries(first)
	putAppendOnlyEntries(second)

	after := AppendOnlyEntryPoolStatsSnapshot()
	if got := after.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes=%d want budget %d", got, wantBytes)
	}
	if got := after.PutsTotal - before.PutsTotal; got != 2 {
		t.Fatalf("puts delta=%d want 2", got)
	}
	if got := after.DropsTotal - before.DropsTotal; got != 1 {
		t.Fatalf("drops delta=%d want 1", got)
	}
	if got := after.DropBytesTotal - before.DropBytesTotal; got < wantBytes {
		t.Fatalf("drop bytes delta=%d want at least %d", got, wantBytes)
	}
	if got := after.AdmissionDropsTotal - before.AdmissionDropsTotal; got != 0 {
		t.Fatalf("admission drops delta=%d want 0", got)
	}
	if got := after.AdmissionDropBytesTotal - before.AdmissionDropBytesTotal; got != 0 {
		t.Fatalf("admission drop bytes delta=%d want 0", got)
	}

	reused := getAppendOnlyEntries(appendOnlyMinInitialEntries)
	afterGet := AppendOnlyEntryPoolStatsSnapshot()
	if cap(reused) != appendOnlyMinInitialEntries {
		t.Fatalf("reused cap=%d want %d", cap(reused), appendOnlyMinInitialEntries)
	}
	switch got := afterGet.GetsTotal - after.GetsTotal; got {
	case 1:
		if got := afterGet.RetainedBytesEstimate; got != 0 {
			t.Fatalf("retained bytes after replacement get=%d want 0", got)
		}
	case 0:
		if !testRaceEnabled {
			t.Fatalf("gets delta after replacement put=%d want 1", got)
		}
		if got := afterGet.RetainedBytesEstimate; got != wantBytes {
			t.Fatalf("retained bytes after sync.Pool race miss=%d want estimate %d", got, wantBytes)
		}
	default:
		t.Fatalf("gets delta after replacement put=%d want 0 or 1", got)
	}
	putAppendOnlyEntries(reused)
}

func TestAppendOnlyReserveAdditionalEntriesAvoidsIntermediateGrowth(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	m := NewAppendOnlyWithEntryCapacity(appendOnlyMinInitialEntries)
	additional := appendOnlyMinInitialEntries * 8
	needed := m.count + additional
	before := AppendOnlyEntryReserveStatsSnapshot()

	m.ReserveAdditionalEntries(additional)

	after := AppendOnlyEntryReserveStatsSnapshot()
	if got := after.CallsTotal - before.CallsTotal; got != 1 {
		t.Fatalf("reserve calls delta=%d want 1", got)
	}
	if got := after.EntriesTotal - before.EntriesTotal; got != uint64(additional) {
		t.Fatalf("reserve entries delta=%d want %d", got, additional)
	}
	if got := after.GrowCallsTotal - before.GrowCallsTotal; got != 1 {
		t.Fatalf("reserve grow calls delta=%d want 1", got)
	}
	if got := after.GrowBytesTotal - before.GrowBytesTotal; got == 0 {
		t.Fatalf("reserve grow bytes did not increase")
	}
	if got := after.SkippedGrowthAllocsTotal - before.SkippedGrowthAllocsTotal; got == 0 {
		t.Fatalf("reserve skipped growth allocs did not increase")
	}
	if got := after.SkippedGrowthBytesTotal - before.SkippedGrowthBytesTotal; got == 0 {
		t.Fatalf("reserve skipped growth bytes did not increase")
	}
	maxHeadroom := needed / appendOnlyReserveHeadroomDivisor
	if maxHeadroom < appendOnlyMinInitialEntries {
		maxHeadroom = appendOnlyMinInitialEntries
	}
	if got, max := cap(m.entries), needed+maxHeadroom; got > max {
		t.Fatalf("reserve cap(entries)=%d want<=%d", got, max)
	}
	if got := cap(m.entries); got < needed {
		t.Fatalf("reserve cap(entries)=%d want>=%d", got, needed)
	}

	reservedLen := len(m.entries)
	reservedCap := cap(m.entries)
	if reservedLen < additional {
		t.Fatalf("reserved len=%d want at least %d", reservedLen, additional)
	}
	for i := 0; i < additional; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagInline)
	}
	if got := len(m.entries); got != reservedLen {
		t.Fatalf("entry slice len changed during reserved appends: got %d want %d", got, reservedLen)
	}
	if got := cap(m.entries); got != reservedCap {
		t.Fatalf("entry slice cap changed during reserved appends: got %d want %d", got, reservedCap)
	}
}

func TestAppendOnlyReserveTargetCapacityBoundsOvershoot(t *testing.T) {
	current := appendOnlyMinInitialEntries
	needed := appendOnlyAggressiveGrowCutoff + 1

	target, skippedAllocs, skippedBytes := appendOnlyReserveTargetCapacity(current, needed)

	if target < needed {
		t.Fatalf("target=%d want >= needed %d", target, needed)
	}
	maxHeadroom := needed / appendOnlyReserveHeadroomDivisor
	if maxHeadroom < appendOnlyMinInitialEntries {
		maxHeadroom = appendOnlyMinInitialEntries
	}
	if max := needed + maxHeadroom; target > max {
		t.Fatalf("target=%d want <= %d", target, max)
	}
	if skippedAllocs == 0 {
		t.Fatalf("skippedAllocs=0 want >0")
	}
	if skippedBytes == 0 {
		t.Fatalf("skippedBytes=0 want >0")
	}
}

func TestAppendOnlyIteratorCloseClearsPooledEntries(t *testing.T) {
	entries := make([]appendOnlyEntry, 2)
	entries[0].key = []byte("k0")
	entries[0].value = []byte("v0")
	entries[1].key = []byte("k1")
	entries[1].value = []byte("v1")

	it := &appendOnlyIterator{
		entries:       entries,
		pooledEntries: true,
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	for i := range entries {
		if entries[i].key != nil || entries[i].value != nil {
			t.Fatalf("entry %d still retains references after close: %+v", i, entries[i])
		}
	}
	if it.entries != nil {
		t.Fatalf("iterator entries not cleared on close")
	}
	if it.pooledEntries {
		t.Fatalf("pooledEntries flag not cleared on close")
	}
}

func TestAppendOnlyIteratorCloseClearsPooledPointerEntries(t *testing.T) {
	entries := []*appendOnlyEntry{
		{key: []byte("k0"), value: []byte("v0")},
		{key: []byte("k1"), value: []byte("v1")},
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

func TestAppendOnlyResetReusesValueBuffersAndDropsArenaKeys(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key1 := []byte("long-key-01")
	val1 := []byte("value-aaaaaa")
	m.Set(key1, val1)
	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	valBufPtr := &m.entries[0].value[0]
	if !m.entries[0].keyArena {
		t.Fatalf("expected small non-inline key to use key arena")
	}

	m.Reset()
	key2 := []byte("long-key-02")
	val2 := []byte("value-bbbbbb")
	m.Set(key2, val2)
	if m.count != 1 {
		t.Fatalf("count after reset=%d want=1", m.count)
	}
	if !m.entries[0].keyArena {
		t.Fatalf("expected small non-inline key to use key arena after reset")
	}
	if m.entries[0].value == nil || cap(m.entries[0].value) < len(val2) {
		t.Fatalf("expected value buffer capacity reuse across reset")
	}
	_ = valBufPtr
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
	if !ent.keyArena {
		t.Fatalf("expected non-inline key to be copied into key arena")
	}
	if !ent.valueOwned {
		t.Fatalf("expected non-steal set to mark valueOwned")
	}
	if len(ent.value) != len(src) {
		t.Fatalf("entry value len=%d want=%d", len(ent.value), len(src))
	}
	key[0] = 'X'
	if &ent.value[0] == &src[0] {
		t.Fatalf("entry value unexpectedly aliases caller buffer")
	}
	src[0] = 'X'

	got, tombstone, ok := m.Get([]byte("long-key-arena"))
	if !ok || tombstone {
		t.Fatalf("Get(long-key-arena) = ok=%v tombstone=%v; want ok=true tombstone=false", ok, tombstone)
	}
	if string(got) != "value-arena-copy" {
		t.Fatalf("stored value changed via source mutation: got=%q", got)
	}
}

func TestAppendOnlyLargeNonInlineKeyUsesReusableSlice(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := bytes.Repeat([]byte("k"), appendOnlyReusableKeyMaxCap+1)

	m.Set(key, []byte("value"))

	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	ent := &m.entries[0]
	if ent.keyArena {
		t.Fatalf("large key unexpectedly used key arena")
	}
	if !ent.keyReusable {
		t.Fatalf("large key should keep reusable slice metadata")
	}
}

func TestAppendOnlyKeyArenaClassForLen(t *testing.T) {
	tests := []struct {
		name     string
		length   int
		wantCap  int
		wantPool bool
	}{
		{name: "zero", length: 0, wantPool: false},
		{name: "single-byte", length: 1, wantCap: appendOnlyKeyArenaDefaultChunk, wantPool: true},
		{name: "default", length: appendOnlyKeyArenaDefaultChunk, wantCap: appendOnlyKeyArenaDefaultChunk, wantPool: true},
		{name: "round-up", length: appendOnlyKeyArenaDefaultChunk + 1, wantCap: appendOnlyKeyArenaDefaultChunk * 2, wantPool: true},
		{name: "retention-maximum", length: appendOnlyKeyArenaPoolMaxCap, wantCap: appendOnlyKeyArenaPoolMaxCap, wantPool: true},
		{name: "above-retention-maximum", length: appendOnlyKeyArenaPoolMaxCap + 1, wantPool: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, gotCap, gotPool := appendOnlyKeyArenaClassForLen(tc.length)
			if gotPool != tc.wantPool {
				t.Fatalf("pooled=%v want %v", gotPool, tc.wantPool)
			}
			if gotPool && gotCap != tc.wantCap {
				t.Fatalf("cap=%d want %d", gotCap, tc.wantCap)
			}
		})
	}
}

func TestAppendOnlyKeyArenaPoolStatsTrackRetainedChunks(t *testing.T) {
	DropAppendOnlyKeyArenaPools()
	t.Cleanup(DropAppendOnlyKeyArenaPools)

	before := AppendOnlyKeyArenaPoolStatsSnapshot()
	chunk := make([]byte, appendOnlyKeyArenaDefaultChunk)
	wantBytes := appendOnlyKeyArenaPoolBytes(cap(chunk))

	putAppendOnlyKeyArenaChunk(chunk)
	afterPut := AppendOnlyKeyArenaPoolStatsSnapshot()
	if got := afterPut.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes after put=%d want %d", got, wantBytes)
	}
	if got := afterPut.PutsTotal; got != before.PutsTotal+1 {
		t.Fatalf("puts total=%d want %d", got, before.PutsTotal+1)
	}

	got, ok := getAppendOnlyKeyArenaChunkFromPool(1)
	if !ok {
		t.Fatal("expected pooled key arena chunk")
	}
	afterGet := AppendOnlyKeyArenaPoolStatsSnapshot()
	if cap(got) != appendOnlyKeyArenaDefaultChunk {
		t.Fatalf("pooled cap=%d want %d", cap(got), appendOnlyKeyArenaDefaultChunk)
	}
	if gotRetained := afterGet.RetainedBytesEstimate; gotRetained != 0 {
		t.Fatalf("retained bytes after pooled get=%d want 0", gotRetained)
	}
	if gotGets := afterGet.GetsTotal; gotGets != before.GetsTotal+1 {
		t.Fatalf("gets total=%d want %d", gotGets, before.GetsTotal+1)
	}

	putAppendOnlyKeyArenaChunk(got)
	beforeDrop := AppendOnlyKeyArenaPoolStatsSnapshot()
	DropAppendOnlyKeyArenaPools()
	afterDrop := AppendOnlyKeyArenaPoolStatsSnapshot()
	if gotRetained := afterDrop.RetainedBytesEstimate; gotRetained != 0 {
		t.Fatalf("retained bytes after drop=%d want 0", gotRetained)
	}
	if gotDrops := afterDrop.DropsTotal; gotDrops != beforeDrop.DropsTotal+1 {
		t.Fatalf("drops total=%d want %d", gotDrops, beforeDrop.DropsTotal+1)
	}
	if gotDropBytes := afterDrop.DropBytesTotal; gotDropBytes < beforeDrop.DropBytesTotal+beforeDrop.RetainedBytesEstimate {
		t.Fatalf("drop bytes total=%d want at least %d", gotDropBytes, beforeDrop.DropBytesTotal+beforeDrop.RetainedBytesEstimate)
	}
}

func TestAppendOnlyKeyArenaPoolRetainsBackingAcrossGC(t *testing.T) {
	DropAppendOnlyKeyArenaPools()
	t.Cleanup(DropAppendOnlyKeyArenaPools)

	chunk := make([]byte, appendOnlyKeyArenaDefaultChunk)
	wantBytes := appendOnlyKeyArenaPoolBytes(cap(chunk))
	putAppendOnlyKeyArenaChunk(chunk)

	runtime.GC()

	afterGC := AppendOnlyKeyArenaPoolStatsSnapshot()
	if got := afterGC.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes after GC=%d want %d", got, wantBytes)
	}

	reused, ok := getAppendOnlyKeyArenaChunkFromPool(1)
	if !ok {
		t.Fatal("expected retained key arena chunk after GC")
	}
	if cap(reused) != cap(chunk) {
		t.Fatalf("reused cap after GC=%d want %d", cap(reused), cap(chunk))
	}
	if got := AppendOnlyKeyArenaPoolStatsSnapshot().RetainedBytesEstimate; got != 0 {
		t.Fatalf("retained bytes after get=%d want 0", got)
	}
	putAppendOnlyKeyArenaChunk(reused)
}

func TestAppendOnlyKeyArenaReusesPooledChunkAfterHardReset(t *testing.T) {
	DropAppendOnlyKeyArenaPools()
	t.Cleanup(DropAppendOnlyKeyArenaPools)

	m := NewAppendOnlyWithCapacity(0)
	key := bytes.Repeat([]byte("k"), 32)
	m.Set(key, nil)
	if m.count != 1 || len(m.entries[0].key) == 0 {
		t.Fatalf("test setup did not store a key-arena key")
	}
	firstPtr := uintptr(unsafe.Pointer(&m.entries[0].key[0]))

	m.ResetWithCapacityHard(0, 0)
	if got := AppendOnlyKeyArenaPoolStatsSnapshot().RetainedBytesEstimate; got == 0 {
		t.Fatalf("retained key arena pool bytes=0 after hard reset")
	}
	beforeGet := AppendOnlyKeyArenaPoolStatsSnapshot().GetsTotal

	key[0] = 'z'
	m.Set(key, nil)
	if m.count != 1 || len(m.entries[0].key) == 0 {
		t.Fatalf("post-reset write did not store a key-arena key")
	}
	reusedPtr := uintptr(unsafe.Pointer(&m.entries[0].key[0]))
	if reusedPtr != firstPtr {
		t.Fatalf("post-reset key arena pointer=%#x want reused %#x", reusedPtr, firstPtr)
	}
	if got := AppendOnlyKeyArenaPoolStatsSnapshot().GetsTotal; got != beforeGet+1 {
		t.Fatalf("key arena pool gets=%d want %d", got, beforeGet+1)
	}
}

func TestAppendOnlyReleaseDropEntriesDropsKeyArenaWithoutPooling(t *testing.T) {
	DropAppendOnlyKeyArenaPools()
	t.Cleanup(DropAppendOnlyKeyArenaPools)

	m := NewAppendOnlyWithCapacity(0)
	m.Set(bytes.Repeat([]byte("k"), 32), nil)
	if len(m.keyArena.chunks) == 0 {
		t.Fatal("test setup did not populate key arena")
	}

	m.ReleaseDropEntries()

	if got := AppendOnlyKeyArenaPoolStatsSnapshot().RetainedBytesEstimate; got != 0 {
		t.Fatalf("retained key arena pool bytes after cold release=%d want 0", got)
	}
	if len(m.keyArena.chunks) != 0 || m.keyArena.cur != nil || m.keyArena.curPos != 0 {
		t.Fatalf("key arena not dropped after cold release: chunks=%d cur=%v pos=%d", len(m.keyArena.chunks), m.keyArena.cur != nil, m.keyArena.curPos)
	}
}

func TestPutAppendOnlyKeyArenaChunkAdmissionDropsOnRetainBudgetPressure(t *testing.T) {
	DropAppendOnlyKeyArenaPools()
	t.Cleanup(DropAppendOnlyKeyArenaPools)

	savedBudget := appendOnlyKeyArenaPoolRetainBudgetBytes
	appendOnlyKeyArenaPoolRetainBudgetBytes = appendOnlyKeyArenaPoolBytes(appendOnlyKeyArenaDefaultChunk)
	t.Cleanup(func() {
		appendOnlyKeyArenaPoolRetainBudgetBytes = savedBudget
	})

	before := AppendOnlyKeyArenaPoolStatsSnapshot()
	putAppendOnlyKeyArenaChunk(make([]byte, appendOnlyKeyArenaDefaultChunk))
	putAppendOnlyKeyArenaChunk(make([]byte, appendOnlyKeyArenaDefaultChunk))

	after := AppendOnlyKeyArenaPoolStatsSnapshot()
	if got, want := after.RetainedBytesEstimate, appendOnlyKeyArenaPoolBytes(appendOnlyKeyArenaDefaultChunk); got != want {
		t.Fatalf("retained bytes=%d want %d", got, want)
	}
	if got := after.PutsTotal - before.PutsTotal; got != 1 {
		t.Fatalf("puts delta=%d want 1", got)
	}
	if got := after.AdmissionDropsTotal - before.AdmissionDropsTotal; got != 1 {
		t.Fatalf("admission drops delta=%d want 1", got)
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
	seen := make(map[uintptr]struct{}, len(m.valueArena.chunks))
	for _, chunk := range m.valueArena.chunks {
		full := chunk[:cap(chunk)]
		seen[uintptr(unsafe.Pointer(&full[0]))] = struct{}{}
	}

	m.Reset()
	if len(m.valueArena.retained) == 0 {
		t.Fatalf("expected retained arena chunks after reset")
	}
	if m.valueArena.retainedB <= 0 {
		t.Fatalf("expected retained arena bytes > 0")
	}

	m.Set([]byte("z"), make([]byte, 1024))
	if m.count != 1 || len(m.entries[0].value) == 0 {
		t.Fatalf("expected first post-reset set to allocate value from arena")
	}
	ptr := uintptr(unsafe.Pointer(&m.entries[0].value[0]))
	if _, ok := seen[ptr]; !ok {
		t.Fatalf("post-reset value buffer did not reuse retained arena chunk")
	}
}

func TestAppendOnlyValueArenaBackingStatsTrackActiveAndRetainedChunks(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("long-key-arena"), make([]byte, 4096))

	active := m.ValueArenaBackingStats()
	if active.ActiveChunks == 0 {
		t.Fatalf("active chunks=0 after set")
	}
	if active.ActiveBytes == 0 {
		t.Fatalf("active bytes=0 after set")
	}
	if active.RetainedChunks != 0 || active.RetainedBytes != 0 {
		t.Fatalf("retained before reset=%+v want zero retained", active)
	}

	m.Reset()
	retained := m.ValueArenaBackingStats()
	if retained.ActiveChunks != 0 || retained.ActiveBytes != 0 {
		t.Fatalf("active after reset=%+v want zero active", retained)
	}
	if retained.RetainedChunks == 0 {
		t.Fatalf("retained chunks=0 after reset")
	}
	if retained.RetainedBytes == 0 {
		t.Fatalf("retained bytes=0 after reset")
	}

	m.ResetWithCapacityHard(0, 0)
	dropped := m.ValueArenaBackingStats()
	if dropped.ActiveChunks != 0 || dropped.ActiveBytes != 0 || dropped.RetainedChunks != 0 || dropped.RetainedBytes != 0 {
		t.Fatalf("value arena stats after hard reset=%+v want all zero", dropped)
	}
}

func TestDropAppendOnlyValueArenaPoolsReplacesPools(t *testing.T) {
	class, _, ok := appendOnlyValueArenaClassForLen(appendOnlyValueArenaDefaultChunk)
	if !ok {
		t.Fatal("test setup failed to map value arena chunk to pool class")
	}
	before := appendOnlyValueArenaPoolForClass(class)
	if before == nil {
		t.Fatal("expected initial append-only value arena pool")
	}
	beforeDrop := AppendOnlyValueArenaPoolDropTotal()

	DropAppendOnlyValueArenaPools()

	after := appendOnlyValueArenaPoolForClass(class)
	if after == nil {
		t.Fatal("expected replacement append-only value arena pool")
	}
	if before == after {
		t.Fatal("drop did not replace append-only value arena pool")
	}
	if got := AppendOnlyValueArenaPoolDropTotal(); got != beforeDrop+1 {
		t.Fatalf("value arena pool drop total=%d want %d", got, beforeDrop+1)
	}
}

func TestAppendOnlyValueArenaPoolStatsTrackRetainedChunks(t *testing.T) {
	DropAppendOnlyValueArenaPools()
	t.Cleanup(DropAppendOnlyValueArenaPools)

	before := AppendOnlyValueArenaPoolStatsSnapshot()
	chunk := make([]byte, 0, appendOnlyValueArenaDefaultChunk)
	wantBytes := appendOnlyValueArenaPoolBytes(cap(chunk))

	putAppendOnlyValueArenaChunk(chunk)
	afterPut := AppendOnlyValueArenaPoolStatsSnapshot()
	if got := afterPut.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes after put=%d want %d", got, wantBytes)
	}
	if got := afterPut.PutsTotal; got != before.PutsTotal+1 {
		t.Fatalf("puts total=%d want %d", got, before.PutsTotal+1)
	}

	got := getAppendOnlyValueArenaChunk(1)
	afterGet := AppendOnlyValueArenaPoolStatsSnapshot()
	if cap(got) != appendOnlyValueArenaDefaultChunk {
		t.Fatalf("pooled cap=%d want %d", cap(got), appendOnlyValueArenaDefaultChunk)
	}
	switch gotGets := afterGet.GetsTotal; gotGets {
	case before.GetsTotal + 1:
		if gotRetained := afterGet.RetainedBytesEstimate; gotRetained != 0 {
			t.Fatalf("retained bytes after pooled get=%d want 0", gotRetained)
		}
	case before.GetsTotal:
		if gotRetained := afterGet.RetainedBytesEstimate; gotRetained != wantBytes {
			t.Fatalf("retained bytes after sync.Pool miss=%d want estimate %d", gotRetained, wantBytes)
		}
	default:
		t.Fatalf("gets total=%d want %d or %d", gotGets, before.GetsTotal, before.GetsTotal+1)
	}

	putAppendOnlyValueArenaChunk(got)
	beforeDrop := AppendOnlyValueArenaPoolStatsSnapshot()
	DropAppendOnlyValueArenaPools()
	afterDrop := AppendOnlyValueArenaPoolStatsSnapshot()
	if gotRetained := afterDrop.RetainedBytesEstimate; gotRetained != 0 {
		t.Fatalf("retained bytes after drop=%d want 0", gotRetained)
	}
	if gotDrops := afterDrop.DropsTotal; gotDrops != beforeDrop.DropsTotal+1 {
		t.Fatalf("drops total=%d want %d", gotDrops, beforeDrop.DropsTotal+1)
	}
	if gotDropBytes := afterDrop.DropBytesTotal; gotDropBytes < beforeDrop.DropBytesTotal+beforeDrop.RetainedBytesEstimate {
		t.Fatalf("drop bytes total=%d want at least %d", gotDropBytes, beforeDrop.DropBytesTotal+beforeDrop.RetainedBytesEstimate)
	}
}

func TestPutAppendOnlyValueArenaChunkDropsOversizedRetainedChunk(t *testing.T) {
	DropAppendOnlyValueArenaPools()
	t.Cleanup(DropAppendOnlyValueArenaPools)

	before := AppendOnlyValueArenaPoolStatsSnapshot()
	chunk := make([]byte, 0, appendOnlyValueArenaPoolMaxCap+1)
	wantBytes := appendOnlyValueArenaPoolBytes(cap(chunk))

	putAppendOnlyValueArenaChunk(chunk)

	after := AppendOnlyValueArenaPoolStatsSnapshot()
	if got := after.RetainedBytesEstimate; got != 0 {
		t.Fatalf("retained bytes after oversized put=%d want 0", got)
	}
	if got := after.PutsTotal - before.PutsTotal; got != 0 {
		t.Fatalf("puts delta=%d want 0", got)
	}
	if got := after.AdmissionDropsTotal - before.AdmissionDropsTotal; got != 1 {
		t.Fatalf("admission drops delta=%d want 1", got)
	}
	if got := after.AdmissionDropBytesTotal - before.AdmissionDropBytesTotal; got != wantBytes {
		t.Fatalf("admission drop bytes delta=%d want %d", got, wantBytes)
	}
}

func TestPutAppendOnlyValueArenaChunkAdmissionDropsOnRetainBudgetPressure(t *testing.T) {
	DropAppendOnlyValueArenaPools()
	t.Cleanup(DropAppendOnlyValueArenaPools)

	savedBudget := appendOnlyValueArenaPoolRetainBudgetBytes
	appendOnlyValueArenaPoolRetainBudgetBytes = appendOnlyValueArenaPoolBytes(appendOnlyValueArenaDefaultChunk)
	t.Cleanup(func() {
		appendOnlyValueArenaPoolRetainBudgetBytes = savedBudget
	})

	before := AppendOnlyValueArenaPoolStatsSnapshot()
	first := make([]byte, 0, appendOnlyValueArenaDefaultChunk)
	second := make([]byte, 0, appendOnlyValueArenaDefaultChunk)
	wantBytes := appendOnlyValueArenaPoolBytes(appendOnlyValueArenaDefaultChunk)

	putAppendOnlyValueArenaChunk(first)
	putAppendOnlyValueArenaChunk(second)

	after := AppendOnlyValueArenaPoolStatsSnapshot()
	if got := after.RetainedBytesEstimate; got != wantBytes {
		t.Fatalf("retained bytes=%d want budget %d", got, wantBytes)
	}
	if got := after.PutsTotal - before.PutsTotal; got != 1 {
		t.Fatalf("puts delta=%d want 1", got)
	}
	if got := after.DropsTotal - before.DropsTotal; got != 0 {
		t.Fatalf("drops delta=%d want 0", got)
	}
	if got := after.DropBytesTotal - before.DropBytesTotal; got != 0 {
		t.Fatalf("drop bytes delta=%d want 0", got)
	}
	if got := after.AdmissionDropsTotal - before.AdmissionDropsTotal; got != 1 {
		t.Fatalf("admission drops delta=%d want 1", got)
	}
	if got := after.AdmissionDropBytesTotal - before.AdmissionDropBytesTotal; got != wantBytes {
		t.Fatalf("admission drop bytes delta=%d want %d", got, wantBytes)
	}

	reused := getAppendOnlyValueArenaChunk(1)
	afterGet := AppendOnlyValueArenaPoolStatsSnapshot()
	if cap(reused) != appendOnlyValueArenaDefaultChunk {
		t.Fatalf("reused cap=%d want %d", cap(reused), appendOnlyValueArenaDefaultChunk)
	}
	switch got := afterGet.GetsTotal - after.GetsTotal; got {
	case 1:
		if got := afterGet.RetainedBytesEstimate; got != 0 {
			t.Fatalf("retained bytes after pooled get=%d want 0", got)
		}
	case 0:
		if !testRaceEnabled {
			t.Fatalf("gets delta after replacement put=%d want 1", got)
		}
		if got := afterGet.RetainedBytesEstimate; got != wantBytes {
			t.Fatalf("retained bytes after sync.Pool race miss=%d want estimate %d", got, wantBytes)
		}
	default:
		t.Fatalf("gets delta after replacement put=%d want 0 or 1", got)
	}
	putAppendOnlyValueArenaChunk(reused)
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

func TestAppendOnlyUnorderedAppendUsesSortedRunIndexBeforeHashFallback(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("b"), []byte("v1"))
	m.Set([]byte("a"), []byte("v2")) // force unordered path
	if m.ordered {
		t.Fatalf("expected unordered memtable")
	}
	if m.latestDirty {
		t.Fatalf("expected sorted-run index to keep latest lookups clean after order break")
	}
	if got := len(m.sortedRuns); got != 2 {
		t.Fatalf("sorted run count after order break=%d want=2", got)
	}
	if got := len(m.latest) + len(m.latest64); got != 0 {
		t.Fatalf("hash latest index len after order break=%d want=0", got)
	}

	it := m.NewIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected iterator snapshot build to keep sorted-run index clean")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered iterator to avoid caching snapshot; got snapCount=%d", m.snapCount)
	}
	if m.snapshot != nil {
		t.Fatalf("expected mutable unordered iterator to leave shared snapshot nil")
	}

	m.Set([]byte("b"), []byte("v3"))
	if m.latestDirty {
		t.Fatalf("expected unordered append to keep sorted-run index clean")
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
		t.Fatalf("expected second iterator snapshot to keep sorted-run index clean")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered iterator to keep shared snapshot uncached after rebuild; got snapCount=%d", m.snapCount)
	}
	if got := len(m.latest) + len(m.latest64); got != 0 {
		t.Fatalf("hash latest index len after sorted-run iterator=%d want=0", got)
	}
	if got := len(m.sortedRuns); got != 2 {
		t.Fatalf("sorted run count after append=%d want=2", got)
	}
}

func TestAppendOnlyUnorderedReverseIteratorDoesNotCacheSharedSnapshot(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("b"), []byte("v1"))
	m.Set([]byte("a"), []byte("v2")) // force unordered path
	if m.ordered {
		t.Fatalf("expected unordered memtable")
	}
	if m.latestDirty {
		t.Fatalf("expected sorted-run index to stay clean after order break")
	}

	it := m.NewReverseIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("reverse iterator close: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected reverse iterator snapshot build to keep sorted-run index clean")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered reverse iterator to avoid caching snapshot; got snapCount=%d", m.snapCount)
	}
	if m.snapshot != nil {
		t.Fatalf("expected mutable unordered reverse iterator to leave shared snapshot nil")
	}

	m.Set([]byte("b"), []byte("v3"))
	if m.latestDirty {
		t.Fatalf("expected unordered append to keep sorted-run index clean")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected snapshot invalidation after append; got snapCount=%d", m.snapCount)
	}

	it = m.NewReverseIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("reverse iterator close after append: %v", err)
	}
	if m.latestDirty {
		t.Fatalf("expected second reverse iterator snapshot to keep sorted-run index clean")
	}
	if m.snapCount != 0 {
		t.Fatalf("expected mutable unordered reverse iterator to keep shared snapshot uncached after rebuild; got snapCount=%d", m.snapCount)
	}
	if m.snapshot != nil {
		t.Fatalf("expected mutable unordered reverse iterator to keep shared snapshot nil after rebuild")
	}
	if got := len(m.latest) + len(m.latest64); got != 0 {
		t.Fatalf("hash latest index len after reverse sorted-run iterator=%d want=0", got)
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

func TestAppendOnlyResetDoesNotPoolStolenKeySlices(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	external := []byte("external-key")
	m.SetEntrySteal(external, nil, page.ValuePtr{}, node.FlagInline)
	m.Reset()

	m.Set([]byte("replacement"), nil)
	if string(external) != "external-key" {
		t.Fatalf("stolen caller key was mutated via pooled reuse: got %q", external)
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

func TestAppendOnlyReleaseDropsOwnedBuffers(t *testing.T) {
	m := NewAppendOnlyWithEntryCapacity(256)
	m.SetSteal([]byte("a"), []byte("v1"))
	m.SetSteal([]byte("b"), []byte("v2"))
	m.Freeze()
	it := m.NewIterator(nil, nil)
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	m.Release()
	if m.entries != nil {
		t.Fatalf("release retained entries with cap=%d", cap(m.entries))
	}
	if m.snapshot != nil {
		t.Fatalf("release retained snapshot with cap=%d", cap(m.snapshot))
	}
	if m.indexBuf != nil {
		t.Fatalf("release retained index buffer with cap=%d", cap(m.indexBuf))
	}
	if m.count != 0 || m.sizeBytes != 0 || m.frozen {
		t.Fatalf("release left table state count=%d size=%d frozen=%t", m.count, m.sizeBytes, m.frozen)
	}

	m.SetSteal([]byte("c"), []byte("v3"))
	if value, deleted, ok := m.Get([]byte("c")); !ok || deleted || string(value) != "v3" {
		t.Fatalf("table not reusable after release: value=%q deleted=%t ok=%t", value, deleted, ok)
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

func TestAppendOnlyLatestIndexReserveHintBounds(t *testing.T) {
	small := NewAppendOnlyWithEntryCapacity(16)
	small.mu.Lock()
	if got := small.latestIndexReserveHintLocked(2); got != 2 {
		small.mu.Unlock()
		t.Fatalf("small reserve hint=%d want exact need 2", got)
	}
	small.mu.Unlock()

	large := NewAppendOnlyWithEntryCapacity(appendOnlyLatestIndexMaxReserve * 4)
	large.mu.Lock()
	if got := large.latestIndexReserveHintLocked(appendOnlySortedRunMaxCount + 1); got != appendOnlyLatestIndexMaxReserve {
		large.mu.Unlock()
		t.Fatalf("large reserve hint=%d want capped %d", got, appendOnlyLatestIndexMaxReserve)
	}
	if got := large.latestIndexReserveHintLocked(appendOnlyLatestIndexMaxReserve + 123); got != appendOnlyLatestIndexMaxReserve {
		large.mu.Unlock()
		t.Fatalf("reserve hint above cap: got=%d want %d", got, appendOnlyLatestIndexMaxReserve)
	}
	large.mu.Unlock()
}

func TestAppendOnlyLateMixedKeyLatestIndexReserveStaysCapped(t *testing.T) {
	m := NewAppendOnlyWithEntryCapacity(appendOnlyLatestIndexMaxReserve * 4)
	var key [appendOnlyInlineKeyLen]byte
	for i := 0; i < appendOnlyLatestIndexMaxReserve+appendOnlySortedRunMaxCount; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(1_000_000+i))
		m.Set(key[:], []byte{1})
	}
	m.mu.Lock()
	if got := m.latestIndexReserveHintLocked(m.count + 1); got != appendOnlyLatestIndexMaxReserve {
		m.mu.Unlock()
		t.Fatalf("late mixed-key reserve hint=%d want capped %d", got, appendOnlyLatestIndexMaxReserve)
	}
	m.mu.Unlock()

	m.Set([]byte("variable-width-key"), []byte("value"))
	if got, _, ok := m.Get([]byte("variable-width-key")); !ok || string(got) != "value" {
		t.Fatalf("late mixed-key lookup=(%q, ok=%t), want value", got, ok)
	}
}

func TestAppendOnlyLatestIndexFallbackLatestWinsAndOwnsBytes(t *testing.T) {
	m := NewAppendOnlyWithEntryCapacity(appendOnlyLatestIndexMaxReserve * 2)
	var key [appendOnlyInlineKeyLen]byte
	for i := 0; i < appendOnlySortedRunMaxCount+8; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(10_000-i))
		value := []byte{byte(i)}
		m.Set(key[:], value)
		key[0] = 0xff
		value[0] = 0xee
	}

	var target [appendOnlyInlineKeyLen]byte
	binary.BigEndian.PutUint64(target[:], 42)
	first := []byte("first-value")
	m.Set(target[:], first)
	first[0] = 'X'
	second := []byte("second-value")
	m.Set(target[:], second)
	second[0] = 'Y'

	m.Freeze()
	got, deleted, ok := m.Get(target[:])
	if !ok || deleted || string(got) != "second-value" {
		t.Fatalf("Get target=(%q, deleted=%t, ok=%t), want second-value", got, deleted, ok)
	}

	m.Reset()
	m.mu.RLock()
	latestLen := len(m.latest)
	latest64Len := len(m.latest64)
	m.mu.RUnlock()
	if latestLen != 0 || latest64Len != 0 {
		t.Fatalf("reset leaked latest-index entries: latest=%d latest64=%d", latestLen, latest64Len)
	}
	if got, _, ok := m.Get(target[:]); ok {
		t.Fatalf("reset leaked old latest-index entry with value %q", got)
	}
}
