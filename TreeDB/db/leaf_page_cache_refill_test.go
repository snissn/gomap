package db

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafPageReadCacheNoAllocRefillColdDeclines(t *testing.T) {
	cache := newLeafPageReadCache(1)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128}
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	admitted := false
	if n := testing.AllocsPerRun(1000, func() {
		admitted = cache.storeReadMissNoAlloc(ptr, leaf, true) || admitted
	}); n != 0 {
		t.Fatalf("cold refusal allocated %g objects", n)
	}
	if admitted || cache.slots[0].data != nil || cache.slots[0].valid || cache.slots[0].keyFP.Load() != 0 || cache.slots[0].recordChecksumVerified || cache.slots[0].pageChecksumVerified.Load() {
		t.Fatal("cold refusal mutated page ownership or verification")
	}
	if stats := cache.stats(); stats.Entries != 0 || stats.Stores != 0 || stats.ReadMissAdmissionStores != 0 || stats.ReadMissAdmissionSkips == 0 || stats.ReadMissAdmissionLockSkips != 0 {
		t.Fatalf("cold refusal changed store accounting: %+v", stats)
	}
	// Exercise the capacity fence even when a selected slot has stale metadata.
	slot := &cache.slots[0]
	slot.data = make([]byte, 32)
	copy(slot.data, leaf)
	slot.key = newLeafPageReadCacheKey(ptr)
	slot.valid, slot.recordChecksumVerified = true, true
	slot.pageChecksumVerified.Store(true)
	slot.keyFP.Store(leafPageReadCacheKeyFingerprint(slot.key))
	key, backing, contents := slot.key, &slot.data[0], append([]byte(nil), slot.data...)
	slot.mu.Lock()
	_, ok := slot.storeWithAllocationPolicyLocked(newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 7, Offset: 256}), leaf, false, false)
	slot.mu.Unlock()
	if ok || slot.key != key || &slot.data[0] != backing || !bytes.Equal(slot.data, contents) || !slot.valid || !slot.recordChecksumVerified || !slot.pageChecksumVerified.Load() || slot.keyFP.Load() != leafPageReadCacheKeyFingerprint(key) {
		t.Fatal("undersized-slot refusal mutated selected slot")
	}
}

func TestLeafPageReadCacheNoAllocRefillReusesBacking(t *testing.T) {
	cache := newLeafPageReadCache(1)
	old := page.LeafLogPtr{FileID: 7, Offset: 128}
	next := page.LeafLogPtr{FileID: 7, Offset: 256}
	oldPage, nextPage := bytes.Repeat([]byte{0x42}, page.PageSize), bytes.Repeat([]byte{0x43}, page.PageSize)
	cache.storeWithRecordChecksumState(old, oldPage, true)
	if !cache.markPageChecksumVerified(old) {
		t.Fatal("old page checksum not marked")
	}
	backing := &cache.slots[0].data[0]
	if cache.storeReadMissNoAlloc(next, nextPage, true) || !cache.storeReadMissNoAlloc(next, nextPage, true) {
		t.Fatal("refill did not preserve repeated-miss probation")
	}
	var dst [page.PageSize]byte
	got, used, state, hit := cache.getToWithRecordPolicy(next, dst[:0], true)
	if !hit || !used || !state.RecordChecksumVerified || state.PageChecksumVerified || !bytes.Equal(got, nextPage) || &cache.slots[0].data[0] != backing {
		t.Fatalf("refill changed backing or verification state: hit=%t used=%t state=%+v", hit, used, state)
	}
	nextPage[0] ^= 1
	got, _, _, _ = cache.getToWithRecordPolicy(next, dst[:0], true)
	if got[0] != 0x43 {
		t.Fatal("refill borrowed caller page")
	}
	if stats := cache.stats(); stats.Entries != 1 || stats.Stores != 2 || stats.ReadMissAdmissionStores != 1 || stats.Evictions != 1 {
		t.Fatalf("refill accounting: %+v", stats)
	}
	// Every measured iteration performs a real replacement of the single slot.
	current := old
	failed := false
	if n := testing.AllocsPerRun(1000, func() {
		cache.storeReadMissNoAlloc(current, oldPage, true)
		if !cache.storeReadMissNoAlloc(current, oldPage, true) {
			failed = true
		}
		if current == old {
			current = next
		} else {
			current = old
		}
	}); n != 0 || failed {
		t.Fatalf("warm replacement allocated %g objects or declined=%t", n, failed)
	}
	if &cache.slots[0].data[0] != backing {
		t.Fatal("warm replacement changed backing")
	}
	cache.disabled.Store(true)
	if cache.storeReadMissNoAlloc(old, oldPage, true) || cache.storeReadMissNoAlloc(next, nextPage, true) {
		t.Fatal("disabled cache admitted refill")
	}
}

func TestLeafPageReadCacheNoAllocRefillBorrowedSlotSkips(t *testing.T) {
	cache := newLeafPageReadCache(1)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128}
	other := page.LeafLogPtr{FileID: 7, Offset: 256}
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	cache.storeWithRecordChecksumState(ptr, leaf, true)
	data, lease, ok := cache.getViewLocked(ptr)
	if !ok {
		t.Fatal("missing borrowed page")
	}
	defer lease.ReleaseLeafLogPageView()
	done := make(chan bool, 1)
	go func() {
		cache.storeReadMissNoAlloc(other, leaf, true)
		done <- cache.storeReadMissNoAlloc(other, leaf, true)
	}()
	select {
	case admitted := <-done:
		if admitted || !bytes.Equal(data, leaf) {
			t.Fatal("borrowed slot was replaced")
		}
	case <-time.After(time.Second):
		t.Fatal("refill waited for a borrowed slot")
	}
	if cache.stats().ReadMissAdmissionLockSkips != 1 {
		t.Fatal("borrowed-slot contention was not counted as a lock skip")
	}
}

func newVerifiedCacheSnapshot(t *testing.T) (*Snapshot, page.LeafLogPtr, *os.File, rootpublication.StableIdentity, []byte) {
	t.Helper()
	dir := t.TempDir()
	pointers := appendPointersInNewSegment(t, dir, 0, 1, 1, 1, func(int) []byte { return bytes.Repeat([]byte{0x42}, page.PageSize) })
	d, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, LeafPageReadCacheEntries: 1, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoErr(t, d) })
	b := d.NewBatch().(*Batch)
	defer b.Close()
	if err := b.SetPointer([]byte("registered-file"), pointers[0]); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	s := d.AcquireSnapshot()
	t.Cleanup(func() { closeNoErr(t, s) })
	// These tests exercise ownership/identity and cache state. The public
	// caching regression supplies real full-CRC-decoded outer leaf pages.
	for id := range s.state.ValueLogSet.Files {
		ptr := page.LeafLogPtr{FileID: id, Offset: 128}
		f, identity, err := s.PinnedValueLogFile(id)
		if err != nil {
			t.Fatal(err)
		}
		return s, ptr, f, identity, bytes.Repeat([]byte{0x42}, page.PageSize)
	}
	t.Fatal("no exact registered file")
	return nil, page.LeafLogPtr{}, nil, rootpublication.StableIdentity{}, nil
}

func TestSnapshotStoreVerifiedCachedLeafPageIdentityAndState(t *testing.T) {
	s, ptr, inspected, identity, leaf := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	other := ptr
	other.Offset++
	cache.storeWithRecordChecksumState(other, leaf, true)
	cache.markPageChecksumVerified(other)
	backing := &cache.slots[0].data[0]
	wrong := identity
	wrong.Generation++
	if err := s.StoreVerifiedCachedLeafPage(ptr, inspected, wrong, leaf); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("wrong identity: %v", err)
	}
	if err := s.StoreVerifiedCachedLeafPage(ptr, nil, identity, leaf); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("wrong inspected handle: %v", err)
	}
	if cache.stats().Stores != 1 {
		t.Fatal("identity refusal changed cache")
	}
	for i := 0; i < 2; i++ {
		if err := s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	var dst [page.PageSize]byte
	got, hit, err := s.CopyVerifiedCachedLeafPage(ptr, inspected, identity, dst[:0])
	if err != nil || !hit || !bytes.Equal(got, leaf) || &cache.slots[0].data[0] != backing || cache.slots[0].pageChecksumVerified.Load() {
		t.Fatalf("verified snapshot refill: hit=%t err=%v", hit, err)
	}
	// A verified-record upgrade preserves the independent page CRC state.
	cache.markPageChecksumVerified(ptr)
	failed := false
	if n := testing.AllocsPerRun(1000, func() {
		if s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf) != nil {
			failed = true
		}
	}); n != 0 || failed || !cache.slots[0].pageChecksumVerified.Load() {
		t.Fatalf("snapshot upgrade allocated=%g failed=%t or lost page verification", n, failed)
	}
	cache.disabled.Store(true)
	before := cache.stats().Stores
	if err := s.StoreVerifiedCachedLeafPage(other, inspected, identity, leaf); err != nil || cache.stats().Stores != before {
		t.Fatal("disabled cache was populated")
	}
	cache.disabled.Store(false)
	closeNoErr(t, s)
	if err := s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot refill: %v", err)
	}
}

func TestSnapshotStoreVerifiedCachedLeafPageColdAndMismatchedBytes(t *testing.T) {
	s, ptr, inspected, identity, leaf := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	failed := false
	if n := testing.AllocsPerRun(1000, func() {
		if s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf) != nil {
			failed = true
		}
	}); n != 0 || failed || cache.slots[0].data != nil || cache.stats().Stores != 0 {
		t.Fatalf("cold snapshot refill allocated=%g failed=%t or populated backing", n, failed)
	}
	mismatch := append([]byte(nil), leaf...)
	mismatch[0] ^= 1
	cache.store(ptr, mismatch)
	for i := 0; i < 2; i++ {
		if err := s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	if cache.slots[0].recordChecksumVerified || !bytes.Equal(cache.slots[0].data, mismatch) {
		t.Fatal("different cached bytes gained record verification")
	}
}

func TestSnapshotStoreVerifiedCachedLeafPageBorrowAndClose(t *testing.T) {
	s, ptr, inspected, identity, leaf := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	cache.store(ptr, leaf)
	_, lease, ok := cache.getViewLocked(ptr)
	if !ok {
		t.Fatal("missing borrowed unverified page")
	}
	defer lease.ReleaseLeafLogPageView()
	done := make(chan error, 1)
	go func() { done <- s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf) }()
	select {
	case err := <-done:
		if err != nil || cache.slots[0].recordChecksumVerified {
			t.Fatalf("borrowed same-key upgrade: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot refill waited for its own borrowed slot")
	}
	// Close may detach the readable state only after any active refill ends.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := s.StoreVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil && !errors.Is(err, ErrClosed) {
				t.Error(err)
				return
			}
		}
	}()
	closeNoErr(t, s)
	wg.Wait()
}
