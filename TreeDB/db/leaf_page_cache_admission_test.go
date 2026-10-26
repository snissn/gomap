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

func TestSnapshotAdmitVerifiedCachedLeafPageColdOwnership(t *testing.T) {
	s, ptr, inspected, identity, leaf := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	if got := cache.stats(); got.BackingBytes != 0 || got.Entries != 0 {
		t.Fatalf("cache was not cold: %+v", got)
	}
	// This explicitly allocating API still observes the original probation.
	if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
		t.Fatal(err)
	}
	if got := cache.stats(); got.BackingBytes != 0 || got.Stores != 0 {
		t.Fatalf("first miss allocated or stored: %+v", got)
	}
	if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
		t.Fatal(err)
	}
	backing := &cache.slots[0].data[0]
	if got := cache.stats(); got.BackingBytes != page.PageSize || got.Bytes != page.PageSize || got.ReadMissAdmissionStores != 1 {
		t.Fatalf("cold admission accounting: %+v", got)
	}
	var dst [page.PageSize]byte
	got, hit, err := s.CopyVerifiedCachedLeafPage(ptr, inspected, identity, dst[:0])
	if err != nil || !hit || !bytes.Equal(got, leaf) || cache.slots[0].pageChecksumVerified.Load() {
		t.Fatalf("admitted record/page state: hit=%t err=%v", hit, err)
	}
	leaf[0] ^= 1
	got, hit, err = s.CopyVerifiedCachedLeafPage(ptr, inspected, identity, dst[:0])
	if err != nil || !hit || got[0] != 0x42 {
		t.Fatal("cache retained caller's decoded page")
	}
	leaf[0] ^= 1
	cache.markPageChecksumVerified(ptr)
	failed := false
	if n := testing.AllocsPerRun(1000, func() {
		failed = s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf) != nil || failed
	}); n != 0 || failed || !cache.slots[0].pageChecksumVerified.Load() {
		t.Fatalf("warm admission allocated=%g failed=%t or lost page verification", n, failed)
	}
	// A later page evicts into the same backing; no snapshot-sized cache appears.
	other := ptr
	other.Offset++
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(other, inspected, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	if &cache.slots[0].data[0] != backing || cache.slots[0].pageChecksumVerified.Load() {
		t.Fatal("eviction replaced backing or kept independent page verification")
	}
	if got := cache.stats(); got.BackingBytes != page.PageSize || got.Evictions != 1 {
		t.Fatalf("replacement accounting: %+v", got)
	}
	closeNoErr(t, s)
	if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed admission: %v", err)
	}
	if cache.stats().BackingBytes != page.PageSize {
		t.Fatal("snapshot close incorrectly released backend-owned backing")
	}
}

func TestSnapshotAdmitVerifiedCachedLeafPageRefusalAndMismatch(t *testing.T) {
	s, ptr, inspected, identity, leaf := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	wrong := identity
	wrong.Generation++
	otherHandle, err := os.Open(inspected.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer otherHandle.Close()
	for _, input := range []struct {
		file *os.File
		id   rootpublication.StableIdentity
	}{{nil, identity}, {otherHandle, identity}, {inspected, wrong}} {
		if err := s.AdmitVerifiedCachedLeafPage(ptr, input.file, input.id, leaf); !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("uninspected physical identity accepted: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf[:len(leaf)-1]); err != nil {
			t.Fatal(err)
		}
	}
	cache.disabled.Store(true)
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	if got := cache.stats(); got.Stores != 0 || got.BackingBytes != 0 {
		t.Fatalf("refused input acquired backing: %+v", got)
	}
	cache.disabled.Store(false)
	mismatch := append([]byte(nil), leaf...)
	mismatch[0] ^= 1
	cache.store(ptr, mismatch)
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	if cache.slots[0].recordChecksumVerified || !bytes.Equal(cache.slots[0].data, mismatch) || cache.stats().BackingBytes != page.PageSize {
		t.Fatal("different bytes gained verification or changed backing")
	}
	// Disabled configuration is a correct uncached path, not an admission error.
	s.reader.leafPageCache = nil
	if err := s.AdmitVerifiedCachedLeafPage(ptr, inspected, identity, leaf); err != nil {
		t.Fatalf("disabled cache: %v", err)
	}
	s.reader.leafPageCache = cache
}

func TestSnapshotAdmitVerifiedCachedLeafPageBorrowAndClose(t *testing.T) {
	s, ptr, inspected, identity, leaf := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	cache.storeWithRecordChecksumState(ptr, leaf, true)
	data, lease, ok := cache.getViewLocked(ptr)
	if !ok {
		t.Fatal("missing borrowed page")
	}
	other := ptr
	other.Offset++
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			if err := s.AdmitVerifiedCachedLeafPage(other, inspected, identity, leaf); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil || !bytes.Equal(data, leaf) || cache.stats().Evictions != 0 {
			lease.ReleaseLeafLogPageView()
			t.Fatalf("borrowed cache changed: %v", err)
		}
	case <-time.After(time.Second):
		lease.ReleaseLeafLogPageView()
		<-done
		t.Fatal("cache admission waited for borrowed page")
	}
	lease.ReleaseLeafLogPageView()
	// Close races real admitted reads; beginRead owns detachment exclusion.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := s.AdmitVerifiedCachedLeafPage(other, inspected, identity, leaf); err != nil && !errors.Is(err, ErrClosed) {
				t.Error(err)
				return
			}
		}
	}()
	closeNoErr(t, s)
	wg.Wait()
	if cache.stats().BackingBytes != page.PageSize {
		t.Fatal("borrow/close changed fixed-slot backing bound")
	}
}

func TestLeafPageReadCacheBackingCapacityConcurrentAdmission(t *testing.T) {
	cache := newLeafPageReadCache(8)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			leaf := bytes.Repeat([]byte{byte(worker + 1)}, page.PageSize)
			for i := 0; i < 128; i++ {
				ptr := page.LeafLogPtr{FileID: 7, Offset: uint64(worker*128 + i + 1)}
				cache.storeReadMiss(ptr, leaf, true)
				cache.storeReadMiss(ptr, leaf, true)
			}
		}(worker)
	}
	// The same maintenance bypass is toggled during admission. No extra cache
	// authority or synchronization is introduced by this stress control.
	for i := 0; i < 128; i++ {
		cache.disabled.Store(true)
		cache.disabled.Store(false)
	}
	wg.Wait()
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	// Fill all configured slots using the original write allocator too.
	for b := range cache.buckets {
		for _, ptr := range findBucketLeafPagePtrs(t, cache, b, cache.ways) {
			cache.store(ptr, leaf)
		}
	}
	var actual uint64
	for i := range cache.slots {
		actual += uint64(cap(cache.slots[i].data))
	}
	stats := cache.stats()
	if stats.BackingBytes != actual || actual != uint64(len(cache.slots))*page.PageSize || stats.Bytes > stats.BackingBytes {
		t.Fatalf("fixed-slot accounting: actual=%d stats=%+v", actual, stats)
	}
}
