package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func makeDistinctLeafPage(seed uint64) []byte {
	buf := make([]byte, page.PageSize)
	binary.LittleEndian.PutUint64(buf[:8], seed)
	binary.LittleEndian.PutUint64(buf[page.PageSize-8:], ^seed)
	return buf
}

func TestAdmitVerifiedCachedLeafPageFusedSingleStepUpgrade(t *testing.T) {
	cache := newLeafPageReadCache(16)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 4096}
	leaf := makeDistinctLeafPage(42)
	limits := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	var dst [page.PageSize]byte

	// Write-origin store must never set recordChecksumVerified or cowReadPolicy.
	cache.storeWrite(ptr, leaf)
	if _, _, _, hit := cache.getToWithRecordPolicy(ptr, dst[:0], true); hit {
		t.Fatal("write-origin slot unexpectedly hit verified read policy")
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, limits); hit {
		t.Fatal("write-origin slot unexpectedly hit dictionary COW read policy")
	}
	before := cache.stats()

	// Single fused admit upgrades both recordChecksumVerified and cowReadPolicy under one lock.
	admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, limits)
	if err != nil || !admitted {
		t.Fatalf("admitVerifiedCachedLeafPage(upgrade): admitted=%t err=%v, want true, nil", admitted, err)
	}
	got, usedDst, state, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, limits)
	if !hit || !usedDst || !state.RecordChecksumVerified || !bytes.Equal(got, leaf) {
		t.Fatalf("getToWithCOWPolicy after single-step upgrade: hit=%t usedDst=%t state=%+v", hit, usedDst, state)
	}
	after := cache.stats()
	if after.Stores != before.Stores || after.ReadMissAdmissionStores != before.ReadMissAdmissionStores || after.Evictions != before.Evictions {
		t.Fatalf("single-step upgrade mutated store/eviction stats: before=%+v after=%+v", before, after)
	}

	// Warm fused upgrade must be zero-allocation.
	var failed bool
	if n := testing.AllocsPerRun(1000, func() {
		ok, admitErr := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, limits)
		if admitErr != nil || !ok {
			failed = true
		}
	}); n != 0 || failed {
		t.Fatalf("warm fused dictionary upgrade allocated=%g failed=%t", n, failed)
	}
}

func TestAdmitVerifiedCachedLeafPageFusedReadMissAdmission(t *testing.T) {
	cache := newLeafPageReadCache(16)
	ptr := page.LeafLogPtr{FileID: 9, Offset: 8192}
	leaf := makeDistinctLeafPage(99)
	limits := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	var dst [page.PageSize]byte

	// First miss must observe repeated-miss probation and not allocate backing.
	admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, limits)
	if err != nil || admitted {
		t.Fatalf("first cold dictionary miss: admitted=%t err=%v, want false, nil", admitted, err)
	}
	if stats := cache.stats(); stats.BackingBytes != 0 || stats.Stores != 0 || stats.ReadMissAdmissionSkips != 1 {
		t.Fatalf("first cold miss stats=%+v, want probation skip with zero backing", stats)
	}

	// Second miss admits page with recordChecksumVerified and cowReadPolicy set atomically under one lock.
	admitted, err = admitVerifiedCachedLeafPage(cache, ptr, leaf, true, limits)
	if err != nil || !admitted {
		t.Fatalf("second cold dictionary miss: admitted=%t err=%v, want true, nil", admitted, err)
	}
	if stats := cache.stats(); stats.BackingBytes != page.PageSize || stats.ReadMissAdmissionStores != 1 || stats.Stores != 1 {
		t.Fatalf("second cold miss stats=%+v, want 1 admitted store and 4 KiB backing", stats)
	}
	got, usedDst, state, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, limits)
	if !hit || !usedDst || !state.RecordChecksumVerified || !bytes.Equal(got, leaf) {
		t.Fatalf("immediate getToWithCOWPolicy after read-miss admit: hit=%t usedDst=%t state=%+v", hit, usedDst, state)
	}
}

func TestAdmitVerifiedCachedLeafPageFusedConcurrentNoPolicyWindow(t *testing.T) {
	const numKeys = 128
	cache := newLeafPageReadCache(numKeys * 4)
	limits := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	ptrs := make([]page.LeafLogPtr, numKeys)
	leaves := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		ptrs[i] = page.LeafLogPtr{FileID: 11, Offset: uint64((i + 1) * page.PageSize)}
		leaves[i] = makeDistinctLeafPage(uint64(i + 1))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	start := make(chan struct{})

	// Writers concurrently admit dictionary misses from cold state.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < numKeys; i++ {
				if _, err := admitVerifiedCachedLeafPage(cache, ptrs[i], leaves[i], true, limits); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if _, err := admitVerifiedCachedLeafPage(cache, ptrs[i], leaves[i], true, limits); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	// Readers continuously inspect slots: whenever a slot is valid and recordChecksumVerified
	// (only dictionary admits run here), its cowReadPolicy must ALREADY satisfy limits.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dst [page.PageSize]byte
			<-start
			for round := 0; round < 16; round++ {
				for i := 0; i < numKeys; i++ {
					key := newLeafPageReadCacheKey(ptrs[i])
					keyFP := leafPageReadCacheKeyFingerprint(key)
					bStart, bEnd := cache.bucketRange(cache.bucketIndex(key))
					for sIdx := bStart; sIdx < bEnd; sIdx++ {
						slot := &cache.slots[sIdx]
						if slot.keyFP.Load() != keyFP {
							continue
						}
						slot.mu.RLock()
						if slot.valid && slot.key == key && slot.recordChecksumVerified && !cowReadPolicyAllows(slot.cowReadPolicy, limits) {
							slot.mu.RUnlock()
							select {
							case errCh <- errors.New("observed verified dictionary slot with missing cowReadPolicy"):
							default:
							}
							return
						}
						slot.mu.RUnlock()
					}
					if got, _, state, hit := cache.getToWithCOWPolicy(ptrs[i], dst[:0], true, limits); hit {
						if !state.RecordChecksumVerified || !bytes.Equal(got, leaves[i]) {
							select {
							case errCh <- errors.New("corrupt hit from concurrent dictionary admission"):
							default:
							}
							return
						}
					}
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	// Every key should now be readable via getToWithCOWPolicy without another admit.
	var dst [page.PageSize]byte
	hits := 0
	for i := 0; i < numKeys; i++ {
		if got, _, _, hit := cache.getToWithCOWPolicy(ptrs[i], dst[:0], true, limits); hit {
			if !bytes.Equal(got, leaves[i]) {
				t.Fatalf("key %d payload mismatch", i)
			}
			hits++
		}
	}
	if hits == 0 {
		t.Fatal("expected concurrent dictionary admissions to populate cache")
	}
}

func TestAdmitVerifiedCachedLeafPageFusedSafetyInvariants(t *testing.T) {
	cache := newLeafPageReadCache(4)
	ptr := page.LeafLogPtr{FileID: 13, Offset: 4096}
	leaf := makeDistinctLeafPage(123)
	mismatch := makeDistinctLeafPage(456)
	canonical := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	native := valuelog.COWReadLimits{MaxRecordBytes: 512 << 20, MaxRawBytes: 512 << 20, MaxValueBytes: 512 << 20}
	var dst [page.PageSize]byte

	// Invalid/zero limits return ErrCOWReadUnsupported without touching cache.
	for _, bad := range []valuelog.COWReadLimits{
		{},
		{MaxRecordBytes: 0, MaxRawBytes: 100, MaxValueBytes: 100},
		{MaxRecordBytes: 100, MaxRawBytes: 0, MaxValueBytes: 100},
		{MaxRecordBytes: 100, MaxRawBytes: 100, MaxValueBytes: 0},
	} {
		admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, bad)
		if !errors.Is(err, valuelog.ErrCOWReadUnsupported) || admitted {
			t.Fatalf("invalid limits %+v: admitted=%t err=%v, want false, ErrCOWReadUnsupported", bad, admitted, err)
		}
	}

	// Disabled cache declines safely without mutating state.
	cache.storeWrite(ptr, leaf)
	cache.disabled.Store(true)
	if admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, canonical); err != nil || admitted {
		t.Fatalf("disabled cache admit: admitted=%t err=%v, want false, nil", admitted, err)
	}
	cache.disabled.Store(false)
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, canonical); hit {
		t.Fatal("disabled cache unexpectedly upgraded slot")
	}

	// Nil cache declines safely.
	if admitted, err := admitVerifiedCachedLeafPage(nil, ptr, leaf, true, canonical); err != nil || admitted {
		t.Fatalf("nil cache admit: admitted=%t err=%v, want false, nil", admitted, err)
	}

	// Mismatched bytes never upgrade an unverified slot or overwrite it.
	for i := 0; i < 2; i++ {
		admitted, err := admitVerifiedCachedLeafPage(cache, ptr, mismatch, true, canonical)
		if err != nil || admitted {
			t.Fatalf("mismatched bytes admit[%d]: admitted=%t err=%v, want false, nil", i, admitted, err)
		}
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, canonical); hit {
		t.Fatal("mismatched bytes gained COW policy credit")
	}
	if got, ok := cache.get(ptr); !ok || !bytes.Equal(got, leaf) {
		t.Fatal("mismatched bytes overwrote existing slot data")
	}

	// Looser -> stricter upgrades; stricter -> looser preserves stricter certificate.
	if admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, native); err != nil || !admitted {
		t.Fatalf("admit native: admitted=%t err=%v", admitted, err)
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, canonical); hit {
		t.Fatal("native credit unexpectedly satisfied stricter canonical limits")
	}
	if admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, canonical); err != nil || !admitted {
		t.Fatalf("upgrade to canonical: admitted=%t err=%v", admitted, err)
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, canonical); !hit {
		t.Fatal("stricter canonical upgrade did not hit")
	}
	if admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, native); err != nil || !admitted {
		t.Fatalf("re-admit native over canonical: admitted=%t err=%v", admitted, err)
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, canonical); !hit {
		t.Fatal("looser native re-admit erased stricter canonical credit")
	}

	// Incomparable policies must never synthesize a stricter combined policy.
	first := valuelog.COWReadLimits{MaxRecordBytes: 100, MaxRawBytes: 200, MaxValueBytes: 100}
	second := valuelog.COWReadLimits{MaxRecordBytes: 200, MaxRawBytes: 100, MaxValueBytes: 100}
	imagined := valuelog.COWReadLimits{MaxRecordBytes: 100, MaxRawBytes: 100, MaxValueBytes: 100}
	ptrInc := page.LeafLogPtr{FileID: 13, Offset: 8192}
	cache.storeWrite(ptrInc, leaf)
	if admitted, err := admitVerifiedCachedLeafPage(cache, ptrInc, leaf, true, first); err != nil || !admitted {
		t.Fatalf("admit first incomparable: admitted=%t err=%v", admitted, err)
	}
	if admitted, err := admitVerifiedCachedLeafPage(cache, ptrInc, leaf, true, second); err != nil || !admitted {
		t.Fatalf("admit second incomparable: admitted=%t err=%v", admitted, err)
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptrInc, dst[:0], true, imagined); hit {
		t.Fatal("incomparable policies synthesized stricter credit")
	}
	if _, _, _, hit := cache.getToWithCOWPolicy(ptrInc, dst[:0], true, second); !hit {
		t.Fatal("second incomparable policy was not retained")
	}

	// Write-origin overwrite resets both recordChecksumVerified and cowReadPolicy.
	cache.storeWrite(ptr, leaf)
	if _, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0], true, native); hit {
		t.Fatal("storeWrite did not reset cowReadPolicy")
	}

	// Contended slot (borrowed via getViewLocked) must skip non-blockingly.
	_, lease, ok := cache.getViewLocked(ptr)
	if !ok {
		t.Fatal("expected borrowed slot view")
	}
	done := make(chan bool, 1)
	go func() {
		_, _ = admitVerifiedCachedLeafPage(cache, ptr, leaf, true, canonical)
		admitted, _ := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, canonical)
		done <- admitted
	}()
	select {
	case admitted := <-done:
		if admitted {
			lease.ReleaseLeafLogPageView()
			t.Fatal("admitted while slot was borrowed")
		}
	case <-time.After(time.Second):
		lease.ReleaseLeafLogPageView()
		t.Fatal("admitVerifiedCachedLeafPage blocked on borrowed slot")
	}
	lease.ReleaseLeafLogPageView()
}

func TestLeafPageReadCacheLazyBackingMemory(t *testing.T) {
	const capacity = 32768
	const populated = 64

	cache := newLeafPageReadCache(capacity)
	stats := cache.stats()
	if stats.Capacity != capacity || stats.Buckets != capacity/leafPageReadCacheWays || stats.Ways != leafPageReadCacheWays {
		t.Fatalf("geometry=%+v, want capacity=%d buckets=%d ways=%d", stats, capacity, capacity/leafPageReadCacheWays, leafPageReadCacheWays)
	}
	if stats.BackingBytes != 0 || stats.Entries != 0 {
		t.Fatalf("cold cache backing=%d entries=%d, want 0", stats.BackingBytes, stats.Entries)
	}

	for i := 0; i < populated; i++ {
		ptr := page.LeafLogPtr{FileID: 3, Offset: uint64((i + 1) * page.PageSize)}
		cache.storeWrite(ptr, makeDistinctLeafPage(uint64(i+1)))
	}
	stats = cache.stats()
	wantBacking := uint64(populated * page.PageSize)
	if stats.BackingBytes != wantBacking {
		t.Fatalf("lazy BackingBytes=%d, want %d (must not preallocate 128 MiB)", stats.BackingBytes, wantBacking)
	}
	if stats.Entries != populated {
		t.Fatalf("Entries=%d, want %d", stats.Entries, populated)
	}
}

func TestLeafPageReadCacheWorkingSet12kMissRateAndConflictEvictions(t *testing.T) {
	const (
		workingSet = 12000
		rounds     = 4
	)
	limits := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	ptrs := make([]page.LeafLogPtr, workingSet)
	// Reuse a small ring of distinct page templates to keep test memory low and fast.
	templates := make([][]byte, 64)
	for i := range templates {
		templates[i] = makeDistinctLeafPage(uint64(i + 1))
	}
	for i := 0; i < workingSet; i++ {
		ptrs[i] = page.LeafLogPtr{FileID: 5, Offset: uint64((i + 1) * page.PageSize)}
	}

	runWorkload := func(capacity int) (steadyMissRate float64, loadConflictEvictions uint64, totalStats leafPageReadCacheStats) {
		cache := newLeafPageReadCache(capacity)
		for i := 0; i < workingSet; i++ {
			cache.storeWrite(ptrs[i], templates[i%len(templates)])
		}
		loadConflictEvictions = cache.stats().ConflictEvictions

		var dst [page.PageSize]byte
		var steadyHits, steadyMisses uint64
		for round := 0; round < rounds; round++ {
			for i := 0; i < workingSet; i++ {
				leaf := templates[i%len(templates)]
				if _, _, _, hit := cache.getToWithCOWPolicy(ptrs[i], dst[:0], true, limits); hit {
					if round > 0 {
						steadyHits++
					}
					continue
				}
				if round > 0 {
					steadyMisses++
				}
				if _, err := admitVerifiedCachedLeafPage(cache, ptrs[i], leaf, true, limits); err != nil {
					t.Fatalf("admitVerifiedCachedLeafPage: %v", err)
				}
			}
		}
		steadyMissRate = float64(steadyMisses) / float64(steadyHits+steadyMisses)
		return steadyMissRate, loadConflictEvictions, cache.stats()
	}

	miss8k, conflict8k, stats8k := runWorkload(8192)
	miss32k, conflict32k, stats32k := runWorkload(32768)

	t.Logf("8192-slot cache:  steadyMissRate=%.2f%% loadConflictEvictions=%d totalConflictEvictions=%d totalEvictions=%d backingMiB=%.1f",
		miss8k*100, conflict8k, stats8k.ConflictEvictions, stats8k.Evictions, float64(stats8k.BackingBytes)/(1<<20))
	t.Logf("32768-slot cache: steadyMissRate=%.2f%% loadConflictEvictions=%d totalConflictEvictions=%d totalEvictions=%d backingMiB=%.1f",
		miss32k*100, conflict32k, stats32k.ConflictEvictions, stats32k.Evictions, float64(stats32k.BackingBytes)/(1<<20))

	if miss8k <= 0.30 {
		t.Fatalf("8192-slot steady miss rate=%.2f%%, expected >30%% on 12k working set", miss8k*100)
	}
	if miss32k >= 0.05 {
		t.Fatalf("32768-slot steady miss rate=%.2f%%, expected <5%% on 12k working set", miss32k*100)
	}
	if stats32k.ConflictEvictions*10 >= stats8k.ConflictEvictions {
		t.Fatalf("expected >10x reduction in conflict evictions: 8192=%d 32768=%d", stats8k.ConflictEvictions, stats32k.ConflictEvictions)
	}
}

func BenchmarkAdmitVerifiedCachedLeafPageCOWPolicy(b *testing.B) {
	limits := valuelog.COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	ptr := page.LeafLogPtr{FileID: 7, Offset: 4096}
	leaf := makeDistinctLeafPage(77)

	b.Run("fused_upgrade_existing", func(b *testing.B) {
		cache := newLeafPageReadCache(32768)
		cache.storeWrite(ptr, leaf)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			admitted, err := admitVerifiedCachedLeafPage(cache, ptr, leaf, true, limits)
			if err != nil || !admitted {
				b.Fatalf("admit: admitted=%t err=%v", admitted, err)
			}
		}
	})

	b.Run("working_set_12k_8192_vs_32768", func(b *testing.B) {
		const workingSet = 12000
		ptrs := make([]page.LeafLogPtr, workingSet)
		templates := make([][]byte, 64)
		for i := range templates {
			templates[i] = makeDistinctLeafPage(uint64(i + 1))
		}
		for i := 0; i < workingSet; i++ {
			ptrs[i] = page.LeafLogPtr{FileID: 5, Offset: uint64((i + 1) * page.PageSize)}
		}
		for _, capCase := range []struct {
			name     string
			capacity int
		}{
			{name: "cap_8192", capacity: 8192},
			{name: "cap_32768", capacity: 32768},
		} {
			b.Run(capCase.name, func(b *testing.B) {
				cache := newLeafPageReadCache(capCase.capacity)
				var dst [page.PageSize]byte
				for i := 0; i < workingSet; i++ {
					leaf := templates[i%len(templates)]
					cache.storeWrite(ptrs[i], leaf)
					_, _ = admitVerifiedCachedLeafPage(cache, ptrs[i], leaf, true, limits)
				}
				var hits, misses uint64
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := i % workingSet
					if _, _, _, hit := cache.getToWithCOWPolicy(ptrs[idx], dst[:0], true, limits); hit {
						hits++
						continue
					}
					misses++
					_, _ = admitVerifiedCachedLeafPage(cache, ptrs[idx], templates[idx%len(templates)], true, limits)
				}
				b.StopTimer()
				if total := hits + misses; total > 0 {
					b.ReportMetric(100*float64(misses)/float64(total), "miss_%")
				}
			})
		}
	})
}
