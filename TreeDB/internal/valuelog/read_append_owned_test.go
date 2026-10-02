package valuelog

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"

	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestFileReadAppend_SealedLazyMmapAndOwnedCacheAdmission(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)
	f.manager = &Manager{files: map[uint32]*File{f.ID: f}}
	f.setGroupedFrameCacheEntries(4)
	f.setGroupedFrameCacheMaxBytes(int64(8 * len(want[0])))
	got, err := f.ReadAppend(ptrs[0], true, nil)
	if err != nil || !bytes.Equal(got, want[0]) {
		t.Fatalf("owned read: %v", err)
	}
	if f.mmapReadHits.Load() != 1 || f.mmapReadFallbackReadAt.Load() != 0 {
		t.Fatalf("owned read did not select lazy mmap: hits=%d fallbacks=%d", f.mmapReadHits.Load(), f.mmapReadFallbackReadAt.Load())
	}
	st := f.ensureGroupedFrameCache().stats()
	if st.Stores != 1 || st.RetainedBytes != uint64(8*len(want[0])) || st.RetainedBytes > st.BudgetBytes {
		t.Fatalf("owned cache admission: %+v", st)
	}
	got[0] ^= 0xff
	got, err = f.ReadAppend(ptrs[0], true, nil)
	if err != nil || !bytes.Equal(got, want[0]) || f.ensureGroupedFrameCache().stats().Hits != 1 {
		t.Fatalf("cache result was not owned: err=%v", err)
	}
}

func TestGroupedFrameCache_ReadAppendOwnershipAndTemplateUnlock(t *testing.T) {
	def := templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{bytes.Repeat([]byte("a"), 16), bytes.Repeat([]byte("b"), 16)}}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := templ.EncodePayload(1, [][]byte{[]byte("left"), []byte("middle"), []byte("right")})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{nil, []byte("raw"), encoded} {
		for _, capacity := range []int{0, 128} {
			f := newTestGroupedCacheFile(1, 1024, 1024)
			raw := append([]byte("padding"), value...)
			offsets := groupedCacheOffsets(7, len(value))
			if !f.groupedFrameCacheStore(1, true, 2, offsets, raw, false) {
				t.Fatal("store")
			}
			f.templateLookup = func(uint64) ([]byte, error) {
				// Lookup may evict/recycle the encoded backing; it must run
				// after unlock with a private encoded copy.
				f.groupedFrameCache.Load().clear()
				clear(raw)
				return defBytes, nil
			}
			prefix := []byte("prefix:")
			dst := make([]byte, len(prefix), len(prefix)+capacity)
			copy(dst, prefix)
			got, _, err, hit := f.groupedFrameCacheReadAppend(1, true, 2, &offsets, uint32(len(raw)), 1, dst)
			want := append(append([]byte(nil), prefix...), value...)
			if templ.IsEncodedPayload(value) {
				want = append(append([]byte(nil), prefix...), bytes.Join([][]byte{[]byte("left"), def.Anchors[0], []byte("middle"), def.Anchors[1], []byte("right")}, nil)...)
			}
			if !hit || err != nil || !bytes.Equal(got, want) {
				t.Fatalf("append capacity=%d hit=%v err=%v got=%q want=%q", capacity, hit, err, got, want)
			}
			f.groupedFrameCache.Load().clear()
			clear(raw)
			if !bytes.Equal(got, want) {
				t.Fatal("cache backing escaped into owned append result")
			}
		}
	}
}

func TestFileReadAppend_SealedMmapBudgetFallback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	withMappedSealedBudget(t, 0)
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)
	f.manager = &Manager{files: map[uint32]*File{f.ID: f}}
	for range 2 {
		got, err := f.ReadAppend(ptrs[0], true, nil)
		if err != nil || !bytes.Equal(got, want[0]) {
			t.Fatalf("budget-denied read: %v", err)
		}
	}
	if f.mmapReadHits.Load() != 0 || f.mmapReadFallbackReadAt.Load() != 2 || f.sealedMapDeniedByCount.Load() != 1 {
		t.Fatal("mapping denial did not preserve memoized ReadAt fallback")
	}
}

func BenchmarkFileReadAppendOwned(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("mmap not supported on windows")
	}
	for _, mode := range []string{"fallback", "mmap_decode", "mmap_cache"} {
		b.Run(mode, func(b *testing.B) {
			f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(b)
			if mode != "fallback" {
				f.remapToFileSize()
			} else {
				// Prevent asynchronous remap from turning the fallback row into
				// a mapped read during warmup or measurement.
				mapped := []byte{0}
				f.mmapData.Store(mapped)
				f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))
			}
			if mode == "mmap_cache" {
				f.setGroupedFrameCacheEntries(4)
				f.setGroupedFrameCacheMaxBytes(int64(len(want) * len(want[0])))
			} else {
				f.setGroupedFrameCacheEntries(0)
			}
			// Pre-admit through the existing reusable-destination path on both
			// revisions. The cache row isolates owned-copy cost from admission.
			dst := make([]byte, 0, len(want[0]))
			for i, ptr := range ptrs {
				got, err := f.ReadAppend(ptr, true, dst[:0])
				if err != nil || !bytes.Equal(got, want[i]) {
					b.Fatalf("fixture validation: %v", err)
				}
			}
			before := f.ReadStats()
			cacheBefore := f.ensureGroupedFrameCache().stats()
			fallbackBefore := f.mmapReadFallbackReadAt.Load()
			mappedBefore := f.mmapReadHits.Load()
			if mode == "mmap_cache" && cacheBefore.RetainedBytes != uint64(len(want)*len(want[0])) {
				b.Fatalf("pre-admitted cache residency: %+v", cacheBefore)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(want[0])))
			b.ResetTimer()
			for i := range b.N {
				got, err := f.ReadAppend(ptrs[i%len(ptrs)], true, nil)
				if err != nil || len(got) != len(want[0]) {
					b.Fatalf("owned append: %v", err)
				}
			}
			b.StopTimer()
			st := f.ensureGroupedFrameCache().stats()
			wantFallback, wantMapped, wantHits := uint64(0), uint64(b.N), uint64(0)
			if mode == "fallback" {
				wantFallback, wantMapped = uint64(b.N), 0
			}
			if mode == "mmap_cache" {
				wantHits = uint64(b.N)
			}
			if f.mmapReadFallbackReadAt.Load()-fallbackBefore != wantFallback || f.mmapReadHits.Load()-mappedBefore != wantMapped || st.Hits-cacheBefore.Hits != wantHits || f.ReadStats().RecordCRCChecks-before.RecordCRCChecks != uint64(b.N) {
				b.Fatalf("row %s route mismatch", mode)
			}
			b.ReportMetric(float64(st.Hits-cacheBefore.Hits)/float64(b.N), "cache_hits/op")
			b.ReportMetric(float64(f.mmapReadFallbackReadAt.Load()-fallbackBefore)/float64(b.N), "fallbacks/op")
			b.ReportMetric(float64(f.ReadStats().RecordCRCChecks-before.RecordCRCChecks)/float64(b.N), "crc_checks/op")
			b.ReportMetric(float64(st.RetainedBytes), "retained_raw_B")
		})
	}
	b.Run("cold_open_map", func(b *testing.B) {
		fixture, ptrs, want := openGroupedCompressedFileReadFallbackFixture(b)
		for i, ptr := range ptrs {
			got, err := fixture.ReadAppend(ptr, true, nil)
			if err != nil || !bytes.Equal(got, want[i]) {
				b.Fatalf("fixture validation: %v", err)
			}
		}
		var mapped, fallback, stores uint64
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			f, err := openFile(fixture.Path, fixture.ID, nil, nil, templ.DecodeOptions{}, nil)
			if err != nil {
				b.Fatal(err)
			}
			f.manager = &Manager{files: map[uint32]*File{f.ID: f}}
			f.setGroupedFrameCacheEntries(4)
			f.setGroupedFrameCacheMaxBytes(int64(len(want) * len(want[0])))
			got, err := f.ReadAppend(ptrs[0], true, nil)
			if err != nil || len(got) != len(want[0]) {
				b.Fatalf("cold owned append: %v", err)
			}
			st := f.ensureGroupedFrameCache().stats()
			if f.mmapReadHits.Load()+f.mmapReadFallbackReadAt.Load() != 1 || st.RetainedBytes > st.BudgetBytes || f.ReadStats().RecordCRCChecks != 1 {
				b.Fatal("cold route or budget mismatch")
			}
			mapped += f.mmapReadHits.Load()
			fallback += f.mmapReadFallbackReadAt.Load()
			stores += st.Stores
			if err := f.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(mapped)/float64(b.N), "mmap_hits/op")
		b.ReportMetric(float64(fallback)/float64(b.N), "fallbacks/op")
		b.ReportMetric(float64(stores)/float64(b.N), "cache_stores/op")
	})
}

func TestFileReadAppend_MmapCacheAllocatesOnlyFinalOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	f, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)
	f.setGroupedFrameCacheEntries(4)
	f.remapToFileSize()
	if _, err := f.ReadAppend(ptrs[0], true, make([]byte, 0, len(want[0]))); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range [][]byte{nil, []byte("prefix:")} {
		allocs := testing.AllocsPerRun(100, func() {
			got, err := f.ReadAppend(ptrs[1], true, prefix)
			if err != nil || !bytes.Equal(got[:len(prefix)], prefix) || !bytes.Equal(got[len(prefix):], want[1]) {
				t.Fatalf("cached append: %v", err)
			}
		})
		if allocs != 1 {
			t.Fatalf("prefix length %d: allocations=%v want only final output allocation", len(prefix), allocs)
		}
	}
}

func TestFileRead_SealedDeadMappingCap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)
	withMaxDeadMappings(t, 1)
	readers := map[string]func(*File, page.ValuePtr) ([]byte, error){
		"owned_append": func(f *File, p page.ValuePtr) ([]byte, error) {
			return f.ReadAppend(p, true, []byte("prefix:"))
		},
		"unsafe": func(f *File, p page.ValuePtr) ([]byte, error) { return f.ReadUnsafe(p, true) },
		"unsafe_to": func(f *File, p page.ValuePtr) ([]byte, error) {
			out, _, err := f.ReadUnsafeTo(p, true, nil)
			return out, err
		},
	}
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			MaxDeadMappings = 1
			check := func(got []byte, err error, want []byte) {
				t.Helper()
				if name == "owned_append" {
					want = append([]byte("prefix:"), want...)
				}
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("read: %v", err)
				}
			}
			// An existing complete mapping remains readable at the cap.
			mapped, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)
			mapped.manager = &Manager{files: map[uint32]*File{mapped.ID: mapped}}
			mapped.remapToFileSize()
			mapped.deadMappingsCount.Store(1)
			got, err := read(mapped, ptrs[0])
			check(got, err, want[0])
			if mapped.mmapReadHits.Load() != 1 || mapped.mmapReadFallbackReadAt.Load() != 0 {
				t.Fatal("complete mapping at cap did not serve initial hit")
			}
			// A stale mapping cannot grow at the cap: one miss, one fallback.
			stale, ptrs, want := openGroupedCompressedFileReadFallbackFixture(t)
			stale.manager = &Manager{files: map[uint32]*File{stale.ID: stale}}
			stale.mmapData.Store([]byte{0})
			stale.deadMappingsCount.Store(1)
			got, err = read(stale, ptrs[0])
			check(got, err, want[0])
			if stale.mmapReadMissOutOfRange.Load() != 1 || stale.mmapReadFallbackReadAt.Load() != 1 || stale.ReadStats().RecordCRCChecks != 1 {
				t.Fatalf("capped miss retried stale mapping: out-of-range=%d fallback=%d CRC=%d", stale.mmapReadMissOutOfRange.Load(), stale.mmapReadFallbackReadAt.Load(), stale.ReadStats().RecordCRCChecks)
			}
			if stale.tryEnableSealedLazyMmap() {
				t.Fatal("capped stale mapping reported eligible")
			}
			if stale.sealedLazyMmapDenied.Load() || stale.sealedMapDeniedByCount.Load() != 0 || stale.sealedMapDeniedByBytes.Load() != 0 {
				t.Fatal("dead-mapping cap changed manager-budget denial state")
			}
			// The guard reads the live cap; increasing it permits recovery.
			MaxDeadMappings = 2
			got, err = read(stale, ptrs[0])
			check(got, err, want[0])
			if stale.mmapReadHits.Load() != 1 || stale.mmapReadFallbackReadAt.Load() != 1 {
				t.Fatal("cap increase did not recover mapped read")
			}
		})
	}
}
