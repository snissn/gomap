package mappedresource

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testKey() Key {
	return Key{Class: ClassTypedColumnAsset, Namespace: "test", Kind: "column_part", Generation: 1, PartID: 2, FileID: 3, Offset: 0, Length: 16, Checksum: 4, Version: 1, Section: Section{Kind: "column_data", Column: "c0"}}
}

func testScope() Scope {
	return Scope{Kind: ScopeSnapshot, ID: "snapshot-1", Namespace: "test", Collection: "events", Generation: 1}
}

func TestAcquireBytesReleaseUpdatesAccountingAndPins(t *testing.T) {
	mgr := NewManager()
	h, err := mgr.AcquireBytes(testKey(), testScope(), SourceHeapCopy, []byte("0123456789abcdef"), AcquireOptions{Reason: "unit", ValidationMode: ValidationVerify})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	stats := mgr.Stats()
	if stats.ActiveHandles != 1 || stats.ActiveHeapCopyBytes != 16 || stats.TotalHeapCopyBytes != 16 || stats.TotalAcquires != 1 {
		t.Fatalf("unexpected stats after acquire: %+v", stats)
	}
	if got := len(mgr.PinSummary()); got != 1 {
		t.Fatalf("PinSummary entries=%d want 1", got)
	}
	if string(h.Bytes()) != "0123456789abcdef" {
		t.Fatalf("handle bytes=%q", string(h.Bytes()))
	}
	if err := h.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := h.Bytes(); got != nil {
		t.Fatalf("released handle Bytes=%v want nil", got)
	}
	stats = mgr.Stats()
	if stats.ActiveHandles != 0 || stats.ActiveHeapCopyBytes != 0 || stats.TotalReleases != 1 {
		t.Fatalf("unexpected stats after release: %+v", stats)
	}
	if got := len(mgr.PinSummary()); got != 0 {
		t.Fatalf("PinSummary entries after release=%d want 0", got)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	if stats := mgr.Stats(); stats.TotalReleases != 1 {
		t.Fatalf("idempotent release TotalReleases=%d want 1", stats.TotalReleases)
	}
}

func TestGlobalPinSummaryAndStatsTrackActiveHandles(t *testing.T) {
	mgr := NewManager()
	key := testKey()
	h, err := mgr.AcquireBytes(key, testScope(), SourceHeapCopy, []byte("0123456789abcdef"), AcquireOptions{Reason: "global-unit", ValidationMode: ValidationVerify, ResourceRoot: "root-A", ResourcePath: "root-A/asset.bin"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	found := false
	for _, pin := range GlobalPinSummary() {
		if pin.Key.Equal(key) && pin.Reason == "global-unit" {
			if pin.Root != "root-A" || pin.Path != "root-A/asset.bin" {
				_ = h.Release()
				t.Fatalf("global pin root/path=(%q,%q), want ResourceRoot/ResourcePath", pin.Root, pin.Path)
			}
			found = true
			break
		}
	}
	if !found {
		_ = h.Release()
		t.Fatalf("global pin summary missing acquired key %+v", key)
	}
	globalStats := GlobalStats()
	if globalStats.ActiveHandles == 0 || globalStats.ActiveHeapCopyBytes < key.Length {
		_ = h.Release()
		t.Fatalf("global stats after acquire=%+v want active heap handle", globalStats)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	for _, pin := range GlobalPinSummary() {
		if pin.Key.Equal(key) && pin.Reason == "global-unit" {
			t.Fatalf("global pin summary retained released key %+v", key)
		}
	}
}

func TestMappedResourceStatsSnapshotIsDeepCopy(t *testing.T) {
	mgr := NewManager()
	h, err := mgr.AcquireBytes(testKey(), testScope(), SourceHeapCopy, []byte("0123456789abcdef"), AcquireOptions{
		Reason:         "snapshot",
		ValidationMode: ValidationCachedVerify,
		FallbackReason: FallbackReadAt,
	})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	defer func() { _ = h.Release() }()

	badKey := testKey()
	badKey.FileID = 0
	if _, err := mgr.AcquireBytes(badKey, testScope(), SourceHeapCopy, []byte("0123456789abcdef"), AcquireOptions{}); err == nil {
		t.Fatal("AcquireBytes with invalid key err=nil, want failure")
	}

	snapshot := mgr.Stats()
	if snapshot.DeniedByReason[DenyInvalidKey] != 1 || snapshot.FallbacksByReason[FallbackReadAt] != 1 || snapshot.ValidationModeReads[ValidationCachedVerify] != 1 {
		t.Fatalf("stats snapshot missing expected map counts: %+v", snapshot)
	}
	snapshot.DeniedByReason[DenyInvalidKey] = 99
	snapshot.DeniedByReason[DenyUnsupported] = 99
	snapshot.FallbacksByReason[FallbackReadAt] = 99
	snapshot.FallbacksByReason[FallbackMmapFailed] = 99
	snapshot.ValidationModeReads[ValidationCachedVerify] = 99
	snapshot.ValidationModeReads[ValidationSkipChecksum] = 99

	fresh := mgr.Stats()
	if fresh.DeniedByReason[DenyInvalidKey] != 1 || fresh.DeniedByReason[DenyUnsupported] != 0 {
		t.Fatalf("DeniedByReason was not deep-copied: %+v", fresh.DeniedByReason)
	}
	if fresh.FallbacksByReason[FallbackReadAt] != 1 || fresh.FallbacksByReason[FallbackMmapFailed] != 0 {
		t.Fatalf("FallbacksByReason was not deep-copied: %+v", fresh.FallbacksByReason)
	}
	if fresh.ValidationModeReads[ValidationCachedVerify] != 1 || fresh.ValidationModeReads[ValidationSkipChecksum] != 0 {
		t.Fatalf("ValidationModeReads was not deep-copied: %+v", fresh.ValidationModeReads)
	}
}

func TestReleaseMismatchDoesNotCorruptActiveAccounting(t *testing.T) {
	mgr := NewManager()
	h, err := mgr.AcquireBytes(testKey(), testScope(), SourceHeapCopy, []byte("0123456789abcdef"), AcquireOptions{Reason: "unit"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	mgr.release(999, SourceHeapCopy, 16, nil)
	stats := mgr.Stats()
	if stats.ActiveHandles != 1 || stats.ActiveHeapCopyBytes != 16 || stats.DeniedByReason[DenyReleaseMismatch] != 1 {
		_ = h.Release()
		t.Fatalf("release mismatch corrupted active stats: %+v", stats)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	stats = mgr.Stats()
	if stats.ActiveHandles != 0 || stats.ActiveHeapCopyBytes != 0 || stats.TotalReleases != 2 {
		t.Fatalf("stats after real release: %+v", stats)
	}
}

func TestAcquireFileRangeMappedAndHeapReturnIdenticalBytes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported in test manager on windows")
	}
	dir := t.TempDir()
	path := dir + "/asset.bin"
	payload := []byte("prefix:0123456789abcdef:suffix")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	key := testKey()
	key.Offset = int64(len("prefix:"))
	key.Length = 16
	mgr := NewManager()
	mapped, err := mgr.AcquireFileRange(key, testScope(), path, AcquireOptions{Reason: "mapped", PreferMapped: true, AllowHeapCopy: false})
	if err != nil {
		t.Fatalf("AcquireFileRange mapped: %v", err)
	}
	heap, err := mgr.AcquireFileRange(key, testScope(), path, AcquireOptions{Reason: "heap", AllowHeapCopy: true})
	if err != nil {
		_ = mapped.Release()
		t.Fatalf("AcquireFileRange heap: %v", err)
	}
	for _, pin := range mgr.PinSummary() {
		if pin.Path != path {
			_ = mapped.Release()
			_ = heap.Release()
			t.Fatalf("AcquireFileRange pin path=%q want %q", pin.Path, path)
		}
	}
	if !bytes.Equal(mapped.Bytes(), heap.Bytes()) {
		t.Fatalf("mapped bytes %q != heap bytes %q", string(mapped.Bytes()), string(heap.Bytes()))
	}
	stats := mgr.Stats()
	if stats.ActiveHandles != 2 || stats.ActiveMappedBytes != 16 || stats.ActiveHeapCopyBytes != 16 || stats.Opens != 2 || stats.Closes != 1 {
		_ = mapped.Release()
		_ = heap.Release()
		t.Fatalf("unexpected stats with mapped+heap active: %+v", stats)
	}
	if err := mapped.Release(); err != nil {
		_ = heap.Release()
		t.Fatalf("mapped Release: %v", err)
	}
	if err := heap.Release(); err != nil {
		t.Fatalf("heap Release: %v", err)
	}
	stats = mgr.Stats()
	if stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 || stats.Opens != 2 || stats.Closes != 2 {
		t.Fatalf("unexpected stats after releases: %+v", stats)
	}
}

func TestAcquireFileRangeMmapFallbackCountsReason(t *testing.T) {
	mgr := NewManager()
	key := testKey()
	key.Length = 0
	path := t.TempDir() + "/empty.bin"
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	h, err := mgr.AcquireFileRange(key, testScope(), path, AcquireOptions{PreferMapped: true, AllowHeapCopy: true})
	if err != nil {
		t.Fatalf("AcquireFileRange empty fallback: %v", err)
	}
	defer h.Release()
	if h.Source() != SourceHeapCopy {
		t.Fatalf("source=%s want heap fallback", h.Source())
	}
	stats := mgr.Stats()
	if stats.FallbackReads != 1 || stats.FallbacksByReason[FallbackMmapFailed] != 1 {
		t.Fatalf("fallback stats=%+v", stats)
	}
}

func TestAcquireFileRangeRejectsOutOfBoundsZeroLengthHeapRange(t *testing.T) {
	mgr := NewManager()
	key := testKey()
	key.Offset = 1
	key.Length = 0
	path := t.TempDir() + "/empty.bin"
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := mgr.AcquireFileRange(key, testScope(), path, AcquireOptions{AllowHeapCopy: true}); err == nil {
		t.Fatal("AcquireFileRange out-of-bounds zero-length heap range err=nil, want failure")
	}
	stats := mgr.Stats()
	if stats.DeniedByReason[DenyOutOfBounds] != 1 || stats.ActiveHandles != 0 {
		t.Fatalf("unexpected stats after out-of-bounds range: %+v", stats)
	}
}

func TestAcquireFileRangeBoundaryMatrix(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/asset.bin"
	payload := []byte("0123456789abcdef")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	tests := []struct {
		name      string
		offset    int64
		length    int64
		wantBytes []byte
		wantErr   bool
	}{
		{name: "full range", offset: 0, length: int64(len(payload)), wantBytes: payload},
		{name: "middle range", offset: 3, length: 5, wantBytes: payload[3:8]},
		{name: "zero length at eof", offset: int64(len(payload)), length: 0, wantBytes: []byte{}},
		{name: "negative offset", offset: -1, length: 1, wantErr: true},
		{name: "negative length", offset: 0, length: -1, wantErr: true},
		{name: "overflowing range", offset: maxInt64 - 1, length: 2, wantErr: true},
		{name: "zero length past eof", offset: int64(len(payload)) + 1, length: 0, wantErr: true},
		{name: "length past eof", offset: int64(len(payload)) - 1, length: 2, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewManager()
			key := testKey()
			key.Offset = tt.offset
			key.Length = tt.length
			h, err := mgr.AcquireFileRange(key, testScope(), path, AcquireOptions{Reason: tt.name, AllowHeapCopy: true})
			if tt.wantErr {
				if err == nil {
					if h != nil {
						_ = h.Release()
					}
					t.Fatal("AcquireFileRange err=nil, want failure")
				}
				assertMappedResourceNoActive(t, mgr)
				stats := mgr.Stats()
				if stats.TotalAcquires != 0 || stats.Opens != stats.Closes {
					t.Fatalf("failure leaked acquisition/open accounting: %+v", stats)
				}
				return
			}
			if err != nil {
				t.Fatalf("AcquireFileRange: %v", err)
			}
			if got := h.Bytes(); !bytes.Equal(got, tt.wantBytes) {
				_ = h.Release()
				t.Fatalf("bytes=%q want %q", got, tt.wantBytes)
			}
			if stats := mgr.Stats(); stats.ActiveHandles != 1 || stats.TotalAcquires != 1 {
				_ = h.Release()
				t.Fatalf("unexpected active stats after success: %+v", stats)
			}
			if err := h.Release(); err != nil {
				t.Fatalf("Release: %v", err)
			}
			assertMappedResourceNoActive(t, mgr)
		})
	}
}

func TestMappedResourceConcurrentAcquireReleasePinSummaryRace(t *testing.T) {
	mgr := NewManager()
	const workers = 8
	const iters = 128
	payload := []byte("0123456789abcdef")

	var done atomic.Bool
	var observer sync.WaitGroup
	observer.Add(1)
	go func() {
		defer observer.Done()
		for !done.Load() {
			_ = mgr.Stats()
			_ = mgr.PinSummary()
			runtime.Gosched()
		}
	}()

	errs := make(chan error, workers*iters)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < iters; iter++ {
				key := testKey()
				key.FileID = uint32(worker + 1)
				key.PartID = uint64(iter + 1)
				key.Checksum = uint64(worker*iters + iter + 1)
				scope := testScope()
				scope.ID = fmt.Sprintf("snapshot-%d-%d", worker, iter)
				source := SourceHeapCopy
				if iter%2 == 0 {
					source = SourceMapped
				}
				h, err := mgr.AcquireBytes(key, scope, source, payload, AcquireOptions{Reason: "concurrent", ValidationMode: ValidationVerify})
				if err != nil {
					errs <- fmt.Errorf("AcquireBytes worker=%d iter=%d: %w", worker, iter, err)
					continue
				}
				_ = mgr.Stats()
				_ = mgr.PinSummary()
				if got := h.Bytes(); len(got) != len(payload) {
					errs <- fmt.Errorf("handle bytes len=%d want %d", len(got), len(payload))
				}
				if err := h.Release(); err != nil {
					errs <- fmt.Errorf("Release worker=%d iter=%d: %w", worker, iter, err)
				}
			}
		}()
	}
	wg.Wait()
	done.Store(true)
	observer.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	stats := mgr.Stats()
	want := uint64(workers * iters)
	if stats.TotalAcquires != want || stats.TotalReleases != want || stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 {
		t.Fatalf("stats after concurrent acquire/release: %+v want acquires/releases=%d and no active bytes", stats, want)
	}
	if pins := mgr.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after concurrent acquire/release=%d want 0", len(pins))
	}
}

func TestMappedResourceReleaseConcurrentIdempotent(t *testing.T) {
	mgr := NewManager()
	releaseErr := errors.New("release failed")
	releaseEntered := make(chan struct{})
	releaseProceed := make(chan struct{})
	h := mgr.acquireRegistered(testKey(), testScope(), SourceHeapCopy, []byte("0123456789abcdef"), func() error {
		close(releaseEntered)
		<-releaseProceed
		return releaseErr
	}, AcquireOptions{Reason: "concurrent-release"})

	const callers = 16
	errs := make(chan error, callers)
	go func() { errs <- h.Release() }()
	<-releaseEntered
	for i := 1; i < callers; i++ {
		go func() { errs <- h.Release() }()
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(releaseProceed)
	}()
	for i := 0; i < callers; i++ {
		if err := <-errs; !errors.Is(err, releaseErr) {
			t.Fatalf("Release caller %d error=%v want %v", i, err, releaseErr)
		}
	}
	stats := mgr.Stats()
	if stats.TotalReleases != 1 || stats.Errors != 1 || stats.ActiveHandles != 0 || stats.ActiveHeapCopyBytes != 0 || stats.DeniedByReason[DenyReleaseMismatch] != 0 {
		t.Fatalf("stats after concurrent idempotent release: %+v", stats)
	}
	if pins := mgr.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after concurrent release=%d want 0", len(pins))
	}
}

func assertMappedResourceNoActive(t testing.TB, mgr *Manager) {
	t.Helper()
	stats := mgr.Stats()
	if stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 || stats.ActiveDerivedMetadataBytes != 0 {
		t.Fatalf("mappedresource active accounting leaked: %+v", stats)
	}
	if pins := mgr.PinSummary(); len(pins) != 0 {
		t.Fatalf("mappedresource pins leaked=%d: %+v", len(pins), pins)
	}
}

func TestFakeColumnPartSectionAcquireReleaseAndMaintenancePins(t *testing.T) {
	mgr := NewManager()
	key := Key{
		Class:      ClassTypedColumnAsset,
		Namespace:  "colparts",
		Kind:       "column_part_image",
		Generation: 10,
		PartID:     22,
		FileID:     5,
		Offset:     4096,
		Length:     32,
		Checksum:   1234,
		Version:    1,
		Encoding:   "little_endian",
		Section:    Section{Kind: "column_data", Category: "declared_columns", Column: "score", Ordinal: 7},
	}
	scope := Scope{Kind: ScopeColumnPartReader, ID: "part-reader-1", Namespace: "colparts", Collection: "events", Generation: 10, Reason: "test"}
	h, err := mgr.AcquireBytes(key, scope, SourceMapped, make([]byte, int(key.Length)), AcquireOptions{Reason: "future-column-part-section"})
	if err != nil {
		t.Fatalf("AcquireBytes fake column part: %v", err)
	}
	pins := mgr.PinSummary()
	if len(pins) != 1 {
		t.Fatalf("pins=%d want 1", len(pins))
	}
	if !pins[0].Key.Equal(key) || pins[0].Scope.Kind != ScopeColumnPartReader || pins[0].Reason != "future-column-part-section" {
		t.Fatalf("unexpected pin: %+v", pins[0])
	}
	if err := h.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := len(mgr.PinSummary()); got != 0 {
		t.Fatalf("pins after release=%d want 0", got)
	}
}
