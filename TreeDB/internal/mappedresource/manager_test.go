package mappedresource

import (
	"bytes"
	"os"
	"runtime"
	"testing"
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
