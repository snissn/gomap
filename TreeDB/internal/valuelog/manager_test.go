package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func withMappedSealedBudget(t *testing.T, maxMapped int) {
	t.Helper()
	prev := MaxMappedSealedSegments
	MaxMappedSealedSegments = maxMapped
	t.Cleanup(func() {
		MaxMappedSealedSegments = prev
	})
}

func withMappedSealedBytesBudget(t *testing.T, maxMappedBytes int64) {
	t.Helper()
	prev := MaxMappedSealedBytes
	MaxMappedSealedBytes = maxMappedBytes
	t.Cleanup(func() {
		MaxMappedSealedBytes = prev
	})
}

func TestManagerMmapReadStatsAggregatesCounters(t *testing.T) {
	mgr := &Manager{
		files: map[uint32]*File{
			1: {},
			2: {},
		},
	}
	mgr.files[1].mmapReadHits.Add(3)
	mgr.files[1].mmapReadMissOutOfRange.Add(5)
	mgr.files[1].mmapReadMissNoMapping.Add(7)
	mgr.files[1].mmapReadMissDeadMappingCap.Add(11)
	mgr.files[1].mmapReadFallbackReadAt.Add(13)
	mgr.files[2].mmapReadHits.Add(17)
	mgr.files[2].mmapReadMissOutOfRange.Add(19)
	mgr.files[2].mmapReadMissNoMapping.Add(23)
	mgr.files[2].mmapReadMissDeadMappingCap.Add(29)
	mgr.files[2].mmapReadFallbackReadAt.Add(31)

	hits, missOutOfRange, missNoMapping, missDeadCap, fallbacks := mgr.MmapReadStats()
	if hits != 20 || missOutOfRange != 24 || missNoMapping != 30 || missDeadCap != 40 || fallbacks != 44 {
		t.Fatalf("MmapReadStats mismatch: hits=%d missOutOfRange=%d missNoMapping=%d missDeadCap=%d fallbacks=%d", hits, missOutOfRange, missNoMapping, missDeadCap, fallbacks)
	}
}

func TestManagerMmapResidencyStatsAggregatesCounters(t *testing.T) {
	mgr := &Manager{
		files: map[uint32]*File{
			1: {},
			2: {},
			3: {},
		},
	}
	mgr.files[1].mmapData.Store(make([]byte, 11))
	mgr.files[1].deadMappingsCount.Store(2)
	mgr.files[1].deadMappedBytes.Store(17)
	mgr.files[1].currentWritable.Store(true)

	mgr.files[2].mmapData.Store(make([]byte, 23))
	mgr.files[2].deadMappingsCount.Store(3)
	mgr.files[2].deadMappedBytes.Store(29)

	// file 3 has no active mapping but contributes dead bytes/counters.
	mgr.files[3].mmapData.Store([]byte(nil))
	mgr.files[3].deadMappingsCount.Store(5)
	mgr.files[3].deadMappedBytes.Store(31)
	mgr.files[3].sealedMapDeniedByCount.Store(5)
	mgr.files[3].sealedMapDeniedByBytes.Store(2)

	currentSegments, currentBytes, sealedSegments, sealedBytes, deadMappings, deadBytes := mgr.MmapResidencyStats()
	if currentSegments != 1 || currentBytes != 11 || sealedSegments != 1 || sealedBytes != 23 || deadMappings != 10 || deadBytes != 77 {
		t.Fatalf("MmapResidencyStats mismatch: currentSegments=%d currentBytes=%d sealedSegments=%d sealedBytes=%d deadMappings=%d deadBytes=%d", currentSegments, currentBytes, sealedSegments, sealedBytes, deadMappings, deadBytes)
	}
	deniedByCount, deniedByBytes := mgr.SealedMapDeniedByReasonStats()
	if deniedByCount != 5 || deniedByBytes != 2 {
		t.Fatalf("SealedMapDeniedByReasonStats count=%d bytes=%d want count=5 bytes=2", deniedByCount, deniedByBytes)
	}
	if got := mgr.SealedMapDeniedStats(); got != 7 {
		t.Fatalf("SealedMapDeniedStats=%d want 7", got)
	}
}

func TestManagerPromoteCurrentWritable_SwitchesPriorLaneSegmentToSealed(t *testing.T) {
	mgr := &Manager{
		files:                 make(map[uint32]*File),
		currentWritableByLane: make(map[uint32]uint32),
	}
	f1 := &File{ID: mustEncodeFileID(t, 3, 1)}
	f2 := &File{ID: mustEncodeFileID(t, 3, 2)}
	mgr.files[f1.ID] = f1
	mgr.files[f2.ID] = f2

	if err := mgr.PromoteCurrentWritable(f1.ID); err != nil {
		t.Fatalf("PromoteCurrentWritable(first): %v", err)
	}
	if !f1.currentWritable.Load() {
		t.Fatalf("first file not marked current writable")
	}

	if err := mgr.PromoteCurrentWritable(f2.ID); err != nil {
		t.Fatalf("PromoteCurrentWritable(second): %v", err)
	}
	if f1.currentWritable.Load() {
		t.Fatalf("first file still marked current writable after promotion")
	}
	if !f2.currentWritable.Load() {
		t.Fatalf("second file not marked current writable")
	}
}

func TestFileRead_CountsDeadMappingCapFallback(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("alpha")); err != nil {
		_ = writer.Close()
		t.Fatalf("Append(alpha): %v", err)
	}
	ptr, err := writer.Append(0, nil, 2, []byte("beta"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("Append(beta): %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	fh, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer func() { _ = fh.Close() }()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	truncated := contents[:int(ptr.Offset)-1]

	f := &File{ID: fileID, Path: path, File: fh}
	f.mmapData.Store(truncated)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(truncated))))

	got, err := f.Read(ptr, false)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != "beta" {
		t.Fatalf("Read mismatch: got=%q want=%q", string(got), "beta")
	}
	if got := f.mmapReadMissDeadMappingCap.Load(); got != 1 {
		t.Fatalf("mmapReadMissDeadMappingCap=%d want 1", got)
	}
	if got := f.mmapReadFallbackReadAt.Load(); got != 1 {
		t.Fatalf("mmapReadFallbackReadAt=%d want 1", got)
	}
}

func TestFileReadAppend_CountsGroupedFallbackReadAt(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptrs, err := writer.AppendFrame(0, nil, []Record{
		{RID: 1, Value: []byte("alpha")},
		{RID: 2, Value: []byte("beta")},
	})
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	fh, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer func() { _ = fh.Close() }()

	f := &File{ID: fileID, Path: path, File: fh}
	// Force ReadAppend to take the grouped fallback read path without spawning
	// an async remap goroutine that can race with test file close in -race CI.
	mapped := []byte{0}
	f.mmapData.Store(mapped)
	f.deadMappingsCount.Store(uint64(effectiveMaxDeadMappings(len(mapped))))

	got, err := f.ReadAppend(ptrs[1], false, nil)
	if err != nil {
		t.Fatalf("ReadAppend: %v", err)
	}
	if string(got) != "beta" {
		t.Fatalf("ReadAppend mismatch: got=%q want=%q", string(got), "beta")
	}
	if got := f.mmapReadFallbackReadAt.Load(); got != 1 {
		t.Fatalf("mmapReadFallbackReadAt=%d want 1", got)
	}
}

func TestOpenFile_DoesNotEagerlyMap(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, []byte("alpha")); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	f, err := openFile(path, fileID, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	if data, _ := f.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected no eager mmap on open, mapped bytes=%d", len(data))
	}
	if got := f.remapCount.Load(); got != 0 {
		t.Fatalf("expected no eager remap on open, remapCount=%d", got)
	}
}

func TestReadUnsafe_SealedLazyMmapBudgetFallsBackToReadAt(t *testing.T) {
	withMappedSealedBudget(t, 1)
	withMappedSealedBytesBudget(t, 1<<30)

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, 0, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, 1, 1, 2, bytes.Repeat([]byte("b"), 64))

	f1, err := openFile(filepath.Join(dir, "value-l0-000001.log"), id1, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile(f1): %v", err)
	}
	defer func() { _ = f1.Close() }()

	f2, err := openFile(filepath.Join(dir, "value-l1-000001.log"), id2, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile(f2): %v", err)
	}
	defer func() { _ = f2.Close() }()

	mgr := &Manager{
		files:                 map[uint32]*File{id1: f1, id2: f2},
		currentWritableByLane: make(map[uint32]uint32),
	}
	f1.manager = mgr
	f2.manager = mgr

	set := mgr.CurrentSetNoRefresh()
	defer func() { _ = mgr.Release(set) }()

	got1, err := set.ReadUnsafe(ptr1)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr1): %v", err)
	}
	if !bytes.Equal(got1, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ptr1 mismatch")
	}
	if data, _ := f1.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected first sealed segment to consume the lazy mmap budget")
	}

	got2, err := set.ReadUnsafe(ptr2)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr2): %v", err)
	}
	if !bytes.Equal(got2, bytes.Repeat([]byte("b"), 64)) {
		t.Fatalf("ptr2 mismatch")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected second sealed segment to stay unmapped once budget was exhausted")
	}
	if got := f2.mmapReadFallbackReadAt.Load(); got == 0 {
		t.Fatalf("expected fallback ReadAt on second sealed segment")
	}
	if got := f2.sealedMapDeniedByCount.Load(); got == 0 {
		t.Fatalf("expected count-cap deny counter to increment")
	}

	got2b, err := set.ReadUnsafe(ptr2)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr2 second): %v", err)
	}
	if !bytes.Equal(got2b, bytes.Repeat([]byte("b"), 64)) {
		t.Fatalf("ptr2 second mismatch")
	}
	if got := f2.sealedMapDeniedByCount.Load(); got != 1 {
		t.Fatalf("expected sealed lazy-mmap deny to be memoized; got count-deny=%d want 1", got)
	}
	if got := f2.mmapReadFallbackReadAt.Load(); got < 2 {
		t.Fatalf("expected repeated fallback ReadAt after memoized deny, got=%d", got)
	}
}

func TestReadUnsafe_SealedLazyMmapByteBudgetFallsBackToReadAt(t *testing.T) {
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, 0, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, 1, 1, 2, bytes.Repeat([]byte("b"), 64))

	f1, err := openFile(filepath.Join(dir, "value-l0-000001.log"), id1, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile(f1): %v", err)
	}
	defer func() { _ = f1.Close() }()

	f2, err := openFile(filepath.Join(dir, "value-l1-000001.log"), id2, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile(f2): %v", err)
	}
	defer func() { _ = f2.Close() }()

	mgr := &Manager{
		files:                 map[uint32]*File{id1: f1, id2: f2},
		currentWritableByLane: make(map[uint32]uint32),
	}
	f1.manager = mgr
	f2.manager = mgr

	set := mgr.CurrentSetNoRefresh()
	defer func() { _ = mgr.Release(set) }()

	got1, err := set.ReadUnsafe(ptr1)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr1): %v", err)
	}
	if !bytes.Equal(got1, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ptr1 mismatch")
	}
	mapped1, _ := f1.mmapData.Load().([]byte)
	if len(mapped1) == 0 {
		t.Fatalf("expected first sealed segment to be mapped")
	}
	// Constrain sealed mmap bytes to exactly the current mapped bytes so the
	// next sealed segment must use ReadAt.
	MaxMappedSealedBytes = int64(len(mapped1))

	got2, err := set.ReadUnsafe(ptr2)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr2): %v", err)
	}
	if !bytes.Equal(got2, bytes.Repeat([]byte("b"), 64)) {
		t.Fatalf("ptr2 mismatch")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected second sealed segment to stay unmapped once byte budget was exhausted")
	}
	if got := f2.mmapReadFallbackReadAt.Load(); got == 0 {
		t.Fatalf("expected fallback ReadAt on second sealed segment")
	}
	if got := f2.sealedMapDeniedByBytes.Load(); got == 0 {
		t.Fatalf("expected byte-cap deny counter to increment")
	}
	if byCount, byBytes := mgr.SealedMapDeniedByReasonStats(); byCount != 0 || byBytes == 0 {
		t.Fatalf("expected byte-cap deny aggregation, got count=%d bytes=%d", byCount, byBytes)
	}
}

func TestReadUnsafe_SealedMappedOutOfRangeRemapsToKnownFileSize(t *testing.T) {
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, []byte("alpha")); err != nil {
		_ = w.Close()
		t.Fatalf("Append(alpha): %v", err)
	}
	ptr, err := w.Append(0, nil, 2, bytes.Repeat([]byte("b"), 64))
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append(beta): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	f, err := openFile(path, fileID, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	mgr := &Manager{
		files:                 map[uint32]*File{fileID: f},
		currentWritableByLane: make(map[uint32]uint32),
	}
	f.manager = mgr

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	stale := raw[:int(ptr.Offset)-1]
	f.mmapData.Store(stale)
	f.fileSize.Store(int64(len(raw)))

	set := mgr.CurrentSetNoRefresh()
	defer func() { _ = mgr.Release(set) }()

	got, err := set.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	want := bytes.Repeat([]byte("b"), 64)
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadUnsafe mismatch: got=%q want=%q", string(got), string(want))
	}
	if got := f.remapCount.Load(); got == 0 {
		t.Fatalf("expected remapCount > 0 for stale sealed mapping")
	}
	if got := f.mmapReadFallbackReadAt.Load(); got != 0 {
		t.Fatalf("expected no ReadAt fallback after sealed remap, got=%d", got)
	}
}

func mustEncodeFileID(t *testing.T, lane, seq uint32) uint32 {
	t.Helper()
	id, err := EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	return id
}

func writeTestSegmentWithPtr(t *testing.T, dir string, lane, seq uint32, rid uint64, value []byte) (uint32, page.ValuePtr) {
	t.Helper()

	fileID, err := EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	defer func() { _ = w.Close() }()

	ptr, err := w.Append(0, nil, rid, value)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return fileID, ptr
}

func writeTestSegment(t *testing.T, dir string, lane, seq uint32, rid uint64, value []byte) uint32 {
	t.Helper()

	fileID, err := EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Append(0, nil, rid, value); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return fileID
}

func TestManagerCurrentSetNoRefresh(t *testing.T) {
	dir := t.TempDir()
	seg1 := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("a"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()

	set1 := mgr.CurrentSetNoRefresh()
	if _, ok := set1.Files[seg1]; !ok {
		t.Fatalf("initial CurrentSetNoRefresh missing segment %d", seg1)
	}
	if err := mgr.Release(set1); err != nil {
		t.Fatalf("Release(set1): %v", err)
	}

	seg2 := writeTestSegment(t, dir, 0, 2, 2, bytes.Repeat([]byte("b"), 64))

	// No-refresh snapshots should not discover newly-created segments on disk.
	set2 := mgr.CurrentSetNoRefresh()
	if _, ok := set2.Files[seg2]; ok {
		t.Fatalf("CurrentSetNoRefresh unexpectedly discovered new segment %d", seg2)
	}
	if err := mgr.Release(set2); err != nil {
		t.Fatalf("Release(set2): %v", err)
	}

	// Refreshing snapshots should discover them.
	set3 := mgr.CurrentSet()
	if _, ok := set3.Files[seg2]; !ok {
		t.Fatalf("CurrentSet missing refreshed segment %d", seg2)
	}
	if err := mgr.Release(set3); err != nil {
		t.Fatalf("Release(set3): %v", err)
	}
}

func TestManagerRegisterSegment_CurrentSetNoRefresh(t *testing.T) {
	dir := t.TempDir()

	origScan := currentScanSegmentPaths()
	scanCalls := 0
	swapScanSegmentPathsForTest(func(scanDir string) ([]segmentInfo, error) {
		scanCalls++
		return origScan(scanDir)
	})
	t.Cleanup(func() { swapScanSegmentPathsForTest(origScan) })

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()
	if scanCalls != 1 {
		t.Fatalf("scan calls after open=%d want 1", scanCalls)
	}

	segID := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("r"), 64))
	path := filepath.Join(dir, "value-l0-000001.log")
	if err := mgr.RegisterSegment(path, segID); err != nil {
		t.Fatalf("RegisterSegment: %v", err)
	}
	if scanCalls != 1 {
		t.Fatalf("RegisterSegment unexpectedly scanned filesystem, calls=%d", scanCalls)
	}

	set := mgr.CurrentSetNoRefresh()
	if _, ok := set.Files[segID]; !ok {
		_ = mgr.Release(set)
		t.Fatalf("CurrentSetNoRefresh missing registered segment %d", segID)
	}
	if err := mgr.Release(set); err != nil {
		t.Fatalf("Release(set): %v", err)
	}
	if scanCalls != 1 {
		t.Fatalf("CurrentSetNoRefresh unexpectedly scanned filesystem, calls=%d", scanCalls)
	}
}

func TestManagerReleaseZombieDeletesSegmentOnSuccess(t *testing.T) {
	dir := t.TempDir()
	segID := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("x"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()

	set := mgr.CurrentSet()
	f, ok := set.Files[segID]
	if !ok {
		_ = mgr.Release(set)
		t.Fatalf("CurrentSet missing segment %d", segID)
	}
	path := f.Path
	if err := mgr.MarkZombie(segID); err != nil {
		_ = mgr.Release(set)
		t.Fatalf("MarkZombie(%d): %v", segID, err)
	}
	if err := mgr.Release(set); err != nil {
		t.Fatalf("Release(set): %v", err)
	}

	mgr.mu.RLock()
	_, stillTracked := mgr.files[segID]
	mgr.mu.RUnlock()
	if stillTracked {
		t.Fatalf("zombie segment %d still tracked after successful delete", segID)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected deleted segment file, stat err=%v", err)
	}
}

func TestManagerReleaseZombieDeleteFailureKeepsSegmentTracked(t *testing.T) {
	dir := t.TempDir()
	segID := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("y"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()

	set := mgr.CurrentSet()
	f, ok := set.Files[segID]
	if !ok {
		_ = mgr.Release(set)
		t.Fatalf("CurrentSet missing segment %d", segID)
	}
	path := f.Path
	if err := mgr.MarkZombie(segID); err != nil {
		_ = mgr.Release(set)
		t.Fatalf("MarkZombie(%d): %v", segID, err)
	}

	origRemove := removeSegmentPath
	removeSegmentPath = func(string) error { return errors.New("remove failed") }
	t.Cleanup(func() { removeSegmentPath = origRemove })

	if err := mgr.Release(set); err == nil {
		t.Fatalf("Release(set) err=nil, want remove failure")
	}

	mgr.mu.RLock()
	tracked, stillTracked := mgr.files[segID]
	mgr.mu.RUnlock()
	if !stillTracked {
		t.Fatalf("segment %d unexpectedly untracked after failed delete", segID)
	}
	if !tracked.IsZombie.Load() {
		t.Fatalf("segment %d should remain zombie after failed delete", segID)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected segment file to remain on disk, stat err=%v", err)
	}
}

func TestManagerRefresh_IgnoresSegmentRemovedAfterScan(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()

	segID := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("z"), 64))

	origOpen := currentOpenSegmentFile()
	swapOpenSegmentFileForTest(func(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error) {
		if id == segID {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Remove(%s): %v", path, err)
			}
		}
		return origOpen(path, id, dictLookup, templateLookup, templateOpts, templateCache)
	})
	t.Cleanup(func() { swapOpenSegmentFileForTest(origOpen) })

	if err := mgr.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	mgr.mu.RLock()
	_, tracked := mgr.files[segID]
	mgr.mu.RUnlock()
	if tracked {
		t.Fatalf("segment %d should not be tracked after disappearing during refresh", segID)
	}
	if _, err := os.Stat(filepath.Join(dir, "value-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("expected segment to be removed, stat err=%v", err)
	}
}

func TestManagerRegisterSegment_ReinitializesNilFilesMap(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()

	mgr.mu.Lock()
	mgr.files = nil
	mgr.mu.Unlock()

	segID := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("m"), 64))

	path := filepath.Join(dir, "value-l0-000001.log")
	if err := mgr.RegisterSegment(path, segID); err != nil {
		t.Fatalf("RegisterSegment: %v", err)
	}

	mgr.mu.RLock()
	_, ok := mgr.files[segID]
	mgr.mu.RUnlock()
	if !ok {
		t.Fatalf("segment %d missing after RegisterSegment reinitialized files map", segID)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mgr = nil
}
