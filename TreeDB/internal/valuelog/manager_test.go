package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

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

func withMappedLeafSealedBudget(t *testing.T, maxMapped int) {
	t.Helper()
	prev := MaxMappedLeafSealedSegments
	MaxMappedLeafSealedSegments = maxMapped
	t.Cleanup(func() {
		MaxMappedLeafSealedSegments = prev
	})
}

func withMappedLeafSealedBytesBudget(t *testing.T, maxMappedBytes int64) {
	t.Helper()
	prev := MaxMappedLeafSealedBytes
	MaxMappedLeafSealedBytes = maxMappedBytes
	t.Cleanup(func() {
		MaxMappedLeafSealedBytes = prev
	})
}

func TestManagerRemoveSegmentIfUnpinnedNil(t *testing.T) {
	var mgr *Manager
	removed, err := mgr.RemoveSegmentIfUnpinned(1)
	if err != nil {
		t.Fatalf("RemoveSegmentIfUnpinned nil manager err=%v", err)
	}
	if removed {
		t.Fatal("RemoveSegmentIfUnpinned nil manager removed=true")
	}
}

func waitForRemapIdle(t *testing.T, files ...*File) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		allIdle := true
		for _, f := range files {
			if f == nil {
				continue
			}
			if f.remapRequested.Load() {
				allIdle = false
				break
			}
			if !f.remapMu.TryLock() {
				allIdle = false
				break
			}
			f.remapMu.Unlock()
			if f.remapRequested.Load() {
				allIdle = false
				break
			}
		}
		if allIdle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for remap goroutine to drain")
		}
		runtime.Gosched()
	}
}

func withMaxDeadMappings(t *testing.T, max int) {
	t.Helper()
	prev := MaxDeadMappings
	MaxDeadMappings = max
	t.Cleanup(func() {
		MaxDeadMappings = prev
	})
}

func withCurrentWritableMmapTargetBytes(t *testing.T, target int64) {
	t.Helper()
	prev := CurrentWritableMmapTargetBytes
	CurrentWritableMmapTargetBytes = target
	t.Cleanup(func() {
		CurrentWritableMmapTargetBytes = prev
	})
}

func TestDefaultLeafMmapPolicyBudget(t *testing.T) {
	withMappedLeafSealedBudget(t, defaultMaxMappedLeafSealed)
	withMappedLeafSealedBytesBudget(t, defaultMaxMappedLeafSealedBytes)

	if got, want := MaxMappedLeafSealedSegments, 512; got != want {
		t.Fatalf("MaxMappedLeafSealedSegments=%d want %d", got, want)
	}
	if got, want := MaxMappedLeafSealedBytes, int64(1536<<20); got != want {
		t.Fatalf("MaxMappedLeafSealedBytes=%d want %d", got, want)
	}
	if enableCurrentLeafWritableMmap {
		t.Fatalf("enableCurrentLeafWritableMmap=true, want false by default")
	}
}

func TestReadUnsafe_CurrentLeafWritableMmapDisabledFallsBackToReadAt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = false
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id, ptr := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	if err := mgr.PromoteCurrentWritable(id); err != nil {
		t.Fatalf("PromoteCurrentWritable: %v", err)
	}
	f := mgr.files[id]
	files = []*File{f}

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe current leaf: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ReadUnsafe current leaf mismatch: got=%q", string(got))
	}
	if data, _ := f.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("current mutable leaf segment was mmapped, len=%d", len(data))
	}
	if got := f.mmapReadFallbackReadAt.Load(); got == 0 {
		t.Fatalf("expected current mutable leaf read to use ReadAt fallback when current mmap is disabled")
	}
}

func TestReadUnsafe_DefaultSealedLeafBudgetUsesMmapWithCurrentMmapDisabled(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	withMappedLeafSealedBudget(t, defaultMaxMappedLeafSealed)
	withMappedLeafSealedBytesBudget(t, defaultMaxMappedLeafSealedBytes)

	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = false
	t.Cleanup(func() {
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	set := mgr.CurrentSetNoRefresh()
	defer func() { _ = mgr.Release(set) }()

	for _, tc := range []struct {
		name string
		id   uint32
		ptr  page.ValuePtr
		want []byte
	}{
		{name: "first", id: id1, ptr: ptr1, want: bytes.Repeat([]byte("a"), 64)},
		{name: "second", id: id2, ptr: ptr2, want: bytes.Repeat([]byte("b"), 64)},
	} {
		got, err := set.ReadUnsafe(tc.ptr)
		if err != nil {
			t.Fatalf("ReadUnsafe(%s): %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("ReadUnsafe(%s) mismatch", tc.name)
		}
		f := mgr.files[tc.id]
		if data, _ := f.mmapData.Load().([]byte); len(data) == 0 {
			t.Fatalf("expected sealed leaf segment %s to mmap under default sealed budget", tc.name)
		}
		if got := f.mmapReadFallbackReadAt.Load(); got != 0 {
			t.Fatalf("expected sealed leaf segment %s to avoid ReadAt fallback, got=%d", tc.name, got)
		}
	}
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

func TestManagerZombieStatsAggregatesPinnedAndUnpinned(t *testing.T) {
	mgr := &Manager{
		files: map[uint32]*File{
			1: {},
			2: {},
			3: {},
		},
	}
	// Zombie + pinned.
	mgr.files[1].IsZombie.Store(true)
	mgr.files[1].RefCount.Store(2)
	mgr.files[1].fileSize.Store(100)
	// Zombie + unpinned.
	mgr.files[2].IsZombie.Store(true)
	mgr.files[2].RefCount.Store(0)
	mgr.files[2].fileSize.Store(200)
	// Non-zombie should be ignored.
	mgr.files[3].RefCount.Store(9)
	mgr.files[3].fileSize.Store(300)

	segments, bytes, pinnedSegments, pinnedBytes, unpinnedSegments, unpinnedBytes := mgr.ZombieStats()
	if segments != 2 || bytes != 300 {
		t.Fatalf("ZombieStats total mismatch: segments=%d bytes=%d want segments=2 bytes=300", segments, bytes)
	}
	if pinnedSegments != 1 || pinnedBytes != 100 {
		t.Fatalf("ZombieStats pinned mismatch: segments=%d bytes=%d want segments=1 bytes=100", pinnedSegments, pinnedBytes)
	}
	if unpinnedSegments != 1 || unpinnedBytes != 200 {
		t.Fatalf("ZombieStats unpinned mismatch: segments=%d bytes=%d want segments=1 bytes=200", unpinnedSegments, unpinnedBytes)
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

func TestManagerPromoteCurrentWritable_AllowsMultiCurrentWritableLane(t *testing.T) {
	mgr := &Manager{
		files:                 make(map[uint32]*File),
		currentWritableByLane: make(map[uint32]uint32),
	}
	mgr.SetMultiCurrentWritableLane(ReservedLeafLogLaneID, true)
	f1 := &File{ID: mustEncodeFileID(t, ReservedLeafLogLaneID, 1)}
	f2 := &File{ID: mustEncodeFileID(t, ReservedLeafLogLaneID, 2)}
	mgr.files[f1.ID] = f1
	mgr.files[f2.ID] = f2

	if err := mgr.PromoteCurrentWritable(f1.ID); err != nil {
		t.Fatalf("PromoteCurrentWritable(first): %v", err)
	}
	if err := mgr.PromoteCurrentWritable(f2.ID); err != nil {
		t.Fatalf("PromoteCurrentWritable(second): %v", err)
	}
	if !f1.currentWritable.Load() {
		t.Fatalf("first file was sealed despite multi-current lane")
	}
	if !f2.currentWritable.Load() {
		t.Fatalf("second file not marked current writable")
	}
	gotIDs := mgr.CurrentWritableFileIDs()
	got := make(map[uint32]struct{}, len(gotIDs))
	for _, id := range gotIDs {
		got[id] = struct{}{}
	}
	for _, id := range []uint32{f1.ID, f2.ID} {
		if _, ok := got[id]; !ok {
			t.Fatalf("CurrentWritableFileIDs missing %d; got %v", id, gotIDs)
		}
	}
}

func TestManagerPromoteCurrentWritableReplacing_DemotesPriorMultiCurrentWriter(t *testing.T) {
	mgr := &Manager{
		files:                 make(map[uint32]*File),
		currentWritableByLane: make(map[uint32]uint32),
	}
	mgr.SetMultiCurrentWritableLane(ReservedLeafLogLaneID, true)
	f1 := &File{ID: mustEncodeFileID(t, ReservedLeafLogLaneID, 1)}
	f2 := &File{ID: mustEncodeFileID(t, ReservedLeafLogLaneID, 2)}
	mgr.files[f1.ID] = f1
	mgr.files[f2.ID] = f2

	if err := mgr.PromoteCurrentWritable(f1.ID); err != nil {
		t.Fatalf("PromoteCurrentWritable(first): %v", err)
	}
	if err := mgr.PromoteCurrentWritableReplacing(f2.ID, f1.ID); err != nil {
		t.Fatalf("PromoteCurrentWritableReplacing(second): %v", err)
	}
	if f1.currentWritable.Load() {
		t.Fatalf("first file still current writable after replacement")
	}
	if !f2.currentWritable.Load() {
		t.Fatalf("second file not marked current writable")
	}
	gotIDs := mgr.CurrentWritableFileIDs()
	if len(gotIDs) != 1 || gotIDs[0] != f2.ID {
		t.Fatalf("CurrentWritableFileIDs=%v want [%d]", gotIDs, f2.ID)
	}
}

func TestManagerSetCurrentWritableReadBarrier_NilClearsCallback(t *testing.T) {
	mgr := &Manager{}
	calls := 0
	mgr.SetCurrentWritableReadBarrier(func(fileID uint32) error {
		calls++
		if fileID != 9 {
			t.Fatalf("barrier fileID=%d want 9", fileID)
		}
		return nil
	})
	barrier := mgr.currentWritableBarrier()
	if barrier == nil {
		t.Fatalf("expected installed barrier")
	}
	if _, err := barrier(9); err != nil {
		t.Fatalf("barrier(9): %v", err)
	}
	if calls != 1 {
		t.Fatalf("barrier calls=%d want 1", calls)
	}

	mgr.SetCurrentWritableReadBarrier(nil)
	if barrier := mgr.currentWritableBarrier(); barrier != nil {
		t.Fatalf("expected nil barrier after clear")
	}
}

func TestManagerCurrentWritableSizedReadBarrier_UpdatesFileSizeHint(t *testing.T) {
	mgr := &Manager{}
	f := &File{ID: 9, manager: mgr}
	f.currentWritable.Store(true)

	mgr.SetCurrentWritableReadBarrierWithSize(func(fileID uint32) (int64, error) {
		if fileID != f.ID {
			t.Fatalf("barrier fileID=%d want %d", fileID, f.ID)
		}
		return 1234, nil
	})

	if err := f.ensureCurrentWritableReadable(); err != nil {
		t.Fatalf("ensureCurrentWritableReadable: %v", err)
	}
	if got := f.fileSize.Load(); got != 1234 {
		t.Fatalf("fileSize hint=%d want 1234", got)
	}
}

func TestManagerCurrentWritableSizedReadBarrier_AvoidsStatForMappedRange(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "segment-*.log")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mgr := &Manager{}
	f := &File{ID: 9, File: tmp, manager: mgr}
	f.currentWritable.Store(true)
	mapped := make([]byte, 128)
	f.mmapData.Store(mapped)
	mgr.SetCurrentWritableReadBarrierWithSize(func(fileID uint32) (int64, error) {
		if fileID != f.ID {
			t.Fatalf("barrier fileID=%d want %d", fileID, f.ID)
		}
		return int64(len(mapped)), nil
	})

	if err := f.ensureCurrentWritableReadable(); err != nil {
		t.Fatalf("ensureCurrentWritableReadable: %v", err)
	}
	if _, ok := f.ensureMmapRangeReadable(mapped, 0, 64); !ok {
		t.Fatalf("mapped range should be readable from size hint without stat")
	}
}

func TestManagerCurrentWritableReadBarrierSkippedForVerifiedRecord(t *testing.T) {
	mgr := &Manager{}
	f := &File{ID: 9, manager: mgr}
	f.currentWritable.Store(true)
	ptr := testCurrentWritableRecordPtr(64)
	f.noteVerifiedFileSize(int64(ptr.Offset + uint64(page.ValuePtrRecordLength(ptr))))
	var calls int
	mgr.SetCurrentWritableReadBarrierWithSize(func(fileID uint32) (int64, error) {
		calls++
		return -1, nil
	})

	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		t.Fatalf("ensureCurrentWritableReadableFor: %v", err)
	}
	if calls != 0 {
		t.Fatalf("barrier calls=%d want 0 for verified readable record", calls)
	}
}

func TestManagerCurrentWritableReadBarrierUsedForUnverifiedRecord(t *testing.T) {
	mgr := &Manager{}
	f := &File{ID: 9, manager: mgr}
	f.currentWritable.Store(true)
	ptr := testCurrentWritableRecordPtr(64)
	f.noteVerifiedFileSize(int64(ptr.Offset + uint64(page.ValuePtrRecordLength(ptr)) - 1))
	var calls int
	mgr.SetCurrentWritableReadBarrierWithSize(func(fileID uint32) (int64, error) {
		if fileID != f.ID {
			t.Fatalf("barrier fileID=%d want %d", fileID, f.ID)
		}
		calls++
		return int64(ptr.Offset + uint64(page.ValuePtrRecordLength(ptr))), nil
	})

	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		t.Fatalf("ensureCurrentWritableReadableFor: %v", err)
	}
	if calls != 1 {
		t.Fatalf("barrier calls=%d want 1 for unverified record", calls)
	}
}

func TestFileEnsureMmapRecordInitialRangeRequiresHeaderForSmallHint(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "segment-*.log")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := tmp.Write(make([]byte, HeaderSize-1)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	file, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = file.Close() }()

	data := make([]byte, HeaderSize-1)
	f := &File{ID: 9, File: file}
	f.mmapData.Store(data)
	f.noteVerifiedFileSize(int64(len(data)))
	ptr := page.ValuePtr{
		Offset: valueLogRecordCRCPrefixBytes,
		Length: 1,
	}

	if _, ok, _ := f.ensureMmapRecordInitialRange(data, ptr, int64(ptr.Offset-4)); ok {
		t.Fatalf("small nonzero hint should not make a partial header readable")
	}
}

func testCurrentWritableRecordPtr(payloadLen uint32) page.ValuePtr {
	return page.ValuePtr{
		Offset: valueLogRecordCRCPrefixBytes,
		Length: uint32(headerWithoutCRC) + payloadLen,
	}
}

func TestManagerReadUnsafe_CurrentWritableFallsBackWithoutPersistentMmap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	prev := enableCurrentWritableMmap
	enableCurrentWritableMmap = false
	t.Cleanup(func() {
		enableCurrentWritableMmap = prev
	})

	dir := t.TempDir()
	fileID := mustEncodeFileID(t, 0, 1)
	path := filepath.Join(dir, "value-l0-000001.log")

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	want := bytes.Repeat([]byte("abc"), 1024)
	ptr, err := w.Append(0, nil, 1, want)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if err := mgr.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("RegisterSegment: %v", err)
	}
	if err := mgr.PromoteCurrentWritable(fileID); err != nil {
		t.Fatalf("PromoteCurrentWritable: %v", err)
	}

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadUnsafe mismatch")
	}

	f := mgr.files[fileID]
	if data, _ := f.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected no current-writable mmap, got len=%d", len(data))
	}
	if remaps := f.remapCount.Load(); remaps != 0 {
		t.Fatalf("expected no remaps, got=%d", remaps)
	}
	if deadMappings := f.deadMappingsCount.Load(); deadMappings != 0 {
		t.Fatalf("expected no dead mappings, got=%d", deadMappings)
	}
	currentSegs, currentBytes, sealedSegs, sealedBytes, deadMappings, deadBytes := mgr.MmapResidencyStats()
	if currentSegs != 0 || currentBytes != 0 || sealedSegs != 0 || sealedBytes != 0 || deadMappings != 0 || deadBytes != 0 {
		t.Fatalf("unexpected mmap residency currentSegs=%d currentBytes=%d sealedSegs=%d sealedBytes=%d deadMappings=%d deadBytes=%d", currentSegs, currentBytes, sealedSegs, sealedBytes, deadMappings, deadBytes)
	}
	hits, _, _, _, fallbacks := mgr.MmapReadStats()
	if hits != 0 || fallbacks == 0 {
		t.Fatalf("expected mmap miss + fallback, hits=%d fallbacks=%d", hits, fallbacks)
	}
}

func TestManagerReadUnsafe_CurrentWritableLeafUsesPersistentMmapByDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var file *File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	t.Cleanup(func() {
		waitForRemapIdle(t, file)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	fileID := mustEncodeFileID(t, ReservedLeafLogLaneID, 1)
	path := segmentPathForID(dir, fileID)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	want := bytes.Repeat([]byte("leaf"), 1024)
	ptr, err := w.Append(0, nil, 1, want)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if err := mgr.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("RegisterSegment: %v", err)
	}
	if err := mgr.PromoteCurrentWritable(fileID); err != nil {
		t.Fatalf("PromoteCurrentWritable: %v", err)
	}

	got, err := mgr.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadUnsafe mismatch")
	}

	f := mgr.files[fileID]
	file = f
	data, _ := f.mmapData.Load().([]byte)
	if len(data) == 0 {
		t.Fatalf("expected current leaf segment to install persistent mmap")
	}
	if remaps := f.remapCount.Load(); remaps == 0 {
		t.Fatalf("expected current leaf segment remapCount > 0")
	}
	currentSegs, currentBytes, sealedSegs, sealedBytes, deadMappings, deadBytes := mgr.MmapResidencyStats()
	if currentSegs != 1 || currentBytes == 0 {
		t.Fatalf("expected one mapped current leaf segment, currentSegs=%d currentBytes=%d", currentSegs, currentBytes)
	}
	if sealedSegs != 0 || sealedBytes != 0 || deadMappings != 0 || deadBytes != 0 {
		t.Fatalf("unexpected non-current residency sealedSegs=%d sealedBytes=%d deadMappings=%d deadBytes=%d", sealedSegs, sealedBytes, deadMappings, deadBytes)
	}
	hits, _, _, _, fallbacks := mgr.MmapReadStats()
	if hits == 0 || fallbacks != 0 {
		t.Fatalf("expected mmap hit without fallback, hits=%d fallbacks=%d", hits, fallbacks)
	}
}

func TestManagerPromoteCurrentWritable_RetiresLeafCurrentMmapBeforeSealedFallback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMappedLeafSealedBudget(t, 0)
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, _ := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	path1 := segmentPathForID(dir, id1)
	path2 := segmentPathForID(dir, id2)
	if err := mgr.RegisterSegment(path1, id1); err != nil {
		t.Fatalf("RegisterSegment(path1): %v", err)
	}
	if err := mgr.RegisterSegment(path2, id2); err != nil {
		t.Fatalf("RegisterSegment(path2): %v", err)
	}
	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1): %v", err)
	}
	got, err := mgr.ReadUnsafe(ptr1)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr1 current): %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ReadUnsafe(ptr1 current) mismatch")
	}

	f1 := mgr.files[id1]
	files = []*File{f1, mgr.files[id2]}
	if data, _ := f1.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected current leaf segment to hold active mmap before promotion")
	}

	if err := mgr.PromoteCurrentWritable(id2); err != nil {
		t.Fatalf("PromoteCurrentWritable(id2): %v", err)
	}
	if f1.currentWritable.Load() {
		t.Fatalf("expected prior leaf segment to be sealed after promotion")
	}
	if data, _ := f1.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected prior current leaf mmap to be retired after promotion, len=%d", len(data))
	}
	if dead := f1.deadMappingsCount.Load(); dead == 0 {
		t.Fatalf("expected retired current leaf mapping to move into deadMappings")
	}

	got, err = mgr.ReadUnsafe(ptr1)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr1 sealed): %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ReadUnsafe(ptr1 sealed) mismatch")
	}
	if data, _ := f1.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected sealed leaf read to respect zero sealed-map budget after promotion")
	}
	if got := f1.mmapReadFallbackReadAt.Load(); got == 0 {
		t.Fatalf("expected sealed leaf read to fall back to ReadAt under zero sealed-map budget")
	}
}

func TestManagerPromoteCurrentWritable_KeepsLeafCurrentMmapWhenSealedBudgetAllows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMappedLeafSealedBudget(t, 1)
	withMappedLeafSealedBytesBudget(t, 1<<20)
	withCurrentWritableMmapTargetBytes(t, 64<<10)
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, _ := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	path1 := segmentPathForID(dir, id1)
	path2 := segmentPathForID(dir, id2)
	if err := mgr.RegisterSegment(path1, id1); err != nil {
		t.Fatalf("RegisterSegment(path1): %v", err)
	}
	if err := mgr.RegisterSegment(path2, id2); err != nil {
		t.Fatalf("RegisterSegment(path2): %v", err)
	}
	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1): %v", err)
	}
	got, err := mgr.ReadUnsafe(ptr1)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr1 current): %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ReadUnsafe(ptr1 current) mismatch")
	}

	f1 := mgr.files[id1]
	f2 := mgr.files[id2]
	files = []*File{f1, f2}
	beforeRemaps := f1.remapCount.Load()
	if data, _ := f1.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected current leaf segment to hold active mmap before promotion")
	}

	if err := mgr.PromoteCurrentWritable(id2); err != nil {
		t.Fatalf("PromoteCurrentWritable(id2): %v", err)
	}
	if f1.currentWritable.Load() {
		t.Fatalf("expected prior leaf segment to be sealed after promotion")
	}
	if data, _ := f1.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected prior current leaf mmap to remain active when sealed budget allows")
	}
	if dead := f1.deadMappingsCount.Load(); dead != 0 {
		t.Fatalf("expected no dead mappings when sealed budget allows, got=%d", dead)
	}

	got, err = mgr.ReadUnsafe(ptr1)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr1 sealed): %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ReadUnsafe(ptr1 sealed) mismatch")
	}
	if got := f1.mmapReadFallbackReadAt.Load(); got != 0 {
		t.Fatalf("expected sealed leaf read to stay on mmap path, fallbacks=%d", got)
	}
	if remaps := f1.remapCount.Load(); remaps != beforeRemaps {
		t.Fatalf("expected no extra remap for recently sealed leaf segment, before=%d after=%d", beforeRemaps, remaps)
	}
	currentSegs, currentBytes, sealedSegs, sealedBytes, deadMappings, deadBytes := mgr.MmapResidencyStats()
	if currentSegs != 0 || currentBytes != 0 {
		t.Fatalf("expected no mapped current segment before reading the new current file, currentSegs=%d currentBytes=%d", currentSegs, currentBytes)
	}
	if sealedSegs != 1 || sealedBytes == 0 {
		t.Fatalf("expected one mapped sealed segment after promotion, sealedSegs=%d sealedBytes=%d", sealedSegs, sealedBytes)
	}
	if deadMappings != 0 || deadBytes != 0 {
		t.Fatalf("expected no dead mapping duplication, deadMappings=%d deadBytes=%d", deadMappings, deadBytes)
	}
}

func TestManagerPromoteCurrentWritable_ExcludesNextCurrentFromSealedBudget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMappedLeafSealedBudget(t, 1)
	withMappedLeafSealedBytesBudget(t, 1<<20)
	withCurrentWritableMmapTargetBytes(t, 64<<10)
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	if err := mgr.RegisterSegment(segmentPathForID(dir, id1), id1); err != nil {
		t.Fatalf("RegisterSegment(id1): %v", err)
	}
	if err := mgr.RegisterSegment(segmentPathForID(dir, id2), id2); err != nil {
		t.Fatalf("RegisterSegment(id2): %v", err)
	}

	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1 first): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr1); err != nil {
		t.Fatalf("ReadUnsafe(ptr1): %v", err)
	}
	if err := mgr.PromoteCurrentWritable(id2); err != nil {
		t.Fatalf("PromoteCurrentWritable(id2): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr2); err != nil {
		t.Fatalf("ReadUnsafe(ptr2): %v", err)
	}

	f1 := mgr.files[id1]
	f2 := mgr.files[id2]
	files = []*File{f1, f2}

	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1 second): %v", err)
	}
	if !f1.currentWritable.Load() {
		t.Fatalf("expected id1 to become current again")
	}
	if f2.currentWritable.Load() {
		t.Fatalf("expected id2 to be sealed after re-promotion")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected demoted id2 mapping to stay active when the next current file is excluded from sealed budgeting")
	}
	if dead := f2.deadMappingsCount.Load(); dead != 0 {
		t.Fatalf("expected no dead mapping duplication when re-promoting a mapped leaf segment, got=%d", dead)
	}
}

func TestManagerPromoteCurrentWritable_RetiresDemotedLeafWhenSealedBudgetAlreadyFull(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMappedLeafSealedBudget(t, 1)
	withMappedLeafSealedBytesBudget(t, 1<<20)
	withCurrentWritableMmapTargetBytes(t, 64<<10)
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))
	id3, _ := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 3, 3, bytes.Repeat([]byte("c"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	path1 := segmentPathForID(dir, id1)
	path2 := segmentPathForID(dir, id2)
	path3 := segmentPathForID(dir, id3)
	if err := mgr.RegisterSegment(path1, id1); err != nil {
		t.Fatalf("RegisterSegment(path1): %v", err)
	}
	if err := mgr.RegisterSegment(path2, id2); err != nil {
		t.Fatalf("RegisterSegment(path2): %v", err)
	}
	if err := mgr.RegisterSegment(path3, id3); err != nil {
		t.Fatalf("RegisterSegment(path3): %v", err)
	}

	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr1); err != nil {
		t.Fatalf("ReadUnsafe(ptr1 current): %v", err)
	}
	if err := mgr.PromoteCurrentWritable(id2); err != nil {
		t.Fatalf("PromoteCurrentWritable(id2): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr2); err != nil {
		t.Fatalf("ReadUnsafe(ptr2 current): %v", err)
	}

	f1 := mgr.files[id1]
	f2 := mgr.files[id2]
	f3 := mgr.files[id3]
	files = []*File{f1, f2, f3}
	if data, _ := f1.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected first sealed leaf segment to remain mapped while within budget")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected second current leaf segment to be mapped before demotion")
	}

	if err := mgr.PromoteCurrentWritable(id3); err != nil {
		t.Fatalf("PromoteCurrentWritable(id3): %v", err)
	}
	if f2.currentWritable.Load() {
		t.Fatalf("expected second segment to be sealed after promotion")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected demoted second segment to retire active mmap once sealed budget is full, len=%d", len(data))
	}
	if dead := f2.deadMappingsCount.Load(); dead == 0 {
		t.Fatalf("expected demoted second segment to move prior current mmap into deadMappings")
	}
	currentSegs, _, sealedSegs, _, deadMappings, _ := mgr.MmapResidencyStats()
	if currentSegs != 0 {
		t.Fatalf("expected new current segment to remain unmapped before any read, currentSegs=%d", currentSegs)
	}
	if sealedSegs != 1 {
		t.Fatalf("expected sealed mmap count to stay capped at one, sealedSegs=%d", sealedSegs)
	}
	if deadMappings == 0 {
		t.Fatalf("expected deadMappings to record the retired second segment")
	}
}

func TestManagerPromoteCurrentWritable_RetiresDemotedLeafWhenFileGrewPastSealedBytesBudget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMappedLeafSealedBudget(t, 2)
	withMappedLeafSealedBytesBudget(t, 512)
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, _ := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	for _, id := range []uint32{id1, id2} {
		if err := mgr.RegisterSegment(segmentPathForID(dir, id), id); err != nil {
			t.Fatalf("RegisterSegment(%d): %v", id, err)
		}
	}
	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr1); err != nil {
		t.Fatalf("ReadUnsafe(ptr1): %v", err)
	}

	path1 := segmentPathForID(dir, id1)
	expand, err := os.OpenFile(path1, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile(path1 append): %v", err)
	}
	if _, err := expand.Write(bytes.Repeat([]byte("x"), 4096)); err != nil {
		_ = expand.Close()
		t.Fatalf("append(path1): %v", err)
	}
	if err := expand.Close(); err != nil {
		t.Fatalf("Close(path1 append): %v", err)
	}

	f1 := mgr.files[id1]
	f2 := mgr.files[id2]
	files = []*File{f1, f2}
	if data, _ := f1.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected id1 mmap to be active before demotion")
	}

	if err := mgr.PromoteCurrentWritable(id2); err != nil {
		t.Fatalf("PromoteCurrentWritable(id2): %v", err)
	}
	waitForRemapIdle(t, f1)
	if data, _ := f1.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected demoted id1 mapping to retire once sealed file size exceeds byte cap, len=%d", len(data))
	}
	if dead := f1.deadMappingsCount.Load(); dead == 0 {
		t.Fatalf("expected demoted id1 mapping to be retained in deadMappings after retirement")
	}
}

func TestManagerPromoteCurrentWritable_DeadMappingCapKeepsDemotedLeafMapped(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	var files []*File
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMappedLeafSealedBudget(t, 1)
	withMappedLeafSealedBytesBudget(t, 1<<20)
	withMaxDeadMappings(t, 1)
	withCurrentWritableMmapTargetBytes(t, 64<<10)
	t.Cleanup(func() {
		waitForRemapIdle(t, files...)
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	})

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))
	id3, _ := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 3, 3, bytes.Repeat([]byte("c"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	for _, id := range []uint32{id1, id2, id3} {
		if err := mgr.RegisterSegment(segmentPathForID(dir, id), id); err != nil {
			t.Fatalf("RegisterSegment(%d): %v", id, err)
		}
	}

	if err := mgr.PromoteCurrentWritable(id1); err != nil {
		t.Fatalf("PromoteCurrentWritable(id1): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr1); err != nil {
		t.Fatalf("ReadUnsafe(ptr1): %v", err)
	}
	if err := mgr.PromoteCurrentWritable(id2); err != nil {
		t.Fatalf("PromoteCurrentWritable(id2): %v", err)
	}
	if _, err := mgr.ReadUnsafe(ptr2); err != nil {
		t.Fatalf("ReadUnsafe(ptr2): %v", err)
	}

	f1 := mgr.files[id1]
	f2 := mgr.files[id2]
	f3 := mgr.files[id3]
	files = []*File{f1, f2, f3}
	var deadBefore uint64
	if data, _ := f2.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected id2 current mapping before saturating dead-mapping cap")
	} else {
		deadBefore = uint64(effectiveMaxDeadMappings(len(data)))
		f2.deadMappingsCount.Store(deadBefore)
	}

	if err := mgr.PromoteCurrentWritable(id3); err != nil {
		t.Fatalf("PromoteCurrentWritable(id3): %v", err)
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected demoted id2 mapping to stay live once dead-mapping cap is exhausted")
	}
	if dead := f2.deadMappingsCount.Load(); dead != deadBefore {
		t.Fatalf("expected demoted id2 to avoid growing dead mappings when cap is exhausted, before=%d after=%d", deadBefore, dead)
	}
}

func TestFileRemapToFileSizePersistentOnly_SkipsDemotedLeafSegment(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID := mustEncodeFileID(t, ReservedLeafLogLaneID, 1)
	path := segmentPathForID(dir, fileID)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("leaf"), 64)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
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
	f.currentWritable.Store(true)
	f.currentWritable.Store(false)

	f.remapToFileSizePersistentOnly()

	if data, _ := f.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected demoted leaf segment to skip persistent-only remap, len=%d", len(data))
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

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	truncated := contents[:int(ptr.Offset)-1]

	f := &File{ID: fileID, Path: path, File: fh}
	defer func() { _ = f.Close() }()
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

	f := &File{ID: fileID, Path: path, File: fh}
	defer func() { _ = f.Close() }()
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

func TestManagerReadRIDUnverifiedReusesRegisteredSegment(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := SegmentPath(dir, fileID)

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
	ptr3, err := writer.Append(0, nil, 3, []byte("gamma"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	origOpen := currentOpenSegmentFile()
	opens := 0
	swapOpenSegmentFileForTest(func(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error) {
		if id == fileID {
			opens++
		}
		return origOpen(path, id, dictLookup, templateLookup, templateOpts, templateCache)
	})
	t.Cleanup(func() { swapOpenSegmentFileForTest(origOpen) })

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	if opens != 1 {
		t.Fatalf("NewManager opened segment %d times, want 1", opens)
	}

	cases := []struct {
		ptr page.ValuePtr
		rid uint64
	}{
		{ptr: ptrs[0], rid: 1},
		{ptr: ptrs[1], rid: 2},
		{ptr: ptr3, rid: 3},
	}
	for repeat := 0; repeat < 3; repeat++ {
		for _, tc := range cases {
			got, err := mgr.ReadRIDUnverified(tc.ptr)
			if err != nil {
				t.Fatalf("ReadRIDUnverified(%+v): %v", tc.ptr, err)
			}
			if got != tc.rid {
				t.Fatalf("ReadRIDUnverified(%+v)=%d, want %d", tc.ptr, got, tc.rid)
			}
		}
	}
	if opens != 1 {
		t.Fatalf("ReadRIDUnverified reopened segment: opens=%d, want 1", opens)
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
	defer func() {
		// This test seeds mmapData with a non-mmap stale slice to simulate
		// out-of-range stale mappings. Clear it before close so Close does not
		// attempt munmap on heap-backed test data.
		f.mmapData.Store([]byte(nil))
		f.deadMappings = nil
		_ = f.Close()
	}()

	if data, _ := f.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected no eager mmap on open, mapped bytes=%d", len(data))
	}
	if got := f.remapCount.Load(); got != 0 {
		t.Fatalf("expected no eager remap on open, remapCount=%d", got)
	}
}

func TestReadUnsafe_SealedLazyMmapBudgetFallsBackToReadAt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
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
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
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

func TestReadUnsafe_LeafLaneUsesLeafSealedMmapByteBudget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1)
	withMappedLeafSealedBudget(t, 8)
	withMappedLeafSealedBytesBudget(t, 1<<30)

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	f1, err := openFile(segmentPathForID(dir, id1), id1, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile(f1): %v", err)
	}
	defer func() { _ = f1.Close() }()

	f2, err := openFile(segmentPathForID(dir, id2), id2, nil, nil, templ.DecodeOptions{}, nil)
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
		t.Fatalf("expected first leaf-lane sealed segment to be mapped")
	}

	got2, err := set.ReadUnsafe(ptr2)
	if err != nil {
		t.Fatalf("ReadUnsafe(ptr2): %v", err)
	}
	if !bytes.Equal(got2, bytes.Repeat([]byte("b"), 64)) {
		t.Fatalf("ptr2 mismatch")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected second leaf-lane sealed segment to be mapped under leaf budget")
	}
	if got := f2.mmapReadFallbackReadAt.Load(); got != 0 {
		t.Fatalf("expected leaf-lane sealed mmap to avoid ReadAt fallback, got=%d", got)
	}
	if got := f2.sealedMapDeniedByBytes.Load(); got != 0 {
		t.Fatalf("expected no leaf-lane byte-cap deny, got=%d", got)
	}
}

func TestReadUnsafe_LeafLaneDenyCacheRechecksWhenLeafBudgetChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)
	withMappedLeafSealedBudget(t, 8)
	withMappedLeafSealedBytesBudget(t, 1)

	dir := t.TempDir()
	id1, ptr1 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 1, 1, bytes.Repeat([]byte("a"), 64))
	id2, ptr2 := writeTestSegmentWithPtr(t, dir, ReservedLeafLogLaneID, 2, 2, bytes.Repeat([]byte("b"), 64))

	f1, err := openFile(segmentPathForID(dir, id1), id1, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile(f1): %v", err)
	}
	defer func() { _ = f1.Close() }()

	f2, err := openFile(segmentPathForID(dir, id2), id2, nil, nil, templ.DecodeOptions{}, nil)
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

	if got, err := set.ReadUnsafe(ptr1); err != nil {
		t.Fatalf("ReadUnsafe(ptr1): %v", err)
	} else if !bytes.Equal(got, bytes.Repeat([]byte("a"), 64)) {
		t.Fatalf("ptr1 mismatch")
	}
	if _, err := set.ReadUnsafe(ptr2); err != nil {
		t.Fatalf("ReadUnsafe(ptr2 first): %v", err)
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) != 0 {
		t.Fatalf("expected initial leaf-lane read to stay unmapped under tight leaf budget")
	}
	if got := f2.sealedMapDeniedByBytes.Load(); got == 0 {
		t.Fatalf("expected leaf-lane byte-cap deny to increment")
	}

	MaxMappedLeafSealedBytes = 1 << 30

	if got, err := set.ReadUnsafe(ptr2); err != nil {
		t.Fatalf("ReadUnsafe(ptr2 second): %v", err)
	} else if !bytes.Equal(got, bytes.Repeat([]byte("b"), 64)) {
		t.Fatalf("ptr2 second mismatch")
	}
	if data, _ := f2.mmapData.Load().([]byte); len(data) == 0 {
		t.Fatalf("expected leaf-lane deny cache to re-check after leaf budget change")
	}
}

func TestReadUnsafe_SealedMappedOutOfRangeRemapsToKnownFileSize(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
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

func segmentPathForID(dir string, id uint32) string {
	return (&Manager{dir: dir}).SegmentPath(id)
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

func TestManagerSegmentPath_UsesRegisteredPathFromExtraScanDir(t *testing.T) {
	dir := t.TempDir()
	leafDir := filepath.Join(dir, "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(leaf_vlog): %v", err)
	}
	segID := writeTestSegment(t, leafDir, 255, 1, 1, bytes.Repeat([]byte("l"), 64))
	wantPath := segmentPathForID(leafDir, segID)

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() {
		if mgr != nil {
			_ = mgr.Close()
		}
	}()
	if err := mgr.AddScanDir(leafDir); err != nil {
		t.Fatalf("AddScanDir: %v", err)
	}

	if got := mgr.SegmentPath(segID); got != wantPath {
		t.Fatalf("SegmentPath(%d)=%q want %q", segID, got, wantPath)
	}
}

func TestSegmentPathFormatsCanonicalPathWithoutManager(t *testing.T) {
	fileID, err := EncodeFileID(7, 42)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	if got, want := SegmentPath("", fileID), "value-l7-000042.log"; got != want {
		t.Fatalf("SegmentPath empty dir=%q want %q", got, want)
	}
	dir := t.TempDir()
	if got, want := SegmentPath(dir, fileID), filepath.Join(dir, "value-l7-000042.log"); got != want {
		t.Fatalf("SegmentPath dir=%q want %q", got, want)
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

func TestListSegments_ParsesLaneAndLegacyNames(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"value-l1-000003.log",
		"value-l0-000010.log",
		"value-000002.log",
		"value-lbad-1.log",
		"value-l2-nope.log",
		"commit-l0-000001.log",
		"value-l0-000001.tmp",
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("WriteFile(%q): %v", name, err)
		}
	}

	segments, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}

	wantLane0, err := EncodeFileID(0, 10)
	if err != nil {
		t.Fatalf("EncodeFileID lane0: %v", err)
	}
	wantLane1, err := EncodeFileID(1, 3)
	if err != nil {
		t.Fatalf("EncodeFileID lane1: %v", err)
	}
	want := []segmentInfo{
		{id: page.ValueLogFileID(2), path: filepath.Join(dir, "value-000002.log")},
		{id: wantLane0, path: filepath.Join(dir, "value-l0-000010.log")},
		{id: wantLane1, path: filepath.Join(dir, "value-l1-000003.log")},
	}
	if len(segments) != len(want) {
		t.Fatalf("len(segments)=%d want %d; got=%+v", len(segments), len(want), segments)
	}
	for i := range want {
		if segments[i].id != want[i].id || segments[i].path != want[i].path {
			t.Fatalf("segment[%d]=%+v want %+v", i, segments[i], want[i])
		}
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

func TestManagerRewriteLaneHint_PrefersHighestUnusedLane(t *testing.T) {
	mgr := &Manager{
		files: make(map[uint32]*File),
	}
	id0, err := EncodeFileID(0, 7)
	if err != nil {
		t.Fatalf("EncodeFileID lane0: %v", err)
	}
	id1, err := EncodeFileID(1, 3)
	if err != nil {
		t.Fatalf("EncodeFileID lane1: %v", err)
	}
	id255, err := EncodeFileID(255, 9)
	if err != nil {
		t.Fatalf("EncodeFileID lane255: %v", err)
	}
	mgr.files[id0] = &File{ID: id0}
	mgr.files[id1] = &File{ID: id1}
	mgr.files[id255] = &File{ID: id255}

	lane, seq, ok := mgr.RewriteLaneHint()
	if !ok {
		t.Fatalf("RewriteLaneHint ok=false")
	}
	if lane != 254 || seq != 0 {
		t.Fatalf("RewriteLaneHint=(lane=%d seq=%d) want lane=254 seq=0", lane, seq)
	}
}

func TestManagerRewriteLaneHint_FallsBackToLaneZeroMaxSeqWhenAllLanesUsed(t *testing.T) {
	mgr := &Manager{
		files: make(map[uint32]*File),
	}
	for lane := uint32(0); lane <= 255; lane++ {
		seq := uint32(1)
		if lane == 0 {
			seq = 42
		}
		id, err := EncodeFileID(lane, seq)
		if err != nil {
			t.Fatalf("EncodeFileID lane=%d seq=%d: %v", lane, seq, err)
		}
		mgr.files[id] = &File{ID: id}
	}

	lane, seq, ok := mgr.RewriteLaneHint()
	if !ok {
		t.Fatalf("RewriteLaneHint ok=false")
	}
	if lane != 0 || seq != 42 {
		t.Fatalf("RewriteLaneHint=(lane=%d seq=%d) want lane=0 seq=42", lane, seq)
	}
}
