package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	templ "github.com/snissn/gomap/TreeDB/template"
)

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
	f.mmapData.Store([]byte(nil))

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
