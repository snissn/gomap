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
	defer func() { _ = mgr.Close() }()

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
	defer func() { _ = mgr.Close() }()
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
	defer func() { _ = mgr.Close() }()

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
	defer func() { _ = mgr.Close() }()

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
	defer func() { _ = mgr.Close() }()

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
	segID := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("m"), 64))

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	mgr.mu.Lock()
	mgr.files = nil
	mgr.mu.Unlock()

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
}
