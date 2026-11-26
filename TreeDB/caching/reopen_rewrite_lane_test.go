package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type reopenRewriteLeafLogMockBackend struct {
	*MockBackend
}

func (*reopenRewriteLeafLogMockBackend) SetLeafPageLog(backenddb.LeafPageLog) {}

func newReopenRewriteLeafLogMockBackend() *reopenRewriteLeafLogMockBackend {
	return &reopenRewriteLeafLogMockBackend{MockBackend: NewMockBackend()}
}

func TestReopenRewriteHighLaneDoesNotExpandLanes(t *testing.T) {
	writeSegment := func(t *testing.T, dir string, lane uint32, seq uint32, rid uint64, val []byte) (uint32, string) {
		t.Helper()
		fileID, err := valuelog.EncodeFileID(lane, seq)
		if err != nil {
			t.Fatalf("EncodeFileID(%d, %d): %v", lane, seq, err)
		}
		path := valuelog.SegmentPath(dir, fileID)
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			t.Fatalf("NewWriter(%s): %v", path, err)
		}
		if _, err := w.Append(0, nil, rid, val); err != nil {
			_ = w.Close()
			t.Fatalf("Append(%s): %v", path, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%s): %v", path, err)
		}
		return fileID, path
	}

	t.Run("default_journal_lanes_with_lane254_rewrite_segment", func(t *testing.T) {
		dir := t.TempDir()
		valueDir := filepath.Join(dir, "value_vlog")
		if err := os.MkdirAll(valueDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(value_vlog): %v", err)
		}
		writeSegment(t, valueDir, 0, 1, 1, bytes.Repeat([]byte("a"), 128))
		rewriteFileID, _ := writeSegment(t, valueDir, 254, 1, 2, bytes.Repeat([]byte("r"), 128))

		policy := backenddb.ValueLogGenerationHotWarmCold
		defaults := backenddb.ResolveJournalLaneDefaults(0, runtime.GOMAXPROCS(0), backenddb.DetectPhysicalCores(), policy)
		wantLanes := defaults.Effective
		if defaults.Defaulted && wantLanes < 3 {
			wantLanes = 3
		}

		db, err := Open(dir, newReopenRewriteLeafLogMockBackend(), Options{
			DisableWAL:                 true,
			AllowUnsafe:                true,
			IndexOuterLeavesInValueLog: true,
			ValueLogGenerationPolicy:   uint8(policy),
			ValueLogPointerThreshold:   1,
			ForceValueLogPointers:      true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		if got := len(db.lanes); got != wantLanes {
			t.Errorf("reopen with lane-254 rewrite segment expanded len(db.lanes)=%d, want %d (hotLanes=%d)", got, wantLanes, len(db.valueLogHotLanes))
		}
		if !db.valueLogReader.HasSegment(rewriteFileID) {
			t.Fatalf("valueLogReader missing pre-existing lane-254 rewrite segment %d", rewriteFileID)
		}

		// Appending through cachingValueLogAppender must stay within active lanes
		// and never spray writers/files across ~253 high lanes.
		appender := newCachingValueLogAppender(db, &db.lanes[0])
		values := make([][]byte, 300)
		for i := range values {
			values[i] = bytes.Repeat([]byte{byte(i)}, 64)
		}
		ptrs, err := appender.AppendValues(values)
		if err != nil {
			t.Fatalf("AppendValues: %v", err)
		}
		if err := appender.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		writtenLanes := make(map[uint32]struct{})
		outOfRangeLanes := 0
		for _, ptr := range ptrs {
			laneID, _ := valuelog.DecodeFileID(ptr.FileID)
			if _, seen := writtenLanes[laneID]; !seen && int(laneID) >= wantLanes {
				outOfRangeLanes++
			}
			writtenLanes[laneID] = struct{}{}
		}
		entries, err := os.ReadDir(valueDir)
		if err != nil {
			t.Fatalf("ReadDir(value_vlog): %v", err)
		}
		t.Logf("baseline metrics: len(db.lanes)=%d wantLanes=%d hotLanes=%d writtenLanes=%d outOfRangeLanes=%d valueLogFiles=%d",
			len(db.lanes), wantLanes, len(db.valueLogHotLanes), len(writtenLanes), outOfRangeLanes, len(entries))
		if outOfRangeLanes > 0 {
			t.Errorf("AppendValues routed writes to %d out-of-range lanes (total writtenLanes=%d, want all < %d)", outOfRangeLanes, len(writtenLanes), wantLanes)
		}
		// Existing lane-0 + lane-254 + newly opened active lane files must remain bounded.
		maxExpectedFiles := 2 + wantLanes*8
		if len(entries) > maxExpectedFiles {
			t.Errorf("value_vlog segment file explosion: got %d files (writtenLanes=%d), want <= %d", len(entries), len(writtenLanes), maxExpectedFiles)
		}
	})

	t.Run("explicit_journal_lanes_with_lane254_rewrite_segment", func(t *testing.T) {
		dir := t.TempDir()
		valueDir := filepath.Join(dir, "value_vlog")
		if err := os.MkdirAll(valueDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(value_vlog): %v", err)
		}
		writeSegment(t, valueDir, 254, 3, 10, bytes.Repeat([]byte("z"), 128))

		db, err := Open(dir, newReopenRewriteLeafLogMockBackend(), Options{
			JournalLanes:               4,
			IndexOuterLeavesInValueLog: true,
			ValueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		if got := len(db.lanes); got != 4 {
			t.Fatalf("len(db.lanes)=%d, want 4 (hotLanes=%d)", got, len(db.valueLogHotLanes))
		}
		if got := db.nextRID.Load(); got < 10 {
			t.Fatalf("nextRID=%d, want >= 10 from lane-254 segment", got)
		}
	})

	t.Run("redo_wal_high_lane_still_expands_lanes_for_recovery", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		if err := os.MkdirAll(walDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(wal): %v", err)
		}
		walPath := filepath.Join(walDir, "commit-l5-000002.log")
		if err := os.WriteFile(walPath, bytes.Repeat([]byte("w"), 64), 0o600); err != nil {
			t.Fatalf("WriteFile(wal): %v", err)
		}

		db, err := Open(dir, newReopenRewriteLeafLogMockBackend(), Options{
			JournalLanes:               2,
			IndexOuterLeavesInValueLog: true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		if got := len(db.lanes); got != 6 {
			t.Fatalf("redo WAL segment commit-l5 did not preserve lane expansion: len(db.lanes)=%d, want 6", got)
		}
		db.lanes[5].walMu.Lock()
		_, tracked := db.lanes[5].walClosedSizes[walPath]
		db.lanes[5].walMu.Unlock()
		if !tracked {
			t.Fatalf("recovered redo WAL segment %s missing from lanes[5].walClosedSizes", walPath)
		}
	})
}
