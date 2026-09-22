package db

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const leafLogMaxSegmentSeqForTest = 1<<23 - 1

func TestLeafLogSeqAllocatorRejectsSegmentIDExhaustion(t *testing.T) {
	alloc := newLeafLogSeqAllocator(leafLogMaxSegmentSeqForTest)
	if _, err := alloc.Next(); err == nil || !strings.Contains(err.Error(), "sequence space exhausted") {
		t.Fatalf("Next exhaustion error=%v, want sequence space exhausted", err)
	}
	if got := alloc.next.Load(); got != leafLogMaxSegmentSeqForTest {
		t.Fatalf("allocator advanced after exhaustion: got %d want %d", got, uint32(leafLogMaxSegmentSeqForTest))
	}

	alloc = newLeafLogSeqAllocator(leafLogMaxSegmentSeqForTest - 1)
	seq, err := alloc.Next()
	if err != nil {
		t.Fatalf("Next at max valid seq: %v", err)
	}
	if seq != leafLogMaxSegmentSeqForTest {
		t.Fatalf("seq=%d want max %d", seq, uint32(leafLogMaxSegmentSeqForTest))
	}
}

func TestLeafPageLogLanes_SmallSegmentCurrentSetAndRefreshBounded(t *testing.T) {
	db, leafLog, opts := openLeafLogLaneGCTestDB(t, 32<<10)
	defer closeLeafLogLaneGCTestDB(t, db, leafLog)

	refreshBefore := db.valueLogManager.RefreshScanCount()
	putBatch(t, db, 0, 4096, "base")
	putBatch(t, db, 0, 4096, "final")
	if got, want := db.valueLogManager.RefreshScanCount(), refreshBefore; got != want {
		t.Fatalf("multi-lane writes triggered value-log refresh scans: before=%d after=%d", want, got)
	}
	if got := requireDBStatUint64(t, db, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		t.Fatalf("span-native used ops = 0, want lane-routed leaf output")
	}

	currentSegments := requireLeafLogCurrentSegments(t, db, 2)
	wantCurrentIDs := make([]uint32, 0, len(currentSegments))
	for _, seg := range currentSegments {
		wantCurrentIDs = append(wantCurrentIDs, seg.FileID)
	}
	sort.Slice(wantCurrentIDs, func(i, j int) bool { return wantCurrentIDs[i] < wantCurrentIDs[j] })
	gotCurrentIDs := db.valueLogManager.CurrentWritableFileIDs()
	if !reflect.DeepEqual(gotCurrentIDs, wantCurrentIDs) {
		t.Fatalf("current writable file IDs=%v want %v", gotCurrentIDs, wantCurrentIDs)
	}
	state := db.State()
	if state == nil || state.ValueLogSet == nil || state.LeafGenerations == nil {
		t.Fatalf("state missing value-log set/leaf generations: %+v", state)
	}
	for _, seg := range currentSegments {
		if _, ok := state.ValueLogSet.Files[seg.FileID]; !ok {
			t.Fatalf("current segment %d missing from ValueLogSet", seg.FileID)
		}
		rawFileID := page.ValueLogSegmentID(seg.FileID)
		if _, ok := state.LeafGenerations.FileToGeneration[rawFileID]; !ok {
			t.Fatalf("current segment raw file %d missing from leaf-generation view", rawFileID)
		}
	}

	files, err := filepath.Glob(filepath.Join(LeafLogDirPath(opts.Dir), "value-l*-*.log"))
	if err != nil {
		t.Fatalf("Glob leaf_vlog: %v", err)
	}
	if len(files) <= len(currentSegments) {
		t.Fatalf("small segment fixture did not rotate: files=%d current=%d", len(files), len(currentSegments))
	}
	if len(files) > 128 {
		t.Fatalf("small segment fixture created unbounded leaf_vlog files=%d", len(files))
	}
	seen := make(map[uint32]string, len(files))
	for _, path := range files {
		base := filepath.Base(path)
		var lane, seq uint32
		if _, err := fmt.Sscanf(base, "value-l%d-%06d.log", &lane, &seq); err != nil {
			t.Fatalf("parse leaf log name %q: %v", base, err)
		}
		fileID, err := valuelog.EncodeFileID(lane, seq)
		if err != nil {
			t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
		}
		if prev, ok := seen[fileID]; ok {
			t.Fatalf("duplicate leaf log fileID %d for %s and %s", fileID, prev, path)
		}
		seen[fileID] = path
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("stat leaf log %s: %v", path, err)
		}
	}
}
