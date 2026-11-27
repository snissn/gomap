package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDetachSealedLeafPageLogSegments_PrunesCreatedDemotesReaderAndLazyReopens(t *testing.T) {
	db, _, _ := openCachingLeafPageLogLaneTestDB(t)
	defer func() { _ = db.Close() }()

	group := &cachingLeafPageLogGroup{db: db}

	// Append via worker 0 and worker 1 to populate two distinct leaf_vlog lanes.
	lane0Log, ok := group.LeafPageLogLane(0)
	if !ok {
		t.Fatal("missing leaf page log lane 0")
	}
	lane1Log, ok := group.LeafPageLogLane(1)
	if !ok {
		t.Fatal("missing leaf page log lane 1")
	}
	dummyPage := bytes.Repeat([]byte{0x5a}, page.PageSize)
	ptr0, err := lane0Log.AppendLeafPage(dummyPage)
	if err != nil {
		t.Fatalf("lane0 AppendLeafPage: %v", err)
	}
	ptr1, err := lane1Log.AppendLeafPage(dummyPage)
	if err != nil {
		t.Fatalf("lane1 AppendLeafPage: %v", err)
	}

	fileID0 := ptr0.ValueLogFileID()
	fileID1 := ptr1.ValueLogFileID()
	if fileID0 == fileID1 {
		t.Fatalf("expected distinct segment file IDs for lane 0 and lane 1, got %d", fileID0)
	}
	raw0 := page.ValueLogSegmentID(fileID0)
	raw1 := page.ValueLogSegmentID(fileID1)

	// Confirm both lanes report current & created segments before detach.
	beforeCurrent, err := group.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CurrentLeafPageLogSegmentsSnapshot before: %v", err)
	}
	if len(beforeCurrent) < 2 {
		t.Fatalf("expected >=2 current leaf segments before detach, got %+v", beforeCurrent)
	}

	// Detach ONLY raw0 (sealed); raw1 (writable) must stay attached.
	if err := group.DetachSealedLeafPageLogSegments(map[uint32]struct{}{raw0: {}}); err != nil {
		t.Fatalf("DetachSealedLeafPageLogSegments: %v", err)
	}

	afterCurrent, err := group.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CurrentLeafPageLogSegmentsSnapshot after: %v", err)
	}
	for _, seg := range afterCurrent {
		if page.ValueLogSegmentID(seg.FileID) == raw0 {
			t.Fatalf("detached sealed raw0=%d still reported in CurrentLeafPageLogSegmentsSnapshot: %+v", raw0, afterCurrent)
		}
	}
	foundRaw1 := false
	for _, seg := range afterCurrent {
		if page.ValueLogSegmentID(seg.FileID) == raw1 {
			foundRaw1 = true
		}
	}
	if !foundRaw1 {
		t.Fatalf("writable raw1=%d should remain attached, got %+v", raw1, afterCurrent)
	}

	// Pending created segments must also have raw0 pruned so a deleted segment
	// cannot cause permanent ENOENT on subsequent publish registration.
	createdAfter, err := group.CreatedLeafPageLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CreatedLeafPageLogSegmentsSnapshot after: %v", err)
	}
	for _, seg := range createdAfter {
		if page.ValueLogSegmentID(seg.FileID) == raw0 {
			t.Fatalf("detached sealed raw0=%d leaked in CreatedLeafPageLogSegmentsSnapshot: %+v", raw0, createdAfter)
		}
	}

	// valueLogReader must have demoted fileID0 from currentWritable while keeping fileID1.
	if db.valueLogReader == nil {
		t.Fatal("expected non-nil valueLogReader")
	}
	foundFileID1 := false
	for _, id := range db.valueLogReader.CurrentWritableFileIDs() {
		if id == fileID0 {
			t.Fatalf("detached fileID0=%d still currentWritable in valueLogReader", fileID0)
		}
		if id == fileID1 {
			foundFileID1 = true
		}
	}
	if !foundFileID1 {
		t.Fatalf("expected writable fileID1=%d to remain currentWritable in valueLogReader", fileID1)
	}

	// Lazy reopen on lane 0 must allocate a strictly newer sequence than both prior segments.
	ptr0Next, err := lane0Log.AppendLeafPage(dummyPage)
	if err != nil {
		t.Fatalf("lane0 lazy reopen AppendLeafPage: %v", err)
	}
	_, seq0Old := valuelog.DecodeFileID(fileID0)
	_, seq1Old := valuelog.DecodeFileID(fileID1)
	_, seq0New := valuelog.DecodeFileID(ptr0Next.ValueLogFileID())
	if seq0New <= seq0Old || seq0New <= seq1Old {
		t.Fatalf("lazy reopen seq=%d want > max(%d,%d)", seq0New, seq0Old, seq1Old)
	}
}
