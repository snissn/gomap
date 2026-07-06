package caching

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const leafLogMaxSegmentSeqForTest = 1<<23 - 1

var valueLogLaneForFileIDTestSink *lane

func TestNextLeafLogAppendSeqRejectsSegmentIDExhaustion(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	db.leafLogAppendSeq.Store(leafLogMaxSegmentSeqForTest)
	if _, err := db.nextLeafLogAppendSeq(); err == nil || !strings.Contains(err.Error(), "sequence space exhausted") {
		t.Fatalf("nextLeafLogAppendSeq exhaustion error=%v, want sequence space exhausted", err)
	}
	if got := db.leafLogAppendSeq.Load(); got != leafLogMaxSegmentSeqForTest {
		t.Fatalf("leafLogAppendSeq advanced after exhaustion: got %d want %d", got, uint32(leafLogMaxSegmentSeqForTest))
	}

	db.leafLogAppendSeq.Store(leafLogMaxSegmentSeqForTest - 1)
	seq, err := db.nextLeafLogAppendSeq()
	if err != nil {
		t.Fatalf("nextLeafLogAppendSeq at max valid seq: %v", err)
	}
	if seq != leafLogMaxSegmentSeqForTest {
		t.Fatalf("seq=%d want max %d", seq, uint32(leafLogMaxSegmentSeqForTest))
	}
}

func TestValueLogLaneForFileID_LeafAppendLookupSkipsNilLanes(t *testing.T) {
	db, fileID, want := newValueLogLaneLookupTestDB(t, 8, 5)
	db.leafLogAppendLanes[2] = nil
	publishLeafLogAppendLanesForTest(db)

	got := db.valueLogLaneForFileID(fileID)
	if got != want {
		t.Fatalf("valueLogLaneForFileID matched lane %p want %p", got, want)
	}
}

func TestValueLogLaneForFileID_LeafAppendLookupRequiresActiveSegment(t *testing.T) {
	db, fileID, _ := newValueLogLaneLookupTestDB(t, 4, 3)
	db.leafLogAppendLanes[3].vlogPath = ""
	db.leafLogAppendLanes[3].vlog = nil

	if got := db.valueLogLaneForFileID(fileID); got != nil {
		t.Fatalf("valueLogLaneForFileID matched inactive lane %p, want nil", got)
	}
}

func TestValueLogLaneForFileID_LeafAppendLookupDoesNotAllocate(t *testing.T) {
	db, fileID, want := newValueLogLaneLookupTestDB(t, 8, 5)
	if got := db.valueLogLaneForFileID(fileID); got != want {
		t.Fatalf("valueLogLaneForFileID matched lane %p want %p", got, want)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		valueLogLaneForFileIDTestSink = db.valueLogLaneForFileID(fileID)
	})
	if allocs != 0 {
		t.Fatalf("valueLogLaneForFileID allocations/run=%v want 0", allocs)
	}
}

func BenchmarkValueLogLaneForFileID_LeafAppendLookup(b *testing.B) {
	db, fileID, want := newValueLogLaneLookupTestDB(b, 8, 5)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valueLogLaneForFileIDTestSink = db.valueLogLaneForFileID(fileID)
	}
	b.StopTimer()
	if valueLogLaneForFileIDTestSink != want {
		b.Fatalf("valueLogLaneForFileID matched lane %p want %p", valueLogLaneForFileIDTestSink, want)
	}
}

func newValueLogLaneLookupTestDB(tb testing.TB, laneCount, matchIndex int) (*DB, uint32, *lane) {
	tb.Helper()
	if laneCount <= 0 || matchIndex < 0 || matchIndex >= laneCount {
		tb.Fatalf("invalid lane lookup fixture: laneCount=%d matchIndex=%d", laneCount, matchIndex)
	}
	db := &DB{indexOuterLeavesInValueLog: true}
	db.leafLog.id = leafLogLaneID
	db.leafLogAppendLanes = make([]*lane, laneCount)
	for i := 0; i < laneCount; i++ {
		db.leafLogAppendLanes[i] = &lane{
			id:       leafLogLaneID,
			vlogSeq:  100 + i,
			vlogPath: "leaf-active",
		}
	}
	publishLeafLogAppendLanesForTest(db)
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), uint32(db.leafLogAppendLanes[matchIndex].vlogSeq))
	if err != nil {
		tb.Fatalf("EncodeFileID: %v", err)
	}
	return db, fileID, db.leafLogAppendLanes[matchIndex]
}

func publishLeafLogAppendLanesForTest(db *DB) {
	db.leafLogAppendMu.Lock()
	defer db.leafLogAppendMu.Unlock()
	db.publishLeafLogAppendLanesLocked()
}
