package caching

import (
	"strings"
	"testing"
)

const leafLogMaxSegmentSeqForTest = 1<<23 - 1

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
