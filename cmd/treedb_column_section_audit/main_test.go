package main

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestIsNonLeafValueLogFileIncludesEveryRegularLane(t *testing.T) {
	const (
		sequenceBits     = 23
		reservedLeafLane = 255
	)
	segmentID := func(lane uint32) uint32 {
		return lane<<sequenceBits | 1
	}
	fileID := func(lane uint32) uint32 {
		return page.ValueLogFileID(segmentID(lane))
	}
	ids := map[uint32]uint32{
		0: fileID(0),
		1: fileID(1),
		2: fileID(2),
	}
	leafSegmentID := segmentID(reservedLeafLane)
	leafID := page.ValueLogFileID(leafSegmentID)
	leafIDs := map[uint32]struct{}{leafSegmentID: {}}

	for lane, id := range ids {
		if !isNonLeafValueLogFile(id, leafIDs) {
			t.Errorf("regular value-log lane %d excluded", lane)
		}
	}
	if isNonLeafValueLogFile(leafID, leafIDs) {
		t.Error("leaf-log file included as a regular value-log file")
	}
	if isNonLeafValueLogFile(0, leafIDs) {
		t.Error("non-value-log file included")
	}
}
