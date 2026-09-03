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
	fileID := func(lane uint32) uint32 {
		return page.ValueLogFileID(lane<<sequenceBits | 1)
	}
	ids := map[uint32]uint32{
		0: fileID(0),
		1: fileID(1),
		2: fileID(2),
	}
	leafID := fileID(reservedLeafLane)
	leafIDs := map[uint32]struct{}{leafID: {}}

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
