package db

import "testing"

func TestClearFlushApplyReadOnlyPrepareBuffersDropsFreeList(t *testing.T) {
	db := &DB{}
	db.flushApplyReadOnlyPrepareFree = []*flushApplyReadOnlyPrepareBuffer{{}, {}}
	db.clearFlushApplyReadOnlyPrepareBuffers()
	if db.flushApplyReadOnlyPrepareFree != nil {
		t.Fatalf("prepare buffer free-list retained after clear: len=%d cap=%d", len(db.flushApplyReadOnlyPrepareFree), cap(db.flushApplyReadOnlyPrepareFree))
	}
}
