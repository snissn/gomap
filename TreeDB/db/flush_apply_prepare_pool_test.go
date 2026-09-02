package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/zipper"
)

func TestClearFlushApplyReadOnlyPrepareBuffersDropsFreeList(t *testing.T) {
	db := &DB{}
	db.flushApplyReadOnlyPrepareFree = []*flushApplyReadOnlyPrepareBuffer{{}, {}}
	db.clearFlushApplyReadOnlyPrepareBuffers()
	if db.flushApplyReadOnlyPrepareFree != nil {
		t.Fatalf("prepare buffer free-list retained after clear: len=%d cap=%d", len(db.flushApplyReadOnlyPrepareFree), cap(db.flushApplyReadOnlyPrepareFree))
	}
}

func TestReleaseFlushApplyReadOnlyPreparePlanBufferReusesOmitKeysSpans(t *testing.T) {
	db := &DB{}
	buf := &flushApplyReadOnlyPrepareBuffer{}
	prepared := zipper.ReadOnlyPrepareResult{
		OmitKeys:  true,
		LeafSpans: make([]zipper.ReadOnlyLeafSpan, 1, 8),
	}
	db.releaseFlushApplyReadOnlyPreparePlanBuffer(buf, &prepared)
	if len(db.flushApplyReadOnlyPrepareFree) != 1 || db.flushApplyReadOnlyPrepareFree[0] != buf {
		t.Fatalf("prepare buffer not returned to free list: %+v", db.flushApplyReadOnlyPrepareFree)
	}
	if !buf.opts.OmitKeys {
		t.Fatalf("reusable opts lost OmitKeys")
	}
	if prepared.LeafSpans != nil || prepared.OmitKeys {
		t.Fatalf("prepared was not cleared after release: %+v", prepared)
	}
}
