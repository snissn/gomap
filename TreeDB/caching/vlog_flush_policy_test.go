package caching

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestShouldFlushDeferredValueLog_RIDJoinOversizedBlobFlushesInWALOff(t *testing.T) {
	db := &DB{
		indexOuterLeafMode:          backenddb.IndexOuterLeafModeV2FencePtr,
		valueLogWALFenceMode:        string(backenddb.ValueLogWALFenceModeRIDJoin),
		outerLeafBlobThresholdBytes: 256,
		disableJournal:              true,
	}
	blob := bytes.Repeat([]byte("x"), 2048)
	recs := []valuelog.Record{{RID: 1, Value: blob}}
	if !db.shouldFlushDeferredValueLog(vlogWriteOff, recs) {
		t.Fatalf("expected oversized raw record batch to flush in WAL-off deferred mode")
	}
	if !db.shouldFlushDeferredValueLogValue(vlogWriteOff, blob) {
		t.Fatalf("expected oversized raw value to flush in WAL-off deferred mode")
	}
}

func TestShouldFlushDeferredValueLog_RIDJoinSmallRawStillFlushes(t *testing.T) {
	db := &DB{
		indexOuterLeafMode:          backenddb.IndexOuterLeafModeV2FencePtr,
		valueLogWALFenceMode:        string(backenddb.ValueLogWALFenceModeRIDJoin),
		outerLeafBlobThresholdBytes: 256,
		disableJournal:              true,
	}
	smallRaw := bytes.Repeat([]byte("x"), 128)
	recs := []valuelog.Record{{RID: 1, Value: smallRaw}}
	if !db.shouldFlushDeferredValueLog(vlogWriteOff, recs) {
		t.Fatalf("expected non-oversized raw record batch to flush")
	}
	if !db.shouldFlushDeferredValueLogValue(vlogWriteOff, smallRaw) {
		t.Fatalf("expected non-oversized raw value to flush")
	}
}
