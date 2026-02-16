package caching

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestShouldFlushDeferredValueLog_RIDJoinOversizedBlobSkipsFlush(t *testing.T) {
	db := &DB{
		indexOuterLeafMode:          backenddb.IndexOuterLeafModeV2FencePtr,
		valueLogWALFenceMode:        string(backenddb.ValueLogWALFenceModeRIDJoin),
		outerLeafBlobThresholdBytes: 256,
	}
	blob := bytes.Repeat([]byte("x"), 2048)
	recs := []valuelog.Record{{RID: 1, Value: blob}}
	if db.shouldFlushDeferredValueLog(vlogWriteOff, recs) {
		t.Fatalf("expected oversized rid_join blob record batch to remain buffered")
	}
	if db.shouldFlushDeferredValueLogValue(vlogWriteOff, blob) {
		t.Fatalf("expected oversized rid_join blob value to remain buffered")
	}
}

func TestShouldFlushDeferredValueLog_RIDJoinSmallRawStillFlushes(t *testing.T) {
	db := &DB{
		indexOuterLeafMode:          backenddb.IndexOuterLeafModeV2FencePtr,
		valueLogWALFenceMode:        string(backenddb.ValueLogWALFenceModeRIDJoin),
		outerLeafBlobThresholdBytes: 256,
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
