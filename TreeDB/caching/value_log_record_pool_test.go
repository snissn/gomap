package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestClearValueLogRecordValues_DropsValueReferences(t *testing.T) {
	recs := []valuelog.Record{{RID: 1, Value: make([]byte, 32)}}
	clearValueLogRecordValues(recs)
	if recs[0].Value != nil {
		t.Fatalf("expected value reference to be cleared")
	}
}

func TestPutValueLogRecordsNoClear_Smoke(t *testing.T) {
	recs := []valuelog.Record{{RID: 1, Value: make([]byte, 32)}}
	putValueLogRecordsNoClear(recs)
	// Do not assert on recs after putValueLogRecordsNoClear; ownership has been
	// returned to the global pool and another goroutine may reuse it.
}

func TestVlogFramePreparerPoolResetsCompressionState(t *testing.T) {
	records := []valuelog.Record{
		{RID: 1, Value: bytes.Repeat([]byte("leaf-page|"), 512)},
		{RID: 2, Value: bytes.Repeat([]byte("leaf-page|"), 512)},
	}
	prep := getVlogFramePreparer()
	prep.SetBlockCompression(valuelog.BlockCodecLZ4, true)
	if _, stats, err := prep.PrepareFrameInto(nil, 0, nil, records); err != nil {
		t.Fatalf("PrepareFrameInto compressed: %v", err)
	} else if !stats.Attempted {
		t.Fatalf("expected compression attempt before returning to pool")
	}
	putVlogFramePreparer(prep)

	reused := getVlogFramePreparer()
	defer putVlogFramePreparer(reused)
	if _, stats, err := reused.PrepareFrameInto(nil, 0, nil, records); err != nil {
		t.Fatalf("PrepareFrameInto after pool reset: %v", err)
	} else if stats.Attempted {
		t.Fatalf("pooled preparer kept compression enabled after reset")
	}
}
