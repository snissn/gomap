package caching

import (
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
