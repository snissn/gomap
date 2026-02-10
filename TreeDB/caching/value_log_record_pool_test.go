package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestPutValueLogRecordsNoClear_DropsValueReferences(t *testing.T) {
	recs := []valuelog.Record{{RID: 1, Value: make([]byte, 32)}}
	putValueLogRecordsNoClear(recs)
	if recs[0].Value != nil {
		t.Fatalf("expected value reference to be cleared before pooling")
	}
}
