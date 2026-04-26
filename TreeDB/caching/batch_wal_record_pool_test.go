package caching

import "testing"

func TestBatchWALRecordPoolClearsPointerFields(t *testing.T) {
	db := &DB{}
	records := make([]logRecord, 1, 2)
	records[0] = logRecord{
		Op:    logOpSetInline,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	db.putBatchWALRecords(records)

	full := records[:cap(records)]
	for i, record := range full {
		if record.Op != 0 || record.Key != nil || record.Value != nil || record.RID != 0 || record.Seq != 0 {
			t.Fatalf("pooled WAL record %d retained data: %+v", i, record)
		}
	}
}

func TestBatchWALRecordPoolSkipsOversizedBuffers(t *testing.T) {
	db := &DB{}
	records := make([]logRecord, 0, batchWALRecordPoolMaxRetain+1)

	db.putBatchWALRecords(records)
	got := db.getBatchWALRecords(1)
	if cap(got) > batchWALRecordPoolMaxRetain {
		t.Fatalf("got oversized WAL record buffer cap=%d max=%d", cap(got), batchWALRecordPoolMaxRetain)
	}
}
