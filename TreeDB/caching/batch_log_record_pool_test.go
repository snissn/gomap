package caching

import "testing"

func TestBatchLogRecordPool_PutClearsReferences(t *testing.T) {
	var db DB
	records := make([]logRecord, 2, 2)
	records[0] = logRecord{Op: logOpSetInline, Key: []byte("k"), Value: []byte("v"), RID: 7}
	records[1] = logRecord{Op: logOpDelete, Key: []byte("k2"), Value: []byte("v2"), RID: 11}

	db.putBatchLogRecords(records)

	for i := range records {
		if records[i].Key != nil || records[i].Value != nil || records[i].RID != 0 {
			t.Fatalf("record %d not cleared: %+v", i, records[i])
		}
	}
}

func TestBatchLogRecordPool_GetMinCapacity(t *testing.T) {
	var db DB
	const wantCap = 64
	records := db.getBatchLogRecords(wantCap)
	if len(records) != 0 {
		t.Fatalf("len(records)=%d want=0", len(records))
	}
	if cap(records) < wantCap {
		t.Fatalf("cap(records)=%d want >= %d", cap(records), wantCap)
	}
}
