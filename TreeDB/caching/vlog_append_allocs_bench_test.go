package caching

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func BenchmarkAppendValueLogAllocs(b *testing.B) {
	clock := valuelog.NewVirtualClock(time.Unix(0, 0))
	sink := &valuelog.VirtualSink{Clock: clock}
	fileID, _ := valuelog.EncodeFileID(0, 1)
	writer := valuelog.NewWriterWithSink(sink, fileID)
	// Avoid encode sampling overhead; this benchmark focuses on caching-layer allocs.
	writer.SetEncodeSampleStride(0)

	db := &DB{
		closeCh: make(chan struct{}),
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			Mode: valuelog.AutotuneOff,
		},
		lanes: []lane{{id: 0, vlog: writer}},
	}

	records := make([]valuelog.Record, 1000)
	value := make([]byte, 128)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}

	// Warm the pools and writer scratch.
	ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityNone, false)
	if err != nil {
		b.Fatalf("appendValueLog warmup: %v", err)
	}
	putValueLogPtrs(ptrs)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityNone, false)
		if err != nil {
			b.Fatalf("appendValueLog: %v", err)
		}
		putValueLogPtrs(ptrs)
	}
}
