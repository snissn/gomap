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
	ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityNone)
	if err != nil {
		b.Fatalf("appendValueLog warmup: %v", err)
	}
	putValueLogPtrs(ptrs)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityNone)
		if err != nil {
			b.Fatalf("appendValueLog: %v", err)
		}
		putValueLogPtrs(ptrs)
	}
}

func BenchmarkAppendValueLogOrdinaryBlockPrepared(b *testing.B) {
	clock := valuelog.NewVirtualClock(time.Unix(0, 0))
	sink := &valuelog.VirtualSink{Clock: clock}
	fileID, _ := valuelog.EncodeFileID(0, 1)
	writer := valuelog.NewWriterWithSink(sink, fileID)
	writer.SetEncodeSampleStride(0)

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockCodec:       valuelog.BlockCodecZSTD,
		valueLogBlockTargetBytes: 256,
		valueLogThreshold:        1 << 30,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			Mode: valuelog.AutotuneOff,
		},
		lanes: []lane{{id: 0, vlog: writer}},
	}
	db.startVlogWriter(&db.lanes[0])
	defer func() {
		close(db.closeCh)
		db.wg.Wait()
	}()

	// This matches the collection pointerizer's 4 MiB batch ceiling with the
	// ~8.7 KiB values seen in the Cohere-shaped path.
	const (
		valuesPerBatch = 480
		valueBytes     = 8_704
	)
	records := make([]valuelog.Record, valuesPerBatch)
	for i := range records {
		value := make([]byte, valueBytes)
		state := uint32(i + 1)
		for j := range value[:valueBytes/2] {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		for j := valueBytes / 2; j < len(value); j++ {
			value[j] = byte((i*31 + j*17 + j>>5) % 251)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	warmPtrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityNone)
	if err != nil {
		b.Fatalf("warm appendValueLog: %v", err)
	}
	putValueLogPtrs(warmPtrs)

	b.ReportAllocs()
	b.SetBytes(valuesPerBatch * valueBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityNone)
		if err != nil {
			b.Fatalf("appendValueLog: %v", err)
		}
		putValueLogPtrs(ptrs)
	}
}
