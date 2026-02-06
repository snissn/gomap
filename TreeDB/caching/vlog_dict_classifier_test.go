package caching

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func newValueLogDictClassifierTrainer(t *testing.T) *compression.Trainer {
	t.Helper()
	tr := compression.NewTrainer(
		compression.TrainConfig{
			TrainBytes:     8 << 20,
			DictBytes:      8 << 10,
			MinRecords:     1024,
			MaxRecordBytes: 8 << 10,
			SampleStride:   1,
			DedupWindow:    64,
		},
		compression.Config{Kind: compression.KindZSTD, Level: zstd.SpeedFastest},
		false,
		false,
	)
	if tr == nil {
		t.Fatalf("expected trainer")
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func randomValueLogRecords(n, valueBytes int, seed int64) []valuelog.Record {
	rng := rand.New(rand.NewSource(seed))
	records := make([]valuelog.Record, n)
	for i := range records {
		value := make([]byte, valueBytes)
		_, _ = rng.Read(value)
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	return records
}

func compressibleValueLogRecords(n, valueBytes int) []valuelog.Record {
	seed := bytes.Repeat([]byte("compressible-"), valueBytes/13+1)
	records := make([]valuelog.Record, n)
	for i := range records {
		value := make([]byte, valueBytes)
		copy(value, seed)
		value[len(value)-1] = byte(i)
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	return records
}

func TestValueLogDictCollectSamples_SkipsIncompressibleBeforeFirstDict(t *testing.T) {
	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		valueLogDictTrainer:            tr,
		valueLogDictMetricsPauseBytes:  1 << 20,
		valueLogDictProbeBytes:         64 << 10,
		valueLogDictPausedSampleStride: 256,
	}

	records := randomValueLogRecords(512, 4096, 1)
	db.valueLogDictCollectSamples(records)

	if pause := db.valueLogDictPauseRemaining.Load(); pause == 0 {
		t.Fatalf("expected pause to arm for incompressible stream")
	}
	if skipped := db.valueLogDictClassifySkipped.Load(); skipped == 0 {
		t.Fatalf("expected classifier skip count > 0")
	}
	stats := tr.Stats()
	if stats.Enqueued != 0 {
		t.Fatalf("expected no trainer enqueue for incompressible records, got=%d", stats.Enqueued)
	}
}

func TestValueLogDictCollectSamples_CompressibleCappedPerBatch(t *testing.T) {
	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		valueLogDictTrainer:            tr,
		valueLogDictMetricsPauseBytes:  1 << 20,
		valueLogDictProbeBytes:         64 << 10,
		valueLogDictPausedSampleStride: 256,
	}

	records := compressibleValueLogRecords(512, 4096)
	db.valueLogDictCollectSamples(records)

	if pause := db.valueLogDictPauseRemaining.Load(); pause != 0 {
		t.Fatalf("expected no pause on compressible stream, got=%d", pause)
	}
	if sampled := db.valueLogDictClassifySampled.Load(); sampled != valueLogDictCollectPerBatchCap {
		t.Fatalf("expected classifier sampled=%d, got=%d", valueLogDictCollectPerBatchCap, sampled)
	}
	stats := tr.Stats()
	if stats.Enqueued != uint64(valueLogDictCollectPerBatchCap) {
		t.Fatalf("expected trainer enqueue=%d, got=%d", valueLogDictCollectPerBatchCap, stats.Enqueued)
	}
}
