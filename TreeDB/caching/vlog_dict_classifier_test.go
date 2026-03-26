package caching

import (
	"bytes"
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

func highEntropyValueLogRecords(n, valueBytes int) []valuelog.Record {
	records := make([]valuelog.Record, n)
	for i := range records {
		value := make([]byte, valueBytes)
		for j := range value {
			value[j] = byte((j + i) & 0xff)
		}
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

	records := highEntropyValueLogRecords(512, 4096)
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

func TestValueLogDictCollectSamples_CompressibleBudgetedByPayloadBytes(t *testing.T) {
	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		valueLogDictTrainer:            tr,
		valueLogDictMetricsPauseBytes:  1 << 20,
		valueLogDictProbeBytes:         64 << 10,
		valueLogDictPausedSampleStride: 256,
	}

	records := compressibleValueLogRecords(512, 4096)
	expectedBudget := db.valueLogDictCollectBudget(records, false)
	db.valueLogDictCollectSamples(records)

	if pause := db.valueLogDictPauseRemaining.Load(); pause != 0 {
		t.Fatalf("expected no pause on compressible stream, got=%d", pause)
	}
	if sampled := db.valueLogDictClassifySampled.Load(); sampled != uint64(expectedBudget) {
		t.Fatalf("expected classifier sampled=%d, got=%d", expectedBudget, sampled)
	}
	stats := tr.Stats()
	if stats.Enqueued == 0 || stats.Enqueued > uint64(expectedBudget) {
		t.Fatalf("expected trainer enqueue in (0,%d], got=%d", expectedBudget, stats.Enqueued)
	}
}

func TestValueLogDictCollectBudget_ScalesForLargeBatchSmallValues(t *testing.T) {
	db := &DB{
		valueLogDictTrain: compression.TrainConfig{
			TrainBytes: 8 << 20,
		},
	}
	records := compressibleValueLogRecords(10_000, 128)
	budget := db.valueLogDictCollectBudget(records, false)

	if budget <= valueLogDictCollectMinPerBatchRecords {
		t.Fatalf("expected scaled budget > %d for large small-value batch, got=%d", valueLogDictCollectMinPerBatchRecords, budget)
	}
	expected := compression.DefaultTrainBootstrapBytes / 128
	if expected < valueLogDictCollectMinPerBatchRecords {
		expected = valueLogDictCollectMinPerBatchRecords
	}
	if expected > valueLogDictCollectMaxPerBatchRecords {
		expected = valueLogDictCollectMaxPerBatchRecords
	}
	if budget != expected {
		t.Fatalf("expected budget=%d from bootstrap bytes, got=%d", expected, budget)
	}
}

func TestValueLogDictClassifierBypass_ArmsIncompressibleHold(t *testing.T) {
	db := &DB{
		valueLogDictIncompressibleHoldBytes:  256 << 10,
		valueLogDictIncompressibleProbeBytes: 64 << 10,
		valueLogDictMetricsPauseBytes:        1 << 20,
	}

	highEntropy := make([]byte, 4096)
	for i := range highEntropy {
		highEntropy[i] = byte(i)
	}
	if !db.valueLogDictClassifierBypass(highEntropy, false) {
		t.Fatalf("expected high-entropy sample to bypass dict path")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold == 0 {
		t.Fatalf("expected hold to arm after high-entropy hit")
	}
}

func TestValueLogDictClassifierBypass_IgnoresOuterLeafPages(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	pageValue := make([]byte, 4096)
	for i := range pageValue {
		pageValue[i] = byte(i)
	}
	if db.valueLogDictClassifierBypass(pageValue, false) {
		t.Fatalf("expected outer-leaf page value to be ignored by classifier bypass")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold from outer-leaf page sample, hold=%d", hold)
	}
}

func TestValueLogDictCollectSamples_IgnoresOuterLeafPages(t *testing.T) {
	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		indexOuterLeavesInValueLog:           true,
		valueLogDictTrainer:                  tr,
		valueLogDictMetricsPauseBytes:        1 << 20,
		valueLogDictProbeBytes:               64 << 10,
		valueLogDictPausedSampleStride:       256,
		valueLogDictIncompressibleProbeBytes: 64 << 10,
	}
	records := compressibleValueLogRecords(512, 4096)
	db.valueLogDictCollectSamples(records)
	stats := tr.Stats()
	if stats.Enqueued != 0 {
		t.Fatalf("expected no trainer enqueue for outer-leaf page samples, got=%d", stats.Enqueued)
	}
}

func TestValueLogDictClassifierBypass_AllowsLargeValuesAfterDictPublish(t *testing.T) {
	db := &DB{
		valueLogDictIncompressibleHoldBytes:  256 << 10,
		valueLogDictIncompressibleProbeBytes: 64 << 10,
		valueLogDictMetricsPauseBytes:        1 << 20,
	}
	db.valueLogDictLastAppliedDictID.Store(7)

	highEntropy := make([]byte, 43<<10)
	for i := range highEntropy {
		highEntropy[i] = byte(i)
	}
	if db.valueLogDictClassifierBypass(highEntropy, false) {
		t.Fatalf("expected large values to skip classifier bypass after dict publish")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected incompressible hold to remain inactive for large value, hold=%d", hold)
	}
}

func TestShouldBypassValueLogDictForRecords_AllowsLargeValuesAfterDictPublish(t *testing.T) {
	db := &DB{
		valueLogDictIncompressibleHoldBytes:  256 << 10,
		valueLogDictIncompressibleProbeBytes: 64 << 10,
		valueLogDictMetricsPauseBytes:        1 << 20,
	}
	db.valueLogDictLastAppliedDictID.Store(9)
	records := highEntropyValueLogRecords(8, 43<<10)
	if db.shouldBypassValueLogDictForRecords(records, false) {
		t.Fatalf("expected large record batches to bypass classifier gating after dict publish")
	}
}

func TestValueLogDictShouldAttemptCompression_IncompressibleHoldProbes(t *testing.T) {
	db := &DB{
		valueLogDictIncompressibleHoldBytes:  256 << 10,
		valueLogDictIncompressibleProbeBytes: 64 << 10,
	}
	db.armValueLogDictIncompressibleHoldBytes(0)

	attempt, probe, paused := db.valueLogDictShouldAttemptCompression(32 << 10)
	if attempt || probe || paused {
		t.Fatalf("expected hold suppression without probe; got attempt=%v probe=%v paused=%v", attempt, probe, paused)
	}

	attempt, probe, paused = db.valueLogDictShouldAttemptCompression(32 << 10)
	if !attempt || !probe {
		t.Fatalf("expected periodic probe during hold; got attempt=%v probe=%v paused=%v", attempt, probe, paused)
	}

	remaining := db.valueLogDictIncompressibleHoldRemaining.Load()
	if remaining == 0 {
		t.Fatalf("expected hold to remain active after one probe")
	}
}

func TestValueLogDictShouldAttemptCompression_LargePayloadClampsPauseProbeInterval(t *testing.T) {
	db := &DB{
		valueLogDictProbeBytes: 16 << 20,
	}
	db.valueLogDictPauseRemaining.Store(64 << 20)
	db.valueLogDictProbeRemaining.Store(16 << 20)

	attempt, probe, paused := db.valueLogDictShouldAttemptCompression(32 << 10)
	if attempt || probe || !paused {
		t.Fatalf("expected initial paused suppression; got attempt=%v probe=%v paused=%v", attempt, probe, paused)
	}
	if got := db.valueLogDictProbeRemaining.Load(); got > valueLogDictLargeProbeIntervalClampByte {
		t.Fatalf("expected probe interval to clamp to <=%d, got=%d", valueLogDictLargeProbeIntervalClampByte, got)
	}

	probed := false
	for i := 0; i < 96; i++ {
		attempt, probe, paused = db.valueLogDictShouldAttemptCompression(32 << 10)
		if attempt && probe && paused {
			probed = true
			break
		}
	}
	if !probed {
		t.Fatalf("expected paused large-payload probe within bounded attempts after clamp")
	}
}

func TestValueLogDictShouldAttemptCompression_LargePayloadPauseProbeDoesNotFireBackToBack(t *testing.T) {
	const rawLen = 3 << 20
	db := &DB{
		valueLogDictProbeBytes: 16 << 20,
	}
	db.valueLogDictPauseRemaining.Store(64 << 20)
	db.valueLogDictProbeRemaining.Store(16 << 20)

	probed := false
	for i := 0; i < 16; i++ {
		attempt, probe, paused := db.valueLogDictShouldAttemptCompression(rawLen)
		if !paused {
			t.Fatalf("expected paused=true while pause budget remains; got attempt=%v probe=%v paused=%v", attempt, probe, paused)
		}
		if attempt && probe {
			probed = true
			break
		}
	}
	if !probed {
		t.Fatalf("expected paused large-payload probe within bounded attempts")
	}

	attempt, probe, paused := db.valueLogDictShouldAttemptCompression(rawLen)
	if attempt || probe || !paused {
		t.Fatalf("expected one full payload suppression after probe; got attempt=%v probe=%v paused=%v", attempt, probe, paused)
	}
}

func TestValueLogDictShouldAttemptCompression_LargePayloadClampsIncompressibleProbeInterval(t *testing.T) {
	db := &DB{
		valueLogDictIncompressibleHoldBytes:  64 << 20,
		valueLogDictIncompressibleProbeBytes: 16 << 20,
	}
	db.armValueLogDictIncompressibleHoldBytes(0)

	attempt, probe, paused := db.valueLogDictShouldAttemptCompression(32 << 10)
	if attempt || probe {
		t.Fatalf("expected incompressible hold suppression before probe; got attempt=%v probe=%v paused=%v", attempt, probe, paused)
	}
	if got := db.valueLogDictIncompressibleProbeRemaining.Load(); got > valueLogDictLargeProbeIntervalClampByte {
		t.Fatalf("expected incompressible probe interval to clamp to <=%d, got=%d", valueLogDictLargeProbeIntervalClampByte, got)
	}

	probed := false
	for i := 0; i < 96; i++ {
		attempt, probe, paused = db.valueLogDictShouldAttemptCompression(32 << 10)
		if attempt && probe {
			probed = true
			break
		}
	}
	if !probed {
		t.Fatalf("expected incompressible-hold large-payload probe within bounded attempts after clamp")
	}
}

func TestValueLogDictIncompressibleDecision_LargePayloadProbeDoesNotFireBackToBack(t *testing.T) {
	const rawLen = uint64(3 << 20)
	db := &DB{
		valueLogDictIncompressibleHoldBytes:  64 << 20,
		valueLogDictIncompressibleProbeBytes: 16 << 20,
	}
	db.armValueLogDictIncompressibleHoldBytes(0)

	probed := false
	for i := 0; i < 16; i++ {
		attempt, probe, holding := db.valueLogDictIncompressibleDecision(rawLen, true)
		if !holding {
			t.Fatalf("expected incompressible hold to remain active during probe window; got attempt=%v probe=%v holding=%v", attempt, probe, holding)
		}
		if attempt && probe {
			probed = true
			break
		}
	}
	if !probed {
		t.Fatalf("expected incompressible large-payload probe within bounded attempts")
	}

	attempt, probe, holding := db.valueLogDictIncompressibleDecision(rawLen, true)
	if attempt || probe || !holding {
		t.Fatalf("expected one full payload suppression after incompressible probe; got attempt=%v probe=%v holding=%v", attempt, probe, holding)
	}
}

func TestValueLogDictCollectSamples_SkipsDuringIncompressibleHold(t *testing.T) {
	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		valueLogDictTrainer:                  tr,
		valueLogDictIncompressibleHoldBytes:  1 << 20,
		valueLogDictIncompressibleProbeBytes: 128 << 10,
		valueLogDictMetricsPauseBytes:        1 << 20,
		valueLogDictProbeBytes:               64 << 10,
		valueLogDictPausedSampleStride:       256,
	}
	db.valueLogDictIncompressibleHoldRemaining.Store(1 << 20)
	db.valueLogDictIncompressibleProbeRemaining.Store(128 << 10)

	records := compressibleValueLogRecords(512, 4096)
	db.valueLogDictCollectSamples(records)

	stats := tr.Stats()
	if stats.Enqueued != 0 {
		t.Fatalf("expected no trainer enqueue while incompressible hold is active, got=%d", stats.Enqueued)
	}
}
