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

func markOuterLeafMagic(value []byte) []byte {
	out := make([]byte, len(value))
	copy(out, value)
	if len(out) >= 4 {
		copy(out, []byte{'T', 'O', 'L', '2'})
	}
	return out
}

func markOuterLeafMagicRecords(records []valuelog.Record) []valuelog.Record {
	out := make([]valuelog.Record, len(records))
	copy(out, records)
	for i := range out {
		out[i].Value = markOuterLeafMagic(out[i].Value)
	}
	return out
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

	records := markOuterLeafMagicRecords(compressibleValueLogRecords(512, 4096))
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

func TestValueLogDictClassifierBypass_BypassesOuterLeafPagesWithoutArmingHold(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	pageValue := make([]byte, 4096)
	for i := range pageValue {
		pageValue[i] = byte(i)
	}
	pageValue = markOuterLeafMagic(pageValue)
	if !db.valueLogDictClassifierBypass(pageValue, false) {
		t.Fatalf("expected outer-leaf page value to bypass dict path")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold from outer-leaf page sample, hold=%d", hold)
	}
}

func TestValueLogDictClassifierBypass_AllowsOuterLeafPagesForDictSizeAggressive(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:             uint8(vlogCompressionDict),
		valueLogAutoPolicy:                  uint8(vlogAutoSize),
		valueLogAutotuneOptions:             valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	pageValue := markOuterLeafMagic(bytes.Repeat([]byte("a"), 4096))
	if db.valueLogDictClassifierBypass(pageValue, false) {
		t.Fatalf("expected outer-leaf page value to stay on dict path in dict+aggressive+size mode")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold for compressible outer-leaf page sample, hold=%d", hold)
	}
}

func TestValueLogDictClassifierBypass_AllowsOuterLeafPagesForDictSizeMedium(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:             uint8(vlogCompressionDict),
		valueLogAutoPolicy:                  uint8(vlogAutoSize),
		valueLogAutotuneOptions:             valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	pageValue := markOuterLeafMagic(bytes.Repeat([]byte("a"), 4096))
	if db.valueLogDictClassifierBypass(pageValue, false) {
		t.Fatalf("expected outer-leaf page value to stay on dict path in dict+size mode when autotune is enabled")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold for compressible outer-leaf page sample, hold=%d", hold)
	}
}

func TestValueLogDictClassifierBypass_AllowsOuterLeafPagesForAutoSize(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:             uint8(vlogCompressionAuto),
		valueLogAutoPolicy:                  uint8(vlogAutoSize),
		valueLogAutotuneOptions:             valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	pageValue := markOuterLeafMagic(bytes.Repeat([]byte("a"), 4096))
	if db.valueLogDictClassifierBypass(pageValue, false) {
		t.Fatalf("expected outer-leaf page value to stay on dict path in auto+size mode")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold for compressible outer-leaf page sample, hold=%d", hold)
	}
}

func TestValueLogDictClassifierBypass_AllowsOuterLeafHighEntropyForAutoSize(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:             uint8(vlogCompressionAuto),
		valueLogAutoPolicy:                  uint8(vlogAutoSize),
		valueLogAutotuneOptions:             valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	pageValue := make([]byte, 4096)
	for i := range pageValue {
		pageValue[i] = byte(i)
	}
	pageValue = markOuterLeafMagic(pageValue)
	if db.valueLogDictClassifierBypass(pageValue, false) {
		t.Fatalf("expected outer-leaf page value to stay on dict path in auto+size mode even when sample entropy is high")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold for outer-leaf page sample, hold=%d", hold)
	}
}

func TestValueLogDictClassifierBypass_BypassesOuterLeafMagicPayloadWithoutArmingHold(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	payload := bytes.Repeat([]byte("x"), 48<<10)
	copy(payload, []byte{'T', 'O', 'L', '2'})
	if !db.valueLogDictClassifierBypass(payload, false) {
		t.Fatalf("expected outer-leaf magic payload to bypass dict path")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold from outer-leaf magic payload, hold=%d", hold)
	}
}

func TestClassifyVlogPayloadKindForValue(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	outerLeafPage := markOuterLeafMagic(make([]byte, 4096))
	if got := db.classifyVlogPayloadKindForValue(outerLeafPage); got != vlogPayloadKindOuterLeaf {
		t.Fatalf("expected outer-leaf kind for 4KiB page payload, got=%v", got)
	}
	if got := db.classifyVlogPayloadKindForValue([]byte("small-value")); got != vlogPayloadKindSingleValue {
		t.Fatalf("expected single-value kind for non-outer payload, got=%v", got)
	}
}

func TestClassifyVlogPayloadKindForRecords(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	outerLeafPage := markOuterLeafMagic(make([]byte, 4096))
	singleValue := []byte("single-value")

	records := []valuelog.Record{
		{RID: 1, Value: outerLeafPage},
		{RID: 2, Value: outerLeafPage},
	}
	if got := db.classifyVlogPayloadKindForRecords(records); got != vlogPayloadKindOuterLeaf {
		t.Fatalf("expected outer-leaf kind for outer-only batch, got=%v", got)
	}

	records = []valuelog.Record{
		{RID: 1, Value: singleValue},
		{RID: 2, Value: singleValue},
	}
	if got := db.classifyVlogPayloadKindForRecords(records); got != vlogPayloadKindSingleValue {
		t.Fatalf("expected single-value kind for non-outer batch, got=%v", got)
	}

	records = []valuelog.Record{
		{RID: 1, Value: outerLeafPage},
		{RID: 2, Value: singleValue},
	}
	if got := db.classifyVlogPayloadKindForRecords(records); got != vlogPayloadKindMixed {
		t.Fatalf("expected mixed kind for mixed batch, got=%v", got)
	}
}

func TestClassifyVlogPayloadSplitForRecords(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	outerLeafPage := markOuterLeafMagic(make([]byte, 4096))
	singleValue := []byte("single-value")

	records := []valuelog.Record{
		{RID: 1, Value: outerLeafPage},
		{RID: 2, Value: outerLeafPage},
		{RID: 3, Value: singleValue},
	}
	split := db.classifyVlogPayloadSplitForRecords(records)
	if split.Kind != vlogPayloadKindMixed {
		t.Fatalf("expected mixed split kind, got=%v", split.Kind)
	}
	if split.OuterLeafRecords != 2 {
		t.Fatalf("expected outer-leaf records=2, got=%d", split.OuterLeafRecords)
	}
	if split.SingleValueRecords != 1 {
		t.Fatalf("expected single-value records=1, got=%d", split.SingleValueRecords)
	}
	if split.OuterLeafRawBytes != 2*len(outerLeafPage) {
		t.Fatalf("expected outer-leaf raw bytes=%d, got=%d", 2*len(outerLeafPage), split.OuterLeafRawBytes)
	}
	if split.SingleValueRawBytes != len(singleValue) {
		t.Fatalf("expected single-value raw bytes=%d, got=%d", len(singleValue), split.SingleValueRawBytes)
	}
}

func TestValueLogDictClassRangesForRecords_SplitOuterLeaf(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog: true,
		valueLogDictClassMode:      uint8(vlogDictClassModeSplitOuterLeaf),
	}
	outerLeafPage := markOuterLeafMagic(make([]byte, 4096))
	singleValue := []byte("single-value")
	records := []valuelog.Record{
		{RID: 1, Value: outerLeafPage},
		{RID: 2, Value: outerLeafPage},
		{RID: 3, Value: singleValue},
		{RID: 4, Value: singleValue},
		{RID: 5, Value: outerLeafPage},
	}

	ranges := db.valueLogDictClassRangesForRecords(records)
	if len(ranges) != 3 {
		t.Fatalf("expected 3 class ranges, got=%d", len(ranges))
	}
	if ranges[0].start != 0 || ranges[0].end != 2 || ranges[0].class != vlogDictClassOuterLeaf {
		t.Fatalf("unexpected range[0]=%+v", ranges[0])
	}
	if ranges[1].start != 2 || ranges[1].end != 4 || ranges[1].class != vlogDictClassSingleValue {
		t.Fatalf("unexpected range[1]=%+v", ranges[1])
	}
	if ranges[2].start != 4 || ranges[2].end != 5 || ranges[2].class != vlogDictClassOuterLeaf {
		t.Fatalf("unexpected range[2]=%+v", ranges[2])
	}
}

func TestValueLogDictClassRangesForRecords_SingleMode(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog: true,
		valueLogDictClassMode:      uint8(vlogDictClassModeSingle),
	}
	outerLeafPage := markOuterLeafMagic(make([]byte, 4096))
	singleValue := []byte("single-value")
	records := []valuelog.Record{
		{RID: 1, Value: outerLeafPage},
		{RID: 2, Value: singleValue},
		{RID: 3, Value: outerLeafPage},
	}

	ranges := db.valueLogDictClassRangesForRecords(records)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 class range in single mode, got=%d", len(ranges))
	}
	if ranges[0].start != 0 || ranges[0].end != len(records) || ranges[0].class != vlogDictClassSingleValue {
		t.Fatalf("unexpected single-mode range=%+v", ranges[0])
	}
}

func TestClassifyVlogOuterLeafCodecKindForValue(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	legacy := make([]byte, 4096)
	if got := db.classifyVlogOuterLeafCodecKindForValue(legacy); got != vlogOuterLeafCodecMixed {
		t.Fatalf("expected non-magic 4KiB payload to be treated as mixed/non-outer, got=%v", got)
	}

	magic := make([]byte, 64)
	copy(magic, []byte{'T', 'O', 'L', '2'})
	magic[outerLeafCodecHeaderOffset] = outerLeafCodecNoneID
	if got := db.classifyVlogOuterLeafCodecKindForValue(magic); got != vlogOuterLeafCodecNone {
		t.Fatalf("expected none outer-leaf codec kind, got=%v", got)
	}
	magic[outerLeafCodecHeaderOffset] = outerLeafCodecSnappyID
	if got := db.classifyVlogOuterLeafCodecKindForValue(magic); got != vlogOuterLeafCodecSnappy {
		t.Fatalf("expected snappy outer-leaf codec kind, got=%v", got)
	}
	magic[outerLeafCodecHeaderOffset] = outerLeafCodecLZ4ID
	if got := db.classifyVlogOuterLeafCodecKindForValue(magic); got != vlogOuterLeafCodecLZ4 {
		t.Fatalf("expected lz4 outer-leaf codec kind, got=%v", got)
	}
	magic[outerLeafCodecHeaderOffset] = 99
	if got := db.classifyVlogOuterLeafCodecKindForValue(magic); got != vlogOuterLeafCodecUnknown {
		t.Fatalf("expected unknown outer-leaf codec kind, got=%v", got)
	}
	if got := db.classifyVlogOuterLeafCodecKindForValue([]byte("single-value")); got != vlogOuterLeafCodecMixed {
		t.Fatalf("expected mixed outer-leaf codec kind for non-outer payload, got=%v", got)
	}
}

func TestClassifyVlogOuterLeafCodecKindForRecords(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true}
	lz4Magic := make([]byte, 64)
	copy(lz4Magic, []byte{'T', 'O', 'L', '2'})
	lz4Magic[outerLeafCodecHeaderOffset] = outerLeafCodecLZ4ID
	snappyMagic := make([]byte, 64)
	copy(snappyMagic, []byte{'T', 'O', 'L', '2'})
	snappyMagic[outerLeafCodecHeaderOffset] = outerLeafCodecSnappyID

	records := []valuelog.Record{
		{RID: 1, Value: lz4Magic},
		{RID: 2, Value: lz4Magic},
	}
	if got := db.classifyVlogOuterLeafCodecKindForRecords(records); got != vlogOuterLeafCodecLZ4 {
		t.Fatalf("expected lz4 outer-leaf codec kind for homogeneous batch, got=%v", got)
	}

	records = []valuelog.Record{
		{RID: 1, Value: lz4Magic},
		{RID: 2, Value: snappyMagic},
	}
	if got := db.classifyVlogOuterLeafCodecKindForRecords(records); got != vlogOuterLeafCodecMixed {
		t.Fatalf("expected mixed outer-leaf codec kind for mixed codecs, got=%v", got)
	}

	records = []valuelog.Record{
		{RID: 1, Value: []byte("single-value")},
		{RID: 2, Value: lz4Magic},
	}
	if got := db.classifyVlogOuterLeafCodecKindForRecords(records); got != vlogOuterLeafCodecMixed {
		t.Fatalf("expected mixed outer-leaf codec kind for non-outer records, got=%v", got)
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
	records := markOuterLeafMagicRecords(compressibleValueLogRecords(512, 4096))
	db.valueLogDictCollectSamples(records)
	stats := tr.Stats()
	if stats.Enqueued != 0 {
		t.Fatalf("expected no trainer enqueue for outer-leaf page samples, got=%d", stats.Enqueued)
	}
}

func TestValueLogDictCollectSampleForLane_SplitLeafLaneUsesOuterLeafTrainer(t *testing.T) {
	singleTrainer := newValueLogDictClassifierTrainer(t)
	outerTrainer := newValueLogDictClassifierTrainer(t)
	leafPage := buildSparseLeafPageForLeafLogTest(t)
	payload, compacted, err := valuelog.MaybeCompactLeafLogPayload(leafPage)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatal("expected sparse test leaf page to compact")
	}
	db := &DB{
		indexOuterLeavesInValueLog: true,
		valueLogDictClassMode:      uint8(vlogDictClassModeSplitOuterLeaf),
		valueLogDictTrainer:        singleTrainer,
		valueLogDictTrainerByClass: [vlogDictClassCount]*compression.Trainer{
			vlogDictClassSingleValue: singleTrainer,
			vlogDictClassOuterLeaf:   outerTrainer,
		},
	}

	db.valueLogDictCollectSampleForLane(&lane{id: leafLogLaneID}, payload)

	if got := outerTrainer.Stats().Enqueued; got == 0 {
		t.Fatalf("expected outer-leaf trainer enqueue, got=%d", got)
	}
	if got := singleTrainer.Stats().Enqueued; got != 0 {
		t.Fatalf("expected single-value trainer to stay idle, got=%d", got)
	}
}

func TestValueLogDictCollectSampleForLane_SplitLeafLaneRoutesRawLeafPages(t *testing.T) {
	singleTrainer := newValueLogDictClassifierTrainer(t)
	outerTrainer := newValueLogDictClassifierTrainer(t)
	db := &DB{
		indexOuterLeavesInValueLog: true,
		valueLogDictClassMode:      uint8(vlogDictClassModeSplitOuterLeaf),
		valueLogDictTrainer:        singleTrainer,
		valueLogDictTrainerByClass: [vlogDictClassCount]*compression.Trainer{
			vlogDictClassSingleValue: singleTrainer,
			vlogDictClassOuterLeaf:   outerTrainer,
		},
	}

	db.valueLogDictCollectSampleForLane(&lane{id: leafLogLaneID}, buildSparseLeafPageForLeafLogTest(t))

	if got := outerTrainer.Stats().Enqueued; got == 0 {
		t.Fatalf("expected raw leaf page to reach outer-leaf trainer, got=%d", got)
	}
	if got := singleTrainer.Stats().Enqueued; got != 0 {
		t.Fatalf("expected raw leaf page to skip single-value trainer, got=%d", got)
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

func TestShouldBypassValueLogDictForRecords_OuterLeafPagesBypassDict(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	records := markOuterLeafMagicRecords(compressibleValueLogRecords(8, 4096))
	if !db.shouldBypassValueLogDictForRecords(records, false) {
		t.Fatalf("expected outer-leaf record batch to bypass dict path")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold from outer-leaf record batch, hold=%d", hold)
	}
}

func TestShouldBypassValueLogDictForRecords_MixedBatchWithIgnoredSamplesDoesNotBypass(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	records := make([]valuelog.Record, 8)
	for i := range records {
		var payload []byte
		if i%2 == 0 {
			// Sampled positions (step=2) look like outer-leaf pages.
			payload = markOuterLeafMagic(bytes.Repeat([]byte("o"), 4096))
		} else {
			// Non-sampled positions are regular values and should keep dict eligible.
			payload = bytes.Repeat([]byte("regular-value-"), 400)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: payload}
	}
	if db.shouldBypassValueLogDictForRecords(records, false) {
		t.Fatalf("expected mixed batch with sparse ignored samples to remain dict-eligible")
	}
}

func TestShouldBypassValueLogDictForRecords_OuterLeafPagesAllowedForDictSizeAggressive(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:             uint8(vlogCompressionDict),
		valueLogAutoPolicy:                  uint8(vlogAutoSize),
		valueLogAutotuneOptions:             valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	records := make([]valuelog.Record, 8)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: markOuterLeafMagic(bytes.Repeat([]byte("a"), 4096))}
	}
	if db.shouldBypassValueLogDictForRecords(records, false) {
		t.Fatalf("expected outer-leaf record batch to stay on dict path in dict+aggressive+size mode")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold for compressible outer-leaf record batch, hold=%d", hold)
	}
}

func TestShouldBypassValueLogDictForRecords_OuterLeafPagesAllowedForAutoSize(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:             uint8(vlogCompressionAuto),
		valueLogAutoPolicy:                  uint8(vlogAutoSize),
		valueLogAutotuneOptions:             valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	records := markOuterLeafMagicRecords(highEntropyValueLogRecords(8, 4096))
	if db.shouldBypassValueLogDictForRecords(records, false) {
		t.Fatalf("expected outer-leaf record batch to stay on dict path in auto+size mode")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold for outer-leaf record batch, hold=%d", hold)
	}
}

func TestShouldBypassValueLogDictForRecords_OuterLeafMagicPayloadsBypassDict(t *testing.T) {
	db := &DB{
		indexOuterLeavesInValueLog:          true,
		valueLogDictIncompressibleHoldBytes: 256 << 10,
		valueLogDictMetricsPauseBytes:       1 << 20,
	}
	records := make([]valuelog.Record, 8)
	for i := range records {
		payload := bytes.Repeat([]byte("x"), 48<<10)
		copy(payload, []byte{'T', 'O', 'L', '2'})
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: payload}
	}
	if !db.shouldBypassValueLogDictForRecords(records, false) {
		t.Fatalf("expected outer-leaf magic record batch to bypass dict path")
	}
	if hold := db.valueLogDictIncompressibleHoldRemaining.Load(); hold != 0 {
		t.Fatalf("expected no incompressible hold from outer-leaf magic record batch, hold=%d", hold)
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
