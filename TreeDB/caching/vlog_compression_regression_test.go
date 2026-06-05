package caching

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func readFirstValueLogFrameHeader(t *testing.T, path string) valuelog.FrameHeader {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < valuelog.HeaderSize {
		t.Fatalf("value log too short: %d", len(data))
	}
	bodyLen := int(binary.LittleEndian.Uint32(data[16:20]))
	if bodyLen < valuelog.FrameHeaderSize || valuelog.HeaderSize+bodyLen > len(data) {
		t.Fatalf("invalid first value-log body length %d in file length %d", bodyLen, len(data))
	}
	header, rids, offsets, _, err := valuelog.DecodeFrame(data[valuelog.HeaderSize : valuelog.HeaderSize+bodyLen])
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if len(rids) != int(header.K) || len(offsets) != int(header.K)+1 {
		t.Fatalf("decoded frame cardinality mismatch: k=%d rids=%d offsets=%d", header.K, len(rids), len(offsets))
	}
	return header
}

func TestResolveVlogWriteMode_DefaultUsesAutoBehavior(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionDefault),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}

	mode, _, probe := db.resolveVlogWriteMode(nil, 0, 4096, 4096, false)
	if mode != vlogWriteBlock || probe {
		t.Fatalf("default mode with no dict should follow auto/block, got mode=%v probe=%v", mode, probe)
	}

	mode, _, probe = db.resolveVlogWriteMode(nil, 7, 4096, 4096, false)
	if mode != vlogWriteDict || probe {
		t.Fatalf("default mode with dict should follow auto/dict, got mode=%v probe=%v", mode, probe)
	}
}

func TestResolveVlogWriteMode_DictAggressiveFallsBackToBlockWithoutDict(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionDict),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
	}

	mode, codec, probe := db.resolveVlogWriteMode(nil, 0, 4096, 4096, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("dict/aggressive with no dict should fall back to block/snappy, got mode=%v codec=%v probe=%v", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_DictAggressiveLargeOuterLeafPayloadPrefersBlock(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:    uint8(vlogCompressionDict),
		valueLogBlockCodec:         valuelog.BlockCodecLZ4,
		valueLogAutotuneOptions:    valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		indexOuterLeavesInValueLog: true,
	}

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	l := &lane{vlogCompressionSelector: selector}
	mode, codec, probe := db.resolveVlogWriteMode(l, 7, 48<<10, 48<<10, true)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 || probe {
		t.Fatalf("dict/aggressive large outer-leaf payload should prefer block/lz4, got mode=%v codec=%v probe=%v", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_DictAggressiveSizeLargeOuterLeafPayloadCanUseDict(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:    uint8(vlogCompressionDict),
		valueLogAutoPolicy:         uint8(vlogAutoSize),
		valueLogBlockCodec:         valuelog.BlockCodecLZ4,
		valueLogAutotuneOptions:    valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		indexOuterLeavesInValueLog: true,
	}

	selector := newVlogCompressionSelector(vlogAutoSize, 0, 0)
	selector.dwellBytes = 0
	selector.currentCandidate = vlogAutoCandidateBlockLZ4
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.74, throughput: 1.05, samples: 16}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.10, samples: 16}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.08, throughput: 1.12, samples: 16}

	l := &lane{vlogCompressionSelector: selector}
	mode, codec, probe := db.resolveVlogWriteMode(l, 7, 48<<10, 48<<10, true)
	if mode != vlogWriteDict || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("dict/aggressive+size large outer-leaf payload should allow dict selector path, got mode=%v codec=%v probe=%v", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_DictAggressiveSelectorBlockUsesConfiguredCodec(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionDict),
		valueLogBlockCodec:      valuelog.BlockCodecLZ4,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
	}

	selector := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	selector.dwellBytes = 0
	selector.currentCandidate = vlogAutoCandidateBlockSnappy
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.10, samples: 16}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.78, throughput: 1.02, samples: 16}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.98, throughput: 0.50, samples: 16}

	l := &lane{vlogCompressionSelector: selector}
	mode, codec, probe := db.resolveVlogWriteMode(l, 7, 4096, 4096, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 || probe {
		t.Fatalf("dict/aggressive selector block fallback should use configured block codec, got mode=%v codec=%v probe=%v", mode, codec, probe)
	}
}

func TestAppendValueLogOne_DictAggressiveWithoutDictRecordsBlockStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionDict),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		valueLogBlockTargetBytes: 4096,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	value := bytes.Repeat([]byte("dict-aggressive-block-fallback-"), 2048)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 0, nil, 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	ratioSnap := snapshotLaneVlogBlockRatio(&db.lanes[0])
	if ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecSnappy)] == 0 {
		t.Fatalf("expected block ratio sample in dict/aggressive no-dict fallback path")
	}

	selectorSnap := selector.snapshot()
	blockFrames := selectorSnap.framesByCandidate[vlogAutoCandidateBlockSnappy] + selectorSnap.framesByCandidate[vlogAutoCandidateBlockLZ4]
	if blockFrames == 0 {
		t.Fatalf("expected selector block frame observation in dict/aggressive no-dict fallback path")
	}
}

func TestAppendValueLogOne_DictAggressiveOuterLeafBypassUsesBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	db := &DB{
		closeCh:                    make(chan struct{}),
		valueLogCompressionMode:    uint8(vlogCompressionDict),
		valueLogBlockCodec:         valuelog.BlockCodecLZ4,
		valueLogAutotuneOptions:    valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	// Explicit outer-leaf magic payloads should stay on block codecs instead of
	// dict mode.
	value := bytes.Repeat([]byte("outer-leaf-page-like-payload-"), 160)
	if len(value) < page.PageSize {
		value = append(value, bytes.Repeat([]byte("x"), page.PageSize-len(value))...)
	}
	value = value[:page.PageSize]
	value = markOuterLeafMagic(value)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 7, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected outer-leaf bypass to record block write mode frame")
	}
	if writeSnap.Frames[vlogWriteDict] != 0 {
		t.Fatalf("expected no dict write-mode frame for outer-leaf bypass path")
	}
}

func TestAppendValueLogOne_DictModeOuterLeafBypassFallsBackToBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                    make(chan struct{}),
		valueLogCompressionMode:    uint8(vlogCompressionDict),
		valueLogBlockCodec:         valuelog.BlockCodecLZ4,
		valueLogAutotuneOptions:    valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
		lanes: []lane{
			{id: 0, vlog: writer},
		},
	}

	value := bytes.Repeat([]byte("outer-leaf-page-like-payload-"), 160)
	if len(value) < page.PageSize {
		value = append(value, bytes.Repeat([]byte("x"), page.PageSize-len(value))...)
	}
	value = value[:page.PageSize]
	value = markOuterLeafMagic(value)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 7, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected outer-leaf bypass in dict mode to fall back to block")
	}
	if writeSnap.Frames[vlogWriteDict] != 0 {
		t.Fatalf("expected no dict write-mode frame for outer-leaf bypass path")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no off/raw write-mode frame for outer-leaf bypass path")
	}
}

func TestAppendValueLogOne_LeafLaneAutoBalancedPrefersLZ4(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "leaf.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                    make(chan struct{}),
		valueLogCompressionMode:    uint8(vlogCompressionAuto),
		valueLogAutoPolicy:         uint8(vlogAutoBalanced),
		valueLogBlockCodec:         valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
	}
	db.leafLog = lane{
		id:                      leafLogLaneID,
		vlog:                    writer,
		vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0),
	}

	value := bytes.Repeat([]byte("l"), page.PageSize)
	ptr, _, err := db.appendValueLogOne(&db.leafLog, 0, nil, 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}
	if got := db.leafLog.vlogBlockCodec; got != valuelog.BlockCodecLZ4 {
		t.Fatalf("leaf lane block codec=%v want lz4", got)
	}

	ratioSnap := snapshotLaneVlogBlockRatio(&db.leafLog)
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecLZ4)]; got == 0 {
		t.Fatalf("expected leaf lane to record lz4 sample")
	}
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecSnappy)]; got != 0 {
		t.Fatalf("expected no snappy sample on leaf-lane lz4 override, got=%d", got)
	}
}

func TestAppendValueLogOne_NonLeafLaneAutoBalancedKeepsConfiguredCodec(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes: 4096,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)},
		},
	}

	value := bytes.Repeat([]byte("v"), page.PageSize)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 0, nil, 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}
	if got := db.lanes[0].vlogBlockCodec; got != valuelog.BlockCodecSnappy {
		t.Fatalf("non-leaf lane block codec=%v want snappy", got)
	}

	ratioSnap := snapshotLaneVlogBlockRatio(&db.lanes[0])
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecSnappy)]; got == 0 {
		t.Fatalf("expected non-leaf lane to record snappy sample")
	}
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecLZ4)]; got != 0 {
		t.Fatalf("expected no lz4 sample on non-leaf lane, got=%d", got)
	}
}

func TestAppendValueLog_AutoForcePointerMediumPayloadUsesGroupedBlockFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)},
		},
	}

	base := bytes.Repeat([]byte(`{"commit":{"operation":"create","collection":"app.bsky.feed.post"},"kind":"commit","did":"did:plc:storage-parity"}`), 8)
	value := base[:forcePointerAutoBlockMinPayloadBytes+128]
	records := []valuelog.Record{
		{RID: 1, Value: value},
		{RID: 2, Value: value},
		{RID: 3, Value: value},
		{RID: 4, Value: value},
	}
	ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != len(records) {
		t.Fatalf("ptr count=%d want %d", len(ptrs), len(records))
	}
	for i, ptr := range ptrs {
		if ptr == (page.ValuePtr{}) {
			t.Fatalf("empty pointer at %d", i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header := readFirstValueLogFrameHeader(t, path)
	if header.K != uint8(len(records)) {
		t.Fatalf("frame K=%d want grouped K=%d", header.K, len(records))
	}
	if header.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected forced-pointer medium payload batch to store a block-compressed frame")
	}
	if header.DictID != 0 {
		t.Fatalf("expected no dict id for block-compressed retained payload, got %d", header.DictID)
	}
	if got := valuelog.BlockCodec(header.Reserved); got != valuelog.BlockCodecSnappy {
		t.Fatalf("block codec=%v want snappy", got)
	}
}

func TestAppendValueLog_AutoBalancedForcePointerHighEntropyPayloadUsesGroupedBlockFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)},
		},
	}

	records := make([]valuelog.Record, 4)
	for i := range records {
		value := make([]byte, forcePointerAutoBlockMinPayloadBytes)
		for j := range value {
			value[j] = byte(j + i*17)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != len(records) {
		t.Fatalf("ptr count=%d want %d", len(ptrs), len(records))
	}
	for i, ptr := range ptrs {
		if ptr == (page.ValuePtr{}) {
			t.Fatalf("empty pointer at %d", i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header := readFirstValueLogFrameHeader(t, path)
	if header.K != uint8(len(records)) {
		t.Fatalf("frame K=%d want grouped K=%d", header.K, len(records))
	}
	if header.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected balanced forced-pointer batch to store a block-compressed frame")
	}
	if header.DictID != 0 {
		t.Fatalf("expected no dict id for block-compressed high-entropy payload, got %d", header.DictID)
	}
	if got := valuelog.BlockCodec(header.Reserved); got != valuelog.BlockCodecSnappy {
		t.Fatalf("block codec=%v want snappy", got)
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected block write-mode observation")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no raw/off write-mode observation for balanced forced-pointer payload")
	}
	selectorSnap := db.lanes[0].vlogCompressionSelector.snapshot()
	if selectorSnap.framesByCandidate[vlogAutoCandidateOff] != 0 {
		t.Fatalf("expected stable block forced-pointer batch not to train selector off frames")
	}
}

func TestAppendValueLog_AutoBalancedForcePointerDictSelectorOffUsesGroupedBlockFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	selector.exploreBytes = 0
	selector.exploreRemaining = 0
	selector.currentCandidate = vlogAutoCandidateOff
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	base := bytes.Repeat([]byte(`{"commit":{"operation":"create","collection":"app.bsky.feed.post"},"kind":"commit","did":"did:plc:storage-parity","text":"value-log-retained-json"}`), 6)
	value := base[:forcePointerAutoBlockMinPayloadBytes+128]
	records := []valuelog.Record{
		{RID: 1, Value: value},
		{RID: 2, Value: value},
		{RID: 3, Value: value},
		{RID: 4, Value: value},
	}
	ptrs, err := db.appendValueLog(&db.lanes[0], 7, []byte("stub-dict"), records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != len(records) {
		t.Fatalf("ptr count=%d want %d", len(ptrs), len(records))
	}
	for i, ptr := range ptrs {
		if ptr == (page.ValuePtr{}) {
			t.Fatalf("empty pointer at %d", i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header := readFirstValueLogFrameHeader(t, path)
	if header.K != uint8(len(records)) {
		t.Fatalf("frame K=%d want grouped K=%d", header.K, len(records))
	}
	if header.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected storage-first forced-pointer dict-available batch to store a block-compressed frame")
	}
	if header.DictID != 0 {
		t.Fatalf("expected selector-off remap to store block frame without dict id, got %d", header.DictID)
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected block write-mode observation")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no raw/off write-mode observation for storage-first forced-pointer dict path")
	}
}

func TestAppendValueLog_AutoThroughputForcePointerHighEntropyPayloadUsesGroupedRawFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoThroughput),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: newVlogCompressionSelector(vlogAutoThroughput, 0, 0)},
		},
	}

	records := make([]valuelog.Record, 4)
	for i := range records {
		value := make([]byte, forcePointerAutoBlockMinPayloadBytes)
		for j := range value {
			value[j] = byte(j + i*17)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != len(records) {
		t.Fatalf("ptr count=%d want %d", len(ptrs), len(records))
	}
	for i, ptr := range ptrs {
		if ptr == (page.ValuePtr{}) {
			t.Fatalf("empty pointer at %d", i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header := readFirstValueLogFrameHeader(t, path)
	if header.K != uint8(len(records)) {
		t.Fatalf("frame K=%d want grouped K=%d", header.K, len(records))
	}
	if header.Flags&valuelog.FrameFlagCompressed != 0 {
		t.Fatalf("expected throughput high-entropy forced-pointer batch to bypass block compression")
	}
	if header.DictID != 0 {
		t.Fatalf("expected no dict id for raw high-entropy payload, got %d", header.DictID)
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteOff] == 0 {
		t.Fatalf("expected raw/off write-mode observation")
	}
	if writeSnap.Frames[vlogWriteBlock] != 0 {
		t.Fatalf("expected no block write-mode observation for bypassed high-entropy payload")
	}
	selectorSnap := db.lanes[0].vlogCompressionSelector.snapshot()
	if selectorSnap.framesByCandidate[vlogAutoCandidateOff] != 0 {
		t.Fatalf("expected bypassed raw batch not to train selector off frames")
	}
}

func TestAppendValueLog_DictModeOuterLeafBypassFallsBackToBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                    make(chan struct{}),
		valueLogCompressionMode:    uint8(vlogCompressionDict),
		valueLogBlockCodec:         valuelog.BlockCodecLZ4,
		valueLogAutotuneOptions:    valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
		lanes: []lane{
			{id: 0, vlog: writer},
		},
	}

	value := bytes.Repeat([]byte("outer-leaf-page-like-payload-"), 160)
	if len(value) < page.PageSize {
		value = append(value, bytes.Repeat([]byte("x"), page.PageSize-len(value))...)
	}
	value = value[:page.PageSize]
	value = markOuterLeafMagic(value)

	records := []valuelog.Record{{RID: 1, Value: value}}
	ptrs, err := db.appendValueLog(&db.lanes[0], 7, []byte("stub-dict"), records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != 1 || ptrs[0] == (page.ValuePtr{}) {
		t.Fatalf("expected one non-empty pointer, got %v", ptrs)
	}

	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected outer-leaf batch bypass in dict mode to fall back to block")
	}
	if writeSnap.Frames[vlogWriteDict] != 0 {
		t.Fatalf("expected no dict write-mode frame for outer-leaf batch bypass path")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no off/raw write-mode frame for outer-leaf batch bypass path")
	}
}

func TestAppendValueLogOne_AutoOuterLeafDictBypassReappliesNoDictBlockCodec(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	selector.currentCandidate = vlogAutoCandidateBlockLZ4
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.69, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.40, throughput: 0.20, samples: 16}

	db := &DB{
		closeCh:                    make(chan struct{}),
		valueLogCompressionMode:    uint8(vlogCompressionAuto),
		valueLogAutoPolicy:         uint8(vlogAutoBalanced),
		valueLogBlockCodec:         valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	value := bytes.Repeat([]byte("outer-leaf-page-like-payload-"), 160)
	if len(value) < page.PageSize {
		value = append(value, bytes.Repeat([]byte("x"), page.PageSize-len(value))...)
	}
	value = value[:page.PageSize]

	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 7, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected block frame for outer-leaf dict bypass in auto mode")
	}
	ratioSnap := snapshotLaneVlogBlockRatio(&db.lanes[0])
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecSnappy)]; got == 0 {
		t.Fatalf("expected snappy sample on no-dict fallback path")
	}
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecLZ4)]; got != 0 {
		t.Fatalf("expected no lz4 sample after no-dict fallback normalization, got %d", got)
	}
}

func TestAppendValueLog_AutoOuterLeafDictBypassReappliesNoDictBlockCodec(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	selector.currentCandidate = vlogAutoCandidateBlockLZ4
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.69, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.40, throughput: 0.20, samples: 16}

	db := &DB{
		closeCh:                    make(chan struct{}),
		valueLogCompressionMode:    uint8(vlogCompressionAuto),
		valueLogAutoPolicy:         uint8(vlogAutoBalanced),
		valueLogBlockCodec:         valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	value := bytes.Repeat([]byte("outer-leaf-page-like-payload-"), 160)
	if len(value) < page.PageSize {
		value = append(value, bytes.Repeat([]byte("x"), page.PageSize-len(value))...)
	}
	value = value[:page.PageSize]
	records := []valuelog.Record{{RID: 1, Value: value}}

	ptrs, err := db.appendValueLog(&db.lanes[0], 7, []byte("stub-dict"), records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != 1 || ptrs[0] == (page.ValuePtr{}) {
		t.Fatalf("expected one non-empty pointer, got %v", ptrs)
	}

	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected block frame for outer-leaf dict bypass batch in auto mode")
	}
	ratioSnap := snapshotLaneVlogBlockRatio(&db.lanes[0])
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecSnappy)]; got == 0 {
		t.Fatalf("expected snappy sample on no-dict batch fallback path")
	}
	if got := ratioSnap.Samples[vlogBlockCodecIndex(valuelog.BlockCodecLZ4)]; got != 0 {
		t.Fatalf("expected no lz4 sample after no-dict batch fallback normalization, got %d", got)
	}
}

func TestAppendValueLogOne_BlockModeSingleWriteTracksCompressedRatio(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		valueLogBlockTargetBytes: 4096,
		lanes: []lane{
			{id: 0, vlog: writer},
		},
	}

	value := bytes.Repeat([]byte("block-mode-single-write-compressible-"), 2048)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 0, nil, 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	snap := snapshotLaneVlogBlockRatio(&db.lanes[0])
	idx := vlogBlockCodecIndex(valuelog.BlockCodecSnappy)
	if snap.Samples[idx] == 0 {
		t.Fatalf("expected block ratio samples > 0")
	}
	if snap.Ratio[idx] >= 0.99 {
		t.Fatalf("expected compressed ratio < 0.99 for highly compressible payload, got %.6f", snap.Ratio[idx])
	}
}

func TestAppendValueLogOne_AutoFallsBackToBlockWhenDictSuppressed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	selector.currentCandidate = vlogAutoCandidateDict
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.93, throughput: 0.95, samples: 8}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.94, throughput: 0.95, samples: 8}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.65, throughput: 0.95, samples: 8}

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}
	// Force dict suppression in valueLogDictShouldAttemptCompression.
	db.valueLogDictPauseRemaining.Store(1 << 20)
	db.valueLogDictProbeBytes = 0

	value := bytes.Repeat([]byte("auto-fallback-block-"), 2048)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 7, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	snap := selector.snapshot()
	blockFrames := snap.framesByCandidate[vlogAutoCandidateBlockSnappy] + snap.framesByCandidate[vlogAutoCandidateBlockLZ4]
	if blockFrames == 0 {
		t.Fatalf("expected auto fallback to record block frame when dict is suppressed")
	}
	if snap.framesByCandidate[vlogAutoCandidateOff] != 0 {
		t.Fatalf("expected no off-mode frame when fallback-to-block is active")
	}
}

func TestAppendValueLogOne_AutoHoldOffSkipsDictSampling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 1<<20, 64<<10)
	selector.currentCandidate = vlogAutoCandidateOff
	selector.holdRemaining = 1 << 20
	selector.probeRemaining = 1 << 20

	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		valueLogDictTrainer:     tr,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	value := bytes.Repeat([]byte("auto-hold-off-no-sample-"), 300)
	before := tr.Stats().Enqueued
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 11, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}
	after := tr.Stats().Enqueued
	if after != before {
		t.Fatalf("expected no dict sample collection while auto selector is in off-hold, enqueued before=%d after=%d", before, after)
	}
}
