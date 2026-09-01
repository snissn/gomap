package caching

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func readFirstValueLogFrameHeader(t *testing.T, path string) valuelog.FrameHeader {
	t.Helper()

	return readValueLogFrameHeaderAt(t, path, 0)
}

func readValueLogFrameHeaderAt(t *testing.T, path string, index int) valuelog.FrameHeader {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if index < 0 {
		t.Fatalf("negative value-log frame index %d", index)
	}
	frameIndex := 0
	for pos := 0; pos < len(data); frameIndex++ {
		if len(data)-pos < valuelog.HeaderSize {
			t.Fatalf("truncated value-log frame header at offset %d in file length %d", pos, len(data))
		}
		bodyLen := int(binary.LittleEndian.Uint32(data[pos+16 : pos+20]))
		end := pos + valuelog.HeaderSize + bodyLen
		if bodyLen < valuelog.FrameHeaderSize || end > len(data) {
			t.Fatalf("invalid value-log body length %d at offset %d in file length %d", bodyLen, pos, len(data))
		}
		header, rids, offsets, _, err := valuelog.DecodeFrame(data[pos+valuelog.HeaderSize : end])
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		if len(rids) != int(header.K) || len(offsets) != int(header.K)+1 {
			t.Fatalf("decoded frame cardinality mismatch: k=%d rids=%d offsets=%d", header.K, len(rids), len(offsets))
		}
		if frameIndex == index {
			return header
		}
		pos = end
	}
	t.Fatalf("value log has %d frames, want frame index %d", frameIndex, index)
	return valuelog.FrameHeader{}
}

func readValueLogFrameHeaders(t *testing.T, path string) []valuelog.FrameHeader {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	headers := make([]valuelog.FrameHeader, 0, 8)
	for pos := 0; pos < len(data); {
		if len(data)-pos < valuelog.HeaderSize {
			t.Fatalf("truncated value-log frame header at offset %d in file length %d", pos, len(data))
		}
		bodyLen := int(binary.LittleEndian.Uint32(data[pos+16 : pos+20]))
		end := pos + valuelog.HeaderSize + bodyLen
		if bodyLen < valuelog.FrameHeaderSize || end > len(data) {
			t.Fatalf("invalid value-log body length %d at offset %d in file length %d", bodyLen, pos, len(data))
		}
		header, rids, offsets, _, err := valuelog.DecodeFrame(data[pos+valuelog.HeaderSize : end])
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		if len(rids) != int(header.K) || len(offsets) != int(header.K)+1 {
			t.Fatalf("decoded frame cardinality mismatch: k=%d rids=%d offsets=%d", header.K, len(rids), len(offsets))
		}
		headers = append(headers, header)
		pos = end
	}
	return headers
}

func compactRetainedTemplatePayload(suffix byte) []byte {
	base := []byte(`TD1D{"commit":{"operation":"create","collection":"app.bsky.feed.post"},"kind":"commit","did":"did:plc:storage-parity","text":"compact-retained-template-body"}`)
	value := bytes.Repeat(base, 2)
	if len(value) > 300 {
		value = value[:300]
	}
	value[len(value)-1] = suffix
	return value
}

func largeRetainedTemplatePayload(suffix byte) []byte {
	base := []byte(`TD1D{"commit":{"operation":"create","collection":"app.bsky.feed.post"},"kind":"commit","did":"did:plc:storage-parity","text":"large-retained-template-body-for-block-resolution"}`)
	value := bytes.Repeat(base, 5)
	value[len(value)-1] = suffix
	return value
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

func TestAppendValueLog_AutoBalancedCompactRetainedTemplateBatchUsesBlockFrames(t *testing.T) {
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
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.20, samples: 16}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.20, samples: 16}
	selector.metrics[vlogAutoCandidateBlockZSTD] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.20, samples: 16}

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogBlockTargetBytes: 4096,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}
	db.valueLogDictCurrentK.Store(32)

	records := make([]valuelog.Record, 64)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: compactRetainedTemplatePayload(byte(i))}
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

	headers := readValueLogFrameHeaders(t, path)
	recordsSeen := 0
	for i, header := range headers {
		recordsSeen += int(header.K)
		if header.Flags&valuelog.FrameFlagCompressed == 0 {
			t.Fatalf("frame %d is raw; compact retained template batches must not follow grouped_raw path", i)
		}
		if header.DictID != 0 {
			t.Fatalf("frame %d dict id=%d, want block-compressed no-dict frame", i, header.DictID)
		}
		if got := valuelog.BlockCodec(header.Reserved); got != valuelog.BlockCodecZSTD {
			t.Fatalf("frame %d block codec=%v, want zstd for retained storage-first bootstrap", i, got)
		}
	}
	if recordsSeen != len(records) {
		t.Fatalf("records in frames=%d want %d", recordsSeen, len(records))
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected block write-mode observation")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no raw/off write-mode observations for compact retained template batch")
	}
}

func TestFlushVlogRequests_AutoBalancedCompactRetainedTemplateQueueUsesBlockFrames(t *testing.T) {
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
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)},
		},
	}
	db.valueLogDictCurrentK.Store(32)

	requests := make([]vlogWriteRequest, 64)
	for i := range requests {
		ack := &vlogAck{}
		ack.wg.Add(1)
		requests[i] = vlogWriteRequest{
			rid:        uint64(i + 1),
			value:      compactRetainedTemplatePayload(byte(i)),
			writeMode:  vlogWriteOff,
			blockCodec: valuelog.BlockCodecSnappy,
			durability: journalDurabilityFlush,
			ack:        ack,
		}
	}

	db.flushVlogRequests(&db.lanes[0], requests)
	for i := range requests {
		ack := requests[i].ack
		ack.wg.Wait()
		if ack.err != nil {
			t.Fatalf("ack[%d] err: %v", i, ack.err)
		}
		if ack.ptr == (page.ValuePtr{}) {
			t.Fatalf("ack[%d] missing pointer", i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	headers := readValueLogFrameHeaders(t, path)
	recordsSeen := 0
	for i, header := range headers {
		recordsSeen += int(header.K)
		if header.Flags&valuelog.FrameFlagCompressed == 0 {
			t.Fatalf("frame %d is raw; queued compact retained template batches must not follow grouped_raw path", i)
		}
		if header.DictID != 0 {
			t.Fatalf("frame %d dict id=%d, want block-compressed no-dict frame", i, header.DictID)
		}
		if got := valuelog.BlockCodec(header.Reserved); got != valuelog.BlockCodecZSTD {
			t.Fatalf("frame %d block codec=%v, want zstd for queued retained storage-first bootstrap", i, got)
		}
	}
	if recordsSeen != len(requests) {
		t.Fatalf("records in frames=%d want %d", recordsSeen, len(requests))
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected queued block write-mode observation")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no queued raw/off write-mode observations for compact retained template batch")
	}
}

func TestFlushVlogRequests_AutoBalancedMixedRetainedResolvedModesCoalesceBeforeRaw(t *testing.T) {
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
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)},
		},
	}
	db.valueLogDictCurrentK.Store(32)

	requests := make([]vlogWriteRequest, 14)
	for i := 0; i < 13; i++ {
		ack := &vlogAck{}
		ack.wg.Add(1)
		requests[i] = vlogWriteRequest{
			rid:        uint64(i + 1),
			value:      compactRetainedTemplatePayload(byte(i)),
			writeMode:  vlogWriteOff,
			blockCodec: valuelog.BlockCodecSnappy,
			durability: journalDurabilityFlush,
			ack:        ack,
		}
	}
	ack := &vlogAck{}
	ack.wg.Add(1)
	requests[len(requests)-1] = vlogWriteRequest{
		rid:        uint64(len(requests)),
		value:      largeRetainedTemplatePayload(byte(len(requests))),
		writeMode:  vlogWriteBlock,
		blockCodec: valuelog.BlockCodecSnappy,
		durability: journalDurabilityFlush,
		ack:        ack,
	}

	db.flushVlogRequests(&db.lanes[0], requests)
	for i := range requests {
		ack := requests[i].ack
		ack.wg.Wait()
		if ack.err != nil {
			t.Fatalf("ack[%d] err: %v", i, ack.err)
		}
		if ack.ptr == (page.ValuePtr{}) {
			t.Fatalf("ack[%d] missing pointer", i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	headers := readValueLogFrameHeaders(t, path)
	recordsSeen := 0
	for i, header := range headers {
		recordsSeen += int(header.K)
		if header.Flags&valuelog.FrameFlagCompressed == 0 {
			t.Fatalf("frame %d is raw; retained mode coalescing must happen before old write-mode boundaries", i)
		}
		if header.DictID != 0 {
			t.Fatalf("frame %d dict id=%d, want block-compressed no-dict frame", i, header.DictID)
		}
	}
	if recordsSeen != len(requests) {
		t.Fatalf("records in frames=%d want %d", recordsSeen, len(requests))
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected mixed resolved-mode queue to record block write-mode observation")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no raw/off observations for mixed retained resolved-mode queue")
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
	if got := valuelog.BlockCodec(header.Reserved); got != valuelog.BlockCodecZSTD {
		t.Fatalf("block codec=%v want zstd", got)
	}
}

func TestAppendValueLogOne_AutoForcePointerRetainedPayloadResetsBlockBackoff(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	writer.SetBlockCompression(valuelog.BlockCodecSnappy, true)

	valueLen := forcePointerAutoBlockMinPayloadBytes + 4096
	incompressible := make([]byte, valueLen)
	x := uint32(0x9e3779b9)
	for i := range incompressible {
		x = x*1664525 + 1013904223
		incompressible[i] = byte(x >> 24)
	}
	_, seedStats, err := writer.AppendOneFrameWithStats(0, nil, 1, incompressible)
	if err != nil {
		t.Fatalf("seed incompressible append: %v", err)
	}
	if !seedStats.Attempted || seedStats.Kept {
		t.Fatalf("seed stats attempted=%v kept=%v, want attempted raw fallback", seedStats.Attempted, seedStats.Kept)
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

	chunk := []byte(`{"commit":{"operation":"create","collection":"app.bsky.feed.post"},"kind":"commit","did":"did:plc:storage-parity","text":"single-retained-json"}`)
	retained := bytes.Repeat(chunk, valueLen/len(chunk)+1)
	retained = retained[:valueLen]
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 0, nil, 2, retained, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	header := readValueLogFrameHeaderAt(t, path, 1)
	if header.K != 1 {
		t.Fatalf("frame K=%d want single frame", header.K)
	}
	if header.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected retained single write to reset block backoff and store a compressed frame")
	}
	if header.DictID != 0 {
		t.Fatalf("expected no dict id for retained single write, got %d", header.DictID)
	}
	if got := valuelog.BlockCodec(header.Reserved); got != valuelog.BlockCodecZSTD {
		t.Fatalf("block codec=%v want zstd", got)
	}
}

func TestAppendValueLog_PreparedProbeResetsWriterBackoff(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() {
		if writer != nil {
			_ = writer.Close()
		}
	})
	writer.SetBlockCompression(valuelog.BlockCodecSnappy, true)

	incompressible := make([]valuelog.Record, 8)
	for i := range incompressible {
		value := make([]byte, 4096)
		state := uint32(i + 1)
		for j := range value {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		incompressible[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	seedPtrs := make([]page.ValuePtr, len(incompressible))
	_, seedStats, err := writer.AppendFrameWithStatsInto(0, nil, incompressible, seedPtrs)
	if err != nil {
		t.Fatalf("seed writer backoff: %v", err)
	}
	if !seedStats.Attempted || seedStats.Kept {
		t.Fatalf("seed stats=%+v, want attempted raw fallback", seedStats)
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
	db.startVlogDictPreparer(&db.lanes[0])
	t.Cleanup(func() {
		close(db.closeCh)
		db.wg.Wait()
	})

	const valueBytes = 32 << 10
	jsonChunk := []byte(`{"kind":"commit","operation":"create","text":"prepared-probe"}`)
	probeRecords := make([]valuelog.Record, 32)
	for i := range probeRecords {
		value := bytes.Repeat(jsonChunk, valueBytes/len(jsonChunk)+1)
		probeRecords[i] = valuelog.Record{RID: uint64(100 + i), Value: value[:valueBytes]}
	}
	if _, err := db.appendValueLog(&db.lanes[0], 0, nil, probeRecords, journalDurabilityFlush); err != nil {
		t.Fatalf("append prepared probe: %v", err)
	}
	db.lanes[0].vlogPrepMu.Lock()
	prepWorkers := db.lanes[0].vlogPrepWorkers
	db.lanes[0].vlogPrepMu.Unlock()
	if prepWorkers < 2 {
		t.Fatalf("prepared probe workers=%d, want at least 2", prepWorkers)
	}

	db.valueLogCompressionMode = uint8(vlogCompressionBlock)
	finalRecords := []valuelog.Record{{
		RID:   1000,
		Value: bytes.Repeat([]byte("compressible-block"), 2048),
	}}
	if _, err := db.appendValueLog(&db.lanes[0], 0, nil, finalRecords, journalDurabilityFlush); err != nil {
		t.Fatalf("append writer-owned frame: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writer = nil

	headers := readValueLogFrameHeaders(t, path)
	last := headers[len(headers)-1]
	if last.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("writer-owned frame remained raw after prepared forced probe")
	}
}

func TestAppendValueLog_AutoBalancedForcePointerHighEntropyPayloadUsesGroupedRawFrame(t *testing.T) {
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
	if header.Flags&valuelog.FrameFlagCompressed != 0 {
		t.Fatalf("expected balanced forced-pointer high-entropy batch to use grouped raw frame")
	}
	if header.DictID != 0 {
		t.Fatalf("expected no dict id for high-entropy raw payload, got %d", header.DictID)
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteOff] == 0 {
		t.Fatalf("expected raw/off write-mode observation")
	}
	if writeSnap.Frames[vlogWriteBlock] != 0 {
		t.Fatalf("expected no block write-mode observation for high-entropy raw bypass")
	}
	selectorSnap := db.lanes[0].vlogCompressionSelector.snapshot()
	if selectorSnap.framesByCandidate[vlogAutoCandidateOff] != 0 {
		t.Fatalf("expected high-entropy forced-pointer bypass not to train selector off frames")
	}
}

func TestAppendValueLog_AutoBalancedValueLogDictSelectorOffUsesGroupedBlockFrame(t *testing.T) {
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
		valueLogThreshold:        page.DefaultInlineThreshold,
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
		t.Fatalf("expected storage-first value-log dict-available batch to store a block-compressed frame")
	}
	if header.DictID != 0 {
		t.Fatalf("expected selector-off remap to store block frame without dict id, got %d", header.DictID)
	}
	writeSnap := snapshotLaneVlogWriteMode(&db.lanes[0])
	if writeSnap.Frames[vlogWriteBlock] == 0 {
		t.Fatalf("expected block write-mode observation")
	}
	if writeSnap.Frames[vlogWriteOff] != 0 {
		t.Fatalf("expected no raw/off write-mode observation for storage-first value-log dict path")
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
		valueLogThreshold:       1 << 20,
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
