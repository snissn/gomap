package valuelog

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func diffReadInstrumentationStats(after, before ReadInstrumentationStats) ReadInstrumentationStats {
	return ReadInstrumentationStats{
		CompactLeafDecodeCallsTotal:            after.CompactLeafDecodeCallsTotal - before.CompactLeafDecodeCallsTotal,
		CompactLeafDecodeBytesTotal:            after.CompactLeafDecodeBytesTotal - before.CompactLeafDecodeBytesTotal,
		CompactLeafAppendDirectCallsTotal:      after.CompactLeafAppendDirectCallsTotal - before.CompactLeafAppendDirectCallsTotal,
		CompactLeafAppendDirectBytesTotal:      after.CompactLeafAppendDirectBytesTotal - before.CompactLeafAppendDirectBytesTotal,
		CompactLeafAppendScratchCallsTotal:     after.CompactLeafAppendScratchCallsTotal - before.CompactLeafAppendScratchCallsTotal,
		CompactLeafAppendScratchBytesTotal:     after.CompactLeafAppendScratchBytesTotal - before.CompactLeafAppendScratchBytesTotal,
		DecodeScratchGlobalSmallHitsTotal:      after.DecodeScratchGlobalSmallHitsTotal - before.DecodeScratchGlobalSmallHitsTotal,
		DecodeScratchGlobalSmallMissesTotal:    after.DecodeScratchGlobalSmallMissesTotal - before.DecodeScratchGlobalSmallMissesTotal,
		DecodeScratchGlobalLargeHitsTotal:      after.DecodeScratchGlobalLargeHitsTotal - before.DecodeScratchGlobalLargeHitsTotal,
		DecodeScratchGlobalLargeMissesTotal:    after.DecodeScratchGlobalLargeMissesTotal - before.DecodeScratchGlobalLargeMissesTotal,
		DecodeScratchGlobalOversizeMissesTotal: after.DecodeScratchGlobalOversizeMissesTotal - before.DecodeScratchGlobalOversizeMissesTotal,
		DecodeScratchFileHitsTotal:             after.DecodeScratchFileHitsTotal - before.DecodeScratchFileHitsTotal,
		DecodeScratchFileMissesTotal:           after.DecodeScratchFileMissesTotal - before.DecodeScratchFileMissesTotal,
		DecodeScratchSmallPutsTotal:            after.DecodeScratchSmallPutsTotal - before.DecodeScratchSmallPutsTotal,
		DecodeScratchLargePutsTotal:            after.DecodeScratchLargePutsTotal - before.DecodeScratchLargePutsTotal,
		DecodeScratchFilePutsTotal:             after.DecodeScratchFilePutsTotal - before.DecodeScratchFilePutsTotal,
		DecodeScratchDropsTotal:                after.DecodeScratchDropsTotal - before.DecodeScratchDropsTotal,
		ReadUnsafeAppendCallsTotal:             after.ReadUnsafeAppendCallsTotal - before.ReadUnsafeAppendCallsTotal,
		ReadAppendCallsTotal:                   after.ReadAppendCallsTotal - before.ReadAppendCallsTotal,
		ReadAppendBytesTotal:                   after.ReadAppendBytesTotal - before.ReadAppendBytesTotal,
		ReadAppendLatencyNsTotal:               after.ReadAppendLatencyNsTotal - before.ReadAppendLatencyNsTotal,
		ReadAppendMmapCallsTotal:               after.ReadAppendMmapCallsTotal - before.ReadAppendMmapCallsTotal,
		ReadAppendMmapBytesTotal:               after.ReadAppendMmapBytesTotal - before.ReadAppendMmapBytesTotal,
		ReadAppendMmapLatencyNsTotal:           after.ReadAppendMmapLatencyNsTotal - before.ReadAppendMmapLatencyNsTotal,
		ReadAppendFileCallsTotal:               after.ReadAppendFileCallsTotal - before.ReadAppendFileCallsTotal,
		ReadAppendFileBytesTotal:               after.ReadAppendFileBytesTotal - before.ReadAppendFileBytesTotal,
		ReadAppendFileLatencyNsTotal:           after.ReadAppendFileLatencyNsTotal - before.ReadAppendFileLatencyNsTotal,
	}
}

func TestReadInstrumentation_CompactLeafAppendDecodePaths(t *testing.T) {
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, t.TempDir(), 1)
	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected compact payload")
	}

	before := ReadInstrumentationStatsSnapshot()
	got, err := appendMaybeDecodeLeafLogPayload(fileID, path, make([]byte, 0, page.PageSize), payload)
	if err != nil {
		t.Fatalf("appendMaybeDecodeLeafLogPayload direct: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("direct decode len=%d want %d", len(got), page.PageSize)
	}
	after := ReadInstrumentationStatsSnapshot()
	delta := diffReadInstrumentationStats(after, before)
	if delta.CompactLeafDecodeCallsTotal != 1 || delta.CompactLeafDecodeBytesTotal != page.PageSize {
		t.Fatalf("direct compact decode delta=%+v", delta)
	}
	if delta.CompactLeafAppendDirectCallsTotal != 1 || delta.CompactLeafAppendDirectBytesTotal != page.PageSize {
		t.Fatalf("direct append decode delta=%+v", delta)
	}
	if delta.CompactLeafAppendScratchCallsTotal != 0 {
		t.Fatalf("unexpected scratch decode delta=%+v", delta)
	}

	before = ReadInstrumentationStatsSnapshot()
	got, err = appendMaybeDecodeLeafLogPayload(fileID, path, nil, payload)
	if err != nil {
		t.Fatalf("appendMaybeDecodeLeafLogPayload scratch: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("scratch decode len=%d want %d", len(got), page.PageSize)
	}
	after = ReadInstrumentationStatsSnapshot()
	delta = diffReadInstrumentationStats(after, before)
	if delta.CompactLeafDecodeCallsTotal != 1 || delta.CompactLeafDecodeBytesTotal != page.PageSize {
		t.Fatalf("scratch compact decode delta=%+v", delta)
	}
	if delta.CompactLeafAppendScratchCallsTotal != 1 || delta.CompactLeafAppendScratchBytesTotal != page.PageSize {
		t.Fatalf("scratch append decode delta=%+v", delta)
	}
}

func TestReadInstrumentation_FileDecodeScratchHitsAndMisses(t *testing.T) {
	f := &File{}
	before := ReadInstrumentationStatsSnapshot()
	buf := f.takeDecodeScratch(1024)
	if cap(buf) < 1024 {
		t.Fatalf("cap(buf)=%d want >=1024", cap(buf))
	}
	f.releaseDecodeScratch(buf)
	buf = f.takeDecodeScratch(1024)
	if cap(buf) < 1024 {
		t.Fatalf("cap(buf)=%d want >=1024 on second take", cap(buf))
	}
	f.releaseDecodeScratch(buf)
	after := ReadInstrumentationStatsSnapshot()
	delta := diffReadInstrumentationStats(after, before)
	if delta.DecodeScratchFileMissesTotal != 1 || delta.DecodeScratchFileHitsTotal != 1 {
		t.Fatalf("file scratch delta=%+v want one miss and one hit", delta)
	}
	if delta.DecodeScratchFilePutsTotal != 2 {
		t.Fatalf("file scratch puts=%d want 2", delta.DecodeScratchFilePutsTotal)
	}
}

func TestReadInstrumentation_ReadUnsafeAppendPathCounters(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := compactLeafPayloadTestPath(t, dir, 1)
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	leaf := buildSparseLeafPageForPayloadTest(t)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		t.Fatalf("expected compact payload")
	}
	ptr, err := w.Append(0, nil, 1, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	mgr := newLeafPayloadTestManager(t, dir, path, fileID)
	defer func() { _ = mgr.Close() }()
	f := mgr.files[fileID]
	if f == nil {
		t.Fatalf("manager missing file %d", fileID)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	f.mmapData.Store(data)

	before := ReadInstrumentationStatsSnapshot()
	got, err := mgr.ReadUnsafeAppend(ptr, make([]byte, 0, page.PageSize))
	if err != nil {
		t.Fatalf("ReadUnsafeAppend: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafeAppend len=%d want %d", len(got), page.PageSize)
	}
	after := ReadInstrumentationStatsSnapshot()
	delta := diffReadInstrumentationStats(after, before)
	if delta.ReadUnsafeAppendCallsTotal != 1 {
		t.Fatalf("ReadUnsafeAppend calls=%d want 1", delta.ReadUnsafeAppendCallsTotal)
	}
	if delta.ReadAppendCallsTotal != 1 || delta.ReadAppendMmapCallsTotal != 1 || delta.ReadAppendFileCallsTotal != 0 {
		t.Fatalf("unexpected ReadAppend path delta=%+v", delta)
	}
	if delta.ReadAppendBytesTotal != page.PageSize || delta.ReadAppendMmapBytesTotal != page.PageSize {
		t.Fatalf("unexpected ReadAppend bytes delta=%+v", delta)
	}
	if delta.ReadAppendLatencyNsTotal == 0 || delta.ReadAppendMmapLatencyNsTotal == 0 {
		t.Fatalf("expected non-zero latency delta=%+v", delta)
	}
}
