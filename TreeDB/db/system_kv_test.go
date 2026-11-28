package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSystemDict_InlineChunkedAndPointer_GCAndRewritePreserveRaw(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:       dir,
		ChunkSize: 64 << 10,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	putInline := func(payload []byte) uint64 {
		sum := sha256.Sum256(payload)
		id := binary.BigEndian.Uint64(sum[:8])
		enc := make([]byte, 1+len(payload))
		enc[0] = SystemDictEncodingInline
		copy(enc[1:], payload)
		if err := d.ApplySystemOps([]SystemKVOp{{Key: SystemDictBytesKey(id), Value: enc}}, true); err != nil {
			t.Fatalf("ApplySystemOps(inline): %v", err)
		}
		return id
	}
	putChunked := func(payload []byte) uint64 {
		sum := sha256.Sum256(payload)
		id := binary.BigEndian.Uint64(sum[:8])
		chunks := uint32((len(payload) + SystemDictChunkSize - 1) / SystemDictChunkSize)
		ops := make([]SystemKVOp, 0, 1+int(chunks))
		ops = append(ops, SystemKVOp{
			Key:   SystemDictBytesKey(id),
			Value: EncodeSystemDictChunkHeader(chunks, uint32(len(payload)), sum),
		})
		for i := uint32(0); i < chunks; i++ {
			start := int(i) * SystemDictChunkSize
			end := start + SystemDictChunkSize
			if end > len(payload) {
				end = len(payload)
			}
			ops = append(ops, SystemKVOp{
				Key:   SystemDictChunkKey(id, i),
				Value: append([]byte(nil), payload[start:end]...),
			})
		}
		if err := d.ApplySystemOps(ops, true); err != nil {
			t.Fatalf("ApplySystemOps(chunked): %v", err)
		}
		return id
	}
	putPointer := func(payload []byte) (uint64, uint32) {
		sum := sha256.Sum256(payload)
		id := binary.BigEndian.Uint64(sum[:8])
		ptr, err := d.AppendRawSystemValueLog(id, payload)
		if err != nil {
			t.Fatalf("AppendRawSystemValueLog: %v", err)
		}
		if err := d.ApplySystemOps([]SystemKVOp{{Key: SystemDictBytesKey(id), ValuePtr: ptr, IsPointer: true}}, true); err != nil {
			t.Fatalf("ApplySystemOps(pointer): %v", err)
		}
		return id, ptr.FileID
	}

	inlineWant := []byte("small-inline-system-dict-v1")
	chunkedWant := bytes.Repeat([]byte("chunked-system-dict-block|"), 240) // > 6 KiB (4 chunks)
	pointerWant := bytes.Repeat([]byte("pointer-system-dict-block|"), 320) // > 8 KiB in maindb/value_vlog

	inlineID := putInline(inlineWant)
	chunkedID := putChunked(chunkedWant)
	pointerID, pointerFileID := putPointer(pointerWant)

	// Verify ValueLogGC retains the segment referenced by SystemRootPageID.
	gcStats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if gcStats.SegmentsDeleted != 0 {
		t.Fatalf("ValueLogGC deleted %d segments while pointer dict %d is live", gcStats.SegmentsDeleted, pointerID)
	}

	// Verify ValueLogRewriteOnline rewrites SystemRootPageID pointer records
	// unencoded (dictID=0, templateID=0) and keeps the dict readable.
	entryBefore, ok, err := d.GetSystemEntry(SystemDictBytesKey(pointerID))
	if err != nil || !ok || entryBefore.Flags&node.FlagPointer == 0 || entryBefore.ValuePtr.FileID != pointerFileID {
		t.Fatalf("pointer entry before rewrite: ok=%v flags=%#x fileID=%d want=%d err=%v", ok, entryBefore.Flags, entryBefore.ValuePtr.FileID, pointerFileID, err)
	}
	if _, err := d.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{pointerFileID},
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	entryAfter, ok, err := d.GetSystemEntry(SystemDictBytesKey(pointerID))
	if err != nil || !ok || entryAfter.Flags&node.FlagPointer == 0 {
		t.Fatalf("pointer entry after online rewrite: ok=%v flags=%#x err=%v", ok, entryAfter.Flags, err)
	}
	if entryAfter.ValuePtr.FileID == pointerFileID {
		t.Fatalf("online rewrite did not move system pointer out of segment %d", pointerFileID)
	}
	// Verify rewritten record header has DictID==0 (no self-compression cycle).
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	file, _, err := snap.PinnedValueLogSegment(entryAfter.ValuePtr.FileID)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("PinnedValueLogSegment: %v", err)
	}
	shape, err := file.InspectCOWRecord(entryAfter.ValuePtr, valuelog.COWReadLimits{
		MaxRecordBytes: 1 << 20,
		MaxRawBytes:    1 << 20,
		MaxValueBytes:  1 << 20,
	})
	_ = snap.Close()
	if err != nil {
		t.Fatalf("InspectCOWRecord: %v", err)
	}
	if shape.Compressed || shape.DictID != 0 {
		t.Fatalf("rewritten system pointer dict became compressed=%v dictID=%d, want raw dictID=0", shape.Compressed, shape.DictID)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil

	// Reopen with zero external DictLookup callbacks: built-in SystemRootPageID
	// lookup must resolve inline, chunked, and pointer dicts seamlessly.
	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()

	for name, tc := range map[string]struct {
		id   uint64
		want []byte
	}{
		"inline":  {inlineID, inlineWant},
		"chunked": {chunkedID, chunkedWant},
		"pointer": {pointerID, pointerWant},
	} {
		got, err := reopen.LookupSystemDictBytes(tc.id)
		if err != nil || !bytes.Equal(got, tc.want) {
			t.Fatalf("reopen LookupSystemDictBytes(%s): len=%d want=%d err=%v", name, len(got), len(tc.want), err)
		}
	}
}

func TestSystemDict_OuterLeavesInValueLog_RebuildAndFixedScratchAcyclic(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 << 10,
		IndexOuterLeavesInValueLog: true,
		FreshSystemPagerLeaves:     false,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{})
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	d.SetLeafPageLog(leafLog)
	defer func() {
		_ = d.Close()
		_ = leafLog.Close()
	}()

	// Seed SystemRootPageID through PublishSystemRootIterator (which writes
	// outer leaves into leaf_vlog when FreshSystemPagerLeaves=false), then
	// publish an inline dictionary via ApplySystemOps, then rebuild via
	// PublishSystemRootIterator again, and verify GetSystemEntryExactWithFixedScratch
	// resolves the dictionary cleanly without leaf-ref conflicts or external-leaf errors.
	seedIter := newSystemKVOpIterator([]SystemKVOp{{Key: []byte("meta/a"), Value: []byte("1")}})
	if _, err := d.PublishSystemRootIterator(seedIter); err != nil {
		t.Fatalf("PublishSystemRootIterator(seed): %v", err)
	}

	payload := []byte("outer-leaf-system-dict-payload")
	sum := sha256.Sum256(payload)
	dictID := binary.BigEndian.Uint64(sum[:8])
	enc := make([]byte, 1+len(payload))
	enc[0] = SystemDictEncodingInline
	copy(enc[1:], payload)
	if err := d.ApplySystemOps([]SystemKVOp{{Key: SystemDictBytesKey(dictID), Value: enc}}, true); err != nil {
		t.Fatalf("ApplySystemOps: %v", err)
	}

	// Rebuild SystemRootPageID including both the dictionary entry and another
	// metadata key through PublishSystemRootIterator.
	rebuildOps, _ := normalizeSystemKVOps([]SystemKVOp{
		{Key: []byte("meta/a"), Value: []byte("1")},
		{Key: []byte("meta/b"), Value: []byte("2")},
		{Key: SystemDictBytesKey(dictID), Value: enc},
	})
	if _, err := d.PublishSystemRootIterator(newSystemKVOpIterator(rebuildOps)); err != nil {
		t.Fatalf("PublishSystemRootIterator(rebuild): %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	var keyScratch, leafScratch [page.PageSize]byte
	entry, err := snap.GetSystemEntryExactWithFixedScratch(SystemDictBytesKey(dictID), keyScratch[:], leafScratch[:])
	if err != nil {
		t.Fatalf("GetSystemEntryExactWithFixedScratch: %v", err)
	}
	if len(entry.Value) != len(enc) || !bytes.Equal(entry.Value, enc) {
		t.Fatalf("GetSystemEntryExactWithFixedScratch value mismatch: len=%d want=%d", len(entry.Value), len(enc))
	}
}

func TestSystemDict_PointerSegmentGC_RotatesSystemWriter(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:       dir,
		ChunkSize: 64 << 10,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	putPointer := func(payload []byte) (uint64, uint32) {
		sum := sha256.Sum256(payload)
		id := binary.BigEndian.Uint64(sum[:8])
		ptr, err := d.AppendRawSystemValueLog(id, payload)
		if err != nil {
			t.Fatalf("AppendRawSystemValueLog: %v", err)
		}
		if err := d.ApplySystemOps([]SystemKVOp{{Key: SystemDictBytesKey(id), ValuePtr: ptr, IsPointer: true}}, true); err != nil {
			t.Fatalf("ApplySystemOps(pointer): %v", err)
		}
		return id, ptr.FileID
	}

	firstPayload := bytes.Repeat([]byte("first-pointer-system-dict|"), 320)
	firstID, firstFileID := putPointer(firstPayload)

	// Rewrite the pointer-backed system dict into a fresh segment.
	if _, err := d.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{firstFileID},
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	// Advance commits past both meta slots so older recoverable roots no longer
	// pin firstFileID, then GC with protected-path filtering to reclaim firstFileID.
	for i := 0; i < 4; i++ {
		if err := d.SetSync([]byte("advance"), []byte{byte(i)}); err != nil {
			t.Fatalf("SetSync(%d): %v", i, err)
		}
	}
	entryAfterRewrite, ok, err := d.GetSystemEntry(SystemDictBytesKey(firstID))
	if err != nil || !ok || entryAfterRewrite.ValuePtr.FileID == firstFileID {
		t.Fatalf("expected rewritten pointer dict out of %d, got ok=%v fileID=%d err=%v", firstFileID, ok, entryAfterRewrite.ValuePtr.FileID, err)
	}
	set := d.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		t.Fatal("missing value log set")
	}
	var rewrittenPath string
	if f := set.Files[entryAfterRewrite.ValuePtr.FileID]; f != nil {
		rewrittenPath = f.Path
	}
	_ = d.valueLogManager.Release(set)
	if rewrittenPath == "" {
		t.Fatalf("missing path for rewritten fileID=%d", entryAfterRewrite.ValuePtr.FileID)
	}
	gcStats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{
		ProtectedPaths: []string{rewrittenPath},
	})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if gcStats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to reclaim original system segment %d, got stats=%+v", firstFileID, gcStats)
	}

	// Publishing a second pointer-backed dictionary without reopening must rotate
	// systemVLogWriter cleanly onto a new registered segment.
	secondPayload := bytes.Repeat([]byte("second-pointer-system-dict|"), 320)
	secondID, secondFileID := putPointer(secondPayload)
	if secondFileID == firstFileID {
		t.Fatalf("expected rotated system value-log segment after GC, still got fileID=%d", secondFileID)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	for name, tc := range map[string]struct {
		id   uint64
		want []byte
	}{
		"first":  {firstID, firstPayload},
		"second": {secondID, secondPayload},
	} {
		got, err := reopen.LookupSystemDictBytes(tc.id)
		if err != nil || !bytes.Equal(got, tc.want) {
			t.Fatalf("reopen LookupSystemDictBytes(%s): len=%d want=%d err=%v", name, len(got), len(tc.want), err)
		}
	}
}

func TestSystemDict_CompactRewriteWriter_SystemLeafNeverUsesSelfDict(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 << 10,
		IndexOuterLeavesInValueLog: true,
		FreshSystemPagerLeaves:     false,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	// Install a compact-style rewriteWriter with a live leafDictID/leafDict
	// configured (mirroring compact_storage.go's leaf-page-log setup).
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 1)
	writer.ridAlloc = newRewriteRIDAllocator(1, nil)
	writer.blockCompression = true
	writer.blockCodec = valuelog.BlockCodecSnappy
	writer.leafBlockCodec = valuelog.BlockCodecSnappy
	d.SetLeafPageLog(writer)
	defer func() {
		d.SetLeafPageLog(nil)
		_ = writer.Close()
	}()

	payload := bytes.Repeat([]byte("self-dict-system-leaf-acyclicity-test|"), 16)
	sum := sha256.Sum256(payload)
	dictID := binary.BigEndian.Uint64(sum[:8])
	enc := make([]byte, 1+len(payload))
	enc[0] = SystemDictEncodingInline
	copy(enc[1:], payload)
	if err := d.ApplySystemOps([]SystemKVOp{{Key: SystemDictBytesKey(dictID), Value: enc}}, true); err != nil {
		t.Fatalf("ApplySystemOps(seed dict): %v", err)
	}

	// Activate leafDictID on the installed compact rewriteWriter, then publish a
	// subsequent SystemRootPageID mutation through ApplySystemOps (and flush).
	writer.SetLeafDictMode(dictID, payload, false)
	if err := d.ApplySystemOps([]SystemKVOp{{Key: []byte("meta/after-dict"), Value: []byte("ok")}}, true); err != nil {
		t.Fatalf("ApplySystemOps(after SetLeafDictMode): %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("writer.Flush: %v", err)
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	// Fixed-scratch lookup enforces DictID==0 on SystemRootPageID outer leaves;
	// if compact's rewriteWriter had dictionary-encoded the system leaf, this
	// would fail with "system outer leaf unexpectedly uses dict".
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	var keyScratch, leafScratch [page.PageSize]byte
	entry, err := snap.GetSystemEntryExactWithFixedScratch(SystemDictBytesKey(dictID), keyScratch[:], leafScratch[:])
	if err != nil {
		t.Fatalf("GetSystemEntryExactWithFixedScratch: %v", err)
	}
	if !bytes.Equal(entry.Value, enc) {
		t.Fatalf("system dict mismatch after compact rewriteWriter system update")
	}
}
