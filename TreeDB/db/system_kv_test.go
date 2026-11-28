package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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
	rebuildOps, _, _ := normalizeSystemKVOps([]SystemKVOp{
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

	// Also cover leaf_generation_pack_rewrite's final collection-descriptor
	// publication path where publishCtx.writer == nil and db.leafPageLog is the
	// compact rewriteWriter with an active leafDictID.
	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("nil index")
	}
	publishCtx := &leafRefRewriteCtx{
		ctx:        context.Background(),
		db:         d,
		pager:      idx.pager,
		leafReader: d.valueLogManager,
		alloc:      idx.allocator,
	}
	publishCtx.initSystemZipper(idx.zipper, d.valueLogManager, d.leafPageLog)
	newSysRoot, changed, err := publishCtx.applySystemRootCollectionRootReplacements(
		snap.state.SystemRootPageID,
		[]vacuumCollectionRootReplacement{{key: []byte("meta/pack-publish-collection"), value: []byte("root-v2")}},
	)
	if err != nil {
		t.Fatalf("applySystemRootCollectionRootReplacements: %v", err)
	}
	if !changed || newSysRoot == 0 {
		t.Fatalf("expected updated system root from collection descriptor publication, changed=%v root=%d", changed, newSysRoot)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("writer.Flush(after initSystemZipper): %v", err)
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet(after initSystemZipper): %v", err)
	}
	if writer.leafDictID != dictID {
		t.Fatalf("expected rewriteWriter leafDictID=%d to be restored, got %d", dictID, writer.leafDictID)
	}
	var leafCheckScratch [page.PageSize]byte
	// Verify every leaf reachable from newSysRoot still decodes with DictID==0
	// via fixed-scratch exact lookup on the updated system root.
	vset := d.valueLogManager.CurrentSetNoRefresh()
	if vset == nil {
		t.Fatal("nil value log set after initSystemZipper")
	}
	defer func() { _ = d.valueLogManager.Release(vset) }()
	readLeaf := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		vptr := ptr.ValuePtr()
		f := vset.Files[vptr.FileID]
		if f == nil {
			return nil, valuelog.ErrFileNotFound
		}
		gotDictID, err := f.PreparedRecordDictionaryID(vptr)
		if err != nil {
			return nil, err
		}
		if gotDictID != 0 {
			return nil, fmt.Errorf("system outer leaf unexpectedly uses dict %d", gotDictID)
		}
		raw, usedDst, err := d.valueLogManager.ReadUnsafeTo(vptr, dst[:0])
		if err != nil {
			return nil, err
		}
		if usedDst {
			return raw, nil
		}
		out := dst[:len(raw)]
		copy(out, raw)
		return out, nil
	}
	entry2, err := tree.New(idx.pager, d.valueLogManager, newSysRoot).GetEntryWithFixedScratch(
		SystemDictBytesKey(dictID),
		keyScratch[:],
		leafCheckScratch[:],
		readLeaf,
	)
	if err != nil {
		t.Fatalf("GetEntryWithFixedScratch(after initSystemZipper): %v", err)
	}
	if !bytes.Equal(entry2.Value, enc) {
		t.Fatalf("system dict mismatch after initSystemZipper publication")
	}

	// Also cover leafRefRewritePageAppender with a non-system dictionary active
	// on publishCtx.writer: initSystemZipper must route through noDict so
	// SystemRootPageID outer leaves still use DictID==0 without mutating
	// publishCtx.writer.leafDictID.
	const nonSystemDictID = uint64(7311)
	nonSystemSamples := make([][]byte, 8)
	basePage := bytes.Repeat([]byte("nonsystem-rewrite-appender-dict|"), page.PageSize/len("nonsystem-rewrite-appender-dict|"))
	for i := range nonSystemSamples {
		s := append([]byte(nil), basePage...)
		binary.LittleEndian.PutUint64(s[len(s)-8:], uint64(i+1))
		nonSystemSamples[i] = s
	}
	nonSystemDict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(nonSystemDictID),
		Contents: nonSystemSamples,
		History:  append([]byte(nil), nonSystemSamples[0]...),
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("zstd.BuildDict(nonSystem): %v", err)
	}
	rewriteW := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
	rewriteW.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 10)
	rewriteW.ridAlloc = newRewriteRIDAllocator(1000, nil)
	rewriteW.blockCompression = true
	rewriteW.blockCodec = valuelog.BlockCodecSnappy
	rewriteW.leafBlockCodec = valuelog.BlockCodecSnappy
	rewriteW.SetLeafDictMode(nonSystemDictID, nonSystemDict, false)
	defer func() { _ = rewriteW.Close() }()

	rewriteCtx := &leafRefRewriteCtx{
		ctx:        context.Background(),
		db:         d,
		pager:      idx.pager,
		leafReader: d.valueLogManager,
		alloc:      idx.allocator,
		writer:     rewriteW,
		ridAlloc:   rewriteW.ridAlloc,
	}
	rewriteCtx.initSystemZipper(idx.zipper, d.valueLogManager, &leafRefRewritePageAppender{ctx: rewriteCtx})
	newSysRoot2, changed2, err := rewriteCtx.applySystemRootCollectionRootReplacements(
		newSysRoot,
		[]vacuumCollectionRootReplacement{{key: []byte("meta/pack-publish-nonsystem-dict"), value: []byte("root-v3")}},
	)
	if err != nil {
		t.Fatalf("applySystemRootCollectionRootReplacements(leafRefRewritePageAppender): %v", err)
	}
	if !changed2 || newSysRoot2 == 0 {
		t.Fatalf("expected updated system root via leafRefRewritePageAppender, changed=%v root=%d", changed2, newSysRoot2)
	}
	if rewriteW.leafDictID != nonSystemDictID {
		t.Fatalf("rewriteW.leafDictID=%d after system root publish; want %d", rewriteW.leafDictID, nonSystemDictID)
	}
	if err := rewriteW.Flush(); err != nil {
		t.Fatalf("rewriteW.Flush: %v", err)
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet(rewriteW): %v", err)
	}
	vset2 := d.valueLogManager.CurrentSetNoRefresh()
	if vset2 == nil {
		t.Fatal("nil value log set after rewriteW flush")
	}
	defer func() { _ = d.valueLogManager.Release(vset2) }()
	readLeaf2 := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		vptr := ptr.ValuePtr()
		f := vset2.Files[vptr.FileID]
		if f == nil {
			return nil, valuelog.ErrFileNotFound
		}
		gotDictID, err := f.PreparedRecordDictionaryID(vptr)
		if err != nil {
			return nil, err
		}
		if gotDictID != 0 {
			return nil, fmt.Errorf("leafRefRewritePageAppender system outer leaf unexpectedly uses dict %d", gotDictID)
		}
		raw, usedDst, err := d.valueLogManager.ReadUnsafeTo(vptr, dst[:0])
		if err != nil {
			return nil, err
		}
		if usedDst {
			return raw, nil
		}
		out := dst[:len(raw)]
		copy(out, raw)
		return out, nil
	}
	entry3, err := tree.New(idx.pager, d.valueLogManager, newSysRoot2).GetEntryWithFixedScratch(
		[]byte("meta/pack-publish-nonsystem-dict"),
		keyScratch[:],
		leafCheckScratch[:],
		readLeaf2,
	)
	if err != nil {
		t.Fatalf("GetEntryWithFixedScratch(leafRefRewritePageAppender): %v", err)
	}
	if !bytes.Equal(entry3.Value, []byte("root-v3")) {
		t.Fatalf("system root entry mismatch after leafRefRewritePageAppender: got=%q", entry3.Value)
	}
}

func TestSystemDict_PendingPointerProtectedAcrossValueLogGC(t *testing.T) {
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

	// Seed a first pointer-backed dictionary and rewrite it off the original
	// system value-log segment so that segment is no longer referenced by any
	// recoverable root.
	firstPayload := bytes.Repeat([]byte("first-pending-pin-system-dict|"), 320)
	firstSum := sha256.Sum256(firstPayload)
	firstID := binary.BigEndian.Uint64(firstSum[:8])
	firstPtr, err := d.AppendRawSystemValueLog(0, firstPayload)
	if err != nil {
		t.Fatalf("AppendRawSystemValueLog(first): %v", err)
	}
	if err := d.ApplySystemOps([]SystemKVOp{{
		Key:       SystemDictBytesKey(firstID),
		ValuePtr:  firstPtr,
		IsPointer: true,
	}}, true); err != nil {
		t.Fatalf("ApplySystemOps(first): %v", err)
	}
	if _, err := d.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{firstPtr.FileID},
		BatchSize:     8,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline(first): %v", err)
	}
	// Advance recoverable roots so no alternate root still pins firstPtr.FileID.
	for i := 0; i < 3; i++ {
		if err := d.SetSync([]byte(fmt.Sprintf("advance-%d", i)), []byte("v")); err != nil {
			t.Fatalf("SetSync(advance-%d): %v", i, err)
		}
	}
	firstAfterEntry, found, err := d.GetSystemEntry(SystemDictBytesKey(firstID))
	if err != nil || !found {
		t.Fatalf("GetSystemEntry(first after rewrite): found=%v err=%v", found, err)
	}
	if firstAfterEntry.ValuePtr.FileID == firstPtr.FileID {
		t.Fatalf("expected rewrite to relocate first dict off %d", firstPtr.FileID)
	}
	firstRewrittenPath := d.valueLogManager.SegmentPath(firstAfterEntry.ValuePtr.FileID)

	// Append a second raw system payload while the original system writer is
	// still registered, then run protected-path ValueLogGC BEFORE publishing the
	// pointer. ProtectedPaths disables lane-tip active exemptions so only the
	// pending append pin keeps secondPtr.FileID alive.
	secondPayload := bytes.Repeat([]byte("second-pending-pin-system-dict|"), 320)
	secondSum := sha256.Sum256(secondPayload)
	secondID := binary.BigEndian.Uint64(secondSum[:8])
	secondPtr, err := d.AppendRawSystemValueLog(0, secondPayload)
	if err != nil {
		t.Fatalf("AppendRawSystemValueLog(second): %v", err)
	}
	if secondPtr.FileID != firstPtr.FileID {
		t.Fatalf("expected second append to reuse still-registered system segment %d, got %d", firstPtr.FileID, secondPtr.FileID)
	}
	gcDuringPending, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{
		ProtectedPaths: []string{firstRewrittenPath},
	})
	if err != nil {
		t.Fatalf("ValueLogGC(while pending): %v", err)
	}
	if gcDuringPending.SegmentsDeleted != 0 || gcDuringPending.SegmentsActive == 0 {
		t.Fatalf("expected pending system pointer to keep segment %d active across GC, stats=%+v", secondPtr.FileID, gcDuringPending)
	}
	// Publishing secondPtr must succeed because the pending append pin kept its
	// segment alive across GC.
	if err := d.ApplySystemOps([]SystemKVOp{{
		Key:       SystemDictBytesKey(secondID),
		ValuePtr:  secondPtr,
		IsPointer: true,
	}}, true); err != nil {
		t.Fatalf("ApplySystemOps(second after GC): %v", err)
	}

	// Verify the pin was released after publication: once secondPtr is rewritten
	// onto another segment and recoverable roots advance, protected-path GC must
	// reclaim secondPtr.FileID.
	if _, err := d.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{secondPtr.FileID},
		BatchSize:     8,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline(second): %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := d.SetSync([]byte(fmt.Sprintf("advance-post-%d", i)), []byte("v")); err != nil {
			t.Fatalf("SetSync(advance-post-%d): %v", i, err)
		}
	}
	secondAfterEntry, found, err := d.GetSystemEntry(SystemDictBytesKey(secondID))
	if err != nil || !found {
		t.Fatalf("GetSystemEntry(second after rewrite): found=%v err=%v", found, err)
	}
	secondRewrittenPath := d.valueLogManager.SegmentPath(secondAfterEntry.ValuePtr.FileID)
	gcStats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{
		ProtectedPaths: []string{firstRewrittenPath, secondRewrittenPath},
	})
	if err != nil {
		t.Fatalf("ValueLogGC(reclaim after publish): %v", err)
	}
	if gcStats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to reclaim unreferenced segment %d once pending pin was released, stats=%+v", secondPtr.FileID, gcStats)
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
	got, err := reopen.LookupSystemDictBytes(secondID)
	if err != nil || !bytes.Equal(got, secondPayload) {
		t.Fatalf("reopen LookupSystemDictBytes(second): len=%d want=%d err=%v", len(got), len(secondPayload), err)
	}
}

func TestStageSystemOps_PiggybackDurableWaitsForSyncAndRelaxedActivatesImmediately(t *testing.T) {
	t.Run("durable_waits_for_sync", func(t *testing.T) {
		dir := t.TempDir()
		d, err := Open(Options{
			Dir:        dir,
			ChunkSize:  64 << 10,
			Durability: DurabilityDurable,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()

		payload := bytes.Repeat([]byte("staged-piggyback-durable-dict|"), 12)
		sum := sha256.Sum256(payload)
		dictID := binary.BigEndian.Uint64(sum[:8])
		enc := append([]byte{SystemDictEncodingInline}, payload...)

		var activated bool
		if err := d.StageSystemOps([]SystemKVOp{{
			Key:   SystemDictBytesKey(dictID),
			Value: enc,
		}}, func() { activated = true }); err != nil {
			t.Fatalf("StageSystemOps: %v", err)
		}
		if !d.HasStagedSystemOps() {
			t.Fatal("expected HasStagedSystemOps=true after staging")
		}

		// Unsynced user batch piggybacks the system ops into SystemRootPageID
		// in a single commit, but must NOT invoke onDurable yet while WAL/durable
		// mode has not synced index.db.
		b := d.NewBatch()
		if err := b.Set([]byte("user/k1"), []byte("v1")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("Write: %v", err)
		}
		_ = b.Close()

		if activated {
			t.Fatal("durable mode activated staged dict before sync barrier")
		}
		if !d.HasStagedSystemOps() {
			t.Fatal("expected HasStagedSystemOps=true while sync waiter is pending")
		}
		got, err := d.LookupSystemDictBytes(dictID)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("LookupSystemDictBytes after piggyback write: len=%d err=%v", len(got), err)
		}

		// Syncing via Checkpoint fires the pending sync waiter with zero extra
		// SystemRootPageID commits.
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if !activated {
			t.Fatal("expected onDurable callback to fire after Checkpoint sync")
		}
		if d.HasStagedSystemOps() {
			t.Fatal("expected HasStagedSystemOps=false after Checkpoint sync")
		}
	})

	t.Run("wal_off_relaxed_activates_on_unsynced_flush", func(t *testing.T) {
		dir := t.TempDir()
		d, err := Open(Options{
			Dir:        dir,
			ChunkSize:  64 << 10,
			Durability: DurabilityWALOffRelaxed,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()

		payload := bytes.Repeat([]byte("staged-piggyback-relaxed-dict|"), 12)
		sum := sha256.Sum256(payload)
		dictID := binary.BigEndian.Uint64(sum[:8])
		enc := append([]byte{SystemDictEncodingInline}, payload...)

		var activated bool
		if err := d.StageSystemOps([]SystemKVOp{{
			Key:   SystemDictBytesKey(dictID),
			Value: enc,
		}}, func() { activated = true }); err != nil {
			t.Fatalf("StageSystemOps: %v", err)
		}

		b := d.NewBatch()
		if err := b.Set([]byte("user/k1"), []byte("v1")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("Write: %v", err)
		}
		_ = b.Close()

		if !activated {
			t.Fatal("expected WALOffRelaxed mode to activate staged dict on unsynced piggybacked write")
		}
		if d.HasStagedSystemOps() {
			t.Fatal("expected HasStagedSystemOps=false after WALOffRelaxed piggyback")
		}
		got, err := d.LookupSystemDictBytes(dictID)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("LookupSystemDictBytes after WALOffRelaxed piggyback: len=%d err=%v", len(got), err)
		}
	})

	t.Run("root_publication_build_group_piggyback_and_pointer_pin", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{
			Dir:        dir,
			ChunkSize:  64 << 10,
			Durability: DurabilityDurable,
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

		payload := bytes.Repeat([]byte("staged-group-pointer-system-dict|"), 320)
		sum := sha256.Sum256(payload)
		dictID := binary.BigEndian.Uint64(sum[:8])
		ptr, err := d.AppendRawSystemValueLog(0, payload)
		if err != nil {
			t.Fatalf("AppendRawSystemValueLog: %v", err)
		}

		var activated bool
		if err := d.StageSystemOps([]SystemKVOp{{
			Key:       SystemDictBytesKey(dictID),
			ValuePtr:  ptr,
			IsPointer: true,
		}}, func() { activated = true }); err != nil {
			t.Fatalf("StageSystemOps: %v", err)
		}

		group, err := d.BeginRootPublicationBuildGroup()
		if err != nil {
			t.Fatalf("BeginRootPublicationBuildGroup: %v", err)
		}
		b := d.NewPhysicalBatch().(*Batch)
		b.rootPublicationBuildGroup = group
		b.rootPublicationBuildGroupFinal = true
		if err := b.Set([]byte("group/k1"), []byte("v1")); err != nil {
			_ = b.Close()
			_ = group.Close()
			t.Fatalf("Set: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			_ = group.Close()
			t.Fatalf("WriteSync(group): %v", err)
		}
		_ = b.Close()
		if err := group.Close(); err != nil {
			t.Fatalf("group.Close: %v", err)
		}

		if !activated {
			t.Fatal("expected WriteSync RootPublicationBuildGroup to activate staged dict immediately")
		}
		if d.HasStagedSystemOps() {
			t.Fatal("expected no remaining staged ops after WriteSync group")
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
		got, err := reopen.LookupSystemDictBytes(dictID)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("reopen LookupSystemDictBytes: len=%d want=%d err=%v", len(got), len(payload), err)
		}
	})

	t.Run("sync_waiters_dispatch_in_strict_commit_order", func(t *testing.T) {
		dir := t.TempDir()
		d, err := Open(Options{
			Dir:        dir,
			ChunkSize:  64 << 10,
			Durability: DurabilityDurable,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()

		var order []string
		// Batch A piggybacked onto an unsynced Write() enters sync waiters.
		if err := d.StageSystemOps([]SystemKVOp{{
			Key:   []byte("dict/test-order"),
			Value: []byte("A"),
		}}, func() { order = append(order, "A") }); err != nil {
			t.Fatalf("StageSystemOps(A): %v", err)
		}
		bA := d.NewBatch()
		if err := bA.Set([]byte("order/k1"), []byte("v1")); err != nil {
			t.Fatalf("Set(A): %v", err)
		}
		if err := bA.Write(); err != nil {
			t.Fatalf("Write(A): %v", err)
		}
		_ = bA.Close()
		if len(order) != 0 || !d.HasUnsyncedSystemOps() {
			t.Fatalf("expected A to remain unsynced waiter, order=%v unsynced=%v", order, d.HasUnsyncedSystemOps())
		}

		// Batch B piggybacked onto WriteSync() must activate A before B.
		if err := d.StageSystemOps([]SystemKVOp{{
			Key:   []byte("dict/test-order"),
			Value: []byte("B"),
		}}, func() { order = append(order, "B") }); err != nil {
			t.Fatalf("StageSystemOps(B): %v", err)
		}
		bB := d.NewBatch()
		if err := bB.Set([]byte("order/k2"), []byte("v2")); err != nil {
			t.Fatalf("Set(B): %v", err)
		}
		if err := bB.WriteSync(); err != nil {
			t.Fatalf("WriteSync(B): %v", err)
		}
		_ = bB.Close()

		if len(order) != 2 || order[0] != "A" || order[1] != "B" {
			t.Fatalf("expected strict commit-order activation [A B], got %v", order)
		}
		if d.HasUnsyncedSystemOps() {
			t.Fatal("expected HasUnsyncedSystemOps=false after WriteSync")
		}
	})

	t.Run("command_wal_write_sync_waits_for_physical_seal", func(t *testing.T) {
		dir := t.TempDir()
		d, err := Open(Options{
			Dir:        dir,
			ChunkSize:  64 << 10,
			Durability: DurabilityDurable,
			CommandWAL: true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()

		// Hold the async root-publication coordinator with a delay so
		// Batch.WriteSync returns after admission but BEFORE physical sealing.
		d.rootPublicationFixedDelay = 200 * time.Millisecond
		var activated atomic.Bool
		if err := d.StageSystemOps([]SystemKVOp{{
			Key:   []byte("dict/test-cwal-seal"),
			Value: []byte("sealed"),
		}}, func() { activated.Store(true) }); err != nil {
			t.Fatalf("StageSystemOps: %v", err)
		}
		b := d.NewBatch()
		if err := b.Set([]byte("cwal/k1"), []byte("v1")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("WriteSync: %v", err)
		}
		_ = b.Close()

		// CommandWAL WriteSync returns before physical root seal completes, so
		// activation MUST NOT have fired yet.
		if activated.Load() {
			t.Fatal("CommandWAL WriteSync activated staged dict before physical root durability")
		}
		if !d.HasUnsyncedSystemOps() {
			t.Fatal("expected HasUnsyncedSystemOps=true while physical seal is in flight")
		}

		// Once physical sealing completes (Checkpoint / WaitThrough), activation
		// fires automatically.
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if !activated.Load() {
			t.Fatal("expected staged dict activation after physical root seal")
		}
		if d.HasUnsyncedSystemOps() {
			t.Fatal("expected HasUnsyncedSystemOps=false after physical root seal")
		}
	})
}

func TestNoDictRewriteLeafPageLogConcurrentAppendPreservesLeafDict(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 << 10,
		IndexOuterLeavesInValueLog: true,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 1)
	writer.ridAlloc = newRewriteRIDAllocator(1, nil)
	writer.blockCompression = true
	writer.blockCodec = valuelog.BlockCodecSnappy
	writer.leafBlockCodec = valuelog.BlockCodecSnappy
	leafPage := bytes.Repeat([]byte("concurrent-nodict-leaf-log-dict|"), page.PageSize/len("concurrent-nodict-leaf-log-dict|"))
	const dictID = uint64(7307)
	dictSamples := make([][]byte, 8)
	for i := range dictSamples {
		sample := append([]byte(nil), leafPage...)
		binary.LittleEndian.PutUint64(sample[len(sample)-8:], uint64(i+1))
		dictSamples[i] = sample
	}
	dictPayload, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: dictSamples,
		History:  append([]byte(nil), dictSamples[0]...),
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("zstd.BuildDict: %v", err)
	}
	writer.SetLeafDictMode(dictID, dictPayload, false)

	// Install through DB.SetLeafPageLog so the test exercises the full production
	// stack: leafPageLogLaneGroup -> leafPageLogWithRecordLengthHints -> rewriteWriter.
	d.SetLeafPageLog(writer)
	laneGroup := d.leafPageLog
	defer func() {
		d.SetLeafPageLog(nil)
		if closer, ok := laneGroup.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()

	// Verify wrapNoDictLeafPageLog does NOT wrap a non-dict-capable log wrapped
	// by leafPageLogWithRecordLengthHints (preserving cached-mode default zipper
	// reuse and prepared-payload fast paths).
	plainWrapped := wrapLeafPageLogWithRecordLengthHints(d, nopPlainLeafPageLog{})
	if got := wrapNoDictLeafPageLog(plainWrapped); got != plainWrapped {
		t.Fatalf("wrapNoDictLeafPageLog unexpectedly wrapped non-dict-capable log %T -> %T", plainWrapped, got)
	}
	plainLaneWrapped := wrapLeafPageLogWithLaneSelection(plainWrapped)
	if got := wrapNoDictLeafPageLog(plainLaneWrapped); got != plainLaneWrapped {
		t.Fatalf("wrapNoDictLeafPageLog unexpectedly wrapped non-dict-capable lane group %T -> %T", plainLaneWrapped, got)
	}

	// Exercise multiple distinct noDict wrappers (matching concurrent optimistic
	// systemRootOrderedPublishOptions calls) alongside concurrent direct
	// dictionary-enabled user leaf appends on the same shared leafPageLogLaneGroup,
	// covering single, batch, and stable-resource paths.
	const noDictWorkers = 6
	const userWorkers = 2
	const iters = 16
	errCh := make(chan error, noDictWorkers+userWorkers)
	var noDictPtrsMu sync.Mutex
	var noDictPtrs []page.LeafLogPtr
	var userPtrsMu sync.Mutex
	var userPtrs []page.LeafLogPtr
	var wg sync.WaitGroup
	modes := 2
	if rootpublication.StableRelativeNamespaceSupported() {
		modes = 4
	}
	for g := 0; g < noDictWorkers; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wrapped := wrapNoDictLeafPageLog(laneGroup)
			target := wrapped
			if provider, ok := wrapped.(LeafPageLogLaneProvider); ok {
				if lane, ok := provider.LeafPageLogLane(id % 3); ok {
					target = lane
				}
			}
			for i := 0; i < iters; i++ {
				switch (id + i) % modes {
				case 0:
					ptr, err := target.AppendLeafPage(leafPage)
					if err != nil {
						errCh <- err
						return
					}
					noDictPtrsMu.Lock()
					noDictPtrs = append(noDictPtrs, ptr)
					noDictPtrsMu.Unlock()
				case 1:
					batcher, ok := target.(LeafPageBatchLog)
					if !ok {
						errCh <- fmt.Errorf("expected LeafPageBatchLog on noDict wrapper")
						return
					}
					ptrs, err := batcher.AppendLeafPages([][]byte{leafPage, leafPage})
					if err != nil {
						errCh <- err
						return
					}
					noDictPtrsMu.Lock()
					noDictPtrs = append(noDictPtrs, ptrs...)
					noDictPtrsMu.Unlock()
				case 2:
					stable, ok := target.(LeafPageStableLog)
					if !ok {
						errCh <- fmt.Errorf("expected LeafPageStableLog on noDict wrapper")
						return
					}
					ptr, res, err := stable.AppendLeafPageWithStableResources(leafPage)
					if err != nil {
						errCh <- err
						return
					}
					if res != nil {
						res.Release()
					}
					noDictPtrsMu.Lock()
					noDictPtrs = append(noDictPtrs, ptr)
					noDictPtrsMu.Unlock()
				case 3:
					stableBatch, ok := target.(LeafPageStableBatchLog)
					if !ok {
						errCh <- fmt.Errorf("expected LeafPageStableBatchLog on noDict wrapper")
						return
					}
					ptrs, res, err := stableBatch.AppendLeafPagesWithStableResources([][]byte{leafPage, leafPage})
					if err != nil {
						errCh <- err
						return
					}
					if res != nil {
						res.Release()
					}
					noDictPtrsMu.Lock()
					noDictPtrs = append(noDictPtrs, ptrs...)
					noDictPtrsMu.Unlock()
				}
			}
		}(g)
	}
	for g := 0; g < userWorkers; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var target LeafPageLog = laneGroup
			if provider, ok := laneGroup.(LeafPageLogLaneProvider); ok {
				if lane, ok := provider.LeafPageLogLane(id % 3); ok {
					target = lane
				}
			}
			for i := 0; i < iters; i++ {
				ptr, err := target.AppendLeafPage(leafPage)
				if err != nil {
					errCh <- err
					return
				}
				userPtrsMu.Lock()
				userPtrs = append(userPtrs, ptr)
				userPtrsMu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent leaf log append failed: %v", err)
	}
	if err := laneGroup.Flush(); err != nil {
		t.Fatalf("laneGroup.Flush: %v", err)
	}
	if writer.leafDictID != dictID {
		t.Fatalf("writer.leafDictID=%d after concurrent appends; want %d", writer.leafDictID, dictID)
	}
	if !bytes.Equal(writer.leafDict, dictPayload) {
		t.Fatalf("writer.leafDict was mutated after concurrent appends")
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	vset := d.valueLogManager.CurrentSetNoRefresh()
	if vset == nil {
		t.Fatal("nil value log set after flush")
	}
	defer func() { _ = d.valueLogManager.Release(vset) }()
	for _, ptr := range noDictPtrs {
		vptr := ptr.ValuePtr()
		f := vset.Files[vptr.FileID]
		if f == nil {
			t.Fatalf("missing leaf log file %d for ptr %+v", vptr.FileID, ptr)
		}
		gotDictID, err := f.PreparedRecordDictionaryID(vptr)
		if err != nil {
			t.Fatalf("PreparedRecordDictionaryID(%+v): %v", ptr, err)
		}
		if gotDictID != 0 {
			t.Fatalf("noDict system outer-leaf ptr %+v used dictID=%d; want 0", ptr, gotDictID)
		}
	}
	userDictHits := 0
	for _, ptr := range userPtrs {
		vptr := ptr.ValuePtr()
		f := vset.Files[vptr.FileID]
		if f == nil {
			t.Fatalf("missing leaf log file %d for user ptr %+v", vptr.FileID, ptr)
		}
		gotDictID, err := f.PreparedRecordDictionaryID(vptr)
		if err != nil {
			t.Fatalf("PreparedRecordDictionaryID(user %+v): %v", ptr, err)
		}
		if gotDictID == dictID {
			userDictHits++
		}
	}
	if userDictHits != len(userPtrs) {
		t.Fatalf("concurrent user outer-leaf appends preserved dictID=%d on %d/%d writes; want all", dictID, userDictHits, len(userPtrs))
	}
}

type nopPlainLeafPageLog struct{}

func (nopPlainLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}
func (nopPlainLeafPageLog) Flush() error { return nil }
func (nopPlainLeafPageLog) Sync() error  { return nil }

func TestCompactStorage_OfflineOuterLeafSystemRootTrainsLeafDict(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 << 10,
		IndexOuterLeavesInValueLog: true,
		FreshSystemPagerLeaves:     false,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionAuto,
		},
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(seed): %v", err)
	}
	seedWriter := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
	seedWriter.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	seedWriter.ridAlloc = newRewriteRIDAllocator(1, nil)
	d.SetLeafPageLog(seedWriter)

	// Seed enough compressible inline keys to exceed DefaultTrainMinRecords (64)
	// and DefaultTrainBytes (1 MiB) of outer leaf pages so offline CompactStorage
	// dictionary training produces and publishes a real outer_leaf dictionary.
	b := d.NewBatch()
	baseVal := bytes.Repeat([]byte("offline-compact-leaf-dict-training-value|"), 6)
	const seedKeys = 5000
	for i := 0; i < seedKeys; i++ {
		key := []byte(fmt.Sprintf("offline-compact/key/%06d", i))
		v := append([]byte(nil), baseVal...)
		binary.LittleEndian.PutUint32(v[len(v)-4:], uint32(i))
		if err := b.Set(key, v); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("seed WriteSync: %v", err)
	}
	_ = b.Close()
	d.SetLeafPageLog(nil)
	if err := seedWriter.Close(); err != nil {
		t.Fatalf("seedWriter.Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close(seed): %v", err)
	}

	// Reopen backend-only with db.leafPageLog == nil and systemRootUsesOuterLeaves() == true,
	// mirroring offline/standalone treedb.OpenBackend -> CompactStorage.
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(reopen): %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.leafPageLog != nil {
		t.Fatalf("expected reopened backend db.leafPageLog == nil")
	}
	if !reopened.systemRootUsesOuterLeaves() {
		t.Fatalf("expected systemRootUsesOuterLeaves() == true for regression coverage")
	}

	var publishedDictID uint64
	reopened.valueLogDictLookup = reopened.LookupSystemDictBytes
	reopened.valueLogDictPut = func(ctx context.Context, dict []byte) (uint64, error) {
		sum := sha256.Sum256(dict)
		id := binary.BigEndian.Uint64(sum[:8])
		ptr, err := reopened.AppendRawSystemValueLog(0, dict)
		if err != nil {
			return 0, err
		}
		if err := reopened.ApplySystemOps([]SystemKVOp{{
			Key:       SystemDictBytesKey(id),
			ValuePtr:  ptr,
			IsPointer: true,
		}}, true); err != nil {
			return 0, err
		}
		publishedDictID = id
		return id, nil
	}
	reopened.valueLogDictPublishRewriteLeaf = func(ctx context.Context, dict []byte, class string, useRawPages bool) (uint64, error) {
		return reopened.valueLogDictPut(ctx, dict)
	}

	if _, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
		t.Fatalf("offline CompactStorage with outer-leaf system root failed: %v", err)
	}
	if publishedDictID == 0 {
		t.Fatalf("expected offline CompactStorage to train and publish an outer_leaf dictionary")
	}
	if reopened.leafPageLog != nil {
		t.Fatalf("expected CompactStorage handoff cleanup to restore db.leafPageLog == nil")
	}

	// Verify the published dictionary in SystemRootPageID decodes via fixed-scratch
	// exact lookup (which enforces DictID == 0 on system outer leaves).
	snap := reopened.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	var keyScratch, leafScratch [page.PageSize]byte
	if _, err := snap.GetSystemEntryExactWithFixedScratch(SystemDictBytesKey(publishedDictID), keyScratch[:], leafScratch[:]); err != nil {
		t.Fatalf("GetSystemEntryExactWithFixedScratch(publishedDictID=%d): %v", publishedDictID, err)
	}

	// Verify user data remained readable after offline compaction.
	got, err := reopened.Get([]byte("offline-compact/key/000300"))
	if err != nil {
		t.Fatalf("Get(user key): %v", err)
	}
	wantVal := append([]byte(nil), baseVal...)
	binary.LittleEndian.PutUint32(wantVal[len(wantVal)-4:], 300)
	if !bytes.Equal(got, wantVal) {
		t.Fatalf("user value mismatch after offline CompactStorage")
	}
}

func TestCompactStorage_ExhaustiveReplaceableOwnerOuterLeafSystemRootTrainsLeafDict(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 << 10,
		CommandWAL:                 true,
		IndexOuterLeavesInValueLog: true,
		FreshSystemPagerLeaves:     false,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionAuto,
		},
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(seed): %v", err)
	}
	if d.leafPageLog == nil {
		_ = d.Close()
		t.Fatal("expected CommandWAL open to install replaceable replay-inline leaf page log")
	}

	b := d.NewBatch()
	baseVal := bytes.Repeat([]byte("exhaustive-replaceable-owner-leaf-dict|"), 6)
	const seedKeys = 5000
	for i := 0; i < seedKeys; i++ {
		key := []byte(fmt.Sprintf("exhaustive-compact/key/%06d", i))
		v := append([]byte(nil), baseVal...)
		binary.LittleEndian.PutUint32(v[len(v)-4:], uint32(i))
		if err := b.Set(key, v); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("seed WriteSync: %v", err)
	}
	_ = b.Close()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("seed Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close(seed): %v", err)
	}

	// Reopen so previousLeafPageLog is a freshly initialized replaceable
	// replayInlineLeafPageLog whose first append during prepareCurrentRewriteLeafDict
	// creates a new leaf segment (seq = maxSeq + 1).
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(reopen): %v", err)
	}
	defer func() {
		if reopened != nil {
			_ = reopened.Close()
		}
	}()
	if reopened.leafPageLog == nil {
		t.Fatal("expected reopened CommandWAL DB to have replaceable leafPageLog")
	}
	if !reopened.systemRootUsesOuterLeaves() {
		t.Fatal("expected systemRootUsesOuterLeaves() == true")
	}
	previousLeafPageLog := reopened.leafPageLog

	var publishedDictID uint64
	reopened.valueLogDictLookup = reopened.LookupSystemDictBytes
	reopened.valueLogDictPut = func(ctx context.Context, dict []byte) (uint64, error) {
		sum := sha256.Sum256(dict)
		id := binary.BigEndian.Uint64(sum[:8])
		ptr, err := reopened.AppendRawSystemValueLog(0, dict)
		if err != nil {
			return 0, err
		}
		if err := reopened.ApplySystemOps([]SystemKVOp{{
			Key:       SystemDictBytesKey(id),
			ValuePtr:  ptr,
			IsPointer: true,
		}}, true); err != nil {
			return 0, err
		}
		publishedDictID = id
		return id, nil
	}
	reopened.valueLogDictPublishRewriteLeaf = func(ctx context.Context, dict []byte, class string, useRawPages bool) (uint64, error) {
		return reopened.valueLogDictPut(ctx, dict)
	}

	beforeSegs, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("listValueLogSegments(before): %v", err)
	}
	beforeMaxLeafSeq := maxRewriteLaneSeq(beforeSegs, rewriteLeafLogLaneID)

	writer, handoff, err := reopened.installCompactStorageLeafPageLog(CompactStorageOptions{
		Mode: CompactStorageExhaustive,
	})
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog(exhaustive replaceable owner) failed: %v", err)
	}
	if writer == nil || handoff == nil {
		t.Fatalf("expected non-nil compact writer and handoff, got writer=%v handoff=%v", writer, handoff)
	}
	if publishedDictID == 0 {
		_ = handoff.cleanup()
		t.Fatal("expected exhaustive installCompactStorageLeafPageLog to train and publish an outer_leaf dictionary")
	}
	if writer.leafDictID != publishedDictID {
		_ = handoff.cleanup()
		t.Fatalf("writer.leafDictID=%d; want publishedDictID=%d", writer.leafDictID, publishedDictID)
	}

	afterDictSegs, err := listValueLogSegments(dir)
	if err != nil {
		_ = handoff.cleanup()
		t.Fatalf("listValueLogSegments(afterDict): %v", err)
	}
	afterDictMaxLeafSeq := maxRewriteLaneSeq(afterDictSegs, rewriteLeafLogLaneID)
	if afterDictMaxLeafSeq <= beforeMaxLeafSeq {
		_ = handoff.cleanup()
		t.Fatalf("expected previousLeafPageLog dict publication to create a new leaf segment (> %d), got %d", beforeMaxLeafSeq, afterDictMaxLeafSeq)
	}

	// Appending through the installed compact writer must allocate a strictly
	// higher leaf segment sequence than the segment created by previousLeafPageLog
	// during prepareCurrentRewriteLeafDict, never colliding with it.
	compactPage := bytes.Repeat([]byte("exhaustive-compact-leaf-page-ok|"), page.PageSize/len("exhaustive-compact-leaf-page-ok|"))
	compactPtr, err := reopened.leafPageLog.AppendLeafPage(compactPage)
	if err != nil {
		_ = handoff.cleanup()
		t.Fatalf("compact writer AppendLeafPage: %v", err)
	}
	_, compactSeq := valuelog.DecodeFileID(compactPtr.FileID)
	if compactSeq <= afterDictMaxLeafSeq {
		_ = handoff.cleanup()
		t.Fatalf("compact writer collided with previousLeafPageLog leaf segment: compactSeq=%d <= afterDictMaxLeafSeq=%d", compactSeq, afterDictMaxLeafSeq)
	}
	if err := reopened.leafPageLog.Flush(); err != nil {
		_ = handoff.cleanup()
		t.Fatalf("compact writer Flush: %v", err)
	}
	if err := handoff.cleanup(); err != nil {
		t.Fatalf("handoff.cleanup: %v", err)
	}
	if reopened.leafPageLog != previousLeafPageLog {
		t.Fatalf("expected CompactStorage handoff to restore previousLeafPageLog %T, got %T", previousLeafPageLog, reopened.leafPageLog)
	}

	// Verify restored previousLeafPageLog can continue writing without colliding
	// with any segment created by either dict publication or compact writer.
	if err := reopened.SetSync([]byte("exhaustive-compact/post"), []byte("post-ok")); err != nil {
		t.Fatalf("SetSync after exhaustive CompactStorage: %v", err)
	}
	if err := reopened.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after exhaustive CompactStorage: %v", err)
	}

	snap := reopened.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	var keyScratch, leafScratch [page.PageSize]byte
	if _, err := snap.GetSystemEntryExactWithFixedScratch(SystemDictBytesKey(publishedDictID), keyScratch[:], leafScratch[:]); err != nil {
		_ = snap.Close()
		t.Fatalf("GetSystemEntryExactWithFixedScratch(publishedDictID=%d): %v", publishedDictID, err)
	}
	_ = snap.Close()

	// Close and reopen to prove every persisted leaf segment is intact with no
	// overlapping writer offsets or RID collisions.
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close(reopened): %v", err)
	}
	reopened = nil

	verified, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(verify): %v", err)
	}
	defer func() { _ = verified.Close() }()
	got, err := verified.Get([]byte("exhaustive-compact/key/000300"))
	if err != nil {
		t.Fatalf("Get(seeded key after reopen): %v", err)
	}
	wantVal := append([]byte(nil), baseVal...)
	binary.LittleEndian.PutUint32(wantVal[len(wantVal)-4:], 300)
	if !bytes.Equal(got, wantVal) {
		t.Fatalf("seeded value mismatch after exhaustive CompactStorage reopen")
	}
	gotPost, err := verified.Get([]byte("exhaustive-compact/post"))
	if err != nil || string(gotPost) != "post-ok" {
		t.Fatalf("post-compact key mismatch after reopen: got=%q err=%v", gotPost, err)
	}
}
