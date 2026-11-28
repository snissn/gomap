package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

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
}
