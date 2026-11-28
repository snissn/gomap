package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
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
