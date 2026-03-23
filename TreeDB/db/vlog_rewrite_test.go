package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func withRewritePlanEstimateCounter(t *testing.T) *atomic.Uint64 {
	t.Helper()
	var counter atomic.Uint64
	unregister := registerRewritePlanLiveEstimateHook(func() {
		counter.Add(1)
	})
	t.Cleanup(func() {
		unregister()
	})
	return &counter
}

func closeNoErr(t *testing.T, c interface{ Close() error }) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func assertRewritePlanStableFieldsEqual(t *testing.T, got, want ValueLogRewritePlan) {
	t.Helper()
	gotIDs := slices.Clone(got.SourceFileIDs)
	wantIDs := slices.Clone(want.SourceFileIDs)
	slices.Sort(gotIDs)
	slices.Sort(wantIDs)
	if !slices.Equal(gotIDs, wantIDs) ||
		got.SegmentsTotal != want.SegmentsTotal ||
		got.SegmentsSelected != want.SegmentsSelected ||
		got.BytesTotal != want.BytesTotal ||
		got.BytesLive != want.BytesLive ||
		got.BytesStale != want.BytesStale ||
		got.SelectedBytesTotal != want.SelectedBytesTotal ||
		got.SelectedBytesLive != want.SelectedBytesLive ||
		got.SelectedBytesStale != want.SelectedBytesStale ||
		got.AgeBlockedSegments != want.AgeBlockedSegments ||
		got.AgeBlockedBytesTotal != want.AgeBlockedBytesTotal ||
		got.AgeBlockedBytesLive != want.AgeBlockedBytesLive ||
		got.AgeBlockedBytesStale != want.AgeBlockedBytesStale ||
		got.AgeBlockedMinRemainingAge != want.AgeBlockedMinRemainingAge {
		t.Fatalf("rewrite plans differ on stable fields:\ngot=%+v\nwant=%+v", got, want)
	}
}

func TestValueLogRewriteOffline_RewritesAndShrinks(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	path1 := filepath.Join(walDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	w1.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptr1a, err := w1.Append(0, nil, 1, bytes.Repeat([]byte{0x01}, 128))
	if err != nil {
		t.Fatalf("append1a: %v", err)
	}
	_, err = w1.Append(0, nil, 2, bytes.Repeat([]byte{0x02}, 128))
	if err != nil {
		t.Fatalf("append1b: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	path2 := filepath.Join(walDir, "value-l0-000002.log")
	id2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	ptr2a, err := w2.Append(0, nil, 3, bytes.Repeat([]byte{0x03}, 128))
	if err != nil {
		t.Fatalf("append2a: %v", err)
	}
	_, err = w2.Append(0, nil, 4, bytes.Repeat([]byte{0x04}, 128))
	if err != nil {
		t.Fatalf("append2b: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1a); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2a); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.BytesAfter >= stats.BytesBefore {
		t.Fatalf("expected bytes to shrink, before=%d after=%d", stats.BytesBefore, stats.BytesAfter)
	}

	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old segment1 removed, err=%v", err)
	}
	if _, err := os.Stat(path2); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old segment2 removed, err=%v", err)
	}

	db, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	val, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if !bytes.Equal(val, bytes.Repeat([]byte{0x01}, 128)) {
		t.Fatalf("k1 mismatch")
	}
	val, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if !bytes.Equal(val, bytes.Repeat([]byte{0x03}, 128)) {
		t.Fatalf("k2 mismatch")
	}
}

func TestNextRewriteRIDStart_IgnoresSegmentRemovedAfterScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(path, []byte{0x01}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	segments := []logSegment{
		{path: path, fileID: fileID, valueLog: true},
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	start, err := nextRewriteRIDStart(segments)
	if err != nil {
		t.Fatalf("nextRewriteRIDStart: %v", err)
	}
	if start != 1 {
		t.Fatalf("start=%d want 1", start)
	}
}

func TestNextRewriteRIDStart_ScansNewestSegmentPerLaneOnly(t *testing.T) {
	dir := t.TempDir()

	oldPath := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(oldPath, []byte{0x01}, 0o644); err != nil {
		t.Fatalf("WriteFile old: %v", err)
	}
	oldID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID old: %v", err)
	}

	newPath := filepath.Join(dir, "value-l0-000002.log")
	newID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("EncodeFileID new: %v", err)
	}
	newWriter, err := valuelog.NewWriter(newPath, newID)
	if err != nil {
		t.Fatalf("NewWriter new: %v", err)
	}
	if _, err := newWriter.Append(0, nil, 100, []byte("lane0-new")); err != nil {
		t.Fatalf("Append new: %v", err)
	}
	if err := newWriter.Close(); err != nil {
		t.Fatalf("Close new: %v", err)
	}

	otherPath := filepath.Join(dir, "value-l1-000001.log")
	otherID, err := valuelog.EncodeFileID(1, 1)
	if err != nil {
		t.Fatalf("EncodeFileID other: %v", err)
	}
	otherWriter, err := valuelog.NewWriter(otherPath, otherID)
	if err != nil {
		t.Fatalf("NewWriter other: %v", err)
	}
	if _, err := otherWriter.Append(0, nil, 90, []byte("lane1")); err != nil {
		t.Fatalf("Append other: %v", err)
	}
	if err := otherWriter.Close(); err != nil {
		t.Fatalf("Close other: %v", err)
	}

	segments := []logSegment{
		{path: oldPath, fileID: oldID, valueLog: true, seq: 1},
		{path: newPath, fileID: newID, valueLog: true, seq: 2},
		{path: otherPath, fileID: otherID, valueLog: true, seq: 1},
	}

	start, err := nextRewriteRIDStart(segments)
	if err != nil {
		t.Fatalf("nextRewriteRIDStart: %v", err)
	}
	if start != 101 {
		t.Fatalf("start=%d want 101", start)
	}
}

func TestRewriteWriter_AppendLeafPages(t *testing.T) {
	dir := t.TempDir()

	w := newRewriteWriter(dir, 0, 0, 1<<30)
	leafPages := [][]byte{
		bytes.Repeat([]byte{0x11}, page.PageSize),
		bytes.Repeat([]byte{0x22}, page.PageSize),
		bytes.Repeat([]byte{0x33}, page.PageSize),
	}
	ptrs, err := w.AppendLeafPages(leafPages)
	if err != nil {
		t.Fatalf("AppendLeafPages: %v", err)
	}
	if len(ptrs) != len(leafPages) {
		t.Fatalf("ptr count=%d want %d", len(ptrs), len(leafPages))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	path := filepath.Join(dir, "value-l0-000001.log")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, f)

	for i := range ptrs {
		got, err := valuelog.ReadAt(f, ptrs[i], true)
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", i, err)
		}
		if !bytes.Equal(got, leafPages[i]) {
			t.Fatalf("page %d mismatch", i)
		}
	}
}

func TestValueLogRewrite_HealthMetadata_PreservedAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 90_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}
	db = nil

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	healthPath := valueLogHealthPath(dir)
	beforeReopen, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health before reopen: %v", err)
	}
	if len(beforeReopen) == 0 {
		t.Fatalf("expected health metadata after rewrite")
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close reopen: %v", err)
	}

	afterReopen, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after reopen: %v", err)
	}
	if !reflect.DeepEqual(beforeReopen, afterReopen) {
		t.Fatalf("health metadata changed across reopen: before=%+v after=%+v", beforeReopen, afterReopen)
	}
}

func TestValueLogRewrite_BatchedPointerSwap_ReopenPreservesData(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 100_000, 4, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 512)
	})
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte{byte('a' + i)}, ptrs[i]); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := range ptrs {
		key := []byte{byte('a' + i)}
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		want := bytes.Repeat([]byte{byte(i + 1)}, 512)
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q", key)
		}
	}
}

func TestValueLogRewriteOnline_NoPointerKeys_DoesNotCreateNewSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 120_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("p"), 512)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete pointer key: %v", err)
	}

	segmentsBefore, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	var maxValueSeqBefore uint64
	for _, seg := range segmentsBefore {
		if seg.valueLog && seg.seq > maxValueSeqBefore {
			maxValueSeqBefore = seg.seq
		}
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 0 {
		t.Fatalf("expected no copied records, got %+v", stats)
	}

	segmentsAfter, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("list segments after rewrite: %v", err)
	}
	for _, seg := range segmentsAfter {
		if seg.valueLog && seg.seq > maxValueSeqBefore {
			t.Fatalf("unexpected new value-log segment created for no-op rewrite: %+v", seg)
		}
	}
}

func TestValueLogRewriteOnline_ProtectedPathsDoNotKeepHistoricalRewriteLanes(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	livePtr := appendPointersInNewSegment(t, dir, 0, 1, 90_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("live|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 0, 2, 91_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("protected|"), 64)
	})
	appendPointersInNewSegment(t, dir, 250, 1, 92_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("dead-a|"), 64)
	})
	appendPointersInNewSegment(t, dir, 250, 2, 93_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("dead-b|"), 64)
	})

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k"), livePtr); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)

	protected := []string{
		filepath.Join(dir, "wal", "value-l0-000002.log"),
	}
	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		ProtectedPaths: protected,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite to copy at least one record, got %+v", stats)
	}

	for _, path := range []string{
		filepath.Join(dir, "wal", "value-l250-000001.log"),
		filepath.Join(dir, "wal", "value-l250-000002.log"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be deleted after rewrite cleanup, err=%v", filepath.Base(path), err)
		}
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("live|"), 64)) {
		t.Fatalf("rewritten value mismatch")
	}
}

func TestValueLogRewriteOnline_UsesBlockCompressionWhenEnabled(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockSnappy,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 1, 1, func(i int) []byte {
		// A highly compressible payload to make FrameFlagCompressed deterministic.
		return bytes.Repeat([]byte{0}, 4096)
	})
	if len(ptrs) != 1 {
		t.Fatalf("expected 1 pointer, got %d", len(ptrs))
	}

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	segmentsBefore, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	before := make(map[string]struct{}, len(segmentsBefore))
	for _, seg := range segmentsBefore {
		before[seg.path] = struct{}{}
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected copied records, got %+v", stats)
	}

	segmentsAfter, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("list segments after rewrite: %v", err)
	}
	var newSeg string
	for _, seg := range segmentsAfter {
		if !seg.valueLog {
			continue
		}
		if _, ok := before[seg.path]; ok {
			continue
		}
		newSeg = seg.path
		break
	}
	if newSeg == "" {
		t.Fatalf("expected rewrite to create a new value-log segment")
	}

	f, err := os.Open(newSeg)
	if err != nil {
		t.Fatalf("open new segment: %v", err)
	}
	defer f.Close()

	var header [valuelog.HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		t.Fatalf("read record header: %v", err)
	}
	if header[4] != valuelog.Version {
		t.Fatalf("unexpected valuelog version %d", header[4])
	}

	valueLen := binary.LittleEndian.Uint32(header[16:20])
	payload := make([]byte, int(valueLen))
	if _, err := io.ReadFull(f, payload); err != nil {
		t.Fatalf("read record payload: %v", err)
	}
	frameHeader, _, _, _, err := valuelog.DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if frameHeader.DictID != 0 {
		t.Fatalf("expected dictID=0, got %d", frameHeader.DictID)
	}
	if frameHeader.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected rewritten frame to be block-compressed, header=%+v", frameHeader)
	}
	if got, want := frameHeader.Reserved, uint8(valuelog.BlockCodecSnappy); got != want {
		t.Fatalf("unexpected codec id %d, want %d", got, want)
	}
}

func TestValueLogRewrite_BatchedPointerSwap_SnapshotIsolation(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 110_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(10 + i)}, 512)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	snap := db.AcquireSnapshot()
	defer snap.Close()
	oldSet := snap.state.ValueLogSet
	if oldSet == nil {
		t.Fatalf("snapshot missing ValueLogSet")
	}
	oldID := ptrs[0].FileID
	if _, ok := oldSet.Files[oldID]; !ok {
		t.Fatalf("snapshot missing old segment %d", oldID)
	}

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	}); err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	gotSnap, err := snap.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("snapshot get k1: %v", err)
	}
	if !bytes.Equal(gotSnap, bytes.Repeat([]byte{10}, 512)) {
		t.Fatalf("snapshot value mismatch after rewrite")
	}

	state := db.State()
	if state == nil || state.ValueLogSet == nil {
		t.Fatalf("db state missing ValueLogSet after rewrite")
	}
	if _, ok := state.ValueLogSet.Files[oldID]; ok {
		t.Fatalf("old segment %d still visible in current state after rewrite", oldID)
	}
}

func TestValueLogRewrite_LocalityPolicy_PreservesGroupedAdjacency(t *testing.T) {
	candidates := []rewriteCandidate{
		{key: []byte("k3"), oldPtr: page.ValuePtr{FileID: 2, Offset: 400, Length: 1}},
		{key: []byte("k1"), oldPtr: page.ValuePtr{FileID: 1, Offset: 100, Length: 1}},
		{key: []byte("k2"), oldPtr: page.ValuePtr{FileID: 1, Offset: 120, Length: 1}},
		{key: []byte("k4"), oldPtr: page.ValuePtr{FileID: 2, Offset: 430, Length: 1}},
		{key: []byte("k0"), oldPtr: page.ValuePtr{FileID: 1, Offset: 80, Length: 1}},
	}

	orderRewriteCandidates(candidates, ValueLogRewriteLocalityGrouped)
	for i := 1; i < len(candidates); i++ {
		prev := candidates[i-1].oldPtr
		cur := candidates[i].oldPtr
		if prev.FileID > cur.FileID {
			t.Fatalf("file id order regressed at %d: prev=%d cur=%d", i, prev.FileID, cur.FileID)
		}
		if prev.FileID == cur.FileID && prev.Offset > cur.Offset {
			t.Fatalf("offset order regressed at %d: prev=%d cur=%d", i, prev.Offset, cur.Offset)
		}
	}
}

func TestValueLogRewrite_LocalityPolicy_NoWorseThanDefaultOnMixedSets(t *testing.T) {
	candidates := []rewriteCandidate{
		{key: []byte("a"), oldPtr: page.ValuePtr{FileID: 2, Offset: 1000, Length: 1}},
		{key: []byte("b"), oldPtr: page.ValuePtr{FileID: 1, Offset: 10, Length: 1}},
		{key: []byte("c"), oldPtr: page.ValuePtr{FileID: 2, Offset: 1040, Length: 1}},
		{key: []byte("d"), oldPtr: page.ValuePtr{FileID: 1, Offset: 30, Length: 1}},
		{key: []byte("e"), oldPtr: page.ValuePtr{FileID: 3, Offset: 7, Length: 1}},
		{key: []byte("f"), oldPtr: page.ValuePtr{FileID: 2, Offset: 1080, Length: 1}},
	}

	defaultOrdered := append([]rewriteCandidate(nil), candidates...)
	groupedOrdered := append([]rewriteCandidate(nil), candidates...)
	orderRewriteCandidates(defaultOrdered, ValueLogRewriteLocalityDefault)
	orderRewriteCandidates(groupedOrdered, ValueLogRewriteLocalityGrouped)

	defaultCost := rewriteLocalityTransitionCost(defaultOrdered)
	groupedCost := rewriteLocalityTransitionCost(groupedOrdered)
	if groupedCost > defaultCost {
		t.Fatalf("grouped locality cost regressed: grouped=%d default=%d", groupedCost, defaultCost)
	}
}

func TestValueLogRewrite_LocalityPolicy_DeterministicOrderingFixture(t *testing.T) {
	input := []rewriteCandidate{
		{key: []byte("k5"), oldPtr: page.ValuePtr{FileID: 2, Offset: 200, Length: 1}},
		{key: []byte("k2"), oldPtr: page.ValuePtr{FileID: 1, Offset: 30, Length: 1}},
		{key: []byte("k3"), oldPtr: page.ValuePtr{FileID: 1, Offset: 30, Length: 1}},
		{key: []byte("k1"), oldPtr: page.ValuePtr{FileID: 1, Offset: 10, Length: 1}},
	}
	orderRewriteCandidates(input, ValueLogRewriteLocalityGrouped)

	got := make([]string, 0, len(input))
	for _, c := range input {
		got = append(got, string(c.key))
	}
	want := []string{"k1", "k2", "k3", "k5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deterministic grouped order mismatch: got=%v want=%v", got, want)
	}
}

func rewriteLocalityTransitionCost(candidates []rewriteCandidate) int {
	if len(candidates) <= 1 {
		return 0
	}
	cost := 0
	for i := 1; i < len(candidates); i++ {
		prev := candidates[i-1].oldPtr
		cur := candidates[i].oldPtr
		if prev.FileID != cur.FileID {
			cost += 10
			continue
		}
		if cur.Offset < prev.Offset {
			cost += 5
			continue
		}
		delta := cur.Offset - prev.Offset
		if delta > 256 {
			cost++
		}
	}
	return cost
}

func TestValueLogRewriteOnline_SourceFileIDs_RestrictsRewriteSet(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 200_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 200_010, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs1[0].FileID},
		BatchSize:     8,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record, got %d", stats.RecordsCopied)
	}

	ptrK1, flagsK1 := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, flagsK2 := readProjectedPointerByKey(t, db, []byte("k2"))
	if flagsK1&node.FlagPointer == 0 || flagsK2&node.FlagPointer == 0 {
		t.Fatalf("expected pointer flags for rewritten keys: k1=%#x k2=%#x", flagsK1, flagsK2)
	}
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off source segment %d", ptrs1[0].FileID)
	}
	if ptrK2.FileID != ptrs2[0].FileID {
		t.Fatalf("expected k2 pointer to remain on non-selected segment %d, got %d", ptrs2[0].FileID, ptrK2.FileID)
	}
}

func TestValueLogRewriteOnline_SourceFileIDs_UsesCachedReferencedSetForCleanup(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 210_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 210_010, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	var cleanupScans atomic.Uint64
	unregister := registerRewriteCleanupReferencedScanHook(func() {
		cleanupScans.Add(1)
	})
	defer unregister()

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs1[0].FileID},
		BatchSize:     8,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record, got %d", stats.RecordsCopied)
	}
	if got := cleanupScans.Load(); got != 0 {
		t.Fatalf("cleanup scan hook=%d want 0 with cached referenced set", got)
	}
}

func TestValueLogRewriteOnline_SourceFileIDs_FallsBackCleanupScanWithoutCachedRefs(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 220_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 220_010, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	var cleanupScans atomic.Uint64
	unregister := registerRewriteCleanupReferencedScanHook(func() {
		cleanupScans.Add(1)
	})
	defer unregister()

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs1[0].FileID},
		BatchSize:     8,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record, got %d", stats.RecordsCopied)
	}
	if got := cleanupScans.Load(); got == 0 {
		t.Fatalf("cleanup scan hook=%d want > 0 without cached referenced set", got)
	}
}

func TestValueLogRewriteOnline_ReserveRIDsUsesExternalAllocator(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 260_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte{byte('a' + i)}, ptrs[i]); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	// If ValueLogRewriteOnline still scans on-disk RIDs despite ReserveRIDs
	// being supplied, this corrupt segment would make nextRewriteRIDStart fail.
	walDir := filepath.Join(dir, "wal")
	if err := os.WriteFile(filepath.Join(walDir, "value-l7-999999.log"), []byte("not-a-valid-vlog"), 0o644); err != nil {
		t.Fatalf("write bogus value-log segment: %v", err)
	}

	segmentsBefore, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("listWALSegments before: %v", err)
	}
	beforeIDs := make(map[uint32]struct{}, len(segmentsBefore))
	for _, seg := range segmentsBefore {
		if seg.valueLog {
			beforeIDs[seg.fileID] = struct{}{}
		}
	}

	var (
		reserveCalls []int
		nextRIDBase  uint64 = 900_000
	)
	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs[0].FileID},
		ReserveRIDs: func(count int) (uint64, error) {
			reserveCalls = append(reserveCalls, count)
			start := nextRIDBase
			nextRIDBase += uint64(count)
			return start, nil
		},
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 2 {
		t.Fatalf("expected 2 copied records, got %d", stats.RecordsCopied)
	}
	if len(reserveCalls) == 0 {
		t.Fatalf("expected ReserveRIDs to be called")
	}

	segmentsAfter, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("listWALSegments after: %v", err)
	}
	var newSegments []logSegment
	for _, seg := range segmentsAfter {
		if !seg.valueLog {
			continue
		}
		if _, ok := beforeIDs[seg.fileID]; ok {
			continue
		}
		newSegments = append(newSegments, seg)
	}
	if len(newSegments) == 0 {
		t.Fatalf("expected rewrite to create at least one new segment")
	}

	gotRIDs := make(map[uint64]struct{})
	for _, seg := range newSegments {
		reader, err := valuelog.NewReader(seg.path, seg.fileID)
		if err != nil {
			t.Fatalf("new reader %s: %v", seg.path, err)
		}
		reader.DisableValueDecode()
		for {
			rid, _, _, err := reader.ReadNext()
			if err == nil {
				gotRIDs[rid] = struct{}{}
				continue
			}
			if err == io.EOF || isTruncatedLogError(err) {
				break
			}
			_ = reader.Close()
			t.Fatalf("ReadNext(%s): %v", seg.path, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close reader %s: %v", seg.path, err)
		}
	}

	wantRIDs := map[uint64]struct{}{
		900_000: {},
		900_001: {},
	}
	if !reflect.DeepEqual(gotRIDs, wantRIDs) {
		t.Fatalf("unexpected rewritten RID set: got=%v want=%v", gotRIDs, wantRIDs)
	}
}

func TestRewriteRIDAllocatorReserve_NilAllocatorFailsEvenForZeroCount(t *testing.T) {
	var alloc *rewriteRIDAllocator
	if _, err := alloc.Reserve(0); err == nil {
		t.Fatalf("expected nil allocator to fail")
	}
}

func TestRewriteRIDAllocatorReserve_ZeroCountFails(t *testing.T) {
	alloc := newRewriteRIDAllocator(10, nil)
	if _, err := alloc.Reserve(0); err == nil {
		t.Fatalf("expected zero-count reserve to fail")
	}
}

func TestRewriteRIDAllocatorReserve_ExternalRangeOverlapFails(t *testing.T) {
	alloc := newRewriteRIDAllocator(100, func(count int) (uint64, error) {
		return 99, nil
	})
	if _, err := alloc.Reserve(1); err == nil {
		t.Fatalf("expected overlapping external RID range to fail")
	}
}

func TestRewriteRIDAllocatorReserve_ExternalRangeOverflowFails(t *testing.T) {
	alloc := newRewriteRIDAllocator(1, func(count int) (uint64, error) {
		return ^uint64(0) - uint64(count) + 2, nil
	})
	if _, err := alloc.Reserve(2); err == nil {
		t.Fatalf("expected overflowing external RID range to fail")
	}
}

func TestValueLogRewriteOnline_SparseSelection_RewritesHighStaleSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	// Segment 1: two records (one referenced, one stale).
	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 210_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	// Segment 2: one referenced record.
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 210_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:            8,
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.30,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record from sparse segment, got %d", stats.RecordsCopied)
	}

	ptrK1, _ := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, _ := readProjectedPointerByKey(t, db, []byte("k2"))
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off stale source segment %d", ptrs1[0].FileID)
	}
	if ptrK2.FileID != ptrs2[0].FileID {
		t.Fatalf("expected k2 pointer to remain on segment %d, got %d", ptrs2[0].FileID, ptrK2.FileID)
	}
}

func TestRewriteRIDAllocatorRejectsOverflowingExternalRange(t *testing.T) {
	alloc := newRewriteRIDAllocator(0, func(count int) (uint64, error) {
		return ^uint64(0) - uint64(count) + 2, nil
	})
	if _, err := alloc.Reserve(2); err == nil {
		t.Fatal("expected external reserve overflow to fail")
	}
}

func TestRewriteRIDAllocatorRejectsNextWraparound(t *testing.T) {
	alloc := newRewriteRIDAllocator(^uint64(0)-1, nil)
	if _, err := alloc.Reserve(2); err == nil {
		t.Fatal("expected next rid wraparound to fail")
	}
}

func TestValueLogRewriteOnline_SparseSelection_NoSelectedSources_IsNoOp(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	// One fully-live segment: no stale bytes means sparse source selection should
	// select nothing and return a deterministic no-op stats result.
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 220_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:            8,
		MaxSourceSegments:    1,
		MinSegmentStaleRatio: 0.5,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 0 {
		t.Fatalf("expected no-op rewrite with zero copied records, got %d", stats.RecordsCopied)
	}
	if stats.SegmentsAfter != stats.SegmentsBefore {
		t.Fatalf("expected no-op segment count stats, before=%d after=%d", stats.SegmentsBefore, stats.SegmentsAfter)
	}
	if stats.BytesAfter != stats.BytesBefore {
		t.Fatalf("expected no-op byte stats, before=%d after=%d", stats.BytesBefore, stats.BytesAfter)
	}
}

func TestValueLogRewritePlan_SparseSelection_SelectsHighStaleSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	// Segment 1: two records (one referenced, one stale).
	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 230_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	// Segment 2: one referenced record.
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 230_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	plan, err := db.ValueLogRewritePlan(context.Background(), ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.30,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	if len(plan.SourceFileIDs) != 1 {
		t.Fatalf("expected one selected source segment, got %d (%v)", len(plan.SourceFileIDs), plan.SourceFileIDs)
	}
	if plan.SourceFileIDs[0] != ptrs1[0].FileID {
		t.Fatalf("expected stale segment %d to be selected, got %d", ptrs1[0].FileID, plan.SourceFileIDs[0])
	}
	if plan.SelectedBytesStale <= 0 {
		t.Fatalf("expected non-zero stale bytes for selected segment, got %d", plan.SelectedBytesStale)
	}
	if plan.SelectedBytesLive <= 0 {
		t.Fatalf("expected non-zero live bytes for selected segment, got %d", plan.SelectedBytesLive)
	}
	if plan.BytesTotal <= 0 {
		t.Fatalf("expected non-zero total bytes, got %d", plan.BytesTotal)
	}
}

func TestValueLogRewritePlan_GroupedPointers_DedupLiveBytes(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	// Segment 1: two records, but many keys reference the first record via
	// grouped pointers (same record, different sub-index). The second record is
	// stale.
	//
	// Live-byte estimation must count the referenced grouped record once, not
	// once per key, otherwise the live-byte sum will saturate to the segment
	// size and hide stale bytes.
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 240_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	// Segment 2: one referenced record so segment 1 is not "active" in the
	// current set (active = latest seq per lane).
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 240_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})
	base := ptrs[0]
	recordLenHint := page.ValuePtrRecordLength(base)

	b := db.NewBatch().(*Batch)
	for i := 0; i < 3; i++ {
		ptr := base
		ptr.Length = page.ValuePtrMarkGrouped(recordLenHint, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("k%d", i)), ptr); err != nil {
			t.Fatalf("set k%d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("k_active"), ptrs2[0]); err != nil {
		t.Fatalf("set k_active: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	plan, err := db.ValueLogRewritePlan(context.Background(), ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.10,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	if len(plan.SourceFileIDs) != 1 {
		t.Fatalf("expected one selected source segment, got %d (%v)", len(plan.SourceFileIDs), plan.SourceFileIDs)
	}
	if plan.SourceFileIDs[0] != base.FileID {
		t.Fatalf("expected stale segment %d to be selected, got %d", base.FileID, plan.SourceFileIDs[0])
	}
	if plan.SelectedBytesStale <= 0 {
		t.Fatalf("expected non-zero stale bytes for selected segment, got %d", plan.SelectedBytesStale)
	}
	if plan.SelectedBytesLive <= 0 {
		t.Fatalf("expected non-zero live bytes for selected segment, got %d", plan.SelectedBytesLive)
	}
}

func TestValueLogRewritePlan_NoSelectionKnobs_ReturnsTotalsOnly(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 245_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	plan, err := db.ValueLogRewritePlan(context.Background(), ValueLogRewriteOnlineOptions{})
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	if len(plan.SourceFileIDs) != 0 {
		t.Fatalf("expected no selected source segments without selection knobs, got %v", plan.SourceFileIDs)
	}
	if plan.SegmentsSelected != 0 {
		t.Fatalf("SegmentsSelected=%d want 0", plan.SegmentsSelected)
	}
	if plan.SelectedBytesTotal != 0 || plan.SelectedBytesLive != 0 || plan.SelectedBytesStale != 0 {
		t.Fatalf("expected zero selected-byte stats without selection knobs, got total=%d live=%d stale=%d", plan.SelectedBytesTotal, plan.SelectedBytesLive, plan.SelectedBytesStale)
	}
	if plan.BytesTotal <= 0 {
		t.Fatalf("expected non-zero total bytes, got %d", plan.BytesTotal)
	}
}

func TestValueLogRewritePlan_CachesLiveBytesForUnchangedState(t *testing.T) {
	dir := t.TempDir()
	counter := withRewritePlanEstimateCounter(t)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 246_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 246_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	opts := ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.10,
		MinSegmentStaleBytes: 1,
	}
	plan1, err := db.ValueLogRewritePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("first ValueLogRewritePlan: %v", err)
	}
	plan2, err := db.ValueLogRewritePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("second ValueLogRewritePlan: %v", err)
	}
	assertRewritePlanStableFieldsEqual(t, plan1, plan2)
	if got := counter.Load(); got != 1 {
		t.Fatalf("live-byte estimate runs=%d want 1", got)
	}
}

func TestValueLogRewritePlan_InvalidatesCachedLiveBytesAfterCommit(t *testing.T) {
	dir := t.TempDir()
	counter := withRewritePlanEstimateCounter(t)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 247_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 247_100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	opts := ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.10,
		MinSegmentStaleBytes: 1,
	}
	if _, err := db.ValueLogRewritePlan(context.Background(), opts); err != nil {
		t.Fatalf("first ValueLogRewritePlan: %v", err)
	}
	if got := counter.Load(); got != 1 {
		t.Fatalf("live-byte estimate runs=%d want 1 after first plan", got)
	}

	b = db.NewBatch().(*Batch)
	if err := b.Set([]byte("new-key"), []byte("new-value")); err != nil {
		t.Fatalf("set new-key: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write new-key: %v", err)
	}
	closeNoErr(t, b)

	if _, err := db.ValueLogRewritePlan(context.Background(), opts); err != nil {
		t.Fatalf("second ValueLogRewritePlan: %v", err)
	}
	if got := counter.Load(); got != 2 {
		t.Fatalf("live-byte estimate runs=%d want 2 after commit", got)
	}
}

func readProjectedPointerByKey(t *testing.T, db *DB, key []byte) (page.ValuePtr, byte) {
	t.Helper()
	it, err := db.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorWithOptions: %v", err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if !bytes.Equal(it.UnsafeKey(), key) {
			continue
		}
		_, ptr, flags := it.UnsafeEntry()
		return ptr, flags
	}
	if err := it.Error(); err != nil {
		t.Fatalf("projection iterator error: %v", err)
	}
	t.Fatalf("missing key %q in projection iterator", key)
	return page.ValuePtr{}, 0
}

func TestSelectRewriteSourceSegments_OversizeCandidates_SelectsOne(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "v1.log")
	path2 := filepath.Join(dir, "v2.log")
	if err := os.WriteFile(path1, bytes.Repeat([]byte{1}, 100), 0o644); err != nil {
		t.Fatalf("write path1: %v", err)
	}
	if err := os.WriteFile(path2, bytes.Repeat([]byte{2}, 100), 0o644); err != nil {
		t.Fatalf("write path2: %v", err)
	}

	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}
	active := map[uint32]struct{}{}
	liveByID := map[uint32]int64{
		1: 90, // stale 10
		2: 80, // stale 20 (preferred)
	}

	selected, _ := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		MaxSourceBytes:       32,
		MaxSourceSegments:    2,
		MinSegmentStaleBytes: 1,
	}, files, active, liveByID)

	if len(selected) != 1 {
		t.Fatalf("expected one selected segment when all candidates exceed byte budget, got %d", len(selected))
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("expected segment 2 selected by stale priority, got=%v", selected)
	}
}

func TestGroupedRecordKeyForPtr_UsesFullOffsetWidth(t *testing.T) {
	ptrA := page.ValuePtr{FileID: 7, Offset: (1 << 32) + 12}
	ptrB := page.ValuePtr{FileID: 7, Offset: (1 << 33) + 12}
	keyA, err := groupedRecordKeyForPtr(ptrA)
	if err != nil {
		t.Fatalf("groupedRecordKeyForPtr(ptrA): %v", err)
	}
	keyB, err := groupedRecordKeyForPtr(ptrB)
	if err != nil {
		t.Fatalf("groupedRecordKeyForPtr(ptrB): %v", err)
	}
	if keyA == keyB {
		t.Fatalf("grouped record keys collided: %+v", keyA)
	}
}

func TestSelectRewriteSourceSegments_SkipsFullyDeadSegments(t *testing.T) {
	dir := t.TempDir()

	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	if err := os.WriteFile(path1, bytes.Repeat([]byte("a"), 100), 0o600); err != nil {
		t.Fatalf("write path1: %v", err)
	}
	if err := os.WriteFile(path2, bytes.Repeat([]byte("b"), 100), 0o600); err != nil {
		t.Fatalf("write path2: %v", err)
	}

	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}
	active := map[uint32]struct{}{}
	liveByID := map[uint32]int64{
		1: 0,  // fully dead; should be GC-only
		2: 80, // partially stale; should remain eligible
	}

	selected, _ := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    2,
		MinSegmentStaleBytes: 1,
	}, files, active, liveByID)

	if _, ok := selected[1]; ok {
		t.Fatalf("fully dead segment 1 selected for rewrite: %v", selected)
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("expected partially live segment 2 selected, got=%v", selected)
	}
}

func TestSelectRewriteSourceSegments_SkipsYoungSegments(t *testing.T) {
	dir := t.TempDir()

	pathOld := filepath.Join(dir, "value-l0-000010.log")
	pathYoung := filepath.Join(dir, "value-l0-000011.log")
	if err := os.WriteFile(pathOld, bytes.Repeat([]byte("o"), 100), 0o600); err != nil {
		t.Fatalf("write old path: %v", err)
	}
	if err := os.WriteFile(pathYoung, bytes.Repeat([]byte("y"), 100), 0o600); err != nil {
		t.Fatalf("write young path: %v", err)
	}
	now := time.Now()
	oldTime := now.Add(-5 * time.Minute)
	youngTime := now.Add(-30 * time.Second)
	if err := os.Chtimes(pathOld, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes old: %v", err)
	}
	if err := os.Chtimes(pathYoung, youngTime, youngTime); err != nil {
		t.Fatalf("chtimes young: %v", err)
	}

	files := map[uint32]*valuelog.File{
		10: {Path: pathOld},
		11: {Path: pathYoung},
	}
	active := map[uint32]struct{}{}
	liveByID := map[uint32]int64{
		10: 80,
		11: 80,
	}

	selected, stats := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    4,
		MinSegmentStaleBytes: 1,
		MinSegmentAge:        2 * time.Minute,
	}, files, active, liveByID)

	if _, ok := selected[11]; ok {
		t.Fatalf("young segment 11 selected for rewrite: %v", selected)
	}
	if _, ok := selected[10]; !ok {
		t.Fatalf("expected older segment 10 selected, got=%v", selected)
	}
	if stats.ageBlockedSegments != 1 {
		t.Fatalf("ageBlockedSegments=%d want 1", stats.ageBlockedSegments)
	}
	if stats.ageBlockedBytesTotal != 100 || stats.ageBlockedBytesLive != 80 || stats.ageBlockedBytesStale != 20 {
		t.Fatalf("unexpected age-blocked bytes: %+v", stats)
	}
	if stats.ageBlockedMinRemainingAge <= 0 || stats.ageBlockedMinRemainingAge > 2*time.Minute {
		t.Fatalf("ageBlockedMinRemainingAge=%s want in (0,2m]", stats.ageBlockedMinRemainingAge)
	}
}

func TestSelectRewriteSourceSegments_SourceFileIDsHonorStaleFiltersWithLiveEstimate(t *testing.T) {
	dir := t.TempDir()

	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	for _, path := range []string{path1, path2} {
		if err := os.WriteFile(path, bytes.Repeat([]byte("p"), 100), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}
	active := map[uint32]struct{}{}
	liveByID := map[uint32]int64{
		1: 80,
		2: 20,
	}

	selected, _ := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		SourceFileIDs:        []uint32{1, 2},
		MinSegmentStaleRatio: 0.5,
		MinSegmentStaleBytes: 1,
	}, files, active, liveByID)

	if _, ok := selected[1]; ok {
		t.Fatalf("expected file 1 to be filtered, got=%v", selected)
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("expected file 2 to remain eligible, got=%v", selected)
	}
}

func TestValueLogRewritePlan_SourceFileIDsWithFiltersEstimatesLiveBytes(t *testing.T) {
	dir := t.TempDir()
	counter := withRewritePlanEstimateCounter(t)

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 247000, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 247100, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	plan, err := db.ValueLogRewritePlan(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs:        []uint32{ptrs1[0].FileID, ptrs2[0].FileID},
		MinSegmentStaleRatio: 0.10,
		MinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	if got := counter.Load(); got != 1 {
		t.Fatalf("live-byte estimate runs=%d want 1", got)
	}
	if len(plan.SourceFileIDs) != 1 || plan.SourceFileIDs[0] != ptrs1[0].FileID {
		t.Fatalf("selected source ids=%v want [%d]", plan.SourceFileIDs, ptrs1[0].FileID)
	}
	if len(plan.SelectedSegments) != 1 || plan.SelectedSegments[0].FileID != ptrs1[0].FileID {
		t.Fatalf("selected segments=%+v want file %d", plan.SelectedSegments, ptrs1[0].FileID)
	}
	if plan.SelectedBytesLive <= 0 || plan.SelectedBytesStale <= 0 {
		t.Fatalf("expected selected live/stale bytes to be populated, got live=%d stale=%d", plan.SelectedBytesLive, plan.SelectedBytesStale)
	}
}
