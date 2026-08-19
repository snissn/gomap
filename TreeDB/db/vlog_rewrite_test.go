package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
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
		got.SelectedBytesStale != want.SelectedBytesStale {
		t.Fatalf("rewrite plans differ on stable fields:\ngot=%+v\nwant=%+v", got, want)
	}
}

func TestRewriteSwapsKeySorted(t *testing.T) {
	ordered := []rewriteSwap{
		{key: []byte("a")},
		{key: []byte("b")},
		{key: []byte("c")},
	}
	if !rewriteSwapsKeySorted(ordered) {
		t.Fatalf("expected ordered swaps to be detected as sorted")
	}

	unsorted := []rewriteSwap{
		{key: []byte("b")},
		{key: []byte("a")},
		{key: []byte("c")},
	}
	if rewriteSwapsKeySorted(unsorted) {
		t.Fatalf("expected unsorted swaps to be detected as unsorted")
	}

	duplicates := []rewriteSwap{
		{key: []byte("a")},
		{key: []byte("a")},
		{key: []byte("b")},
	}
	if !rewriteSwapsKeySorted(duplicates) {
		t.Fatalf("expected duplicate keys to be treated as sorted")
	}
}

func TestNoteRewriteSwapTouchedSegments(t *testing.T) {
	b := batchpkg.New(nil, page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	swaps := []rewriteSwap{
		{newPtr: page.ValuePtr{FileID: page.ValueLogFileID(7)}},
		{newPtr: page.ValuePtr{FileID: page.ValueLogFileID(2)}},
		{newPtr: page.ValuePtr{FileID: page.ValueLogFileID(7)}},
		{newPtr: page.ValuePtr{FileID: 12}}, // non-value-log file ID
		{newPtr: page.ValuePtr{FileID: page.ValueLogFileID(5)}},
	}
	noteRewriteSwapTouchedSegments(b, swaps)
	got := b.TouchedValueLogSegments()
	if len(got) != 3 {
		t.Fatalf("len(got)=%d want=3 (got=%v)", len(got), got)
	}
	slices.Sort(got)
	want := []uint32{
		page.ValueLogFileID(2),
		page.ValueLogFileID(5),
		page.ValueLogFileID(7),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("touched segments=%v want=%v", got, want)
	}

	b.Reset()
	noteRewriteSwapTouchedSegments(b, []rewriteSwap{
		{newPtr: page.ValuePtr{FileID: 1}},
		{newPtr: page.ValuePtr{FileID: 2}},
	})
	none := b.TouchedValueLogSegments()
	if len(none) != 0 {
		t.Fatalf("non-value-log touched segments=%v want=[]", none)
	}
}

func TestRegisterRewriteCreatedValueLogSegmentsDemotesPriorMultiCurrentLeafSegment(t *testing.T) {
	dir := t.TempDir()
	ids := make([]uint32, 2)
	for i := range ids {
		id, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, uint32(i+1))
		if err != nil {
			t.Fatalf("EncodeFileID(%d): %v", i+1, err)
		}
		ids[i] = id
		path := valuelog.SegmentPath(dir, id)
		w, err := valuelog.NewWriter(path, id)
		if err != nil {
			t.Fatalf("NewWriter(%q): %v", path, err)
		}
		if _, err := w.Append(0, nil, uint64(i+1), []byte("leaf")); err != nil {
			_ = w.Close()
			t.Fatalf("Append(%q): %v", path, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%q): %v", path, err)
		}
	}

	mgr, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	mgr.SetMultiCurrentWritableLane(valuelog.ReservedLeafLogLaneID, true)
	d := &DB{valueLogManager: mgr}

	last, hasLast, err := d.registerRewriteCreatedValueLogSegments([]uint32{ids[0]}, 0, false)
	if err != nil {
		t.Fatalf("register first segment: %v", err)
	}
	if got := mgr.CurrentWritableFileIDs(); !slices.Equal(got, []uint32{ids[0]}) {
		t.Fatalf("current writable after first register=%v want [%d]", got, ids[0])
	}
	last, hasLast, err = d.registerRewriteCreatedValueLogSegments([]uint32{ids[1]}, last, hasLast)
	if err != nil {
		t.Fatalf("register second segment: %v", err)
	}
	if !hasLast || last != ids[1] {
		t.Fatalf("last registered=(%d,%t) want (%d,true)", last, hasLast, ids[1])
	}
	if got := mgr.CurrentWritableFileIDs(); !slices.Equal(got, []uint32{ids[1]}) {
		t.Fatalf("current writable after second register=%v want [%d]", got, ids[1])
	}
}

func TestLeafRefRewriteCtx_LeafRemapInlinePromotion(t *testing.T) {
	var ctx leafRefRewriteCtx

	for i := 0; i < leafRefRewriteInlineRemapCap; i++ {
		oldID := uint64(i + 1)
		newID := uint64(i + 1001)
		ctx.storeLeafRemap(oldID, newID)
	}
	if ctx.leafMap != nil {
		t.Fatalf("leaf map allocated before inline cache filled")
	}
	for i := 0; i < leafRefRewriteInlineRemapCap; i++ {
		oldID := uint64(i + 1)
		want := uint64(i + 1001)
		got, ok := ctx.lookupLeafRemap(oldID)
		if !ok || got != want {
			t.Fatalf("lookup leaf remap[%d]: got=%d ok=%v want=%d", i, got, ok, want)
		}
	}

	promoteOld := uint64(9999)
	promoteNew := uint64(19999)
	ctx.storeLeafRemap(promoteOld, promoteNew)
	if ctx.leafMap == nil {
		t.Fatalf("leaf map not promoted after inline cache overflow")
	}
	got, ok := ctx.lookupLeafRemap(promoteOld)
	if !ok || got != promoteNew {
		t.Fatalf("lookup promoted leaf remap: got=%d ok=%v want=%d", got, ok, promoteNew)
	}
}

func TestLeafRefRewriteCtx_InternalRemapInlinePromotion(t *testing.T) {
	var ctx leafRefRewriteCtx

	for i := 0; i < leafRefRewriteInlineRemapCap; i++ {
		oldID := uint64(i + 11)
		newID := uint64(i + 2011)
		ctx.storeInternalRemap(oldID, newID)
	}
	if ctx.internalMap != nil {
		t.Fatalf("internal map allocated before inline cache filled")
	}
	for i := 0; i < leafRefRewriteInlineRemapCap; i++ {
		oldID := uint64(i + 11)
		want := uint64(i + 2011)
		got, ok := ctx.lookupInternalRemap(oldID)
		if !ok || got != want {
			t.Fatalf("lookup internal remap[%d]: got=%d ok=%v want=%d", i, got, ok, want)
		}
	}

	promoteOld := uint64(8888)
	promoteNew := uint64(28888)
	ctx.storeInternalRemap(promoteOld, promoteNew)
	if ctx.internalMap == nil {
		t.Fatalf("internal map not promoted after inline cache overflow")
	}
	got, ok := ctx.lookupInternalRemap(promoteOld)
	if !ok || got != promoteNew {
		t.Fatalf("lookup promoted internal remap: got=%d ok=%v want=%d", got, ok, promoteNew)
	}
}

type rewriteTestLeafReader struct {
	values map[page.ValuePtr][]byte
}

func (r *rewriteTestLeafReader) Read(ptr page.ValuePtr) ([]byte, error) {
	val, ok := r.values[ptr]
	if !ok {
		return nil, fmt.Errorf("leaf pointer not found")
	}
	return append([]byte(nil), val...), nil
}

func (r *rewriteTestLeafReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	val, ok := r.values[ptr]
	if !ok {
		return nil, fmt.Errorf("leaf pointer not found")
	}
	return val, nil
}

type rewriteTestLeafUnsafeToReader struct {
	values map[page.ValuePtr][]byte
}

func (r *rewriteTestLeafUnsafeToReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	val, ok := r.values[ptr]
	if !ok {
		return nil, false, fmt.Errorf("leaf pointer not found")
	}
	if cap(dst) >= len(val) {
		dst = dst[:len(val)]
		copy(dst, val)
		return dst, true, nil
	}
	return val, false, nil
}

func TestLeafRefRewriteCtx_RewriteNodeUsesConfiguredLeafLog(t *testing.T) {
	root := t.TempDir()
	valueDir := filepath.Join(root, "value_vlog")
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("mkdir value dir: %v", err)
	}
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf dir: %v", err)
	}

	writer := newRewriteWriter(valueDir, 254, 0, 64<<20)
	writer.ConfigureLeafLog(leafDir, rewriteLeafLogLaneID, 0)
	leafPage := bytes.Repeat([]byte("r"), page.PageSize)
	sourceFileID, err := valuelog.EncodeFileID(3, 9)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	oldPtr := page.ValuePtr{FileID: sourceFileID, Offset: 128, Length: page.ValuePtrMarkGrouped(0, 0)}
	oldLeafPtr, err := page.LeafLogPtrFromValuePtr(oldPtr)
	if err != nil {
		t.Fatalf("LeafLogPtrFromValuePtr: %v", err)
	}
	ctx := leafRefRewriteCtx{
		ctx:        context.Background(),
		leafReader: &rewriteTestLeafReader{values: map[page.ValuePtr][]byte{oldPtr: leafPage}},
		writer:     writer,
		ridAlloc:   newRewriteRIDAllocator(1000, nil),
		sourceIDs:  map[uint32]struct{}{sourceFileID: {}},
	}

	newRef, changed, err := ctx.rewriteLeafRef(oldLeafPtr)
	if err != nil {
		t.Fatalf("rewriteLeafRef: %v", err)
	}
	if !changed {
		t.Fatalf("expected leaf ref rewrite to change node")
	}
	if newRef.Kind != page.ChildRefLeafLog {
		t.Fatalf("rewritten ref kind=%d want leaf-log", newRef.Kind)
	}
	newLeafPtr := newRef.Log
	lane, _ := valuelog.DecodeFileID(newLeafPtr.ValueLogFileID())
	if lane != rewriteLeafLogLaneID {
		t.Fatalf("rewritten leaf lane=%d want=%d", lane, rewriteLeafLogLaneID)
	}
	createdSegments, err := writer.createdSegmentsSnapshot()
	if err != nil {
		t.Fatalf("createdSegmentsSnapshot: %v", err)
	}
	if len(createdSegments) != 1 {
		t.Fatalf("expected 1 created segment, got %d", len(createdSegments))
	}
	if !strings.HasPrefix(createdSegments[0].path, leafDir+string(os.PathSeparator)) {
		t.Fatalf("rewritten leaf segment path=%q want prefix %q", createdSegments[0].path, leafDir)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLeafRefRewriteCtx_RewriteNodeUsesUnsafeToReaderWhenLeafReaderNil(t *testing.T) {
	root := t.TempDir()
	valueDir := filepath.Join(root, "value_vlog")
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("mkdir value dir: %v", err)
	}
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf dir: %v", err)
	}

	writer := newRewriteWriter(valueDir, 254, 0, 64<<20)
	writer.ConfigureLeafLog(leafDir, rewriteLeafLogLaneID, 0)
	leafPage := bytes.Repeat([]byte("u"), page.PageSize)
	sourceFileID, err := valuelog.EncodeFileID(3, 11)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	oldPtr := page.ValuePtr{FileID: sourceFileID, Offset: 128, Length: page.ValuePtrMarkGrouped(0, 0)}
	oldLeafPtr, err := page.LeafLogPtrFromValuePtr(oldPtr)
	if err != nil {
		t.Fatalf("LeafLogPtrFromValuePtr: %v", err)
	}
	ctx := leafRefRewriteCtx{
		ctx:       context.Background(),
		leafToer:  &rewriteTestLeafUnsafeToReader{values: map[page.ValuePtr][]byte{oldPtr: leafPage}},
		writer:    writer,
		ridAlloc:  newRewriteRIDAllocator(1000, nil),
		sourceIDs: map[uint32]struct{}{sourceFileID: {}},
	}

	newRef, changed, err := ctx.rewriteLeafRef(oldLeafPtr)
	if err != nil {
		t.Fatalf("rewriteLeafRef: %v", err)
	}
	if !changed {
		t.Fatalf("expected leaf ref rewrite to change node")
	}
	if newRef.Kind != page.ChildRefLeafLog {
		t.Fatalf("rewritten ref kind=%d want leaf-log", newRef.Kind)
	}
	newLeafPtr := newRef.Log
	lane, _ := valuelog.DecodeFileID(newLeafPtr.ValueLogFileID())
	if lane != rewriteLeafLogLaneID {
		t.Fatalf("rewritten leaf lane=%d want=%d", lane, rewriteLeafLogLaneID)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
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

	walDir := filepath.Join(dir, "value_vlog")
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
	if err := db.RegisterValueLogSegment(path1, id1); err != nil {
		t.Fatalf("register producer segment1: %v", err)
	}
	if err := db.RegisterValueLogSegmentReplacing(path2, id2, id1); err != nil {
		t.Fatalf("register producer segment2: %v", err)
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

func TestValueLogRewriteOffline_RewritesCollectionRootPointers(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 500_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-pointer-live|"), 32)
	})[0]
	oldPath := valueLogSegmentPath(t, dir, ptr.FileID)
	publishCollectionPointerRoot(t, db, maintenanceTestCollectionRootKey, ptr)

	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}

	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected collection pointer record to be copied, stats=%+v", stats)
	}
	if _, err := os.Stat(oldPath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old collection pointer segment removed, err=%v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, reopen)

	got := readCollectionRootValue(t, reopen, maintenanceTestCollectionRootKey, []byte("doc/p"))
	want := bytes.Repeat([]byte("collection-pointer-live|"), 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("collection value mismatch after rewrite")
	}
}

func TestValueLogRewriteOnline_RewritesCollectionRootPointers(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 510_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-online-pointer-live|"), 32)
	})[0]
	publishCollectionPointerRoot(t, db, maintenanceTestCollectionRootKey, ptr)
	primeValueLogRefTracker(t, db)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptr.FileID},
		BatchSize:     1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one collection pointer record to be copied, stats=%+v", stats)
	}
	if stats.SourceSegmentsUnreferenced != 1 {
		t.Fatalf("source segments unreferenced=%d want 1 (stats=%+v)", stats.SourceSegmentsUnreferenced, stats)
	}

	got := readCollectionRootValue(t, db, maintenanceTestCollectionRootKey, []byte("doc/p"))
	want := bytes.Repeat([]byte("collection-online-pointer-live|"), 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("collection value mismatch after online rewrite")
	}
	newPtr, flags := readCollectionProjectedPointerByKey(t, db, maintenanceTestCollectionRootKey, []byte("doc/p"))
	if flags&node.FlagPointer == 0 {
		t.Fatalf("collection entry flags=%#x want pointer", flags)
	}
	if newPtr.FileID == ptr.FileID {
		t.Fatalf("collection pointer still references source segment %d", ptr.FileID)
	}
	trackerRefs := requireValueLogRefTrackerValid(t, db)
	if _, ok := trackerRefs[ptr.FileID]; ok {
		t.Fatalf("value-log ref tracker still references source segment %d: %v", ptr.FileID, trackerRefs)
	}
	if _, ok := trackerRefs[newPtr.FileID]; !ok {
		t.Fatalf("value-log ref tracker missing rewritten segment %d: %v", newPtr.FileID, trackerRefs)
	}
	referenced, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if _, ok := referenced[ptr.FileID]; ok {
		t.Fatalf("source segment %d remains referenced after collection rewrite: %v", ptr.FileID, referenced)
	}
}

func TestValueLogRewriteOnline_PreservesRefTrackerForSystemRootPointers(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 512_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("system-online-pointer-live|"), 32)
	})[0]
	if _, err := db.PublishSystemRootIterator(mustFrozenSystemPointerMemtable(t, "sys/p", ptr).NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system pointer root: %v", err)
	}
	primeValueLogRefTracker(t, db)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptr.FileID},
		BatchSize:     1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one system pointer record to be copied, stats=%+v", stats)
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.State() == nil {
		t.Fatal("expected snapshot")
	}
	entry, err := snap.GetEntryAtRoot(snap.State().SystemRootPageID, []byte("sys/p"))
	if err != nil {
		_ = snap.Close()
		t.Fatalf("read system pointer: %v", err)
	}
	_ = snap.Close()
	if entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("system entry flags=%#x want pointer", entry.Flags)
	}
	if entry.ValuePtr.FileID == ptr.FileID {
		t.Fatalf("system pointer still references source segment %d", ptr.FileID)
	}
	trackerRefs := requireValueLogRefTrackerValid(t, db)
	if _, ok := trackerRefs[ptr.FileID]; ok {
		t.Fatalf("value-log ref tracker still references source segment %d: %v", ptr.FileID, trackerRefs)
	}
	if _, ok := trackerRefs[entry.ValuePtr.FileID]; !ok {
		t.Fatalf("value-log ref tracker missing rewritten system segment %d: %v", entry.ValuePtr.FileID, trackerRefs)
	}
}

func TestValueLogRewriteOnline_RepointsAliasedCollectionRootDescriptors(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 515_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("collection-online-alias-pointer-live-%d|", i)), 24)
	})
	rootTable, err := memtable.NewWithCapacityMode(2, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new root memtable: %v", err)
	}
	rootTable.SetEntry([]byte("doc/p"), nil, ptrs[0], node.FlagPointer)
	rootTable.SetEntry([]byte("doc/q"), nil, ptrs[1], node.FlagPointer)
	rootTable.Freeze()
	aliasDescriptorKey := "collections/root/users/alias"
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          rootTable.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			aliasDescriptorKey, encodeMaintenanceRootID(rootIDs[0]),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish aliased collection root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
	oldRoot := rootIDs[0]

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs[0].FileID},
		BatchSize:     1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 2 {
		t.Fatalf("expected two collection pointer records to be copied, stats=%+v", stats)
	}
	if stats.SourceSegmentsUnreferenced != 1 {
		t.Fatalf("source segments unreferenced=%d want 1 (stats=%+v)", stats.SourceSegmentsUnreferenced, stats)
	}

	newRoot := readCollectionRootID(t, db, maintenanceTestCollectionRootKey)
	aliasRoot := readCollectionRootID(t, db, aliasDescriptorKey)
	if newRoot == oldRoot {
		t.Fatalf("primary descriptor still points at old root %d", oldRoot)
	}
	if aliasRoot != newRoot {
		t.Fatalf("alias descriptor root=%d want rewritten root %d", aliasRoot, newRoot)
	}
	for _, descriptorKey := range []string{maintenanceTestCollectionRootKey, aliasDescriptorKey} {
		for i, key := range []string{"doc/p", "doc/q"} {
			got := readCollectionRootValue(t, db, descriptorKey, []byte(key))
			want := bytes.Repeat([]byte(fmt.Sprintf("collection-online-alias-pointer-live-%d|", i)), 24)
			if !bytes.Equal(got, want) {
				t.Fatalf("collection value mismatch through descriptor %q key %q after online rewrite", descriptorKey, key)
			}
		}
	}
	referenced, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if _, ok := referenced[ptrs[0].FileID]; ok {
		t.Fatalf("source segment %d remains referenced after aliased collection rewrite: %v", ptrs[0].FileID, referenced)
	}
}

func TestValueLogRewriteOnline_RefreshesCollectionRootThroughDescriptorAlias(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 530_000, 1, func(int) []byte {
		return []byte("old collection alias value")
	})[0]
	newValue := []byte("new collection alias value")
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 531_000, 1, func(int) []byte {
		return newValue
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value log set: %v", err)
	}

	aliasDescriptorKey := "collections/root/users/alias"
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", oldPtr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			aliasDescriptorKey, encodeMaintenanceRootID(rootIDs[0]),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish aliased collection root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
	oldRoot := rootIDs[0]
	state := db.State()
	if state == nil {
		t.Fatal("expected db state")
	}
	target := &collectionRewriteRootState{
		descriptorKey: append([]byte(nil), maintenanceTestCollectionRootKey...),
		descriptorAliases: [][]byte{
			append([]byte(nil), maintenanceTestCollectionRootKey...),
			[]byte(aliasDescriptorKey),
		},
		rootID:     oldRoot,
		systemRoot: state.SystemRootPageID,
	}

	if _, err := db.PublishSystemRootIterator(mustFrozenRawMemtable(t, aliasDescriptorKey, encodeMaintenanceRootID(oldRoot)).NewIterator(nil, nil)); err != nil {
		t.Fatalf("remove primary descriptor through system publish: %v", err)
	}
	if err := db.applyRewriteSwapBatchToCollectionRoot(target, []rewriteSwap{{key: []byte("doc/p"), oldPtr: oldPtr, newPtr: newPtr}}, false); err != nil {
		t.Fatalf("apply collection rewrite swap through descriptor alias: %v", err)
	}
	if got := readCollectionRootValue(t, db, aliasDescriptorKey, []byte("doc/p")); !bytes.Equal(got, newValue) {
		t.Fatalf("alias descriptor value=%q want %q", got, newValue)
	}
	if len(target.descriptorAliases) != 1 || !bytes.Equal(target.descriptorAliases[0], []byte(aliasDescriptorKey)) {
		t.Fatalf("refreshed descriptor aliases=%q want alias only", target.descriptorAliases)
	}
}

func TestValueLogRewriteOnline_ToleratesDivergedDescriptorAlias(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 540_000, 1, func(int) []byte {
		return []byte("old collection diverged alias value")
	})[0]
	newValue := []byte("new collection diverged alias value")
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 541_000, 1, func(int) []byte {
		return newValue
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value log set: %v", err)
	}

	aliasDescriptorKey := "collections/root/users/diverged-alias"
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", oldPtr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			aliasDescriptorKey, encodeMaintenanceRootID(rootIDs[0]),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish aliased collection root: %v", err)
	}
	oldRoot := rootIDs[0]
	otherRoot, err := db.PublishOrderedRootIterator(0, mustFrozenRawMemtable(t, "doc/other", []byte("other")).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish other root: %v", err)
	}
	if _, err := db.PublishSystemRootIterator(mustFrozenRawMemtable(t,
		maintenanceTestCollectionRootKey, encodeMaintenanceRootID(otherRoot),
		aliasDescriptorKey, encodeMaintenanceRootID(oldRoot),
	).NewIterator(nil, nil)); err != nil {
		t.Fatalf("diverge primary descriptor: %v", err)
	}

	target := &collectionRewriteRootState{
		descriptorKey: append([]byte(nil), maintenanceTestCollectionRootKey...),
		descriptorAliases: [][]byte{
			append([]byte(nil), maintenanceTestCollectionRootKey...),
			[]byte(aliasDescriptorKey),
		},
		rootID:     oldRoot,
		systemRoot: 0,
	}
	if err := db.applyRewriteSwapBatchToCollectionRoot(target, []rewriteSwap{{key: []byte("doc/p"), oldPtr: oldPtr, newPtr: newPtr}}, false); err != nil {
		t.Fatalf("apply collection rewrite swap with diverged alias: %v", err)
	}
	if got := readCollectionRootValue(t, db, aliasDescriptorKey, []byte("doc/p")); !bytes.Equal(got, newValue) {
		t.Fatalf("alias descriptor value=%q want %q", got, newValue)
	}
	if len(target.descriptorAliases) != 1 || !bytes.Equal(target.descriptorAliases[0], []byte(aliasDescriptorKey)) {
		t.Fatalf("refreshed descriptor aliases=%q want diverged alias only", target.descriptorAliases)
	}
	if gotRoot := readCollectionRootID(t, db, maintenanceTestCollectionRootKey); gotRoot != otherRoot {
		t.Fatalf("primary descriptor root=%d want unrelated root %d", gotRoot, otherRoot)
	}
}

func TestValueLogRewriteOnline_RefreshesFullyRenamedCollectionAlias(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 542_000, 1, func(int) []byte {
		return []byte("old collection fully renamed alias value")
	})[0]
	newValue := []byte("new collection fully renamed alias value")
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 543_000, 1, func(int) []byte {
		return newValue
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value log set: %v", err)
	}

	aliasDescriptorKey := "collections/root/users/renamed-old-alias"
	newDescriptorKey := "collections/root/users/renamed-new-alias"
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", oldPtr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			aliasDescriptorKey, encodeMaintenanceRootID(rootIDs[0]),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish aliased collection root: %v", err)
	}
	oldRoot := rootIDs[0]
	otherRoot, err := db.PublishOrderedRootIterator(0, mustFrozenRawMemtable(t, "doc/other", []byte("other")).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish other root: %v", err)
	}
	if _, err := db.PublishSystemRootIterator(mustFrozenRawMemtable(t,
		maintenanceTestCollectionRootKey, encodeMaintenanceRootID(otherRoot),
		aliasDescriptorKey, encodeMaintenanceRootID(otherRoot),
		newDescriptorKey, encodeMaintenanceRootID(oldRoot),
	).NewIterator(nil, nil)); err != nil {
		t.Fatalf("rename collection descriptor: %v", err)
	}

	target := &collectionRewriteRootState{
		descriptorKey: append([]byte(nil), maintenanceTestCollectionRootKey...),
		descriptorAliases: [][]byte{
			append([]byte(nil), maintenanceTestCollectionRootKey...),
			[]byte(aliasDescriptorKey),
		},
		rootID:     oldRoot,
		systemRoot: 0,
	}
	if err := db.applyRewriteSwapBatchToCollectionRoot(target, []rewriteSwap{{key: []byte("doc/p"), oldPtr: oldPtr, newPtr: newPtr}}, false); err != nil {
		t.Fatalf("apply collection rewrite swap with fully renamed alias: %v", err)
	}
	if got := readCollectionRootValue(t, db, newDescriptorKey, []byte("doc/p")); !bytes.Equal(got, newValue) {
		t.Fatalf("renamed descriptor value=%q want %q", got, newValue)
	}
	if len(target.descriptorAliases) != 1 || !bytes.Equal(target.descriptorAliases[0], []byte(newDescriptorKey)) {
		t.Fatalf("refreshed descriptor aliases=%q want renamed alias only", target.descriptorAliases)
	}
	for _, descriptorKey := range []string{maintenanceTestCollectionRootKey, aliasDescriptorKey} {
		if gotRoot := readCollectionRootID(t, db, descriptorKey); gotRoot != otherRoot {
			t.Fatalf("descriptor %q root=%d want unrelated root %d", descriptorKey, gotRoot, otherRoot)
		}
	}
}

func TestValueLogRewriteOnline_RewritesCollectionLeafRefRootPointers(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)
	dir := db.dir

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 520_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-online-leafref-pointer-live|"), 24)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value log set: %v", err)
	}
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", ptr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	oldRoot := rootIDs[0]
	requireLeafLogRootChildren(t, db, oldRoot)
	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptr.FileID},
		BatchSize:     1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one collection leaf-ref pointer record to be copied, stats=%+v", stats)
	}
	if stats.SourceSegmentsUnreferenced != 1 {
		t.Fatalf("source segments unreferenced=%d want 1 (stats=%+v)", stats.SourceSegmentsUnreferenced, stats)
	}

	newRoot := readCollectionRootID(t, db, maintenanceTestCollectionRootKey)
	if newRoot == oldRoot {
		t.Fatalf("collection descriptor still points at old leaf-ref root %d", oldRoot)
	}
	requireLeafLogRootChildren(t, db, newRoot)
	got := readCollectionRootValue(t, db, maintenanceTestCollectionRootKey, []byte("doc/p"))
	want := bytes.Repeat([]byte("collection-online-leafref-pointer-live|"), 24)
	if !bytes.Equal(got, want) {
		t.Fatalf("collection leaf-ref value mismatch after online rewrite")
	}
	newPtr, flags := readCollectionProjectedPointerByKey(t, db, maintenanceTestCollectionRootKey, []byte("doc/p"))
	if flags&node.FlagPointer == 0 {
		t.Fatalf("collection leaf-ref entry flags=%#x want pointer", flags)
	}
	if newPtr.FileID == ptr.FileID {
		t.Fatalf("collection leaf-ref pointer still references source segment %d", ptr.FileID)
	}
}

func TestValueLogRewriteOnline_RetainsStaleTrailingLeafGenerationCleanup(t *testing.T) {
	// Keep the relaxed root unpublished until Checkpoint advances its durable
	// basis while the trailing leaf-generation scan is paused.
	db, leafLog := openLeafGenerationGCTestDBWithRootPublicationDelay(t, 100*time.Millisecond)
	dir := db.dir

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 521_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-online-stale-leaf-gc|"), 24)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value log set: %v", err)
	}
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", ptr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	oldRoot := rootIDs[0]
	oldLeafPtrs := requireLeafLogRootChildren(t, db, oldRoot)
	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}

	scanEntered := make(chan struct{})
	releaseScan := make(chan struct{})
	var paused atomic.Bool
	unregister := registerLeafGenerationLiveScanHook(func() {
		if paused.CompareAndSwap(false, true) {
			close(scanEntered)
			<-releaseScan
		}
	})
	defer unregister()

	type rewriteResult struct {
		stats ValueLogRewriteStats
		err   error
	}
	done := make(chan rewriteResult, 1)
	go func() {
		stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
			SourceFileIDs: []uint32{ptr.FileID},
			BatchSize:     1,
		})
		done <- rewriteResult{stats: stats, err: err}
	}()
	select {
	case <-scanEntered:
	case <-time.After(5 * time.Second):
		close(releaseScan)
		t.Fatal("online rewrite did not enter trailing leaf-generation scan")
	}
	if err := db.Checkpoint(); err != nil {
		close(releaseScan)
		t.Fatalf("checkpoint while trailing leaf-generation scan paused: %v", err)
	}
	close(releaseScan)

	var result rewriteResult
	select {
	case result = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("online rewrite did not finish after releasing leaf-generation scan")
	}
	if result.err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", result.err)
	}
	if !result.stats.LeafGenerationCleanupRetainedRecoverableRootStale {
		t.Fatalf("retained stale leaf-generation cleanup=false, stats=%+v", result.stats)
	}
	if result.stats.RecordsCopied != 1 {
		t.Fatalf("records copied=%d want 1, stats=%+v", result.stats.RecordsCopied, result.stats)
	}
	for _, ptr := range oldLeafPtrs {
		path := leafLogSegmentPath(t, dir, ptr.FileID)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("stale trailing cleanup removed recoverable leaf segment %s: %v", path, err)
		}
	}
	want := bytes.Repeat([]byte("collection-online-stale-leaf-gc|"), 24)
	if got := readCollectionRootValue(t, db, maintenanceTestCollectionRootKey, []byte("doc/p")); !bytes.Equal(got, want) {
		t.Fatalf("collection value mismatch after retained stale leaf-generation cleanup")
	}
}

func TestFinishValueLogRewriteLeafGenerationCleanup_ReturnsNonStaleError(t *testing.T) {
	want := errors.New("leaf-generation cleanup failed")
	stats := ValueLogRewriteStats{}
	got := finishValueLogRewriteLeafGenerationCleanup(&stats, want)
	if !errors.Is(got, want) {
		t.Fatalf("cleanup error=%v want %v", got, want)
	}
	if stats.LeafGenerationCleanupRetainedRecoverableRootStale {
		t.Fatalf("non-stale cleanup marked as retained debt: %+v", stats)
	}
}

func TestValueLogRewriteOnline_CollectionRootCommitUsesCurrentStoragePolicy(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)
	dir := db.dir

	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 525_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-current-policy-old|"), 24)
	})[0]
	newValue := bytes.Repeat([]byte("collection-current-policy-new|"), 24)
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 526_000, 1, func(int) []byte {
		return newValue
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value log set: %v", err)
	}
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", oldPtr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	oldRoot := rootIDs[0]
	requireLeafLogRootChildren(t, db, oldRoot)
	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	primeValueLogRefTracker(t, db)

	state := db.State()
	if state == nil {
		t.Fatal("expected db state")
	}
	target := &collectionRewriteRootState{
		descriptorKey: append([]byte(nil), maintenanceTestCollectionRootKey...),
		descriptorAliases: [][]byte{
			append([]byte(nil), maintenanceTestCollectionRootKey...),
		},
		rootID:        oldRoot,
		systemRoot:    state.SystemRootPageID,
		storagePolicy: OrderedRootStoragePagerLeaves, // stale scan-time policy; current root uses value-log leaves.
	}
	if err := db.applyRewriteSwapBatchToCollectionRoot(target, []rewriteSwap{{key: []byte("doc/p"), oldPtr: oldPtr, newPtr: newPtr}}, false); err != nil {
		t.Fatalf("apply collection rewrite swap with stale policy: %v", err)
	}
	if db.valueLogRefTracker != nil {
		if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); ok {
			t.Fatal("expected value-log-leaf collection rewrite to invalidate value-log ref tracker")
		}
	}

	newRoot := readCollectionRootID(t, db, maintenanceTestCollectionRootKey)
	if newRoot == oldRoot {
		t.Fatalf("collection descriptor still points at old leaf-ref root %d", oldRoot)
	}
	requireLeafLogRootChildren(t, db, newRoot)
	got := readCollectionRootValue(t, db, maintenanceTestCollectionRootKey, []byte("doc/p"))
	if !bytes.Equal(got, newValue) {
		t.Fatalf("collection value mismatch after stale-policy rewrite")
	}
}

func TestValueLogRewriteOffline_RewritesCollectionLeafRefRoot(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 600_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-leafref-live|"), 32)
	})[0]
	oldValuePath := valueLogSegmentPath(t, dir, ptr.FileID)
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", ptr).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	oldRoot := rootIDs[0]
	oldLeafFileIDs := requireCollectionLeafFileIDs(t, db, oldRoot)

	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("close leaf log: %v", err)
	}

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected collection leaf-ref records to be copied, stats=%+v", stats)
	}
	if _, err := os.Stat(oldValuePath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected old collection value segment removed, err=%v", err)
	}
	requireLeafLogSegmentsRemoved(t, dir, oldLeafFileIDs)

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, reopen)

	newRoot := readCollectionRootID(t, reopen, maintenanceTestCollectionRootKey)
	if newRoot == oldRoot {
		t.Fatalf("collection descriptor still points at old leaf-ref root %d", oldRoot)
	}
	newLeafPtr := requireLeafLogRootChildren(t, reopen, newRoot)[0]
	if _, err := os.Stat(leafLogSegmentPath(t, dir, newLeafPtr.FileID)); err != nil {
		t.Fatalf("expected rewritten collection leaf segment: %v", err)
	}

	got := readCollectionRootValue(t, reopen, maintenanceTestCollectionRootKey, []byte("doc/p"))
	want := bytes.Repeat([]byte("collection-leafref-live|"), 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("collection leaf-ref value mismatch after rewrite")
	}
}

func TestValueLogRewriteOffline_RewritesCollectionLeafRefRootWithPagerDefault(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
		LeafPrefixCompression:  true,
		IndexColumnarLeaves:    true,
		IndexPackedValuePtr:    true,
		IndexInternalBaseDelta: true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	const descriptorKey = "collections/root/users/pager-default-primary"
	docValue := bytes.Repeat([]byte("collection-leafref-pager-default|"), 24)
	docs := memtable.NewAppendOnlyWithEntryCapacity(1024)
	for i := 0; i < 1024; i++ {
		docs.Set([]byte(fmt.Sprintf("doc/%04d", i)), docValue)
	}
	docs.Freeze()
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          docs.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, descriptorKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	oldRoot := rootIDs[0]
	oldLeafFileIDs := requireCollectionLeafFileIDs(t, db, oldRoot)
	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("close leaf log: %v", err)
	}

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	_ = stats
	requireLeafLogSegmentsRemoved(t, dir, oldLeafFileIDs)

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, reopen)

	newRoot := readCollectionRootID(t, reopen, descriptorKey)
	if newRoot == oldRoot {
		t.Fatalf("collection descriptor still points at old leaf-ref root %d", oldRoot)
	}
	if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(reopen.Pager(), newRoot); err != nil {
		t.Fatalf("inspect rewritten collection root %d: %v", newRoot, err)
	} else if allLeafRefs {
		t.Fatalf("rewritten collection root=%d still has only leaf-ref children with pager-default options", newRoot)
	}
	got := readCollectionRootValue(t, reopen, descriptorKey, []byte("doc/0512"))
	if !bytes.Equal(got, docValue) {
		t.Fatalf("collection leaf-ref value mismatch after rewrite")
	}
}

func TestValueLogRewriteOffline_PreservesCollectionRootStoragePolicy(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 700_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("collection-mixed-policy-live|"), 32)
	})[0]
	indexDescriptorKey := "collections/root/users/by-email"
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{
		{
			BaseRoot:      0,
			Iter:          mustFrozenSystemPointerMemtable(t, "doc/p", ptr).NewIterator(nil, nil),
			StoragePolicy: OrderedRootStorageValueLogLeaves,
		},
		{
			BaseRoot:      0,
			Iter:          mustFrozenRawMemtable(t, "idx/email/a", "").NewIterator(nil, nil),
			StoragePolicy: OrderedRootStoragePagerLeaves,
		},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 {
			return nil, fmt.Errorf("rootIDs=%d want 2", len(rootIDs))
		}
		return mustFrozenRawMemtable(t,
			maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0]),
			indexDescriptorKey, encodeMaintenanceRootID(rootIDs[1]),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish mixed collection roots: %v", err)
	}
	requireLeafLogRootChildren(t, db, rootIDs[0])
	if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), rootIDs[1]); err != nil {
		t.Fatalf("inspect index root %d: %v", rootIDs[1], err)
	} else if allLeafRefs {
		t.Fatalf("index root=%d want pager leaves", rootIDs[1])
	}

	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before rewrite: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("close leaf log: %v", err)
	}

	if _, err := ValueLogRewriteOffline(opts); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, reopen)

	primaryRoot := readCollectionRootID(t, reopen, maintenanceTestCollectionRootKey)
	requireLeafLogRootChildren(t, reopen, primaryRoot)
	indexRoot := readCollectionRootID(t, reopen, indexDescriptorKey)
	if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(reopen.Pager(), indexRoot); err != nil {
		t.Fatalf("inspect rewritten index root %d: %v", indexRoot, err)
	} else if allLeafRefs {
		t.Fatalf("rewritten index root=%d want pager leaves", indexRoot)
	}
	got := readCollectionRootValue(t, reopen, maintenanceTestCollectionRootKey, []byte("doc/p"))
	want := bytes.Repeat([]byte("collection-mixed-policy-live|"), 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("collection mixed-policy value mismatch after rewrite")
	}
	snap := reopen.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	idxVal, err := snap.GetAtRoot(indexRoot, []byte("idx/email/a"))
	if err != nil {
		t.Fatalf("read collection index root: %v", err)
	}
	if len(idxVal) != 0 {
		t.Fatalf("collection index value=%q want empty", string(idxVal))
	}
}

func TestValueLogRewriteOffline_RejectsPendingCommitLog(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	path1 := filepath.Join(valueLogDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	ptr, err := w1.Append(0, nil, 1, bytes.Repeat([]byte{0x01}, 128))
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}
	registerTestValueLogProducer(t, dir, path1, id1)

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	commitPath := filepath.Join(WALDirPath(dir), "commit-l0-999999.log")
	ww, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	if err := ww.AppendBatch([]commitlog.Record{{
		Op:    commitlog.OpSetInline,
		Key:   []byte("pending"),
		Value: []byte("v"),
		Seq:   1,
	}}); err != nil {
		_ = ww.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := ww.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	_, err = ValueLogRewriteOffline(Options{Dir: dir})
	if err == nil {
		t.Fatalf("expected clean commitlog error")
	}
	if got := err.Error(); !strings.Contains(got, "vlog-rewrite requires a clean commitlog") || !strings.Contains(got, filepath.Base(commitPath)) {
		t.Fatalf("unexpected error: %v", err)
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

func TestNextRewriteRIDStartFromSet_TracksGroupedFrameRIDs(t *testing.T) {
	dir := t.TempDir()

	fileID1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID(0,1): %v", err)
	}
	path1 := filepath.Join(dir, "value-l0-000001.log")
	w1, err := valuelog.NewWriter(path1, fileID1)
	if err != nil {
		t.Fatalf("NewWriter(path1): %v", err)
	}
	if _, err := w1.Append(0, nil, 5, bytes.Repeat([]byte{0x01}, 64)); err != nil {
		t.Fatalf("Append(path1): %v", err)
	}
	if _, err := w1.AppendFrame(0, nil, []valuelog.Record{
		{RID: 7, Value: bytes.Repeat([]byte{0x02}, 32)},
		{RID: 23, Value: bytes.Repeat([]byte{0x03}, 32)},
	}); err != nil {
		t.Fatalf("AppendFrame(path1): %v", err)
	}
	closeNoErr(t, w1)
	f1, err := os.Open(path1)
	if err != nil {
		t.Fatalf("Open(path1): %v", err)
	}
	defer closeNoErr(t, f1)

	fileID2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("EncodeFileID(0,2): %v", err)
	}
	path2 := filepath.Join(dir, "value-l0-000002.log")
	w2, err := valuelog.NewWriter(path2, fileID2)
	if err != nil {
		t.Fatalf("NewWriter(path2): %v", err)
	}
	if _, err := w2.Append(0, nil, 19, bytes.Repeat([]byte{0x04}, 64)); err != nil {
		t.Fatalf("Append(path2): %v", err)
	}
	closeNoErr(t, w2)
	f2, err := os.Open(path2)
	if err != nil {
		t.Fatalf("Open(path2): %v", err)
	}
	defer closeNoErr(t, f2)

	set := &valuelog.Set{
		Files: map[uint32]*valuelog.File{
			fileID1: {ID: fileID1, Path: path1, File: f1},
			fileID2: {ID: fileID2, Path: path2, File: f2},
		},
	}
	start, err := nextRewriteRIDStartFromSet(set)
	if err != nil {
		t.Fatalf("nextRewriteRIDStartFromSet: %v", err)
	}
	if start != 24 {
		t.Fatalf("start=%d want 24", start)
	}
}

func TestNextRewriteRIDStartFromSet_IgnoresMissingPathEntries(t *testing.T) {
	dir := t.TempDir()

	missingID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID(0,1): %v", err)
	}
	missingPath := filepath.Join(dir, "value-l0-000001.log")

	fileID2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("EncodeFileID(0,2): %v", err)
	}
	path2 := filepath.Join(dir, "value-l0-000002.log")
	w2, err := valuelog.NewWriter(path2, fileID2)
	if err != nil {
		t.Fatalf("NewWriter(path2): %v", err)
	}
	if _, err := w2.Append(0, nil, 41, bytes.Repeat([]byte{0x05}, 64)); err != nil {
		t.Fatalf("Append(path2): %v", err)
	}
	closeNoErr(t, w2)
	f2, err := os.Open(path2)
	if err != nil {
		t.Fatalf("Open(path2): %v", err)
	}
	defer closeNoErr(t, f2)

	set := &valuelog.Set{
		Files: map[uint32]*valuelog.File{
			missingID: {ID: missingID, Path: missingPath, File: nil},
			fileID2:   {ID: fileID2, Path: path2, File: f2},
		},
	}
	start, err := nextRewriteRIDStartFromSet(set)
	if err != nil {
		t.Fatalf("nextRewriteRIDStartFromSet: %v", err)
	}
	if start != 42 {
		t.Fatalf("start=%d want 42", start)
	}
}

func TestNextRewriteRIDStartFromSet_MatchesSegmentScanner(t *testing.T) {
	dir := t.TempDir()

	fileID1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID(0,1): %v", err)
	}
	path1 := filepath.Join(dir, "value-l0-000001.log")
	w1, err := valuelog.NewWriter(path1, fileID1)
	if err != nil {
		t.Fatalf("NewWriter(path1): %v", err)
	}
	if _, err := w1.Append(0, nil, 4, bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Fatalf("Append(path1): %v", err)
	}
	if _, err := w1.AppendFrame(0, nil, []valuelog.Record{
		{RID: 6, Value: bytes.Repeat([]byte("b"), 64)},
		{RID: 15, Value: bytes.Repeat([]byte("c"), 64)},
	}); err != nil {
		t.Fatalf("AppendFrame(path1): %v", err)
	}
	closeNoErr(t, w1)
	f1, err := os.Open(path1)
	if err != nil {
		t.Fatalf("Open(path1): %v", err)
	}
	defer closeNoErr(t, f1)

	fileID2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("EncodeFileID(0,2): %v", err)
	}
	path2 := filepath.Join(dir, "value-l0-000002.log")
	w2, err := valuelog.NewWriter(path2, fileID2)
	if err != nil {
		t.Fatalf("NewWriter(path2): %v", err)
	}
	if _, err := w2.Append(0, nil, 22, bytes.Repeat([]byte("d"), 64)); err != nil {
		t.Fatalf("Append(path2): %v", err)
	}
	closeNoErr(t, w2)
	f2, err := os.Open(path2)
	if err != nil {
		t.Fatalf("Open(path2): %v", err)
	}
	defer closeNoErr(t, f2)

	segments := []logSegment{
		{path: path1, fileID: fileID1, valueLog: true},
		{path: path2, fileID: fileID2, valueLog: true},
	}
	wantStart, err := nextRewriteRIDStart(segments)
	if err != nil {
		t.Fatalf("nextRewriteRIDStart: %v", err)
	}

	set := &valuelog.Set{
		Files: map[uint32]*valuelog.File{
			fileID1: {ID: fileID1, Path: path1, File: f1},
			fileID2: {ID: fileID2, Path: path2, File: f2},
		},
	}
	gotStart, err := nextRewriteRIDStartFromSet(set)
	if err != nil {
		t.Fatalf("nextRewriteRIDStartFromSet: %v", err)
	}
	if gotStart != wantStart {
		t.Fatalf("start mismatch: from-set=%d from-segments=%d", gotStart, wantStart)
	}
}

func TestNextRewriteRIDStartFromSet_ErrorsOnCorruptMidFileRecord(t *testing.T) {
	dir := t.TempDir()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID(0,1): %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter(path): %v", err)
	}
	if _, err := w.Append(0, nil, 11, bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Fatalf("Append(first): %v", err)
	}

	const (
		crcStart     = 0
		versionOff   = 4
		flagsOff     = 5
		ridStart     = 8
		ridEnd       = 16
		bodyLenStart = 16
		bodyLenEnd   = 20
	)

	// Corrupt grouped record: grouped flag set but body too short for frame header.
	rawRecord := make([]byte, valuelog.HeaderSize+1)
	rawRecord[versionOff] = valuelog.Version
	rawRecord[flagsOff] = 1 << 0
	binary.LittleEndian.PutUint64(rawRecord[ridStart:ridEnd], 20)
	binary.LittleEndian.PutUint32(rawRecord[bodyLenStart:bodyLenEnd], 1)
	rawRecord[valuelog.HeaderSize] = 0xff
	binary.LittleEndian.PutUint32(rawRecord[crcStart:versionOff], crc.Checksum(rawRecord[versionOff:]))
	if _, err := w.AppendRawRecord(rawRecord, uint32(len(rawRecord)-versionOff)); err != nil {
		t.Fatalf("AppendRawRecord(corrupt grouped): %v", err)
	}
	if _, err := w.Append(0, nil, 200, bytes.Repeat([]byte("b"), 64)); err != nil {
		t.Fatalf("Append(last): %v", err)
	}
	closeNoErr(t, w)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(path): %v", err)
	}
	defer closeNoErr(t, f)

	set := &valuelog.Set{
		Files: map[uint32]*valuelog.File{
			fileID: {ID: fileID, Path: path, File: f},
		},
	}
	_, err = nextRewriteRIDStartFromSet(set)
	if !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("nextRewriteRIDStartFromSet error=%v want ErrCorrupt", err)
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
	if err := db.RegisterValueLogSegment(valueLogSegmentPath(t, dir, ptrs[0].FileID), ptrs[0].FileID); err != nil {
		t.Fatalf("register producer segment: %v", err)
	}
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

func TestUpdateValueLogHealthAfterRewrite_SetInitAgeFromFileMetadata(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptr := appendPointersInNewSegment(t, dir, 0, 1, 120_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	segPath := db.valueLogManager.SegmentPath(ptr.FileID)
	past := time.Now().Add(-3 * time.Second)
	if err := os.Chtimes(segPath, past, past); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	healthPath := valueLogHealthPath(dir)
	_ = os.Remove(healthPath)

	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		t.Fatalf("missing current value-log set")
	}
	defer func() { _ = db.valueLogManager.Release(set) }()
	oldIDs := map[uint32]struct{}{ptr.FileID: {}}
	if err := updateValueLogHealthAfterRewrite(dir, oldIDs, set); err != nil {
		t.Fatalf("updateValueLogHealthAfterRewrite: %v", err)
	}

	health, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("loadValueLogHealth: %v", err)
	}
	h, ok := health[ptr.FileID]
	if !ok {
		t.Fatalf("missing health entry for segment %d", ptr.FileID)
	}
	if h.LastUpdatedUnixNano == 0 {
		t.Fatalf("expected LastUpdatedUnixNano to be initialized")
	}
	if h.AgeSeconds <= 0 {
		t.Fatalf("expected AgeSeconds initialized from file metadata, got %d", h.AgeSeconds)
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

	segmentsBefore, err := listValueLogSegments(dir)
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

	segmentsAfter, err := listValueLogSegments(dir)
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
		filepath.Join(dir, "value_vlog", "value-l0-000002.log"),
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
		filepath.Join(dir, "value_vlog", "value-l250-000001.log"),
		filepath.Join(dir, "value_vlog", "value-l250-000002.log"),
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

func TestValueLogRewriteOnline_ExplicitSourceDoesNotDeleteActiveSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 200_000, 1, func(_ int) []byte {
		return bytes.Repeat([]byte("active-protected|"), 64)
	})
	targetPath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write pointer key: %v", err)
	}
	closeNoErr(t, b)

	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("delete pointer key: %v", err)
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs[0].FileID},
		BatchSize:     1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.SourceSegmentsUnreferenced != 1 {
		t.Fatalf("source segment unreferenced=%d want 1", stats.SourceSegmentsUnreferenced)
	}

	if _, err := os.Stat(targetPath); err != nil {
		t.Fatalf("expected active segment %s to remain, stat=%v", filepath.Base(targetPath), err)
	}
}

func TestValueLogRewriteOnline_ObservedSourceGCReclaimsActiveUnreferencedSource(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	want := bytes.Repeat([]byte("rewrite-active-source|"), 64)
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 210_000, 1, func(_ int) []byte {
		return want
	})
	sourceID := ptrs[0].FileID
	sourcePath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	if err := db.RegisterValueLogSegment(sourcePath, sourceID); err != nil {
		t.Fatalf("register producer segment: %v", err)
	}

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write pointer key: %v", err)
	}
	closeNoErr(t, b)

	rewriteStats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{sourceID},
		BatchSize:     1,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if !slices.Equal(rewriteStats.SourceFileIDsUnreferenced, []uint32{sourceID}) {
		t.Fatalf("unreferenced source ids=%v want [%d]", rewriteStats.SourceFileIDsUnreferenced, sourceID)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("rewrite should leave active source for explicit observed reclaim, stat=%v", err)
	}

	activeStats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            rewriteStats.SourceFileIDsUnreferenced,
		ObservedSourceAssumeUnreferenced: true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC without active reclaim: %v", err)
	}
	if activeStats.ObservedSourceSegmentsDeleted != 0 ||
		activeStats.ObservedSourceSegmentsReferenced+activeStats.ObservedSourceSegmentsActive != 1 {
		t.Fatalf("observed source should remain while active or recoverable: %+v", activeStats)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("active or recoverable source should remain without reclaim option, stat=%v", err)
	}

	advancePastRetainedDurableSlotForTest(t, db)
	reclaimStats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            rewriteStats.SourceFileIDsUnreferenced,
		ObservedSourceAssumeUnreferenced: true,
		ObservedSourceReclaimActive:      true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC with active reclaim: %v", err)
	}
	if reclaimStats.ObservedSourceSegmentsDeleted != 1 || reclaimStats.ObservedSourceBytesDeleted <= 0 {
		t.Fatalf("observed active source was not reclaimed: %+v", reclaimStats)
	}
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("expected active source to be removed after observed reclaim, err=%v", err)
	}
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after observed reclaim: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after observed reclaim")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close after observed reclaim: %v", err)
	}
	db = nil
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("expected active source to stay removed after close, err=%v", err)
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen after observed reclaim: %v", err)
	}
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("expected active source to stay removed after reopen, err=%v", err)
	}
	got, err = db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen observed reclaim: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen observed reclaim")
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

	segmentsBefore, err := listValueLogSegments(dir)
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

	segmentsAfter, err := listValueLogSegments(dir)
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

func TestValueLogRewriteOnline_GroupsSingleRecordJSONFrames(t *testing.T) {
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

	const rows = 64
	rawBytes := 0
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 1, rows, func(i int) []byte {
		doc := []byte(fmt.Sprintf(
			`{"did":"did:plc:%08d","time_us":1732206349%06d,"kind":"commit","commit":{"rev":"3lbhuvzds%04d","operation":"create","collection":"app.bsky.feed.post","rkey":"rkey%06d","record":{"$type":"app.bsky.feed.post","createdAt":"2024-11-21T16:%02d:%02d.095Z","langs":["en"],"text":"repeatable fixture text for value-log rewrite grouping number %06d"}}}`,
			i%17, i, i%10000, i, i%60, (i*7)%60, i,
		))
		rawBytes += len(doc)
		return doc
	})
	if len(ptrs) != rows {
		t.Fatalf("expected %d pointers, got %d", rows, len(ptrs))
	}

	b := db.NewBatch().(*Batch)
	for i, ptr := range ptrs {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := b.SetPointer(key, ptr); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	segmentsBefore, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	before := make(map[string]struct{}, len(segmentsBefore))
	for _, seg := range segmentsBefore {
		before[seg.path] = struct{}{}
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     rows,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != rows {
		t.Fatalf("copied records=%d want %d; stats=%+v", stats.RecordsCopied, rows, stats)
	}

	segmentsAfter, err := listValueLogSegments(dir)
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
	bodyLen := binary.LittleEndian.Uint32(header[16:20])
	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(f, body); err != nil {
		t.Fatalf("read frame body: %v", err)
	}
	frameHeader, _, offsets, _, err := valuelog.DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if frameHeader.DictID != 0 {
		t.Fatalf("expected block frame, got dictID=%d", frameHeader.DictID)
	}
	if frameHeader.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected rewritten frame to be compressed, header=%+v", frameHeader)
	}
	if frameHeader.K <= 1 {
		t.Fatalf("expected rewrite to group JSON records, got k=%d", frameHeader.K)
	}
	if got, want := frameHeader.Reserved, uint8(valuelog.BlockCodecSnappy); got != want {
		t.Fatalf("unexpected codec id %d, want %d", got, want)
	}
	storedPayload := int(bodyLen) - (valuelog.FrameHeaderSize + int(frameHeader.K)*8 + (int(frameHeader.K)+1)*4)
	groupRaw := int(offsets[frameHeader.K])
	if groupRaw <= 0 || groupRaw > rawBytes {
		t.Fatalf("unexpected grouped raw bytes %d, total fixture raw=%d", groupRaw, rawBytes)
	}
	if storedPayload*100 >= groupRaw*80 {
		t.Fatalf("expected grouped rewrite compression below 0.80 stored/raw, stored=%d raw=%d", storedPayload, groupRaw)
	}
}

func TestValueLogRewriteOnline_UsesCurrentSingleValueDictInAutoMode(t *testing.T) {
	dir := t.TempDir()

	const (
		rows   = 128
		dictID = uint64(734411)
	)
	values := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		values[i] = []byte(fmt.Sprintf(
			`{"did":"did:plc:%08d","time_us":1732206349%06d,"kind":"commit","commit":{"rev":"3lbhuvzds%04d","operation":"create","collection":"app.bsky.feed.post","rkey":"rkey%06d","record":{"$type":"app.bsky.feed.post","createdAt":"2024-11-21T16:%02d:%02d.095Z","langs":["en"],"text":"repeatable fixture text for value-log rewrite dictionary number %06d"}}}`,
			i%17, i, i%10000, i, i%60, (i*7)%60, i,
		))
	}
	history := append([]byte(nil), values[0]...)
	for i := 1; i < 8; i++ {
		history = append(history, values[i]...)
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: values,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionAuto,
			BlockCodec:  ValueLogBlockSnappy,
			DictLookup: func(id uint64) ([]byte, error) {
				if id != dictID {
					return nil, valuelog.ErrMissingDict
				}
				return dict, nil
			},
			DictCurrentForClass: func(_ context.Context, class string) (uint64, error) {
				if class != "single_value" {
					t.Fatalf("current dict class=%q want single_value", class)
				}
				return dictID, nil
			},
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

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 1, rows, func(i int) []byte {
		return values[i]
	})
	b := db.NewBatch().(*Batch)
	for i, ptr := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("k%04d", i)), ptr); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	segmentsBefore, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	before := make(map[string]struct{}, len(segmentsBefore))
	for _, seg := range segmentsBefore {
		before[seg.path] = struct{}{}
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     rows,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != rows {
		t.Fatalf("copied records=%d want %d; stats=%+v", stats.RecordsCopied, rows, stats)
	}

	segmentsAfter, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments after rewrite: %v", err)
	}
	dictFrames := 0
	blockFrames := 0
	maxDictK := 0
	for _, seg := range segmentsAfter {
		if !seg.valueLog {
			continue
		}
		if _, ok := before[seg.path]; ok {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			t.Fatalf("open new segment %s: %v", filepath.Base(seg.path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(seg.path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read frame body %s: %v", filepath.Base(seg.path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(seg.path), err)
				}
				if frameHeader.DictID == dictID {
					dictFrames++
					if int(frameHeader.K) > maxDictK {
						maxDictK = int(frameHeader.K)
					}
				}
				if frameHeader.DictID == 0 && frameHeader.Flags&valuelog.FrameFlagCompressed != 0 {
					blockFrames++
				}
			}
		}()
	}
	if dictFrames == 0 {
		t.Fatalf("expected online rewrite to use current single_value dict")
	}
	if maxDictK < 2 {
		t.Fatalf("expected online rewrite to batch dict frames, max dict k=%d", maxDictK)
	}
	if blockFrames != 0 {
		t.Fatalf("expected dict rewrite for all test payloads, block frames=%d", blockFrames)
	}
}

func TestValueLogRewriteOnline_ReportsActualBytesAfterSegmentGrowth(t *testing.T) {
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

	const rows = 4
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 1, rows, func(i int) []byte {
		return []byte(fmt.Sprintf(
			`{"kind":"commit","commit":{"collection":"app.bsky.feed.post","rkey":"%06d"},"payload":"%s"}`,
			i,
			strings.Repeat("jsonbench-value-log-rewrite-fixture-", 32),
		))
	})
	b := db.NewBatch().(*Batch)
	for i, ptr := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("k%04d", i)), ptr); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     1,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}

	postSet := db.valueLogManager.CurrentSetNoRefresh()
	if postSet == nil {
		t.Fatalf("missing post-rewrite value-log set")
	}
	defer func() { _ = db.valueLogManager.Release(postSet) }()

	var actualBytes int64
	var actualSegments int
	for _, f := range db.valueOnlyValueLogFiles(postSet.Files) {
		info, err := os.Stat(f.Path)
		if err != nil {
			t.Fatalf("stat %s: %v", f.Path, err)
		}
		actualSegments++
		actualBytes += info.Size()
	}
	if stats.SegmentsAfter != actualSegments || stats.BytesAfter != actualBytes {
		t.Fatalf("rewrite stats after=(segments=%d bytes=%d) want actual=(segments=%d bytes=%d)",
			stats.SegmentsAfter, stats.BytesAfter, actualSegments, actualBytes)
	}
}

func TestValueLogRewriteOffline_GroupsSingleRecordCompressedJSONFrames(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockSnappy,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

	const rows = 64
	ptrs := appendBlockCompressedPointersInNewSegment(t, dir, 0, 1, 1, rows, func(i int) []byte {
		return []byte(fmt.Sprintf(
			`{"did":"did:plc:%08d","kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"rkey%06d","record":{"$type":"app.bsky.feed.post","text":"%s"}}}`,
			i%17,
			i,
			strings.Repeat("offline rewrite grouped compression fixture ", 12),
		))
	})
	if len(ptrs) != rows {
		t.Fatalf("expected %d pointers, got %d", rows, len(ptrs))
	}

	b := db.NewBatch().(*Batch)
	for i, ptr := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("k%04d", i)), ptr); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	segmentsBefore, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	before := make(map[string]struct{}, len(segmentsBefore))
	for _, seg := range segmentsBefore {
		before[seg.path] = struct{}{}
	}

	closeNoErr(t, db)
	db = nil

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied != rows {
		t.Fatalf("copied records=%d want %d; stats=%+v", stats.RecordsCopied, rows, stats)
	}

	segmentsAfter, err := listValueLogSegments(dir)
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
		t.Fatalf("expected offline rewrite to create a new value-log segment")
	}
	frameHeader := readFirstFrameHeaderFromSegment(t, newSeg)
	if frameHeader.DictID != 0 {
		t.Fatalf("expected block frame, got dictID=%d", frameHeader.DictID)
	}
	if frameHeader.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected rewritten frame to be compressed, header=%+v", frameHeader)
	}
	if frameHeader.K <= 1 {
		t.Fatalf("expected offline rewrite to group compressed JSON records, got k=%d", frameHeader.K)
	}
}

func TestValueLogRewriteOffline_ReencodesSingleUncompressedFramesWhenCompressionEnabled(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockSnappy,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 1, 1, func(i int) []byte {
		// The helper writes dictID=0 records without block compression enabled.
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

	segmentsBefore, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments before rewrite: %v", err)
	}
	before := make(map[string]struct{}, len(segmentsBefore))
	for _, seg := range segmentsBefore {
		before[seg.path] = struct{}{}
	}

	closeNoErr(t, db)
	db = nil

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected copied records, got %+v", stats)
	}

	segmentsAfter, err := listValueLogSegments(dir)
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

func TestValueLogRewriteOffline_ReencodesLargeBlockFramesWithObservedDict(t *testing.T) {
	dir := t.TempDir()

	value1 := bytes.Repeat([]byte("celestia/state/value/alpha|"), 1700)
	value2 := bytes.Repeat([]byte("celestia/state/value/beta|"), 1700)
	value3 := bytes.Repeat([]byte("celestia/state/value/gamma|"), 1700)
	samples := [][]byte{value1, value2, value3}
	history := append([]byte(nil), value1[:8<<10]...)
	dictID := uint64(90321)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, valuelog.ErrMissingDict
		}
		return dict, nil
	}
	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockSnappy,
			DictLookup:  dictLookup,
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("file id: %v", err)
	}
	path := filepath.Join(walDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	w.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptrDict, err := w.Append(dictID, dict, 1, value1)
	if err != nil {
		t.Fatalf("append dict: %v", err)
	}
	ptrBlock, err := w.Append(0, nil, 2, value2)
	if err != nil {
		t.Fatalf("append block: %v", err)
	}
	ptrBlock2, err := w.Append(0, nil, 3, value3)
	if err != nil {
		t.Fatalf("append block2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrDict); err != nil {
		t.Fatalf("set pointer k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrBlock); err != nil {
		t.Fatalf("set pointer k2: %v", err)
	}
	if err := b.SetPointer([]byte("k3"), ptrBlock2); err != nil {
		t.Fatalf("set pointer k3: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)
	closeNoErr(t, db)

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected copied records, got %+v", stats)
	}

	dictFrames := 0
	blockFrames := 0
	maxDictK := 0
	segments, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(seg.path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(seg.path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read body %s: %v", filepath.Base(seg.path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(seg.path), err)
				}
				if frameHeader.DictID == dictID {
					dictFrames++
					if int(frameHeader.K) > maxDictK {
						maxDictK = int(frameHeader.K)
					}
				}
				if frameHeader.DictID == 0 && frameHeader.Flags&valuelog.FrameFlagCompressed != 0 {
					blockFrames++
				}
			}
		}()
	}
	if dictFrames == 0 {
		t.Fatalf("expected rewrite to increase dict coverage, dict frames=%d", dictFrames)
	}
	if maxDictK < 2 {
		t.Fatalf("expected rewrite to batch dict re-encodes, max dict k=%d", maxDictK)
	}
	if blockFrames != 0 {
		t.Fatalf("expected no remaining block-compressed frames for test payloads, block frames=%d", blockFrames)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, db2)
	got1, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if !bytes.Equal(got1, value1) {
		t.Fatalf("k1 mismatch after rewrite")
	}
	got2, err := db2.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get k2: %v", err)
	}
	if !bytes.Equal(got2, value2) {
		t.Fatalf("k2 mismatch after rewrite")
	}
	got3, err := db2.Get([]byte("k3"))
	if err != nil {
		t.Fatalf("Get k3: %v", err)
	}
	if !bytes.Equal(got3, value3) {
		t.Fatalf("k3 mismatch after rewrite")
	}
}

func TestValueLogRewriteOffline_ReencodesGroupedBlockFramesWithObservedDict(t *testing.T) {
	dir := t.TempDir()

	value1 := bytes.Repeat([]byte("celestia/state/value/alpha|"), 1700)
	value2 := bytes.Repeat([]byte("celestia/state/value/beta|"), 1700)
	value3 := bytes.Repeat([]byte("celestia/state/value/gamma|"), 1700)
	value4 := bytes.Repeat([]byte("celestia/state/value/delta|"), 1700)
	samples := [][]byte{value1, value2, value3, value4}
	history := append([]byte(nil), value1[:8<<10]...)
	dictID := uint64(90421)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, valuelog.ErrMissingDict
		}
		return dict, nil
	}
	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockSnappy,
			DictLookup:  dictLookup,
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("file id: %v", err)
	}
	path := filepath.Join(walDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	w.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptrDict, err := w.Append(dictID, dict, 1, value1)
	if err != nil {
		t.Fatalf("append dict: %v", err)
	}
	groupedRecords := []valuelog.Record{
		{RID: 2, Value: value2},
		{RID: 3, Value: value3},
		{RID: 4, Value: value4},
	}
	ptrsBlock, _, err := w.AppendFrameWithStats(0, nil, groupedRecords)
	if err != nil {
		t.Fatalf("append grouped block frame: %v", err)
	}
	if len(ptrsBlock) != len(groupedRecords) {
		t.Fatalf("grouped pointer count mismatch: got=%d want=%d", len(ptrsBlock), len(groupedRecords))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("z1"), ptrDict); err != nil {
		t.Fatalf("set pointer z1: %v", err)
	}
	// Put grouped block pointers on keys that sort before the dict-keyed entry.
	// This verifies rewrite can discover a preferred dict even when block frames
	// are visited first in key order.
	if err := b.SetPointer([]byte("a1"), ptrsBlock[0]); err != nil {
		t.Fatalf("set pointer a1: %v", err)
	}
	if err := b.SetPointer([]byte("a2"), ptrsBlock[1]); err != nil {
		t.Fatalf("set pointer a2: %v", err)
	}
	if err := b.SetPointer([]byte("a3"), ptrsBlock[2]); err != nil {
		t.Fatalf("set pointer a3: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)
	closeNoErr(t, db)

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected copied records, got %+v", stats)
	}

	dictFrames := 0
	blockFrames := 0
	maxDictK := 0
	segments, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(seg.path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(seg.path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read body %s: %v", filepath.Base(seg.path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(seg.path), err)
				}
				if frameHeader.DictID == dictID {
					dictFrames++
					if int(frameHeader.K) > maxDictK {
						maxDictK = int(frameHeader.K)
					}
				}
				if frameHeader.DictID == 0 && frameHeader.Flags&valuelog.FrameFlagCompressed != 0 {
					blockFrames++
				}
			}
		}()
	}
	if dictFrames == 0 {
		t.Fatalf("expected rewrite to produce dict frames, dictFrames=%d", dictFrames)
	}
	if maxDictK < 3 {
		t.Fatalf("expected grouped block re-encodes to batch under dict, maxDictK=%d", maxDictK)
	}
	if blockFrames != 0 {
		t.Fatalf("expected grouped block frames to be rewritten under dict, blockFrames=%d", blockFrames)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, db2)
	got1, err := db2.Get([]byte("z1"))
	if err != nil {
		t.Fatalf("Get z1: %v", err)
	}
	if !bytes.Equal(got1, value1) {
		t.Fatalf("z1 mismatch after rewrite")
	}
	got2, err := db2.Get([]byte("a1"))
	if err != nil {
		t.Fatalf("Get a1: %v", err)
	}
	if !bytes.Equal(got2, value2) {
		t.Fatalf("a1 mismatch after rewrite")
	}
	got3, err := db2.Get([]byte("a2"))
	if err != nil {
		t.Fatalf("Get a2: %v", err)
	}
	if !bytes.Equal(got3, value3) {
		t.Fatalf("a2 mismatch after rewrite")
	}
	got4, err := db2.Get([]byte("a3"))
	if err != nil {
		t.Fatalf("Get a3: %v", err)
	}
	if !bytes.Equal(got4, value4) {
		t.Fatalf("a3 mismatch after rewrite")
	}
}

func TestScanValueLogSegmentPreferredDictID_ToleratesTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	value := bytes.Repeat([]byte("rewrite/truncated-tail"), 512)
	if _, err := w.Append(0, nil, 1, value); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// Append a partial trailing frame body to simulate a torn tail record.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append: %v", err)
	}
	tail := make([]byte, valuelog.HeaderSize+valuelog.FrameHeaderSize)
	binary.LittleEndian.PutUint32(tail[16:20], uint32(valuelog.FrameHeaderSize+128))
	if _, err := f.Write(tail); err != nil {
		_ = f.Close()
		t.Fatalf("append tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close append file: %v", err)
	}

	readFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open read file: %v", err)
	}
	defer closeNoErr(t, readFile)

	seg := &valuelog.File{File: readFile}
	dictID, err := scanValueLogSegmentPreferredDictID(seg)
	if err != nil {
		t.Fatalf("scanValueLogSegmentPreferredDictID: %v", err)
	}
	if dictID != 0 {
		t.Fatalf("expected no preferred dict ID, got %d", dictID)
	}
}

func TestScanValueLogSegmentPreferredDictID_SkipsNonGroupedRecords(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	rawPayload := make([]byte, valuelog.FrameHeaderSize+16)
	binary.LittleEndian.PutUint64(rawPayload[4:12], uint64(0xdeadbeefcafebabe))
	rawRecord := make([]byte, valuelog.HeaderSize+len(rawPayload))
	rawRecord[4] = valuelog.Version
	binary.LittleEndian.PutUint64(rawRecord[8:16], 1)
	binary.LittleEndian.PutUint32(rawRecord[16:20], uint32(len(rawPayload)))
	copy(rawRecord[valuelog.HeaderSize:], rawPayload)
	binary.LittleEndian.PutUint32(rawRecord[0:4], crc.Checksum(rawRecord[4:]))
	if _, err := w.AppendRawRecord(rawRecord, uint32(len(rawRecord)-4)); err != nil {
		t.Fatalf("AppendRawRecord: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	readFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open read file: %v", err)
	}
	defer closeNoErr(t, readFile)

	seg := &valuelog.File{File: readFile}
	dictID, err := scanValueLogSegmentPreferredDictID(seg)
	if err != nil {
		t.Fatalf("scanValueLogSegmentPreferredDictID: %v", err)
	}
	if dictID != 0 {
		t.Fatalf("expected no preferred dict ID from non-grouped record, got %d", dictID)
	}
}

func TestScanValueLogSegmentPreferredDictID_IgnoresInvalidRecordVersion(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	payload := make([]byte, valuelog.FrameHeaderSize)
	payload[0] = valuelog.FrameVersion
	binary.LittleEndian.PutUint64(payload[4:12], 0xfeedface)

	rawRecord := make([]byte, valuelog.HeaderSize+len(payload))
	rawRecord[4] = 0 // invalid/unexpected record version
	rawRecord[5] = 1 // grouped flag set
	binary.LittleEndian.PutUint64(rawRecord[8:16], 1)
	binary.LittleEndian.PutUint32(rawRecord[16:20], uint32(len(payload)))
	copy(rawRecord[valuelog.HeaderSize:], payload)
	binary.LittleEndian.PutUint32(rawRecord[0:4], crc.Checksum(rawRecord[4:]))
	if _, err := w.AppendRawRecord(rawRecord, uint32(len(rawRecord)-4)); err != nil {
		t.Fatalf("AppendRawRecord: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	readFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open read file: %v", err)
	}
	defer closeNoErr(t, readFile)

	seg := &valuelog.File{File: readFile}
	dictID, err := scanValueLogSegmentPreferredDictID(seg)
	if err != nil {
		t.Fatalf("scanValueLogSegmentPreferredDictID: %v", err)
	}
	if dictID != 0 {
		t.Fatalf("expected no preferred dict ID from invalid record version, got %d", dictID)
	}
}

func TestRewriteIteratorPreferredDictID_FallsBackWhenScannedDictUnavailable(t *testing.T) {
	dir := t.TempDir()
	fileID := uint32(1)
	path := filepath.Join(dir, "value-l0-000001.log")

	// Write a minimal grouped frame record with a non-zero dictID so the
	// preferred-dict segment scan discovers it.
	rawRecord := make([]byte, valuelog.HeaderSize+valuelog.FrameHeaderSize)
	rawRecord[4] = valuelog.Version
	rawRecord[5] = 1 << 0 // grouped record flag
	binary.LittleEndian.PutUint32(rawRecord[16:20], uint32(valuelog.FrameHeaderSize))
	rawRecord[valuelog.HeaderSize] = valuelog.FrameVersion
	scannedDictID := uint64(12345)
	binary.LittleEndian.PutUint64(rawRecord[valuelog.HeaderSize+4:valuelog.HeaderSize+12], scannedDictID)
	if err := os.WriteFile(path, rawRecord, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	readFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open read file: %v", err)
	}
	defer closeNoErr(t, readFile)

	globalDictID := uint64(777)
	it := &rewriteIterator{
		vlogs: &valuelog.Set{
			Files: map[uint32]*valuelog.File{
				fileID: &valuelog.File{File: readFile},
			},
		},
		preferredDictGlobal: globalDictID,
		dictLookup: func(id uint64) ([]byte, error) {
			if id == globalDictID {
				return []byte{1, 2, 3, 4}, nil
			}
			return nil, valuelog.ErrMissingDict
		},
	}

	got, err := it.preferredDictID(fileID)
	if err != nil {
		t.Fatalf("preferredDictID first call: %v", err)
	}
	if got != globalDictID {
		t.Fatalf("preferredDictID first call=%d want=%d", got, globalDictID)
	}
	if it.preferredDictByFile[fileID] != 0 {
		t.Fatalf("preferredDictByFile[%d]=%d want 0 for unresolved scanned dict", fileID, it.preferredDictByFile[fileID])
	}
	if cached, ok := it.dictCache[scannedDictID]; !ok || cached.ok {
		t.Fatalf("expected unresolved scanned dictID %d to be cached as miss", scannedDictID)
	}

	got, err = it.preferredDictID(fileID)
	if err != nil {
		t.Fatalf("preferredDictID second call: %v", err)
	}
	if got != globalDictID {
		t.Fatalf("preferredDictID second call=%d want=%d", got, globalDictID)
	}
}

func TestValueLogRewriteOffline_ReencodesGroupedBlockOuterLeafPagesWithObservedDict(t *testing.T) {
	dir := t.TempDir()

	outerLeafPage := func(tag byte) []byte {
		v := make([]byte, page.PageSize)
		copy(v, bytes.Repeat([]byte("outerleaf/rewrite/dict/"), 256))
		for i := 0; i < len(v); i += 257 {
			v[i] = tag
		}
		return v
	}
	value1 := outerLeafPage('a')
	value2 := outerLeafPage('b')
	value3 := outerLeafPage('c')
	value4 := outerLeafPage('d')
	samples := [][]byte{value1, value2, value3, value4}
	history := append([]byte(nil), value1...)
	dictID := uint64(90422)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	dictLookup := func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, valuelog.ErrMissingDict
		}
		return dict, nil
	}
	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockSnappy,
			DictLookup:  dictLookup,
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("file id: %v", err)
	}
	path := filepath.Join(walDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	w.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptrDict, err := w.Append(dictID, dict, 1, value1)
	if err != nil {
		t.Fatalf("append dict: %v", err)
	}
	groupedRecords := []valuelog.Record{
		{RID: 2, Value: value2},
		{RID: 3, Value: value3},
		{RID: 4, Value: value4},
	}
	ptrsBlock, _, err := w.AppendFrameWithStats(0, nil, groupedRecords)
	if err != nil {
		t.Fatalf("append grouped block frame: %v", err)
	}
	if len(ptrsBlock) != len(groupedRecords) {
		t.Fatalf("grouped pointer count mismatch: got=%d want=%d", len(ptrsBlock), len(groupedRecords))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrDict); err != nil {
		t.Fatalf("set pointer k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrsBlock[0]); err != nil {
		t.Fatalf("set pointer k2: %v", err)
	}
	if err := b.SetPointer([]byte("k3"), ptrsBlock[1]); err != nil {
		t.Fatalf("set pointer k3: %v", err)
	}
	if err := b.SetPointer([]byte("k4"), ptrsBlock[2]); err != nil {
		t.Fatalf("set pointer k4: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)
	closeNoErr(t, db)

	stats, err := ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected copied records, got %+v", stats)
	}

	dictFrames := 0
	blockFrames := 0
	maxDictK := 0
	segments, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(seg.path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(seg.path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read body %s: %v", filepath.Base(seg.path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(seg.path), err)
				}
				if frameHeader.DictID == dictID {
					dictFrames++
					if int(frameHeader.K) > maxDictK {
						maxDictK = int(frameHeader.K)
					}
				}
				if frameHeader.DictID == 0 && frameHeader.Flags&valuelog.FrameFlagCompressed != 0 {
					blockFrames++
				}
			}
		}()
	}
	if dictFrames == 0 {
		t.Fatalf("expected rewrite to produce dict frames, dictFrames=%d", dictFrames)
	}
	if maxDictK < 3 {
		t.Fatalf("expected grouped 4KiB block frames to batch under dict, maxDictK=%d", maxDictK)
	}
	if blockFrames != 0 {
		t.Fatalf("expected grouped 4KiB block frames to be rewritten under dict, blockFrames=%d", blockFrames)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, db2)
	got1, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if !bytes.Equal(got1, value1) {
		t.Fatalf("k1 mismatch after rewrite")
	}
	got2, err := db2.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get k2: %v", err)
	}
	if !bytes.Equal(got2, value2) {
		t.Fatalf("k2 mismatch after rewrite")
	}
	got3, err := db2.Get([]byte("k3"))
	if err != nil {
		t.Fatalf("Get k3: %v", err)
	}
	if !bytes.Equal(got3, value3) {
		t.Fatalf("k3 mismatch after rewrite")
	}
	got4, err := db2.Get([]byte("k4"))
	if err != nil {
		t.Fatalf("Get k4: %v", err)
	}
	if !bytes.Equal(got4, value4) {
		t.Fatalf("k4 mismatch after rewrite")
	}
}

type rewriteTemplateStore struct {
	templateID uint64
	defBytes   []byte
}

func TestValueLogRewriteOfflineRejectsTemplateActivationWithoutStableRootAuthority(t *testing.T) {
	_, err := ValueLogRewriteOffline(Options{
		Dir: t.TempDir(),
		ValueLog: ValueLogOptions{
			TemplateMode: templ.TemplateOnly,
		},
	})
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("offline template rewrite error=%v want unresolved stable root authority", err)
	}
}

func (s rewriteTemplateStore) GetCandidates(context.Context, uint64, int) ([]templ.Candidate, error) {
	if s.templateID == 0 || len(s.defBytes) == 0 {
		return nil, nil
	}
	return []templ.Candidate{{ID: s.templateID, Size: len(s.defBytes)}}, nil
}

func (s rewriteTemplateStore) GetTemplateDef(_ context.Context, templateID uint64) ([]byte, error) {
	if templateID == 0 || templateID != s.templateID {
		return nil, templ.ErrMissingTemplate
	}
	return append([]byte(nil), s.defBytes...), nil
}

func (s rewriteTemplateStore) PutTemplateDef(_ context.Context, defBytes []byte, _ []uint64) (uint64, error) {
	return templ.TemplateID(defBytes, 0), nil
}

func readFirstRewriteFrameHeader(t *testing.T, walDir string) valuelog.FrameHeader {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(walDir, "value-l0-*.log"))
	if err != nil {
		t.Fatalf("glob value-log files: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("expected at least one value-log file in %s", walDir)
	}
	slices.Sort(paths)
	return readFirstFrameHeaderFromSegment(t, paths[0])
}

func readFirstFrameHeaderFromSegment(t *testing.T, path string) valuelog.FrameHeader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", filepath.Base(path), err)
	}
	defer f.Close()
	var header [valuelog.HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		t.Fatalf("read record header: %v", err)
	}
	bodyLen := binary.LittleEndian.Uint32(header[16:20])
	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(f, body); err != nil {
		t.Fatalf("read frame body: %v", err)
	}
	frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	return frameHeader
}

func appendBlockCompressedPointersInNewSegment(t *testing.T, dir string, lane, seq uint32, ridBase uint64, n int, valueAt func(i int) []byte) []page.ValuePtr {
	t.Helper()
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("encode file id lane=%d seq=%d: %v", lane, seq, err)
	}
	path := filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	w.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptrs := make([]page.ValuePtr, 0, n)
	for i := 0; i < n; i++ {
		ptr, err := w.Append(0, nil, ridBase+uint64(i), valueAt(i))
		if err != nil {
			t.Fatalf("append rid=%d: %v", ridBase+uint64(i), err)
		}
		ptrs = append(ptrs, ptr)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, path, fileID)
	return ptrs
}

func readFirstRewriteFrameHeaderForLane(t *testing.T, walDir string, lane uint32) valuelog.FrameHeader {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(walDir, fmt.Sprintf("value-l%d-*.log", lane)))
	if err != nil {
		t.Fatalf("glob value-log files: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("expected at least one lane-%d value-log file in %s", lane, walDir)
	}
	slices.Sort(paths)
	f, err := os.Open(paths[0])
	if err != nil {
		t.Fatalf("open %s: %v", filepath.Base(paths[0]), err)
	}
	defer f.Close()
	var header [valuelog.HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		t.Fatalf("read record header: %v", err)
	}
	bodyLen := binary.LittleEndian.Uint32(header[16:20])
	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(f, body); err != nil {
		t.Fatalf("read frame body: %v", err)
	}
	frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	return frameHeader
}

func buildRewriteLeafPageFixture(t *testing.T, seed string) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("%s-key-%03d", seed, i))
		value := []byte(fmt.Sprintf("%s-value-%03d-%s", seed, i, strings.Repeat(seed, 8)))
		if err := b.AddLeafEntry(key, value, node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

type rewriteTestLeafPageLog struct {
	db      *DB
	dir     string
	w       *valuelog.Writer
	path    string
	fileID  uint32
	nextRID uint64
}

func (l *rewriteTestLeafPageLog) ensureWriter() error {
	if l.w != nil {
		return nil
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(l.dir, "leaf_vlog", fmt.Sprintf("value-l%d-%06d.log", rewriteLeafLogLaneID, 1))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		return err
	}
	if l.db != nil {
		if err := l.db.RegisterValueLogSegment(path, fileID); err != nil {
			_ = w.Close()
			return err
		}
	}
	l.w = w
	l.path = path
	l.fileID = fileID
	return nil
}

func (l *rewriteTestLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if err := l.ensureWriter(); err != nil {
		return page.LeafLogPtr{}, err
	}
	l.nextRID++
	ptr, err := l.w.Append(0, nil, l.nextRID, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return page.LeafLogPtrFromValuePtr(ptr)
}

func (l *rewriteTestLeafPageLog) Flush() error {
	if err := l.ensureWriter(); err != nil {
		return err
	}
	return l.w.Flush()
}

func (l *rewriteTestLeafPageLog) Sync() error {
	if err := l.ensureWriter(); err != nil {
		return err
	}
	return l.w.Sync()
}

func (l *rewriteTestLeafPageLog) Close() error {
	if l.w == nil {
		return nil
	}
	err := l.w.Close()
	l.w = nil
	return err
}

func (l *rewriteTestLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || l.path == "" || l.fileID == 0 {
		return "", 0, false
	}
	return l.path, l.fileID, true
}

func buildRewriteTemplateFixture(t *testing.T) (templ.Config, rewriteTemplateStore, func(uint64) ([]byte, error), []byte) {
	t.Helper()
	def := templ.TemplateDef{
		Kind: templ.TemplateAnchors,
		Anchors: [][]byte{
			[]byte(`{"type":"account","id":"`),
			[]byte(`","status":"bonded","chain":"celestia"}`),
		},
	}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	templateID := templ.TemplateID(defBytes, 0)
	store := rewriteTemplateStore{templateID: templateID, defBytes: defBytes}
	lookup := func(id uint64) ([]byte, error) {
		if id != templateID {
			return nil, valuelog.ErrMissingTemplate
		}
		return append([]byte(nil), defBytes...), nil
	}
	cfg := templ.Config{
		MinSavingsBytes:    1,
		FingerprintK:       8,
		FingerprintW:       8,
		MaxFingerprints:    16,
		MaxFPReads:         16,
		MaxCandidatesPerFP: 8,
		MaxTemplateFetch:   8,
	}
	value := []byte(`{"type":"account","id":"acct-000001","status":"bonded","chain":"celestia"}`)
	return cfg, store, lookup, value
}

func TestRewriteWriter_CreatedFileIDs_StableAcrossCalls(t *testing.T) {
	const (
		lane     = uint32(2)
		startSeq = uint32(7)
		maxSize  = int64(512)
	)

	w := newRewriteWriter(t.TempDir(), lane, startSeq, maxSize)
	value := bytes.Repeat([]byte("rewrite-created-ids|"), 24)

	for i := 0; i < 3; i++ {
		if _, err := w.appendValue(uint64(i+1), value); err != nil {
			t.Fatalf("appendValue(%d): %v", i+1, err)
		}
	}

	ids1, err := w.createdFileIDs()
	if err != nil {
		t.Fatalf("createdFileIDs first: %v", err)
	}
	if len(ids1) != 3 {
		t.Fatalf("expected 3 created IDs, got %d", len(ids1))
	}
	if cap(ids1) != len(ids1) {
		t.Fatalf("expected append-safe created ID view (cap=len), len=%d cap=%d", len(ids1), cap(ids1))
	}
	for i := range ids1 {
		want, err := valuelog.EncodeFileID(lane, startSeq+1+uint32(i))
		if err != nil {
			t.Fatalf("EncodeFileID(%d): %v", i, err)
		}
		if ids1[i] != want {
			t.Fatalf("created ID[%d] mismatch: got=%d want=%d", i, ids1[i], want)
		}
	}

	ids2, err := w.createdFileIDs()
	if err != nil {
		t.Fatalf("createdFileIDs second: %v", err)
	}
	if !reflect.DeepEqual(ids2, ids1) {
		t.Fatalf("created IDs changed across calls: first=%v second=%v", ids1, ids2)
	}

	_ = append(ids1, 999)
	ids3, err := w.createdFileIDs()
	if err != nil {
		t.Fatalf("createdFileIDs after append copy: %v", err)
	}
	if !reflect.DeepEqual(ids3, ids2) {
		t.Fatalf("created IDs changed after caller append: before=%v after=%v", ids2, ids3)
	}

	if _, err := w.appendValue(4, value); err != nil {
		t.Fatalf("appendValue(4): %v", err)
	}
	ids4, err := w.createdFileIDs()
	if err != nil {
		t.Fatalf("createdFileIDs after additional append: %v", err)
	}
	if len(ids4) != 4 {
		t.Fatalf("expected created IDs to grow to 4, got %d", len(ids4))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRewriteWriter_LeafPagesUseConfiguredLeafLogDir(t *testing.T) {
	root := t.TempDir()
	valueDir := filepath.Join(root, "value_vlog")
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("mkdir value dir: %v", err)
	}
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf dir: %v", err)
	}

	w := newRewriteWriter(valueDir, 254, 0, 64<<20)
	w.ConfigureLeafLog(leafDir, rewriteLeafLogLaneID, 0)
	if _, err := w.appendValue(1, []byte("value-payload")); err != nil {
		t.Fatalf("appendValue: %v", err)
	}
	leafBuf := make([]byte, page.PageSize)
	leafBuilder := node.NewBuilderWithOptions(leafBuf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		if err := leafBuilder.AddLeafEntry([]byte("rewrite-key-"+string(rune('a'+i))), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	leafBuilder.FinishNoNode()
	leafPtr, err := w.AppendLeafPage(leafBuf)
	if err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if got := w.LastLeafPageRecordLength(); got == 0 || int(got) >= valuelog.HeaderSize+page.PageSize {
		t.Fatalf("LastLeafPageRecordLength=%d want compact leaf payload smaller than raw %d", got, valuelog.HeaderSize+page.PageSize)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	valuePaths, err := filepath.Glob(filepath.Join(valueDir, "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value_vlog: %v", err)
	}
	if len(valuePaths) == 0 {
		t.Fatalf("expected value_vlog file after append")
	}
	leafPaths, err := filepath.Glob(filepath.Join(leafDir, "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog: %v", err)
	}
	if len(leafPaths) == 0 {
		t.Fatalf("expected leaf_vlog file after leaf append")
	}

	lane, _ := valuelog.DecodeFileID(leafPtr.FileID)
	if lane != rewriteLeafLogLaneID {
		t.Fatalf("leaf ptr lane=%d want=%d", lane, rewriteLeafLogLaneID)
	}

	ids, err := w.createdFileIDs()
	if err != nil {
		t.Fatalf("createdFileIDs: %v", err)
	}
	var sawValue, sawLeaf bool
	for _, id := range ids {
		lane, _ := valuelog.DecodeFileID(id)
		if lane == 254 {
			sawValue = true
		}
		if lane == rewriteLeafLogLaneID {
			sawLeaf = true
		}
	}
	if !sawValue || !sawLeaf {
		t.Fatalf("expected created IDs to include value and leaf lanes, ids=%v", ids)
	}
	createdSegments, err := w.createdSegmentsSnapshot()
	if err != nil {
		t.Fatalf("createdSegmentsSnapshot: %v", err)
	}
	if len(createdSegments) != 2 {
		t.Fatalf("expected 2 created segments, got %d", len(createdSegments))
	}
	var sawValuePath, sawLeafPath bool
	for _, seg := range createdSegments {
		lane, _ := valuelog.DecodeFileID(seg.fileID)
		switch lane {
		case 254:
			if !strings.HasPrefix(seg.path, valueDir+string(os.PathSeparator)) {
				t.Fatalf("value segment path=%q want prefix %q", seg.path, valueDir)
			}
			sawValuePath = true
		case rewriteLeafLogLaneID:
			if !strings.HasPrefix(seg.path, leafDir+string(os.PathSeparator)) {
				t.Fatalf("leaf segment path=%q want prefix %q", seg.path, leafDir)
			}
			sawLeafPath = true
		}
	}
	if !sawValuePath || !sawLeafPath {
		t.Fatalf("expected created segments to preserve value and leaf paths, got=%v", createdSegments)
	}
}

func TestLeafPageBlockCodecFromOptions_AutoBalancedPrefersLZ4(t *testing.T) {
	got := leafPageBlockCodecFromOptions(ValueLogCompressionAuto, ValueLogAutoBalanced, ValueLogBlockSnappy, true)
	if got != valuelog.BlockCodecLZ4 {
		t.Fatalf("leafPageBlockCodecFromOptions auto/balanced=%v want lz4", got)
	}
}

func TestLeafPageBlockCodecFromOptions_ThroughputKeepsConfiguredCodec(t *testing.T) {
	got := leafPageBlockCodecFromOptions(ValueLogCompressionAuto, ValueLogAutoThroughput, ValueLogBlockSnappy, true)
	if got != valuelog.BlockCodecSnappy {
		t.Fatalf("leafPageBlockCodecFromOptions auto/throughput=%v want snappy", got)
	}
}

func TestRewriteWriter_TemplatePrepassEncodesBeforeDict(t *testing.T) {
	walDir := t.TempDir()
	cfg, store, lookup, value := buildRewriteTemplateFixture(t)

	dictID := uint64(95101)
	sampleA := bytes.Repeat(value, 64)
	sampleB := bytes.Repeat([]byte(`{"type":"account","id":"acct-000002","status":"bonded","chain":"celestia"}`), 64)
	sampleC := bytes.Repeat([]byte(`{"type":"account","id":"acct-000003","status":"bonded","chain":"celestia"}`), 64)
	history := append([]byte(nil), sampleA...)
	if len(history) > 8<<10 {
		history = history[:8<<10]
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{sampleA, sampleB, sampleC},
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	w := newRewriteWriter(walDir, 0, 0, 64<<20)
	w.SetTemplateCompression(templ.TemplatePrepass, cfg, store)
	ptr, err := w.appendValueWithDict(dictID, dict, 1, value)
	if err != nil {
		t.Fatalf("appendValueWithDict: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close rewrite writer: %v", err)
	}
	if w.templateAttempts == 0 {
		t.Fatalf("expected template prepass attempts > 0")
	}
	if w.templateKept == 0 {
		t.Fatalf("expected template prepass keeps > 0")
	}
	if w.templateOutBytes >= w.templateInBytes {
		t.Fatalf("expected template prepass to reduce bytes: in=%d out=%d", w.templateInBytes, w.templateOutBytes)
	}

	frameHeader := readFirstRewriteFrameHeader(t, walDir)
	if frameHeader.DictID != dictID {
		t.Fatalf("expected dict rewrite to remain active under prepass, got dictID=%d", frameHeader.DictID)
	}

	m, err := valuelog.NewManager(walDir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer m.Close()
	m.SetDictLookup(func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, valuelog.ErrMissingDict
		}
		return dict, nil
	})
	gotEncoded, err := m.Read(ptr)
	if err != nil {
		t.Fatalf("Read encoded: %v", err)
	}
	if !templ.IsEncodedPayload(gotEncoded) {
		t.Fatalf("expected template-encoded payload before decode, got=%q", gotEncoded)
	}

	m.SetTemplateLookup(lookup, templ.DecodeOptions{})
	gotDecoded, err := m.Read(ptr)
	if err != nil {
		t.Fatalf("Read decoded: %v", err)
	}
	if !bytes.Equal(gotDecoded, value) {
		t.Fatalf("decoded payload mismatch: got=%q want=%q", gotDecoded, value)
	}
}

func TestRewriteWriter_TemplateOnlyDisablesDictCompression(t *testing.T) {
	walDir := t.TempDir()
	cfg, store, lookup, value := buildRewriteTemplateFixture(t)

	dictID := uint64(95102)
	sampleA := bytes.Repeat(value, 64)
	sampleB := bytes.Repeat([]byte(`{"type":"account","id":"acct-000004","status":"bonded","chain":"celestia"}`), 64)
	history := append([]byte(nil), sampleA...)
	if len(history) > 8<<10 {
		history = history[:8<<10]
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{sampleA, sampleB},
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	w := newRewriteWriter(walDir, 0, 0, 64<<20)
	w.SetTemplateCompression(templ.TemplateOnly, cfg, store)
	ptr, err := w.appendValueWithDict(dictID, dict, 1, value)
	if err != nil {
		t.Fatalf("appendValueWithDict: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close rewrite writer: %v", err)
	}
	if w.templateAttempts == 0 {
		t.Fatalf("expected template-only attempts > 0")
	}
	if w.templateKept == 0 {
		t.Fatalf("expected template-only keeps > 0")
	}

	frameHeader := readFirstRewriteFrameHeader(t, walDir)
	if frameHeader.DictID != 0 {
		t.Fatalf("expected template-only mode to bypass dict compression, got dictID=%d", frameHeader.DictID)
	}

	m, err := valuelog.NewManager(walDir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer m.Close()
	gotEncoded, err := m.Read(ptr)
	if err != nil {
		t.Fatalf("Read encoded: %v", err)
	}
	if !templ.IsEncodedPayload(gotEncoded) {
		t.Fatalf("expected template-encoded payload, got=%q", gotEncoded)
	}

	m.SetTemplateLookup(lookup, templ.DecodeOptions{})
	gotDecoded, err := m.Read(ptr)
	if err != nil {
		t.Fatalf("Read decoded: %v", err)
	}
	if !bytes.Equal(gotDecoded, value) {
		t.Fatalf("decoded payload mismatch: got=%q want=%q", gotDecoded, value)
	}
}

func TestRewriteWriter_DictBatchMaxSizeGuardAppliesAtOffsetZero(t *testing.T) {
	walDir := t.TempDir()

	valueA := bytes.Repeat([]byte("offset-zero-guard/value-a|"), 512)
	valueB := bytes.Repeat([]byte("offset-zero-guard/value-b|"), 512)
	dictID := uint64(94021)
	history := append([]byte(nil), valueA...)
	if len(history) > 8<<10 {
		history = history[:8<<10]
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{valueA, valueB},
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	singleEstimate := rewriteDictFrameRecordLen(len(valueA), 1)
	doubleEstimate := rewriteDictFrameRecordLen(len(valueA)+len(valueB), 2)
	maxSize := singleEstimate + 64
	if doubleEstimate <= maxSize {
		t.Fatalf("invalid test setup: doubleEstimate=%d maxSize=%d", doubleEstimate, maxSize)
	}

	w := newRewriteWriter(walDir, 0, 0, maxSize)
	if _, err := w.appendValueWithDict(dictID, dict, 1, valueA); err != nil {
		t.Fatalf("append first dict value: %v", err)
	}
	if _, err := w.appendValueWithDict(dictID, dict, 2, valueB); err != nil {
		t.Fatalf("append second dict value: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close rewrite writer: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join(walDir, "value-l0-*.log"))
	if err != nil {
		t.Fatalf("glob segments: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("expected rewrite writer to create at least one value-log segment")
	}

	frames := 0
	maxK := 0
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read body %s: %v", filepath.Base(path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(path), err)
				}
				frames++
				if int(frameHeader.K) > maxK {
					maxK = int(frameHeader.K)
				}
			}
		}()
	}
	if frames == 0 {
		t.Fatalf("expected at least one frame in rewritten output")
	}
	if maxK > 1 {
		t.Fatalf("expected offset-zero max-size guard to split grouped dict frame, maxK=%d", maxK)
	}
}

func TestRewriteWriter_AppendLeafPageUsesLeafDictWhenConfigured(t *testing.T) {
	walDir := t.TempDir()

	leafPage := func(tag byte) []byte {
		v := make([]byte, page.PageSize)
		copy(v, bytes.Repeat([]byte("rewrite/leaf/page/dict/"), 256))
		for i := 0; i < len(v); i += 257 {
			v[i] = tag
		}
		return v
	}
	valueA := leafPage('a')
	valueB := leafPage('b')
	valueC := leafPage('c')
	dictID := uint64(94022)
	history := append([]byte(nil), valueA...)
	if len(history) > 8<<10 {
		history = history[:8<<10]
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{valueA, valueB, valueC},
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("BuildDict returned empty dict")
	}

	w := newRewriteWriter(walDir, 0, 0, defaultValueLogRewriteSegmentBytes)
	w.blockCompression = true
	w.blockCodec = valuelog.BlockCodecSnappy
	w.SetKeepPolicy(0, 0, 0)
	w.SetLeafDict(dictID, dict)
	ptrA, err := w.AppendLeafPage(valueA)
	if err != nil {
		t.Fatalf("append leaf A: %v", err)
	}
	ptrB, err := w.AppendLeafPage(valueB)
	if err != nil {
		t.Fatalf("append leaf B: %v", err)
	}
	ptrC, err := w.AppendLeafPage(valueC)
	if err != nil {
		t.Fatalf("append leaf C: %v", err)
	}
	if page.ValuePtrSubIndex(ptrA.ValuePtr()) != 0 || page.ValuePtrSubIndex(ptrB.ValuePtr()) != 0 || page.ValuePtrSubIndex(ptrC.ValuePtr()) != 0 {
		t.Fatalf("leaf pointers must keep grouped sub-index 0 for leafref encoding: A=%d B=%d C=%d",
			page.ValuePtrSubIndex(ptrA.ValuePtr()), page.ValuePtrSubIndex(ptrB.ValuePtr()), page.ValuePtrSubIndex(ptrC.ValuePtr()))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close rewrite writer: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join(walDir, "value-l0-*.log"))
	if err != nil {
		t.Fatalf("glob segments: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("expected rewrite writer to create value-log segments")
	}

	dictFrames := 0
	blockFrames := 0
	maxDictK := 0
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read body %s: %v", filepath.Base(path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(path), err)
				}
				if frameHeader.DictID == dictID {
					dictFrames++
					if int(frameHeader.K) > maxDictK {
						maxDictK = int(frameHeader.K)
					}
				}
				if frameHeader.DictID == 0 && frameHeader.Flags&valuelog.FrameFlagCompressed != 0 {
					blockFrames++
				}
			}
		}()
	}
	if dictFrames == 0 {
		t.Fatalf("expected leaf pages to be written with dict frames")
	}
	if maxDictK != 1 {
		t.Fatalf("expected leaf dict writes to remain single-record (k=1), maxDictK=%d", maxDictK)
	}
	if blockFrames != 0 {
		t.Fatalf("expected no block-compressed frames when leaf dict is configured, blockFrames=%d", blockFrames)
	}
}

func TestRewriteWriter_AppendLeafPageLane0KeepsRawLeafPayload(t *testing.T) {
	walDir := t.TempDir()
	w := newRewriteWriter(walDir, 0, 0, 64<<20)
	w.blockCompression = false

	leafBuf := make([]byte, page.PageSize)
	leafBuilder := node.NewBuilderWithOptions(leafBuf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		if err := leafBuilder.AddLeafEntry([]byte("lane0-leaf-"+string(rune('a'+i))), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	leafBuilder.FinishNoNode()
	leafPtr, err := w.AppendLeafPage(leafBuf)
	if err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mgr, err := valuelog.NewManager(walDir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	got, err := mgr.ReadUnsafe(leafPtr.ValuePtr())
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(got) != page.PageSize {
		t.Fatalf("ReadUnsafe len=%d want %d", len(got), page.PageSize)
	}
}

func TestRewriteWriter_AppendLeafPageDoesNotRetainCallerPageAsCompactScratch(t *testing.T) {
	root := t.TempDir()
	valueDir := filepath.Join(root, "value_vlog")
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("mkdir value dir: %v", err)
	}
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf dir: %v", err)
	}

	w := newRewriteWriter(valueDir, 254, 0, 64<<20)
	w.blockCompression = false
	w.ConfigureLeafLog(leafDir, rewriteLeafLogLaneID, 0)

	denseLeaf := buildNonCompactRewriteLeafPage(t)
	beforeDense := append([]byte(nil), denseLeaf...)
	if _, err := w.AppendLeafPage(denseLeaf); err != nil {
		t.Fatalf("AppendLeafPage dense: %v", err)
	}

	compactLeaf := buildCompactRewriteLeafPage(t)
	if _, err := w.AppendLeafPage(compactLeaf); err != nil {
		t.Fatalf("AppendLeafPage compact: %v", err)
	}
	if !bytes.Equal(denseLeaf, beforeDense) {
		t.Fatalf("non-compact caller leaf page was mutated after later compact append: before=%x after=%x", beforeDense[:32], denseLeaf[:32])
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func buildCompactRewriteLeafPage(t *testing.T) []byte {
	t.Helper()
	leafBuf := make([]byte, page.PageSize)
	leafBuilder := node.NewBuilderWithOptions(leafBuf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		key := []byte(fmt.Sprintf("compact-leaf-key-%04d", i))
		if err := leafBuilder.AddLeafEntry(key, []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry compact %d: %v", i, err)
		}
	}
	leafBuilder.FinishNoNode()
	if _, compacted := valuelog.MaybeCompactLeafLogPayloadLength(leafBuf); !compacted {
		t.Fatal("compact leaf fixture did not produce compact payload")
	}
	return leafBuf
}

func buildNonCompactRewriteLeafPage(t *testing.T) []byte {
	t.Helper()
	leafBuf := bytes.Repeat([]byte{0x5a}, page.PageSize)
	if _, compacted := valuelog.MaybeCompactLeafLogPayloadLength(leafBuf); compacted {
		t.Fatal("non-compact fixture unexpectedly produced compact payload")
	}
	return leafBuf
}

func TestRewriteWriter_AppendLeafPageSkipsLeafDictWhenCompressionDisabled(t *testing.T) {
	walDir := t.TempDir()
	leafPage := bytes.Repeat([]byte("rewrite/leaf/page/dict/off/"), 256)
	if len(leafPage) < page.PageSize {
		leafPage = append(leafPage, bytes.Repeat([]byte{'x'}, page.PageSize-len(leafPage))...)
	}
	leafPage = leafPage[:page.PageSize]

	w := newRewriteWriter(walDir, 0, 0, defaultValueLogRewriteSegmentBytes)
	w.blockCompression = false
	w.blockCodec = valuelog.BlockCodecSnappy
	w.SetLeafDict(31001, []byte("dummy-dict-bytes"))
	if _, err := w.AppendLeafPage(leafPage); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join(walDir, "value-l0-*.log"))
	if err != nil {
		t.Fatalf("glob segments: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("expected output segment")
	}

	foundDictFrame := false
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		func() {
			defer f.Close()
			for {
				var header [valuelog.HeaderSize]byte
				if _, err := io.ReadFull(f, header[:]); err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						return
					}
					t.Fatalf("read header %s: %v", filepath.Base(path), err)
				}
				bodyLen := binary.LittleEndian.Uint32(header[16:20])
				body := make([]byte, int(bodyLen))
				if _, err := io.ReadFull(f, body); err != nil {
					t.Fatalf("read body %s: %v", filepath.Base(path), err)
				}
				frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
				if err != nil {
					t.Fatalf("DecodeFrame %s: %v", filepath.Base(path), err)
				}
				if frameHeader.DictID != 0 {
					foundDictFrame = true
					return
				}
			}
		}()
	}
	if foundDictFrame {
		t.Fatalf("expected compression-off leaf rewrite to avoid dict frames")
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
	if _, ok := state.ValueLogSet.Files[oldID]; !ok {
		t.Fatalf("old segment %d missing while an older durable root can still select it", oldID)
	}

	advancePastRetainedDurableSlotForTest(t, db)
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{oldID},
		ObservedSourceAssumeUnreferenced: true,
		ObservedSourceReclaimActive:      true,
	}); err != nil {
		t.Fatalf("ValueLogGC after durable-slot advance: %v", err)
	}
	state = db.State()
	if state == nil || state.ValueLogSet == nil {
		t.Fatalf("db state missing ValueLogSet after source retirement")
	}
	if _, ok := state.ValueLogSet.Files[oldID]; ok {
		t.Fatalf("old segment %d still visible after every recoverable root released it", oldID)
	}
	gotSnap, err = snap.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("snapshot get k2 after source retirement: %v", err)
	}
	if !bytes.Equal(gotSnap, bytes.Repeat([]byte{11}, 512)) {
		t.Fatalf("snapshot value mismatch after source retirement")
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

func TestValueLogRewriteOnline_MaxCopiedBytes_ProcessesExplicitSourceIncrementally(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 205_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 256)
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

	recordLen, err := db.valueLogRecordLengthForRewrite(ptrs[0])
	if err != nil {
		t.Fatalf("record length: %v", err)
	}

	stats1, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs:  []uint32{ptrs[0].FileID},
		BatchSize:      8,
		MaxCopiedBytes: int64(recordLen),
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline first pass: %v", err)
	}
	if stats1.RecordsCopied != 1 {
		t.Fatalf("first pass copied=%d want 1", stats1.RecordsCopied)
	}
	if stats1.SourceSegmentsStillReferenced != 1 || stats1.SourceSegmentsUnreferenced != 0 {
		t.Fatalf("first pass source segment stats still=%d unref=%d want still=1 unref=0", stats1.SourceSegmentsStillReferenced, stats1.SourceSegmentsUnreferenced)
	}
	if !slices.Equal(stats1.SourceFileIDsStillReferenced, []uint32{ptrs[0].FileID}) {
		t.Fatalf("first pass still referenced ids=%v want [%d]", stats1.SourceFileIDsStillReferenced, ptrs[0].FileID)
	}
	if len(stats1.SourceFileIDsUnreferenced) != 0 {
		t.Fatalf("first pass unreferenced ids=%v want empty", stats1.SourceFileIDsUnreferenced)
	}
	if stats1.SourceBytesProcessed != int64(recordLen) {
		t.Fatalf("first pass source bytes processed=%d want %d", stats1.SourceBytesProcessed, recordLen)
	}

	ptrK1, _ := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, _ := readProjectedPointerByKey(t, db, []byte("k2"))
	if ptrK1.FileID == ptrs[0].FileID && ptrK2.FileID == ptrs[0].FileID {
		t.Fatalf("expected one pointer to move off source segment after first pass: k1=%d k2=%d source=%d", ptrK1.FileID, ptrK2.FileID, ptrs[0].FileID)
	}
	if ptrK1.FileID != ptrs[0].FileID && ptrK2.FileID != ptrs[0].FileID {
		t.Fatalf("expected one pointer to remain on source segment after first pass: k1=%d k2=%d source=%d", ptrK1.FileID, ptrK2.FileID, ptrs[0].FileID)
	}

	stats2, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs:  []uint32{ptrs[0].FileID},
		BatchSize:      8,
		MaxCopiedBytes: int64(recordLen),
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline second pass: %v", err)
	}
	if stats2.RecordsCopied != 1 {
		t.Fatalf("second pass copied=%d want 1", stats2.RecordsCopied)
	}
	if stats2.SourceSegmentsStillReferenced != 0 || stats2.SourceSegmentsUnreferenced != 1 {
		t.Fatalf("second pass source segment stats still=%d unref=%d want still=0 unref=1", stats2.SourceSegmentsStillReferenced, stats2.SourceSegmentsUnreferenced)
	}
	if !slices.Equal(stats2.SourceFileIDsUnreferenced, []uint32{ptrs[0].FileID}) {
		t.Fatalf("second pass unreferenced ids=%v want [%d]", stats2.SourceFileIDsUnreferenced, ptrs[0].FileID)
	}

	ptrK1, _ = readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, _ = readProjectedPointerByKey(t, db, []byte("k2"))
	if ptrK1.FileID == ptrs[0].FileID || ptrK2.FileID == ptrs[0].FileID {
		t.Fatalf("expected both pointers off source segment after second pass: k1=%d k2=%d source=%d", ptrK1.FileID, ptrK2.FileID, ptrs[0].FileID)
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

	segmentsBefore, err := listValueLogSegments(dir)
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
	origRIDStartScanner := rewriteRIDStartScanner
	ridStartScanCalls := 0
	rewriteRIDStartScanner = func([]logSegment) (uint64, error) {
		ridStartScanCalls++
		return 0, fmt.Errorf("unexpected rid-start scan")
	}
	t.Cleanup(func() { rewriteRIDStartScanner = origRIDStartScanner })
	origWALLister := rewriteWALSegmentsLister
	walScanCalls := 0
	rewriteWALSegmentsLister = func(string) ([]logSegment, error) {
		walScanCalls++
		return nil, fmt.Errorf("unexpected wal segment scan")
	}
	t.Cleanup(func() { rewriteWALSegmentsLister = origWALLister })

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
	if ridStartScanCalls != 0 {
		t.Fatalf("expected ReserveRIDs mode to skip rid-start scan, calls=%d", ridStartScanCalls)
	}
	if walScanCalls != 0 {
		t.Fatalf("expected ReserveRIDs mode to skip wal segment scan, calls=%d", walScanCalls)
	}

	segmentsAfter, err := listValueLogSegments(dir)
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

func TestValueLogRewriteOnline_WithoutReserveRIDs_UsesRIDStartScanner(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 310_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	sentinel := errors.New("rid-start scan invoked")
	origRIDStartScanner := rewriteRIDStartScanner
	ridStartScanCalls := 0
	rewriteRIDStartScanner = func([]logSegment) (uint64, error) {
		ridStartScanCalls++
		return 0, sentinel
	}
	t.Cleanup(func() { rewriteRIDStartScanner = origRIDStartScanner })
	origWALLister := rewriteWALSegmentsLister
	walScanCalls := 0
	rewriteWALSegmentsLister = func(dir string) ([]logSegment, error) {
		walScanCalls++
		return listValueLogSegments(dir)
	}
	t.Cleanup(func() { rewriteWALSegmentsLister = origWALLister })
	lane, seq, ok := db.valueLogManager.RewriteLaneHint()
	if !ok {
		t.Fatalf("RewriteLaneHint: ok=false")
	}
	probePath := filepath.Join(dir, "value_vlog", fmt.Sprintf("value-l%d-%06d.log", lane, seq+1))
	if err := os.WriteFile(probePath, nil, 0o644); err != nil {
		t.Fatalf("write probe file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(probePath) })

	_, err = db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs[0].FileID},
		BatchSize:     1,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected rid-start scanner error %v, got %v", sentinel, err)
	}
	if ridStartScanCalls != 1 {
		t.Fatalf("expected one rid-start scan call, got %d", ridStartScanCalls)
	}
	if walScanCalls != 1 {
		t.Fatalf("expected one wal segment scan call, got %d", walScanCalls)
	}
}

func TestValueLogRewriteOnline_WithoutReserveRIDs_FastPathStillScansRIDStart(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 315_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	origRIDStartScanner := rewriteRIDStartScanner
	ridStartScanCalls := 0
	rewriteRIDStartScanner = func([]logSegment) (uint64, error) {
		ridStartScanCalls++
		return 315_001, nil
	}
	t.Cleanup(func() { rewriteRIDStartScanner = origRIDStartScanner })
	origWALLister := rewriteWALSegmentsLister
	walScanCalls := 0
	rewriteWALSegmentsLister = func(string) ([]logSegment, error) {
		walScanCalls++
		return nil, fmt.Errorf("unexpected wal segment scan")
	}
	t.Cleanup(func() { rewriteWALSegmentsLister = origWALLister })

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs[0].FileID},
		BatchSize:     1,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one copied record, got %d", stats.RecordsCopied)
	}
	if ridStartScanCalls != 1 {
		t.Fatalf("expected one rid-start scan call, got %d", ridStartScanCalls)
	}
	if walScanCalls != 0 {
		t.Fatalf("expected no wal segment scan calls, got %d", walScanCalls)
	}
	segments, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("listValueLogSegments: %v", err)
	}
	nextRID, err := nextRewriteRIDStart(segments)
	if err != nil {
		t.Fatalf("nextRewriteRIDStart: %v", err)
	}
	allocatedRID := nextRID - 1
	if allocatedRID != 315_001 {
		t.Fatalf("allocated rewrite RID=%d want 315001", allocatedRID)
	}
	if nextRID != 315_002 {
		t.Fatalf("next RID after rewrite=%d want 315002", nextRID)
	}
}

func TestValueLogRewriteOnline_ManagerHintAvoidsReservedLaneAndSkipsFallbackScan(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := &registeredLeafPageLog{db: db, dir: dir}
	db.SetLeafPageLog(leafLog)
	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 316_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 256)
	})
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptrs[0]); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	lane, _, ok := db.valueLogManager.RewriteLaneHint()
	if !ok {
		t.Fatalf("RewriteLaneHint: ok=false")
	}
	if lane == rewriteLeafLogLaneID {
		t.Fatalf("RewriteLaneHint selected reserved leaf lane %d", lane)
	}

	sentinel := errors.New("rid-start scan invoked")
	origRIDStartScanner := rewriteRIDStartScanner
	ridStartScanCalls := 0
	rewriteRIDStartScanner = func([]logSegment) (uint64, error) {
		ridStartScanCalls++
		return 0, sentinel
	}
	t.Cleanup(func() { rewriteRIDStartScanner = origRIDStartScanner })
	origWALLister := rewriteWALSegmentsLister
	walScanCalls := 0
	rewriteWALSegmentsLister = func(dir string) ([]logSegment, error) {
		walScanCalls++
		return listValueLogSegments(dir)
	}
	t.Cleanup(func() { rewriteWALSegmentsLister = origWALLister })

	_, err = db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrs[0].FileID},
		BatchSize:     1,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected rid-start scanner error %v, got %v", sentinel, err)
	}
	if ridStartScanCalls != 1 {
		t.Fatalf("expected one rid-start scan call, got %d", ridStartScanCalls)
	}
	if walScanCalls != 0 {
		t.Fatalf("manager hint unexpectedly used fallback wal segment scan: calls=%d", walScanCalls)
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

func requireCollectionLeafPtr(t *testing.T, db *DB, root uint64) page.LeafLogPtr {
	t.Helper()
	children, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), root)
	if err != nil {
		t.Fatalf("collect collection leaf-ref children for root %d: %v", root, err)
	}
	if !allLeafRefs || len(children) == 0 {
		t.Fatalf("collection root=%d want leaf-ref root or leaf-ref children", root)
	}
	if children[0].childRef.Kind != page.ChildRefLeafLog {
		t.Fatalf("collection child root=%d want leaf-log ref", root)
	}
	return children[0].childRef.Log
}

func requireCollectionLeafFileIDs(t *testing.T, db *DB, root uint64) []uint32 {
	t.Helper()
	children, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), root)
	if err != nil {
		t.Fatalf("collect collection leaf-ref children for root %d: %v", root, err)
	}
	if !allLeafRefs || len(children) == 0 {
		t.Fatalf("collection root=%d want leaf-ref root or leaf-ref children", root)
	}
	fileIDs := make([]uint32, 0, len(children))
	seen := make(map[uint32]struct{}, len(children))
	for _, child := range children {
		if child.childRef.Kind != page.ChildRefLeafLog {
			t.Fatalf("collection child root=%d want leaf-log ref", root)
		}
		leafPtr := child.childRef.Log
		if _, ok := seen[leafPtr.FileID]; ok {
			continue
		}
		seen[leafPtr.FileID] = struct{}{}
		fileIDs = append(fileIDs, leafPtr.FileID)
	}
	return fileIDs
}

func requireLeafLogSegmentsRemoved(t *testing.T, dir string, fileIDs []uint32) {
	t.Helper()
	if len(fileIDs) == 0 {
		t.Fatal("expected at least one leaf-log file id")
	}
	for _, fileID := range fileIDs {
		path := leafLogSegmentPath(t, dir, fileID)
		if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
			t.Fatalf("expected old collection leaf segment %s removed, err=%v", path, err)
		}
	}
}

func readCollectionRootID(t *testing.T, db *DB, descriptorKey string) uint64 {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	return readCollectionRootIDFromSnapshot(t, snap, descriptorKey)
}

func readCollectionRootIDFromSnapshot(t *testing.T, snap *Snapshot, descriptorKey string) uint64 {
	t.Helper()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	encoded, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(descriptorKey))
	if err != nil {
		t.Fatalf("read collection descriptor %q: %v", descriptorKey, err)
	}
	if len(encoded) != 8 {
		t.Fatalf("collection descriptor %q length=%d want 8", descriptorKey, len(encoded))
	}
	return binary.BigEndian.Uint64(encoded)
}

func readCollectionRootValue(t *testing.T, db *DB, descriptorKey string, key []byte) []byte {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	rootID := readCollectionRootIDFromSnapshot(t, snap, descriptorKey)
	val, err := snap.GetAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("read collection key %q at root %d: %v", key, rootID, err)
	}
	return append([]byte(nil), val...)
}

func readCollectionProjectedPointerByKey(t *testing.T, db *DB, descriptorKey string, key []byte) (page.ValuePtr, byte) {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	rootID := readCollectionRootIDFromSnapshot(t, snap, descriptorKey)
	it, err := snap.IteratorAtRootWithOptions(rootID, key, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorAtRootWithOptions(%d): %v", rootID, err)
	}
	defer it.Close()
	if it.Valid() {
		if bytes.Equal(it.UnsafeKey(), key) {
			_, ptr, flags := it.UnsafeEntry()
			return ptr, flags
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("collection projection iterator error: %v", err)
	}
	t.Fatalf("missing collection key %q in projection iterator", key)
	return page.ValuePtr{}, 0
}

func primeValueLogRefTracker(t *testing.T, db *DB) {
	t.Helper()
	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("prime value-log ref tracker: %v", err)
	}
	if db.valueLogRefTracker == nil {
		return
	}
	requireValueLogRefTrackerValid(t, db)
}

func requireValueLogRefTrackerValid(t *testing.T, db *DB) map[uint32]struct{} {
	t.Helper()
	if db == nil || db.valueLogRefTracker == nil {
		t.Fatal("missing value-log ref tracker")
	}
	seq := db.currentCommitSeq()
	refs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected value-log ref tracker to remain valid at seq=%d", seq)
	}
	return refs
}

func leafLogSegmentPath(t *testing.T, dir string, fileID uint32) string {
	t.Helper()
	lane, seq := valuelog.DecodeFileID(fileID)
	return filepath.Join(LeafLogDirPath(dir), fmt.Sprintf("value-l%d-%06d.log", lane, seq))
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

	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
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

func TestSelectRewriteSourceSegments_StaleBytesFloorRemainsUncappedForLargeSegment(t *testing.T) {
	const (
		segmentSize = int64(32 << 20)
		staleBytes  = int64(6 << 20)
	)
	file := rewriteSelectorTestFile(t, "large.log", segmentSize)
	files := map[uint32]*valuelog.File{1: file}
	liveByID := map[uint32]int64{1: segmentSize - staleBytes}
	if capBytes := int64(math.Ceil(float64(segmentSize) * 0.30)); capBytes <= 8<<20 {
		t.Fatalf("fixture cap=%d want > 8 MiB", capBytes)
	}

	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		MinSegmentStaleRatio:         0.10,
		MinSegmentStaleBytes:         8 << 20,
		MinSegmentStaleBytesCapRatio: 0.30,
	}, files, map[uint32]struct{}{}, liveByID)

	if got := float64(staleBytes) / float64(segmentSize); got < 0.10 {
		t.Fatalf("fixture stale ratio=%v want >= 0.10", got)
	}
	if _, ok := selected[1]; ok {
		t.Fatalf("large segment selected below uncapped 8 MiB floor: selected=%v", selected)
	}
}

func TestSelectRewriteSourceSegments_StaleBytesCapCrossoverAndEquality(t *testing.T) {
	const (
		staleFloor = int64(8 << 20)
		capRatio   = 0.30
	)
	// This is the largest integer segment size whose rounded-up cap is exactly
	// the absolute floor. One byte larger crosses to the uncapped side.
	equalitySize := staleFloor * 10 / 3
	if got := int64(math.Ceil(float64(equalitySize) * capRatio)); got != staleFloor {
		t.Fatalf("equality cap=%d want %d for segment size %d", got, staleFloor, equalitySize)
	}
	if got := int64(math.Ceil(float64(equalitySize+1) * capRatio)); got <= staleFloor {
		t.Fatalf("post-crossover cap=%d want > %d", got, staleFloor)
	}
	belowSize := (staleFloor - 1) * 10 / 3
	belowCap := int64(math.Ceil(float64(belowSize) * capRatio))
	if belowCap >= staleFloor {
		t.Fatalf("pre-crossover cap=%d want < %d", belowCap, staleFloor)
	}

	files := map[uint32]*valuelog.File{
		1: rewriteSelectorTestFile(t, "below.log", belowSize),
		2: rewriteSelectorTestFile(t, "equal.log", equalitySize),
		3: rewriteSelectorTestFile(t, "above-below-floor.log", equalitySize+1),
		4: rewriteSelectorTestFile(t, "above-at-floor.log", equalitySize+1),
	}
	liveByID := map[uint32]int64{
		1: belowSize - belowCap,
		2: equalitySize - staleFloor,
		3: equalitySize + 1 - (staleFloor - 1),
		4: equalitySize + 1 - staleFloor,
	}
	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		MinSegmentStaleBytes:         staleFloor,
		MinSegmentStaleBytesCapRatio: capRatio,
	}, files, map[uint32]struct{}{}, liveByID)

	for _, id := range []uint32{1, 2, 4} {
		if _, ok := selected[id]; !ok {
			t.Fatalf("segment %d not selected at effective floor equality: selected=%v", id, selected)
		}
	}
	if _, ok := selected[3]; ok {
		t.Fatalf("post-crossover segment selected below uncapped floor: selected=%v", selected)
	}
}

func rewriteSelectorTestFile(t *testing.T, name string, size int64) *valuelog.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create selector fixture %s: %v", path, err)
	}
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate selector fixture %s to %d: %v", path, size, err)
	}
	return &valuelog.File{Path: path}
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

	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
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

	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
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
}

func TestSelectRewriteSourceSegments_SourceFileIDsRespectAgeWithoutLiveEstimate(t *testing.T) {
	dir := t.TempDir()

	pathOld := filepath.Join(dir, "value-l0-000001.log")
	pathActive := filepath.Join(dir, "value-l0-000002.log")
	pathProtected := filepath.Join(dir, "value-l0-000003.log")
	pathYoung := filepath.Join(dir, "value-l0-000004.log")
	for _, path := range []string{pathOld, pathActive, pathProtected, pathYoung} {
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 100), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	now := time.Now()
	oldTime := now.Add(-5 * time.Minute)
	youngTime := now.Add(-30 * time.Second)
	if err := os.Chtimes(pathOld, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes old: %v", err)
	}
	if err := os.Chtimes(pathActive, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes active: %v", err)
	}
	if err := os.Chtimes(pathProtected, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes protected: %v", err)
	}
	if err := os.Chtimes(pathYoung, youngTime, youngTime); err != nil {
		t.Fatalf("chtimes young: %v", err)
	}

	files := map[uint32]*valuelog.File{
		1: {Path: pathOld},
		2: {Path: pathActive},
		3: {Path: pathProtected},
		4: {Path: pathYoung},
	}
	active := map[uint32]struct{}{
		2: {},
	}

	selected, _ := selectRewriteSourceSegmentsWithStats(ValueLogRewriteOnlineOptions{
		SourceFileIDs:  []uint32{1, 2, 3, 4},
		ProtectedPaths: []string{pathProtected},
		MinSegmentAge:  2 * time.Minute,
	}, files, active, nil)

	if len(selected) != 3 {
		t.Fatalf("expected explicit sources except young segment, got=%v", selected)
	}
	if _, ok := selected[1]; !ok {
		t.Fatalf("expected old explicit source selected, got=%v", selected)
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("active explicit source should remain eligible, got=%v", selected)
	}
	if _, ok := selected[3]; !ok {
		t.Fatalf("protected explicit source should remain eligible, got=%v", selected)
	}
	if _, ok := selected[4]; ok {
		t.Fatalf("young explicit source should be skipped, got=%v", selected)
	}
}

func TestSelectRewriteSourceSegments_MinSegmentAgeAloneEnablesSelection(t *testing.T) {
	dir := t.TempDir()

	pathOld := filepath.Join(dir, "value-l0-000001.log")
	pathYoung := filepath.Join(dir, "value-l0-000002.log")
	for _, path := range []string{pathOld, pathYoung} {
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 100), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
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
		1: {Path: pathOld},
		2: {Path: pathYoung},
	}
	liveByID := map[uint32]int64{
		1: 80,
		2: 80,
	}

	selected, _ := selectRewriteSourceSegmentsWithStats(ValueLogRewriteOnlineOptions{
		MinSegmentAge: 2 * time.Minute,
	}, files, map[uint32]struct{}{}, liveByID)

	if len(selected) != 1 {
		t.Fatalf("expected only old segment selected, got=%v", selected)
	}
	if _, ok := selected[1]; !ok {
		t.Fatalf("expected old segment selected, got=%v", selected)
	}
	if _, ok := selected[2]; ok {
		t.Fatalf("young segment should be skipped, got=%v", selected)
	}
}

func TestSelectRewriteSourceSegments_MinSegmentAgeOnlyWithoutLiveEstimateSelectsEligibleSegments(t *testing.T) {
	dir := t.TempDir()

	pathOld := filepath.Join(dir, "value-l0-000001.log")
	pathYoung := filepath.Join(dir, "value-l0-000002.log")
	for _, path := range []string{pathOld, pathYoung} {
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 100), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
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
		1: {Path: pathOld},
		2: {Path: pathYoung},
	}

	selected, _ := selectRewriteSourceSegmentsWithStats(ValueLogRewriteOnlineOptions{
		MinSegmentAge: 2 * time.Minute,
	}, files, map[uint32]struct{}{}, nil)

	if len(selected) != 1 {
		t.Fatalf("expected only old segment selected without live estimate, got=%v", selected)
	}
	if _, ok := selected[1]; !ok {
		t.Fatalf("expected old segment selected, got=%v", selected)
	}
	if _, ok := selected[2]; ok {
		t.Fatalf("young segment should be skipped, got=%v", selected)
	}
}

func TestSelectRewriteSourceSegments_SourceFileIDsDeduplicatesBeforeBudgeting(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	if err := os.WriteFile(path1, bytes.Repeat([]byte("a"), 64), 0o600); err != nil {
		t.Fatalf("write path1: %v", err)
	}
	if err := os.WriteFile(path2, bytes.Repeat([]byte("b"), 64), 0o600); err != nil {
		t.Fatalf("write path2: %v", err)
	}
	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}
	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		SourceFileIDs:  []uint32{1, 1, 2},
		MaxSourceBytes: 96,
	}, files, map[uint32]struct{}{}, nil)

	if len(selected) != 2 {
		t.Fatalf("expected duplicate explicit IDs collapsed to unique sources, got=%v", selected)
	}
	if _, ok := selected[1]; !ok {
		t.Fatalf("expected source 1 selected, got=%v", selected)
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("expected source 2 selected after dedupe, got=%v", selected)
	}
}

func TestSelectRewriteSourceSegments_SourceFileIDsIgnoreSparseCaps(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	if err := os.WriteFile(path1, bytes.Repeat([]byte("a"), 64), 0o600); err != nil {
		t.Fatalf("write path1: %v", err)
	}
	if err := os.WriteFile(path2, bytes.Repeat([]byte("b"), 64), 0o600); err != nil {
		t.Fatalf("write path2: %v", err)
	}
	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}

	selected := selectRewriteSourceSegments(ValueLogRewriteOnlineOptions{
		SourceFileIDs:     []uint32{1, 2},
		MaxSourceSegments: 1,
		MaxSourceBytes:    64,
	}, files, map[uint32]struct{}{}, nil)

	if len(selected) != 2 {
		t.Fatalf("expected explicit sources to ignore sparse caps, got=%v", selected)
	}
	if _, ok := selected[1]; !ok {
		t.Fatalf("expected source 1 selected, got=%v", selected)
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("expected source 2 selected, got=%v", selected)
	}
}

func TestRewritePlanNeedsLiveEstimate_MinSegmentAgeOnly(t *testing.T) {
	if rewritePlanNeedsLiveEstimate(ValueLogRewriteOnlineOptions{MinSegmentAge: time.Minute}) {
		t.Fatalf("expected MinSegmentAge-only selection to avoid live-byte estimation")
	}
	if !rewritePlanNeedsLiveEstimate(ValueLogRewriteOnlineOptions{MinSegmentAge: time.Minute, MaxSourceBytes: 1}) {
		t.Fatalf("expected MinSegmentAge+MaxSourceBytes to require live-byte estimation")
	}
	if !rewritePlanNeedsLiveEstimate(ValueLogRewriteOnlineOptions{MinSegmentAge: time.Minute, MinSegmentStaleRatio: 0.5}) {
		t.Fatalf("expected stale-ratio selection to require live-byte estimation")
	}
}

func TestValueLogRewriteOnline_SourceFileIDsWithStaleFilterMatchesPlanSelection(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 210_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 210_010, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), 256)
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
		SourceFileIDs:        []uint32{ptrs1[0].FileID, ptrs2[0].FileID},
		MinSegmentStaleRatio: 0.25,
		BatchSize:            8,
	}

	plan, err := db.ValueLogRewritePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("ValueLogRewritePlan: %v", err)
	}
	if !slices.Equal(plan.SourceFileIDs, []uint32{ptrs1[0].FileID}) {
		t.Fatalf("plan source IDs=%v want [%d]", plan.SourceFileIDs, ptrs1[0].FileID)
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("expected one rewritten record from selected explicit source, got %d", stats.RecordsCopied)
	}
	if stats.SourceSegmentsRequested != 1 {
		t.Fatalf("source segments requested=%d want 1", stats.SourceSegmentsRequested)
	}
	if stats.SourceSegmentsStillReferenced != 0 {
		t.Fatalf("source segments still referenced=%d want 0", stats.SourceSegmentsStillReferenced)
	}
	if stats.SourceSegmentsUnreferenced != 1 {
		t.Fatalf("source segments unreferenced=%d want 1", stats.SourceSegmentsUnreferenced)
	}
	if stats.SourceBytesRequested <= 0 {
		t.Fatalf("source bytes requested=%d want > 0", stats.SourceBytesRequested)
	}
	if stats.SourceBytesStillReferenced != 0 {
		t.Fatalf("source bytes still referenced=%d want 0", stats.SourceBytesStillReferenced)
	}
	if stats.SourceBytesUnreferenced != stats.SourceBytesRequested {
		t.Fatalf("source bytes unreferenced=%d want requested=%d", stats.SourceBytesUnreferenced, stats.SourceBytesRequested)
	}

	ptrK1, flagsK1 := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, flagsK2 := readProjectedPointerByKey(t, db, []byte("k2"))
	if flagsK1&node.FlagPointer == 0 || flagsK2&node.FlagPointer == 0 {
		t.Fatalf("expected pointer flags for rewritten keys: k1=%#x k2=%#x", flagsK1, flagsK2)
	}
	if ptrK1.FileID == ptrs1[0].FileID {
		t.Fatalf("expected k1 pointer to move off filtered source segment %d", ptrs1[0].FileID)
	}
	if ptrK2.FileID != ptrs2[0].FileID {
		t.Fatalf("expected k2 pointer to remain on fully-live explicit segment %d, got %d", ptrs2[0].FileID, ptrK2.FileID)
	}
}

func TestValueLogRewriteOffline_UsesCurrentOuterLeafDictForSplitLeafLog(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: ValueLogOptions{
			Compression:      ValueLogCompressionBlock,
			BlockCodec:       ValueLogBlockLZ4,
			PointerThreshold: 4096,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := &rewriteTestLeafPageLog{db: db, dir: dir}
	db.SetLeafPageLog(leafLog)
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("leaf-dict-rewrite-%04d", i))
		val := bytes.Repeat([]byte(fmt.Sprintf("leaf-dict-%02d|", i%32)), 2)
		if err := db.Set(key, val); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("close leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	leafA, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "outer-a"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(a): %v", err)
	}
	leafB, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "outer-b"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(b): %v", err)
	}
	leafC, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "outer-c"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(c): %v", err)
	}
	dictID := uint64(99231)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{leafA, leafB, leafC},
		History:  append([]byte(nil), leafA...),
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatal("expected non-empty dict")
	}

	rewriteOpts := opts
	rewriteOpts.ValueLog.DictLookup = func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, valuelog.ErrMissingDict
		}
		return dict, nil
	}
	rewriteOpts.ValueLog.DictCurrentForClass = func(_ context.Context, class string) (uint64, error) {
		if class == "outer_leaf" {
			return dictID, nil
		}
		return 0, nil
	}
	stats, err := ValueLogRewriteOffline(rewriteOpts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected copied records, stats=%+v", stats)
	}

	leafDir := filepath.Join(dir, "leaf_vlog")
	frameHeader := readFirstRewriteFrameHeaderForLane(t, leafDir, rewriteLeafLogLaneID)
	if frameHeader.DictID != dictID {
		t.Fatalf("leaf rewrite dict id=%d want=%d", frameHeader.DictID, dictID)
	}
}

func TestPrepareRewriteLeafDict_LiveLeafRefBootstrapBestEffort(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: ValueLogOptions{
			Compression:      ValueLogCompressionBlock,
			BlockCodec:       ValueLogBlockLZ4,
			PointerThreshold: 4096,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	leafLog := &rewriteTestLeafPageLog{db: db, dir: dir}
	db.SetLeafPageLog(leafLog)
	defer func() { _ = leafLog.Close() }()
	for i := 0; i < 1024; i++ {
		key := []byte(fmt.Sprintf("leaf-dict-bootstrap-%04d", i))
		val := bytes.Repeat([]byte(fmt.Sprintf("bootstrap/%02d/%02d/%s|", i%32, (i/32)%16, strings.Repeat(string('a'+byte(i%26)), 6))), 8)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	state := db.State()
	if state == nil {
		t.Fatal("expected db state")
	}
	var putCalls int
	var gotPutDict []byte
	var setCurrentCalls int
	dictID, dictBytes, useRawPages, err := prepareRewriteLeafDict(db, state, nil, nil, nil, func(_ context.Context, dict []byte) (uint64, error) {
		putCalls++
		gotPutDict = append([]byte(nil), dict...)
		return 55123, nil
	}, func(_ context.Context, class string, dictID uint64) error {
		setCurrentCalls++
		if class != "outer_leaf" {
			t.Fatalf("set current class=%q want outer_leaf", class)
		}
		if dictID != 55123 {
			t.Fatalf("set current dict id=%d want=55123", dictID)
		}
		return nil
	}, func(_ context.Context, dictID uint64, useRawPages bool) error {
		if dictID != 55123 {
			t.Fatalf("set payload mode dict id=%d want=55123", dictID)
		}
		if useRawPages {
			t.Fatal("bootstrapped compact leaf dict unexpectedly marked raw")
		}
		return nil
	}, compression.TrainConfig{
		TrainBytes: 64 << 10,
		DictBytes:  8 << 10,
		MinRecords: 4,
	})
	if err != nil {
		t.Fatalf("prepareRewriteLeafDict: %v", err)
	}
	if useRawPages {
		t.Fatal("expected bootstrapped leaf dict to target compact leaf payloads")
	}
	if dictID != 0 {
		if dictID != 55123 {
			t.Fatalf("dict id=%d want=55123", dictID)
		}
		if putCalls != 1 {
			t.Fatalf("dict put calls=%d want=1", putCalls)
		}
		if setCurrentCalls != 1 {
			t.Fatalf("set current calls=%d want=1", setCurrentCalls)
		}
		if len(dictBytes) == 0 {
			t.Fatal("expected trained leaf dict bytes")
		}
		if !bytes.Equal(dictBytes, gotPutDict) {
			t.Fatal("expected returned dict bytes to match published bytes")
		}
		return
	}
	if putCalls != 0 {
		t.Fatalf("dict put calls=%d want=0 when no dict is produced", putCalls)
	}
	if setCurrentCalls != 0 {
		t.Fatalf("set current calls=%d want=0 when no dict is produced", setCurrentCalls)
	}
	if len(dictBytes) != 0 {
		t.Fatalf("dict bytes len=%d want=0 when bootstrap does not produce a usable dict", len(dictBytes))
	}
}

func TestPrepareRewriteLeafDict_NoUsableCallbacksSkipsWork(t *testing.T) {
	calledCurrent := false
	dictID, dictBytes, useRawPages, err := prepareRewriteLeafDict(
		&DB{},
		&DBState{ValueLogSet: &valuelog.Set{}},
		func(context.Context, string) (uint64, error) {
			calledCurrent = true
			return 0, errors.New("current callback should be skipped without lookup or put")
		},
		nil,
		nil,
		nil,
		nil,
		nil,
		compression.TrainConfig{},
	)
	if err != nil {
		t.Fatalf("prepareRewriteLeafDict: %v", err)
	}
	if calledCurrent {
		t.Fatal("current callback was called even though no lookup or publish callback was available")
	}
	if dictID != 0 || len(dictBytes) != 0 || useRawPages {
		t.Fatalf("unexpected dict result: id=%d bytes=%d useRawPages=%t", dictID, len(dictBytes), useRawPages)
	}
}

func TestPrepareRewriteLeafDict_CurrentClassCompactModePreserved(t *testing.T) {
	state := &DBState{ValueLogSet: &valuelog.Set{}}
	dictBytes := []byte("compact-leaf-dict")
	dictID, gotDict, useRawPages, err := prepareRewriteLeafDict(
		&DB{},
		state,
		func(_ context.Context, class string) (uint64, error) {
			if class != "outer_leaf" {
				t.Fatalf("current class=%q want outer_leaf", class)
			}
			return 8123, nil
		},
		func(_ context.Context, id uint64) (bool, bool, error) {
			if id != 8123 {
				t.Fatalf("mode lookup dict id=%d want=8123", id)
			}
			return false, true, nil
		},
		func(id uint64) ([]byte, error) {
			if id != 8123 {
				t.Fatalf("lookup dict id=%d want=8123", id)
			}
			return append([]byte(nil), dictBytes...), nil
		},
		nil,
		nil,
		nil,
		compression.TrainConfig{},
	)
	if err != nil {
		t.Fatalf("prepareRewriteLeafDict: %v", err)
	}
	if dictID != 8123 {
		t.Fatalf("dict id=%d want=8123", dictID)
	}
	if useRawPages {
		t.Fatal("expected explicit compact mode to preserve compact payloads")
	}
	if !bytes.Equal(gotDict, dictBytes) {
		t.Fatalf("dict bytes mismatch: got=%x want=%x", gotDict, dictBytes)
	}
}

func TestPrepareRewriteLeafDict_CurrentClassMissingModeDefaultsCompact(t *testing.T) {
	dictID, _, useRawPages, err := prepareRewriteLeafDict(
		&DB{},
		&DBState{ValueLogSet: &valuelog.Set{}},
		func(_ context.Context, class string) (uint64, error) {
			if class != "outer_leaf" {
				t.Fatalf("current class=%q want outer_leaf", class)
			}
			return 9123, nil
		},
		func(_ context.Context, id uint64) (bool, bool, error) {
			if id != 9123 {
				t.Fatalf("mode lookup dict id=%d want=9123", id)
			}
			return false, false, nil
		},
		func(id uint64) ([]byte, error) {
			if id != 9123 {
				t.Fatalf("lookup dict id=%d want=9123", id)
			}
			return []byte("class-current-dict"), nil
		},
		nil,
		nil,
		nil,
		compression.TrainConfig{},
	)
	if err != nil {
		t.Fatalf("prepareRewriteLeafDict: %v", err)
	}
	if dictID != 9123 {
		t.Fatalf("dict id=%d want=9123", dictID)
	}
	if useRawPages {
		t.Fatal("expected class-current dict without mode metadata to default to compact payloads")
	}
}

func TestRewriteWriterAppendLeafPagesCapsGroupedFrames(t *testing.T) {
	leafDir := t.TempDir()
	w := &rewriteWriter{
		leafDir:  leafDir,
		leafLane: uint32(255),
		maxSize:  1 << 20,
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	leafPages := make([][]byte, rewriteLeafLogBatchMaxK+1)
	for i := range leafPages {
		leafPages[i] = bytes.Repeat([]byte{byte(i + 1)}, page.PageSize)
	}
	ptrs, err := w.appendLeafPagesWithRIDStart(1, leafPages)
	if err != nil {
		t.Fatalf("appendLeafPagesWithRIDStart: %v", err)
	}
	if len(ptrs) != len(leafPages) {
		t.Fatalf("ptrs=%d want %d", len(ptrs), len(leafPages))
	}
	first := ptrs[0]
	if !first.IsGrouped() {
		t.Fatalf("first rewritten leaf ptr not grouped: %+v", first)
	}
	for i := 0; i < rewriteLeafLogBatchMaxK; i++ {
		if ptrs[i].FileID != first.FileID || ptrs[i].Offset != first.Offset {
			t.Fatalf("ptr[%d]=%+v not in first capped frame %+v", i, ptrs[i], first)
		}
		if ptrs[i].SubIndex != uint16(i) {
			t.Fatalf("ptr[%d].SubIndex=%d want %d", i, ptrs[i].SubIndex, i)
		}
	}
	if ptrs[rewriteLeafLogBatchMaxK].FileID == first.FileID && ptrs[rewriteLeafLogBatchMaxK].Offset == first.Offset {
		t.Fatalf("ptr[%d]=%+v remained in first frame capped at %d", rewriteLeafLogBatchMaxK, ptrs[rewriteLeafLogBatchMaxK], rewriteLeafLogBatchMaxK)
	}
}
