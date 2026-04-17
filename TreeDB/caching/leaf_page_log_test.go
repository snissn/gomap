package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCachingLeafPageLog_AppendLeafPageReturnsAppendError(t *testing.T) {
	db := &DB{}
	db.closeCh = make(chan struct{})
	close(db.closeCh)
	leaf := lane{id: leafLogLaneID}
	leafLog := &cachingLeafPageLog{db: db, lane: &leaf}
	_, err := leafLog.AppendLeafPage([]byte("leaf"))
	if !errors.Is(err, errWALClosed) {
		t.Fatalf("AppendLeafPage error=%v want %v", err, errWALClosed)
	}
}

func TestCachingLeafPageLog_SyncRespectsRelaxedSync(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "leaf.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{relaxedSync: true}
	var flushCalls atomic.Int32
	var syncCalls atomic.Int32
	db.testOnVlogFlush = func(laneID int) {
		flushCalls.Add(1)
	}
	db.testOnVlogSync = func(laneID int) {
		syncCalls.Add(1)
	}
	leaf := lane{id: leafLogLaneID, vlog: writer}
	leaf.vlogDirty.Store(true)
	leafLog := &cachingLeafPageLog{db: db, lane: &leaf}

	if err := leafLog.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := flushCalls.Load(); got != 1 {
		t.Fatalf("flush calls=%d want 1", got)
	}
	if got := syncCalls.Load(); got != 0 {
		t.Fatalf("sync calls=%d want 0", got)
	}
}

type leafRecordLengthNotifierBackendStub struct {
	notified []page.ValuePtr
}

func (s *leafRecordLengthNotifierBackendStub) Get([]byte) ([]byte, error)       { return nil, nil }
func (s *leafRecordLengthNotifierBackendStub) GetUnsafe([]byte) ([]byte, error) { return nil, nil }
func (s *leafRecordLengthNotifierBackendStub) GetAppend(key, dst []byte) ([]byte, error) {
	return dst, nil
}
func (s *leafRecordLengthNotifierBackendStub) Has([]byte) (bool, error) { return false, nil }
func (s *leafRecordLengthNotifierBackendStub) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, nil
}
func (s *leafRecordLengthNotifierBackendStub) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, nil
}
func (s *leafRecordLengthNotifierBackendStub) NewBatch() batch.Interface { return nil }
func (s *leafRecordLengthNotifierBackendStub) Close() error              { return nil }
func (s *leafRecordLengthNotifierBackendStub) Print() error              { return nil }
func (s *leafRecordLengthNotifierBackendStub) Stats() map[string]string  { return nil }
func (s *leafRecordLengthNotifierBackendStub) NoteLeafGenerationRecordLength(ptr page.ValuePtr) {
	s.notified = append(s.notified, ptr)
}

func TestDB_NoteLeafGenerationRecordLength_ForwardsToBackend(t *testing.T) {
	stub := &leafRecordLengthNotifierBackendStub{}
	db := &DB{backend: stub}
	ptr := page.ValuePtr{FileID: 7, Offset: 11, Length: page.ValuePtrMarkCompressed(123)}

	db.noteLeafGenerationRecordLength(ptr)

	if got, want := len(stub.notified), 1; got != want {
		t.Fatalf("notified=%d want %d", got, want)
	}
	if got := stub.notified[0]; got != ptr {
		t.Fatalf("notified ptr=%+v want %+v", got, ptr)
	}
}

func buildSparseLeafPageForLeafLogTest(t *testing.T) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		if err := b.AddLeafEntry([]byte("key-"+string(rune('a'+i))), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

func buildOuterLeafDictTrainingPageForLeafLogTest(t *testing.T, seed int) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 24; i++ {
		key := []byte(fmt.Sprintf("namespace/%02d/account/%03d/slot/%02d", seed%17, i, seed%5))
		value := bytes.Repeat([]byte(fmt.Sprintf("value-block-%02d-", (seed+i)%23)), 8)
		if err := b.AddLeafEntry(key, value[:96], node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(seed=%d, entry=%d): %v", seed, i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

func TestCachingLeafPageLog_AppendLeafPageCompactsSparseLeafPayload(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(uint32(leafLogLaneID), 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "leaf.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{closeCh: make(chan struct{})}
	db.nextRID.Store(0)
	leaf := lane{id: leafLogLaneID, vlog: writer}
	leafLog := &cachingLeafPageLog{db: db, lane: &leaf}

	if _, err := leafLog.AppendLeafPage(buildSparseLeafPageForLeafLogTest(t)); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	writer = nil
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q): %v", path, err)
	}
	if info.Size() >= int64(valuelog.HeaderSize+page.PageSize) {
		t.Fatalf("file size=%d want compact leaf payload smaller than raw %d", info.Size(), valuelog.HeaderSize+page.PageSize)
	}
}

func TestCachingLeafPageLog_AppendLeafPageUsesOuterLeafDictFramesInAutoBalancedSplitMode(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "maindb")})
	if err != nil {
		t.Fatalf("backenddb.Open: %v", err)
	}
	opts := Options{
		AllowUnsafe:                true,
		DisableWAL:                 true,
		RelaxedSync:                true,
		IndexOuterLeavesInValueLog: true,
		ValueLogCompression:        uint8(vlogCompressionAuto),
		ValueLogAutoPolicy:         uint8(vlogAutoBalanced),
		ValueLogDictClassMode:      uint8(vlogDictClassModeSplitOuterLeaf),
		ValueLogCompressionAutotune: valuelog.AutotuneOptions{
			Mode: valuelog.AutotuneOff,
		},
		ValueLogDictAdaptiveRatio: -1,
		ValueLogDictTrain: compression.TrainConfig{
			TrainBytes:   32 << 10,
			MinRecords:   8,
			SampleStride: 1,
		},
	}
	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{Dir: filepath.Join(dir, "dictdb")})
	if err != nil {
		t.Fatalf("dictdb.Open: %v", err)
	}
	db.SetDictStore(store)

	leafLog := newCachingLeafPageLog(db, &db.leafLog)
	for i := 0; i < 512; i++ {
		if _, err := leafLog.AppendLeafPage(buildOuterLeafDictTrainingPageForLeafLogTest(t, i)); err != nil {
			t.Fatalf("AppendLeafPage(train %d): %v", i, err)
		}
	}
	if err := leafLog.Flush(); err != nil {
		t.Fatalf("Flush(train): %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	var outerDictID uint64
	for time.Now().Before(deadline) {
		stats := db.Stats()
		rawOuterDictID := stats["treedb.cache.vlog_dict.last_applied_dict_id.outer_leaf"]
		if rawOuterDictID != "" {
			parsed, parseErr := strconv.ParseUint(rawOuterDictID, 10, 64)
			if parseErr != nil {
				t.Fatalf("parse outer leaf dict id %q: %v", rawOuterDictID, parseErr)
			}
			if parsed > 0 {
				outerDictID = parsed
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if outerDictID == 0 {
		stats := db.Stats()
		outerTrainerStats := db.valueLogDictTrainerByClass[vlogDictClassOuterLeaf].Stats()
		t.Fatalf("expected outer-leaf dict publish, got outer_leaf dict id=%q frames.dict=%q trainer=%+v",
			stats["treedb.cache.vlog_dict.last_applied_dict_id.outer_leaf"],
			stats["treedb.cache.vlog_auto.frames.dict"],
			outerTrainerStats)
	}
	if useRawPages, ok, err := store.GetLeafPayloadMode(context.Background(), outerDictID); err != nil {
		t.Fatalf("GetLeafPayloadMode: %v", err)
	} else if !ok {
		t.Fatalf("expected outer-leaf dict payload mode for dict %d", outerDictID)
	} else if useRawPages {
		t.Fatalf("expected live outer-leaf dict %d to target compact payloads", outerDictID)
	}
	resolveProbePage, _, err := valuelog.MaybeCompactLeafLogPayload(buildOuterLeafDictTrainingPageForLeafLogTest(t, 4096))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(resolve probe): %v", err)
	}
	chosenMode, chosenCodec, chosenProbe := db.resolveVlogWriteMode(&db.leafLog, outerDictID, len(resolveProbePage), len(resolveProbePage), true)

	for i := 0; i < 256; i++ {
		if _, err := leafLog.AppendLeafPage(buildOuterLeafDictTrainingPageForLeafLogTest(t, 1024+i)); err != nil {
			t.Fatalf("AppendLeafPage(dict %d): %v", i, err)
		}
	}
	if err := leafLog.Flush(); err != nil {
		t.Fatalf("Flush(dict): %v", err)
	}

	stats := db.Stats()
	rawDictFrames := stats["treedb.cache.vlog_auto.frames.dict"]
	dictFrames, err := strconv.ParseUint(rawDictFrames, 10, 64)
	if err != nil {
		t.Fatalf("parse frames.dict %q: %v", rawDictFrames, err)
	}
	if dictFrames == 0 {
		t.Fatalf("expected live leaf-log appends to emit dict frames, got outer_leaf dict id=%q cached_current=%q frames.dict=%q frames.off=%q block_lz4=%q block_snappy=%q chosen_mode=%v chosen_codec=%v chosen_probe=%t",
			stats["treedb.cache.vlog_dict.last_applied_dict_id.outer_leaf"],
			stats["treedb.cache.vlog_dict.cached_current_id.outer_leaf"],
			rawDictFrames,
			stats["treedb.cache.vlog_auto.frames.off"],
			stats["treedb.cache.vlog_auto.frames.block_lz4"],
			stats["treedb.cache.vlog_auto.frames.block_snappy"],
			chosenMode,
			chosenCodec,
			chosenProbe)
	}
	if got := stats["treedb.cache.vlog_dict.cached_current_id.outer_leaf"]; got != strconv.FormatUint(outerDictID, 10) {
		t.Fatalf("cached outer_leaf current id=%q want %d", got, outerDictID)
	}
}
