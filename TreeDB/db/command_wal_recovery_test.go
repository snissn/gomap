package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

var testRegisteredReplayHandlerKind atomic.Uint32

func TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("keep"), []byte("before")); err != nil {
		t.Fatalf("Set keep before: %v", err)
	}
	if err := b.Set([]byte("drop"), []byte("gone")); err != nil {
		t.Fatalf("Set drop before: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync before: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close before batch: %v", err)
	}

	db.testFailFinalizeCommit.Store(true)
	crashBatch := db.NewBatch()
	if err := crashBatch.Set([]byte("keep"), []byte("after")); err != nil {
		t.Fatalf("Set keep crash batch: %v", err)
	}
	if err := crashBatch.Delete([]byte("drop")); err != nil {
		t.Fatalf("Delete drop crash batch: %v", err)
	}
	err := crashBatch.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("crash batch WriteSync error=%v, want failpoint", err)
	}
	_ = crashBatch.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close crashed db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "keep", "after")
	if got, err := reopen.Get([]byte("drop")); err != nil || got != nil {
		t.Fatalf("Get(drop)=%q err=%v, want missing after command WAL delete replay", got, err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALRawEmptyPointKeyAndZeroLengthValueReplayReopen(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte{}, Value: []byte("before-delete")},
		{Op: commitlog.RawKVOpDelete, Key: []byte{}},
		{Op: commitlog.RawKVOpSet, Key: []byte{}, Value: []byte{}},
		{Op: commitlog.RawKVOpSet, Key: []byte("zero"), Value: []byte{}},
	})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	for _, key := range [][]byte{[]byte{}, []byte("zero")} {
		has, err := reopen.Has(key)
		if err != nil {
			t.Fatalf("Has(%q): %v", key, err)
		}
		if !has {
			t.Fatalf("Has(%q)=false, want true", key)
		}
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if got == nil || len(got) != 0 {
			t.Fatalf("Get(%q)=%#v (len=%d), want non-nil zero-length", key, got, len(got))
		}
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALCrashAfterFrameBeforeRootPublishRecovers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	beforeFileSyncs := commandWALTestStatUint64(t, db.Stats(), "treedb.command_wal.file_sync.calls_total")
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("WriteSync error=%v, want failpoint", err)
	}
	if got := commandWALTestStatUint64(t, db.Stats(), "treedb.command_wal.file_sync.calls_total"); got != beforeFileSyncs+1 {
		t.Fatalf("command WAL file sync calls=%d, want %d before publication failpoint", got, beforeFileSyncs+1)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALCrashDuringRootPublishSelectsOldTupleOrNewTuple(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	activeMetaPage := db.metaPageID
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptIndexPageByte(t, dir, activeMetaPage)
	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALCrashAfterRootAppliedLSNBeforeCleanupSkipsFrame(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("command WAL frame missing before cleanup-resume reopen: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALReopenAfterPublishedFrameUsesFreshSegment(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("command WAL frame missing before reopen: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); err != nil {
		t.Fatalf("fresh command WAL segment missing after reopen: %v; entries=%v", err, commandWALSegmentNamesForTest(t, dir))
	}
}

func TestCommandWALOpenUsesLaneZeroActiveSegmentWhenOtherLaneHigher(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	writeCommandWALRawKVFrameForLane(t, dir, 0, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("a"), Value: []byte("1")}})
	writeCommandWALRawKVFrameForLane(t, dir, 1, 9, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("b"), Value: []byte("2")}})
	lane0Path := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	f, err := os.OpenFile(lane0Path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile terminal tail: %v", err)
	}
	if _, err := f.Write([]byte{0x01, 0x02}); err != nil {
		_ = f.Close()
		t.Fatalf("Write terminal tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close terminal tail writer: %v", err)
	}

	db := openCommandWALDB(t, dir)
	defer db.Close()
	assertDBValue(t, db, "a", "1")
	assertDBValue(t, db, "b", "2")
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 10))); !os.IsNotExist(err) {
		t.Fatalf("lane 0 opened at global max segment: stat err=%v entries=%v", err, commandWALSegmentNamesForTest(t, dir))
	}
}

func TestCommandWALCleanReopenCleansCoveredNonActiveSegmentWithNoReplayFrames(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("covered"), Value: []byte("v")}})
	writeCommandWALRawKVFrame(t, dir, 2, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("covered-active"), Value: []byte("v")}})
	coveredPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	if _, err := os.Stat(coveredPath); err != nil {
		t.Fatalf("covered segment missing before reopen: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
	if _, err := os.Stat(coveredPath); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat error=%v, want removed on clean reopen", err)
	}
	// The active covered segment (seq=2, highest seq among covered segments)
	// must be retained so that the next open can compute commandSegmentSeq
	// correctly and does not collide with a surviving segment.
	activeCoveredPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2))
	if _, err := os.Stat(activeCoveredPath); err != nil {
		t.Fatalf("active covered segment stat error=%v, want retained on clean reopen", err)
	}
}

func TestCommandWALInlineReplayDoesNotScanValueLogRIDMap(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	valueLogDir := resolveStorageLayout(dir).valueVLogDir
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("MkdirAll value_vlog: %v", err)
	}
	if err := os.WriteFile(filepath.Join(valueLogDir, "value-l0-000001.log"), []byte("corrupt value log"), 0o600); err != nil {
		t.Fatalf("write corrupt value log: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("inline"), Value: []byte("v")}})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "inline", "v")
}

func TestCommandWALRecoveryRejectsNonIncreasingLSNInSegment(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	writeCommandWALRawKVFrame(t, dir, 1, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("b"), Value: []byte("2")}})
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("a"), Value: []byte("1")}})

	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open error=%v, want ErrCommandWALDuplicateLSN for non-increasing segment LSNs", err)
	}
}

func TestCommandWALSetRIDReplayDoesNotNeedInlineAppenderWithoutOuterLeafLog(t *testing.T) {
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSetRID, Key: []byte("ptr-key"), RID: 7},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	frames := []commandWALReplayFrame{{
		env: commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed,
			LSN:           1,
			Kind:          commitlog.CommandKindRawKVBatch,
			Scope:         commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
			Payload:       payload,
		},
	}}
	needs, err := commandWALReplayFramesNeedLogSupport(&DB{}, frames, 0)
	if err != nil {
		t.Fatalf("commandWALReplayFramesNeedLogSupport: %v", err)
	}
	if needs {
		t.Fatalf("SetRID-only replay should need only the RID map when outer-leaf log is disabled")
	}
	needs, err = commandWALReplayFramesNeedLogSupport(&DB{indexOuterLeavesInValueLog: true}, frames, 0)
	if err != nil {
		t.Fatalf("commandWALReplayFramesNeedLogSupport outer-leaf: %v", err)
	}
	if !needs {
		t.Fatalf("outer-leaf replay still needs leaf-page log support")
	}
}

func TestCommandWALRawSetReplayRePointersWhenThresholdDrops(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	value := strings.Repeat("threshold-drop-", 64)
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("k"), Value: []byte(value)}})

	reopen, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open with lower pointer threshold: %v", err)
	}
	defer reopen.Close()
	assertDBValue(t, reopen, "k", value)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALRawSetNeedsReplayValueLogMatchesBatchSet(t *testing.T) {
	db, err := Open(Options{
		Dir: t.TempDir(),
		ValueLog: ValueLogOptions{
			PointerThreshold: 256,
			DomainInlineThresholds: []ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 16},
				{Prefix: []byte("cold/"), InlineThreshold: 1024},
			},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	cases := []struct {
		key      string
		valueLen int
	}{
		{key: "hot/key", valueLen: 64},
		{key: "cold/key", valueLen: 64},
		{key: "other/small", valueLen: 64},
		{key: "other/large", valueLen: 300},
	}
	for _, tc := range cases {
		value := bytes.Repeat([]byte("v"), tc.valueLen)
		b := db.NewBatch().(*Batch)
		err := b.Set([]byte(tc.key), value)
		if closeErr := b.Close(); closeErr != nil {
			t.Fatalf("Close batch %s: %v", tc.key, closeErr)
		}
		batchNeedsPointer := errors.Is(err, batchpkg.ErrValueTooLarge)
		if err != nil && !batchNeedsPointer {
			t.Fatalf("Batch.Set(%s, len=%d) error: %v", tc.key, tc.valueLen, err)
		}
		recoveryNeedsAppender := commandWALRawSetNeedsReplayValueLog(db, []byte(tc.key), value)
		if recoveryNeedsAppender != batchNeedsPointer {
			t.Fatalf("commandWALRawSetNeedsReplayValueLog(%s, len=%d)=%v, Batch.Set pointer need=%v", tc.key, tc.valueLen, recoveryNeedsAppender, batchNeedsPointer)
		}
	}
}

func TestCommandWALRawSetReplayLazilyCreatesAppenderIfPlacementDrifts(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := strings.Repeat("large-replay-value-", 64)
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte("k"),
		Value: []byte(value),
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	var ensured bool
	var appender *replayInlineAppender
	ensure := func() (map[uint64]page.ValuePtr, *replayInlineAppender, error) {
		ensured = true
		if appender != nil {
			return nil, appender, nil
		}
		var err error
		appender, err = newReplayInlineAppender(db, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		return nil, appender, nil
	}
	defer func() {
		if appender != nil {
			_ = appender.close()
		}
	}()

	err = applyRawKVCommandWALFrame(db, commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed,
		LSN:           1,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
		Payload:       payload,
	}, nil, nil, nil, ensure)
	if err != nil {
		t.Fatalf("applyRawKVCommandWALFrame: %v", err)
	}
	if !ensured {
		t.Fatalf("replay did not lazily create value-log appender")
	}
	assertDBValue(t, db, "k", value)
	if got := db.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALRegisteredReplayHandlerInstallsValueLogAppender(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	kindOffset := testRegisteredReplayHandlerKind.Add(1)
	if kindOffset > 5000 {
		t.Fatalf("test replay handler kind offset exhausted: %d", kindOffset)
	}
	kind := commitlog.CommandKind(60000 + kindOffset)
	var handlerCalled atomic.Bool
	RegisterCommandWALReplayHandler(kind, func(db *DB, env commitlog.CommandEnvelope) error {
		handlerCalled.Store(true)
		if !db.HasValueLogAppender() {
			return ErrValueLogAppenderUnavailable
		}
		_, err := db.AppendValueLogValues([][]byte{[]byte(strings.Repeat("collection-replay-value-", 32))})
		return err
	})

	var ensured atomic.Bool
	var appender *replayInlineAppender
	ensure := func() (map[uint64]page.ValuePtr, *replayInlineAppender, error) {
		ensured.Store(true)
		if appender != nil {
			return nil, appender, nil
		}
		var err error
		appender, err = newReplayInlineAppender(db, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		db.SetValueLogAppender(appender)
		return nil, appender, nil
	}
	defer func() {
		db.SetValueLogAppender(nil)
		if appender != nil {
			_ = appender.close()
		}
	}()

	err = applyCommandWALFrame(db, commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed,
		LSN:           1,
		Kind:          kind,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormat(60000 + kindOffset),
		Payload:       []byte{1},
	}, nil, nil, nil, ensure)
	if err != nil {
		t.Fatalf("applyCommandWALFrame: %v", err)
	}
	if !ensured.Load() {
		t.Fatal("registered command replay did not install replay log support")
	}
	if !handlerCalled.Load() {
		t.Fatal("registered command replay handler was not called")
	}
}

func TestCommandWALInstalledAppendersUseSplitValueAndLeafDirs(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{
		Dir:                    dir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appender := db.currentValueLogAppender()
	if appender == nil {
		t.Fatal("value-log appender was not installed")
	}
	if db.leafPageLog == nil {
		t.Fatal("leaf-page log was not installed")
	}
	if _, err := db.AppendValueLogValues([][]byte{[]byte(strings.Repeat("row-value-", 64))}); err != nil {
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if _, err := db.leafPageLog.AppendLeafPage(bytes.Repeat([]byte("l"), page.PageSize)); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := appender.Flush(); err != nil {
		t.Fatalf("value appender Flush: %v", err)
	}
	if err := db.leafPageLog.Flush(); err != nil {
		t.Fatalf("leaf page log Flush: %v", err)
	}

	valuePath, valueFileID, ok := appender.CurrentValueLogSegment()
	if !ok {
		t.Fatal("value appender did not report current segment")
	}
	leafPath, leafFileID, ok := db.currentLeafPageLogSegment()
	if !ok {
		t.Fatal("leaf page log did not report current segment")
	}
	if got, want := filepath.Dir(valuePath), ValueLogDirPath(dir); got != want {
		t.Fatalf("value appender path dir=%q want %q", got, want)
	}
	if got, want := filepath.Dir(leafPath), LeafLogDirPath(dir); got != want {
		t.Fatalf("leaf page log path dir=%q want %q", got, want)
	}
	if valueFileID == leafFileID {
		t.Fatalf("value and leaf segments share file id %d", valueFileID)
	}
}

func TestReplayInlineLeafPageLogCurrentSegmentNilAppenderM12A(t *testing.T) {
	path, fileID, ok := (replayInlineLeafPageLog{}).CurrentValueLogSegment()
	if ok || path != "" || fileID != 0 {
		t.Fatalf("CurrentValueLogSegment with nil appender = (%q, %d, %v), want empty false", path, fileID, ok)
	}
}

func TestCommandWALRegisteredReplayHandlerCanOptOutOfReplayLogSupport(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer func() { _ = db.Close() }()

	kindOffset := testRegisteredReplayHandlerKind.Add(1)
	if kindOffset > 5000 {
		t.Fatalf("test replay handler kind offset exhausted: %d", kindOffset)
	}
	kind := commitlog.CommandKind(60000 + kindOffset)
	var handlerCalled atomic.Bool
	RegisterCommandWALReplayHandlerWithOptions(kind, func(db *DB, env commitlog.CommandEnvelope) error {
		handlerCalled.Store(true)
		return nil
	}, CommandWALReplayHandlerOptions{})

	var ensured atomic.Bool
	ensure := func() (map[uint64]page.ValuePtr, *replayInlineAppender, error) {
		ensured.Store(true)
		return nil, nil, nil
	}
	if err := applyCommandWALFrame(db, commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed,
		LSN:           1,
		Kind:          kind,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatNativeWireDeterministic,
	}, nil, nil, nil, ensure); err != nil {
		t.Fatalf("applyCommandWALFrame: %v", err)
	}
	if !handlerCalled.Load() {
		t.Fatal("registered replay handler was not called")
	}
	if ensured.Load() {
		t.Fatal("replay log support was installed for opt-out handler")
	}
}

func TestCommandWALRegisteredReplayFrameRestoresStateOnSuccessM10C(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.commandWALReplayLSN.Store(41)
	db.commandWALReplayToken.Store(42)

	env := commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed, LSN: 77}
	err = db.applyRegisteredCommandWALFrame(env, commandWALReplayHandlerRegistration{
		handler: func(db *DB, env commitlog.CommandEnvelope) error {
			if got := db.commandWALReplayLSN.Load(); got != env.LSN {
				t.Fatalf("active replay LSN=%d, want %d", got, env.LSN)
			}
			if got := db.commandWALReplayToken.Load(); got == 0 || got == 42 {
				t.Fatalf("active replay token=%d, want fresh non-zero token", got)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("applyRegisteredCommandWALFrame: %v", err)
	}
	if got := db.commandWALReplayLSN.Load(); got != 41 {
		t.Fatalf("restored replay LSN=%d, want 41", got)
	}
	if got := db.commandWALReplayToken.Load(); got != 42 {
		t.Fatalf("restored replay token=%d, want 42", got)
	}
}

func TestCommandWALRegisteredReplayFrameRestoresStateOnPanicM10C(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.commandWALReplayLSN.Store(41)
	db.commandWALReplayToken.Store(42)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("applyRegisteredCommandWALFrame did not repanic")
		}
		if got := db.commandWALReplayLSN.Load(); got != 41 {
			t.Fatalf("restored replay LSN after panic=%d, want 41", got)
		}
		if got := db.commandWALReplayToken.Load(); got != 42 {
			t.Fatalf("restored replay token after panic=%d, want 42", got)
		}
	}()
	_ = db.applyRegisteredCommandWALFrame(commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed, LSN: 77}, commandWALReplayHandlerRegistration{
		handler: func(db *DB, env commitlog.CommandEnvelope) error {
			panic("replay handler panic")
		},
	})
}

func TestPublishCommandWALNoopRequiresCommandWALEnabled(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed,
		LSN:           1,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}, 0)
	if err := db.PublishCommandWALNoop(intent, false); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("PublishCommandWALNoop error=%v, want ErrCommandWALUnsupported", err)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0", got)
	}
}

func TestCommandWALRejectsUnloggedCommit(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	if err := db.Commit(db.State().RootPageID + 1); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("Commit in command WAL mode error=%v, want ErrCommandWALUnsupported", err)
	}
	if err := db.CompactIndex(); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("CompactIndex in command WAL mode error=%v, want ErrCommandWALUnsupported", err)
	}
	if _, err := db.PublishSystemRootIterator(nil); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("PublishSystemRootIterator in command WAL mode error=%v, want ErrCommandWALUnsupported", err)
	}
}

func TestCommandWALReplayIntentRequestsSynchronousPublish(t *testing.T) {
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{DurabilityClass: commitlog.CommandDurabilityRelaxed,
		LSN:           1,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}, 0)
	if !commandWALIntentPublishSync(intent, false) {
		t.Fatal("replay intent did not request synchronous publish")
	}
	if !commandWALIntentPublishSync(nil, true) {
		t.Fatal("explicit sync publish was not preserved")
	}
	if commandWALIntentPublishSync(nil, false) {
		t.Fatal("nil intent unexpectedly requested sync publish")
	}
}

func TestCommandWALRawEmptyBatchAdvancesAppliedLSNAsNoop(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	before := db.State()
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, nil)

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	after := reopen.State()
	if after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("empty RawKVBatch roots changed: before=%+v after=%+v", before, after)
	}
	if after.AppliedCommandLSN != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", after.AppliedCommandLSN)
	}
}

func TestCommandWALFlushFailurePoisonsOpenHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.testFailCommandWALFlush.Store(true)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set first: %v", err)
	}
	err := b.Write()
	if !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("Write first error=%v, want command WAL flush failpoint", err)
	}
	_ = b.Close()
	db.testFailCommandWALFlush.Store(false)

	retry := db.NewBatch()
	if err := retry.Set([]byte("later"), []byte("value")); err != nil {
		t.Fatalf("Set retry: %v", err)
	}
	err = retry.Write()
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Write retry error=%v, want ErrRecoveryRequired after poisoned command WAL flush", err)
	}
	_ = retry.Close()
}

func TestCommandWALPointAppendReturnsLSNOnFlushFailure(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.testFailCommandWALFlush.Store(true)
	lsn, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("k"), []byte("v"), true)
	if !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("AppendRawKVPointCommandWALTrusted error=%v, want command WAL flush failpoint", err)
	}
	if lsn != 1 {
		t.Fatalf("AppendRawKVPointCommandWALTrusted lsn=%d, want allocated LSN 1 on post-append flush failure", lsn)
	}
	db.testFailCommandWALFlush.Store(false)
	if err := db.FlushCommandWAL(true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("FlushCommandWAL after poison error=%v, want ErrRecoveryRequired", err)
	}
}

func TestAppendCommandWALIntentReturnsLSNOnFlushFailure(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	intent := mustRawKVCommandWALIntent(t, db, "k", "v")
	db.testFailCommandWALFlush.Store(true)
	lsn, err := db.AppendCommandWALIntent(intent, true)
	if !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("AppendCommandWALIntent error=%v, want command WAL flush failpoint", err)
	}
	if lsn != 1 {
		t.Fatalf("AppendCommandWALIntent lsn=%d, want allocated LSN 1 on post-append flush failure", lsn)
	}
	if got := intent.AssignedLSN(); got != lsn {
		t.Fatalf("intent AssignedLSN=%d, want %d", got, lsn)
	}
	db.testFailCommandWALFlush.Store(false)
	if _, err := db.AppendCommandWALIntent(intent, true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("AppendCommandWALIntent retry error=%v, want ErrRecoveryRequired", err)
	}
}

func TestAppendCommandWALIntentPostAppendCutRecordsLSNAndPoisonsHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	intent := mustRawKVCommandWALIntent(t, db, "k", "v")
	wantErr := errors.New("injected post-append cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
			return wantErr
		}
		return nil
	})
	lsn, err := db.AppendCommandWALIntent(intent, true)
	restore()
	if !errors.Is(err, wantErr) {
		t.Fatalf("AppendCommandWALIntent error=%v, want post-append cut", err)
	}
	if lsn != 1 || intent.AssignedLSN() != lsn {
		t.Fatalf("post-append cut lsn=%d assigned=%d, want 1", lsn, intent.AssignedLSN())
	}
	if _, err := db.AppendCommandWALIntent(intent, true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("AppendCommandWALIntent retry error=%v, want ErrRecoveryRequired", err)
	}
}

func TestAppendCommandWALIntentPreFlushCutRecordsLSNAndPoisonsHandle(t *testing.T) {
	tests := []struct {
		name       string
		durability DurabilityMode
		point      durabilitycut.Point
	}{
		{
			name:       "strict-sync",
			durability: DurabilityDurable,
			point:      durabilitycut.BeforeDependencyFileSync,
		},
		{
			name:       "relaxed-flush",
			durability: DurabilityWALOnRelaxed,
			point:      durabilitycut.BeforeUserspaceFlush,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db, err := Open(Options{Dir: dir, Durability: tt.durability})
			if err != nil {
				t.Fatalf("Open command WAL DB: %v", err)
			}
			defer db.Close()

			intent := mustRawKVCommandWALIntent(t, db, "k", "v")
			wantErr := errors.New("injected pre-flush cut")
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == tt.point && event.Resource == durabilitycut.ResourceCommandWAL {
					return wantErr
				}
				return nil
			})
			lsn, err := db.AppendCommandWALIntent(intent, true)
			restore()
			if !errors.Is(err, wantErr) {
				t.Fatalf("AppendCommandWALIntent error=%v, want pre-flush cut", err)
			}
			if lsn != 1 || intent.AssignedLSN() != lsn {
				t.Fatalf("pre-flush cut lsn=%d assigned=%d, want 1", lsn, intent.AssignedLSN())
			}
			if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
			}
			if _, err := db.AppendCommandWALIntent(intent, true); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("AppendCommandWALIntent retry error=%v, want ErrRecoveryRequired", err)
			}
		})
	}
}

func TestCommandWALDirectPostAppendCutRecordsLSNAndPoisonsHandle(t *testing.T) {
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("batch"), Value: []byte("value")}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	tests := []struct {
		name   string
		append func(*DB) (uint64, error)
	}{
		{
			name: "point",
			append: func(db *DB) (uint64, error) {
				return db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("k"), []byte("v"), true)
			},
		},
		{
			name: "encoded-payload",
			append: func(db *DB) (uint64, error) {
				return db.AppendRawKVBatchPayloadCommandWALTrusted(payload, true)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			defer db.Close()

			wantErr := errors.New("injected direct post-append cut")
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
					return wantErr
				}
				return nil
			})
			lsn, err := tt.append(db)
			restore()
			if !errors.Is(err, wantErr) {
				t.Fatalf("append error=%v, want post-append cut", err)
			}
			if lsn != 1 {
				t.Fatalf("post-append cut lsn=%d, want 1", lsn)
			}
			if _, err := tt.append(db); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("append retry error=%v, want ErrRecoveryRequired", err)
			}
		})
	}
}

func TestCommandWALPointAppendFlushesAsyncFrame(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.testFailCommandWALFlush.Store(true)
	lsn, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("k"), []byte("v"), false)
	if !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("AppendRawKVPointCommandWALTrusted async error=%v, want command WAL flush failpoint", err)
	}
	if lsn != 1 {
		t.Fatalf("AppendRawKVPointCommandWALTrusted async lsn=%d, want allocated LSN 1 on post-append flush failure", lsn)
	}
}

func TestCommandWALPublishReadyReportsPoisonedHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.testFailCommandWALFlush.Store(true)
	lsn, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("k"), []byte("v"), false)
	if !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("AppendRawKVPointCommandWALTrusted async error=%v, want command WAL flush failpoint", err)
	}
	if lsn != 1 {
		t.Fatalf("AppendRawKVPointCommandWALTrusted async lsn=%d, want allocated LSN 1 on post-append flush failure", lsn)
	}
	db.testFailCommandWALFlush.Store(false)
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALRawPublishBarriersSkippedAfterPoison(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.testFailCommandWALFlush.Store(true)
	lsn, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("k"), []byte("v"), false)
	if !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("AppendRawKVPointCommandWALTrusted error=%v, want command WAL flush failpoint", err)
	}
	if lsn != 1 {
		t.Fatalf("AppendRawKVPointCommandWALTrusted lsn=%d, want allocated LSN 1 on post-append flush failure", lsn)
	}
	db.testFailCommandWALFlush.Store(false)

	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return nil
	})
	defer unregister()
	_, err = db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("after"), []byte("poison"), false)
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("AppendRawKVPointCommandWALTrusted after poison error=%v, want ErrRecoveryRequired", err)
	}
	if barrierCalled.Load() {
		t.Fatalf("raw publish barrier ran after command WAL handle was poisoned")
	}
}

func TestAppendCommandWALPayloadRunsRawPublishBarriers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	barrierErr := errors.New("raw publish barrier ran")
	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return barrierErr
	})
	defer unregister()

	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{{
		ID:       []byte("u1"),
		Document: []byte(`{"city":"sea"}`),
	}})
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	lsn, err := db.AppendCommandWALPayload(
		commitlog.CommandKindCollectionUpdateBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionUpdateBatchByIDV1,
		payload,
		false,
	)
	if !errors.Is(err, barrierErr) {
		t.Fatalf("AppendCommandWALPayload error=%v, want barrier error", err)
	}
	if lsn != 0 {
		t.Fatalf("AppendCommandWALPayload lsn=%d, want no append after barrier error", lsn)
	}
	if !barrierCalled.Load() {
		t.Fatalf("higher-level command WAL payload append did not run raw publish barrier")
	}
}

func TestPublishCommandWALNoopNilIntentSkipsRawPublishBarriers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	barrierErr := errors.New("raw publish barrier ran")
	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return barrierErr
	})
	defer unregister()

	if err := db.PublishCommandWALNoop(nil, false); err != nil {
		t.Fatalf("PublishCommandWALNoop nil intent: %v", err)
	}
	if barrierCalled.Load() {
		t.Fatalf("nil command WAL no-op publish ran raw publish barrier")
	}
}

func TestAppendCommandWALIntentNoAppendSkipsRawPublishBarriers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	barrierErr := errors.New("raw publish barrier ran")
	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return barrierErr
	})
	defer unregister()

	if lsn, err := db.AppendCommandWALIntent(nil, false); err != nil || lsn != 0 {
		t.Fatalf("AppendCommandWALIntent nil = (%d, %v), want zero nil", lsn, err)
	}
	if barrierCalled.Load() {
		t.Fatalf("nil command WAL intent append ran raw publish barrier")
	}

	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewCommandWALIntent: %v", err)
	}
	lsn, err := db.AppendStagedCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendStagedCommandWALIntent: %v", err)
	}
	barrierCalled.Store(false)
	got, err := db.AppendCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent already-appended: %v", err)
	}
	if got != lsn {
		t.Fatalf("AppendCommandWALIntent already-appended lsn=%d want %d", got, lsn)
	}
	if barrierCalled.Load() {
		t.Fatalf("already-appended command WAL intent ran raw publish barrier")
	}
	if err := db.PublishStagedCommandWALNoop(intent, false); err != nil {
		t.Fatalf("PublishStagedCommandWALNoop: %v", err)
	}
}

func TestCommandWALRawPublishBarrierUnregisterCompacts(t *testing.T) {
	db := &DB{commandWAL: true}
	var calls []int
	unregisterFirst := db.RegisterCommandWALRawPublishBarrier(func() error {
		calls = append(calls, 1)
		return nil
	})
	unregisterSecond := db.RegisterCommandWALRawPublishBarrier(func() error {
		calls = append(calls, 2)
		return nil
	})

	unregisterFirst()
	unregisterFirst()
	if got := len(db.commandWALRawBarriers); got != 1 {
		t.Fatalf("barrier count after unregister=%d, want 1", got)
	}
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		t.Fatalf("runCommandWALRawPublishBarriers: %v", err)
	}
	if len(calls) != 1 || calls[0] != 2 {
		t.Fatalf("barrier calls=%v, want only second barrier", calls)
	}

	unregisterSecond()
	if got := len(db.commandWALRawBarriers); got != 0 {
		t.Fatalf("barrier count after unregister second=%d, want 0", got)
	}
}

func TestCommandWALRawPublishBarrierUnregisterWaitsForInFlight(t *testing.T) {
	db := &DB{commandWAL: true}
	barrierEntered := make(chan struct{})
	releaseBarrier := make(chan struct{})
	unregisterStarted := make(chan struct{})
	unregisterReturned := make(chan struct{})
	runDone := make(chan error, 1)

	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		close(barrierEntered)
		<-releaseBarrier
		return nil
	})
	go func() {
		runDone <- db.runCommandWALRawPublishBarriers()
	}()
	select {
	case <-barrierEntered:
	case <-time.After(time.Second):
		t.Fatalf("raw publish barrier did not start")
	}
	go func() {
		close(unregisterStarted)
		unregister()
		close(unregisterReturned)
	}()
	select {
	case <-unregisterStarted:
	case <-time.After(time.Second):
		t.Fatalf("unregister goroutine did not start")
	}
	select {
	case <-unregisterReturned:
		t.Fatalf("unregister returned before in-flight barrier completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseBarrier)
	select {
	case <-unregisterReturned:
	case <-time.After(time.Second):
		t.Fatalf("unregister did not return after in-flight barrier completed")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runCommandWALRawPublishBarriers: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("runCommandWALRawPublishBarriers did not finish")
	}
	if got := len(db.commandWALRawBarriers); got != 0 {
		t.Fatalf("barrier count after unregister=%d, want 0", got)
	}
}

func TestCommandWALRawPublishBarrierNoopWhenDisabled(t *testing.T) {
	db := &DB{}
	var called atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		called.Store(true)
		return nil
	})
	unregister()
	if got := len(db.commandWALRawBarriers); got != 0 {
		t.Fatalf("barrier count with command WAL disabled=%d, want 0", got)
	}
	db.commandWAL = true
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		t.Fatalf("runCommandWALRawPublishBarriers: %v", err)
	}
	if called.Load() {
		t.Fatalf("disabled command WAL raw publish barrier was registered")
	}
}

func TestCommandWALRawPublishBarrierRejectsCloseHookDrainedDB(t *testing.T) {
	db := &DB{commandWAL: true}
	if err := db.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	var called atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		called.Store(true)
		return nil
	})
	unregister()
	if got := len(db.commandWALRawBarriers); got != 0 {
		t.Fatalf("barrier count after late register=%d, want 0", got)
	}
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		t.Fatalf("runCommandWALRawPublishBarriers: %v", err)
	}
	if called.Load() {
		t.Fatalf("late raw publish barrier ran after close hooks drained")
	}
}

func TestMarkCommandWALIntentRecoveryRequiredNilNoop(t *testing.T) {
	db := &DB{}
	db.MarkCommandWALIntentRecoveryRequired(nil)
	(*DB)(nil).MarkCommandWALIntentRecoveryRequired(nil)
}

func TestCommandWALFinalizeFailurePoisonsOpenHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)

	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set first: %v", err)
	}
	err := b.Write()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Write first error=%v, want finalize commit failpoint", err)
	}
	_ = b.Close()
	db.testFailFinalizeCommit.Store(false)

	if err := db.Commit(db.State().RootPageID); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Commit after poison error=%v, want ErrRecoveryRequired", err)
	}
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ValueLogGC after poison error=%v, want ErrRecoveryRequired", err)
	}
	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ValueLogRewriteOnline after poison error=%v, want ErrRecoveryRequired", err)
	}
	if _, err := db.CompactStorage(context.Background(), CompactStorageOptions{}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CompactStorage after poison error=%v, want ErrRecoveryRequired", err)
	}

	retry := db.NewBatch()
	if err := retry.Set([]byte("later"), []byte("value")); err != nil {
		t.Fatalf("Set retry: %v", err)
	}
	err = retry.Write()
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Write retry error=%v, want ErrRecoveryRequired after poisoned command WAL finalize", err)
	}
	_ = retry.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close poisoned db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got, err := reopen.Get([]byte("later")); err != nil || got != nil {
		t.Fatalf("Get(later)=%q err=%v, want missing retry mutation", got, err)
	}
}

func TestCommandWALRecoveryCrashDuringReplayResumesFromAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("a"), Value: []byte("1")}})
	writeCommandWALRawKVFrame(t, dir, 2, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("b"), Value: []byte("2")}})

	_, err := Open(Options{Dir: dir, testCommandWALRecoveryFailAfterLSN: 1})
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Open with recovery failpoint error=%v, want failpoint", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "a", "1")
	assertDBValue(t, reopen, "b", "2")
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALRIDFencePreservedForRawKVBatch(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 7, []byte("large-value"))
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("k"), RID: 7}})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "large-value")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALPointerBatchReplaysThroughRIDFence(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	largeValue := strings.Repeat("large-value-", 1024)
	ptr := writeValueLogRID(t, dir, 17, []byte(largeValue))

	db = openCommandWALDB(t, dir)
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("WriteSync error=%v, want failpoint", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close crashed db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", largeValue)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

type commandWALExternalRefSyncTestAppender struct {
	t       *testing.T
	fileID  uint32
	syncs   atomic.Int32
	flushes atomic.Int32
}

func (a *commandWALExternalRefSyncTestAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	// AppendValues must not be called in external-ref flush tests; panic
	// rather than return a sentinel so misuse fails loudly.
	if a.t != nil {
		a.t.Fatal("AppendValues must not be called during flushCommandWALExternalRefs")
	}
	panic("AppendValues must not be called during flushCommandWALExternalRefs")
}

func (a *commandWALExternalRefSyncTestAppender) Flush() error {
	a.flushes.Add(1)
	return nil
}

func (a *commandWALExternalRefSyncTestAppender) Sync() error {
	a.syncs.Add(1)
	return nil
}

func (a *commandWALExternalRefSyncTestAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", a.fileID, true
}

type commandWALExternalRefLaneFlushTestAppender struct {
	t               *testing.T
	fileIDs         []uint32
	sync            bool
	externalFlushes atomic.Int32
	syncs           atomic.Int32
	flushes         atomic.Int32
}

func (a *commandWALExternalRefLaneFlushTestAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	if a.t != nil {
		a.t.Fatal("AppendValues must not be called during flushCommandWALExternalRefs")
	}
	panic("AppendValues must not be called during flushCommandWALExternalRefs")
}

func (a *commandWALExternalRefLaneFlushTestAppender) Flush() error {
	a.flushes.Add(1)
	return nil
}

func (a *commandWALExternalRefLaneFlushTestAppender) Sync() error {
	a.syncs.Add(1)
	return nil
}

func (a *commandWALExternalRefLaneFlushTestAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}

func (a *commandWALExternalRefLaneFlushTestAppender) FlushValueLogExternalRefs(fileIDs []uint32, sync bool) error {
	a.externalFlushes.Add(1)
	a.sync = sync
	a.fileIDs = append(a.fileIDs[:0], fileIDs...)
	return nil
}

// TestCommandWALExternalRefFlushDoesNotDoubleSyncActiveSegment verifies that
// flushCommandWALExternalRefs does not sync the active appender segment a
// second time via the per-fileID loop. With sync=true, exactly one Sync call
// (from the appender block) should occur; the per-fileID loop skips fileID==17
// because it equals activeFileID.
//
// With sync=false, exactly one Flush call (from the appender block) should
// occur, and the per-fileID sync loop is skipped entirely.
func TestCommandWALExternalRefFlushDoesNotDoubleSyncActiveSegment(t *testing.T) {
	t.Run("sync=true", func(t *testing.T) {
		dir := t.TempDir()
		enableCommandWALFormat(t, dir)
		db := openCommandWALDB(t, dir)
		defer db.Close()

		appender := &commandWALExternalRefSyncTestAppender{t: t, fileID: 17}
		db.SetValueLogAppender(appender)
		if err := db.flushCommandWALExternalRefs(true, []uint32{17}); err != nil {
			t.Fatalf("flushCommandWALExternalRefs active segment: %v", err)
		}
		if appender.syncs.Load() != 1 {
			t.Fatalf("appender syncs=%d, want 1", appender.syncs.Load())
		}
		if appender.flushes.Load() != 0 {
			t.Fatalf("appender flushes=%d, want 0 for sync=true", appender.flushes.Load())
		}
	})

	t.Run("sync=false", func(t *testing.T) {
		dir := t.TempDir()
		enableCommandWALFormat(t, dir)
		db := openCommandWALDB(t, dir)
		defer db.Close()

		appender := &commandWALExternalRefSyncTestAppender{t: t, fileID: 17}
		db.SetValueLogAppender(appender)
		// sync=false: only Flush should be called (no per-fileID Sync loop).
		if err := db.flushCommandWALExternalRefs(false, []uint32{17}); err != nil {
			t.Fatalf("flushCommandWALExternalRefs sync=false: %v", err)
		}
		if appender.flushes.Load() != 1 {
			t.Fatalf("appender flushes=%d, want 1 for sync=false", appender.flushes.Load())
		}
		if appender.syncs.Load() != 0 {
			t.Fatalf("appender syncs=%d, want 0 for sync=false", appender.syncs.Load())
		}
	})
}

func TestCommandWALExternalRefFlushUsesReferencedLaneFlusher(t *testing.T) {
	fileIDs := []uint32{17, 18}
	for _, tc := range []struct {
		name string
		sync bool
	}{
		{name: "sync=false", sync: false},
		{name: "sync=true", sync: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{}
			appender := &commandWALExternalRefLaneFlushTestAppender{t: t}
			db.SetValueLogAppender(appender)
			if err := db.flushCommandWALExternalRefs(tc.sync, fileIDs); err != nil {
				t.Fatalf("flushCommandWALExternalRefs referenced lanes: %v", err)
			}
			if appender.externalFlushes.Load() != 1 {
				t.Fatalf("external flushes=%d, want 1", appender.externalFlushes.Load())
			}
			if appender.sync != tc.sync {
				t.Fatalf("external flush sync=%v, want %v", appender.sync, tc.sync)
			}
			if len(appender.fileIDs) != len(fileIDs) {
				t.Fatalf("external flush fileIDs=%v, want %v", appender.fileIDs, fileIDs)
			}
			for i := range fileIDs {
				if appender.fileIDs[i] != fileIDs[i] {
					t.Fatalf("external flush fileIDs=%v, want %v", appender.fileIDs, fileIDs)
				}
			}
			if appender.flushes.Load() != 0 {
				t.Fatalf("appender flushes=%d, want 0 when referenced-lane flusher is available", appender.flushes.Load())
			}
			if appender.syncs.Load() != 0 {
				t.Fatalf("appender syncs=%d, want 0 when referenced-lane flusher is available", appender.syncs.Load())
			}
		})
	}
}

func TestCommandWALMissingRIDFenceFailsRecovery(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("k"), RID: 99}})

	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, ErrCommandWALMissingValueLogRID) {
		t.Fatalf("Open error=%v, want missing rid recovery failure", err)
	}
}

func TestCommandWALLaterDurableMissingRIDWinsOverRelaxedRepairBoundary(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		name := "writable"
		if readOnly {
			name = "read-only"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			if got := db.State().AppliedCommandLSN; got != 0 {
				t.Fatalf("bootstrap AppliedCommandLSN=%d, want 0", got)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close bootstrap db: %v", err)
			}
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("relaxed"), RID: 99}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 2, 2, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("durable"), RID: 100}})
			paths := []string{
				filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1)),
				filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2)),
			}
			before := make([][]byte, len(paths))
			for i, path := range paths {
				var err error
				before[i], err = os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile before open: %v", err)
				}
			}
			var err error
			if readOnly {
				_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
			} else {
				_, err = Open(Options{Dir: dir})
			}
			if !errors.Is(err, ErrCommandWALMissingValueLogRID) {
				t.Fatalf("Open error=%v, want ErrCommandWALMissingValueLogRID", err)
			}
			if !strings.Contains(err.Error(), "lsn=2 rid=100") {
				t.Fatalf("Open error=%v, want deterministic later durable diagnostic", err)
			}
			for i, path := range paths {
				after, readErr := os.ReadFile(path)
				if readErr != nil {
					t.Fatalf("ReadFile after open: %v", readErr)
				}
				if !bytes.Equal(after, before[i]) {
					t.Fatalf("Open mutated command WAL segment %s", filepath.Base(path))
				}
			}
		})
	}
}

func TestCommandWALV1OpenReturnsPublicRebuildRequired(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	path := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 16 {
		t.Fatalf("command WAL segment len=%d, want physical header", len(raw))
	}
	binary.LittleEndian.PutUint16(raw[12:14], 1)
	binary.LittleEndian.PutUint16(raw[14:16], 1)
	binary.LittleEndian.PutUint32(raw[4:8], crc.Checksum(raw[8:]))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrCommandWALRebuildRequired) || !errors.Is(err, commitlog.ErrCommandWALUnsupportedVersion) {
		t.Fatalf("Open V1 error=%v, want public rebuild-required and internal unsupported-version causes", err)
	}
}

func TestCommandWALRelaxedMissingRIDAppliesPrefixRepairsSuffixAndContinues(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 1, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("prefix"), Value: []byte("kept")}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 2, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 2, 3, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix"), Value: []byte("discarded")}})
	// A torn active segment in another lane has no decodable LSN. The repair
	// planner must still remove it as part of the suffix.
	tornPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(1, 1))
	if err := os.WriteFile(tornPath, []byte{1, 2, 3}, 0o600); err != nil {
		t.Fatalf("write torn later segment: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "prefix", "kept")
	for _, key := range []string{"missing", "suffix"} {
		if got, err := reopen.Get([]byte(key)); err != nil || got != nil {
			t.Fatalf("Get(%q)=%q err=%v, want discarded", key, got, err)
		}
	}
	stats := reopen.Stats()
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.recovery.first_discarded_lsn"); got != 2 {
		t.Fatalf("first discarded lsn=%d, want 2", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.recovery.discarded_frames"); got != 2 {
		t.Fatalf("discarded frames=%d, want 2", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.recovery.missing_rids"); got != 1 {
		t.Fatalf("missing rids=%d, want 1", got)
	}
	reopenedSegmentPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2))
	if info, err := os.Stat(reopenedSegmentPath); err != nil || info.Size() != 0 {
		t.Fatalf("replacement active segment stat=(%v,%v), want empty replacement", info, err)
	}
	if _, err := os.Stat(tornPath); !os.IsNotExist(err) {
		t.Fatalf("torn later suffix segment stat error=%v, want removed", err)
	}
	b := reopen.NewBatch()
	if err := b.Set([]byte("next"), []byte("value")); err != nil {
		t.Fatalf("Set next: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync next: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after next append=%d, want 2", got)
	}
}

func TestCommandWALRelaxedMissingRIDReadOnlyRequiresRecoveryWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
	path := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile before read-only open: %v", err)
	}

	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	var recoveryErr *CommandWALRecoveryRequiredError
	if !errors.Is(err, ErrRecoveryRequired) || !errors.As(err, &recoveryErr) {
		t.Fatalf("read-only open error=%v, want typed ErrRecoveryRequired", err)
	}
	if recoveryErr.Diagnostic.FirstDiscardedLSN != 1 {
		t.Fatalf("read-only first discarded lsn=%d, want 1", recoveryErr.Diagnostic.FirstDiscardedLSN)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("ReadFile after read-only open: %v", readErr)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("read-only recovery planner mutated command WAL")
	}
}

func TestCommandWALRelaxedMissingRIDAllowsGapInsideDiscardedSuffix(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 1, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("prefix"), Value: []byte("kept")}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 2, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
	// Simulate another lane persisting LSN 4 after LSN 3 was lost. The gap is
	// inside the suffix already made incomplete by the relaxed LSN 2 frame.
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 4, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix"), Value: []byte("discarded")}})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "prefix", "kept")
	for _, key := range []string{"missing", "suffix"} {
		if got, err := reopen.Get([]byte(key)); err != nil || got != nil {
			t.Fatalf("Get(%q)=%q err=%v, want discarded", key, got, err)
		}
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after repair=%d, want 1", got)
	}
	if got := commandWALTestStatUint64(t, reopen.Stats(), "treedb.command_wal.recovery.first_discarded_lsn"); got != 2 {
		t.Fatalf("first discarded lsn=%d, want 2", got)
	}
}

func TestRepairIncompleteCommandWALTailIsCrashIdempotent(t *testing.T) {
	dir := t.TempDir()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	anchorPath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	laterPath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 2))
	if err := os.WriteFile(anchorPath, bytes.Repeat([]byte{1}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(laterPath, bytes.Repeat([]byte{2}, 16), 0o600); err != nil {
		t.Fatal(err)
	}
	anchor := logSegment{lane: 0, seq: 1, path: anchorPath, size: 32}
	later := logSegment{lane: 0, seq: 2, path: laterPath, size: 16}
	frames := []commandWALReplayFrame{{
		env:         commitlog.CommandEnvelope{LSN: 1, DurabilityClass: commitlog.CommandDurabilityRelaxed},
		segment:     anchor,
		startOffset: 8,
		endOffset:   32,
	}}
	diagnostic := CommandWALRecoveryDiagnostic{FirstDiscardedLSN: 1}
	originalSyncDir := syncDirFn
	failOnce := true
	syncDirFn = func(path string) error {
		if failOnce {
			failOnce = false
			return errors.New("injected directory sync failure")
		}
		return originalSyncDir(path)
	}
	_, err := repairIncompleteCommandWALTail(dir, []logSegment{anchor, later}, frames, 1, diagnostic)
	syncDirFn = originalSyncDir
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("first repair error=%v, want ErrRecoveryRequired", err)
	}
	if _, err := os.Stat(anchorPath); err != nil {
		t.Fatalf("anchor removed after interrupted repair: %v", err)
	}
	if _, err := os.Stat(laterPath); !os.IsNotExist(err) {
		t.Fatalf("later segment stat after interrupted repair=%v, want removed", err)
	}
	repaired, err := repairIncompleteCommandWALTail(dir, []logSegment{anchor, later}, frames, 1, diagnostic)
	if err != nil {
		t.Fatalf("second idempotent repair: %v", err)
	}
	if !repaired.TruncationCompleted || !repaired.DirectorySyncCompleted {
		t.Fatalf("repair diagnostic=%+v, want completed", repaired)
	}
	info, err := os.Stat(anchorPath)
	if err != nil {
		t.Fatalf("Stat anchor: %v", err)
	}
	if info.Size() != 8 {
		t.Fatalf("anchor size=%d, want truncation offset 8", info.Size())
	}
}

func TestCommandWALRepairTruncatesLogicalTailBeforeEarlierFrames(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 2, 2, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-2"), Value: []byte("discarded")}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 3, 3, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-3"), Value: []byte("discarded")}})

	walDir := WALDirPath(dir)
	anchorPath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	middlePath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 2))
	tailPath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 3))
	cutErr := errors.New("injected post-tail-truncation cut")
	var firstSynced string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.AfterDependencyFileSync || event.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		firstSynced = event.Path
		return cutErr
	})
	_, err := Open(Options{Dir: dir})
	restore()
	if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("first Open error=%v, want injected ErrRecoveryRequired", err)
	}
	if firstSynced != tailPath {
		t.Fatalf("first synced segment=%q, want logical tail %q", firstSynced, tailPath)
	}
	if info, err := os.Stat(tailPath); err != nil || info.Size() != 0 {
		t.Fatalf("tail segment stat after cut=(%v,%v), want empty", info, err)
	}
	for _, path := range []string{anchorPath, middlePath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("surviving segment %s stat after cut: %v", filepath.Base(path), err)
		}
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	for _, key := range []string{"missing", "suffix-2", "suffix-3"} {
		if got, err := reopen.Get([]byte(key)); err != nil || got != nil {
			t.Fatalf("Get(%q)=%q err=%v, want discarded after retry", key, got, err)
		}
	}
	if got := commandWALTestStatUint64(t, reopen.Stats(), "treedb.command_wal.recovery.first_discarded_lsn"); got != 1 {
		t.Fatalf("first discarded lsn after retry=%d, want 1", got)
	}
}

func TestCommandWALRepairInterleavedLanesIsCrashIdempotent(t *testing.T) {
	for cutAfter := 1; cutAfter <= 5; cutAfter++ {
		t.Run(fmt.Sprintf("cut-after-sync-%d", cutAfter), func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			if err := db.Close(); err != nil {
				t.Fatalf("Close bootstrap db: %v", err)
			}
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 2, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-2"), Value: []byte("discarded")}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 2, 1, 3, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-3"), Value: []byte("discarded")}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 4, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-4"), Value: []byte("discarded")}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 2, 1, 5, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-5"), Value: []byte("discarded")}})
			suffixPaths := []string{
				filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(1, 1)),
				filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(2, 1)),
			}

			cutErr := errors.New("injected interleaved-lane repair cut")
			syncs := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point != durabilitycut.AfterDependencyFileSync || event.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				syncs++
				if syncs == cutAfter {
					return cutErr
				}
				return nil
			})
			_, err := Open(Options{Dir: dir})
			restore()
			if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("first Open error=%v, want injected ErrRecoveryRequired", err)
			}

			reopen := openCommandWALDB(t, dir)
			defer reopen.Close()
			for _, key := range []string{"missing", "suffix-2", "suffix-3", "suffix-4", "suffix-5"} {
				if got, err := reopen.Get([]byte(key)); err != nil || got != nil {
					t.Fatalf("Get(%q)=%q err=%v, want discarded after retry", key, got, err)
				}
			}
			if got := reopen.State().AppliedCommandLSN; got != 0 {
				t.Fatalf("AppliedCommandLSN after repair=%d, want 0", got)
			}
			for _, path := range suffixPaths {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("suffix segment %s stat after retry=%v, want removed", filepath.Base(path), err)
				}
			}
		})
	}
}

func TestCommandWALRepairNamespaceCutsRetainAnchorUntilDirectorySync(t *testing.T) {
	tests := []struct {
		name  string
		point durabilitycut.Point
	}{
		{name: "before-suffix-unlink", point: durabilitycut.BeforeWALOrAssetUnlink},
		{name: "after-suffix-unlink", point: durabilitycut.AfterWALOrAssetUnlink},
		{name: "before-directory-sync", point: durabilitycut.BeforeDeletionDirectorySync},
		{name: "after-directory-sync", point: durabilitycut.AfterDeletionDirectorySync},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			if err := db.Close(); err != nil {
				t.Fatalf("Close bootstrap db: %v", err)
			}
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 2, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-2"), Value: []byte("discarded")}})
			writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 2, 1, 3, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix-3"), Value: []byte("discarded")}})
			walDir := WALDirPath(dir)
			anchorPath := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
			suffixPaths := []string{
				filepath.Join(walDir, commitlog.CommandSegmentName(1, 1)),
				filepath.Join(walDir, commitlog.CommandSegmentName(2, 1)),
			}

			cutErr := errors.New("injected namespace repair cut")
			triggered := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !triggered && event.Point == test.point && event.Resource == durabilitycut.ResourceCommandWAL {
					triggered = true
					return cutErr
				}
				return nil
			})
			_, err := Open(Options{Dir: dir})
			restore()
			if !triggered {
				t.Fatalf("repair did not reach cut point %s", test.point)
			}
			if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("first Open error=%v, want injected ErrRecoveryRequired", err)
			}
			if info, err := os.Stat(anchorPath); err != nil || info.Size() == 0 {
				t.Fatalf("anchor stat after cut=(%v,%v), want intact repair marker", info, err)
			}

			reopen := openCommandWALDB(t, dir)
			defer reopen.Close()
			for _, path := range suffixPaths {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("suffix segment %s stat after retry=%v, want removed", filepath.Base(path), err)
				}
			}
			if info, err := os.Stat(anchorPath); err != nil || info.Size() != 0 {
				t.Fatalf("anchor stat after completed retry=(%v,%v), want empty retained segment", info, err)
			}
			if got := commandWALTestStatUint64(t, reopen.Stats(), "treedb.command_wal.recovery.first_discarded_lsn"); got != 1 {
				t.Fatalf("first discarded lsn after retry=%d, want 1", got)
			}
		})
	}
}

func TestCommandWALRepairTruncatesCrossLaneMixedSegmentBeforeAnchor(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 1, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("prefix"), Value: []byte("kept")}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 0, 1, 2, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("missing"), RID: 99}})
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, 1, 1, 3, commitlog.CommandDurabilityDurable, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("suffix"), Value: []byte("discarded")}})
	segments, err := listRecoverySegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readCommandWALReplayFrames(segments, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 {
		t.Fatalf("replay frames=%d, want 3", len(frames))
	}
	anchorPath := frames[1].segment.path
	mixedPath := frames[0].segment.path
	anchorBefore, err := os.Stat(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected after non-anchor truncation sync")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDependencyFileSync && event.Resource == durabilitycut.ResourceCommandWAL && event.Path == mixedPath {
			return injected
		}
		return nil
	})
	_, err = Open(Options{Dir: dir})
	restore()
	if !errors.Is(err, injected) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("first Open error=%v, want injected ErrRecoveryRequired", err)
	}
	anchorAfter, err := os.Stat(anchorPath)
	if err != nil {
		t.Fatalf("Stat anchor after cut: %v", err)
	}
	if anchorAfter.Size() != anchorBefore.Size() {
		t.Fatalf("anchor size after non-anchor cut=%d, want unchanged %d", anchorAfter.Size(), anchorBefore.Size())
	}
	mixedAfter, err := os.Stat(mixedPath)
	if err != nil {
		t.Fatalf("Stat mixed segment after cut: %v", err)
	}
	if mixedAfter.Size() != frames[0].endOffset {
		t.Fatalf("mixed segment size after cut=%d, want prefix end %d", mixedAfter.Size(), frames[0].endOffset)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "prefix", "kept")
	for _, key := range []string{"missing", "suffix"} {
		if got, err := reopen.Get([]byte(key)); err != nil || got != nil {
			t.Fatalf("Get(%q)=%q err=%v, want discarded", key, got, err)
		}
	}
}

func TestCommandWALExternalRefFlushRequiresAppender(t *testing.T) {
	db := &DB{}
	if err := db.flushCommandWALExternalRefs(true, nil); !errors.Is(err, ErrValueLogAppenderUnavailable) {
		t.Fatalf("flushCommandWALExternalRefs error=%v, want ErrValueLogAppenderUnavailable", err)
	}
}

func TestCommandWALIdempotentSkipRequiresDigestProof(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})

	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, ErrCommandWALAppliedLSNNonContig) {
		t.Fatalf("Open error=%v, want ErrCommandWALAppliedLSNNonContig instead of skipping missing LSN", err)
	}
}

func TestCommandWALExistingRawReplayTestsMappedToRawKVBatch(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("a"), Value: []byte("1")},
		{Op: commitlog.RawKVOpSet, Key: []byte("b"), Value: []byte("2")},
		{Op: commitlog.RawKVOpDelete, Key: []byte("a")},
		{Op: commitlog.RawKVOpSet, Key: []byte("same"), Value: []byte("old")},
		{Op: commitlog.RawKVOpDelete, Key: []byte("same")},
		{Op: commitlog.RawKVOpSet, Key: []byte("same"), Value: []byte("final")},
	})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	if got, err := reopen.Get([]byte("a")); err != nil || got != nil {
		t.Fatalf("Get(a)=%q err=%v, want missing after typed RawKVBatch delete replay", got, err)
	}
	assertDBValue(t, reopen, "b", "2")
	assertDBValue(t, reopen, "same", "final")
}

func TestCommandWALExistingRIDFenceTestsMappedToExternalRefFence(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 1, []byte("one"))
	writeValueLogRID(t, dir, 2, []byte("two"))
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSetRID, Key: []byte("a"), RID: 1},
		{Op: commitlog.RawKVOpSetRID, Key: []byte("b"), RID: 2},
	})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "a", "one")
	assertDBValue(t, reopen, "b", "two")
}

func TestCommandWALRecoveryPreservesEntryRevision(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	largeValue := strings.Repeat("large-value-", 1024)
	ptr := writeValueLogRID(t, dir, 31, []byte(largeValue))

	db = openCommandWALDB(t, dir)
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch().(*Batch)
	if err := b.SetWithRevision([]byte("inline"), []byte("value"), page.EntryRevision(41)); err != nil {
		t.Fatalf("SetWithRevision inline: %v", err)
	}
	if err := b.SetPointerWithRevision([]byte("ptr"), ptr, page.EntryRevision(42)); err != nil {
		t.Fatalf("SetPointerWithRevision ptr: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("crash batch WriteSync error=%v, want failpoint", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close crashed db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	inlineValue, inlineRevision, err := reopen.GetVersioned([]byte("inline"))
	if err != nil {
		t.Fatalf("GetVersioned inline: %v", err)
	}
	if !bytes.Equal(inlineValue, []byte("value")) || inlineRevision != page.EntryRevision(41) {
		t.Fatalf("GetVersioned inline=(%q,%d), want (value,41)", inlineValue, inlineRevision)
	}
	ptrValue, ptrRevision, err := reopen.GetVersioned([]byte("ptr"))
	if err != nil {
		t.Fatalf("GetVersioned ptr: %v", err)
	}
	if !bytes.Equal(ptrValue, []byte(largeValue)) || ptrRevision != page.EntryRevision(42) {
		t.Fatalf("GetVersioned ptr=(len=%d,%d), want (len=%d,42)", len(ptrValue), ptrRevision, len(largeValue))
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if got := reopen.State().MaxEntryRevision; got < page.EntryRevision(42) {
		t.Fatalf("MaxEntryRevision=%d, want >= 42", got)
	}
}

func TestCommandWALRecoveryAssignsLegacyEntryRevision(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte("legacy"), []byte("value")); err != nil {
		t.Fatalf("Set legacy: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("crash batch WriteSync error=%v, want failpoint", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close crashed db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	value, revision, err := reopen.GetVersioned([]byte("legacy"))
	if err != nil {
		t.Fatalf("GetVersioned legacy: %v", err)
	}
	if !bytes.Equal(value, []byte("value")) || revision == page.LegacyEntryRevision {
		t.Fatalf("GetVersioned legacy=(%q,%d), want (value,non-legacy)", value, revision)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if got := reopen.State().MaxEntryRevision; got < revision {
		t.Fatalf("MaxEntryRevision=%d, want >= assigned revision %d", got, revision)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}

	reopen = openCommandWALDB(t, dir)
	defer reopen.Close()
	value, reopenedRevision, err := reopen.GetVersioned([]byte("legacy"))
	if err != nil {
		t.Fatalf("second reopen GetVersioned legacy: %v", err)
	}
	if !bytes.Equal(value, []byte("value")) || reopenedRevision != revision {
		t.Fatalf("second reopen GetVersioned legacy=(%q,%d), want (value,%d)", value, reopenedRevision, revision)
	}
	if got := reopen.State().MaxEntryRevision; got < revision {
		t.Fatalf("second reopen MaxEntryRevision=%d, want >= assigned revision %d", got, revision)
	}
}

func enableCommandWALFormat(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV2}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
}

func openCommandWALDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open command WAL DB: %v", err)
	}
	return db
}

func assertDBValue(t *testing.T, db *DB, key string, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%q)=%q, want %q", key, got, want)
	}
}

func commandWALSegmentNamesForTest(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(WALDirPath(dir))
	if err != nil {
		t.Fatalf("ReadDir command WAL: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func writeCommandWALRawKVFrame(t testing.TB, dir string, segmentSeq uint64, lsn uint64, ops []commitlog.RawKVOperation) {
	t.Helper()
	writeCommandWALRawKVFrameForLane(t, dir, 0, segmentSeq, lsn, ops)
}

func writeCommandWALRawKVFrameForLane(t testing.TB, dir string, lane int, segmentSeq uint64, lsn uint64, ops []commitlog.RawKVOperation) {
	t.Helper()
	writeCommandWALRawKVFrameForLaneAndDurability(t, dir, lane, segmentSeq, lsn, commitlog.CommandDurabilityDurable, ops)
}

func writeCommandWALRawKVFrameForLaneAndDurability(t testing.TB, dir string, lane int, segmentSeq uint64, lsn uint64, durability commitlog.CommandDurabilityClass, ops []commitlog.RawKVOperation) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(lane, segmentSeq))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(commitlog.CommandEnvelope{DurabilityClass: durability,
		LSN:           lsn,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
		Payload:       payload,
	}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func writeValueLogRID(t testing.TB, dir string, rid uint64, value []byte) page.ValuePtr {
	t.Helper()
	return writeValueLogRIDBatchFrom(t, dir, rid, 1, value)[0]
}
