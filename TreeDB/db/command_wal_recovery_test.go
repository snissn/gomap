package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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

func TestCommandWALCrashAfterFrameBeforeRootPublishRecovers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("WriteSync error=%v, want failpoint", err)
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

func TestCommandWALSetRIDReplayDoesNotNeedInlineAppenderWithoutOuterLeafLog(t *testing.T) {
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSetRID, Key: []byte("ptr-key"), RID: 7},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	frames := []commandWALReplayFrame{{
		env: commitlog.CommandEnvelope{
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

	err = applyRawKVCommandWALFrame(db, commitlog.CommandEnvelope{
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

	err = applyCommandWALFrame(db, commitlog.CommandEnvelope{
		LSN:           1,
		Kind:          kind,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormat(60000 + kindOffset),
		Payload:       []byte{1},
	}, nil, nil, ensure)
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
	if err := applyCommandWALFrame(db, commitlog.CommandEnvelope{
		LSN:           1,
		Kind:          kind,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatNativeWireDeterministic,
	}, nil, nil, ensure); err != nil {
		t.Fatalf("applyCommandWALFrame: %v", err)
	}
	if !handlerCalled.Load() {
		t.Fatal("registered replay handler was not called")
	}
	if ensured.Load() {
		t.Fatal("replay log support was installed for opt-out handler")
	}
}

func TestPublishCommandWALNoopRequiresCommandWALEnabled(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	intent := NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           1,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	})
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
	intent := NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           1,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	})
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

func enableCommandWALFormat(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
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
	if err := w.AppendCommand(commitlog.CommandEnvelope{
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
	valueLogDir := resolveStorageLayout(dir).valueVLogDir
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("MkdirAll value_vlog: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(valueLogDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("New value writer: %v", err)
	}
	ptr, err := w.Append(0, nil, rid, value)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append value log: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close value writer: %v", err)
	}
	return ptr
}
