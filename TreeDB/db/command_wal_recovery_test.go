package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	w, err := commitlog.NewWriter(lane0Path)
	if err != nil {
		t.Fatalf("NewWriter terminal tail: %v", err)
	}
	if err := w.AppendCommand(commitlog.CommandEnvelope{
		Version:         commitlog.CommandFrameVersionV2,
		DurabilityClass: commitlog.CommandDurabilityRelaxed,
		LSN:             3,
		Kind:            commitlog.CommandKindRawKVBatch,
		Scope:           commitlog.CommandScopeRawKV,
		PayloadFormat:   commitlog.PayloadFormatRawKVBatchV1,
		Payload:         payload,
	}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand terminal tail: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close terminal tail writer: %v", err)
	}
	info, err := os.Stat(lane0Path)
	if err != nil {
		t.Fatalf("Stat terminal tail: %v", err)
	}
	if err := os.Truncate(lane0Path, info.Size()-2); err != nil {
		t.Fatalf("Truncate terminal tail: %v", err)
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
	if _, err := os.Stat(coveredPath); err != nil {
		t.Fatalf("covered segment stat after one-root coverage=%v, want retained", err)
	}
	// Publish the same frontier into the other independently recoverable slot.
	// Only then does ordinary cleanup have two-root deletion authority.
	state = reopen.State()
	if err := reopen.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, nil, true); err != nil {
		t.Fatalf("publishCommandWALRoots second slot: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
	cleanupReopen := openCommandWALDB(t, dir)
	stats := make(map[string]string)
	writeCommandWALStats(stats, cleanupReopen)
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.cleanup.proof.capture_epoch"); got == 0 {
		t.Fatalf("cleanup proof capture epoch=%d, want journal-owned recovery cleanup (stats=%#v)", got, stats)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.cleanup.proof.namespace_generation"); got == 0 {
		t.Fatalf("cleanup proof namespace generation=%d, want journal-owned recovery cleanup (stats=%#v)", got, stats)
	}
	if err := cleanupReopen.Close(); err != nil {
		t.Fatalf("Close cleanup reopen: %v", err)
	}
	if _, err := os.Stat(coveredPath); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat error=%v, want removed on clean reopen", err)
	}
	// A later journal lifecycle may advance the active anchor beyond seq=2, but
	// cleanup must always leave an exact active successor in the namespace.
	var commandSegments int
	for _, name := range commandWALSegmentNamesForTest(t, dir) {
		if commitlog.IsCommandSegmentName(name) {
			commandSegments++
		}
	}
	if commandSegments == 0 {
		t.Fatal("cleanup removed every command-WAL segment, want active successor retained")
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

func TestCommandWALReplayAllocatesAbovePendingMaterializedRIDs(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}

	legacyValue := strings.Repeat("legacy-externalized-", 8)
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("legacy"), Value: []byte(legacyValue),
	}})
	writeCommandWALRawKVFrameWithFormat(t, dir, 2, 2, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("materialized"), RID: 1, Value: []byte("exact-rid-one"),
	}})

	recovered, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open mixed V1/V2 recovery: %v", err)
	}
	assertDBValue(t, recovered, "legacy", legacyValue)
	assertDBValue(t, recovered, "materialized", "exact-rid-one")
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close mixed V1/V2 recovery: %v", err)
	}

	reopened, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Reopen mixed V1/V2 recovery: %v", err)
	}
	defer reopened.Close()
	assertDBValue(t, reopened, "legacy", legacyValue)
	assertDBValue(t, reopened, "materialized", "exact-rid-one")

	segments, err := listSegmentsInDir(ValueLogDirPath(dir))
	if err != nil {
		t.Fatalf("list value-log segments: %v", err)
	}
	ridMap, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatalf("scan value-log segments: %v", err)
	}
	if len(ridMap) != 2 {
		t.Fatalf("recovered RID count=%d, want exact RID plus replay allocation", len(ridMap))
	}
	if _, ok := ridMap[1]; !ok {
		t.Fatalf("pending materialized RID 1 missing after replay: %+v", ridMap)
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
	if err := applyCommandWALFrame(db, commitlog.CommandEnvelope{
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

	env := commitlog.CommandEnvelope{LSN: 77}
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
	_ = db.applyRegisteredCommandWALFrame(commitlog.CommandEnvelope{LSN: 77}, commandWALReplayHandlerRegistration{
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
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{
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

	if err := db.ForceCommit(db.State().RootPageID + 1); !errors.Is(err, ErrCommandWALUnsupported) {
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
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{
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

func TestCommandWALIntentResolvedProfileControlsOrdinaryStagedAppendSync(t *testing.T) {
	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	tests := []struct {
		name       string
		profile    DurabilityProfile
		durability DurabilityMode
		wantSyncs  uint64
	}{
		{name: "durable", profile: ProfileCommandWALDurable, durability: DurabilityDurable, wantSyncs: 1},
		{name: "relaxed", profile: ProfileCommandWALRelaxed, durability: DurabilityWALOnRelaxed, wantSyncs: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := SaveFormatConfig(dir, FormatConfig{
				RequiredFeatures:  []string{RequiredFeatureCommandWALV1},
				DurabilityProfile: tc.profile,
			}); err != nil {
				t.Fatalf("SaveFormatConfig: %v", err)
			}
			database, err := Open(Options{
				Dir:             dir,
				CommandWAL:      true,
				Durability:      tc.durability,
				ResolvedProfile: tc.profile,
				ValueLog: ValueLogOptions{
					ReadIntegrity: IntegrityVerify,
				},
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer database.Close()

			intent, err := database.NewCommandWALIntent(
				commitlog.CommandKindRawKVBatch,
				commitlog.CommandScopeRawKV,
				commitlog.PayloadFormatRawKVBatchV1,
				payload,
			)
			if err != nil {
				t.Fatalf("NewCommandWALIntent: %v", err)
			}
			before := commandWALTestStatUint64(t, database.Stats(), "treedb.command_wal.sync.count_total")
			if _, err := database.AppendStagedCommandWALIntent(intent, false); err != nil {
				t.Fatalf("AppendStagedCommandWALIntent: %v", err)
			}
			after := commandWALTestStatUint64(t, database.Stats(), "treedb.command_wal.sync.count_total")
			if got := after - before; got != tc.wantSyncs {
				t.Fatalf("ordinary staged append command WAL syncs=%d want %d", got, tc.wantSyncs)
			}
			if err := database.PublishStagedCommandWALNoop(intent, false); err != nil {
				t.Fatalf("PublishStagedCommandWALNoop: %v", err)
			}
		})
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
	if got := db.Stats()["treedb.command_wal.dependency_debt.entries"]; got != "1" {
		t.Fatalf("post-append cut debt entries=%q, want appended LSN retained", got)
	}
	if _, err := db.AppendCommandWALIntent(intent, true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("AppendCommandWALIntent retry error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishStagedCommandWALNoopSyncFailureRetainsDebtForRetry(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	intent := mustRawKVCommandWALIntent(t, db, "k", "v")
	dependency, err := os.Create(filepath.Join(dir, "staged-sync-dependency"))
	if err != nil {
		t.Fatalf("create staged sync dependency: %v", err)
	}
	defer dependency.Close()
	if err := dependency.Truncate(1); err != nil {
		t.Fatalf("truncate staged sync dependency: %v", err)
	}
	wantErr := errors.New("injected staged sync dependency failure")
	failNextSync := true
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		Kind:           rootpublication.ResourceValueLog,
		LogicalLane:    "test/staged-sync",
		ResourceID:     "dependency",
		Generation:     1,
		DiagnosticPath: "staged-sync-dependency",
		File:           dependency,
		Frontier:       rootpublication.DurableFrontier{Bytes: 1},
		Reachability:   rootpublication.ReachabilityValueLogPointer,
		SyncThrough: func(file *os.File, _ rootpublication.DurableFrontier) error {
			if failNextSync {
				failNextSync = false
				return wantErr
			}
			return file.Sync()
		},
	})
	if err != nil {
		t.Fatalf("NewStableResourceToken: %v", err)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatalf("add staged sync dependency: %v", err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatalf("freeze staged sync dependency: %v", err)
	}
	intent.inner.dependencyResources = resources
	lsn, err := db.AppendStagedCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendStagedCommandWALIntent relaxed: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("staged intent lsn=%d, want 1", lsn)
	}

	err = db.PublishStagedCommandWALNoop(intent, true)
	if !errors.Is(err, wantErr) {
		t.Fatalf("PublishStagedCommandWALNoop error=%v, want injected dependency failure", err)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 after pre-barrier failure", got)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "0" {
		t.Fatalf("durable_wal_lsn=%q, want 0 after pre-barrier failure", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "1" {
		t.Fatalf("pending debt entries=%q, want original staged frame retained", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.retries_total"]; got != "1" {
		t.Fatalf("pending debt retries=%q, want 1 after pre-barrier failure", got)
	}

	if err := db.PublishStagedCommandWALNoop(intent, true); err != nil {
		t.Fatalf("PublishStagedCommandWALNoop retry: %v", err)
	}
	const barrierLSN = uint64(2)
	if got := db.State().AppliedCommandLSN; got != barrierLSN {
		t.Fatalf("AppliedCommandLSN=%d, want durable barrier lsn %d after retry", got, barrierLSN)
	}
	stats = db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "2" {
		t.Fatalf("durable_wal_lsn=%q, want barrier lsn 2 after retry", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries=%q, want 0 after retry", got)
	}
}

func TestAppendCommandWALIntentPreFlushCutRecordsLSNAndPoisonsHandle(t *testing.T) {
	tests := []struct {
		name       string
		durability DurabilityMode
		sync       bool
		point      durabilitycut.Point
	}{
		{
			name:       "strict-sync",
			durability: DurabilityDurable,
			sync:       true,
			point:      durabilitycut.BeforeDependencyFileSync,
		},
		{
			name:       "relaxed-flush",
			durability: DurabilityWALOnRelaxed,
			sync:       false,
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
			lsn, err := db.AppendCommandWALIntent(intent, tt.sync)
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

func TestCommandWALPreparedPublishAdmissionWorkerWinsBeforeQuiescence(t *testing.T) {
	db := &DB{commandWAL: true}
	releaseFirstPrepared, ok := db.TryLockCommandWALPreparedPublish()
	if !ok {
		t.Fatal("prepared publisher did not claim shared admission")
	}
	firstPreparedHeld := true
	defer func() {
		if firstPreparedHeld {
			releaseFirstPrepared()
		}
	}()
	boundaryStarted := make(chan struct{})
	boundaryDone := make(chan struct{})
	go func() {
		close(boundaryStarted)
		unlock := db.lockCommandWALQuiescentAdmission()
		unlock()
		close(boundaryDone)
	}()
	select {
	case <-boundaryStarted:
	case <-time.After(time.Second):
		t.Fatal("quiescent boundary did not start")
	}

	// sync.RWMutex has no queued-writer hook. Once the writer is queued, its
	// writer preference makes TryRLock fail; that proves a second prepared
	// publisher cannot barge ahead of the quiescent boundary.
	deadline := time.Now().Add(time.Second)
	for {
		if releaseSecondPrepared, ok := db.TryLockCommandWALPreparedPublish(); !ok {
			break
		} else {
			releaseSecondPrepared()
		}
		if time.Now().After(deadline) {
			t.Fatal("quiescent boundary did not queue behind the prepared publisher")
		}
		runtime.Gosched()
	}

	releaseFirstPrepared()
	firstPreparedHeld = false
	select {
	case <-boundaryDone:
	case <-time.After(time.Second):
		t.Fatal("quiescent boundary did not finish after prepared publisher released admission")
	}
}

func TestCommandWALPreparedPublishAdmissionBoundaryWins(t *testing.T) {
	db := &DB{commandWAL: true}
	unlockBoundary := db.lockCommandWALQuiescentAdmission()
	defer unlockBoundary()
	if unlockPrepared, ok := db.TryLockCommandWALPreparedPublish(); ok {
		unlockPrepared()
		t.Fatal("prepared publisher claimed admission while quiescent boundary was held")
	}
}

func TestCommandWALStagingDoesNotWaitForQuiescentAdmission(t *testing.T) {
	db := &DB{commandWAL: true}
	unlockBoundary := db.lockCommandWALQuiescentAdmission()
	defer unlockBoundary()
	staged := make(chan struct{})
	go func() {
		unlock := db.LockCommandWALStaging()
		unlock()
		close(staged)
	}()
	select {
	case <-staged:
	case <-time.After(time.Second):
		t.Fatal("ordinary command WAL staging waited for quiescent admission")
	}
}

func TestCheckpointRunsRawPublishBarrierBeforeRawAdmission(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return nil
	})
	defer unregister()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !barrierCalled.Load() {
		t.Fatal("Checkpoint did not run raw publish barrier before its raw boundary")
	}
}

func TestCloseWaitsForTeardownPinnedQuiescentAdmission(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	barrierEntered := make(chan struct{})
	releaseBarrier := make(chan struct{})
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		close(barrierEntered)
		<-releaseBarrier
		return nil
	})
	defer unregister()
	publisherDone := make(chan error, 1)
	db.teardownMu.RLock()
	go func() {
		unlock, err := db.lockCommandWALPublishWithBarriersTeardownPinned()
		if err == nil {
			unlock()
		}
		db.teardownMu.RUnlock()
		publisherDone <- err
	}()
	select {
	case <-barrierEntered:
	case <-time.After(time.Second):
		t.Fatal("teardown-pinned publisher did not enter its raw barrier")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before teardown-pinned publisher released admission: %v", err)
	default:
	}
	close(releaseBarrier)
	select {
	case err := <-publisherDone:
		if err != nil {
			t.Fatalf("teardown-pinned publisher: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("teardown-pinned publisher did not finish")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after teardown-pinned publisher released admission")
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

func TestAppendCommandWALIntentExistingRelaxedSyncRunsRawPublishBarriers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	intent := mustRawKVCommandWALIntent(t, db, "existing-relaxed", "value")
	lsn, err := db.AppendCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent relaxed: %v", err)
	}
	if lsn == 0 {
		t.Fatal("AppendCommandWALIntent relaxed lsn=0")
	}

	barrierErr := errors.New("raw publish barrier ran")
	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return barrierErr
	})
	got, err := db.AppendCommandWALIntent(intent, true)
	if !errors.Is(err, barrierErr) {
		t.Fatalf("AppendCommandWALIntent existing relaxed sync error=%v, want %v", err, barrierErr)
	}
	if got != lsn {
		t.Fatalf("AppendCommandWALIntent existing relaxed sync lsn=%d, want original %d before barrier append", got, lsn)
	}
	if !barrierCalled.Load() {
		t.Fatal("AppendCommandWALIntent existing relaxed sync skipped raw publish barriers")
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d after raw publish barrier failure, want 0", got)
	}
	unregister()

	got, err = db.AppendCommandWALIntent(intent, true)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent existing relaxed sync retry: %v", err)
	}
	if got != lsn {
		t.Fatalf("AppendCommandWALIntent existing relaxed sync retry lsn=%d, want original %d", got, lsn)
	}
	if err := db.PublishCommandWALNoop(intent, false); err != nil {
		t.Fatalf("PublishCommandWALNoop after durable barrier: %v", err)
	}
	if got := db.State().AppliedCommandLSN; got != lsn+1 {
		t.Fatalf("AppliedCommandLSN=%d, want relaxed frame plus barrier %d", got, lsn+1)
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
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Write first error=%v, want finalize commit failpoint", err)
	}
	_ = b.Close()
	db.testFailFinalizeCommit.Store(false)

	if err := db.ForceCommit(db.State().RootPageID); !errors.Is(err, ErrRecoveryRequired) {
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

func TestCommandWALAcceptedWaitFailureDoesNotPoisonOpenHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)

	baseAppliedLSN := db.State().AppliedCommandLSN
	db.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
	db.testFailWriteMeta.Store(true)
	err := db.SetSync([]byte("accepted"), []byte("visible-before-durable"))
	db.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("accepted wait error=%v, want retryable meta failpoint", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("accepted wait error=%v unexpectedly requires recovery", err)
	}
	if got := db.State().AppliedCommandLSN; got <= baseAppliedLSN {
		t.Fatalf("visible AppliedCommandLSN after accepted wait error=%d, want greater than %d", got, baseAppliedLSN)
	}
	assertDBValue(t, db, "accepted", "visible-before-durable")
	if err := db.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after accepted wait error: %v", err)
	}
	if err := db.SetSync([]byte("later"), []byte("same-handle-progress")); err != nil {
		t.Fatalf("SetSync after accepted wait error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after accepted wait retry: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "accepted", "visible-before-durable")
	assertDBValue(t, reopen, "later", "same-handle-progress")
}

func TestCommandWALNoopAcceptedWaitFailureDoesNotPoisonOpenHandle(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	intent := mustRawKVCommandWALIntent(t, db, "noop-covered", "external-apply")
	db.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
	db.testFailWriteMeta.Store(true)
	err := db.PublishCommandWALNoop(intent, true)
	db.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("accepted no-op wait error=%v, want retryable meta failpoint", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("accepted no-op wait error=%v unexpectedly requires recovery", err)
	}
	if got, want := db.State().AppliedCommandLSN, intent.AssignedLSN(); got == 0 || got != want {
		t.Fatalf("visible AppliedCommandLSN after accepted no-op error=%d, want assigned %d", got, want)
	}
	if err := db.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after accepted no-op error: %v", err)
	}
	if err := db.SetSync([]byte("after-noop"), []byte("same-handle-progress")); err != nil {
		t.Fatalf("SetSync after accepted no-op error: %v", err)
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

func TestCommandWALMaterializedRIDRecoveryCreatesExactRIDAndSurvivesCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})

	_, err := Open(Options{Dir: dir, testCommandWALRecoveryFailAfterLSN: 1})
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Open after materialized RID replay error=%v, want post-publication failpoint", err)
	}

	reopen := openCommandWALDB(t, dir)
	assertDBValue(t, reopen, "k", "materialized")
	if err := reopen.Checkpoint(); err != nil {
		_ = reopen.Close()
		t.Fatalf("Checkpoint materialized RID replay: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close materialized RID replay: %v", err)
	}

	segments, err := listSegmentsInDir(ValueLogDirPath(dir))
	if err != nil {
		t.Fatalf("list value-log segments: %v", err)
	}
	ridMap, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatalf("scanValueLogSegments: %v", err)
	}
	if len(ridMap) != 1 {
		t.Fatalf("materialized recovery RID count=%d, want exactly 1 after replay retry", len(ridMap))
	}
	sourcePtr, ok := ridMap[42]
	if !ok {
		t.Fatalf("materialized recovery did not preserve exact RID 42: %+v", ridMap)
	}

	final := openCommandWALDB(t, dir)
	assertDBValue(t, final, "k", "materialized")
	if got := final.State().AppliedCommandLSN; got != 1 {
		_ = final.Close()
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	appender, ok := final.currentValueLogAppender().(*replayInlineAppender)
	if !ok {
		_ = final.Close()
		t.Fatalf("value-log appender type=%T, want replayInlineAppender", final.currentValueLogAppender())
	}
	appender.mu.Lock()
	nextRID := appender.nextRID
	appender.mu.Unlock()
	if nextRID <= 42 {
		_ = final.Close()
		t.Fatalf("next value-log RID=%d, want allocator advanced past recovered RID 42", nextRID)
	}
	rewriteStats, err := final.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{sourcePtr.FileID},
		BatchSize:     1,
	})
	if err != nil {
		_ = final.Close()
		t.Fatalf("ValueLogRewriteOnline after materialized RID recovery: %v", err)
	}
	if rewriteStats.RecordsCopied != 1 {
		_ = final.Close()
		t.Fatalf("materialized RID rewrite records copied=%d, want 1 (stats=%+v)", rewriteStats.RecordsCopied, rewriteStats)
	}
	if _, err := final.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		_ = final.Close()
		t.Fatalf("ValueLogGC after materialized RID recovery: %v", err)
	}
	assertDBValue(t, final, "k", "materialized")
	if err := final.Close(); err != nil {
		t.Fatalf("Close after materialized RID rewrite/GC: %v", err)
	}

	afterGC := openCommandWALDB(t, dir)
	defer afterGC.Close()
	assertDBValue(t, afterGC, "k", "materialized")
}

func TestCommandWALMaterializedRIDRecoveryReusesMatchingRID(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 42, []byte("materialized"))
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "materialized")

	segments, err := listSegmentsInDir(ValueLogDirPath(dir))
	if err != nil {
		t.Fatalf("list value-log segments: %v", err)
	}
	ridMap, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatalf("scanValueLogSegments: %v", err)
	}
	if len(ridMap) != 1 {
		t.Fatalf("matching materialized recovery RID count=%d, want no duplicate", len(ridMap))
	}
}

func TestCommandWALMaterializedRIDRecoverySyncsReusedRIDBeforeDurableRootPublication(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	ptr := writeValueLogRID(t, dir, 42, []byte("materialized"))
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})
	valueLogPath := valuelog.SegmentPath(ValueLogDirPath(dir), ptr.FileID)
	containsValueLogPath := func(event durabilitycut.Event) bool {
		if event.Path == valueLogPath {
			return true
		}
		for _, path := range event.Paths {
			if path == valueLogPath {
				return true
			}
		}
		return false
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	reopen := openCommandWALDB(t, dir)
	restore()
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "materialized")

	beforeSync, afterSync, applied, beforeSeal := -1, -1, -1, -1
	for i, event := range events {
		switch {
		case event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.BeforeDependencyFileSync && containsValueLogPath(event):
			beforeSync = i
		case event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.AfterDependencyFileSync && containsValueLogPath(event):
			afterSync = i
		case event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.AfterAppliedLSNAdvance && event.LSN == 1:
			applied = i
		case event.Resource == durabilitycut.ResourceSeal && event.Point == durabilitycut.BeforePublicationSealWrite:
			beforeSeal = i
		}
	}
	if applied < 0 || beforeSync <= applied || afterSync <= beforeSync || beforeSeal <= afterSync {
		t.Fatalf("reused RID sync must precede durable root publication: applied=%d before=%d after=%d seal=%d path=%q events=%#v", applied, beforeSync, afterSync, beforeSeal, valueLogPath, events)
	}
}

func TestCommandWALMaterializedRIDRecoveryReusedRIDSyncFailureFailsClosed(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	ptr := writeValueLogRID(t, dir, 42, []byte("materialized"))
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})
	valueLogPath := valuelog.SegmentPath(ValueLogDirPath(dir), ptr.FileID)
	wantErr := errors.New("injected reused RID sync failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceAuxiliary || event.Point != durabilitycut.BeforeDependencyFileSync {
			return nil
		}
		if event.Path == valueLogPath {
			return wantErr
		}
		for _, path := range event.Paths {
			if path == valueLogPath {
				return wantErr
			}
		}
		return nil
	})
	_, err := Open(Options{Dir: dir})
	restore()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Open reused RID sync error=%v, want %v", err, wantErr)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "materialized")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1 after successful retry", got)
	}
}

func TestCommandWALMaterializedRIDRecoveryPreservesExistingRIDHighWaterAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 200, []byte("existing-high-water"))
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("recovered"), RID: 42, Value: []byte("materialized-older-rid"),
	}})

	recovered := openCommandWALDB(t, dir)
	assertDBValue(t, recovered, "recovered", "materialized-older-rid")
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close recovered db: %v", err)
	}

	reopened := openCommandWALDB(t, dir)
	defer reopened.Close()
	appender, ok := reopened.currentValueLogAppender().(*replayInlineAppender)
	if !ok {
		t.Fatalf("value-log appender type=%T, want replayInlineAppender", reopened.currentValueLogAppender())
	}
	appender.mu.Lock()
	nextRID := appender.nextRID
	appender.mu.Unlock()
	if nextRID != 201 {
		t.Fatalf("next value-log RID=%d, want 201 after exact-RID recovery below existing high-water", nextRID)
	}
}

func TestCommandWALMaterializedRIDRecoveryDuplicateRIDWithinFrame(t *testing.T) {
	t.Run("matching", func(t *testing.T) {
		dir := t.TempDir()
		enableCommandWALFormat(t, dir)
		bootstrap := openCommandWALDB(t, dir)
		if err := bootstrap.Close(); err != nil {
			t.Fatalf("Close bootstrap db: %v", err)
		}
		writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{
			{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("a"), RID: 42, Value: []byte("materialized")},
			{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("b"), RID: 42, Value: []byte("materialized")},
		})

		reopen := openCommandWALDB(t, dir)
		defer reopen.Close()
		assertDBValue(t, reopen, "a", "materialized")
		assertDBValue(t, reopen, "b", "materialized")
		segments, err := listSegmentsInDir(ValueLogDirPath(dir))
		if err != nil {
			t.Fatalf("list value-log segments: %v", err)
		}
		ridMap, err := scanValueLogSegments(segments, nil)
		if err != nil {
			t.Fatalf("scanValueLogSegments: %v", err)
		}
		if len(ridMap) != 1 {
			t.Fatalf("matching duplicate RID count=%d, want one physical record", len(ridMap))
		}
	})

	t.Run("conflicting", func(t *testing.T) {
		dir := t.TempDir()
		enableCommandWALFormat(t, dir)
		bootstrap := openCommandWALDB(t, dir)
		if err := bootstrap.Close(); err != nil {
			t.Fatalf("Close bootstrap db: %v", err)
		}
		writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{
			{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("a"), RID: 42, Value: []byte("first")},
			{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("b"), RID: 42, Value: []byte("second")},
		})

		_, err := Open(Options{Dir: dir})
		if !errors.Is(err, ErrCommandWALConflictingValueLogRID) || !errors.Is(err, commitlog.ErrCorrupt) {
			t.Fatalf("Open conflicting duplicate RID error=%v, want conflict plus corruption", err)
		}
	})
}

func TestCommandWALMaterializedRIDRecoveryRejectsConflictingRID(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 42, []byte("other"))
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})

	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, ErrCommandWALConflictingValueLogRID) {
		t.Fatalf("Open conflicting materialized RID error=%v, want %v", err, ErrCommandWALConflictingValueLogRID)
	}
	if !errors.Is(err, commitlog.ErrCorrupt) {
		t.Fatalf("Open conflicting materialized RID error=%v, want corruption classification", err)
	}
}

func TestCommandWALMaterializedRIDRecoveryReplacesTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 42, []byte("partial-materialization"))
	valueLogPath := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	info, err := os.Stat(valueLogPath)
	if err != nil {
		t.Fatalf("Stat value-log segment: %v", err)
	}
	if err := os.Truncate(valueLogPath, info.Size()-4); err != nil {
		t.Fatalf("Truncate value-log tail: %v", err)
	}
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "materialized")
}

func TestCommandWALMaterializedRIDRecoveryRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 42, []byte("materialized"))
	valueLogPath := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	f, err := os.OpenFile(valueLogPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile value-log segment: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		t.Fatalf("Stat value-log segment: %v", err)
	}
	var last [1]byte
	if _, err := f.ReadAt(last[:], info.Size()-1); err != nil {
		_ = f.Close()
		t.Fatalf("ReadAt value-log tail: %v", err)
	}
	last[0] ^= 0xff
	if _, err := f.WriteAt(last[:], info.Size()-1); err != nil {
		_ = f.Close()
		t.Fatalf("WriteAt value-log tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close corrupted value-log segment: %v", err)
	}
	writeCommandWALRawKVFrameWithFormat(t, dir, 1, 1, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("k"), RID: 42, Value: []byte("materialized"),
	}})

	if _, err := Open(Options{Dir: dir}); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("Open checksum-mismatched materialized RID error=%v, want %v", err, valuelog.ErrCorrupt)
	}
}

func TestCommandWALMaterializedRIDRecoveryMixedV1V2AndDelete(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("pointer"), Value: []byte("old")},
		{Op: commitlog.RawKVOpSet, Key: []byte("legacy"), Value: []byte("v1")},
	})
	writeCommandWALRawKVFrameWithFormat(t, dir, 2, 2, commitlog.PayloadFormatRawKVBatchV2, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("pointer"), RID: 42, Value: []byte("v2")},
		{Op: commitlog.RawKVOpSetMaterializedRID, Key: []byte("doomed"), RID: 43, Value: []byte("gone")},
		{Op: commitlog.RawKVOpDelete, Key: []byte("doomed")},
		{Op: commitlog.RawKVOpDelete, Key: []byte("legacy")},
	})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "pointer", "v2")
	if has, err := reopen.Has([]byte("legacy")); err != nil || has {
		t.Fatalf("Has(legacy)=(%t,%v), want false,nil after mixed V1/V2 delete", has, err)
	}
	if has, err := reopen.Has([]byte("doomed")); err != nil || has {
		t.Fatalf("Has(doomed)=(%t,%v), want false,nil after materialized V2 delete", has, err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALRelaxedRIDReplaySyncsDependenciesBeforeRootPublication(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 7, []byte("large-value"))
	writeCommandWALRawKVFrameWithDurability(t, dir, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSetRID, Key: []byte("k"), RID: 7,
	}})

	_, err := Open(Options{Dir: dir, testCommandWALRecoveryFailBeforeDependencySync: true})
	if !errors.Is(err, errTestCommandWALRecoveryDependencySyncFailpoint) {
		t.Fatalf("Open before dependency sync error=%v, want failpoint", err)
	}

	// If the dependency failure published the replayed root, this second open
	// would have no unapplied LSN at which to trigger the post-publication cut.
	_, err = Open(Options{Dir: dir, testCommandWALRecoveryFailAfterLSN: 1})
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Open after dependency-sync retry error=%v, want post-publication failpoint", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "large-value")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALRelaxedReplayRetainsCleanupUntilLaterRecoveryBaseline(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrameWithDurability(t, dir, 1, 1, commitlog.CommandDurabilityRelaxed, []commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("relaxed"), Value: []byte("value"),
	}})
	coveredPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))

	replayOpen := openCommandWALDB(t, dir)
	assertDBValue(t, replayOpen, "relaxed", "value")
	if got := replayOpen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if got := replayOpen.commandWALDurableLSN.Load(); got != 0 {
		t.Fatalf("durable WAL LSN=%d after relaxed replay, want classified frontier 0", got)
	}
	if _, err := replayOpen.captureDurableWALCleanupProofV1(); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("cleanup proof after relaxed replay error=%v, want conservative unavailable authority", err)
	}
	if _, err := os.Stat(coveredPath); err != nil {
		t.Fatalf("relaxed replay cleanup removed WAL before authority existed: %v", err)
	}
	batch := replayOpen.NewBatch()
	if err := batch.Set([]byte("same-session"), []byte("value-2")); err != nil {
		_ = batch.Close()
		t.Fatalf("Set same-session value: %v", err)
	}
	if err := batch.Write(); err != nil {
		_ = batch.Close()
		t.Fatalf("Write same-session relaxed frame: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("Close same-session batch: %v", err)
	}
	if got := replayOpen.CommandWALActiveBytes(); got == 0 {
		t.Fatal("same-session relaxed write left no active WAL bytes, want checkpoint rotation coverage")
	}
	if err := replayOpen.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint relaxed replay handle: %v", err)
	}
	assertDBValue(t, replayOpen, "same-session", "value-2")
	if got := replayOpen.commandWALDurableLSN.Load(); got != 0 {
		t.Fatalf("durable WAL LSN after same-session checkpoint=%d, want classified frontier 0", got)
	}
	if _, err := replayOpen.captureDurableWALCleanupProofV1(); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("cleanup proof after same-session checkpoint error=%v, want conservative unavailable authority", err)
	}
	if _, err := os.Stat(coveredPath); err != nil {
		t.Fatalf("same-session checkpoint removed recovered WAL without physical frontier authority: %v", err)
	}
	if err := replayOpen.Close(); err != nil {
		t.Fatalf("Close relaxed replay handle: %v", err)
	}
	if _, err := os.Stat(coveredPath); err != nil {
		t.Fatalf("Close removed WAL while cleanup authority remained unavailable: %v", err)
	}

	converged := openCommandWALDB(t, dir)
	assertDBValue(t, converged, "relaxed", "value")
	assertDBValue(t, converged, "same-session", "value-2")
	if got := converged.commandWALDurableLSN.Load(); got != 2 {
		t.Fatalf("durable WAL LSN on later recovery baseline=%d, want 2", got)
	}
	state := converged.State()
	if err := converged.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, nil, true); err != nil {
		t.Fatalf("publish replay frontier into older recoverable slot: %v", err)
	}
	if err := converged.Close(); err != nil {
		t.Fatalf("Close converged reopen: %v", err)
	}
	if _, err := os.Stat(coveredPath); !os.IsNotExist(err) {
		t.Fatalf("covered WAL stat after later recovery baseline=%v, want eventual cleanup", err)
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

// Visibility flush is separate from durability: dependency durability must use
// the exact stable handle captured after this flush, never Sync or pathname
// reopen here.
func TestCommandWALExternalRefFlushDoesNotSync(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	appender := &commandWALExternalRefSyncTestAppender{t: t, fileID: 17}
	db.SetValueLogAppender(appender)
	if err := db.flushCommandWALExternalRefs([]uint32{17}); err != nil {
		t.Fatalf("flushCommandWALExternalRefs active segment: %v", err)
	}
	if appender.flushes.Load() != 1 {
		t.Fatalf("appender flushes=%d, want 1", appender.flushes.Load())
	}
	if appender.syncs.Load() != 0 {
		t.Fatalf("appender syncs=%d, want 0", appender.syncs.Load())
	}
}

func TestCommandWALExternalRefFlushUsesReferencedLaneFlusher(t *testing.T) {
	fileIDs := []uint32{17, 18}
	db := &DB{}
	appender := &commandWALExternalRefLaneFlushTestAppender{t: t}
	db.SetValueLogAppender(appender)
	if err := db.flushCommandWALExternalRefs(fileIDs); err != nil {
		t.Fatalf("flushCommandWALExternalRefs referenced lanes: %v", err)
	}
	if appender.externalFlushes.Load() != 1 {
		t.Fatalf("external flushes=%d, want 1", appender.externalFlushes.Load())
	}
	if appender.sync {
		t.Fatal("referenced-lane visibility flush unexpectedly requested sync")
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
	if err := db.flushCommandWALExternalRefs(nil); !errors.Is(err, ErrValueLogAppenderUnavailable) {
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
	writeCommandWALRawKVFrameWithDurability(t, dir, segmentSeq, lsn, commitlog.CommandDurabilityDurable, ops)
}

func writeCommandWALRawKVFrameWithFormat(t testing.TB, dir string, segmentSeq uint64, lsn uint64, format commitlog.PayloadFormat, ops []commitlog.RawKVOperation) {
	t.Helper()
	writeCommandWALRawKVFrameForLaneWithDurabilityAndFormat(t, dir, 0, segmentSeq, lsn, commitlog.CommandDurabilityDurable, format, ops)
}

func writeCommandWALRawKVFrameWithDurability(t testing.TB, dir string, segmentSeq uint64, lsn uint64, durability commitlog.CommandDurabilityClass, ops []commitlog.RawKVOperation) {
	t.Helper()
	writeCommandWALRawKVFrameForLaneWithDurability(t, dir, 0, segmentSeq, lsn, durability, ops)
}

func writeCommandWALRawKVFrameForLane(t testing.TB, dir string, lane int, segmentSeq uint64, lsn uint64, ops []commitlog.RawKVOperation) {
	t.Helper()
	writeCommandWALRawKVFrameForLaneWithDurability(t, dir, lane, segmentSeq, lsn, commitlog.CommandDurabilityDurable, ops)
}

func writeCommandWALRawKVFrameForLaneWithDurability(t testing.TB, dir string, lane int, segmentSeq uint64, lsn uint64, durability commitlog.CommandDurabilityClass, ops []commitlog.RawKVOperation) {
	t.Helper()
	writeCommandWALRawKVFrameForLaneWithDurabilityAndFormat(t, dir, lane, segmentSeq, lsn, durability, commitlog.PayloadFormatRawKVBatchV1, ops)
}

func writeCommandWALRawKVFrameForLaneWithDurabilityAndFormat(t testing.TB, dir string, lane int, segmentSeq uint64, lsn uint64, durability commitlog.CommandDurabilityClass, format commitlog.PayloadFormat, ops []commitlog.RawKVOperation) {
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
		Version:         commitlog.CommandFrameVersionV2,
		DurabilityClass: durability,
		LSN:             lsn,
		Kind:            commitlog.CommandKindRawKVBatch,
		Scope:           commitlog.CommandScopeRawKV,
		PayloadFormat:   format,
		Payload:         payload,
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
