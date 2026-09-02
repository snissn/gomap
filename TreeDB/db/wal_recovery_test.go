package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestNextReplayAppenderRIDStartPreservesRIDHighWaterAcrossAllSegments(t *testing.T) {
	dir := t.TempDir()
	older := writeRIDStartValueLogSegment(t, dir, 2, 1, 200)
	latest := writeRIDStartValueLogSegment(t, dir, 2, 2, 42)
	leafLatest := writeRIDStartValueLogSegment(t, dir, rewriteLeafLogLaneID, 1, 150)
	ignoredCommit := logSegment{lane: 2, seq: 99, path: filepath.Join(dir, "commit.log")}

	next, err := nextReplayAppenderRIDStart([]logSegment{older, latest, leafLatest, ignoredCommit})
	if err != nil {
		t.Fatalf("nextReplayAppenderRIDStart: %v", err)
	}
	if next != 201 {
		t.Fatalf("next RID=%d want 201", next)
	}
}

func writeRIDStartValueLogSegment(t *testing.T, dir string, lane uint32, seq uint32, maxRID uint64) logSegment {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("fileID: %v", err)
	}
	path := valueLogSegmentPath(t, dir, fileID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir segment dir: %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	for _, rid := range []uint64{maxRID - 1, maxRID} {
		if _, err := w.Append(0, nil, rid, []byte("value")); err != nil {
			t.Fatalf("append rid %d: %v", rid, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	return logSegment{lane: int(lane), seq: uint64(seq), path: path, size: info.Size(), valueLog: true, fileID: fileID}
}

func TestNewReplayInlineAppender_PackedValuePtrCapsSegmentSize(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		indexPackedValuePtr: true,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if app.writer == nil {
		t.Fatalf("expected replay inline appender writer")
	}
	want := int64(^uint32(0)) - 4
	if got := app.writer.maxSize; got != want {
		t.Fatalf("unexpected replay appender maxSize: got %d want %d", got, want)
	}
}

func TestNewReplayInlineAppender_UnpackedNoSegmentCap(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		indexPackedValuePtr: false,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if app.writer == nil {
		t.Fatalf("expected replay inline appender writer")
	}
	if got := app.writer.maxSize; got != 0 {
		t.Fatalf("unexpected replay appender maxSize: got %d want 0", got)
	}
}

func TestNewReplayInlineAppender_UsesConfiguredValueLogBlockCompression(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		valueLogCompression: ValueLogCompressionBlock,
		valueLogBlockCodec:  ValueLogBlockLZ4,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if !app.writer.blockCompression {
		t.Fatalf("expected replay writer block compression enabled")
	}
	if got, want := app.writer.blockCodec, valuelog.BlockCodecLZ4; got != want {
		t.Fatalf("unexpected replay writer block codec: got=%v want=%v", got, want)
	}
}

func TestNewReplayInlineAppender_ValueLogCompressionOff_DisablesBlockCompression(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		valueLogCompression: ValueLogCompressionOff,
		valueLogBlockCodec:  ValueLogBlockSnappy,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if app.writer.blockCompression {
		t.Fatalf("expected replay writer block compression disabled")
	}
}

func TestReplayInlineAppenderConcurrentAppendValuesM12A(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	db := &DB{
		dir:                 dir,
		valueLogCompression: ValueLogCompressionOff,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	const (
		workers   = 8
		perWorker = 64
	)
	ptrs := make(chan page.ValuePtr, workers*perWorker)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				values := [][]byte{[]byte(fmt.Sprintf("worker=%02d value=%02d", worker, i))}
				batchPtrs, err := app.AppendValues(values)
				if err != nil {
					errs <- err
					return
				}
				ptrs <- batchPtrs[0]
			}
		}()
	}
	wg.Wait()
	close(ptrs)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("AppendValues: %v", err)
		}
	}
	if err := app.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	seen := make(map[page.ValuePtr]struct{}, workers*perWorker)
	for ptr := range ptrs {
		if _, ok := seen[ptr]; ok {
			t.Fatalf("duplicate value-log ptr from concurrent appends: %+v", ptr)
		}
		seen[ptr] = struct{}{}
	}
	if got, want := len(seen), workers*perWorker; got != want {
		t.Fatalf("ptr count=%d, want %d", got, want)
	}
}

func TestReplayInlineAppenderFlushKeepsDirtyUntilSyncM12A(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	db := &DB{
		dir:                 dir,
		valueLogCompression: ValueLogCompressionOff,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if _, err := app.AppendValues([][]byte{[]byte("unsynced-value")}); err != nil {
		t.Fatalf("AppendValues: %v", err)
	}
	if !app.dirty {
		t.Fatalf("AppendValues left appender clean, want dirty until Sync")
	}
	if err := app.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !app.dirty {
		t.Fatalf("Flush cleared dirty state before Sync")
	}
	if err := app.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if app.dirty {
		t.Fatalf("Sync left appender dirty")
	}
}

func TestReplayInlineAppender_LeafPagesUseConfiguredLeafLogDir(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	db := &DB{
		dir:                        dir,
		indexOuterLeavesInValueLog: true,
		valueLogCompression:        ValueLogCompressionBlock,
		valueLogBlockCodec:         ValueLogBlockSnappy,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	leafPtr, err := app.AppendLeafPage(bytes.Repeat([]byte("l"), page.PageSize))
	if err != nil {
		_ = app.close()
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := app.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	lane, _ := valuelog.DecodeFileID(leafPtr.ValueLogFileID())
	if lane != rewriteLeafLogLaneID {
		t.Fatalf("leaf ptr lane=%d want=%d", lane, rewriteLeafLogLaneID)
	}
	valuePaths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value_vlog: %v", err)
	}
	if len(valuePaths) != 0 {
		t.Fatalf("expected no value_vlog files for replayed leaf page, got %v", valuePaths)
	}
	leafPaths, err := filepath.Glob(filepath.Join(dir, "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog: %v", err)
	}
	if len(leafPaths) != 1 {
		t.Fatalf("expected one leaf_vlog file, got %v", leafPaths)
	}
}

type replayInlineRIDReserveTestAppender struct {
	next  uint64
	calls []int
}

func (a *replayInlineRIDReserveTestAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	return nil, fmt.Errorf("AppendValues should not be called by stale replay appender")
}

func (a *replayInlineRIDReserveTestAppender) ReserveRIDs(count int) (uint64, error) {
	if err := validateRewriteRIDCount(count); err != nil {
		return 0, err
	}
	start := a.next
	if err := validateRewriteRIDRange(start, count); err != nil {
		return 0, err
	}
	a.next += uint64(count)
	a.calls = append(a.calls, count)
	return start, nil
}

func (a *replayInlineRIDReserveTestAppender) Flush() error { return nil }

func (a *replayInlineRIDReserveTestAppender) Sync() error { return nil }

func (a *replayInlineRIDReserveTestAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}

func TestReplayInlineAppenderUsesCurrentRIDReserverWhenStaleM12A(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	db := &DB{
		dir:                 dir,
		valueLogCompression: ValueLogCompressionOff,
	}
	stale, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}

	active := &replayInlineRIDReserveTestAppender{next: 100}
	db.SetValueLogAppender(active)

	if _, err := stale.AppendValues([][]byte{[]byte("retained-value-1"), []byte("retained-value-2")}); err != nil {
		_ = stale.close()
		t.Fatalf("AppendValues: %v", err)
	}
	if _, err := stale.AppendLeafPage(bytes.Repeat([]byte("l"), page.PageSize)); err != nil {
		_ = stale.close()
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if err := stale.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got, want := fmt.Sprint(active.calls), "[2 1]"; got != want {
		t.Fatalf("ReserveRIDs calls=%s want %s", got, want)
	}
	valueSegments, err := listSegmentsInDir(ValueLogDirPath(dir))
	if err != nil {
		t.Fatalf("list value segments: %v", err)
	}
	leafSegments, err := listSegmentsInDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("list leaf segments: %v", err)
	}
	ridMap, err := scanValueLogSegments(append(valueSegments, leafSegments...), nil)
	if err != nil {
		t.Fatalf("scanValueLogSegments: %v", err)
	}
	for _, rid := range []uint64{100, 101, 102} {
		if _, ok := ridMap[rid]; !ok {
			t.Fatalf("rid %d missing from stale replay append rid map: %v", rid, ridMap)
		}
	}
	for _, rid := range []uint64{1, 2, 3} {
		if _, ok := ridMap[rid]; ok {
			t.Fatalf("stale replay appender used local rid %d instead of active allocator", rid)
		}
	}
}

func TestReplayWALIntoBackend_IgnoresEmptyCommitSegments(t *testing.T) {
	segments := []logSegment{
		{path: "/tmp/empty-commit.log", size: 0, valueLog: false},
		{path: "/tmp/value.log", size: 128, valueLog: true, fileID: 1},
	}
	if err := replayWALIntoBackend(nil, segments, 0, nil); err != nil {
		t.Fatalf("replayWALIntoBackend: %v", err)
	}
}

func TestOpenLegacyCachedRedoJournalRequiresExplicitReplay(t *testing.T) {
	dir := t.TempDir()
	commitPath := writeLegacyCachedRedoJournalSegment(t, dir, []commitlog.Record{
		{Op: commitlog.OpSetInline, Key: []byte("k"), Value: []byte("v"), Seq: 1},
	})

	_, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, ErrLegacyCachedRedoJournalReplayDisabled) {
		t.Fatalf("Open legacy redo journal error=%v, want ErrRecoveryRequired and ErrLegacyCachedRedoJournalReplayDisabled", err)
	}
	if _, statErr := os.Stat(commitPath); statErr != nil {
		t.Fatalf("legacy redo journal removed after failed open: %v", statErr)
	}

	_, err = Open(Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, ErrLegacyCachedRedoJournalReplayDisabled) {
		t.Fatalf("Open read-only legacy redo journal error=%v, want ErrRecoveryRequired and ErrLegacyCachedRedoJournalReplayDisabled", err)
	}

	db, err := Open(Options{
		Dir:                                dir,
		DisableBackgroundPrune:             true,
		AllowLegacyCachedRedoJournalReplay: true,
	})
	if err != nil {
		t.Fatalf("Open explicit legacy redo journal replay: %v", err)
	}
	defer db.Close()

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get=%q want v", got)
	}
	if info, err := os.Stat(commitPath); err == nil && info.Size() != 0 {
		t.Fatalf("legacy redo journal size after replay=%d want removed or empty", info.Size())
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatalf("stat legacy redo journal after replay: %v", err)
	}
}

func writeLegacyCachedRedoJournalSegment(t *testing.T, dir string, records []commitlog.Record) string {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll WAL dir: %v", err)
	}
	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	w, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	if err := w.AppendBatch(records); err != nil {
		_ = w.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := w.Sync(); err != nil {
		_ = w.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}
	return commitPath
}

func TestScanValueLogSegments_IgnoresSegmentRemovedAfterScan(t *testing.T) {
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

	ridMap, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatalf("scanValueLogSegments: %v", err)
	}
	if len(ridMap) != 0 {
		t.Fatalf("ridMap len=%d want 0", len(ridMap))
	}
}
