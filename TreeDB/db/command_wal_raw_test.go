package db

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

type commandWALCountingValueLogAppender struct {
	inner   ValueLogAppender
	flushes int
	syncs   int
}

type commandWALBarrierTestAppender struct {
	externalFlushes int
	externalSync    bool
	externalFileIDs []uint32
	externalErr     error
}

func (a *commandWALBarrierTestAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	return nil, errors.New("unexpected AppendValues")
}

func (a *commandWALBarrierTestAppender) Flush() error { return nil }
func (a *commandWALBarrierTestAppender) Sync() error  { return nil }

func (a *commandWALBarrierTestAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}

func (a *commandWALBarrierTestAppender) FlushValueLogExternalRefs(fileIDs []uint32, sync bool) error {
	a.externalFlushes++
	a.externalSync = sync
	a.externalFileIDs = append(a.externalFileIDs[:0], fileIDs...)
	return a.externalErr
}

func (a *commandWALCountingValueLogAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	return a.inner.AppendValues(values)
}

func (a *commandWALCountingValueLogAppender) Flush() error {
	a.flushes++
	return a.inner.Flush()
}

func (a *commandWALCountingValueLogAppender) Sync() error {
	a.syncs++
	return a.inner.Sync()
}

func (a *commandWALCountingValueLogAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return a.inner.CurrentValueLogSegment()
}

func TestFlushCommandWALBarrierOrdersExternalRefsBeforeCommandWAL(t *testing.T) {
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		CommandWALStatsScan:    true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	externalErr := errors.New("external value-log sync failed")
	appender := &commandWALBarrierTestAppender{externalErr: externalErr}
	d.SetValueLogAppender(appender)
	beforeStats := d.Stats()
	before := commandWALTestStatUint64(t, beforeStats, "treedb.command_wal.sync.count_total")
	beforeFileSyncs := commandWALTestStatUint64(t, beforeStats, "treedb.command_wal.file_sync.calls_total")
	if err := d.FlushCommandWALBarrier(true); !errors.Is(err, externalErr) {
		t.Fatalf("FlushCommandWALBarrier error=%v, want %v", err, externalErr)
	}
	if appender.externalFlushes != 1 || !appender.externalSync || len(appender.externalFileIDs) != 0 {
		t.Fatalf("external barrier calls=%d sync=%t fileIDs=%v, want one all-ref sync", appender.externalFlushes, appender.externalSync, appender.externalFileIDs)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.sync.count_total"); got != before {
		t.Fatalf("command WAL sync count=%d, want %d when external barrier fails", got, before)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total"); got != beforeFileSyncs {
		t.Fatalf("command WAL file sync calls=%d, want %d when external barrier fails", got, beforeFileSyncs)
	}

	appender.externalErr = nil
	if err := d.FlushCommandWALBarrier(true); err != nil {
		t.Fatalf("FlushCommandWALBarrier retry: %v", err)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.sync.count_total"); got != before+1 {
		t.Fatalf("command WAL sync count=%d, want %d", got, before+1)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total"); got != beforeFileSyncs+1 {
		t.Fatalf("command WAL file sync calls=%d, want %d", got, beforeFileSyncs+1)
	}
}

func TestCommandWALIntentZeroValueLSNSentinelsM10C(t *testing.T) {
	var nilIntent *CommandWALIntent
	if got := nilIntent.AssignedLSN(); got != 0 {
		t.Fatalf("nil AssignedLSN=%d, want 0", got)
	}
	if got, replay := nilIntent.ReplayAssignedLSN(); got != 0 || replay {
		t.Fatalf("nil ReplayAssignedLSN=(%d,%t), want (0,false)", got, replay)
	}

	var zero CommandWALIntent
	if got := zero.AssignedLSN(); got != 0 {
		t.Fatalf("zero AssignedLSN=%d, want 0", got)
	}
	if got, replay := zero.ReplayAssignedLSN(); got != 0 || replay {
		t.Fatalf("zero ReplayAssignedLSN=(%d,%t), want (0,false)", got, replay)
	}
}

func TestCommandWALIntentRawKVPayloadSetsMaxEntryRevision(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("alpha"), Value: []byte("one"), Revision: 17},
		{Op: commitlog.RawKVOpDelete, Key: []byte("bravo"), Revision: 29},
		{Op: commitlog.RawKVOpDeleteRange, Key: []byte("range-a"), Value: []byte("range-z")},
	})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := d.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewCommandWALIntent: %v", err)
	}
	if got := intent.inner.maxEntryRevision; got != page.EntryRevision(29) {
		t.Fatalf("intent maxEntryRevision=%d, want 29", got)
	}
	intent.inner.lsn = 7
	if got := commandWALFinalizeOptionsForPublicIntent(intent).maxEntryRevision; got != page.EntryRevision(29) {
		t.Fatalf("finalize maxEntryRevision=%d, want 29", got)
	}

	trusted, err := d.NewTrustedCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewTrustedCommandWALIntent: %v", err)
	}
	if got := trusted.inner.maxEntryRevision; got != page.EntryRevision(29) {
		t.Fatalf("trusted maxEntryRevision=%d, want 29", got)
	}
}

func TestRawKVCommandWALRIDCacheUsesInlinePrefixAndOverflow(t *testing.T) {
	cache := makeRawKVCommandWALRIDCache(1024)
	for i := 0; i < rawKVCommandWALRIDInlineCacheEntries+1; i++ {
		ptr := page.ValuePtr{FileID: page.ValueLogFileID(uint32(i + 1)), Offset: uint64(i + 1), Length: 8}
		cache.store(ptr, uint64(i+10))
	}

	first := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 1, Length: 8}
	if got, ok := cache.lookup(first); !ok || got != 10 {
		t.Fatalf("lookup first=(%d,%t), want (10,true)", got, ok)
	}

	lastInline := page.ValuePtr{
		FileID: page.ValueLogFileID(rawKVCommandWALRIDInlineCacheEntries),
		Offset: uint64(rawKVCommandWALRIDInlineCacheEntries),
		Length: 8,
	}
	if got, ok := cache.lookup(lastInline); !ok || got != uint64(rawKVCommandWALRIDInlineCacheEntries+9) {
		t.Fatalf("lookup last inline=(%d,%t), want (%d,true)", got, ok, rawKVCommandWALRIDInlineCacheEntries+9)
	}

	overflow := page.ValuePtr{
		FileID: page.ValueLogFileID(rawKVCommandWALRIDInlineCacheEntries + 1),
		Offset: uint64(rawKVCommandWALRIDInlineCacheEntries + 1),
		Length: 8,
	}
	if got, ok := cache.lookup(overflow); !ok || got != uint64(rawKVCommandWALRIDInlineCacheEntries+10) {
		t.Fatalf("lookup overflow=(%d,%t), want (%d,true)", got, ok, rawKVCommandWALRIDInlineCacheEntries+10)
	}
	if cache.overflow == nil {
		t.Fatal("overflow map is nil after overflow store")
	}
	if got := cache.overflowCount; got != 1 {
		t.Fatalf("overflow count=%d, want 1", got)
	}

	cache.release()
	if got, ok := cache.lookup(first); ok || got != 0 {
		t.Fatalf("lookup after release=(%d,%t), want (0,false)", got, ok)
	}
	if cache.overflow != nil {
		t.Fatal("overflow map retained after release")
	}
	if got := cache.overflowCount; got != 0 {
		t.Fatalf("overflow count after release=%d, want 0", got)
	}
}

func TestTrustedCommandWALIntentAppendsCanonicalCollectionPayload(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{
		{ID: []byte("user-001"), Document: []byte(`{"_id":"user-001"}`)},
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	intent, err := d.NewTrustedCommandWALIntent(
		commitlog.CommandKindCollectionInsertBatchByID,
		commitlog.CommandScopeCollection,
		commitlog.PayloadFormatCollectionInsertBatchByIDV1,
		payload,
	)
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewTrustedCommandWALIntent: %v", err)
	}
	lsn, err := d.AppendCommandWALIntent(intent, false)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	if lsn == 0 || intent.AssignedLSN() != lsn {
		_ = d.Close()
		t.Fatalf("trusted intent lsn=%d assigned=%d, want non-zero match", lsn, intent.AssignedLSN())
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != lsn || env.Kind != commitlog.CommandKindCollectionInsertBatchByID || env.Scope != commitlog.CommandScopeCollection || env.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
		t.Fatalf("decoded trusted command identity mismatch: %+v", env)
	}
	if _, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload); err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
}

func TestAppendRawKVSingleCommandWALSupportsDeleteRange(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	lsn, err := d.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{
		Op:    commitlog.RawKVOpDeleteRange,
		Key:   nil,
		Value: []byte("m"),
	}, true)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendRawKVSingleCommandWAL DeleteRange: %v", err)
	}
	if lsn == 0 {
		_ = d.Close()
		t.Fatalf("AppendRawKVSingleCommandWAL DeleteRange lsn=0")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != lsn || env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV || env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		t.Fatalf("decoded DeleteRange command identity mismatch: %+v", env)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpDeleteRange || ops[0].Key != nil || string(ops[0].Value) != "m" {
		t.Fatalf("decoded DeleteRange ops=%+v, want single [nil,m)", ops)
	}
}

func TestRawKVCommandWALIntentUsesDirectEntries(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	bi := d.NewBatch()
	b, ok := bi.(*Batch)
	if !ok {
		_ = d.Close()
		t.Fatalf("NewBatch type=%T, want *Batch", bi)
	}
	if err := b.Set([]byte("alpha"), []byte("one")); err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("Set alpha: %v", err)
	}
	if err := b.Delete([]byte("bravo")); err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("Delete bravo: %v", err)
	}
	if err := b.DeleteRange(nil, []byte("charlie")); err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	intent, err := d.prepareRawKVCommandWALIntent(b)
	if err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("prepareRawKVCommandWALIntent: %v", err)
	}
	if intent == nil || !intent.rawKVDirect || len(intent.payload) != 0 || intent.rawKVPlan.Count != 3 {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("intent direct=%t payload_len=%d plan=%+v", intent != nil && intent.rawKVDirect, len(intent.payload), intent.rawKVPlan)
	}
	lsn, err := d.appendRawKVCommandWALIntent(intent, false)
	if err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("appendRawKVCommandWALIntent: %v", err)
	}
	if lsn == 0 || intent.lsn != lsn {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("lsn=%d intent=%d, want non-zero match", lsn, intent.lsn)
	}
	if err := b.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("batch Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	if env.LSN != lsn || env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV || env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		t.Fatalf("decoded direct command identity mismatch: %+v", env)
	}
	var got []batchpkg.Entry
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
		got = append(got, batchpkg.Entry{Type: rawKVOpTypeForTest(op), Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if len(got) != 3 || got[0].Type != batchpkg.OpPut || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || got[1].Type != batchpkg.OpDelete || string(got[1].Key) != "bravo" || got[2].Type != batchpkg.OpDeleteRange || got[2].Key != nil || string(got[2].Value) != "charlie" {
		t.Fatalf("decoded direct ops=%+v", got)
	}
}

func TestAppendRawKVCommandWALOrderedEntryScanStreamsReplay(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("alpha"), Value: []byte("one")},
		{Type: batchpkg.OpDelete, Key: []byte("bravo")},
		{Type: batchpkg.OpPut, Key: []byte("charlie"), Value: []byte(strings.Repeat("x", 128))},
	}
	replayCalls := 0
	lsn, err := d.AppendRawKVCommandWALOrderedEntryScan(func(emit func(batchpkg.Entry) error) error {
		replayCalls++
		for i := range entries {
			if err := emit(entries[i]); err != nil {
				return err
			}
		}
		return nil
	}, false)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendRawKVCommandWALOrderedEntryScan: %v", err)
	}
	if lsn == 0 {
		_ = d.Close()
		t.Fatal("AppendRawKVCommandWALOrderedEntryScan lsn=0")
	}
	if replayCalls != 2 {
		_ = d.Close()
		t.Fatalf("replay calls=%d, want 2 planning/writing scans", replayCalls)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var got []batchpkg.Entry
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
		got = append(got, batchpkg.Entry{Type: rawKVOpTypeForTest(op), Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if len(got) != 3 || got[0].Type != batchpkg.OpPut || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || got[1].Type != batchpkg.OpDelete || string(got[1].Key) != "bravo" || got[2].Type != batchpkg.OpPut || string(got[2].Key) != "charlie" || string(got[2].Value) != strings.Repeat("x", 128) {
		t.Fatalf("decoded scan ops=%+v", got)
	}
}

func TestAppendRawKVCommandWALOrderedEntryScanFlushesFreshValueLogPointer(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inner := d.currentValueLogAppender()
	if inner == nil {
		_ = d.Close()
		t.Fatalf("command WAL value-log appender unavailable")
	}
	counting := &commandWALCountingValueLogAppender{inner: inner}
	d.SetValueLogAppender(counting)

	ptrs, err := d.AppendValueLogValues([][]byte{bytes.Repeat([]byte("fresh-pointer-value|"), 16)})
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != 1 {
		_ = d.Close()
		t.Fatalf("AppendValueLogValues returned %d ptrs, want 1", len(ptrs))
	}
	ptr := ptrs[0]
	path, fileID, ok := counting.CurrentValueLogSegment()
	if !ok || path == "" || fileID != ptr.FileID {
		_ = d.Close()
		t.Fatalf("CurrentValueLogSegment=(%q,%d,%t), ptr file_id=%d", path, fileID, ok, ptr.FileID)
	}
	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		_ = d.Close()
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}
	if _, err := d.valueLogManager.ReadRIDUnverified(ptr); !isCommandWALRIDLookupVisibilityError(err) {
		_ = d.Close()
		t.Fatalf("ReadRIDUnverified before flush error=%v, want short-read visibility error", err)
	}

	entries := []batchpkg.Entry{{
		Type:     batchpkg.OpPut,
		Key:      []byte("fresh-pointer"),
		IsPtr:    true,
		ValuePtr: ptr,
	}}
	lsn, err := d.AppendRawKVCommandWALOrderedEntryScan(func(emit func(batchpkg.Entry) error) error {
		for i := range entries {
			if err := emit(entries[i]); err != nil {
				return err
			}
		}
		return nil
	}, false)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendRawKVCommandWALOrderedEntryScan: %v", err)
	}
	if lsn == 0 {
		_ = d.Close()
		t.Fatalf("AppendRawKVCommandWALOrderedEntryScan lsn=0")
	}
	if counting.flushes == 0 {
		_ = d.Close()
		t.Fatalf("value-log appender flushes=0, want retry path to flush fresh pointer segment")
	}
	if counting.syncs != 0 {
		_ = d.Close()
		t.Fatalf("value-log appender syncs=%d, want 0 for sync=false", counting.syncs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpSetRID || string(ops[0].Key) != "fresh-pointer" || ops[0].RID == 0 {
		t.Fatalf("decoded ops=%+v, want single SetRID for fresh-pointer", ops)
	}
}

func TestRawKVOrderedEntriesIntentOwnsPayloadBytes(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	key := []byte("alpha")
	value := []byte("one")
	entries := []batchpkg.Entry{{Type: batchpkg.OpPut, Key: key, Value: value}}
	intent, err := d.NewRawKVCommandWALIntentFromOrderedEntries(entries)
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewRawKVCommandWALIntentFromOrderedEntries: %v", err)
	}
	key[0] = 'X'
	value[0] = 'Y'
	entries[0] = batchpkg.Entry{Type: batchpkg.OpDelete, Key: []byte("mutated")}

	lsn, err := d.AppendCommandWALIntent(intent, false)
	if err != nil {
		_ = d.Close()
		t.Fatalf("AppendCommandWALIntent: %v", err)
	}
	if lsn == 0 {
		_ = d.Close()
		t.Fatal("AppendCommandWALIntent lsn=0")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpSet || string(ops[0].Key) != "alpha" || string(ops[0].Value) != "one" {
		t.Fatalf("decoded mutable ordered-entry intent ops=%+v, want alpha=one", ops)
	}
}

func rawKVOpTypeForTest(op commitlog.RawKVOp) batchpkg.OpType {
	switch op {
	case commitlog.RawKVOpSet, commitlog.RawKVOpSetRID:
		return batchpkg.OpPut
	case commitlog.RawKVOpDelete:
		return batchpkg.OpDelete
	case commitlog.RawKVOpDeleteRange:
		return batchpkg.OpDeleteRange
	default:
		return batchpkg.OpPut
	}
}

func TestCommandWALReplayIntentZeroLSNFailsClosedM10C(t *testing.T) {
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}, 0)
	if got := intent.AssignedLSN(); got != 0 {
		t.Fatalf("zero-lsn replay AssignedLSN=%d, want 0", got)
	}
	if got, replay := intent.ReplayAssignedLSN(); got != 0 || replay {
		t.Fatalf("zero-lsn replay ReplayAssignedLSN=(%d,%t), want (0,false)", got, replay)
	}

	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.AppendCommandWALIntent(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent zero-lsn replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing assigned lsn") {
		t.Fatalf("AppendCommandWALIntent zero-lsn replay error=%v, want missing assigned lsn", err)
	}
	if err := d.PublishCommandWALNoop(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("PublishCommandWALNoop zero-lsn replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing assigned lsn") {
		t.Fatalf("PublishCommandWALNoop zero-lsn replay error=%v, want missing assigned lsn", err)
	}
}

func TestCommandWALReplayIntentRequiresActiveRecoveryFrameM10C(t *testing.T) {
	intent := newCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}, 0)
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.AppendCommandWALIntent(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent fabricated replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "active recovery frame") {
		t.Fatalf("AppendCommandWALIntent fabricated replay error=%v, want active recovery frame", err)
	}
	if err := d.PublishCommandWALNoop(intent, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("PublishCommandWALNoop fabricated replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "active recovery frame") {
		t.Fatalf("PublishCommandWALNoop fabricated replay error=%v, want active recovery frame", err)
	}
}

func TestCommandWALReplayIntentConstructorRequiresActiveRecoveryFrameM10C(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, err := d.NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent zero lsn error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing assigned lsn") {
		t.Fatalf("NewCommandWALReplayIntent zero lsn error=%v, want missing assigned lsn", err)
	}

	if _, err := d.NewCommandWALReplayIntent(commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent outside recovery error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "no active recovery frame") {
		t.Fatalf("NewCommandWALReplayIntent outside recovery error=%v, want no active recovery frame", err)
	}
}

func TestCommandWALReplayIntentRequiresActiveRecoveryTokenM10C(t *testing.T) {
	env := commitlog.CommandEnvelope{
		LSN:           7,
		Kind:          commitlog.CommandKindCollectionInsertBatchByID,
		Scope:         commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1,
	}
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	d.commandWALReplayLSN.Store(env.LSN)

	if _, err := d.NewCommandWALReplayIntent(env); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent missing token error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "no active recovery token") {
		t.Fatalf("NewCommandWALReplayIntent missing token error=%v, want no active recovery token", err)
	}

	d.commandWALReplayLSN.Store(env.LSN + 1)
	d.commandWALReplayToken.Store(99)
	if _, err := d.NewCommandWALReplayIntent(env); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("NewCommandWALReplayIntent lsn mismatch error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "does not match active recovery frame lsn") {
		t.Fatalf("NewCommandWALReplayIntent lsn mismatch error=%v, want active recovery frame lsn mismatch", err)
	}

	d.commandWALReplayLSN.Store(env.LSN)
	d.commandWALReplayToken.Store(99)

	forged := newCommandWALReplayIntent(env, 0)
	if _, err := d.AppendCommandWALIntent(forged, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent forged replay error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "missing recovery token") {
		t.Fatalf("AppendCommandWALIntent forged replay error=%v, want missing recovery token", err)
	}

	forged = newCommandWALReplayIntent(env, 100)
	if _, err := d.AppendCommandWALIntent(forged, false); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("AppendCommandWALIntent forged replay token mismatch error=%v, want ErrCommandWALRejected", err)
	} else if !strings.Contains(err.Error(), "recovery token mismatch") {
		t.Fatalf("AppendCommandWALIntent forged replay token mismatch error=%v, want recovery token mismatch", err)
	}

	authorized, err := d.NewCommandWALReplayIntent(env)
	if err != nil {
		t.Fatalf("NewCommandWALReplayIntent active recovery: %v", err)
	}
	if got, err := d.AppendCommandWALIntent(authorized, false); err != nil || got != env.LSN {
		t.Fatalf("AppendCommandWALIntent authorized replay=(%d,%v), want (%d,nil)", got, err, env.LSN)
	}
}
