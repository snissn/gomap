package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSyncCommandWALDependenciesThroughNoDebtIsAllocationFree(t *testing.T) {
	db := &DB{}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := db.syncCommandWALDependenciesThrough(1, nil); err != nil {
			t.Fatalf("syncCommandWALDependenciesThrough: %v", err)
		}
	}); allocs != 0 {
		t.Fatalf("syncCommandWALDependenciesThrough allocations=%v, want 0", allocs)
	}
}

func TestSyncCommandWALAppliedPrefixIsContiguousRepeatableAndReopenDurable(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i, value := range []string{"one", "two"} {
		batch := d.NewBatch()
		if err := batch.Set([]byte("key"), []byte(value)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if err := batch.Close(); err != nil {
			t.Fatalf("Close batch %d: %v", i, err)
		}
		before := d.State().AppliedCommandLSN
		if before == 0 || d.CommandWALNextLSN() != before+1 {
			t.Fatalf("before sync %d: applied=%d next=%d", i, before, d.CommandWALNextLSN())
		}
		physicalSync, err := d.SyncCommandWALAppliedPrefix()
		if err != nil {
			t.Fatalf("SyncCommandWALAppliedPrefix %d: %v", i, err)
		}
		if !physicalSync {
			t.Fatalf("SyncCommandWALAppliedPrefix %d physicalSync=false, want true", i)
		}
		after := d.State().AppliedCommandLSN
		if after != before+1 || d.CommandWALNextLSN() != after+1 {
			t.Fatalf("after sync %d: applied=%d want=%d next=%d", i, after, before+1, d.CommandWALNextLSN())
		}
		if durable := d.commandWALDurableLSN.Load(); durable < after {
			t.Fatalf("after sync %d: durable=%d want >=%d", i, durable, after)
		}
		fileSyncs := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total")
		// An already-durable applied prefix needs neither another physical sync
		// nor an artificial command identity or AppliedCommandLSN advance.
		physicalSync, err = d.SyncCommandWALAppliedPrefix()
		if err != nil {
			t.Fatalf("repeat SyncCommandWALAppliedPrefix %d: %v", i, err)
		}
		if physicalSync {
			t.Fatalf("repeat SyncCommandWALAppliedPrefix %d physicalSync=true, want durable-prefix reuse", i)
		}
		if got := d.State().AppliedCommandLSN; got != after || d.CommandWALNextLSN() != after+1 {
			t.Fatalf("repeat sync %d: applied=%d next=%d want applied=%d next=%d", i, got, d.CommandWALNextLSN(), after, after+1)
		}
		if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total"); got != fileSyncs {
			t.Fatalf("repeat sync %d: file syncs=%d want unchanged %d", i, got, fileSyncs)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("two")) {
		t.Fatalf("Get after reopen=%q want %q", got, "two")
	}
}

func TestSyncCommandWALAppliedPrefixReusesDurableWriteBoundary(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	batch := d.NewBatch()
	if err := batch.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	applied := d.State().AppliedCommandLSN
	next := d.CommandWALNextLSN()
	fileSyncs := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total")
	if applied == 0 || d.commandWALDurableLSN.Load() < applied {
		t.Fatalf("WriteSync did not establish durable applied prefix: applied=%d durable=%d", applied, d.commandWALDurableLSN.Load())
	}
	physicalSync, err := d.SyncCommandWALAppliedPrefix()
	if err != nil {
		t.Fatalf("SyncCommandWALAppliedPrefix: %v", err)
	}
	if physicalSync {
		t.Fatal("SyncCommandWALAppliedPrefix physicalSync=true after durable WriteSync, want reuse")
	}
	if got := d.State().AppliedCommandLSN; got != applied || d.CommandWALNextLSN() != next {
		t.Fatalf("sync changed durable prefix: applied=%d want=%d next=%d want=%d", got, applied, d.CommandWALNextLSN(), next)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total"); got != fileSyncs {
		t.Fatalf("sync added file sync: got=%d want=%d", got, fileSyncs)
	}
}

func TestSyncCommandWALAppliedPrefixOrdersConcurrentMutations(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const workers = 4
	const writesPerWorker = 3
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for write := 0; write < writesPerWorker; write++ {
				key := []byte(fmt.Sprintf("worker-%d-write-%d", worker, write))
				batch := d.NewBatch()
				if err := batch.Set(key, []byte("value")); err != nil {
					errs <- err
					return
				}
				if err := batch.Write(); err != nil {
					_ = batch.Close()
					errs <- err
					return
				}
				if err := batch.Close(); err != nil {
					errs <- err
					return
				}
				if _, err := d.SyncCommandWALAppliedPrefix(); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent write/sync: %v", err)
	}
	if t.Failed() {
		_ = d.Close()
		return
	}
	if _, err := d.SyncCommandWALAppliedPrefix(); err != nil {
		t.Fatalf("final SyncCommandWALAppliedPrefix: %v", err)
	}
	applied := d.State().AppliedCommandLSN
	if next := d.CommandWALNextLSN(); next != applied+1 {
		t.Fatalf("final command LSNs are non-contiguous: applied=%d next=%d", applied, next)
	}
	if durable := d.commandWALDurableLSN.Load(); durable < applied {
		t.Fatalf("final durable prefix=%d want >=%d", durable, applied)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for worker := 0; worker < workers; worker++ {
		for write := 0; write < writesPerWorker; write++ {
			key := []byte(fmt.Sprintf("worker-%d-write-%d", worker, write))
			if got, err := reopened.Get(key); err != nil || !bytes.Equal(got, []byte("value")) {
				t.Fatalf("Get %q after reopen=%q err=%v", key, got, err)
			}
		}
	}
}

func TestSyncCommandWALAppliedPrefixCrashBeforeRootNeutralPublishRecovers(t *testing.T) {
	if os.Getenv("TREEDB_SYNC_APPLIED_PREFIX_CRASH_HELPER") == "1" {
		d, err := Open(Options{Dir: os.Getenv("TREEDB_SYNC_APPLIED_PREFIX_DIR"), CommandWAL: true, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		batch := d.NewBatch()
		if err := batch.Set([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
		if got := d.State().AppliedCommandLSN; got != 1 {
			t.Fatalf("relaxed mutation applied=%d want 1", got)
		}
		d.testCommandWALBeforeDurablePublishLockHook = func() { os.Exit(0) }
		_, _ = d.SyncCommandWALAppliedPrefix()
		os.Exit(2)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSyncCommandWALAppliedPrefixCrashBeforeRootNeutralPublishRecovers$")
	cmd.Env = append(os.Environ(), "TREEDB_SYNC_APPLIED_PREFIX_CRASH_HELPER=1", "TREEDB_SYNC_APPLIED_PREFIX_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get([]byte("key"))
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get after crash/reopen=%q err=%v", got, err)
	}
	if state := reopened.State(); state.AppliedCommandLSN != 2 || reopened.CommandWALNextLSN() != 3 {
		t.Fatalf("recovered barrier state: applied=%d next=%d want applied=2 next=3", state.AppliedCommandLSN, reopened.CommandWALNextLSN())
	}
}

func TestCommandWALReplayDurablePrefixBarrierNeedsOuterLeafLog(t *testing.T) {
	d := &DB{indexOuterLeavesInValueLog: true}
	tests := []struct {
		name    string
		frames  []commandWALReplayFrame
		applied uint64
		want    bool
	}{
		{
			name: "unapplied barrier",
			frames: []commandWALReplayFrame{{env: commitlog.CommandEnvelope{
				LSN:           2,
				Kind:          commitlog.CommandKindDurablePrefixBarrier,
				PayloadFormat: commitlog.PayloadFormatDurablePrefixBarrierV1,
			}}},
			applied: 1,
			want:    true,
		},
		{
			name: "already applied barrier",
			frames: []commandWALReplayFrame{{env: commitlog.CommandEnvelope{
				LSN:           2,
				Kind:          commitlog.CommandKindDurablePrefixBarrier,
				PayloadFormat: commitlog.PayloadFormatDurablePrefixBarrierV1,
			}}},
			applied: 2,
		},
		{
			name: "non root publishing frame",
			frames: []commandWALReplayFrame{{env: commitlog.CommandEnvelope{
				LSN:  2,
				Kind: commitlog.CommandKind(254),
			}}},
			applied: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			needsLog, err := commandWALReplayFramesNeedLogSupport(d, tc.frames, tc.applied)
			if err != nil {
				t.Fatal(err)
			}
			if needsLog != tc.want {
				t.Fatalf("needs replay leaf log=%t want %t", needsLog, tc.want)
			}
		})
	}
}

func TestRawKVPointCommandWALRejectsMaterializedRIDBeforeRevisionAllocation(t *testing.T) {
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	assigned := false
	_, err = d.AppendRawKVPointCommandWALTrustedWithPreparedRevision(
		commitlog.RawKVOpSetMaterializedRID,
		[]byte("key"),
		[]byte("value"),
		func() page.EntryRevision {
			assigned = true
			return page.EntryRevision(1)
		},
		false,
	)
	if !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("prepared materialized RID error=%v, want %v", err, ErrCommandWALUnsupported)
	}
	if assigned {
		t.Fatal("materialized RID rejection consumed a prepared revision")
	}

	if _, err := d.AppendRawKVPointCommandWALTrustedWithRevision(
		commitlog.RawKVOpSetMaterializedRID,
		[]byte("key"),
		[]byte("value"),
		page.EntryRevision(1),
		false,
	); !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("revision materialized RID error=%v, want %v", err, ErrCommandWALUnsupported)
	}
}

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

	appender := &commandWALBarrierTestAppender{externalErr: errors.New("unused external value-log sync failure")}
	d.SetValueLogAppender(appender)
	beforeStats := d.Stats()
	before := commandWALTestStatUint64(t, beforeStats, "treedb.command_wal.sync.count_total")
	beforeFileSyncs := commandWALTestStatUint64(t, beforeStats, "treedb.command_wal.file_sync.calls_total")
	beforeNextLSN := d.CommandWALNextLSN()
	if err := d.FlushCommandWALBarrier(true); err != nil {
		t.Fatalf("FlushCommandWALBarrier: %v", err)
	}
	if got := d.CommandWALNextLSN(); got != beforeNextLSN {
		t.Fatalf("next LSN after empty durable-prefix sync=%d, want unchanged %d", got, beforeNextLSN)
	}
	if appender.externalFlushes != 0 {
		t.Fatalf("external barrier calls=%d, want exact debt resources only", appender.externalFlushes)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.sync.count_total"); got != before+1 {
		t.Fatalf("command WAL sync count=%d, want %d", got, before+1)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total"); got != beforeFileSyncs+1 {
		t.Fatalf("command WAL file sync calls=%d, want %d", got, beforeFileSyncs+1)
	}

	if err := d.FlushCommandWALBarrier(true); err != nil {
		t.Fatalf("FlushCommandWALBarrier retry: %v", err)
	}
	if got := d.CommandWALNextLSN(); got != beforeNextLSN {
		t.Fatalf("next LSN after repeated empty durable-prefix sync=%d, want unchanged %d", got, beforeNextLSN)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.sync.count_total"); got != before+2 {
		t.Fatalf("command WAL sync count=%d, want %d", got, before+2)
	}
	if got := commandWALTestStatUint64(t, d.Stats(), "treedb.command_wal.file_sync.calls_total"); got != beforeFileSyncs+2 {
		t.Fatalf("command WAL file sync calls=%d, want %d", got, beforeFileSyncs+2)
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
	intent, err := d.prepareRawKVCommandWALIntent(b, false)
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

func TestAppendRawKVCommandWALOrderedEntriesRejectsMalformedValueLogPointerBeforeDurability(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	appender := &commandWALBarrierTestAppender{}
	d.SetValueLogAppender(appender)

	before := d.Stats()
	_, err = d.AppendRawKVCommandWALOrderedEntries([]batchpkg.Entry{{
		Type:  batchpkg.OpPut,
		Key:   []byte("malformed-pointer"),
		IsPtr: true,
		ValuePtr: page.ValuePtr{
			FileID: page.ValueLogFileID(1),
			Length: 0,
		},
	}}, true)
	if err == nil || !strings.Contains(err.Error(), "invalid value-log pointer") {
		t.Fatalf("AppendRawKVCommandWALOrderedEntries error=%v, want invalid value-log pointer", err)
	}
	for _, key := range []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.file_sync.calls_total",
	} {
		got := commandWALTestStatUint64(t, d.Stats(), key) - commandWALTestStatUint64(t, before, key)
		if got != 0 {
			t.Fatalf("%s delta=%d, want 0 after pointer validation failure", key, got)
		}
	}
	if appender.externalFlushes != 0 {
		t.Fatalf("external-reference flushes=%d, want 0 after pointer validation failure", appender.externalFlushes)
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

func TestRawKVCommandWALPreAppendFailureReleasesOneShotDependencies(t *testing.T) {
	d, entry := openCommandWALPointerDependencyTestDB(t)
	defer func() { _ = d.Close() }()

	baselinePins := d.valueLogIdentityPins.ActivePins()
	wantErr := errors.New("injected pre-append failure")
	reached := false
	active := true
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if !active {
			return nil
		}
		if event.Point == durabilitycut.BeforeDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
			reached = true
			return wantErr
		}
		return nil
	})
	defer restore()
	lsn, err := d.AppendRawKVCommandWALOrderedEntries([]batchpkg.Entry{entry}, true)
	active = false
	if !errors.Is(err, wantErr) || lsn != 0 || !reached {
		t.Fatalf("AppendRawKVCommandWALOrderedEntries=(lsn=%d,error=%v,reached=%t), want (0,injected,true)", lsn, err, reached)
	}
	if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
		t.Fatalf("active value-log pins after one-shot pre-append failure=%d, want baseline %d", got, baselinePins)
	}
}

func TestRawKVCommandWALPreAppendFailureRetainsReusableDependenciesForRetry(t *testing.T) {
	d, entry := openCommandWALPointerDependencyTestDB(t)
	defer func() { _ = d.Close() }()

	intent, err := d.NewRawKVCommandWALIntentFromOrderedEntries([]batchpkg.Entry{entry})
	if err != nil {
		t.Fatalf("NewRawKVCommandWALIntentFromOrderedEntries: %v", err)
	}
	baselinePins := d.valueLogIdentityPins.ActivePins()
	wantErr := errors.New("injected pre-append failure")
	reached := false
	active := true
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if !active {
			return nil
		}
		if event.Point == durabilitycut.BeforeDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
			reached = true
			return wantErr
		}
		return nil
	})
	defer restore()
	lsn, err := d.AppendCommandWALIntent(intent, true)
	active = false
	if !errors.Is(err, wantErr) || lsn != 0 || intent.AssignedLSN() != 0 || !reached {
		t.Fatalf("AppendCommandWALIntent=(lsn=%d,assigned=%d,error=%v,reached=%t), want (0,0,injected,true)", lsn, intent.AssignedLSN(), err, reached)
	}
	if intent.inner.dependencyResources == nil {
		t.Fatal("reusable intent discarded exact dependency handles after pre-append failure")
	}
	if got := d.valueLogIdentityPins.ActivePins(); got <= baselinePins {
		t.Fatalf("active value-log pins after reusable pre-append failure=%d, want greater than baseline %d", got, baselinePins)
	}

	lsn, err = d.AppendCommandWALIntent(intent, true)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent retry: %v", err)
	}
	if lsn == 0 || intent.AssignedLSN() != lsn {
		t.Fatalf("AppendCommandWALIntent retry lsn=%d assigned=%d, want non-zero match", lsn, intent.AssignedLSN())
	}
	if got := d.valueLogIdentityPins.ActivePins(); got != baselinePins {
		t.Fatalf("active value-log pins after successful durable retry=%d, want baseline %d", got, baselinePins)
	}
}

func TestRawKVCommandWALMixedMaterializedAndSetRIDPreservesExternalDependency(t *testing.T) {
	d, external := openCommandWALPointerDependencyTestDB(t)
	defer func() { _ = d.Close() }()

	conflicting := external
	conflicting.Value = []byte("wrong-materialization")
	if _, err := d.AppendRawKVCommandWALOrderedEntriesWithMode([]batchpkg.Entry{conflicting}, RawKVCommandWALAppendDurable); !errors.Is(err, ErrCommandWALConflictingValueLogRID) {
		t.Fatalf("conflicting live materialization error=%v, want %v", err, ErrCommandWALConflictingValueLogRID)
	}

	materializedValue := bytes.Repeat([]byte("materialized-command-value|"), 16)
	materializedPtrs, err := d.AppendValueLogValues([][]byte{materializedValue})
	if err != nil {
		t.Fatalf("AppendValueLogValues materialized: %v", err)
	}
	if len(materializedPtrs) != 1 {
		t.Fatalf("AppendValueLogValues materialized returned %d pointers, want 1", len(materializedPtrs))
	}
	path, fileID, ok := d.currentValueLogAppender().CurrentValueLogSegment()
	if !ok || path == "" || fileID != materializedPtrs[0].FileID {
		t.Fatalf("CurrentValueLogSegment materialized=(%q,%d,%t), pointer file_id=%d", path, fileID, ok, materializedPtrs[0].FileID)
	}
	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment materialized: %v", err)
	}

	materialized := external
	materialized.Key = []byte("materialized")
	materialized.Value = materializedValue
	materialized.ValuePtr = materializedPtrs[0]
	inner, err := d.newRawKVCommandWALPayloadIntentFromEntries([]batchpkg.Entry{materialized, external}, true)
	if err != nil {
		t.Fatalf("newRawKVCommandWALPayloadIntentFromEntries: %v", err)
	}
	intent := &CommandWALIntent{inner: *inner}
	if intent == nil || intent.inner.payloadFormat != commitlog.PayloadFormatRawKVBatchV2 || !intent.inner.externalRefs {
		t.Fatalf("mixed intent=%+v, want RawKVBatchV2 with external refs", intent)
	}
	lsn, err := d.AppendCommandWALIntent(intent, true)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent mixed materialized/SetRID: %v", err)
	}
	if lsn == 0 {
		t.Fatal("AppendCommandWALIntent mixed materialized/SetRID lsn=0")
	}
	if intent.inner.dependencyResources != nil {
		t.Fatal("mixed materialized/SetRID dependency resources retained after durable append")
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(intent.inner.payload)
	if err != nil {
		t.Fatalf("DecodeRawKVBatchPayload mixed materialized/SetRID: %v", err)
	}
	if len(ops) != 2 || ops[0].Op != commitlog.RawKVOpSetMaterializedRID || ops[1].Op != commitlog.RawKVOpSetRID ||
		ops[0].RID == 0 || ops[1].RID == 0 || ops[0].RID == ops[1].RID {
		t.Fatalf("mixed materialized/SetRID ops=%+v", ops)
	}
	fence, err := commitlog.ExternalRefFenceV1FromRawKVPayload(intent.inner.payload)
	if err != nil {
		t.Fatalf("ExternalRefFenceV1FromRawKVPayload: %v", err)
	}
	if fence.Count != 1 {
		t.Fatalf("mixed materialized/SetRID fence count=%d, want only SetRID dependency", fence.Count)
	}
}

func TestRawKVCommandWALMaterializedRIDSelectionRequiresDurableModeAndBounds(t *testing.T) {
	materializedValue := bytes.Repeat([]byte("materialized-mode-value|"), 16)

	t.Run("inline-only-reuses-first-plan", func(t *testing.T) {
		d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		scans := 0
		intent, err := d.newRawKVCommandWALIntentFromEntryScanWithHint(func(emit func(batchpkg.Entry) error) error {
			scans++
			return emit(batchpkg.Entry{Type: batchpkg.OpPut, Key: []byte("inline"), Value: []byte("value")})
		}, 1, true)
		if err != nil {
			t.Fatalf("newRawKVCommandWALIntentFromEntryScanWithHint: %v", err)
		}
		defer releaseUnassignedCommandWALIntent(intent)
		if scans != 1 {
			t.Fatalf("planning scans=%d, want 1 for an already-correct inline V1 plan", scans)
		}
		if intent == nil || intent.payloadFormat != commitlog.PayloadFormatRawKVBatchV1 || intent.externalRefs {
			t.Fatalf("inline intent=%+v, want dependency-free RawKVBatchV1", intent)
		}
	})

	t.Run("reusable-intent-falls-back", func(t *testing.T) {
		d, external := openCommandWALPointerDependencyTestDB(t)
		defer func() { _ = d.Close() }()
		external.Value = bytes.Repeat([]byte("command-wal-dependency|"), 16)
		intent, err := d.NewRawKVCommandWALIntentFromOrderedEntries([]batchpkg.Entry{external})
		if err != nil {
			t.Fatalf("NewRawKVCommandWALIntentFromOrderedEntries: %v", err)
		}
		if intent == nil || intent.inner.payloadFormat != commitlog.PayloadFormatRawKVBatchV1 || !intent.inner.externalRefs {
			t.Fatalf("reusable intent=%+v, want dependency-bearing RawKVBatchV1", intent)
		}
		ops, err := commitlog.DecodeRawKVBatchPayload(intent.inner.payload)
		if err != nil {
			t.Fatalf("DecodeRawKVBatchPayload: %v", err)
		}
		if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpSetRID {
			t.Fatalf("reusable intent ops=%+v, want SetRID", ops)
		}
	})

	for _, tc := range []struct {
		name       string
		mode       RawKVCommandWALAppendMode
		legacyAPI  bool
		wantFormat commitlog.PayloadFormat
		wantOp     commitlog.RawKVOp
		wantClass  commitlog.CommandDurabilityClass
	}{
		{name: "relaxed", mode: RawKVCommandWALAppendRelaxed, legacyAPI: true, wantFormat: commitlog.PayloadFormatRawKVBatchV1, wantOp: commitlog.RawKVOpSetRID, wantClass: commitlog.CommandDurabilityRelaxed},
		{name: "direct-durable", mode: RawKVCommandWALAppendDurable, legacyAPI: true, wantFormat: commitlog.PayloadFormatRawKVBatchV2, wantOp: commitlog.RawKVOpSetMaterializedRID, wantClass: commitlog.CommandDurabilityDurable},
		{name: "durable-prefix-participant", mode: RawKVCommandWALAppendDurablePrefixParticipant, wantFormat: commitlog.PayloadFormatRawKVBatchV2, wantOp: commitlog.RawKVOpSetMaterializedRID, wantClass: commitlog.CommandDurabilityRelaxed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, external := openCommandWALPointerDependencyTestDB(t)
			external.Value = bytes.Repeat([]byte("command-wal-dependency|"), 16)
			var err error
			if tc.legacyAPI {
				_, err = d.AppendRawKVCommandWALOrderedEntries([]batchpkg.Entry{external}, tc.mode == RawKVCommandWALAppendDurable)
			} else {
				_, err = d.AppendRawKVCommandWALOrderedEntriesWithMode([]batchpkg.Entry{external}, tc.mode)
			}
			if err != nil {
				_ = d.Close()
				t.Fatalf("AppendRawKVCommandWALOrderedEntriesWithMode: %v", err)
			}
			env := readFirstRawKVCommandWALEnvelope(t, d)
			if env.PayloadFormat != tc.wantFormat || env.DurabilityClass != tc.wantClass {
				_ = d.Close()
				t.Fatalf("envelope format=%d class=%d, want format=%d class=%d", env.PayloadFormat, env.DurabilityClass, tc.wantFormat, tc.wantClass)
			}
			ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
			if err != nil {
				_ = d.Close()
				t.Fatalf("DecodeRawKVBatchPayload: %v", err)
			}
			if len(ops) != 1 || ops[0].Op != tc.wantOp {
				_ = d.Close()
				t.Fatalf("ops=%+v, want one %d", ops, tc.wantOp)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}

	t.Run("value-over-bound", func(t *testing.T) {
		d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		entry := appendCommandWALPointerEntry(t, d, []byte("oversize"), bytes.Repeat([]byte("x"), RawKVCommandWALMaterializedRIDMaxValueBytes+1))
		intent, err := d.newRawKVCommandWALIntentFromEntries([]batchpkg.Entry{entry}, true)
		if err != nil {
			t.Fatalf("newRawKVCommandWALIntentFromEntries: %v", err)
		}
		defer releaseUnassignedCommandWALIntent(intent)
		if intent.payloadFormat != commitlog.PayloadFormatRawKVBatchV1 || !intent.externalRefs {
			t.Fatalf("oversize intent format=%d external=%t, want V1 external", intent.payloadFormat, intent.externalRefs)
		}
	})

	t.Run("operation-count-over-bound", func(t *testing.T) {
		d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		entries := make([]batchpkg.Entry, 0, RawKVCommandWALMaterializedRIDMaxOperations+1)
		entries = append(entries, appendCommandWALPointerEntry(t, d, []byte("materialized"), materializedValue))
		for i := 1; i < RawKVCommandWALMaterializedRIDMaxOperations; i++ {
			entries = append(entries, batchpkg.Entry{Type: batchpkg.OpDelete, Key: []byte(fmt.Sprintf("delete-%03d", i))})
		}
		intent, err := d.newRawKVCommandWALIntentFromEntries(entries, true)
		if err != nil {
			t.Fatalf("newRawKVCommandWALIntentFromEntries: %v", err)
		}
		if intent.payloadFormat != commitlog.PayloadFormatRawKVBatchV2 || intent.externalRefs {
			releaseUnassignedCommandWALIntent(intent)
			t.Fatalf("256-op intent format=%d external=%t, want self-contained V2", intent.payloadFormat, intent.externalRefs)
		}
		releaseUnassignedCommandWALIntent(intent)

		entries = append(entries, batchpkg.Entry{Type: batchpkg.OpDelete, Key: []byte("delete-256")})
		intent, err = d.newRawKVCommandWALIntentFromEntries(entries, true)
		if err != nil {
			t.Fatalf("newRawKVCommandWALIntentFromEntries over bound: %v", err)
		}
		defer releaseUnassignedCommandWALIntent(intent)
		if intent.payloadFormat != commitlog.PayloadFormatRawKVBatchV1 || !intent.externalRefs {
			t.Fatalf("257-op intent format=%d external=%t, want V1 external", intent.payloadFormat, intent.externalRefs)
		}
	})

	t.Run("operation-count-cap-stops-materialization-validation", func(t *testing.T) {
		d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		pointer := appendCommandWALPointerEntry(t, d, []byte("pointer-000"), materializedValue)
		entries := make([]batchpkg.Entry, RawKVCommandWALMaterializedRIDMaxOperations+1)
		for i := range entries {
			entries[i] = pointer
			entries[i].Key = []byte(fmt.Sprintf("pointer-%03d", i))
		}
		entries[len(entries)-1].Value = []byte("conflicting-retained-value-after-cap")

		intent, err := d.newRawKVCommandWALIntentFromEntries(entries, true)
		if err != nil {
			t.Fatalf("newRawKVCommandWALIntentFromEntries after cap: %v", err)
		}
		defer releaseUnassignedCommandWALIntent(intent)
		if intent.payloadFormat != commitlog.PayloadFormatRawKVBatchV1 || !intent.externalRefs {
			t.Fatalf("257-op capped intent format=%d external=%t, want V1 external", intent.payloadFormat, intent.externalRefs)
		}
	})

	t.Run("frame-over-bound", func(t *testing.T) {
		d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		entries := []batchpkg.Entry{appendCommandWALPointerEntry(t, d, []byte("materialized"), materializedValue)}
		for i := 0; i < 16; i++ {
			entries = append(entries, batchpkg.Entry{Type: batchpkg.OpPut, Key: []byte(fmt.Sprintf("inline-%02d", i)), Value: bytes.Repeat([]byte{byte(i + 1)}, 64<<10)})
		}
		intent, err := d.newRawKVCommandWALIntentFromEntries(entries, true)
		if err != nil {
			t.Fatalf("newRawKVCommandWALIntentFromEntries: %v", err)
		}
		defer releaseUnassignedCommandWALIntent(intent)
		if intent.rawKVPlan.PayloadLen <= RawKVCommandWALMaterializedRIDMaxFrameBytes-RawKVCommandWALMaterializedRIDFrameReserve {
			t.Fatalf("fallback payload len=%d, want over materialized frame bound", intent.rawKVPlan.PayloadLen)
		}
		if intent.payloadFormat != commitlog.PayloadFormatRawKVBatchV1 || !intent.externalRefs {
			t.Fatalf("oversize-frame intent format=%d external=%t, want V1 external", intent.payloadFormat, intent.externalRefs)
		}
	})
}

func readFirstRawKVCommandWALEnvelope(t *testing.T, d *DB) commitlog.CommandEnvelope {
	t.Helper()
	if d == nil || d.commandJournal == nil {
		t.Fatal("missing command journal")
	}
	if err := d.commandJournal.FlushObserved(false); err != nil {
		t.Fatalf("FlushObserved: %v", err)
	}
	r, err := commitlog.NewReader(filepath.Join(WALDirPath(d.dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	env, err := r.ReadCommandFrame()
	if err != nil {
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	return env
}

func appendCommandWALPointerEntry(t *testing.T, d *DB, key, value []byte) batchpkg.Entry {
	t.Helper()
	ptrs, err := d.AppendValueLogValues([][]byte{value})
	if err != nil {
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("AppendValueLogValues returned %d pointers, want 1", len(ptrs))
	}
	path, fileID, ok := d.currentValueLogAppender().CurrentValueLogSegment()
	if !ok || path == "" || fileID != ptrs[0].FileID {
		t.Fatalf("CurrentValueLogSegment=(%q,%d,%t), pointer file_id=%d", path, fileID, ok, ptrs[0].FileID)
	}
	if err := d.RegisterValueLogSegment(path, fileID); err != nil {
		t.Fatalf("RegisterValueLogSegment: %v", err)
	}
	return batchpkg.Entry{Type: batchpkg.OpPut, Key: key, Value: value, IsPtr: true, ValuePtr: ptrs[0]}
}

func openCommandWALPointerDependencyTestDB(t *testing.T) (*DB, batchpkg.Entry) {
	t.Helper()
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		Durability:             DurabilityWALOnRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	entry := appendCommandWALPointerEntry(t, d, []byte("pointer-dependency"), bytes.Repeat([]byte("command-wal-dependency|"), 16))
	entry.Value = nil
	return d, entry
}

func rawKVOpTypeForTest(op commitlog.RawKVOp) batchpkg.OpType {
	switch op {
	case commitlog.RawKVOpSet, commitlog.RawKVOpSetRID, commitlog.RawKVOpSetMaterializedRID:
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
