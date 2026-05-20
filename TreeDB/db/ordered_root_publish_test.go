package db

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/storagemaintenance"
	"github.com/snissn/gomap/TreeDB/page"
)

type closeCountingUnsafeIterator struct {
	closes int
	err    error
}

func (it *closeCountingUnsafeIterator) Valid() bool { return false }
func (it *closeCountingUnsafeIterator) Next()       {}
func (it *closeCountingUnsafeIterator) Seek([]byte) {}
func (it *closeCountingUnsafeIterator) UnsafeKey() []byte {
	return nil
}
func (it *closeCountingUnsafeIterator) UnsafeValue() []byte {
	return nil
}
func (it *closeCountingUnsafeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return nil, page.ValuePtr{}, 0
}
func (it *closeCountingUnsafeIterator) Key() []byte   { return nil }
func (it *closeCountingUnsafeIterator) Value() []byte { return nil }
func (it *closeCountingUnsafeIterator) KeyCopy(dst []byte) []byte {
	return dst
}
func (it *closeCountingUnsafeIterator) ValueCopy(dst []byte) []byte {
	return dst
}
func (it *closeCountingUnsafeIterator) IsDeleted() bool { return false }
func (it *closeCountingUnsafeIterator) Error() error    { return it.err }
func (it *closeCountingUnsafeIterator) Close() error {
	it.closes++
	return nil
}
func (it *closeCountingUnsafeIterator) Domain() ([]byte, []byte) { return nil, nil }

type forgedStorageMaintenancePlan struct{}

func (forgedStorageMaintenancePlan) StorageMaintenancePlanToken() storagemaintenance.Plan {
	return storagemaintenance.Plan{}
}

type typedNilStorageMaintenancePlan struct{}

func (*typedNilStorageMaintenancePlan) StorageMaintenancePlanToken() storagemaintenance.Plan {
	panic("typed nil maintenance plan should fail closed before token access")
}

func mustRawKVCommandWALIntent(tb testing.TB, db *DB, key, value string) *CommandWALIntent {
	tb.Helper()
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte(key),
		Value: []byte(value),
	}})
	if err != nil {
		tb.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		tb.Fatalf("NewCommandWALIntent: %v", err)
	}
	if intent == nil {
		tb.Fatal("expected command WAL intent")
	}
	return intent
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextPassesAssignedLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	rootDelta := mustFrozenSystemMemtable(t, "root/a", "value-a")
	var seenCtx CommandWALPublishContext
	newSystemRoot, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     rootDelta.NewIterator(nil, nil),
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/a", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			seenCtx = ctx
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in context")
			}
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				t.Fatalf("rootIDs=%v, want one non-zero root", rootIDs)
			}
			sys := mustFrozenSystemMemtable(t, "system/root", strconv.FormatUint(rootIDs[0], 10))
			return sys.NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder: %v", err)
	}
	if newSystemRoot == 0 || len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("newSystemRoot=%d rootIDs=%v, want non-zero roots", newSystemRoot, rootIDs)
	}
	if seenCtx.AppliedCommandLSN == 0 {
		t.Fatalf("builder AppliedCommandLSN=0, want assigned LSN")
	}
	if got := db.State().AppliedCommandLSN; got != seenCtx.AppliedCommandLSN {
		t.Fatalf("state AppliedCommandLSN=%d, want builder LSN %d", got, seenCtx.AppliedCommandLSN)
	}
}

func TestErrOrderedRootCommandWALContextConcurrentModificationWrapsSentinel(t *testing.T) {
	err := errOrderedRootCommandWALContextConcurrentModification(1, 2, 3, 4)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("err=%v want ErrConcurrentModification", err)
	}
	for _, want := range []string{"user_root want=1 got=2", "system_root want=3 got=4"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err=%q missing %q", err, want)
		}
	}
}

func TestPublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderPreflightRunsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	wantErr := errors.New("preflight rejected")
	var contextBuilderCalled bool
	var systemBuilderCalled bool
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
		nil,
		func() error {
			return wantErr
		},
		mustRawKVCommandWALIntent(t, db, "cmd/preflight", "1"),
		func(ctx CommandWALPublishContext) ([]OrderedRootDeltaPublishInput, error) {
			contextBuilderCalled = true
			return nil, nil
		},
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			systemBuilderCalled = true
			return mustFrozenSystemMemtable(t, "system/preflight", "unexpected").NewIterator(nil, nil), nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want %v", err, wantErr)
	}
	if contextBuilderCalled || systemBuilderCalled {
		t.Fatalf("builders called after preflight failure: context=%v system=%v", contextBuilderCalled, systemBuilderCalled)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 because preflight rejected before command WAL append", got)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextPassesAssignedLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Set([]byte("root/b"), []byte("value-b")); err != nil {
		t.Fatalf("delta set: %v", err)
	}
	defer delta.Close()

	var seenLSN uint64
	beforeLSN := db.State().AppliedCommandLSN
	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: 0,
			Delta:    delta,
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/b", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			seenLSN = ctx.AppliedCommandLSN
			sys := mustFrozenSystemMemtable(t, "system/root-b", strconv.FormatUint(rootIDs[0], 10))
			return sys.NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder: %v", err)
	}
	if seenLSN == 0 || seenLSN <= beforeLSN {
		t.Fatalf("builder AppliedCommandLSN=%d, want non-zero LSN greater than previous %d", seenLSN, beforeLSN)
	}
	if got := db.State().AppliedCommandLSN; got != seenLSN {
		t.Fatalf("state AppliedCommandLSN=%d, want builder LSN %d", got, seenLSN)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextRejectsMissingFrame(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
		nil,
		nil,
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("system builder should not run without a command frame")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrCommandWALContextMissingFrame) {
		t.Fatalf("publish error=%v, want ErrCommandWALContextMissingFrame", err)
	}
	if errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("publish error=%v must not look like ErrCommandWALUnsupported", err)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 after missing frame rejection", got)
	}
	if err := db.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after missing frame rejection: %v", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceAllowsCommandWALWithoutLogicalFrame(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(
		nil,
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("ordinary unlogged system builder should not run in command WAL mode")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("ordinary publish error=%v want ErrCommandWALUnsupported", err)
	}

	rootDelta := mustFrozenSystemMemtable(t, "root/k", "v")
	newSystemRoot, rootIDs, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]OrderedRootDeltaPublishInput{{
			BaseRoot:                  0,
			Iter:                      rootDelta.NewIterator(nil, nil),
			StorageMaintenanceRewrite: true,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
			}
			return mustFrozenSystemMemtable(t, "maintenance/column/rewrite", "ok").NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("maintenance publish: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d want 0 for storage maintenance publish", got)
	}
	if err := db.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after maintenance publish: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("maintenance/column/rewrite"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot maintenance key: %v", err)
	}
	if got := string(entry.Value); got != "ok" {
		t.Fatalf("maintenance value=%q want ok", got)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsUnmarkedRootDelta(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	iter := mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil)
	defer iter.Close()
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     iter,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run for an unmarked root delta")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenanceRewriteMarkerMissing) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenanceRewriteMarkerMissing", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsForgedPlan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		forgedStorageMaintenancePlan{},
		[]OrderedRootDeltaPublishInput{{
			BaseRoot:                  0,
			Iter:                      mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
			StorageMaintenanceRewrite: true,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run for a forged maintenance plan")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenancePlanMissing) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePlanMissing", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsTypedNilPlan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	var plan *typedNilStorageMaintenancePlan
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		plan,
		[]OrderedRootDeltaPublishInput{{
			BaseRoot:                  0,
			Iter:                      mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
			StorageMaintenanceRewrite: true,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run for a typed nil maintenance plan")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenancePlanMissing) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePlanMissing", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsMissingPlan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		nil,
		nil,
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run without a maintenance plan")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenancePlanMissing) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePlanMissing", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsSystemOnly(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		nil,
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run for system-only maintenance publish")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenanceRootDeltaMissing) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenanceRootDeltaMissing", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsEmptyRootDelta(t *testing.T) {
	dir := t.TempDir()
	setupDB, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open setup DB: %v", err)
	}
	baseRoot, err := setupDB.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	if err := setupDB.Close(); err != nil {
		t.Fatalf("close setup DB: %v", err)
	}

	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	emptyDelta := mustFrozenSystemMemtable(t)
	_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]OrderedRootDeltaPublishInput{{
			BaseRoot:                  baseRoot,
			Iter:                      emptyDelta.NewIterator(nil, nil),
			StorageMaintenanceRewrite: true,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run for an empty maintenance root delta")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenanceRootDeltaEmpty) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenanceRootDeltaEmpty", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceWrapsRootDeltaIteratorErrorPreApply(t *testing.T) {
	dir := t.TempDir()
	setupDB, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open setup DB: %v", err)
	}
	baseRoot, err := setupDB.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	if err := setupDB.Close(); err != nil {
		t.Fatalf("close setup DB: %v", err)
	}

	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	wantErr := errors.New("maintenance root delta iterator materialization failure")
	iter := &closeCountingUnsafeIterator{err: wantErr}
	_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]OrderedRootDeltaPublishInput{{
			BaseRoot:                  baseRoot,
			Iter:                      iter,
			StorageMaintenanceRewrite: true,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run after root delta iterator failure")
			return nil, nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("maintenance publish error=%v want %v", err, wantErr)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
	if iter.closes != 1 {
		t.Fatalf("root delta iterator closes=%d want 1", iter.closes)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextRejectsMissingFrame(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
		nil,
		nil,
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("system builder should not run without a command frame")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrCommandWALContextMissingFrame) {
		t.Fatalf("publish error=%v, want ErrCommandWALContextMissingFrame", err)
	}
	if errors.Is(err, ErrCommandWALUnsupported) {
		t.Fatalf("publish error=%v must not look like ErrCommandWALUnsupported", err)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 after missing frame rejection", got)
	}
	if err := db.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after missing frame rejection: %v", err)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextNilSystemBuilderNamesBatch(t *testing.T) {
	var db *DB
	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(nil, nil, nil)
	if !errors.Is(err, ErrOrderedRootDeltaBatchGroupCommandWALContextNilSystemBuilder) {
		t.Fatalf("err=%v want %v", err, ErrOrderedRootDeltaBatchGroupCommandWALContextNilSystemBuilder)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextNilSystemBuilderIsExported(t *testing.T) {
	var db *DB
	_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(nil, nil, nil)
	if !errors.Is(err, ErrOrderedRootGroupCommandWALContextNilSystemBuilder) {
		t.Fatalf("err=%v want %v", err, ErrOrderedRootGroupCommandWALContextNilSystemBuilder)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextPreflightFailureDoesNotPoison(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Set([]byte("root/preflight"), []byte("value")); err != nil {
		t.Fatalf("delta set: %v", err)
	}
	defer delta.Close()

	wantErr := errors.New("preflight rejected")
	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{Delta: delta}},
		func() error { return wantErr },
		mustRawKVCommandWALIntent(t, db, "cmd/preflight", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("system builder should not run after preflight failure")
			return nil, nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 after preflight rejection", got)
	}
	if err := db.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("CheckCommandWALPublishReady after preflight rejection: %v", err)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextBuilderFailurePoisonsAfterAppend(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	rootDelta := mustFrozenSystemMemtable(t, "root/fail", "value")
	wantErr := errors.New("builder failure after command append")
	_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     rootDelta.NewIterator(nil, nil),
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/fail", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in builder")
			}
			return nil, wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 after failed publish", got)
	}
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextSystemBuilderErrorClosesReturnedIterator(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	iter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("system builder returned iterator with error")
	_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
		nil,
		mustRawKVCommandWALIntent(t, db, "cmd/system-iter-fail", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in system builder")
			}
			if len(rootIDs) != 0 {
				t.Fatalf("rootIDs=%v, want empty roots", rootIDs)
			}
			return iter, wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if iter.closes != 1 {
		t.Fatalf("system iterator closes=%d, want 1", iter.closes)
	}
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderFailureClosesReturnedIterators(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	iter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("context root builder failure after command append")
	_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(
		nil,
		mustRawKVCommandWALIntent(t, db, "cmd/context-fail", "1"),
		func(ctx CommandWALPublishContext) ([]OrderedRootDeltaPublishInput, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in context builder")
			}
			return []OrderedRootDeltaPublishInput{{Iter: iter}}, wantErr
		},
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("system builder should not run after context root builder failure")
			return nil, nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if iter.closes != 1 {
		t.Fatalf("context iterator closes=%d, want 1", iter.closes)
	}
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderFailureClosesReturnedDeltas(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Set([]byte("root/context-batch-fail"), []byte("value")); err != nil {
		t.Fatalf("delta set: %v", err)
	}
	wantErr := errors.New("context batch root builder failure after command append")
	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(
		nil,
		mustRawKVCommandWALIntent(t, db, "cmd/context-batch-fail", "1"),
		func(ctx CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in context builder")
			}
			return []OrderedRootDeltaBatchPublishInput{{Delta: delta}}, wantErr
		},
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("system builder should not run after context batch root builder failure")
			return nil, nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if err := delta.Set([]byte("after-close"), []byte("value")); !errors.Is(err, batch.ErrBatchClosed) {
		t.Fatalf("delta Set after failed context builder error=%v, want ErrBatchClosed", err)
	}
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextSystemBuilderErrorClosesReturnedIterator(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	iter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("batch system builder returned iterator with error")
	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
		nil,
		mustRawKVCommandWALIntent(t, db, "cmd/batch-system-iter-fail", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in system builder")
			}
			if len(rootIDs) != 0 {
				t.Fatalf("rootIDs=%v, want empty roots", rootIDs)
			}
			return iter, wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if iter.closes != 1 {
		t.Fatalf("system iterator closes=%d, want 1", iter.closes)
	}
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALSystemBuilderFailureClosesContextDeltas(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Set([]byte("root/context-batch-system-fail"), []byte("value")); err != nil {
		t.Fatalf("delta set: %v", err)
	}
	wantErr := errors.New("system builder failure after context batch delta")
	_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(
		nil,
		mustRawKVCommandWALIntent(t, db, "cmd/context-batch-system-fail", "1"),
		func(ctx CommandWALPublishContext) ([]OrderedRootDeltaBatchPublishInput, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in context builder")
			}
			return []OrderedRootDeltaBatchPublishInput{{Delta: delta}}, nil
		},
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in system builder")
			}
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				t.Fatalf("rootIDs=%v, want one published context root", rootIDs)
			}
			return nil, wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error=%v, want %v", err, wantErr)
	}
	if err := delta.Set([]byte("after-close"), []byte("value")); !errors.Is(err, batch.ErrBatchClosed) {
		t.Fatalf("delta Set after failed system builder error=%v, want ErrBatchClosed", err)
	}
	if err := db.CheckCommandWALPublishReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckCommandWALPublishReady error=%v, want ErrRecoveryRequired", err)
	}
}

func TestPublishOrderedRootIterator_WarmSparseDelta_PreservesPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	baseRoot, _, _, _, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("initial publish ordered root: %v", err)
	}
	oldPages := collectRootPageIDs(t, db, baseRoot)

	sparse := mustFrozenSystemMemtable(t, systemRangeKVs(2048, map[int]string{
		1024: "value-1024-updated",
	})...)
	newRoot, retired, _, stats, _, err := db.publishOrderedRootIterator(baseRoot, sparse.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("sparse publish ordered root: %v", err)
	}
	if newRoot == baseRoot {
		t.Fatalf("expected new root id after sparse publish")
	}
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 1 {
		t.Fatalf("warmNativeApplyAttempts=%d want 1", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 0 {
		t.Fatalf("warmRebuildFallbacks=%d want 0", stats.warmRebuildFallbacks)
	}
	if stats.warmPreservedPages == 0 {
		t.Fatalf("warmPreservedPages=%d want >0", stats.warmPreservedPages)
	}
	if len(retired) >= len(oldPages) {
		t.Fatalf("retired=%d want <%d", len(retired), len(oldPages))
	}
}

func TestPublishOrderedRootIterator_WarmDenseDelta_FallsBackToRebuild(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	baseRoot, _, _, _, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("initial publish ordered root: %v", err)
	}

	denseOverrides := make(map[int]string, 1024)
	for i := 0; i < 1024; i++ {
		denseOverrides[i] = "dense-updated"
	}
	dense := mustFrozenSystemMemtable(t, systemRangeKVs(2048, denseOverrides)...)
	newRoot, retired, _, stats, _, err := db.publishOrderedRootIterator(baseRoot, dense.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("dense publish ordered root: %v", err)
	}
	if newRoot == 0 {
		t.Fatal("expected non-zero root id")
	}
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 0 {
		t.Fatalf("warmNativeApplyAttempts=%d want 0", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 1 {
		t.Fatalf("warmRebuildFallbacks=%d want 1", stats.warmRebuildFallbacks)
	}
	if len(retired) == 0 {
		t.Fatal("expected rebuild fallback to retire old pages")
	}
}

func TestPublishOrderedRootIterator_ColdBuild_SkipsWarmCounters(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	initial := mustFrozenSystemMemtable(t, "sys/a", "sv-a")
	newRoot, retired, _, stats, _, err := db.publishOrderedRootIterator(0, initial.NewIterator(nil, nil), systemRootOrderedPublishOptions(db), false)
	if err != nil {
		t.Fatalf("cold publish ordered root: %v", err)
	}
	if newRoot == 0 {
		t.Fatal("expected non-zero root id")
	}
	if len(retired) != 0 {
		t.Fatalf("retired=%d want 0", len(retired))
	}
	if stats.warmAttempts != 0 || stats.warmNativeApplyAttempts != 0 || stats.warmRebuildFallbacks != 0 {
		t.Fatalf("unexpected warm stats: %+v", stats)
	}
}

func TestPublishOrderedRootIterator_PersistsAndPreservesMetaRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}
	sys := mustFrozenSystemMemtable(t, "sys/a", "sv")
	if _, err := db.PublishSystemRootIterator(sys.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system root: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	rootTable := mustFrozenSystemMemtable(t, "iter/a", "iv")
	newRoot, err := db.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish ordered root: %v", err)
	}
	after := db.State()
	if after == nil {
		t.Fatal("expected backend state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("system root changed: got %d want %d", after.SystemRootPageID, before.SystemRootPageID)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	entry, err := snap.GetEntryAtRoot(newRoot, []byte("iter/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("iter value=%q want %q", got, "iv")
	}
	_ = snap.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	reopenSnap := reopened.AcquireSnapshot()
	if reopenSnap == nil {
		t.Fatal("expected reopen snapshot")
	}
	defer reopenSnap.Close()
	entry, err = reopenSnap.GetEntryAtRoot(newRoot, []byte("iter/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("reopen iter value=%q want %q", got, "iv")
	}
}

func TestPublishOrderedRootGroup_PersistsSystemAndOrderedRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	systemTable := mustFrozenSystemMemtable(t, "sys/a", "sv")
	pointTable := mustFrozenSystemMemtable(t, "root/a", "rv")
	iterTable := mustFrozenSystemMemtable(t, "iter/a", "iv")
	newSystemRoot, rootIDs, err := db.PublishOrderedRootGroup(systemTable.NewIterator(nil, nil), []OrderedRootPublishInput{
		{BaseRoot: 0, Iter: pointTable.NewIterator(nil, nil)},
		{BaseRoot: 0, Iter: iterTable.NewIterator(nil, nil)},
	})
	if err != nil {
		t.Fatalf("publish ordered root group: %v", err)
	}
	if len(rootIDs) != 2 {
		t.Fatalf("rootIDs=%d want 2", len(rootIDs))
	}
	after := db.State()
	if after == nil {
		t.Fatal("expected backend state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != newSystemRoot {
		t.Fatalf("system root changed: got %d want %d", after.SystemRootPageID, newSystemRoot)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	sysEntry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/a"))
	if err != nil {
		t.Fatalf("GetEntry(sys): %v", err)
	}
	if got := string(sysEntry.Value); got != "sv" {
		t.Fatalf("system value=%q want %q", got, "sv")
	}
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(root): %v", err)
	}
	if got := string(entry.Value); got != "rv" {
		t.Fatalf("root value=%q want %q", got, "rv")
	}
	entry, err = snap.GetEntryAtRoot(rootIDs[1], []byte("iter/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("iter value=%q want %q", got, "iv")
	}
	_ = snap.Close()

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	reopenSnap := reopened.AcquireSnapshot()
	if reopenSnap == nil {
		t.Fatal("expected reopen snapshot")
	}
	defer reopenSnap.Close()
	sysEntry, err = reopenSnap.GetEntryAtRoot(newSystemRoot, []byte("sys/a"))
	if err != nil {
		t.Fatalf("reopen GetEntry(sys): %v", err)
	}
	if got := string(sysEntry.Value); got != "sv" {
		t.Fatalf("reopen system value=%q want %q", got, "sv")
	}
	entry, err = reopenSnap.GetEntryAtRoot(rootIDs[0], []byte("root/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(root): %v", err)
	}
	if got := string(entry.Value); got != "rv" {
		t.Fatalf("reopen root value=%q want %q", got, "rv")
	}
	entry, err = reopenSnap.GetEntryAtRoot(rootIDs[1], []byte("iter/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(iter): %v", err)
	}
	if got := string(entry.Value); got != "iv" {
		t.Fatalf("reopen iter value=%q want %q", got, "iv")
	}
}

func TestPublishOrderedRootGroup_ClosesUnconsumedIteratorsOnPolicyError(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	first := &closeCountingUnsafeIterator{}
	second := &closeCountingUnsafeIterator{}
	_, _, err = db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{
		{Iter: first, StoragePolicy: OrderedRootStoragePolicy(255)},
		{Iter: second, StoragePolicy: OrderedRootStoragePagerLeaves},
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if first.closes != 1 {
		t.Fatalf("first iterator closes=%d want 1", first.closes)
	}
	if second.closes != 1 {
		t.Fatalf("second iterator closes=%d want 1", second.closes)
	}
}

func TestPublishOrderedRootDeltaGroups_CloseUnconsumedIteratorsOnPolicyError(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	builderCalled := false
	buildSystemIter := func([]uint64) (iterator.UnsafeIterator, error) {
		builderCalled = true
		return nil, nil
	}
	for name, publish := range map[string]func([]OrderedRootDeltaPublishInput) error{
		"system builder": func(ordered []OrderedRootDeltaPublishInput) error {
			_, _, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, buildSystemIter)
			return err
		},
		"system delta builder": func(ordered []OrderedRootDeltaPublishInput) error {
			_, _, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, buildSystemIter)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			builderCalled = false
			first := &closeCountingUnsafeIterator{}
			second := &closeCountingUnsafeIterator{}
			err := publish([]OrderedRootDeltaPublishInput{
				{Iter: first, StoragePolicy: OrderedRootStoragePolicy(255)},
				{Iter: second, StoragePolicy: OrderedRootStoragePagerLeaves},
			})
			if err == nil {
				t.Fatal("expected publish error")
			}
			if builderCalled {
				t.Fatal("system builder should not run after policy error")
			}
			if first.closes != 1 {
				t.Fatalf("first iterator closes=%d want 1", first.closes)
			}
			if second.closes != 1 {
				t.Fatalf("second iterator closes=%d want 1", second.closes)
			}
		})
	}
}

func TestPublishOrderedRootGroup_UsesPerRootStoragePolicy(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	db.SetLeafPageLog(leafLog)
	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	compressedTable := mustFrozenSystemMemtable(t, "doc/u1", "document")
	fastTable := mustFrozenSystemMemtable(t, "idx/email/u1", "")
	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{
		{BaseRoot: 0, Iter: compressedTable.NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves},
		{BaseRoot: 0, Iter: fastTable.NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
	})
	if err != nil {
		t.Fatalf("publish ordered root group: %v", err)
	}
	if len(rootIDs) != 2 {
		t.Fatalf("rootIDs=%d want 2", len(rootIDs))
	}
	requireLeafLogRootChildren(t, db, rootIDs[0])
	if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), rootIDs[1]); err != nil {
		t.Fatalf("inspect fast root %d: %v", rootIDs[1], err)
	} else if allLeafRefs {
		t.Fatalf("fast root id=%d, want pager-backed leaves", rootIDs[1])
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("doc/u1"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(compressed): %v", err)
	}
	if got := string(entry.Value); got != "document" {
		t.Fatalf("compressed value=%q want document", got)
	}
	entry, err = snap.GetEntryAtRoot(rootIDs[1], []byte("idx/email/u1"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(fast): %v", err)
	}
	if got := string(entry.Value); got != "" {
		t.Fatalf("fast index value=%q want empty", got)
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_UsesPerRootStoragePolicy(t *testing.T) {
	t.Run("pager leaves override value-log default", func(t *testing.T) {
		dir := t.TempDir()
		db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
		db.SetLeafPageLog(leafLog)
		defer func() {
			_ = leafLog.Close()
			_ = db.Close()
		}()

		_, baseRootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
			BaseRoot:      0,
			Iter:          mustFrozenSystemMemtable(t, "doc/a", "va").NewIterator(nil, nil),
			StoragePolicy: OrderedRootStoragePagerLeaves,
		}})
		if err != nil {
			t.Fatalf("publish base root: %v", err)
		}

		_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
			BaseRoot:      baseRootIDs[0],
			Iter:          mustFrozenSystemMemtable(t, "doc/b", "vb").NewIterator(nil, nil),
			StoragePolicy: OrderedRootStoragePagerLeaves,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		})
		if err != nil {
			t.Fatalf("publish delta root: %v", err)
		}
		if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), rootIDs[0]); err != nil {
			t.Fatalf("inspect delta root %d: %v", rootIDs[0], err)
		} else if allLeafRefs {
			t.Fatalf("delta root id=%d, want pager-backed leaves", rootIDs[0])
		}

		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		defer snap.Close()
		for key, want := range map[string]string{"doc/a": "va", "doc/b": "vb"} {
			entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(%s): %v", key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("%s=%q want %q", key, got, want)
			}
		}
	})

	t.Run("value-log leaves override pager default", func(t *testing.T) {
		dir := t.TempDir()
		db, err := Open(Options{Dir: dir})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
		db.SetLeafPageLog(leafLog)
		defer func() {
			_ = leafLog.Close()
			_ = db.Close()
		}()

		_, baseRootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
			BaseRoot:      0,
			Iter:          mustFrozenSystemMemtable(t, "doc/a", "va").NewIterator(nil, nil),
			StoragePolicy: OrderedRootStorageValueLogLeaves,
		}})
		if err != nil {
			t.Fatalf("publish base root: %v", err)
		}

		_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
			BaseRoot:      baseRootIDs[0],
			Iter:          mustFrozenSystemMemtable(t, "doc/b", "vb").NewIterator(nil, nil),
			StoragePolicy: OrderedRootStorageValueLogLeaves,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		})
		if err != nil {
			t.Fatalf("publish delta root: %v", err)
		}
		requireLeafLogRootChildren(t, db, rootIDs[0])

		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		defer snap.Close()
		for key, want := range map[string]string{"doc/a": "va", "doc/b": "vb"} {
			entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(%s): %v", key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("%s=%q want %q", key, got, want)
			}
		}
	})
}

func TestPublishOrderedRootGroupWithSystemBuilder_PersistsSystemDescriptorWithOrderedRootIDs(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	primaryTable := mustFrozenSystemMemtable(t, "doc/u1", "document")
	indexTable := mustFrozenSystemMemtable(t, "idx/email/u1", "")
	var builderRootIDs []uint64
	newSystemRoot, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{
		{BaseRoot: 0, Iter: primaryTable.NewIterator(nil, nil)},
		{BaseRoot: 0, Iter: indexTable.NewIterator(nil, nil)},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		builderRootIDs = append([]uint64(nil), rootIDs...)
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/email_idx", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish ordered root group with system builder: %v", err)
	}
	if len(rootIDs) != 2 {
		t.Fatalf("rootIDs=%d want 2", len(rootIDs))
	}
	if !reflect.DeepEqual(builderRootIDs, rootIDs) {
		t.Fatalf("builder root IDs=%v want %v", builderRootIDs, rootIDs)
	}
	after := db.State()
	if after == nil {
		t.Fatal("expected backend state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != newSystemRoot {
		t.Fatalf("system root changed: got %d want %d", after.SystemRootPageID, newSystemRoot)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	sysEntry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/collections/users/primary"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(system primary descriptor): %v", err)
	}
	if got, want := string(sysEntry.Value), strconv.FormatUint(rootIDs[0], 10); got != want {
		t.Fatalf("primary descriptor root=%q want %q", got, want)
	}
	sysEntry, err = snap.GetEntryAtRoot(newSystemRoot, []byte("sys/collections/users/email_idx"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(system index descriptor): %v", err)
	}
	if got, want := string(sysEntry.Value), strconv.FormatUint(rootIDs[1], 10); got != want {
		t.Fatalf("index descriptor root=%q want %q", got, want)
	}
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("doc/u1"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(primary): %v", err)
	}
	if got := string(entry.Value); got != "document" {
		t.Fatalf("primary value=%q want %q", got, "document")
	}
	entry, err = snap.GetEntryAtRoot(rootIDs[1], []byte("idx/email/u1"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(index): %v", err)
	}
	if got := string(entry.Value); got != "" {
		t.Fatalf("index value=%q want empty", got)
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_PreservesOmittedBaseEntries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
		"root/b", "vb",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}

	delta := mustFrozenSystemMemtable(t,
		"root/b", "vb2",
		"root/c", "vc",
	)
	newSystemRoot, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
		}
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delta group: %v", err)
	}
	if newSystemRoot == 0 || len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("newSystemRoot=%d rootIDs=%v", newSystemRoot, rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for key, want := range map[string]string{
		"root/a": "va",
		"root/b": "vb2",
		"root/c": "vc",
	} {
		entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte(key))
		if err != nil {
			t.Fatalf("GetEntryAtRoot(%s): %v", key, err)
		}
		if got := string(entry.Value); got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_ReportsPublishStats(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	delta := mustFrozenSystemMemtable(t, "root/b", "vb")
	if _, _, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	}); err != nil {
		t.Fatalf("publish delta group: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.calls_total"]; got != "1" {
		t.Fatalf("calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.roots_total"]; got != "1" {
		t.Fatalf("roots stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.avg_roots_per_call"]; got != "1.000" {
		t.Fatalf("avg roots stat=%q want 1.000", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.errors_total"]; got != "0" {
		t.Fatalf("errors stat=%q want 0", got)
	}
	if _, ok := stats["treedb.publish.ordered_root_delta_group.write_lock_wait_share_pct"]; !ok {
		t.Fatalf("missing ordered root delta write lock wait share stat")
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "1" {
		t.Fatalf("root apply calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.system_apply_calls_total"]; got != "1" {
		t.Fatalf("system apply calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.finalize_calls_total"]; got != "1" {
		t.Fatalf("finalize calls stat=%q want 1", got)
	}
	for _, key := range []string{
		"treedb.publish.ordered_root_delta_group.preflight_ns_total",
		"treedb.publish.ordered_root_delta_group.root_apply_ns_total",
		"treedb.publish.ordered_root_delta_group.root_apply_ops_total",
		"treedb.publish.ordered_root_delta_group.root_apply_node_loads_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_loads_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_cache_hits_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_reader_calls_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_view_reads_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_scratch_reads_total",
		"treedb.publish.ordered_root_delta_group.root_apply_pager_node_bytes_read_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_node_bytes_read_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_record_hint_bytes_read_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_pages_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_page_bytes_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_pager_leaf_page_bytes_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_page_bytes_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_leaf_log_record_hint_bytes_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_internal_page_bytes_written_total",
		"treedb.publish.ordered_root_delta_group.root_apply_internal_child_refs_total",
		"treedb.publish.ordered_root_delta_group.root_apply_internal_page_child_refs_total",
		"treedb.publish.ordered_root_delta_group.root_apply_internal_leaf_log_refs_total",
		"treedb.publish.ordered_root_delta_group.root_apply_internal_leaf_log_ref_copies_total",
		"treedb.publish.ordered_root_delta_group.system_build_ns_total",
		"treedb.publish.ordered_root_delta_group.system_apply_ns_total",
		"treedb.publish.ordered_root_delta_group.system_apply_ops_total",
		"treedb.publish.ordered_root_delta_group.system_apply_node_loads_total",
		"treedb.publish.ordered_root_delta_group.finalize_ns_total",
	} {
		if _, ok := stats[key]; !ok {
			t.Fatalf("missing ordered root delta phase stat %q", key)
		}
	}
}

func TestPublishOrderedRootDeltaGroupPreflightFailureDoesNotCountRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	delta := mustFrozenSystemMemtable(t, "root/b", "vb")
	wantErr := errors.New("preflight failed")
	_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func() error {
		return wantErr
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want %v", err, wantErr)
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.calls_total"]; got != "1" {
		t.Fatalf("calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.errors_total"]; got != "1" {
		t.Fatalf("errors stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.roots_total"]; got != "0" {
		t.Fatalf("roots stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "0" {
		t.Fatalf("root apply calls stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.finalize_calls_total"]; got != "0" {
		t.Fatalf("finalize calls stat=%q want 0", got)
	}
}

func TestPublishOrderedRootDeltaGroupSystemBuilderFailureDoesNotCountRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	delta := mustFrozenSystemMemtable(t, "root/b", "vb")
	wantErr := errors.New("system builder failed")
	_, _, err = db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want %v", err, wantErr)
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.calls_total"]; got != "1" {
		t.Fatalf("calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.errors_total"]; got != "1" {
		t.Fatalf("errors stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.roots_total"]; got != "0" {
		t.Fatalf("roots stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "1" {
		t.Fatalf("root apply calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.system_apply_calls_total"]; got != "0" {
		t.Fatalf("system apply calls stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.finalize_calls_total"]; got != "0" {
		t.Fatalf("finalize calls stat=%q want 0", got)
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_AppliesDeletes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
		"root/b", "vb",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	delta, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new delta table: %v", err)
	}
	delta.Delete([]byte("root/b"))
	delta.Freeze()

	_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delta group: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(root/a): %v", err)
	}
	if got, want := string(entry.Value), "va"; got != want {
		t.Fatalf("root/a=%q want %q", got, want)
	}
	if _, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/b")); err == nil {
		t.Fatal("root/b still exists after delta delete")
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_PreservesOmittedBaseEntries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
		"root/b", "vb",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}

	deltaTable := mustFrozenSystemMemtable(t,
		"root/b", "vb2",
		"root/c", "vc",
	)
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	newSystemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot: baseRoot,
		Delta:    delta,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
		}
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delta batch group: %v", err)
	}
	if newSystemRoot == 0 || len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("newSystemRoot=%d rootIDs=%v", newSystemRoot, rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for key, want := range map[string]string{
		"root/a": "va",
		"root/b": "vb2",
		"root/c": "vc",
	} {
		entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte(key))
		if err != nil {
			t.Fatalf("GetEntryAtRoot(%s): %v", key, err)
		}
		if got := string(entry.Value); got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticPublishesUnderSharedWriteGate(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}

	deltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	done := make(chan struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}, 1)
	db.writeMu.RLock()
	locked := true
	go func() {
		systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: baseRoot,
			Delta:    delta,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				return nil, errors.New("unexpected optimistic root IDs")
			}
			mt, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
			if err != nil {
				return nil, err
			}
			mt.Set([]byte("sys/collections/users/primary"), []byte(strconv.FormatUint(rootIDs[0], 10)))
			mt.Freeze()
			return mt.NewIterator(nil, nil), nil
		})
		done <- struct {
			systemRoot uint64
			rootIDs    []uint64
			err        error
		}{systemRoot: systemRoot, rootIDs: rootIDs, err: err}
	}()

	var result struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		db.writeMu.RUnlock()
		locked = false
		t.Fatal("batch root delta group publish blocked behind an existing shared write gate")
	}
	if locked {
		db.writeMu.RUnlock()
	}
	if result.err != nil {
		t.Fatalf("publish delta batch group: %v", result.err)
	}
	if result.systemRoot == 0 || len(result.rootIDs) != 1 || result.rootIDs[0] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero system root and one non-zero root", result.systemRoot, result.rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for key, want := range map[string]string{
		"root/a": "va",
		"root/b": "vb",
	} {
		entry, err := snap.GetEntryAtRoot(result.rootIDs[0], []byte(key))
		if err != nil {
			t.Fatalf("GetEntryAtRoot(%s): %v", key, err)
		}
		if got := string(entry.Value); got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticAppliesRootsInParallel(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRootA, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"a/1", "base-a",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root a: %v", err)
	}
	baseRootB, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"b/1", "base-b",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root b: %v", err)
	}

	deltaAIter := mustFrozenSystemMemtable(t, "a/2", "delta-a").NewIterator(nil, nil)
	deltaA, err := OrderedRootDeltaBatchFromIterator(deltaAIter)
	_ = deltaAIter.Close()
	if err != nil {
		t.Fatalf("delta a batch: %v", err)
	}
	defer func() { _ = deltaA.Close() }()

	deltaBIter := mustFrozenSystemMemtable(t, "b/2", "delta-b").NewIterator(nil, nil)
	deltaB, err := OrderedRootDeltaBatchFromIterator(deltaBIter)
	_ = deltaBIter.Close()
	if err != nil {
		t.Fatalf("delta b batch: %v", err)
	}
	defer func() { _ = deltaB.Close() }()

	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{BaseRoot: baseRootA, Delta: deltaA, ParallelApply: true},
		{BaseRoot: baseRootB, Delta: deltaB, ParallelApply: true},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			return nil, errors.New("unexpected optimistic root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delta batch group: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero system root and two non-zero roots", systemRoot, rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for rootIdx, kv := range []map[string]string{
		{"a/1": "base-a", "a/2": "delta-a"},
		{"b/1": "base-b", "b/2": "delta-b"},
	} {
		for key, want := range kv {
			entry, err := snap.GetEntryAtRoot(rootIDs[rootIdx], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(root=%d key=%s): %v", rootIdx, key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("root=%d key=%s got=%q want %q", rootIdx, key, got, want)
			}
		}
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"]; got != "1" {
		t.Fatalf("parallel groups stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"]; got != "2" {
		t.Fatalf("parallel roots stat=%q want 2", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "2" {
		t.Fatalf("root apply calls stat=%q want 2", got)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticColdBuildsRootsInParallel(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	deltaAIter := mustFrozenSystemMemtable(t, "a/1", "delta-a").NewIterator(nil, nil)
	deltaA, err := OrderedRootDeltaBatchFromIterator(deltaAIter)
	_ = deltaAIter.Close()
	if err != nil {
		t.Fatalf("delta a batch: %v", err)
	}
	defer func() { _ = deltaA.Close() }()

	deltaBIter := mustFrozenSystemMemtable(t, "b/1", "delta-b").NewIterator(nil, nil)
	deltaB, err := OrderedRootDeltaBatchFromIterator(deltaBIter)
	_ = deltaBIter.Close()
	if err != nil {
		t.Fatalf("delta b batch: %v", err)
	}
	defer func() { _ = deltaB.Close() }()

	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{BaseRoot: 0, Delta: deltaA, ParallelApply: true},
		{BaseRoot: 0, Delta: deltaB, ParallelApply: true},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			return nil, errors.New("unexpected optimistic cold root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish cold delta batch group: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero system root and two non-zero roots", systemRoot, rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for rootIdx, kv := range []map[string]string{
		{"a/1": "delta-a"},
		{"b/1": "delta-b"},
	} {
		for key, want := range kv {
			entry, err := snap.GetEntryAtRoot(rootIDs[rootIdx], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(root=%d key=%s): %v", rootIdx, key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("root=%d key=%s got=%q want %q", rootIdx, key, got, want)
			}
		}
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"]; got != "1" {
		t.Fatalf("parallel groups stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"]; got != "2" {
		t.Fatalf("parallel roots stat=%q want 2", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "2" {
		t.Fatalf("root apply calls stat=%q want 2", got)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticMixedOptInStaysSerialized(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRootA, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"a/1", "base-a",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root a: %v", err)
	}
	baseRootB, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"b/1", "base-b",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root b: %v", err)
	}

	deltaAIter := mustFrozenSystemMemtable(t, "a/2", "delta-a").NewIterator(nil, nil)
	deltaA, err := OrderedRootDeltaBatchFromIterator(deltaAIter)
	_ = deltaAIter.Close()
	if err != nil {
		t.Fatalf("delta a batch: %v", err)
	}
	defer func() { _ = deltaA.Close() }()

	deltaBIter := mustFrozenSystemMemtable(t, "b/2", "delta-b").NewIterator(nil, nil)
	deltaB, err := OrderedRootDeltaBatchFromIterator(deltaBIter)
	_ = deltaBIter.Close()
	if err != nil {
		t.Fatalf("delta b batch: %v", err)
	}
	defer func() { _ = deltaB.Close() }()

	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{BaseRoot: baseRootA, Delta: deltaA, ParallelApply: true},
		{BaseRoot: baseRootB, Delta: deltaB},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			return nil, errors.New("unexpected optimistic root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delta batch group: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero system root and two non-zero roots", systemRoot, rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for rootIdx, kv := range []map[string]string{
		{"a/1": "base-a", "a/2": "delta-a"},
		{"b/1": "base-b", "b/2": "delta-b"},
	} {
		for key, want := range kv {
			entry, err := snap.GetEntryAtRoot(rootIDs[rootIdx], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(root=%d key=%s): %v", rootIdx, key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("root=%d key=%s got=%q want %q", rootIdx, key, got, want)
			}
		}
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"]; got != "0" {
		t.Fatalf("parallel groups stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"]; got != "0" {
		t.Fatalf("parallel roots stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "2" {
		t.Fatalf("root apply calls stat=%q want 2", got)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticMixedOptInParallelizesEligibleRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRootA, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "a/1", "base-a").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root a: %v", err)
	}
	baseRootB, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "b/1", "base-b").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root b: %v", err)
	}
	baseRootC, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "c/1", "base-c").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root c: %v", err)
	}

	deltaAIter := mustFrozenSystemMemtable(t, "a/2", "delta-a").NewIterator(nil, nil)
	deltaA, err := OrderedRootDeltaBatchFromIterator(deltaAIter)
	_ = deltaAIter.Close()
	if err != nil {
		t.Fatalf("delta a batch: %v", err)
	}
	defer func() { _ = deltaA.Close() }()

	deltaBIter := mustFrozenSystemMemtable(t, "b/2", "delta-b").NewIterator(nil, nil)
	deltaB, err := OrderedRootDeltaBatchFromIterator(deltaBIter)
	_ = deltaBIter.Close()
	if err != nil {
		t.Fatalf("delta b batch: %v", err)
	}
	defer func() { _ = deltaB.Close() }()

	deltaCIter := mustFrozenSystemMemtable(t, "c/2", "delta-c").NewIterator(nil, nil)
	deltaC, err := OrderedRootDeltaBatchFromIterator(deltaCIter)
	_ = deltaCIter.Close()
	if err != nil {
		t.Fatalf("delta c batch: %v", err)
	}
	defer func() { _ = deltaC.Close() }()

	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{BaseRoot: baseRootA, Delta: deltaA, ParallelApply: true},
		{BaseRoot: baseRootB, Delta: deltaB},
		{BaseRoot: baseRootC, Delta: deltaC, ParallelApply: true},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 3 || rootIDs[0] == 0 || rootIDs[1] == 0 || rootIDs[2] == 0 {
			return nil, errors.New("unexpected mixed optimistic root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
			"sys/collections/users/c", strconv.FormatUint(rootIDs[2], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish mixed delta batch group: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 3 || rootIDs[0] == 0 || rootIDs[1] == 0 || rootIDs[2] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero system root and three non-zero roots", systemRoot, rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for rootIdx, kv := range []map[string]string{
		{"a/1": "base-a", "a/2": "delta-a"},
		{"b/1": "base-b", "b/2": "delta-b"},
		{"c/1": "base-c", "c/2": "delta-c"},
	} {
		for key, want := range kv {
			entry, err := snap.GetEntryAtRoot(rootIDs[rootIdx], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(root=%d key=%s): %v", rootIdx, key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("root=%d key=%s got=%q want %q", rootIdx, key, got, want)
			}
		}
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"]; got != "1" {
		t.Fatalf("parallel groups stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"]; got != "2" {
		t.Fatalf("parallel roots stat=%q want 2", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "3" {
		t.Fatalf("root apply calls stat=%q want 3", got)
	}
}

func TestApplyOrderedRootDeltaBatchGroupRoots_MixedOptInStartsParallelBeforeSerial(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}

	baseRootB, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"b/1", "base-b",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root B: %v", err)
	}
	if baseRootB == 0 {
		t.Fatal("expected non-zero base root B")
	}

	deltaAIter := mustFrozenSystemMemtable(t, "a/1", "delta-a").NewIterator(nil, nil)
	deltaA, err := OrderedRootDeltaBatchFromIterator(deltaAIter)
	_ = deltaAIter.Close()
	if err != nil {
		t.Fatalf("delta a batch: %v", err)
	}
	defer func() { _ = deltaA.Close() }()

	deltaBIter := mustFrozenSystemMemtable(t, "b/2", "delta-b").NewIterator(nil, nil)
	deltaB, err := OrderedRootDeltaBatchFromIterator(deltaBIter)
	_ = deltaBIter.Close()
	if err != nil {
		t.Fatalf("delta b batch: %v", err)
	}
	defer func() { _ = deltaB.Close() }()

	deltaCIter := mustFrozenSystemMemtable(t, "c/1", "delta-c").NewIterator(nil, nil)
	deltaC, err := OrderedRootDeltaBatchFromIterator(deltaCIter)
	_ = deltaCIter.Close()
	if err != nil {
		t.Fatalf("delta c batch: %v", err)
	}
	defer func() { _ = deltaC.Close() }()

	parallelStarted := make(chan struct{})
	var parallelAllocCalls atomic.Int32
	var closeParallelStarted sync.Once
	coldAlloc := &orderedRootDeltaBatchGroupTestAllocator{
		delegate: &pagerAllocator{p: idx.pager},
		onAlloc: func() {
			if parallelAllocCalls.Add(1) >= 2 {
				closeParallelStarted.Do(func() { close(parallelStarted) })
			}
		},
	}

	var serialStartedBeforeParallel atomic.Bool
	serialAlloc := &orderedRootDeltaBatchGroupTestAllocator{
		delegate: idx.allocator,
		onAlloc: func() {
			select {
			case <-parallelStarted:
			default:
				serialStartedBeforeParallel.Store(true)
			}
		},
	}

	results, parallel := db.applyOrderedRootDeltaBatchGroupRoots(idx, []OrderedRootDeltaBatchPublishInput{
		{BaseRoot: 0, Delta: deltaA, ParallelApply: true},
		{BaseRoot: baseRootB, Delta: deltaB},
		{BaseRoot: 0, Delta: deltaC, ParallelApply: true},
	}, serialAlloc, coldAlloc)
	if !parallel {
		t.Fatal("expected mixed group to use parallel apply")
	}
	if serialStartedBeforeParallel.Load() {
		t.Fatal("serialized root apply started before both eligible parallel roots started")
	}
	if parallelAllocCalls.Load() < 2 {
		t.Fatalf("parallel allocator calls=%d want at least 2", parallelAllocCalls.Load())
	}
	if len(results) != 3 {
		t.Fatalf("results len=%d want 3", len(results))
	}
	for idx, result := range results {
		if result.err != nil {
			t.Fatalf("result %d err: %v", idx, result.err)
		}
		if result.rootID == 0 {
			t.Fatalf("result %d rootID=0", idx)
		}
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_SerializedColdBatchCanPreserveDeletes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Delete([]byte("doc/u1")); err != nil {
		t.Fatalf("delete delta: %v", err)
	}
	defer func() { _ = delta.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:                  0,
		Delta:                     delta,
		IncludeDeletedOnColdBuild: true,
	}}, func() error {
		return nil
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			return nil, errors.New("unexpected cold tombstone root ID")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish cold delete batch group: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	it, err := snap.IteratorAtRootWithOptions(rootIDs[0], []byte("doc/u1"), nil, IteratorOptions{IncludeTombstones: true})
	if err != nil {
		t.Fatalf("IteratorAtRootWithOptions: %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() || !bytes.Equal(it.UnsafeKey(), []byte("doc/u1")) || !it.IsDeleted() {
		t.Fatalf("iterator valid/key/deleted=%v/%q/%v, want tombstone doc/u1", it.Valid(), it.UnsafeKey(), it.Valid() && it.IsDeleted())
	}
}

type orderedRootDeltaBatchGroupTestAllocator struct {
	delegate interface {
		Alloc(uint64) (uint64, error)
	}
	onAlloc func()
}

func (a *orderedRootDeltaBatchGroupTestAllocator) Alloc(hint uint64) (uint64, error) {
	if a.onAlloc != nil {
		a.onAlloc()
	}
	return a.delegate.Alloc(hint)
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticInvalidatesLeafGenerationSubtreeStats(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}

	deltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	maxSeedPageID := idx.pager.PageCount() + 1024
	for pageID := uint64(1); pageID <= maxSeedPageID; pageID++ {
		db.storeLeafGenerationSubtreeStats(pageID, leafGenerationSubtreeStats{
			1: {LivePages: 1, LiveBytes: 4096},
		})
	}

	done := make(chan struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}, 1)
	db.writeMu.RLock()
	locked := true
	go func() {
		systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: baseRoot,
			Delta:    delta,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				return nil, errors.New("unexpected optimistic root IDs")
			}
			return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		})
		done <- struct {
			systemRoot uint64
			rootIDs    []uint64
			err        error
		}{systemRoot: systemRoot, rootIDs: rootIDs, err: err}
	}()

	var result struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		db.writeMu.RUnlock()
		locked = false
		t.Fatal("optimistic batch root publish did not finish under a shared write gate")
	}
	if locked {
		db.writeMu.RUnlock()
	}
	if result.err != nil {
		t.Fatalf("publish delta batch group: %v", result.err)
	}
	if len(result.rootIDs) != 1 || result.rootIDs[0] == 0 || result.systemRoot == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero roots", result.systemRoot, result.rootIDs)
	}
	if result.rootIDs[0] > maxSeedPageID || result.systemRoot > maxSeedPageID {
		t.Fatalf("test did not seed new roots: maxSeed=%d systemRoot=%d rootIDs=%v", maxSeedPageID, result.systemRoot, result.rootIDs)
	}
	if stats, ok := db.loadLeafGenerationSubtreeStats(result.rootIDs[0]); ok {
		t.Fatalf("new non-system root retained stale leaf-generation subtree stats: %v", stats)
	}
	if stats, ok := db.loadLeafGenerationSubtreeStats(result.systemRoot); ok {
		t.Fatalf("new system root retained stale leaf-generation subtree stats: %v", stats)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticPreservesConcurrentUserRootWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	deltaTable := mustFrozenSystemMemtable(t, "root/b", "vb")
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	done := make(chan struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}, 1)
	rawWriteStarted := false
	db.writeMu.RLock()
	locked := true
	go func() {
		systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: baseRoot,
			Delta:    delta,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				return nil, errors.New("unexpected optimistic root IDs")
			}
			if !rawWriteStarted {
				rawWriteStarted = true
				done := make(chan error, 1)
				go func() {
					b := db.NewBatch()
					defer func() { _ = b.Close() }()
					if err := b.Set([]byte("raw/user-root-key"), []byte("raw-value")); err != nil {
						done <- err
						return
					}
					done <- b.Write()
				}()
				select {
				case err := <-done:
					if err != nil {
						return nil, err
					}
				case <-time.After(2 * time.Second):
					return nil, errors.New("raw user-root write blocked during optimistic batch root publish")
				}
			}
			mt, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
			if err != nil {
				return nil, err
			}
			mt.Set([]byte("sys/collections/users/primary"), []byte(strconv.FormatUint(rootIDs[0], 10)))
			mt.Freeze()
			return mt.NewIterator(nil, nil), nil
		})
		done <- struct {
			systemRoot uint64
			rootIDs    []uint64
			err        error
		}{systemRoot: systemRoot, rootIDs: rootIDs, err: err}
	}()
	var result struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		db.writeMu.RUnlock()
		locked = false
		t.Fatal("optimistic batch root publish did not finish under a shared write gate")
	}
	if locked {
		db.writeMu.RUnlock()
	}
	if result.err != nil {
		t.Fatalf("publish delta batch group: %v", result.err)
	}
	newSystemRoot := result.systemRoot
	rootIDs := result.rootIDs
	if newSystemRoot == 0 || len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("newSystemRoot=%d rootIDs=%v, want non-zero system root and one non-zero root", newSystemRoot, rootIDs)
	}
	if !rawWriteStarted {
		t.Fatal("raw write was not attempted")
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	raw, err := snap.Get([]byte("raw/user-root-key"))
	if err != nil {
		t.Fatalf("Get raw user-root key: %v", err)
	}
	if got := string(raw); got != "raw-value" {
		t.Fatalf("raw user-root key=%q want raw-value", got)
	}
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/b"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(root/b): %v", err)
	}
	if got := string(entry.Value); got != "vb" {
		t.Fatalf("root/b=%q want vb", got)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_OptimisticRebasesSystemDeltaAfterConcurrentSystemCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRootA, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root A: %v", err)
	}
	baseRootB, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/x", "vx",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root B: %v", err)
	}

	deltaTableA := mustFrozenSystemMemtable(t, "root/b", "vb")
	iterA := deltaTableA.NewIterator(nil, nil)
	deltaA, err := OrderedRootDeltaBatchFromIterator(iterA)
	_ = iterA.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator A: %v", err)
	}
	defer func() { _ = deltaA.Close() }()

	deltaTableB := mustFrozenSystemMemtable(t, "root/y", "vy")
	iterB := deltaTableB.NewIterator(nil, nil)
	deltaB, err := OrderedRootDeltaBatchFromIterator(iterB)
	_ = iterB.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator B: %v", err)
	}
	defer func() { _ = deltaB.Close() }()

	type publishResult struct {
		systemRoot uint64
		rootIDs    []uint64
		err        error
	}
	aBuilderEntered := make(chan struct{})
	releaseABuilder := make(chan struct{})
	aDone := make(chan publishResult, 1)
	var releaseABuilderOnce sync.Once
	releaseA := func() {
		releaseABuilderOnce.Do(func() {
			close(releaseABuilder)
		})
	}
	t.Cleanup(releaseA)
	var aBuilderCalls atomic.Int32
	go func() {
		systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: baseRootA,
			Delta:    deltaA,
		}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				return nil, errors.New("unexpected root IDs for A")
			}
			if aBuilderCalls.Add(1) == 1 {
				close(aBuilderEntered)
				<-releaseABuilder
			}
			return mustFrozenSystemMemtable(t,
				"sys/collections/a/primary", strconv.FormatUint(rootIDs[0], 10),
			).NewIterator(nil, nil), nil
		})
		aDone <- publishResult{systemRoot: systemRoot, rootIDs: rootIDs, err: err}
	}()

	select {
	case <-aBuilderEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("publish A did not reach system delta builder")
	}

	systemRootB, rootIDsB, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot: baseRootB,
		Delta:    deltaB,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			return nil, errors.New("unexpected root IDs for B")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/b/primary", strconv.FormatUint(rootIDs[0], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish B: %v", err)
	}
	if systemRootB == 0 || len(rootIDsB) != 1 || rootIDsB[0] == 0 {
		t.Fatalf("systemRootB=%d rootIDsB=%v, want non-zero roots", systemRootB, rootIDsB)
	}

	releaseA()
	var resultA publishResult
	select {
	case resultA = <-aDone:
	case <-time.After(2 * time.Second):
		t.Fatal("publish A did not complete after system root rebase")
	}
	if resultA.err != nil {
		t.Fatalf("publish A: %v", resultA.err)
	}
	if resultA.systemRoot == 0 || len(resultA.rootIDs) != 1 || resultA.rootIDs[0] == 0 {
		t.Fatalf("systemRootA=%d rootIDsA=%v, want non-zero roots", resultA.systemRoot, resultA.rootIDs)
	}
	if got := aBuilderCalls.Load(); got != 2 {
		t.Fatalf("A system delta builder calls=%d want 2", got)
	}

	stats := db.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.calls_total"]; got != "2" {
		t.Fatalf("calls stat=%q want 2", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.errors_total"]; got != "0" {
		t.Fatalf("errors stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_calls_total"]; got != "2" {
		t.Fatalf("root apply calls stat=%q want 2; non-system root work should not be rebuilt after system-root rebase", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.system_apply_calls_total"]; got != "3" {
		t.Fatalf("system apply calls stat=%q want 3", got)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	for key, wantRoot := range map[string]uint64{
		"sys/collections/a/primary": resultA.rootIDs[0],
		"sys/collections/b/primary": rootIDsB[0],
	} {
		entry, err := snap.GetEntryAtRoot(snap.State().SystemRootPageID, []byte(key))
		if err != nil {
			t.Fatalf("GetEntryAtRoot(%s): %v", key, err)
		}
		if got := string(entry.Value); got != strconv.FormatUint(wantRoot, 10) {
			t.Fatalf("%s=%q want %d", key, got, wantRoot)
		}
	}
	for rootID, kv := range map[uint64]map[string]string{
		resultA.rootIDs[0]: {"root/a": "va", "root/b": "vb"},
		rootIDsB[0]:        {"root/x": "vx", "root/y": "vy"},
	} {
		for key, want := range kv {
			entry, err := snap.GetEntryAtRoot(rootID, []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(root=%d key=%s): %v", rootID, key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("root=%d key=%s got %q want %q", rootID, key, got, want)
			}
		}
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_DeleteOnlyColdBuildPublishesEmptyRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	if err := delta.Delete([]byte("root/missing")); err != nil {
		t.Fatalf("delete root/missing: %v", err)
	}
	defer func() { _ = delta.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot: 0,
		Delta:    delta,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			return nil, errors.New("expected delete-only cold build to publish a non-zero empty root")
		}
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delete-only cold build: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero empty root", rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	if _, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/missing")); err == nil {
		t.Fatal("root/missing exists in delete-only cold root")
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder_AppliesDeletes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
		"root/b", "vb",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	deltaTable, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new delta table: %v", err)
	}
	deltaTable.Delete([]byte("root/b"))
	deltaTable.Freeze()
	iter := deltaTable.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot: baseRoot,
		Delta:    delta,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish delta batch group: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(root/a): %v", err)
	}
	if got, want := string(entry.Value), "va"; got != want {
		t.Fatalf("root/a=%q want %q", got, want)
	}
	if _, err := snap.GetEntryAtRoot(rootIDs[0], []byte("root/b")); err == nil {
		t.Fatal("root/b still exists after delta delete")
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_AcceptsLargeDeltaValueLogLeafValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	db.SetLeafPageLog(leafLog)
	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	_, baseRootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenSystemMemtable(t, "doc/a", "small").NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}})
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	if len(baseRootIDs) != 1 {
		t.Fatalf("base roots=%d want 1", len(baseRootIDs))
	}

	largeValue := bytes.Repeat([]byte("x"), page.DefaultInlineThreshold+1024)
	delta, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new delta table: %v", err)
	}
	delta.Set([]byte("doc/large"), largeValue)
	delta.Freeze()

	_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRootIDs[0],
		Iter:          delta.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish large value delta: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("doc/large"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(doc/large): %v", err)
	}
	if !bytes.Equal(entry.Value, largeValue) {
		t.Fatalf("large value mismatch: got %d bytes want %d", len(entry.Value), len(largeValue))
	}
}

func TestOrderedRootDeltaBatchFromIterator_StableIteratorUsesViews(t *testing.T) {
	key := []byte("root/a")
	value := []byte("value-a")
	iter := &stableRootDeltaIterator{
		entries: []stableRootDeltaEntry{{key: key, value: value}},
	}

	delta, err := orderedRootDeltaBatchFromIterator(iter)
	if err != nil {
		t.Fatalf("orderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	entries := delta.SortedEntries()
	if len(entries) != 1 {
		t.Fatalf("entries len=%d want 1", len(entries))
	}
	if got := string(entries[0].Key); got != string(key) {
		t.Fatalf("key=%q want %q", got, key)
	}
	if got := string(entries[0].Value); got != string(value) {
		t.Fatalf("value=%q want %q", got, value)
	}
	if &entries[0].Key[0] != &key[0] {
		t.Fatal("stable iterator key was copied into batch arena")
	}
	if &entries[0].Value[0] != &value[0] {
		t.Fatal("stable iterator value was copied into batch arena")
	}
}

func TestOrderedRootDeltaBatchIteratorLenSkipsDeletedWhenHidden(t *testing.T) {
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte("root/a"), []byte("va")); err != nil {
		t.Fatalf("set root/a: %v", err)
	}
	if err := delta.Delete([]byte("root/b")); err != nil {
		t.Fatalf("delete root/b: %v", err)
	}
	if err := delta.Set([]byte("root/c"), []byte("vc")); err != nil {
		t.Fatalf("set root/c: %v", err)
	}

	withoutDeletes := newOrderedRootDeltaBatchIterator(delta, false)
	defer func() { _ = withoutDeletes.Close() }()
	if got := withoutDeletes.Len(); got != 2 {
		t.Fatalf("Len without deletes=%d want 2", got)
	}
	withoutDeletes.Next()
	if got := withoutDeletes.Len(); got != 1 {
		t.Fatalf("Len after Next without deletes=%d want 1", got)
	}

	withDeletes := newOrderedRootDeltaBatchIterator(delta, true)
	defer func() { _ = withDeletes.Close() }()
	if got := withDeletes.Len(); got != 3 {
		t.Fatalf("Len with deletes=%d want 3", got)
	}
}

type stableRootDeltaEntry struct {
	key   []byte
	value []byte
}

type stableRootDeltaIterator struct {
	entries []stableRootDeltaEntry
	idx     int
}

func (it *stableRootDeltaIterator) StableUnsafeIteratorSlices() bool { return true }

func (it *stableRootDeltaIterator) OrderedUniqueUnsafeIterator() bool { return true }

func (it *stableRootDeltaIterator) Len() int {
	if it == nil {
		return 0
	}
	return len(it.entries)
}

func (it *stableRootDeltaIterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.entries)
}

func (it *stableRootDeltaIterator) Next() {
	if it != nil && it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *stableRootDeltaIterator) Seek(key []byte) {
	it.idx = len(it.entries)
	for i := range it.entries {
		if string(it.entries[i].key) >= string(key) {
			it.idx = i
			return
		}
	}
}

func (it *stableRootDeltaIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].key
}

func (it *stableRootDeltaIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *stableRootDeltaIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.UnsafeValue(), page.ValuePtr{}, 0
}

func (it *stableRootDeltaIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *stableRootDeltaIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *stableRootDeltaIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].key...)
}

func (it *stableRootDeltaIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].value...)
}

func (it *stableRootDeltaIterator) IsDeleted() bool {
	return false
}

func (it *stableRootDeltaIterator) Error() error {
	return nil
}

func (it *stableRootDeltaIterator) Close() error {
	return nil
}

func (it *stableRootDeltaIterator) Domain() (start, end []byte) {
	return nil, nil
}

func TestPublishOrderedRootGroupWithSystemBuilder_ErrorLeavesMetaRootsUnchanged(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	var builderRootIDs []uint64
	_, _, err = db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot: 0,
		Iter:     mustFrozenSystemMemtable(t, "doc/u1", "document").NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		builderRootIDs = append([]uint64(nil), rootIDs...)
		return nil, errors.New("system descriptor build failed")
	})
	if err == nil {
		t.Fatal("expected system builder error")
	}
	if len(builderRootIDs) != 1 || builderRootIDs[0] == 0 {
		t.Fatalf("builder root IDs=%v want one non-zero root", builderRootIDs)
	}
	after := db.State()
	if after == nil {
		t.Fatal("expected backend state after failed publish")
	}
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("commit seq changed after failed publish: got %d want %d", after.CommitSeq, before.CommitSeq)
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed after failed publish: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("system root changed after failed publish: got %d want %d", after.SystemRootPageID, before.SystemRootPageID)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	if _, err := snap.GetEntryAtRoot(after.SystemRootPageID, []byte("sys/collections/users/primary")); err == nil {
		t.Fatal("unexpected system descriptor after failed publish")
	}
}

func TestPublishOrderedRootGroup_SystemWarmApplyUpdatesValueLogRefTrackerInline(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 21, 30_000, 1, func(int) []byte {
		return []byte("old-grouped-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 22, 40_000, 1, func(int) []byte {
		return []byte("new-grouped-system-pointer")
	})[0]

	initialSystem := mustFrozenSystemPointerMemtable(t, "sys/p", oldPtr)
	if _, err := db.PublishSystemRootIterator(initialSystem.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish initial system root: %v", err)
	}

	pointTable := mustFrozenSystemMemtable(t, "root/a", "rv")
	iterTable := mustFrozenSystemMemtable(t, "iter/a", "iv")
	if _, _, err := db.PublishOrderedRootGroup(mustFrozenSystemPointerMemtable(t, "sys/p", newPtr).NewIterator(nil, nil), []OrderedRootPublishInput{
		{BaseRoot: 0, Iter: pointTable.NewIterator(nil, nil)},
		{BaseRoot: 0, Iter: iterTable.NewIterator(nil, nil)},
	}); err != nil {
		t.Fatalf("publish ordered root group: %v", err)
	}

	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}
	if _, ok := incRefs[newPtr.FileID]; !ok {
		t.Fatalf("expected new pointer file %d in ref set", newPtr.FileID)
	}
	if _, ok := incRefs[oldPtr.FileID]; ok {
		t.Fatalf("expected old pointer file %d to be removed", oldPtr.FileID)
	}

	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
}

func TestPublishOrderedRootIterator_NonSystemWarmApplyPreservesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	livePtr := appendPointersInNewSegment(t, dir, 0, 31, 50_000, 1, func(int) []byte {
		return []byte("live-user-pointer")
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("user/live"), livePtr); err != nil {
		t.Fatalf("SetPointer(user/live): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write live pointer: %v", err)
	}
	_ = b.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 32, 60_000, 1, func(int) []byte {
		return []byte("old-non-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 33, 70_000, 1, func(int) []byte {
		return []byte("new-non-system-pointer")
	})[0]

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "root/p", oldPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish initial non-system root: %v", err)
	}
	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("refresh value-log refs: %v", err)
	}

	newRoot, err := db.PublishOrderedRootIterator(baseRoot, mustFrozenSystemPointerMemtable(t, "root/p", newPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("warm publish non-system root: %v", err)
	}
	if newRoot == baseRoot {
		t.Fatalf("expected warm publish to produce a new root")
	}

	assertValueLogRefTrackerMatchesFullScan(t, db)
}

func TestPublishOrderedRootGroup_NonSystemWarmApplyPreservesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	livePtr := appendPointersInNewSegment(t, dir, 0, 41, 80_000, 1, func(int) []byte {
		return []byte("group-live-user-pointer")
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("user/live"), livePtr); err != nil {
		t.Fatalf("SetPointer(user/live): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write live pointer: %v", err)
	}
	_ = b.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 42, 90_000, 1, func(int) []byte {
		return []byte("old-group-non-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 43, 100_000, 1, func(int) []byte {
		return []byte("new-group-non-system-pointer")
	})[0]

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "root/p", oldPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish initial non-system root: %v", err)
	}
	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("refresh value-log refs: %v", err)
	}

	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		BaseRoot: baseRoot,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", newPtr).NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("warm publish non-system root group: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
	}
	if rootIDs[0] == baseRoot {
		t.Fatalf("expected grouped warm publish to produce a new root")
	}

	assertValueLogRefTrackerMatchesFullScan(t, db)
}

func TestPublishOrderedRootDeltaGroupWithSystemDeltaBuilder_InvalidatesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	livePtr := appendPointersInNewSegment(t, dir, 0, 51, 110_000, 1, func(int) []byte {
		return []byte("live-user-pointer-before-system-delta")
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("user/live"), livePtr); err != nil {
		t.Fatalf("SetPointer(user/live): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write live pointer: %v", err)
	}
	_ = b.Close()

	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("refresh value-log refs: %v", err)
	}
	beforeSeq := db.currentCommitSeq()
	if _, ok := db.valueLogRefTracker.referencedSet(beforeSeq); !ok {
		t.Fatalf("expected incremental ref set before system delta at seq=%d", beforeSeq)
	}

	systemPtr := appendPointersInNewSegment(t, dir, 0, 52, 120_000, 1, func(int) []byte {
		return []byte("system-pointer-delta")
	})[0]
	if _, _, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemPointerMemtable(t, "sys/p", systemPtr).NewIterator(nil, nil), nil
	}); err != nil {
		t.Fatalf("publish system delta group: %v", err)
	}

	afterSeq := db.currentCommitSeq()
	if refs, ok := db.valueLogRefTracker.referencedSet(afterSeq); ok {
		t.Fatalf("value-log ref tracker stayed valid after untracked system delta: refs=%v", refs)
	}
}

func TestPublishOrderedRootDeltaGroupWithSystemBuilder_InvalidatesValueLogRefTrackerForNonSystemDelta(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	livePtr := appendPointersInNewSegment(t, dir, 0, 53, 130_000, 1, func(int) []byte {
		return []byte("live-user-pointer-before-non-system-delta")
	})[0]
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("user/live"), livePtr); err != nil {
		t.Fatalf("SetPointer(user/live): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write live pointer: %v", err)
	}
	_ = b.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 54, 140_000, 1, func(int) []byte {
		return []byte("old-non-system-delta-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 55, 150_000, 1, func(int) []byte {
		return []byte("new-non-system-delta-pointer")
	})[0]
	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "root/p", oldPtr).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish initial non-system root: %v", err)
	}

	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("refresh value-log refs: %v", err)
	}
	beforeSeq := db.currentCommitSeq()
	if _, ok := db.valueLogRefTracker.referencedSet(beforeSeq); !ok {
		t.Fatalf("expected incremental ref set before non-system delta at seq=%d", beforeSeq)
	}

	_, _, err = db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     mustFrozenSystemPointerMemtable(t, "root/p", newPtr).NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			t.Fatalf("rootIDs len=%d want 1", len(rootIDs))
		}
		return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish non-system delta group: %v", err)
	}

	afterSeq := db.currentCommitSeq()
	if refs, ok := db.valueLogRefTracker.referencedSet(afterSeq); ok {
		t.Fatalf("value-log ref tracker stayed valid after untracked non-system delta: refs=%v", refs)
	}
}

func assertValueLogRefTrackerMatchesFullScan(t *testing.T, db *DB) {
	t.Helper()
	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}
	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
}
