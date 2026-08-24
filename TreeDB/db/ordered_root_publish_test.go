package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

type panickingStorageMaintenancePlan struct{}

func (panickingStorageMaintenancePlan) StorageMaintenancePlanToken() storagemaintenance.Plan {
	panic("panicking maintenance plan should fail closed")
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

func requireCommandWALPublishReady(tb testing.TB, db *DB, label string) {
	tb.Helper()
	if err := db.CheckCommandWALPublishReady(); err != nil {
		tb.Fatalf("CheckCommandWALPublishReady after %s: %v", label, err)
	}
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

func TestPublishOrderedRootDeltaGroupWithCommandWALContextPreservesExactValueLogProjection(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	makeDelta := func(key, value string) memtable.Table {
		t.Helper()
		delta := memtable.NewAppendOnlyWithEntryCapacity(1)
		delta.Set([]byte(key), []byte(value))
		delta.Freeze()
		return delta
	}
	publish := func(baseRoot uint64, delta memtable.Table, commandKey string) uint64 {
		t.Helper()
		_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
			[]OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: delta.NewIterator(nil, nil)}},
			mustRawKVCommandWALIntent(t, db, commandKey, "1"),
			func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenSystemMemtable(t, "system/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("publish base root %d: %v", baseRoot, err)
		}
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			t.Fatalf("rootIDs=%v, want one non-zero root", rootIDs)
		}
		return rootIDs[0]
	}

	var scans atomic.Int64
	db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
	first := makeDelta("root/a", "value-a")
	firstRoot := publish(0, first, "cmd/exact-ref-base")
	second := makeDelta("root/b", "value-b")
	_ = publish(firstRoot, second, "cmd/exact-ref-update")
	db.testScanCandidateExternalReferencesHook = nil
	if got := scans.Load(); got != 0 {
		t.Fatalf("command-WAL grouped delta candidate dependency scans=%d want 0", got)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextOuterLeafReplacementAvoidsCandidateScan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	opts := Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityDurable,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 610_000, 1, func(int) []byte { return []byte("value-0") })[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 620_000, 1, func(int) []byte { return []byte("value-1") })[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh value-log set: %v", err)
	}
	publish := func(baseRoot uint64, ptr page.ValuePtr, commandKey string) uint64 {
		t.Helper()
		delta := mustFrozenSystemPointerMemtable(t, "doc/p", ptr)
		_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
			[]OrderedRootDeltaPublishInput{{
				BaseRoot:      baseRoot,
				Iter:          delta.NewIterator(nil, nil),
				StoragePolicy: OrderedRootStorageValueLogLeaves,
			}},
			mustRawKVCommandWALIntent(t, db, commandKey, "1"),
			func(_ CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("publish base root %d: %v", baseRoot, err)
		}
		return rootIDs[0]
	}

	root := publish(0, oldPtr, "cmd/outer-leaf-base")
	primeValueLogRefTracker(t, db)
	if !db.valueLogRefTracker.canTrack(db.currentCommitSeq()) {
		t.Fatal("logical value-log tracker is not current after base publication")
	}
	var scans atomic.Int64
	db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
	_ = publish(root, newPtr, "cmd/outer-leaf-replace")
	db.testScanCandidateExternalReferencesHook = nil
	if got := scans.Load(); got != 0 {
		t.Fatalf("outer-leaf replacement candidate dependency scans=%d want 0", got)
	}
	refs, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq())
	if !ok {
		t.Fatal("logical value-log tracker is not current after replacement")
	}
	if _, ok := refs[oldPtr.FileID]; ok {
		t.Fatalf("logical value-log tracker retained replaced segment %d", oldPtr.FileID)
	}
	if _, ok := refs[newPtr.FileID]; !ok {
		t.Fatalf("logical value-log tracker omitted replacement segment %d", newPtr.FileID)
	}

	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("close leaf log: %v", err)
	}
	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, reopen)
	if got := readCollectionRootValue(t, reopen, maintenanceTestCollectionRootKey, []byte("doc/p")); !bytes.Equal(got, []byte("value-1")) {
		t.Fatalf("reopened collection value=%q want value-1", got)
	}
}

func TestPublishOrderedRootDeltaGroupWithCommandWALContextProjectsCollectionDescriptorReachability(t *testing.T) {
	tests := []struct {
		name    string
		publish func(*testing.T, *DB) error
	}{
		{
			name: "iterator-group",
			publish: func(t *testing.T, db *DB) error {
				rootDelta := mustFrozenSystemMemtable(t, "doc/a", "value-a")
				_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
					[]OrderedRootDeltaPublishInput{{BaseRoot: 0, Iter: rootDelta.NewIterator(nil, nil)}},
					mustRawKVCommandWALIntent(t, db, "cmd/descriptor-iterator", "1"),
					func(_ CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenRawMemtable(t,
							collectionRootDescriptorPrefix+"command-wal-iterator",
							encodeMaintenanceRootID(rootIDs[0]),
						).NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name: "batch-group",
			publish: func(t *testing.T, db *DB) error {
				rootIter := mustFrozenSystemMemtable(t, "doc/a", "value-a").NewIterator(nil, nil)
				rootDelta, err := OrderedRootDeltaBatchFromIterator(rootIter)
				_ = rootIter.Close()
				if err != nil {
					return err
				}
				defer func() { _ = rootDelta.Close() }()
				_, _, err = db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
					[]OrderedRootDeltaBatchPublishInput{{BaseRoot: 0, Delta: rootDelta}},
					mustRawKVCommandWALIntent(t, db, "cmd/descriptor-batch", "1"),
					func(_ CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenRawMemtable(t,
							collectionRootDescriptorPrefix+"command-wal-batch",
							encodeMaintenanceRootID(rootIDs[0]),
						).NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			defer db.Close()

			var scans atomic.Int64
			db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
			if err := test.publish(t, db); err != nil {
				t.Fatalf("publish: %v", err)
			}
			db.testScanCandidateExternalReferencesHook = nil
			if got := scans.Load(); got != 1 {
				t.Fatalf("candidate dependency scans=%d want 1 for collection descriptor reachability", got)
			}
		})
	}
}

func TestPublishOrderedRootCommandWALWarmCoveredDescriptorTransitionAvoidsCandidateScan(t *testing.T) {
	tests := []struct {
		name    string
		publish func(*testing.T, *DB, uint64, string) uint64
	}{
		{
			name: "context-iterator-group",
			publish: func(t *testing.T, db *DB, baseRoot uint64, commandKey string) uint64 {
				t.Helper()
				rootDelta := mustFrozenSystemMemtable(t, "sys/1024", "value-updated")
				if baseRoot == 0 {
					rootDelta = mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
				}
				_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
					[]OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: rootDelta.NewIterator(nil, nil)}},
					mustRawKVCommandWALIntent(t, db, commandKey, "1"),
					func(_ CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenRawMemtable(t,
							collectionRootDescriptorPrefix+"covered-iterator",
							encodeMaintenanceRootID(rootIDs[0]),
						).NewIterator(nil, nil), nil
					},
				)
				if err != nil {
					t.Fatalf("publish iterator base root %d: %v", baseRoot, err)
				}
				return rootIDs[0]
			},
		},
		{
			name: "iterator-wrapper",
			publish: func(t *testing.T, db *DB, baseRoot uint64, commandKey string) uint64 {
				t.Helper()
				rootDelta := mustFrozenSystemMemtable(t, "sys/1024", "value-updated")
				if baseRoot == 0 {
					rootDelta = mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
				}
				_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(
					[]OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: rootDelta.NewIterator(nil, nil)}},
					mustRawKVCommandWALIntent(t, db, commandKey, "1"),
					func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenRawMemtable(t,
							collectionRootDescriptorPrefix+"covered-iterator-wrapper",
							encodeMaintenanceRootID(rootIDs[0]),
						).NewIterator(nil, nil), nil
					},
				)
				if err != nil {
					t.Fatalf("publish iterator wrapper base root %d: %v", baseRoot, err)
				}
				return rootIDs[0]
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			defer db.Close()

			baseRoot := test.publish(t, db, 0, "cmd/covered-base")
			if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
				t.Fatalf("prime value-log reference tracker: %v", err)
			}
			var scans atomic.Int64
			db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
			newRoot := test.publish(t, db, baseRoot, "cmd/covered-warm")
			db.testScanCandidateExternalReferencesHook = nil
			if newRoot == baseRoot {
				t.Fatalf("warm publish root=%d want a distinct root transition", newRoot)
			}
			if got := scans.Load(); got != 0 {
				t.Fatalf("warm covered descriptor candidate dependency scans=%d want 0", got)
			}
		})
	}
}

func TestPublishOrderedRootCommandWALContextWarmUnmatchedDescriptorTransitionRetainsCandidateScan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	publish := func(baseRoot uint64, descriptorRoot uint64, commandKey string) uint64 {
		t.Helper()
		rootDelta := mustFrozenSystemMemtable(t, "sys/1024", "value-updated")
		if baseRoot == 0 {
			rootDelta = mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
		}
		_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
			[]OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: rootDelta.NewIterator(nil, nil)}},
			mustRawKVCommandWALIntent(t, db, commandKey, "1"),
			func(_ CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				rootID := descriptorRoot
				if rootID == 0 {
					rootID = rootIDs[0]
				}
				return mustFrozenRawMemtable(t,
					collectionRootDescriptorPrefix+"unmatched-iterator",
					encodeMaintenanceRootID(rootID),
				).NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("publish iterator base root %d: %v", baseRoot, err)
		}
		return rootIDs[0]
	}

	baseRoot := publish(0, 0, "cmd/unmatched-base")
	if _, err := db.referencedValueLogSegments(context.Background()); err != nil {
		t.Fatalf("prime value-log reference tracker: %v", err)
	}
	unrelatedTable := mustFrozenSystemMemtable(t, "unrelated/root", "value")
	unrelatedRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		0,
		unrelatedTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish unrelated root: %v", err)
	}

	var scans atomic.Int64
	db.testScanCandidateExternalReferencesHook = func() { scans.Add(1) }
	newRoot := publish(baseRoot, unrelatedRoot, "cmd/unmatched-warm")
	db.testScanCandidateExternalReferencesHook = nil
	if newRoot == baseRoot {
		t.Fatalf("warm publish root=%d want a distinct grouped root transition", newRoot)
	}
	if got := scans.Load(); got != 1 {
		t.Fatalf("unmatched descriptor candidate dependency scans=%d want 1", got)
	}
}

func TestOrderedRootCollectionDescriptorTransitionsCoveredResolvesPointerBackedDescriptors(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	baseTable := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	baseRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		0,
		baseTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish base collection root: %v", err)
	}
	newTable := mustFrozenSystemMemtable(t, "sys/1024", "value-updated")
	newRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		baseRoot,
		newTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish new collection root: %v", err)
	}
	if newRoot == baseRoot {
		t.Fatalf("collection root transition=%d want distinct roots", newRoot)
	}

	appendDescriptor := func(seq uint32, rootID uint64) page.ValuePtr {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], rootID)
		return appendPointersInNewSegment(t, dir, 0, seq, uint64(seq)*1000, 1, func(int) []byte {
			return encoded[:]
		})[0]
	}
	descriptorKey := collectionRootDescriptorPrefix + "pointer-backed"
	basePtr := appendDescriptor(191, baseRoot)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh base descriptor segment: %v", err)
	}
	baseSystemTable := mustFrozenSystemPointerMemtable(t, descriptorKey, basePtr)
	baseSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		0,
		baseSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish base system root: %v", err)
	}

	newPtr := appendDescriptor(192, newRoot)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh new descriptor segment: %v", err)
	}
	newSystemTable := mustFrozenSystemPointerMemtable(t, descriptorKey, newPtr)
	newSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		baseSystemRoot,
		newSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish new system root: %v", err)
	}

	if !db.orderedRootCollectionDescriptorTransitionsCovered(
		db.idx.Load(),
		0,
		baseSystemRoot,
		newSystemRoot,
		[]uint64{baseRoot},
		[]uint64{newRoot},
	) {
		t.Fatal("pointer-backed exact descriptor transition was not covered")
	}
}

func TestOrderedRootCollectionDescriptorTransitionsCoveredRejectsAliasFanOut(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	baseSystemTable := mustFrozenRawMemtable(t,
		collectionRootDescriptorPrefix+"alias-a", encodeMaintenanceRootID(101),
		collectionRootDescriptorPrefix+"alias-b", encodeMaintenanceRootID(101),
	)
	baseSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		0,
		baseSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish base system root: %v", err)
	}
	newSystemTable := mustFrozenRawMemtable(t,
		collectionRootDescriptorPrefix+"alias-a", encodeMaintenanceRootID(201),
		collectionRootDescriptorPrefix+"alias-b", encodeMaintenanceRootID(202),
	)
	newSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		baseSystemRoot,
		newSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish new system root: %v", err)
	}

	if db.orderedRootCollectionDescriptorTransitionsCovered(
		db.idx.Load(),
		0,
		baseSystemRoot,
		newSystemRoot,
		[]uint64{101, 101},
		[]uint64{201, 202},
	) {
		t.Fatal("alias fan-out descriptor transition unexpectedly covered")
	}
}

func TestOrderedRootCollectionDescriptorTransitionsCoveredRejectsUnconsumedGroupTransition(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	baseSystemTable := mustFrozenRawMemtable(t,
		collectionRootDescriptorPrefix+"consumed", encodeMaintenanceRootID(101),
	)
	baseSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		0,
		baseSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish base system root: %v", err)
	}
	newSystemTable := mustFrozenRawMemtable(t,
		collectionRootDescriptorPrefix+"consumed", encodeMaintenanceRootID(201),
	)
	newSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		baseSystemRoot,
		newSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish new system root: %v", err)
	}

	if db.orderedRootCollectionDescriptorTransitionsCovered(
		db.idx.Load(),
		0,
		baseSystemRoot,
		newSystemRoot,
		[]uint64{101, 301},
		[]uint64{201, 302},
	) {
		t.Fatal("descriptor proof unexpectedly covered an unconsumed grouped transition")
	}
}

func TestOrderedRootCollectionDescriptorTransitionsCoveredRejectsPrimaryRootAlias(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	baseSystemTable := mustFrozenRawMemtable(t,
		collectionRootDescriptorPrefix+"primary-alias", encodeMaintenanceRootID(101),
	)
	baseSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		0,
		baseSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish base system root: %v", err)
	}
	newSystemTable := mustFrozenRawMemtable(t,
		collectionRootDescriptorPrefix+"primary-alias", encodeMaintenanceRootID(201),
	)
	newSystemRoot, _, _, _, _, err := db.publishOrderedRootIterator(
		baseSystemRoot,
		newSystemTable.NewIterator(nil, nil),
		systemRootOrderedPublishOptions(db),
		false,
	)
	if err != nil {
		t.Fatalf("publish new system root: %v", err)
	}

	if db.orderedRootCollectionDescriptorTransitionsCovered(
		db.idx.Load(),
		101,
		baseSystemRoot,
		newSystemRoot,
		[]uint64{101},
		[]uint64{201},
	) {
		t.Fatal("descriptor proof unexpectedly covered a root still reachable through the primary user root")
	}
}

func TestOrderedRootCollectionDescriptorTransitionsCoveredRejectsSystemRootAlias(t *testing.T) {
	tests := []struct {
		name                          string
		baseDescriptor, newDescriptor uint64
		baseSystemRoot, newSystemRoot uint64
	}{
		{
			name:           "base descriptor aliases base system root",
			baseDescriptor: 101, newDescriptor: 201,
			baseSystemRoot: 101, newSystemRoot: 301,
		},
		{
			name:           "new descriptor aliases new system root",
			baseDescriptor: 101, newDescriptor: 301,
			baseSystemRoot: 201, newSystemRoot: 301,
		},
		{
			name:           "new descriptor reuses base system root",
			baseDescriptor: 101, newDescriptor: 201,
			baseSystemRoot: 201, newSystemRoot: 301,
		},
		{
			name:           "base descriptor aliases new system root",
			baseDescriptor: 301, newDescriptor: 201,
			baseSystemRoot: 101, newSystemRoot: 301,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseEntries := []collectionEntry{{
				key:           []byte(collectionRootDescriptorPrefix + "system-alias"),
				sourceRootIDs: []uint64{tc.baseDescriptor},
			}}
			newEntries := []collectionEntry{{
				key:           []byte(collectionRootDescriptorPrefix + "system-alias"),
				sourceRootIDs: []uint64{tc.newDescriptor},
			}}

			if orderedRootCollectionDescriptorTransitionsCoveredEntries(
				baseEntries,
				newEntries,
				0,
				tc.baseSystemRoot,
				tc.newSystemRoot,
				[]uint64{tc.baseDescriptor},
				[]uint64{tc.newDescriptor},
			) {
				t.Fatal("descriptor proof unexpectedly covered a grouped transition aliased by a system root")
			}
		})
	}
}

func TestOrderedRootCommandWALAcceptedWaitFailureDoesNotPoisonOpenHandle(t *testing.T) {
	tests := []struct {
		name    string
		publish func(*testing.T, *DB, *CommandWALIntent, *uint64) error
	}{
		{
			name: "iterator-group",
			publish: func(t *testing.T, db *DB, intent *CommandWALIntent, seenLSN *uint64) error {
				_, _, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
					nil,
					intent,
					func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
						*seenLSN = ctx.AppliedCommandLSN
						return mustFrozenSystemMemtable(t, "system/accepted-iterator", strconv.FormatUint(ctx.AppliedCommandLSN, 10)).NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
		{
			name: "batch-group",
			publish: func(t *testing.T, db *DB, intent *CommandWALIntent, seenLSN *uint64) error {
				_, _, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
					nil,
					intent,
					func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
						*seenLSN = ctx.AppliedCommandLSN
						return mustFrozenSystemMemtable(t, "system/accepted-batch", strconv.FormatUint(ctx.AppliedCommandLSN, 10)).NewIterator(nil, nil), nil
					},
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			enableCommandWALFormat(t, dir)
			db := openCommandWALDB(t, dir)
			defer db.Close()

			intent := mustRawKVCommandWALIntent(t, db, "ordered/"+test.name, "covered")
			db.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
			db.testFailWriteMeta.Store(true)
			var seenLSN uint64
			err := test.publish(t, db, intent, &seenLSN)
			db.testFailWriteMeta.Store(false)
			if !errors.Is(err, errTestWriteMetaFailpoint) {
				t.Fatalf("accepted ordered-root wait error=%v, want retryable meta failpoint", err)
			}
			if errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("accepted ordered-root wait error=%v unexpectedly requires recovery", err)
			}
			if got := db.State().AppliedCommandLSN; seenLSN == 0 || got != seenLSN {
				t.Fatalf("visible AppliedCommandLSN after accepted error=%d, want builder LSN %d", got, seenLSN)
			}
			if err := db.CheckCommandWALPublishReady(); err != nil {
				t.Fatalf("CheckCommandWALPublishReady after accepted ordered-root error: %v", err)
			}
			if err := db.SetSync([]byte("after-"+test.name), []byte("same-handle-progress")); err != nil {
				t.Fatalf("SetSync after accepted ordered-root error: %v", err)
			}
		})
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextUsesCommandWALRouteForSystemDelta(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openOrderedRootSpanNativeRouteCounterDB(t, dir)
	defer func() { _ = db.Close() }()

	makeDelta := func(kv ...string) *batch.Batch {
		t.Helper()
		table := mustFrozenSystemMemtable(t, kv...)
		iter := table.NewIterator(nil, nil)
		delta, err := OrderedRootDeltaBatchFromIterator(iter)
		_ = iter.Close()
		if err != nil {
			t.Fatalf("delta batch: %v", err)
		}
		return delta
	}

	baseDelta := makeDelta("root/a", "base")
	defer func() { _ = baseDelta.Close() }()
	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: 0,
			Delta:    baseDelta,
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/batch-context-base", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "system/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder base: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v, want one non-zero root", rootIDs)
	}

	updateDelta := makeDelta("root/b", "update")
	defer func() { _ = updateDelta.Close() }()
	_, updatedRootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
		[]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: rootIDs[0],
			Delta:    updateDelta,
		}},
		mustRawKVCommandWALIntent(t, db, "cmd/batch-context-update", "1"),
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "system/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder update: %v", err)
	}
	if len(updatedRootIDs) != 1 || updatedRootIDs[0] == 0 {
		t.Fatalf("updatedRootIDs=%v, want one non-zero root", updatedRootIDs)
	}

	stats := db.Stats()
	commandRoutePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish."
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, commandRoutePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.collection_buffered_roots.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.multi_index_group_publish.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.system_delta_builder_publish.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total")
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALContextWaitsBeforeWriteMu(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	staged := mustRawKVCommandWALIntent(t, db, "cmd/staged", "1")
	unlockStage := db.LockCommandWALStaging()
	stageReleased := false
	defer func() {
		if !stageReleased {
			unlockStage()
		}
	}()
	lsn, err := db.AppendStagedCommandWALIntent(staged, false)
	if err != nil {
		t.Fatalf("AppendStagedCommandWALIntent: %v", err)
	}
	if lsn == 0 {
		t.Fatalf("AppendStagedCommandWALIntent lsn=0")
	}

	publishDone := make(chan error, 1)
	contextIntent := mustRawKVCommandWALIntent(t, db, "cmd/context", "2")
	go func() {
		_, _, publishErr := db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilder(
			nil,
			contextIntent,
			func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				if ctx.AppliedCommandLSN == 0 {
					return nil, errors.New("AppliedCommandLSN=0 in system builder")
				}
				if len(rootIDs) != 0 {
					return nil, errors.New("non-empty rootIDs in system builder")
				}
				sys, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
				if err != nil {
					return nil, err
				}
				sys.Set([]byte("system/context-wait"), []byte(strconv.FormatUint(ctx.AppliedCommandLSN, 10)))
				sys.Freeze()
				return sys.NewIterator(nil, nil), nil
			},
		)
		publishDone <- publishErr
	}()

	select {
	case err := <-publishDone:
		t.Fatalf("context publish completed while raw stage lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	stagedDone := make(chan error, 1)
	go func() {
		stagedDone <- db.PublishStagedCommandWALNoop(staged, false)
	}()
	select {
	case err := <-stagedDone:
		if err != nil {
			t.Fatalf("PublishStagedCommandWALNoop: %v", err)
		}
	case <-time.After(time.Second):
		unlockStage()
		stageReleased = true
		select {
		case <-publishDone:
		case <-time.After(time.Second):
		}
		t.Fatalf("staged command WAL finalize blocked behind ordered-root context publish")
	}
	if got := db.State().AppliedCommandLSN; got != lsn {
		t.Fatalf("AppliedCommandLSN after staged publish=%d, want %d", got, lsn)
	}

	unlockStage()
	stageReleased = true
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("context publish after stage release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("context publish did not finish after stage release")
	}
	if got := db.State().AppliedCommandLSN; got != lsn+1 {
		t.Fatalf("AppliedCommandLSN after context publish=%d, want %d", got, lsn+1)
	}
}

func TestFinalizeOrderedRootPublishExistingCoverageSyncRunsRawPublishBarriers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	appended := mustRawKVCommandWALIntent(t, db, "coverage-relaxed", "value")
	lsn, err := db.AppendCommandWALIntent(appended, false)
	if err != nil {
		t.Fatalf("AppendCommandWALIntent relaxed: %v", err)
	}
	coverage, err := NewCommandWALCoverageIntent(lsn, CommandWALLSNRange{First: lsn, Last: lsn})
	if err != nil {
		t.Fatalf("NewCommandWALCoverageIntent: %v", err)
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	barrierErr := errors.New("raw publish barrier ran")
	var barrierCalled atomic.Bool
	unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
		barrierCalled.Store(true)
		return barrierErr
	})
	err = db.finalizeOrderedRootPublishWithCommandWALOptions(
		userRoot,
		systemRoot,
		nil,
		true,
		adaptive.Metrics{},
		nil,
		false,
		nil,
		nil,
		nil,
		baseSeq,
		coverage,
		orderedRootCommandWALPublishOptions{},
		func() {},
	)
	if !errors.Is(err, barrierErr) {
		t.Fatalf("finalize existing coverage sync error=%v, want %v", err, barrierErr)
	}
	if !barrierCalled.Load() {
		t.Fatal("finalize existing coverage sync skipped raw publish barriers")
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN=%d after raw publish barrier failure, want 0", got)
	}
	unregister()

	if err := db.finalizeOrderedRootPublishWithCommandWALOptions(
		userRoot,
		systemRoot,
		nil,
		true,
		adaptive.Metrics{},
		nil,
		false,
		nil,
		nil,
		nil,
		baseSeq,
		coverage,
		orderedRootCommandWALPublishOptions{},
		func() {},
	); err != nil {
		t.Fatalf("finalize existing coverage sync retry: %v", err)
	}
	if got := db.State().AppliedCommandLSN; got != lsn+1 {
		t.Fatalf("AppliedCommandLSN=%d, want coverage plus barrier %d", got, lsn+1)
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
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     rootDelta.NewIterator(nil, nil),
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

func TestPublishOrderedRootDeltaGroupMaintenanceSystemBuilderErrorClosesReturnedIterator(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	systemIter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("maintenance system builder returned iterator with error")
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 1 || rootIDs[0] == 0 {
				t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
			}
			return systemIter, wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("maintenance publish error=%v want %v", err, wantErr)
	}
	if errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v must not be marked pre-apply after root delta apply", err)
	}
	requireCommandWALPublishReady(t, db, "maintenance system builder failure")
	if systemIter.closes != 1 {
		t.Fatalf("system iterator closes=%d want 1", systemIter.closes)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsForgedPlan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		forgedStorageMaintenancePlan{},
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
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
	requireCommandWALPublishReady(t, db, "forged maintenance plan rejection")
}

func TestPublishOrderedRootDeltaGroupMaintenanceConsumesDurableResourcesOnPreApplyFailure(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	resources := stableContractResourceSet(t, stableContractDescriptor{
		generation:   1,
		kind:         rootpublication.ResourceOuterLeafLog,
		reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		frontier:     4096,
	})
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		forgedStorageMaintenancePlan{},
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot:         0,
			Iter:             mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
			DurableResources: resources,
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
	if got := resources.Owner(); got != rootpublication.ResourceOwnerReleased {
		t.Fatalf("durable resource owner=%v want released after pre-apply failure", got)
	}
	requireCommandWALPublishReady(t, db, "durable resource pre-apply rejection")
}

func TestPublishOrderedRootDeltaGroupMaintenanceReleasesCollectedDurableResourcesOnPreflightFailure(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	var releases atomic.Int32
	resources := stableContractResourceSet(t, stableContractDescriptor{
		generation:   1,
		kind:         rootpublication.ResourceOuterLeafLog,
		reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		frontier:     4096,
		onRelease:    func() { releases.Add(1) },
	})
	wantErr := errors.New("maintenance preflight failed")
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot:         0,
			Iter:             mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
			DurableResources: resources,
		}},
		func() error { return wantErr },
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run after preflight failure")
			return nil, nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("maintenance publish error=%v want %v", err, wantErr)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want pre-apply marker", err)
	}
	if got := resources.Owner(); got != rootpublication.ResourceOwnerTransferred {
		t.Fatalf("producer resource owner=%v want transferred into collected union", got)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("durable resource releases=%d want 1", got)
	}
	requireCommandWALPublishReady(t, db, "collected durable resource preflight failure")
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsInvalidInputBeforeWriteLock(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.writeMu.Lock()
	locked := true
	unlock := func() {
		if locked {
			locked = false
			db.writeMu.Unlock()
		}
	}
	defer unlock()

	var systemBuilderRan atomic.Bool
	done := make(chan error, 1)
	go func() {
		_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
			forgedStorageMaintenancePlan{},
			[]StorageMaintenanceRootDeltaPublishInput{{
				BaseRoot: 0,
				Iter:     mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
			}},
			nil,
			func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				systemBuilderRan.Store(true)
				return nil, nil
			},
		)
		done <- err
	}()

	select {
	case err := <-done:
		unlock()
		if !errors.Is(err, ErrStorageMaintenancePlanMissing) {
			t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePlanMissing", err)
		}
		if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
			t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("invalid maintenance input blocked behind writeMu")
	}
	if systemBuilderRan.Load() {
		t.Fatalf("maintenance system builder ran for invalid maintenance input")
	}
	requireCommandWALPublishReady(t, db, "pre-lock forged maintenance plan rejection")
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsNilIteratorBeforeWriteLock(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	db.writeMu.Lock()
	locked := true
	unlock := func() {
		if locked {
			locked = false
			db.writeMu.Unlock()
		}
	}
	defer unlock()

	iter := &closeCountingUnsafeIterator{}
	var systemBuilderRan atomic.Bool
	done := make(chan error, 1)
	go func() {
		_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
			storagemaintenance.ColumnAssetRewritePlan(),
			[]StorageMaintenanceRootDeltaPublishInput{
				{BaseRoot: 0, Iter: iter},
				{BaseRoot: 0, Iter: nil},
			},
			nil,
			func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				systemBuilderRan.Store(true)
				return nil, nil
			},
		)
		done <- err
	}()

	select {
	case err := <-done:
		unlock()
		if !errors.Is(err, ErrStorageMaintenanceRootDeltaIteratorMissing) {
			t.Fatalf("maintenance publish error=%v want ErrStorageMaintenanceRootDeltaIteratorMissing", err)
		}
		if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
			t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("nil-iterator maintenance input blocked behind writeMu")
	}
	if systemBuilderRan.Load() {
		t.Fatalf("maintenance system builder ran for nil-iterator maintenance input")
	}
	if iter.closes != 1 {
		t.Fatalf("input iterator closes=%d want 1", iter.closes)
	}
	requireCommandWALPublishReady(t, db, "nil-iterator maintenance input rejection")
}

func TestPublishOrderedRootDeltaGroupMaintenanceReadOnlyClosesIterators(t *testing.T) {
	iter := &closeCountingUnsafeIterator{}
	db := &DB{readOnly: true}
	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     iter,
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run on read-only DB")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("maintenance publish read-only error=%v want ErrReadOnly", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish read-only error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
	if iter.closes != 1 {
		t.Fatalf("iterator closes=%d want 1", iter.closes)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenancePreLockFailuresCloseIterators(t *testing.T) {
	closingDB := &DB{}
	closingDB.closing.Store(true)
	nonnullBuilder := func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		t.Fatalf("maintenance system builder should not run for pre-lock failure")
		return nil, nil
	}
	tests := []struct {
		name      string
		db        *DB
		builder   OrderedRootGroupSystemBuilder
		wantErr   error
		wantCause error
	}{
		{
			name:      "nil builder",
			db:        &DB{},
			builder:   nil,
			wantErr:   ErrStorageMaintenancePublishPreApplyFailed,
			wantCause: ErrStorageMaintenanceSystemBuilderMissing,
		},
		{
			name:    "nil db",
			db:      nil,
			builder: nonnullBuilder,
			wantErr: ErrClosed,
		},
		{
			name:    "closing db",
			db:      closingDB,
			builder: nonnullBuilder,
			wantErr: ErrClosed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := &closeCountingUnsafeIterator{}
			_, _, err := tt.db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
				storagemaintenance.ColumnAssetRewritePlan(),
				[]StorageMaintenanceRootDeltaPublishInput{{
					BaseRoot: 0,
					Iter:     iter,
				}},
				nil,
				tt.builder,
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("maintenance publish error=%v want %v", err, tt.wantErr)
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Fatalf("maintenance publish error=%v want cause %v", err, tt.wantCause)
			}
			if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
				t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
			}
			if iter.closes != 1 {
				t.Fatalf("iterator closes=%d want 1", iter.closes)
			}
		})
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
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
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
	requireCommandWALPublishReady(t, db, "typed nil maintenance plan rejection")
}

func TestPublishOrderedRootDeltaGroupMaintenanceRejectsPanickingPlan(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	_, _, err := db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		panickingStorageMaintenancePlan{},
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: 0,
			Iter:     mustFrozenSystemMemtable(t, "root/k", "v").NewIterator(nil, nil),
		}},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run for a panicking maintenance plan")
			return nil, nil
		},
	)
	if !errors.Is(err, ErrStorageMaintenancePlanMissing) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePlanMissing", err)
	}
	if !errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v want ErrStorageMaintenancePublishPreApplyFailed", err)
	}
	requireCommandWALPublishReady(t, db, "panicking maintenance plan rejection")
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
	requireCommandWALPublishReady(t, db, "missing maintenance plan rejection")
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
	requireCommandWALPublishReady(t, db, "system-only maintenance rejection")
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
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot: baseRoot,
			Iter:     emptyDelta.NewIterator(nil, nil),
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
	requireCommandWALPublishReady(t, db, "empty maintenance root delta rejection")
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
	var resourceReleases atomic.Int32
	resources := stableContractResourceSet(t, stableContractDescriptor{
		generation:   1,
		kind:         rootpublication.ResourceOuterLeafLog,
		reachability: rootpublication.ReachabilityOuterLeafRawPointer,
		frontier:     4096,
		onRelease:    func() { resourceReleases.Add(1) },
	})
	_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{{
			BaseRoot:         baseRoot,
			Iter:             iter,
			DurableResources: resources,
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
	requireCommandWALPublishReady(t, db, "maintenance root delta iterator failure")
	if iter.closes != 1 {
		t.Fatalf("root delta iterator closes=%d want 1", iter.closes)
	}
	if got := resources.Owner(); got != rootpublication.ResourceOwnerTransferred {
		t.Fatalf("producer resource owner=%v want transferred into collected union", got)
	}
	if got := resourceReleases.Load(); got != 1 {
		t.Fatalf("root-apply failure durable resource releases=%d want 1", got)
	}
}

func TestPublishOrderedRootDeltaGroupMaintenanceDoesNotMarkPostRootApplyErrorPreApply(t *testing.T) {
	dir := t.TempDir()
	setupDB, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open setup DB: %v", err)
	}
	baseRootA, err := setupDB.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "base-a").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root A: %v", err)
	}
	baseRootB, err := setupDB.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/b", "base-b").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root B: %v", err)
	}
	if err := setupDB.Close(); err != nil {
		t.Fatalf("close setup DB: %v", err)
	}

	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

	wantErr := errors.New("maintenance second root delta iterator failure")
	iter := &closeCountingUnsafeIterator{err: wantErr}
	_, _, err = db.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(
		storagemaintenance.ColumnAssetRewritePlan(),
		[]StorageMaintenanceRootDeltaPublishInput{
			{
				BaseRoot: baseRootA,
				Iter:     mustFrozenSystemMemtable(t, "root/a", "value-a").NewIterator(nil, nil),
			},
			{
				BaseRoot: baseRootB,
				Iter:     iter,
			},
		},
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			t.Fatalf("maintenance system builder should not run after post-root-apply failure")
			return nil, nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("maintenance publish error=%v want %v", err, wantErr)
	}
	if errors.Is(err, ErrStorageMaintenancePublishPreApplyFailed) {
		t.Fatalf("maintenance publish error=%v must not be marked pre-apply after a root delta was applied", err)
	}
	requireCommandWALPublishReady(t, db, "post-root-apply maintenance failure")
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

func TestPublishOrderedRootDeltaBatchGroupWithSystemBuilderErrorClosesReturnedIterator(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	iter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("batch system builder returned iterator with error")
	_, _, err = db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(
		nil,
		func() error { return nil },
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
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
}

func TestPublishOrderedRootDeltaBatchGroupOptimisticSystemBuilderErrorClosesReturnedIterator(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, _, err = db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(
		nil,
		func() error { return nil },
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != 0 {
				t.Fatalf("rootIDs=%v, want empty roots", rootIDs)
			}
			return mustFrozenSystemMemtable(t, "sys/init", "1").NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("seed system root: %v", err)
	}

	iter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("optimistic batch system builder returned iterator with error")
	_, _, err = db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(
		nil,
		func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
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

func TestOrderedRootDeltaRejectsBaseActivatedAfterRootBuild(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = database.Close() }()

	firstReleased := make(chan struct{})
	releaseFirst := make(chan struct{})
	database.testAfterFinalizeRootSerializationReleaseHook = func() {
		close(firstReleased)
		<-releaseFirst
	}
	publishDelta := func(key, value string) error {
		_, _, err := database.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			delta := mustFrozenSystemMemtable(t, key, value)
			return delta.NewIterator(nil, nil), nil
		})
		return err
	}

	firstErr := make(chan error, 1)
	go func() { firstErr <- publishDelta("sys/first", "first") }()
	select {
	case <-firstReleased:
	case <-time.After(5 * time.Second):
		t.Fatal("first publication did not release root serialization")
	}
	database.testAfterFinalizeRootSerializationReleaseHook = nil

	secondAtFinalize := make(chan struct{})
	releaseSecond := make(chan struct{})
	database.testBeforeFinalizeCommitHook = func() {
		close(secondAtFinalize)
		<-releaseSecond
	}
	secondErr := make(chan error, 1)
	go func() { secondErr <- publishDelta("sys/second", "second") }()
	select {
	case <-secondAtFinalize:
	case <-time.After(5 * time.Second):
		t.Fatal("second publication did not finish its stale root build")
	}

	close(releaseFirst)
	if err := <-firstErr; err != nil {
		t.Fatalf("first publication: %v", err)
	}
	close(releaseSecond)
	err = <-secondErr
	database.testBeforeFinalizeCommitHook = nil
	if !errors.Is(err, errDurableRootCandidateStale) {
		t.Fatalf("second publication err=%v want stale candidate rejection", err)
	}
	if CommitPublicationAccepted(err) {
		t.Fatalf("second publication stale rejection was marked accepted: %v", err)
	}

	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot after stale rejection is nil")
	}
	state, ok := snap.StateToken()
	if !ok {
		_ = snap.Close()
		t.Fatal("snapshot state unavailable")
	}
	entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte("sys/first"))
	_ = snap.Close()
	if err != nil || string(entry.Value) != "first" {
		t.Fatalf("first value after stale rejection=%q err=%v want first", string(entry.Value), err)
	}

	if err := publishDelta("sys/second", "second"); err != nil {
		t.Fatalf("retry second publication: %v", err)
	}
	snap = database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot after retry is nil")
	}
	defer func() { _ = snap.Close() }()
	state, ok = snap.StateToken()
	if !ok {
		t.Fatal("snapshot state after retry unavailable")
	}
	for key, want := range map[string]string{"sys/first": "first", "sys/second": "second"} {
		entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte(key))
		if err != nil || string(entry.Value) != want {
			t.Fatalf("%s after retry=%q err=%v want %q", key, string(entry.Value), err, want)
		}
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
		"treedb.publish.ordered_root_delta_group.publish_prepare_ns_total",
		"treedb.publish.ordered_root_delta_group.publish_prepare_calls_total",
		"treedb.publish.ordered_root_delta_group.publish_prepare_errors_total",
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
	systemIter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("system builder failed")
	_, _, err = db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return systemIter, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want %v", err, wantErr)
	}
	if systemIter.closes != 1 {
		t.Fatalf("system iterator closes=%d want 1", systemIter.closes)
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

func TestPublishOrderedRootDeltaGroupWithSystemBuilderErrorClosesReturnedIterator(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	baseRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "root/a", "va").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}
	delta := mustFrozenSystemMemtable(t, "root/b", "vb")
	systemIter := &closeCountingUnsafeIterator{}
	wantErr := errors.New("system builder returned iterator with error")
	_, _, err = db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot: baseRoot,
		Iter:     delta.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
		}
		return systemIter, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want %v", err, wantErr)
	}
	if systemIter.closes != 1 {
		t.Fatalf("system iterator closes=%d want 1", systemIter.closes)
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
	if got := stats["treedb.publish.ordered_root_delta_group.publish_prepare_calls_total"]; got != "1" {
		t.Fatalf("publish prepare calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.publish_prepare_errors_total"]; got != "0" {
		t.Fatalf("publish prepare errors stat=%q want 0", got)
	}
}

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder_AppliesRootsInParallel(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()

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

	systemRoot, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{BaseRoot: 0, Delta: deltaA, ParallelApply: true},
		{BaseRoot: 0, Delta: deltaB, ParallelApply: true},
	}, mustRawKVCommandWALIntent(t, db, "cmd/parallel-roots", "1"), func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			return nil, errors.New("unexpected command WAL root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish command WAL delta batch group: %v", err)
	}
	if systemRoot == 0 || len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
		t.Fatalf("systemRoot=%d rootIDs=%v, want non-zero system root and two non-zero roots", systemRoot, rootIDs)
	}
	if got := db.State().AppliedCommandLSN; got == 0 {
		t.Fatalf("AppliedCommandLSN=%d, want command WAL frame applied", got)
	}
	requireCommandWALPublishReady(t, db, "parallel command WAL root publish")

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

func TestPublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder_WarmRootsUseFlushApplyOptions(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{
		Dir:                        dir,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      2,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	makeDelta := func(kv ...string) *batch.Batch {
		t.Helper()
		table := mustFrozenSystemMemtable(t, kv...)
		iter := table.NewIterator(nil, nil)
		delta, err := OrderedRootDeltaBatchFromIterator(iter)
		_ = iter.Close()
		if err != nil {
			t.Fatalf("delta batch: %v", err)
		}
		return delta
	}

	baseA := makeDelta("a/1", "base-a")
	defer func() { _ = baseA.Close() }()
	baseB := makeDelta("b/1", "base-b")
	defer func() { _ = baseB.Close() }()
	_, baseRootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{BaseRoot: 0, Delta: baseA},
		{BaseRoot: 0, Delta: baseB},
	}, mustRawKVCommandWALIntent(t, db, "cmd/base-roots", "1"), func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			return nil, errors.New("unexpected base root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish base roots: %v", err)
	}

	updateA := makeDelta("a/2", "delta-a")
	defer func() { _ = updateA.Close() }()
	updateB := makeDelta("b/2", "delta-b")
	defer func() { _ = updateB.Close() }()
	_, updatedRootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{
		{
			BaseRoot:          baseRootIDs[0],
			Delta:             updateA,
			ParallelApply:     true,
			SpanNativeRoute:   OrderedRootSpanNativeRouteCollectionBufferedRoots,
			SpanNativeContext: "collection route must be superseded by command WAL",
		},
		{
			BaseRoot:          baseRootIDs[1],
			Delta:             updateB,
			ParallelApply:     true,
			SpanNativeRoute:   OrderedRootSpanNativeRouteCollectionBufferedRoots,
			SpanNativeContext: "collection route must be superseded by command WAL",
		},
	}, mustRawKVCommandWALIntent(t, db, "cmd/warm-roots", "1"), func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			return nil, errors.New("unexpected updated root IDs")
		}
		return mustFrozenSystemMemtable(t,
			"sys/collections/users/a", strconv.FormatUint(rootIDs[0], 10),
			"sys/collections/users/b", strconv.FormatUint(rootIDs[1], 10),
		).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish warm roots: %v", err)
	}
	requireCommandWALPublishReady(t, db, "warm command WAL root publish")

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
			entry, err := snap.GetEntryAtRoot(updatedRootIDs[rootIdx], []byte(key))
			if err != nil {
				t.Fatalf("GetEntryAtRoot(root=%d key=%s): %v", rootIdx, key, err)
			}
			if got := string(entry.Value); got != want {
				t.Fatalf("root=%d key=%s got=%q want %q", rootIdx, key, got, want)
			}
		}
	}

	stats := db.Stats()
	// Two command-WAL publishes prepare both warm root-local applies and the
	// warm system-root iterator applies that cover those roots.
	if got := stats["treedb.flush_apply.read_only_prepare.calls_total"]; got != "4" {
		t.Fatalf("read-only prepare calls stat=%q want 4", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"]; got != "1" {
		t.Fatalf("parallel groups stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"]; got != "2" {
		t.Fatalf("parallel roots stat=%q want 2", got)
	}
	commandRoutePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish."
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.collection_buffered_roots.candidate_ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.system_delta_builder_publish.candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"eligible_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, commandRoutePrefix+"used_ops_total")
	requireOrderedRootStatCounterZero(t, stats, commandRoutePrefix+"fallback.reason."+FlushSpanRunFallbackSpanNativeNotImplemented.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.delta_batch_publish.candidate_ops_total")
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
		{
			BaseRoot:          0,
			Delta:             deltaA,
			ParallelApply:     true,
			SpanNativeRoute:   OrderedRootSpanNativeRouteCollectionBufferedRoots,
			SpanNativeContext: "collection route must not override cold build",
		},
		{
			BaseRoot:          0,
			Delta:             deltaB,
			ParallelApply:     true,
			SpanNativeRoute:   OrderedRootSpanNativeRouteCommandWALPublish,
			SpanNativeContext: "command WAL route must not override cold build",
		},
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
	overlayPrefix := "treedb.publish.ordered_root_delta_group.span_native.route.overlay_cold_build."
	requireOrderedRootStatCounterPositive(t, stats, overlayPrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, overlayPrefix+"fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.collection_buffered_roots.fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, "treedb.publish.ordered_root_delta_group.span_native.route.command_wal_publish.fallback.reason."+FlushSpanRunFallbackColdBuild.String()+".ops_total")
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
	}, serialAlloc, coldAlloc, OrderedRootSpanNativeRouteMultiIndexGroupPublish, "test mixed group root apply", false)
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

type orderedRootMixedKeyFamily struct {
	prefix string
	count  int
}

var orderedRootMixedKeyFamilies = []orderedRootMixedKeyFamily{
	{"like-", 9_000},
	{"post-", 2_000},
	{"graph-", 9_000},
	{"identity-", 1_000},
}

func newOrderedRootMixedKeyFamilyTable(tb testing.TB) (memtable.Table, []byte) {
	tb.Helper()
	total := orderedRootMixedKeyFamilyRowCount()
	table, values := newOrderedRootMixedKeyFamilyTableRange(tb, 0, total)
	identityZeroValue := values["identity-0"]
	if identityZeroValue == nil {
		tb.Fatal("test fixture did not create identity-0")
	}
	return table, identityZeroValue
}

func orderedRootMixedKeyFamilyRowCount() int {
	total := 0
	for _, family := range orderedRootMixedKeyFamilies {
		total += family.count
	}
	return total
}

func orderedRootMixedKeyFamilyRowKey(row int) string {
	for _, family := range orderedRootMixedKeyFamilies {
		if row < family.count {
			return family.prefix + strconv.Itoa(row)
		}
		row -= family.count
	}
	return ""
}

func newOrderedRootMixedKeyFamilyTableRange(tb testing.TB, start, end int) (memtable.Table, map[string][]byte) {
	tb.Helper()
	total := orderedRootMixedKeyFamilyRowCount()
	if start < 0 || end < start || end > total {
		tb.Fatalf("invalid row range [%d,%d) total=%d", start, end, total)
	}
	table := memtable.NewAppendOnlyWithEntryCapacity(total)
	values := make(map[string][]byte)
	for row := start; row < end; row++ {
		key := orderedRootMixedKeyFamilyRowKey(row)
		if key == "" {
			tb.Fatalf("missing key for row %d", row)
		}
		value := orderedRootSemanticStreamLocatorValueForTest(row)
		if key == "identity-0" {
			values[key] = append([]byte(nil), value...)
		}
		table.Set([]byte(key), value)
	}
	table.Freeze()
	return table, values
}

func orderedRootSemanticStreamLocatorValueForTest(row int) []byte {
	blockKey := sha256.Sum256([]byte("block-" + strconv.Itoa(row/4096)))
	out := make([]byte, 0, len("crss1loc\x00")+sha256.Size+binary.MaxVarintLen64)
	out = append(out, []byte("crss1loc\x00")...)
	out = append(out, blockKey[:]...)
	out = binary.AppendUvarint(out, uint64(row%4096))
	return out
}

func TestPublishOrderedRootValueLogLeavesColdBuildSeekMixedKeyFamilies(t *testing.T) {
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

	table, value := newOrderedRootMixedKeyFamilyTable(t)

	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          table.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}})
	if err != nil {
		t.Fatalf("publish value-log root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("identity-0"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(identity-0): %v", err)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Fatalf("identity-0 value len=%d want %d", len(entry.Value), len(value))
	}

	it, err := snap.IteratorAtRoot(rootIDs[0], []byte("identity-0"), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(identity-0): %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatalf("seek identity-0 invalid: %v", it.Error())
	}
	if got := string(it.UnsafeKey()); got != "identity-0" {
		t.Fatalf("seek identity-0 landed on %q", got)
	}
}

func TestPublishOrderedRootDeltaGroupValueLogLeavesColdBuildSeekMixedKeyFamilies(t *testing.T) {
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

	table, value := newOrderedRootMixedKeyFamilyTable(t)

	_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot:      0,
		Iter:          table.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish value-log root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("identity-0"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(identity-0): %v", err)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Fatalf("identity-0 value len=%d want %d", len(entry.Value), len(value))
	}

	it, err := snap.IteratorAtRoot(rootIDs[0], []byte("identity-0"), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(identity-0): %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatalf("seek identity-0 invalid: %v", it.Error())
	}
	if got := string(it.UnsafeKey()); got != "identity-0" {
		t.Fatalf("seek identity-0 landed on %q", got)
	}
}

func TestPublishOrderedRootDeltaGroupCommandWALContextValueLogLeavesColdBuildSeekMixedKeyFamilies(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if !db.HasValueLogAppender() {
		t.Fatal("command-WAL DB did not install value-log appender")
	}

	table, value := newOrderedRootMixedKeyFamilyTable(t)

	var seenContext bool
	_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
		[]OrderedRootDeltaPublishInput{{
			BaseRoot:      0,
			Iter:          table.NewIterator(nil, nil),
			StoragePolicy: OrderedRootStorageValueLogLeaves,
		}},
		nil,
		mustRawKVCommandWALIntent(t, db, "cmd/root-initial", "1"),
		func(ctx CommandWALPublishContext) ([]OrderedRootDeltaPublishInput, error) {
			seenContext = ctx.AppliedCommandLSN != 0
			return nil, nil
		},
		func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if ctx.AppliedCommandLSN == 0 {
				t.Fatalf("AppliedCommandLSN=0 in system builder")
			}
			return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		},
	)
	if err != nil {
		t.Fatalf("publish value-log root: %v", err)
	}
	if !seenContext {
		t.Fatal("context root builder did not observe command WAL context")
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("identity-0"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(identity-0): %v", err)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Fatalf("identity-0 value len=%d want %d", len(entry.Value), len(value))
	}

	it, err := snap.IteratorAtRoot(rootIDs[0], []byte("identity-0"), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(identity-0): %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatalf("seek identity-0 invalid: %v", it.Error())
	}
	if got := string(it.UnsafeKey()); got != "identity-0" {
		t.Fatalf("seek identity-0 landed on %q", got)
	}
}

func TestPublishOrderedRootDeltaGroupCommandWALContextValueLogLeavesSplitSeekMixedKeyFamilies(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if !db.HasValueLogAppender() {
		t.Fatal("command-WAL DB did not install value-log appender")
	}

	total := orderedRootMixedKeyFamilyRowCount()
	mid := total / 2
	first, _ := newOrderedRootMixedKeyFamilyTableRange(t, 0, mid)
	second, values := newOrderedRootMixedKeyFamilyTableRange(t, mid, total)
	value := values["identity-0"]
	if value == nil {
		t.Fatal("second split did not contain identity-0")
	}

	publish := func(baseRoot uint64, table memtable.Table, label string) uint64 {
		t.Helper()
		var seenContext bool
		_, rootIDs, err := db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
			[]OrderedRootDeltaPublishInput{{
				BaseRoot:      baseRoot,
				Iter:          table.NewIterator(nil, nil),
				StoragePolicy: OrderedRootStorageValueLogLeaves,
			}},
			nil,
			mustRawKVCommandWALIntent(t, db, "cmd/"+label, "1"),
			func(ctx CommandWALPublishContext) ([]OrderedRootDeltaPublishInput, error) {
				seenContext = ctx.AppliedCommandLSN != 0
				return nil, nil
			},
			func(ctx CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				if ctx.AppliedCommandLSN == 0 {
					t.Fatalf("AppliedCommandLSN=0 in system builder")
				}
				return mustFrozenSystemMemtable(t, "sys/"+label, strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatalf("publish %s: %v", label, err)
		}
		if !seenContext {
			t.Fatalf("%s context root builder did not observe command WAL context", label)
		}
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			t.Fatalf("%s rootIDs=%v want one non-zero root", label, rootIDs)
		}
		return rootIDs[0]
	}

	rootID := publish(0, first, "split-first")
	rootID = publish(rootID, second, "split-second")

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	entry, err := snap.GetEntryAtRoot(rootID, []byte("identity-0"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(identity-0): %v", err)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Fatalf("identity-0 value len=%d want %d", len(entry.Value), len(value))
	}

	it, err := snap.IteratorAtRoot(rootID, []byte("identity-0"), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(identity-0): %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatalf("seek identity-0 invalid: %v", it.Error())
	}
	if got := string(it.UnsafeKey()); got != "identity-0" {
		t.Fatalf("seek identity-0 landed on %q", got)
	}
}

func TestPublishOrderedRootDeltaBatchValueLogLeavesColdBuildSeekMixedKeyFamilies(t *testing.T) {
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

	table, value := newOrderedRootMixedKeyFamilyTable(t)

	iter := table.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      0,
		Delta:         delta,
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish value-log root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("identity-0"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(identity-0): %v", err)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Fatalf("identity-0 value len=%d want %d", len(entry.Value), len(value))
	}

	it, err := snap.IteratorAtRoot(rootIDs[0], []byte("identity-0"), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(identity-0): %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatalf("seek identity-0 invalid: %v", it.Error())
	}
	if got := string(it.UnsafeKey()); got != "identity-0" {
		t.Fatalf("seek identity-0 landed on %q", got)
	}
}

func TestPublishOrderedRootCommandWALValueLogLeavesSeekAfterMixedKeyMutations(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if !db.HasValueLogAppender() {
		t.Fatal("command-WAL DB did not install value-log appender")
	}

	table, value := newOrderedRootMixedKeyFamilyTable(t)

	iter := table.NewIterator(nil, nil)
	initialDelta, err := OrderedRootDeltaBatchFromIterator(iter)
	_ = iter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator(initial): %v", err)
	}
	defer func() { _ = initialDelta.Close() }()

	_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:      0,
		Delta:         initialDelta,
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, mustRawKVCommandWALIntent(t, db, "cmd/root-initial", "1"), func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish value-log root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}
	rootID := rootIDs[0]

	publishSet := func(key string, suffix byte) {
		t.Helper()
		deltaTable, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new delta table: %v", err)
		}
		updated := bytes.Repeat([]byte{suffix}, 43)
		deltaTable.Set([]byte(key), updated)
		deltaTable.Freeze()
		iter := deltaTable.NewIterator(nil, nil)
		delta, err := OrderedRootDeltaBatchFromIterator(iter)
		_ = iter.Close()
		if err != nil {
			t.Fatalf("OrderedRootDeltaBatchFromIterator(%s): %v", key, err)
		}
		defer func() { _ = delta.Close() }()

		_, updatedRoots, err := db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot:      rootID,
			Delta:         delta,
			StoragePolicy: OrderedRootStorageValueLogLeaves,
		}}, mustRawKVCommandWALIntent(t, db, "cmd/"+key, "1"), func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return mustFrozenSystemMemtable(t, "sys/root", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
		})
		if err != nil {
			t.Fatalf("publish delta %s: %v", key, err)
		}
		if len(updatedRoots) != 1 || updatedRoots[0] == 0 {
			t.Fatalf("updatedRoots=%v want one non-zero root", updatedRoots)
		}
		rootID = updatedRoots[0]
	}

	publishSet("post-0", 'p')
	publishSet("post-1", 'q')

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	entry, err := snap.GetEntryAtRoot(rootID, []byte("identity-0"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(identity-0): %v", err)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Fatalf("identity-0 value len=%d want %d", len(entry.Value), len(value))
	}

	it, err := snap.IteratorAtRoot(rootID, []byte("identity-0"), nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot(identity-0): %v", err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatalf("seek identity-0 invalid: %v", it.Error())
	}
	if got := string(it.UnsafeKey()); got != "identity-0" {
		t.Fatalf("seek identity-0 landed on %q", got)
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
	systemIter := &closeCountingUnsafeIterator{}
	_, _, err = db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot: 0,
		Iter:     mustFrozenSystemMemtable(t, "doc/u1", "document").NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		builderRootIDs = append([]uint64(nil), rootIDs...)
		return systemIter, errors.New("system descriptor build failed")
	})
	if err == nil {
		t.Fatal("expected system builder error")
	}
	if systemIter.closes != 1 {
		t.Fatalf("system iterator closes=%d want 1", systemIter.closes)
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
