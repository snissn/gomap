package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestPreparedRootPointProfileBindsWarmAndColdDeltas(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	limit := PreparedRootPointCensusLimit{MaxPages: 64, MaxOldEntries: 1024, MaxDepth: 8, MaxPointOps: 1024}
	putBatch := func(key string) *batch.Batch {
		t.Helper()
		b := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
		if err := b.Set([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		return b
	}
	cold := putBatch("doc/1")
	defer cold.Close()
	coldInput := OrderedRootDeltaBatchPublishInput{Delta: cold}
	coldProfile, err := database.ProfilePreparedRootPointBatch(coldInput, limit)
	if err != nil || coldProfile.BaseRoot != 0 || coldProfile.PointOps != 1 || coldProfile.OutputPages == 0 {
		t.Fatalf("cold profile=%+v err=%v", coldProfile, err)
	}
	if err := checkPreparedRootPointBatch(coldInput, coldProfile); err != nil {
		t.Fatal(err)
	}

	baseRoot, err := database.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "doc/1", "old").NewIterator(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	warm := putBatch("doc/2")
	defer warm.Close()
	warmInput := OrderedRootDeltaBatchPublishInput{BaseRoot: baseRoot, Delta: warm}
	warmProfile, err := database.ProfilePreparedRootPointBatch(warmInput, limit)
	if err != nil || warmProfile.BaseRoot != baseRoot || warmProfile.PointOps != 1 || warmProfile.OutputPages == 0 {
		t.Fatalf("warm profile=%+v err=%v", warmProfile, err)
	}
	if err := checkPreparedRootPointBatch(warmInput, warmProfile); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ProfilePreparedRootPointKeys(baseRoot, OrderedRootStorageDefault, [][]byte{[]byte("doc/2")}, limit); err != nil {
		t.Fatalf("known-key profile: %v", err)
	}
	whole, err := database.ProfilePreparedRootWholePointBudget(baseRoot, OrderedRootStorageDefault, 2, 8, limit)
	if err != nil || whole.PointOps != 2 || whole.OutputPages < warmProfile.OutputPages {
		t.Fatalf("whole-root profile=%+v err=%v", whole, err)
	}
	if err := checkPreparedRootPointBatch(warmInput, PreparedRootPointProfile{BaseRoot: baseRoot, PointOps: 0, MaxKeyBytes: 8}); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("point budget: %v", err)
	}
	if err := checkPreparedRootPointBatch(warmInput, PreparedRootPointProfile{BaseRoot: 0, PointOps: 1, MaxKeyBytes: 8}); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("root binding: %v", err)
	}
	if err := checkPreparedRootPointBatch(warmInput, PreparedRootPointProfile{BaseRoot: baseRoot, PointOps: 1, MaxKeyBytes: 4}); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("key width: %v", err)
	}
}

func TestPreparedRootPointProfileViolationAfterCommandWALPoisons(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	database := openCommandWALDB(t, dir)
	defer database.Close()
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer delta.Close()
	if err := delta.Set([]byte("root/b"), []byte("value-b")); err != nil {
		t.Fatal(err)
	}
	intent := mustRawKVCommandWALIntent(t, database, "cmd/profile", "1")
	beforeApplied := database.State().AppliedCommandLSN
	_, _, err := database.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilderWithPreparedLimits(
		[]OrderedRootDeltaBatchPublishInput{{Delta: delta}},
		func() error { return nil }, intent, nil,
		func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
			t.Fatal("system builder ran after prepared root profile violation")
			return nil, nil
		},
		&PreparedRootPublicationLimits{RootPointProfiles: []PreparedRootPointProfile{{BaseRoot: 0}}},
	)
	if !errors.Is(err, ErrPreparedRootPointProfileLimit) || intent.AssignedLSN() == 0 {
		t.Fatalf("post-WAL profile violation=%v assigned=%d", err, intent.AssignedLSN())
	}
	if got := database.State().AppliedCommandLSN; got != beforeApplied {
		t.Fatalf("failed publication advanced applied LSN: %d, want %d", got, beforeApplied)
	}
	if err := database.commandWALPoisonedError(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("profile violation did not poison command WAL: %v", err)
	}
}

func TestPreparedIteratorRootPointProfileViolationAfterCommandWALPoisons(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	database := openCommandWALDB(t, dir)
	defer database.Close()
	intent := mustRawKVCommandWALIntent(t, database, "cmd/iterator-profile", "1")
	beforeApplied := database.State().AppliedCommandLSN
	_, _, err := database.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilderWithPreparedLimits(
		[]OrderedRootDeltaPublishInput{{Iter: mustFrozenSystemMemtable(t, "root/b", "value-b").NewIterator(nil, nil)}},
		func() error { return nil }, intent, nil,
		func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
			t.Fatal("system builder ran after prepared iterator profile violation")
			return nil, nil
		},
		&PreparedRootPublicationLimits{RootPointProfiles: []PreparedRootPointProfile{{BaseRoot: 0}}},
	)
	if !errors.Is(err, ErrPreparedRootPointProfileLimit) || intent.AssignedLSN() == 0 {
		t.Fatalf("iterator profile violation=%v assigned=%d", err, intent.AssignedLSN())
	}
	if got := database.State().AppliedCommandLSN; got != beforeApplied {
		t.Fatalf("failed iterator publication advanced applied LSN: %d, want %d", got, beforeApplied)
	}
	if err := database.commandWALPoisonedError(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("iterator profile violation did not poison command WAL: %v", err)
	}
}

func TestPreparedCallerRootProfileRejectsBeforeCommandWAL(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	database := openCommandWALDB(t, dir)
	defer database.Close()
	intent := mustRawKVCommandWALIntent(t, database, "cmd/oversized-point-profile", "1")
	beforeApplied := database.State().AppliedCommandLSN
	_, _, err := database.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilderWithPreparedLimits(
		[]OrderedRootDeltaPublishInput{{Iter: mustFrozenSystemMemtable(t, "root/b", "value-b", "root/c", "value-c").NewIterator(nil, nil)}},
		func() error { return nil }, intent, nil,
		func(CommandWALPublishContext, []uint64) (iterator.UnsafeIterator, error) {
			t.Fatal("system builder ran after pre-WAL profile rejection")
			return nil, nil
		},
		&PreparedRootPublicationLimits{InitialPointCensusLimit: PreparedRootPointCensusLimit{MaxPages: 1, MaxOldEntries: 1, MaxDepth: 8, MaxPointOps: 1}},
	)
	if !errors.Is(err, ErrPreparedRootPointProfileLimit) || intent.AssignedLSN() != 0 {
		t.Fatalf("pre-WAL profile rejection=%v assigned=%d", err, intent.AssignedLSN())
	}
	if got := database.State().AppliedCommandLSN; got != beforeApplied {
		t.Fatalf("rejected profile advanced applied LSN: %d, want %d", got, beforeApplied)
	}
	if err := database.commandWALPoisonedError(); err != nil {
		t.Fatalf("pre-WAL rejection poisoned command WAL: %v", err)
	}
}

func TestPreparedRootPointProfileRejectsUnsupportedShape(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	limit := PreparedRootPointCensusLimit{MaxPages: 64, MaxOldEntries: 1024, MaxDepth: 8, MaxPointOps: 1024}
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer delta.Close()
	if err := delta.Delete([]byte("doc/1")); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ProfilePreparedRootPointBatch(OrderedRootDeltaBatchPublishInput{Delta: delta}, limit); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("cold delete: %v", err)
	}
	unsorted := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer unsorted.Close()
	if err := unsorted.Set([]byte("doc/2"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := unsorted.Set([]byte("doc/1"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ProfilePreparedRootPointBatch(OrderedRootDeltaBatchPublishInput{Delta: unsorted}, limit); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("unsorted batch: %v", err)
	}
	if _, err := database.ProfilePreparedRootPointKeys(0, OrderedRootStorageDefault, [][]byte{[]byte("doc/2"), []byte("doc/1")}, limit); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("unordered keys: %v", err)
	}
	if _, err := database.ProfilePreparedRootWholePointBudget(0, OrderedRootStorageDefault, 2, 8, PreparedRootPointCensusLimit{}); !errors.Is(err, ErrPreparedRootPointProfileLimit) {
		t.Fatalf("missing census limits: %v", err)
	}
}
