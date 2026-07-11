package db

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func stabilizeRecoveryWindowForTest(t testing.TB, db *DB) {
	t.Helper()
	if db == nil || db.rootPublication == nil {
		t.Fatal("missing root publication coordinator")
	}
	if err := db.rootPublication.stabilizeRecoveryWindow(db.currentCommitSeq()); err != nil {
		t.Fatalf("stabilize recovery window: %v", err)
	}
}

type rootPublicationTestAppender struct {
	syncCalls atomic.Uint64
	refCalls  [][]uint32
}

type rootPublicationTestLeafLog struct {
	syncCalls atomic.Uint64
}

func (*rootPublicationTestLeafLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}
func (*rootPublicationTestLeafLog) Flush() error { return nil }
func (l *rootPublicationTestLeafLog) Sync() error {
	l.syncCalls.Add(1)
	return nil
}

func (a *rootPublicationTestAppender) AppendValues([][]byte) ([]page.ValuePtr, error) {
	return nil, nil
}
func (a *rootPublicationTestAppender) Flush() error { return nil }
func (a *rootPublicationTestAppender) Sync() error {
	a.syncCalls.Add(1)
	return nil
}
func (a *rootPublicationTestAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}
func (a *rootPublicationTestAppender) FlushValueLogExternalRefs(fileIDs []uint32, sync bool) error {
	if !sync {
		return errors.New("publication dependency was not synced")
	}
	a.refCalls = append(a.refCalls, append([]uint32(nil), fileIDs...))
	return nil
}

func TestRootPublicationRelaxedVisibilityLeadsDurability(t *testing.T) {
	db, err := Open(Options{
		Dir:                    t.TempDir(),
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	appender := &rootPublicationTestAppender{}
	db.SetValueLogAppender(appender)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testRootPublicationBeforeDependencySync = func() {
		close(entered)
		<-release
	}
	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte("visible"), []byte("before-durable")); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- b.Write() }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relaxed write waited for publication fence")
	}
	<-entered
	if got := appender.syncCalls.Load(); got != 0 {
		t.Fatalf("relaxed caller waited for %d dependency syncs", got)
	}
	s := db.rootPublication.snapshot()
	if s.visibleCommitSeq <= s.durableCommitSeq {
		t.Fatalf("visible=%d durable=%d, want visible lead", s.visibleCommitSeq, s.durableCommitSeq)
	}
	if got, err := db.Get([]byte("visible")); err != nil || string(got) != "before-durable" {
		t.Fatalf("Get=%q err=%v", got, err)
	}
	close(release)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s = db.rootPublication.snapshot()
	if s.visibleCommitSeq != s.durableCommitSeq {
		t.Fatalf("visible=%d durable=%d after checkpoint", s.visibleCommitSeq, s.durableCommitSeq)
	}
}

func TestRootPublicationFreshRelaxedWriteCheckpointReopens(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("first"), []byte("publication")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get([]byte("first"))
	if err != nil || string(got) != "publication" {
		t.Fatalf("reopened Get=%q err=%v", got, err)
	}
}

func TestRootPublicationCoalescedFrontierSyncsDependencyClosure(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	first := &rootPublicationTestAppender{}
	rotated := &rootPublicationTestAppender{}
	firstLeaf := &rootPublicationTestLeafLog{}
	rotatedLeaf := &rootPublicationTestLeafLog{}
	candidates := []*PreparedRootCandidate{
		{CommitSeq: 1, ValueLogAppender: first, LeafPageLog: firstLeaf, TouchedValueLogFiles: []uint32{11}},
		{CommitSeq: 2, ValueLogAppender: rotated, LeafPageLog: rotatedLeaf, TouchedValueLogFiles: []uint32{22}},
		{CommitSeq: 3, ValueLogAppender: first, LeafPageLog: firstLeaf, TouchedValueLogFiles: []uint32{33}},
	}
	if err := db.flushRootPublicationClosureDurability(db.idx.Load(), candidates); err != nil {
		t.Fatal(err)
	}
	if got := first.refCalls; len(got) != 2 || len(got[0]) != 1 || got[0][0] != 11 || len(got[1]) != 1 || got[1][0] != 33 {
		t.Fatalf("first appender closure calls=%v", got)
	}
	if got := rotated.refCalls; len(got) != 1 || len(got[0]) != 1 || got[0][0] != 22 {
		t.Fatalf("rotated appender closure calls=%v", got)
	}
	if got := firstLeaf.syncCalls.Load(); got != 2 {
		t.Fatalf("first leaf-log closure sync calls=%d want=2", got)
	}
	if got := rotatedLeaf.syncCalls.Load(); got != 1 {
		t.Fatalf("rotated leaf-log closure sync calls=%d want=1", got)
	}
}

func TestRootPublicationIndexPagesExcludePagesRetiredByFrontier(t *testing.T) {
	candidates := []*PreparedRootCandidate{
		{TouchedIndexPages: []uint64{9, 3, 7}, FreelistHeadID: 17},
		{TouchedIndexPages: []uint64{11}, RetiredPages: []uint64{7}, FreelistHeadID: 19},
		{TouchedIndexPages: []uint64{13}, RetiredPages: []uint64{11}, FreelistHeadID: 23},
	}

	got := rootPublicationIndexPages(candidates)
	want := []uint64{3, 9, 13, 23}
	if len(got) != len(want) {
		t.Fatalf("root publication pages=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("root publication pages=%v want=%v", got, want)
		}
	}
}

func TestRootPublicationIndexPagesKeepReusedFormerFreelistHeadAsTreeOutput(t *testing.T) {
	candidates := []*PreparedRootCandidate{
		{TouchedIndexPages: []uint64{3}, FreelistHeadID: 7},
		{TouchedIndexPages: []uint64{7}, FreelistHeadID: 9},
	}

	got := rootPublicationIndexPages(candidates)
	want := []uint64{3, 7, 9}
	if len(got) != len(want) {
		t.Fatalf("root publication pages=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("root publication pages=%v want=%v", got, want)
		}
	}
}

func TestRootPublicationSealsCurrentAllocatorStateWithoutMutatingCandidate(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	idx := db.idx.Load()
	pageID, err := idx.pager.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.allocator.Free(pageID); err != nil {
		t.Fatal(err)
	}
	candidate := &PreparedRootCandidate{
		FreelistHeadID: 1,
		TotalPages:     2,
		Meta: page.MetaPageBody{
			FreelistHeadID: 1,
			TotalPages:     2,
		},
	}

	sealed := db.rootPublication.sealedAllocatorFrontier(idx, candidate)
	if sealed == candidate {
		t.Fatal("sealed frontier must not mutate the registered candidate")
	}
	if got, want := sealed.FreelistHeadID, idx.allocator.Head(); got != want {
		t.Fatalf("sealed freelist head=%d want=%d", got, want)
	}
	if got, want := sealed.TotalPages, idx.pager.PageCount(); got != want {
		t.Fatalf("sealed total pages=%d want=%d", got, want)
	}
	if sealed.Meta.FreelistHeadID != sealed.FreelistHeadID || sealed.Meta.TotalPages != sealed.TotalPages {
		t.Fatalf("sealed meta=%+v candidate=%+v", sealed.Meta, sealed)
	}
	if candidate.FreelistHeadID != 1 || candidate.TotalPages != 2 {
		t.Fatalf("registered candidate mutated: %+v", candidate)
	}
}

func TestRootPublicationDebtCapsRejectBeforeVisibility(t *testing.T) {
	for _, test := range []struct {
		name         string
		pending      []*PreparedRootCandidate
		pendingBytes uint64
		candidate    *PreparedRootCandidate
	}{
		{
			name:      "commit count",
			pending:   make([]*PreparedRootCandidate, rootPublicationMaxPendingCommits),
			candidate: &PreparedRootCandidate{CommitSeq: 9001},
		},
		{
			name:         "owned bytes",
			pendingBytes: rootPublicationMaxPendingBytes,
			candidate:    &PreparedRootCandidate{CommitSeq: 9002, OwnedBytes: 1},
		},
		{
			name:         "external side-store bytes",
			pendingBytes: rootPublicationMaxPendingBytes - 1,
			candidate: &PreparedRootCandidate{
				CommitSeq:  9003,
				OwnedBytes: rootPublicationOwnedBytes(nil, nil, adaptive.Metrics{SlabWriteBytes: 2}, 0),
			},
		},
		{
			name:         "saturating owned bytes",
			pendingBytes: 1,
			candidate:    &PreparedRootCandidate{CommitSeq: 9004, OwnedBytes: ^uint64(0)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := &DB{}
			db.publishPrepareMu.RLock()
			test.candidate.holdsPreparePin = true
			r := &RootPublicationCoordinator{
				db:               db,
				pending:          test.pending,
				pendingBytes:     test.pendingBytes,
				visibleCommitSeq: 7,
				stall:            ErrPublicationStalled,
			}
			if err := r.register(test.candidate); !errors.Is(err, ErrPublicationStalled) {
				t.Fatalf("register error=%v", err)
			}
			if got := r.visibleCommitSeq; got != 7 {
				t.Fatalf("visibility advanced to %d past debt cap", got)
			}
		})
	}
}

func TestRootPublicationStallRemainsUntilSuccessAndEnforcesDebtCap(t *testing.T) {
	db := &DB{}
	r := &RootPublicationCoordinator{
		db:               db,
		wake:             make(chan struct{}, 1),
		pending:          make([]*PreparedRootCandidate, rootPublicationMaxPendingCommits-1),
		visibleCommitSeq: 7,
		stall:            ErrPublicationStalled,
	}
	register := func(seq uint64) error {
		db.publishPrepareMu.RLock()
		return r.register(&PreparedRootCandidate{CommitSeq: seq, holdsPreparePin: true})
	}
	if err := register(8); err != nil {
		t.Fatalf("candidate at cap was not accepted for retry: %v", err)
	}
	if !errors.Is(r.stall, ErrPublicationStalled) {
		t.Fatalf("accepted retry cleared stall before publication success: %v", r.stall)
	}
	if err := register(9); !errors.Is(err, ErrPublicationStalled) {
		t.Fatalf("candidate beyond stalled cap error=%v", err)
	}
	if got := len(r.pending); got != rootPublicationMaxPendingCommits {
		t.Fatalf("pending candidates=%d want=%d", got, rootPublicationMaxPendingCommits)
	}
	for _, candidate := range r.pending {
		if candidate != nil && candidate.holdsPreparePin {
			candidate.holdsPreparePin = false
			db.publishPrepareMu.RUnlock()
		}
	}
}

func TestRootPublicationTracksValueLogRootsPerRecoverableMetaSlot(t *testing.T) {
	db := &DB{}
	r := &RootPublicationCoordinator{
		db:                db,
		cond:              sync.NewCond(&sync.Mutex{}),
		durableMetaPageID: 0,
		metaSlotValid:     [2]bool{true, true},
		metaSlotMeta: [2]page.MetaPageBody{
			{CommitSeq: 1, UserRootPageID: 10, SystemRootPageID: 11},
			{CommitSeq: 2, UserRootPageID: 20, SystemRootPageID: 21},
		},
		pending: []*PreparedRootCandidate{{
			CommitSeq: 3,
			Meta:      page.MetaPageBody{CommitSeq: 3, UserRootPageID: 30, SystemRootPageID: 31},
		}},
	}
	r.cond = sync.NewCond(&r.mu)
	r.complete(r.pending[0])
	userRoots, systemRoots, generation := r.recoverableValueLogRoots()
	if got, want := fmt.Sprint(userRoots), "[10 30]"; got != want {
		t.Fatalf("recoverable user roots=%s want=%s", got, want)
	}
	if got, want := fmt.Sprint(systemRoots), "[11 31]"; got != want {
		t.Fatalf("recoverable system roots=%s want=%s", got, want)
	}
	if generation != 1 {
		t.Fatalf("recovery root generation=%d want=1", generation)
	}
}

func TestRootPublicationSignalsOnlyForEmptyQueueOrStallRetry(t *testing.T) {
	db := &DB{}
	r := &RootPublicationCoordinator{db: db, wake: make(chan struct{}, 4)}
	register := func(seq uint64) {
		db.publishPrepareMu.RLock()
		candidate := &PreparedRootCandidate{CommitSeq: seq, holdsPreparePin: true}
		if err := r.register(candidate); err != nil {
			t.Fatalf("register %d: %v", seq, err)
		}
	}
	defer func() {
		for _, candidate := range r.pending {
			if candidate.holdsPreparePin {
				candidate.holdsPreparePin = false
				db.publishPrepareMu.RUnlock()
			}
		}
	}()

	register(1)
	register(2)
	if got := len(r.wake); got != 1 {
		t.Fatalf("queued wake events=%d want=1", got)
	}
	<-r.wake
	r.mu.Lock()
	r.stall = ErrPublicationStalled
	r.mu.Unlock()
	register(3)
	if got := len(r.wake); got != 1 {
		t.Fatalf("stall retry wake events=%d want=1", got)
	}
}

func TestRootPublicationRegisterUsesTransferredPinPastQueuedMaintenance(t *testing.T) {
	db := &DB{}
	db.publishPrepareMu.RLock()
	guard := &finalizeCommitPrepareGuard{db: db}
	candidate := &PreparedRootCandidate{CommitSeq: 1}
	guard.transferTo(candidate)
	if !candidate.holdsPreparePin {
		t.Fatal("candidate did not receive preparation pin")
	}
	r := &RootPublicationCoordinator{db: db, wake: make(chan struct{}, 1)}

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	go func() {
		close(writerStarted)
		db.publishPrepareMu.Lock()
		close(writerAcquired)
		db.publishPrepareMu.Unlock()
	}()
	<-writerStarted
	time.Sleep(20 * time.Millisecond) // let the exclusive maintenance waiter queue

	registered := make(chan error, 1)
	go func() { registered <- r.register(candidate) }()
	select {
	case err := <-registered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("registration recursively reacquired preparation RLock")
	}

	r.mu.Lock()
	r.pending = nil
	r.pendingBytes = 0
	r.mu.Unlock()
	candidate.holdsPreparePin = false
	db.publishPrepareMu.RUnlock()
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("queued maintenance writer did not acquire preparation lock")
	}
}

func TestRootPublicationRecoveryStabilizationLetsPublisherReleaseExistingPin(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	r := db.rootPublication

	// Queue an already-prepared candidate without waking the publisher. This
	// models a candidate registering between stabilization's initial durable
	// check and its admission fence.
	r.publishMu.Lock()
	db.publishPrepareMu.RLock()
	r.mu.Lock()
	candidate := &PreparedRootCandidate{
		CommitSeq:       r.durableMeta.CommitSeq,
		Meta:            r.durableMeta,
		holdsPreparePin: true,
	}
	r.pending = append(r.pending, candidate)
	r.recoveryClosureGeneration++
	r.mu.Unlock()

	stabilized := make(chan error, 1)
	go func() { stabilized <- r.stabilizeRecoveryWindow(candidate.CommitSeq) }()
	// Let stabilization reach the exclusive preparation fence. It must not hold
	// publishMu while waiting for the candidate's read pin.
	time.Sleep(20 * time.Millisecond)
	r.signal()
	r.publishMu.Unlock()

	select {
	case err := <-stabilized:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovery stabilization deadlocked with existing candidate pin")
	}
}

func TestRootPublicationStopReleasesPendingPreparationPin(t *testing.T) {
	db := &DB{}
	db.publishPrepareMu.RLock()
	candidate := &PreparedRootCandidate{CommitSeq: 1, holdsPreparePin: true}
	r := &RootPublicationCoordinator{
		db:      db,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		pending: []*PreparedRootCandidate{candidate},
	}
	go func() {
		<-r.stop
		close(r.done)
	}()

	r.stopPublisher()
	if candidate.holdsPreparePin {
		t.Fatal("stopped coordinator retained candidate preparation pin")
	}
	if !db.publishPrepareMu.TryLock() {
		t.Fatal("stopped coordinator left publishPrepareMu read-locked")
	}
	db.publishPrepareMu.Unlock()
}

func TestRootPublicationCloseDrainsAndStopsPublisher(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	db.testRootPublicationBeforeDependencySync = func() {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	if err := db.Set([]byte("close"), []byte("drain")); err != nil {
		t.Fatal(err)
	}
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before publisher drain: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain and stop root publisher")
	}
	select {
	case <-db.rootPublication.done:
	default:
		t.Fatal("root publisher goroutine remained live after Close")
	}
	publication := db.rootPublication.snapshot()
	if publication.pendingCandidates != 0 || publication.pendingBytes != 0 {
		t.Fatalf("publication debt remained after Close: %+v", publication)
	}
}

func TestRootPublicationPendingCandidateDoesNotHoldGCPreparationLock(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	db.testRootPublicationBeforeDependencySync = func() {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	if err := db.Set([]byte("pending"), []byte("visible")); err != nil {
		t.Fatal(err)
	}
	<-entered
	if !db.publishPrepareMu.TryLock() {
		close(release)
		t.Fatal("visible pending candidate retained coarse GC preparation lock")
	}
	db.publishPrepareMu.Unlock()
	close(release)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
}

func TestRootPublicationOrderedRootGroupPublishesThroughCoordinator(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	registered := make(chan *PreparedRootCandidate, 1)
	db.testRootPublicationRegistered = func(candidate *PreparedRootCandidate) { registered <- candidate }
	newSystemRoot, _, err := db.PublishOrderedRootGroup(
		mustFrozenSystemMemtable(t, "ordered/coordinator", "durable").NewIterator(nil, nil), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := <-registered
	if candidate.Meta.SystemRootPageID != newSystemRoot || candidate.CommitSeq != db.State().CommitSeq {
		t.Fatalf("registered candidate=%+v state=%+v newSystemRoot=%d", candidate.Meta, db.State(), newSystemRoot)
	}
	db.testRootPublicationRegistered = nil
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	snap := reopened.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing reopened snapshot")
	}
	defer func() { _ = snap.Close() }()
	got, err := snap.GetAtRoot(reopened.State().SystemRootPageID, []byte("ordered/coordinator"))
	if err != nil || string(got) != "durable" {
		t.Fatalf("reopened ordered root value=%q err=%v", got, err)
	}
}

func TestRootPublicationNoopAppliedLSNPublishesThroughCoordinator(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	before := db.State()
	registered := make(chan *PreparedRootCandidate, 1)
	db.testRootPublicationRegistered = func(candidate *PreparedRootCandidate) { registered <- candidate }
	if err := db.publishCommandWALRoots(
		before.RootPageID, before.SystemRootPageID, before.AppliedCommandLSN+1,
		[]CommandWALLSNRange{{First: before.AppliedCommandLSN + 1, Last: before.AppliedCommandLSN + 1}}, false,
	); err != nil {
		t.Fatal(err)
	}
	candidate := <-registered
	if candidate.Meta.UserRootPageID != before.RootPageID || candidate.Meta.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("no-op AppliedLSN candidate changed roots: before=%+v candidate=%+v", before, candidate.Meta)
	}
	if candidate.Meta.AppliedCommandLSN != before.AppliedCommandLSN+1 {
		t.Fatalf("candidate AppliedCommandLSN=%d want=%d", candidate.Meta.AppliedCommandLSN, before.AppliedCommandLSN+1)
	}
	db.testRootPublicationRegistered = nil
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	after := reopened.State()
	if after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID || after.AppliedCommandLSN != before.AppliedCommandLSN+1 {
		t.Fatalf("reopened no-op AppliedLSN state=%+v before=%+v", after, before)
	}
}

func TestRootPublicationWriteSyncWaitsForDurableSequence(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	db.testRootPublicationBeforeDependencySync = func() {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- db.SetSync([]byte("sync"), []byte("wait")) }()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("WriteSync returned before durable fence: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s := db.rootPublication.snapshot()
	if s.visibleCommitSeq != s.durableCommitSeq {
		t.Fatalf("visible=%d durable=%d", s.visibleCommitSeq, s.durableCommitSeq)
	}
}

func TestRootPublicationWriteSyncWaitsForItsExactSequence(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	secondRelease := make(chan struct{})
	var attempt atomic.Uint64
	db.testRootPublicationBeforeDependencySync = func() {
		switch attempt.Add(1) {
		case 1:
			close(firstEntered)
			<-firstRelease
		case 2:
			close(secondEntered)
			<-secondRelease
		}
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- db.SetSync([]byte("first"), []byte("durable")) }()
	<-firstEntered
	if err := db.Set([]byte("later"), []byte("visible")); err != nil {
		t.Fatal(err)
	}
	close(firstRelease)
	<-secondEntered
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WriteSync waited for a later visible candidate")
	}
	close(secondRelease)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	db.testRootPublicationBeforeDependencySync = nil
}

func TestRootPublicationPhysicalWriteSyncWaitsWithCommandWALEnabled(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	db.testRootPublicationBeforeDependencySync = func() {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	b := db.NewPhysicalBatch()
	defer b.Close()
	if err := b.Set([]byte("physical"), []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.WriteSync() }()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("physical WriteSync returned before exact root durability: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRootPublicationDurabilityPathRunsWithoutRootSerializationLocks(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tryExclusive := func(tryLock func() bool, unlock func()) bool {
		deadline := time.Now().Add(100 * time.Millisecond)
		for {
			if tryLock() {
				unlock()
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
			runtime.Gosched()
		}
	}
	checkLocks := func(stage string) error {
		if !tryExclusive(db.mu.TryLock, db.mu.Unlock) {
			return fmt.Errorf("db.mu held during %s", stage)
		}
		if !tryExclusive(db.writeMu.TryLock, db.writeMu.Unlock) {
			return fmt.Errorf("writeMu held during %s", stage)
		}
		if !tryExclusive(db.commitMu.TryLock, db.commitMu.Unlock) {
			return fmt.Errorf("commitMu held during %s", stage)
		}
		return nil
	}
	checked := make(chan error, 5)
	db.testRootPublicationBeforeDependencySync = func() { checked <- checkLocks("dependency sync") }
	db.testRootPublicationBeforeWait = func() { checked <- checkLocks("durability wait") }
	var observedMu sync.Mutex
	observed := make(map[durabilitycut.Point]bool)
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch event.Point {
		case durabilitycut.BeforeIndexDataSync, durabilitycut.BeforeMetaWrite, durabilitycut.BeforeMetaSync:
			if err := checkLocks(string(event.Point)); err != nil {
				return err
			}
			observedMu.Lock()
			observed[event.Point] = true
			observedMu.Unlock()
		}
		return nil
	})
	if err := db.SetSync([]byte("lock-free"), []byte("publisher")); err != nil {
		t.Fatal(err)
	}
	db.testRootPublicationBeforeDependencySync = nil
	db.testRootPublicationBeforeWait = nil
	restore()
	for i := 0; i < 2; i++ {
		if err := <-checked; err != nil {
			t.Fatal(err)
		}
	}
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeIndexDataSync, durabilitycut.BeforeMetaWrite, durabilitycut.BeforeMetaSync} {
		if !observed[point] {
			t.Fatalf("did not observe %s", point)
		}
	}
}

func TestRootPublicationDurabilityCutOrder(t *testing.T) {
	db, err := Open(Options{
		Dir:                    t.TempDir(),
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	want := []durabilitycut.Point{
		durabilitycut.BeforeIndexDataSync,
		durabilitycut.AfterIndexDataSync,
		durabilitycut.BeforeMetaWrite,
		durabilitycut.AfterMetaWrite,
		durabilitycut.BeforeMetaSync,
		durabilitycut.AfterMetaSync,
	}
	var gotMu sync.Mutex
	var got []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		for _, point := range want {
			if event.Point == point {
				gotMu.Lock()
				got = append(got, event.Point)
				gotMu.Unlock()
				break
			}
		}
		return nil
	})
	if err := db.SetSync([]byte("ordered"), []byte("cuts")); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("durability cut order=%v want=%v", got, want)
	}
}

func TestRootPublicationProductionPointerBytesCountTowardDebt(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.SetValueLogAppender(appender)
	defer func() {
		db.SetValueLogAppender(nil)
		_ = appender.close()
		_ = db.Close()
	}()
	ptrs, err := db.AppendValueLogValues([][]byte{bytes.Repeat([]byte("publication-debt|"), 1<<15)})
	if err != nil || len(ptrs) != 1 {
		t.Fatalf("append pointers=%d err=%v", len(ptrs), err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	db.testRootPublicationBeforeDependencySync = func() {
		close(entered)
		<-release
	}
	registered := make(chan *PreparedRootCandidate, 1)
	db.testRootPublicationRegistered = func(candidate *PreparedRootCandidate) { registered <- candidate }
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("large-pointer"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	candidate := <-registered
	<-entered
	wantBytes := uint64(page.ValuePtrRecordLength(ptrs[0]))
	if candidate.OwnedBytes < wantBytes {
		t.Fatalf("candidate owned bytes=%d want at least pointer record bytes=%d", candidate.OwnedBytes, wantBytes)
	}
	if got := db.rootPublication.snapshot().pendingBytes; got < wantBytes {
		t.Fatalf("pending bytes=%d want at least pointer record bytes=%d", got, wantBytes)
	}
	db.testRootPublicationBeforeDependencySync = nil
	db.testRootPublicationRegistered = nil
	close(release)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
}

func TestRootPublicationFreelistHeadIncludedAndDurableOnReuse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, PreferAppendAlloc: false, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	start, err := db.idx.Load().pager.Alloc(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.idx.Load().allocator.Free(start); err != nil {
		t.Fatal(err)
	}
	if err := db.idx.Load().allocator.Free(start + 1); err != nil {
		t.Fatal(err)
	}
	head := db.idx.Load().allocator.Head()
	registered := make(chan *PreparedRootCandidate, 1)
	db.testRootPublicationRegistered = func(candidate *PreparedRootCandidate) { registered <- candidate }
	if err := db.SetSync([]byte("freelist-reuse"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	candidate := <-registered
	if candidate.FreelistHeadID != head {
		t.Fatalf("candidate freelist head=%d want=%d", candidate.FreelistHeadID, head)
	}
	for _, pageID := range candidate.TouchedIndexPages {
		if pageID == head {
			t.Fatalf("freelist head %d must not be owned as a COW tree page: %v", head, candidate.TouchedIndexPages)
		}
	}
	publicationPages := rootPublicationIndexPages([]*PreparedRootCandidate{candidate})
	found := false
	for _, pageID := range publicationPages {
		found = found || pageID == head
	}
	if !found {
		t.Fatalf("frontier freelist head %d absent from publication pages %v", head, publicationPages)
	}
	db.testRootPublicationRegistered = nil
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, PreferAppendAlloc: false, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get([]byte("freelist-reuse"))
	if err != nil || string(got) != "value" {
		t.Fatalf("reopened value len=%d err=%v", len(got), err)
	}
}

func TestRootPublicationOptimisticCandidateExcludesConcurrentBuilderPages(t *testing.T) {
	db := openFlushApplyTestDB(t, 4)
	putBatch(t, db, 0, 9000, "base")
	type registration struct {
		candidate []uint64
		dirty     []uint64
	}
	registered := make(chan registration, 2)
	db.testRootPublicationRegistered = func(candidate *PreparedRootCandidate) {
		registered <- registration{
			candidate: append([]uint64(nil), candidate.TouchedIndexPages...),
			dirty:     db.idx.Load().pager.DirtyIndexPages(),
		}
	}
	defer func() { db.testRootPublicationRegistered = nil }()

	var fired atomic.Bool
	db.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		other := db.NewBatch()
		for i := 0; i < 1000; i++ {
			if err := other.Set([]byte(fmt.Sprintf("candidate-b-%06d", i)), []byte("b")); err != nil {
				t.Fatal(err)
			}
		}
		if err := other.Write(); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { db.testAfterOptimisticApplyHook = nil }()

	first := db.NewBatch()
	for i := 0; i < 1000; i++ {
		if err := first.Set([]byte(fmt.Sprintf("candidate-a-%06d", i)), []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Write(); err != nil {
		t.Fatal(err)
	}
	got := <-registered // nested candidate B registers before A retries
	candidateSet := make(map[uint64]struct{}, len(got.candidate))
	for _, pageID := range got.candidate {
		candidateSet[pageID] = struct{}{}
	}
	var foreign int
	for _, pageID := range got.dirty {
		if _, ok := candidateSet[pageID]; !ok {
			foreign++
		}
	}
	if foreign == 0 {
		t.Fatalf("candidate pages=%v consumed global concurrent dirty frontier=%v", got.candidate, got.dirty)
	}
}

func TestRootPublicationPostMetaPoisonPreservesLastVisibleSnapshot(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	db.testFailSyncMeta.Store(true)
	err = db.SetSync([]byte("visible-before-poison"), []byte("still-readable"))
	db.testFailSyncMeta.Store(false)
	if !errors.Is(err, errTestSyncMetaFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync error=%v", err)
	}
	visible := db.state.Load()
	if visible == nil {
		t.Fatal("visible state missing after post-meta poison")
	}
	got, err := db.Get([]byte("visible-before-poison"))
	if err != nil || string(got) != "still-readable" {
		t.Fatalf("Get after poison=%q err=%v", got, err)
	}
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireSnapshot returned nil after poison")
	}
	if snapshot.State().CommitSeq != visible.CommitSeq {
		t.Fatalf("snapshot seq=%d visible=%d", snapshot.State().CommitSeq, visible.CommitSeq)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("blocked"), []byte("mutation")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("mutation after poison error=%v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Close error=%v, want ErrRecoveryRequired", err)
	}
}

func TestRootPublicationWriteMetaAttemptPoisons(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	db.testFailWriteMeta.Store(true)
	err = db.SetSync([]byte("visible-write-meta"), []byte("readable"))
	db.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync error=%v", err)
	}
	if got, err := db.Get([]byte("visible-write-meta")); err != nil || string(got) != "readable" {
		t.Fatalf("Get after ambiguous writeMeta=%q err=%v", got, err)
	}
	if err := db.Set([]byte("blocked"), []byte("mutation")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("mutation error=%v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Close error=%v", err)
	}
}

func TestRootPublicationPreMetaFailureStallsAndExplicitBoundaryRetries(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	oldDurable := db.meta.CommitSeq
	wantErr := errors.New("index durability unavailable")
	var failed atomic.Bool
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeIndexDataSync && failed.CompareAndSwap(false, true) {
			return wantErr
		}
		return nil
	})
	err = db.SetSync([]byte("retry"), []byte("me"))
	restore()
	if !errors.Is(err, ErrPublicationStalled) || !errors.Is(err, wantErr) {
		t.Fatalf("SetSync error=%v", err)
	}
	if got := db.meta.CommitSeq; got != oldDurable {
		t.Fatalf("durable meta advanced on pre-meta failure: got=%d want=%d", got, oldDurable)
	}
	if got, err := db.Get([]byte("retry")); err != nil || string(got) != "me" {
		t.Fatalf("visible value=%q err=%v", got, err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("retry Checkpoint: %v", err)
	}
	if db.meta.CommitSeq <= oldDurable {
		t.Fatalf("durable meta did not advance after retry: %d", db.meta.CommitSeq)
	}
}

func TestOrdinaryMetaWritesAreCoordinatorOwned(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]map[string]bool{
		"db.go":               {"recover": true},
		"root_publication.go": {"writeAndSyncMeta": true},
	}
	for _, pkg := range pkgs {
		for _, parsed := range pkg.Files {
			path := filepath.Base(fset.Position(parsed.Pos()).Filename)
			for _, decl := range parsed.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "writeMeta" {
						return true
					}
					pos := fset.Position(call.Pos())
					if allowed[path][fn.Name.Name] {
						return true
					}
					t.Errorf("writeMeta call outside named setup/coordinator owner: %s:%s:%d", path, fn.Name.Name, pos.Line)
					return true
				})
			}
		}
	}
}
