package rootpublication

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type durableRootTransactionFixture struct {
	pager       *pager.Pager
	allocator   *freelist.Allocator
	prepared    *freelist.PreparedCOWCandidateV1
	capability  freelist.ReuseCapability
	payload     DurableRootPayload
	resources   *StableResourceSet
	tx          *DurableRootTransaction
	candidate   *PreparedRootCandidate
	activates   atomic.Uint64
	aborts      atomic.Uint64
	consumes    atomic.Uint64
	failures    atomic.Uint64
	activateErr error
}

func newDurableRootTransactionFixture(t *testing.T, lineage DurableRootLineageID, seq uint64) *durableRootTransactionFixture {
	return newDurableRootTransactionFixtureWithPayload(t, lineage, seq, true)
}

func newDurableRootTransactionFixtureWithPayload(t *testing.T, lineage DurableRootLineageID, seq uint64, withPayload bool) *durableRootTransactionFixture {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	fixture := &durableRootTransactionFixture{pager: p}
	fixture.allocator = freelist.New(p, 0)
	if err := fixture.allocator.EnableCOWV1(freelist.MustNewFreelistGenerationV1(1, 4, nil, nil), freelist.NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	fixture.capability, err = freelist.NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var candidateID freelist.CandidateIDV1
	candidateID[0] = byte(seq)
	auxiliaryCount := 0
	if withPayload {
		auxiliaryCount = 2
	}
	fixture.prepared, err = fixture.allocator.PrepareCOWCandidateV1(seq, seq, candidateID, fixture.capability, auxiliaryCount, freelist.NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	generation := fixture.prepared.Candidate().Generation()
	if withPayload {
		auxiliary := fixture.prepared.AuxiliaryPageIDs()
		manifest, err := NewDependencyManifestV1(nil)
		if err != nil {
			t.Fatal(err)
		}
		manifestRef, err := manifest.Materialize(auxiliary[0], freelist.NewMemoryPageStoreV1())
		if err != nil {
			t.Fatal(err)
		}
		recordPageID := auxiliary[1]
		record := DurableRootRecordV1{
			CommitSeq: seq, DurableSeq: seq,
			UserRootPageID: 2, SystemRootPageID: 3, TotalPages: generation.HighWater(),
			Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(),
			Manifest: manifestRef, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(seq, seq, recordPageID),
		}
		_, recordDigest, err := record.EncodePage(recordPageID)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := page.NewDurableMetaV1(seq, seq, recordPageID, recordDigest)
		if err != nil {
			t.Fatal(err)
		}
		fixture.payload = DurableRootPayload{TargetMetaPageID: seq % 2, Meta: meta, Record: record}
	}
	fixture.tx, err = NewDurableRootTransaction(DurableRootTransactionSpec{
		Lineage: lineage, Sequence: seq, Payload: fixture.payload, PreparedCOW: fixture.prepared,
		Activate: func(input DurableRootCallbackInput) error {
			fixture.activates.Add(1)
			if input.PreparedCOW != fixture.prepared {
				return errors.New("activate lost exact COW candidate")
			}
			return fixture.activateErr
		},
		Abort: func(input DurableRootCallbackInput) error {
			fixture.aborts.Add(1)
			if input.PreparedCOW != fixture.prepared {
				return errors.New("abort lost exact COW candidate")
			}
			return fixture.allocator.AbortCOWCandidateV1(input.PreparedCOW)
		},
		Consume: func(input DurableRootCallbackInput) error {
			fixture.consumes.Add(1)
			if input.PreparedCOW != fixture.prepared {
				return errors.New("consume lost exact COW candidate")
			}
			generation := fixture.prepared.Candidate().Generation()
			if err := fixture.pager.Truncate(generation.HighWater()); err != nil {
				return err
			}
			for _, image := range fixture.prepared.Candidate().Pages() {
				if err := fixture.pager.Write(image.PageID, image.Data); err != nil {
					return err
				}
			}
			return fixture.allocator.PublishCOWCandidateV1(input.PreparedCOW, fixture.capability)
		},
		Fail: func(input DurableRootCallbackInput, cause error) error {
			fixture.failures.Add(1)
			if input.PreparedCOW != fixture.prepared || cause == nil {
				return errors.New("fail lost exact COW candidate or cause")
			}
			return fixture.allocator.FailCOWCandidateV1(input.PreparedCOW, cause)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.resources, err = NewStableResourceSetBuilder().Freeze()
	if err != nil {
		t.Fatal(err)
	}
	fixture.candidate, err = NewPreparedRootCandidate(CandidateSpec{
		Frontier: NewFrontier(seq, 2, 3, 0, 0), TotalPages: generation.HighWater(),
		ResourceSet: fixture.resources, DurableRoot: fixture.tx,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func durableLineage(value byte) DurableRootLineageID {
	var lineage DurableRootLineageID
	lineage[0] = value
	return lineage
}

func TestDurableRootTransactionCopiesPayloadAndCandidateAbandonAbortsExactCOW(t *testing.T) {
	fixture := newDurableRootTransactionFixture(t, durableLineage(1), 2)
	if got := fixture.tx.Owner(); got != ResourceOwnerCandidate {
		t.Fatalf("owner=%v want candidate", got)
	}
	payload := fixture.tx.Payload()
	payload.Record.CommitSeq = 99
	if got := fixture.tx.Payload().Record.CommitSeq; got != 2 {
		t.Fatalf("transaction payload mutated to commit %d", got)
	}
	if got := fixture.candidate.DurableRoot(); got != fixture.tx {
		t.Fatalf("candidate durable root=%p want %p", got, fixture.tx)
	}
	if err := fixture.candidate.Abandon(); err != nil {
		t.Fatal(err)
	}
	if got := fixture.tx.Owner(); got != ResourceOwnerReleased {
		t.Fatalf("owner after abandon=%v want released", got)
	}
	if got := fixture.aborts.Load(); got != 1 {
		t.Fatalf("abort callbacks=%d want 1", got)
	}

	var nextID freelist.CandidateIDV1
	nextID[0] = 3
	if _, err := fixture.allocator.PrepareCOWCandidateV1(3, 3, nextID, fixture.capability, 0, freelist.NewMemoryPageStoreV1()); err != nil {
		t.Fatalf("prepare after exact abort: %v", err)
	}
}

func TestDurableRootTransactionAllowsCallbackTimeSealWithoutPayloadOrAuxiliaryPage(t *testing.T) {
	fixture := newDurableRootTransactionFixtureWithPayload(t, durableLineage(1), 2, false)
	if fixture.tx.Payload() != (DurableRootPayload{}) || len(fixture.prepared.AuxiliaryPageIDs()) != 0 {
		t.Fatalf("payload/auxiliary=(%+v,%v), want zero/empty", fixture.tx.Payload(), fixture.prepared.AuxiliaryPageIDs())
	}
	coordinator, err := New(Options{
		Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
			latest := candidate.DurableRootGroup().Latest()
			if latest == nil || latest.Payload() != (DurableRootPayload{}) {
				return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("queued member froze a publication seal")}
			}
			return PublishResult{Outcome: PublishSucceeded}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), fixture.candidate); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitThrough(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if fixture.activates.Load() != 1 || fixture.consumes.Load() != 1 {
		t.Fatalf("activate/consume=(%d,%d), want (1,1)", fixture.activates.Load(), fixture.consumes.Load())
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDurableRootGroupRetainsOrderedExactMembersAcrossRetryThenConsumesThroughLatest(t *testing.T) {
	lineage := durableLineage(1)
	first := newDurableRootTransactionFixture(t, lineage, 2)
	second := newDurableRootTransactionFixture(t, lineage, 3)
	var calls int
	coordinator, err := New(Options{
		Clock: NewFakeClock(time.Unix(1, 0)),
		Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
			calls++
			group := candidate.DurableRootGroup()
			members := group.Members()
			if group.Lineage() != lineage || len(members) != 2 || members[0] != first.tx || members[1] != second.tx || group.Latest() != second.tx {
				return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("lost ordered durable-root lineage")}
			}
			if calls == 1 {
				return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("pre-meta")}
			}
			return PublishResult{Outcome: PublishSucceeded}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), first.candidate); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), second.candidate); err != nil {
		t.Fatal(err)
	}
	if got := first.tx.Owner(); got != ResourceOwnerCoordinator {
		t.Fatalf("first owner=%v want coordinator", got)
	}
	if got := second.tx.Owner(); got != ResourceOwnerCoordinator {
		t.Fatalf("second owner=%v want coordinator", got)
	}
	if first.activates.Load() != 1 || second.activates.Load() != 1 {
		t.Fatalf("activation callbacks=(%d,%d), want (1,1)", first.activates.Load(), second.activates.Load())
	}
	if err := coordinator.WaitThrough(context.Background(), 3); err == nil || err.Error() != "pre-meta" {
		t.Fatalf("first wait error=%v want pre-meta", err)
	}
	if first.consumes.Load() != 0 || second.consumes.Load() != 0 {
		t.Fatal("retry consumed a durable-root member")
	}
	if err := coordinator.WaitThrough(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || first.consumes.Load() != 1 || second.consumes.Load() != 1 {
		t.Fatalf("calls/consumes=(%d,%d,%d), want (2,1,1)", calls, first.consumes.Load(), second.consumes.Load())
	}
	if first.tx.Owner() != ResourceOwnerReleased || second.tx.Owner() != ResourceOwnerReleased {
		t.Fatalf("owners after success=(%v,%v)", first.tx.Owner(), second.tx.Owner())
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDurableRootActivationFailureRollsBackOwnershipBeforeVisibility(t *testing.T) {
	fixture := newDurableRootTransactionFixture(t, durableLineage(1), 2)
	fixture.activateErr = errors.New("activate failed")
	coordinator, err := New(Options{
		Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), fixture.candidate); err == nil || !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("enqueue activation error=%v", err)
	}
	stats := coordinator.Stats()
	if stats.PendingCommits != 0 || stats.VisibleCommitSeq != 0 || stats.RejectedCandidates != 1 {
		t.Fatalf("stats after failed activation=%+v", stats)
	}
	if fixture.activates.Load() != 1 || fixture.tx.Owner() != ResourceOwnerCandidate || fixture.resources.Owner() != ResourceOwnerCandidate {
		t.Fatalf("activation/root/resource owner=(%d,%v,%v), want (1,candidate,candidate)", fixture.activates.Load(), fixture.tx.Owner(), fixture.resources.Owner())
	}
	if err := fixture.candidate.Abandon(); err != nil {
		t.Fatal(err)
	}
	if fixture.aborts.Load() != 1 {
		t.Fatalf("abort callbacks=%d want 1", fixture.aborts.Load())
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorRejectsUnrelatedOrGappedDurableRootLineageBeforeTransfer(t *testing.T) {
	lineage := durableLineage(1)
	first := newDurableRootTransactionFixture(t, lineage, 2)
	unrelated := newDurableRootTransactionFixture(t, durableLineage(2), 3)
	gapped := newDurableRootTransactionFixture(t, lineage, 4)
	coordinator, err := New(Options{
		Clock: NewFakeClock(time.Unix(1, 0)),
		Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("unexpected publish")}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), first.candidate); err != nil {
		t.Fatal(err)
	}
	rootless, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(3, 2, 3, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), rootless); !errors.Is(err, ErrDurableRootLineage) {
		t.Fatalf("rootless enqueue=%v want ErrDurableRootLineage", err)
	}
	for name, fixture := range map[string]*durableRootTransactionFixture{"unrelated": unrelated, "gapped": gapped} {
		if err := coordinator.Enqueue(context.Background(), fixture.candidate); !errors.Is(err, ErrDurableRootLineage) {
			t.Fatalf("%s enqueue=%v want ErrDurableRootLineage", name, err)
		}
		if got := fixture.tx.Owner(); got != ResourceOwnerCandidate {
			t.Fatalf("%s rejected transaction owner=%v want candidate", name, got)
		}
		if err := fixture.candidate.Abandon(); err != nil {
			t.Fatal(err)
		}
	}
	stats := coordinator.Stats()
	if stats.VisibleCommitSeq != 2 || stats.PendingCommits != 1 || stats.RejectedCandidates != 3 {
		t.Fatalf("stats after rejected durable roots=%+v", stats)
	}
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("stop=%v want ErrPublicationStopped", err)
	}
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if handoff.DurableRootLen() != 1 || handoff.DurableRoots()[0] != first.tx {
		t.Fatalf("durable-root handoff=%v", handoff.DurableRoots())
	}
	if first.failures.Load() != 1 || first.tx.Owner() != ResourceOwnerRecovery {
		t.Fatalf("stop failure/owner=(%d,%v), want (1,recovery)", first.failures.Load(), first.tx.Owner())
	}
	handoff.Release()
}

func TestDurableRootAmbiguityHandsOffEveryUnconsumedMember(t *testing.T) {
	lineage := durableLineage(1)
	first := newDurableRootTransactionFixture(t, lineage, 2)
	second := newDurableRootTransactionFixture(t, lineage, 3)
	coordinator, err := New(Options{
		Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("meta sync unknown")}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), first.candidate); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), second.candidate); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitThrough(context.Background(), 3); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("wait=%v want recovery required", err)
	}
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	roots := handoff.DurableRoots()
	if len(roots) != 2 || roots[0] != first.tx || roots[1] != second.tx {
		t.Fatalf("handoff roots=%v", roots)
	}
	if first.failures.Load() != 1 || second.failures.Load() != 1 {
		t.Fatalf("failure callbacks=(%d,%d), want (1,1)", first.failures.Load(), second.failures.Load())
	}
	handoff.Release()
	_ = coordinator.Stop(context.Background())
}
