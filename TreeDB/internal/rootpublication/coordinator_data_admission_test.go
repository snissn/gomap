package rootpublication

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestDATAOriginalAdmissionRefusesBeforeTransferAndRefundsTerminal(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	fixture := newDurableRootTransactionFixtureWithMetadata(t, durableLineage(71), 2, false, &owner)
	if fixture.candidate != &fixture.tx.dataCandidate || fixture.tx.terminalMetadataV6.owner != &owner || fixture.resources.MetadataOwner() != &owner {
		t.Fatal("DATA constructor did not retain same original owner/storage")
	}
	c, err := New(Options{MetadataOwner: &owner, StartupSynchronous: true, InitialDurableFrontier: NewFrontier(1, 2, 3, 0, 0), DurableRootLineage: durableLineage(71), DurableRootSequence: 1, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	// The real input-array scratch fits. The combined pending+recovery capacity
	// does not: no original backing or physical owner may change on refusal.
	budget.cap = before + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof((*StableResourceSet)(nil))))
	err = c.Enqueue(context.Background(), fixture.candidate)
	budget.cap = 1 << 24
	if !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != before || budget.bytes != before || fixture.tx.Owner() != ResourceOwnerCandidate || fixture.resources.Owner() != ResourceOwnerCandidate || fixture.activates.Load() != 0 || len(c.pending) != 0 || cap(c.pending) != 0 || cap(c.recoverySets) != 0 || cap(c.recoveryDurableRoots) != 0 {
		t.Fatalf("pre-transfer admission changed original custody/charge: err=%v owner=%d before=%d transaction=%v resources=%v activation=%d", err, owner.Bytes(), before, fixture.tx.Owner(), fixture.resources.Owner(), fixture.activates.Load())
	}
	if err = c.Enqueue(context.Background(), fixture.candidate); err != nil {
		t.Fatal(err)
	}
	if fixture.tx.Owner() != ResourceOwnerCoordinator || fixture.resources.Owner() != ResourceOwnerCoordinator {
		t.Fatal("successful admission did not transfer same originals")
	}
	if err = c.WaitThrough(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if fixture.consumes.Load() != 1 || fixture.tx.Owner() != ResourceOwnerReleased || fixture.resources.Owner() != ResourceOwnerReleased {
		t.Fatal("original DATA terminal did not finish exactly once")
	}
	if err = c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	handoff, err := c.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	if err = handoff.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	completion, err := c.ReleasePhysicalMetadata()
	if err != nil {
		t.Fatal(err)
	}
	c, handoff, fixture = nil, nil, nil
	completion.Complete()
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 {
		t.Fatalf("final original DATA accounting retained: owner=%d budget=%d", owner.Bytes(), budget.bytes)
	}
}

func TestDATARecoveryAdmissionKeepsExactOriginalCustodyOnRefusal(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	fixture := newDurableRootTransactionFixtureWithMetadata(t, durableLineage(72), 2, false, &owner)
	c, err := New(Options{MetadataOwner: &owner, StartupSynchronous: true, InitialDurableFrontier: NewFrontier(1, 2, 3, 0, 0), DurableRootLineage: durableLineage(72), DurableRootSequence: 1, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		t.Fatal("unexpected publisher")
		return PublishResult{}
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Enqueue(context.Background(), fixture.candidate); err != nil {
		t.Fatal(err)
	}
	if err = c.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatal(err)
	}
	if len(c.recoverySets) != 1 || len(c.recoveryDurableRoots) != 1 || c.recoverySets[0] != fixture.resources || c.recoveryDurableRoots[0] != fixture.tx {
		t.Fatalf("original capture shapes: sets=%d transactions=%d fail calls=%d", len(c.recoverySets), len(c.recoveryDurableRoots), fixture.failures.Load())
	}
	sets, roots := &c.recoverySets[0], &c.recoveryDurableRoots[0]
	before, failures := owner.Bytes(), fixture.failures.Load()
	budget.cap = before
	handoff, first := c.TakeRecoveryHandoff()
	budget.cap = 1 << 24
	if handoff != nil || !errors.Is(first, retainedalloc.ErrCapacity) || c.recoveryHandoffErr != first || owner.Bytes() != before || budget.bytes != before {
		t.Fatalf("handoff admission lost original error/charge: handoff=%p error=%v bytes=%d before=%d", handoff, first, owner.Bytes(), before)
	}
	if &c.recoverySets[0] != sets || &c.recoveryDurableRoots[0] != roots || c.recoverySets[0] != fixture.resources || c.recoveryDurableRoots[0] != fixture.tx || fixture.resources.Owner() != ResourceOwnerCoordinator || fixture.tx.Owner() != ResourceOwnerCoordinator {
		t.Fatal("refusal moved original arrays or physical owners")
	}
	again, err := c.TakeRecoveryHandoff()
	if again != nil || err != first || fixture.failures.Load() != failures || owner.Bytes() != before {
		t.Fatal("error observation retried original capture/transfer/refund")
	}
	if _, err = c.ReleasePhysicalMetadata(); !errors.Is(err, ErrRecoveryHandoffUnavailable) || owner.Bytes() != before {
		t.Fatal("failed physical custody refunded containing coordinator")
	}
	// This deliberately failed original graph remains charged and reachable
	// through c.recoveryHandoffErr and the SAME coordinator arrays until exit.
}

func TestDATACoordinatorContainingAdmissionWaitsForBothOriginalCompletions(t *testing.T) {
	for _, physicalFirst := range []bool{false, true} {
		name := "prefix_first"
		if physicalFirst {
			name = "physical_first"
		}
		t.Run(name, func(t *testing.T) {
			var owner retainedalloc.Owner
			owner.Initialize(0)
			c, err := New(Options{MetadataOwner: &owner, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if err != nil {
				t.Fatal(err)
			}
			// The existing cache caller separately admits these fixed arrays. This
			// control exercises the original embedded coordinator's containing charge.
			if err = c.ConfigureAcceptedPrefix(2, LogicalFrontiers{}); err != nil {
				t.Fatal(err)
			}
			before := owner.Bytes()
			if err = c.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			var first, second StableResourceTokenReleaseCompletionV6
			if physicalFirst {
				first, err = c.ReleasePhysicalMetadata()
				if err != nil {
					t.Fatal(err)
				}
				first.Complete()
				if owner.Bytes() != before {
					t.Fatal("physical completion refunded live original prefix owner")
				}
				second, err = c.CloseAcceptedWithCompletion()
			} else {
				first, err = c.CloseAcceptedWithCompletion()
				if err != nil {
					t.Fatal(err)
				}
				first.Complete()
				if owner.Bytes() != before {
					t.Fatal("accepted close refunded live original physical coordinator")
				}
				second, err = c.ReleasePhysicalMetadata()
			}
			if err != nil {
				t.Fatal(err)
			}
			if owner.Bytes() != before || second.charge != before {
				t.Fatal("second original completion did not return independent containing accounting")
			}
			c = nil
			second.Complete()
			if owner.Bytes() != 0 {
				t.Fatal("completed original coordinator retained containing charge")
			}
		})
	}
}
