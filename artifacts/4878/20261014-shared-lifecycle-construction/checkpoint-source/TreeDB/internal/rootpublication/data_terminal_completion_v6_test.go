package rootpublication

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"testing"
)

// Reuses the original DATA allocator/publisher fixture through the selected
// constructor's public metadata input; it does not install private phase state.
func newSelectedDataTerminalFixture(t *testing.T, owner *retainedalloc.Owner, abortErr error, lineage DurableRootLineageID, seq uint64) *durableRootTransactionFixture {
	t.Helper()
	withPayload := false
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
		TerminalMetadataOwnerV6: owner,
		Lineage:                 lineage, Sequence: seq, Payload: fixture.payload, PreparedCOW: fixture.prepared,
		Activate: func(input DurableRootCallbackInput) error {
			fixture.activates.Add(1)
			if input.PreparedCOW != fixture.prepared {
				return errors.New("activate lost exact COW candidate")
			}
			return fixture.activateErr
		},
		Abort: func(input DurableRootCallbackInput) error {
			fixture.aborts.Add(1)
			if abortErr != nil {
				return abortErr
			}
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

func TestDurableRootTransactionSelectedDataTerminalAccounting(t *testing.T) {
	for _, route := range []string{"abort", "consume", "recovery"} {
		t.Run(route, func(t *testing.T) {
			var owner retainedalloc.Owner
			owner.Initialize(0)
			f := newSelectedDataTerminalFixture(t, &owner, nil, durableLineage(1), 2)
			if owner.Bytes() == 0 {
				t.Fatal("selected containing allocations were not admitted")
			}
			if route == "abort" {
				if err := f.candidate.Abandon(); err != nil {
					t.Fatal(err)
				}
			} else {
				outcome := PublishSucceeded
				if route == "recovery" {
					outcome = PublishAmbiguous
				}
				c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
					if owner.Bytes() == 0 {
						t.Error("publisher lost original selected containing custody")
					}
					if outcome == PublishAmbiguous {
						return PublishResult{Outcome: outcome, Err: errors.New("selected seal ambiguous")}
					}
					return PublishResult{Outcome: outcome}
				})})
				if err != nil {
					t.Fatal(err)
				}
				if err = c.Enqueue(context.Background(), f.candidate); err != nil {
					t.Fatal(err)
				}
				err = c.WaitThrough(context.Background(), 2)
				if route == "consume" {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					if !errors.Is(err, ErrRecoveryRequired) {
						t.Fatal(err)
					}
					if owner.Bytes() == 0 {
						t.Fatal("ambiguous selected DATA lost original debt")
					}
					handoff, err := c.TakeRecoveryHandoff()
					if err != nil {
						t.Fatal(err)
					}
					if err = handoff.ReleaseWithError(); err != nil {
						t.Fatal(err)
					}
				}
				_ = c.Stop(context.Background())
			}
			if got := owner.Bytes(); got != 0 {
				t.Fatalf("selected %s terminal charge=%d", route, got)
			}
		})
	}
}

func TestDurableRootTransactionSelectedDataBirthRefusalBeforeTransfer(t *testing.T) {
	f := newDurableRootTransactionFixtureWithPayload(t, durableLineage(1), 2, false)
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.cap = budget.bytes
	txn, err := NewDurableRootTransaction(DurableRootTransactionSpec{
		TerminalMetadataOwnerV6: &owner, Lineage: f.tx.Lineage(), Sequence: f.tx.Sequence(), PreparedCOW: f.prepared,
		Activate: f.tx.activate, Consume: f.tx.consume, Abort: f.tx.abort, Fail: f.tx.fail,
	})
	if txn != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("birth refusal=(%p,%v)", txn, err)
	}
	if owner.Bytes() != before || budget.bytes != before || f.tx.Owner() != ResourceOwnerCandidate || f.activates.Load() != 0 {
		t.Fatal("refused birth changed original custody/admission")
	}
	if err = f.candidate.Abandon(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableRootTransactionSelectedDataFailedAbortRetainsExactDebt(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	cause := errors.New("original physical abort refused")
	f := newSelectedDataTerminalFixture(t, &owner, cause, durableLineage(1), 2)
	before := owner.Bytes()
	if err := f.candidate.Abandon(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if err := f.tx.Abort(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if owner.Bytes() != before || f.aborts.Load() != 1 || f.tx.PreparedCOW() != f.prepared {
		t.Fatal("failed physical abort lost original debt or repeated effect")
	}
}
