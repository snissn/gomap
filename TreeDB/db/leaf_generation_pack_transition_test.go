package db

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestLeafGenerationPackTransitionFollowsPrivateAcceptance(t *testing.T) {
	for _, outcome := range []string{"success", "before_acceptance", "accepted_error"} {
		t.Run(outcome, func(t *testing.T) {
			d, leafLog, _ := openLeafGenerationPackTestDB(t)
			prepareLeafGenerationPackTestCandidate(t, d, leafLog, 1024)
			// A real collection-root descriptor makes the existing relocation
			// observer capture the accepted snapshot after the publication fence
			// is released, including before an accepted error poisons admission.
			_, _, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
				Iter: mustFrozenRawMemtable(t, "doc", []byte("unchanged")).NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves,
			}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenRawMemtable(t, "collections/root/transition/users/primary", encodeMaintenanceRootID(roots[0])).NewIterator(nil, nil), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			owner := transactionowner.New()
			if err := d.BindPrivateCOWOwner(owner); err != nil {
				t.Fatal(err)
			}
			// These are backend producer/guard controls, not public native
			// activation evidence. Leave the success case inactive so a later
			// ordinary backend write can test the exact-endpoint rejection.
			if outcome != "success" {
				d.nativeSemanticActive.Store(true)
			}
			if _, err := d.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true}); err != nil {
				t.Fatal(err)
			}
			before := d.AcquireSnapshot()
			if before == nil {
				t.Fatal("missing original snapshot")
			}
			t.Cleanup(func() { closeNoErr(t, before) })
			var after *Snapshot
			calls := 0
			unregister := d.RegisterCollectionRootRelocation(func(_ *Snapshot, _ *pager.Pager, _ map[uint64]uint64) (func(bool), error) {
				return func(accepted bool) {
					calls++
					if accepted {
						after = d.AcquireSnapshot()
					}
				}, nil
			})
			defer unregister()
			t.Cleanup(func() {
				if after != nil {
					closeNoErr(t, after)
				}
			})
			injected := errors.New("transition before acceptance")
			restore := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
				if event.Phase == leafGenerationPackBeforeMetaWrite {
					switch outcome {
					case "before_acceptance":
						return injected
					case "accepted_error":
						d.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
						d.testFailWriteMeta.Store(true)
					}
				}
				return nil
			})
			defer restore()
			defer d.testFailWriteMeta.Store(false)
			opts := LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Force: true, Sync: true}
			// Neither a rejected owner nor a declined plan may mint authority.
			rejected, rejectedErr := d.LeafGenerationPackRunOnceForPrivateCOW(transactionowner.New(), context.Background(), opts)
			if !errors.Is(rejectedErr, ErrNativeSemanticUnsupported) || !rejected.Transition.IsZero() {
				t.Fatalf("rejected owner: transition=%+v error=%v", rejected.Transition, rejectedErr)
			}
			declined := opts
			declined.Force, declined.MinCandidateGenerations = false, 1000
			if stats, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), declined); err != nil || stats.Ran || !stats.Transition.IsZero() {
				t.Fatalf("declined plan: stats=%+v error=%v", stats, err)
			}
			stats, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
			d.testFailWriteMeta.Store(false)
			if calls != 1 {
				t.Fatalf("relocation calls=%d want 1", calls)
			}
			switch outcome {
			case "before_acceptance":
				if !errors.Is(err, injected) || CommitPublicationAccepted(err) || stats.Ran || !stats.Transition.IsZero() || after != nil {
					t.Fatalf("preacceptance: stats=%+v error=%v after=%p", stats, err, after)
				}
				return
			case "accepted_error":
				if !errors.Is(err, errTestWriteMetaFailpoint) || !CommitPublicationAccepted(err) || stats.Ran {
					t.Fatalf("accepted error: stats=%+v error=%v", stats, err)
				}
			case "success":
				if err != nil || !stats.Ran || stats.Pack.BytesCopied == 0 {
					t.Fatalf("successful pack: stats=%+v error=%v", stats, err)
				}
			}
			if after == nil || stats.Transition.IsZero() {
				t.Fatalf("accepted pack lost endpoint: transition=%+v after=%p", stats.Transition, after)
			}
			beforeToken, _ := before.StateToken()
			afterToken, _ := after.StateToken()
			t.Logf("outcome=%s before=%+v after=%+v producer_before=%+v producer_after=%+v", outcome, beforeToken, afterToken, stats.Transition.before, stats.Transition.after)
			if err := stats.Transition.Validate(before, after); err != nil {
				t.Fatalf("actual accepted endpoints: %v", err)
			}
			for _, snap := range []*Snapshot{before, after} {
				got, err := snap.Get(leafGenerationKey("pack-concurrent", 768))
				if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'a'}, 32)) {
					t.Fatalf("retained logical value=%q error=%v", got, err)
				}
			}
			if outcome == "success" {
				checkLeafGenerationPackTransitionInvalidBases(t, d, stats.Transition, before, after)
			}
		})
	}
}

func checkLeafGenerationPackTransitionInvalidBases(t *testing.T, d *DB, transition LeafGenerationPackTransition, before, after *Snapshot) {
	t.Helper()
	foreignDB, _, _ := openLeafGenerationPackTestDB(t)
	foreign := foreignDB.AcquireSnapshot()
	if foreign == nil {
		t.Fatal("missing foreign snapshot")
	}
	t.Cleanup(func() { closeNoErr(t, foreign) })
	for _, tc := range []struct {
		name          string
		transition    LeafGenerationPackTransition
		before, after *Snapshot
	}{
		{"zero", LeafGenerationPackTransition{}, before, after},
		{"reversed", transition, after, before},
		{"unchanged", transition, before, before},
		{"foreign_before", transition, foreign, after},
		{"foreign_after", transition, before, foreign},
		{"nil_before", transition, nil, after},
		{"nil_after", transition, before, nil},
	} {
		if err := tc.transition.Validate(tc.before, tc.after); !errors.Is(err, ErrRootPublicationBasisMismatch) {
			t.Fatalf("%s accepted invalid endpoints: %v", tc.name, err)
		}
	}
	wrongOwner := transition
	wrongOwner.owner = transactionowner.New()
	if err := wrongOwner.Validate(before, after); !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("foreign owner accepted: %v", err)
	}
	var validationErr error
	if allocations := testing.AllocsPerRun(20, func() { validationErr = transition.Validate(before, after) }); allocations != 0 || validationErr != nil {
		t.Fatalf("validation allocations=%v error=%v", allocations, validationErr)
	}
	// A real later publication is not covered, while the original two retained
	// views remain authorized even though the backend has advanced.
	writeLeafGenerationKeys(t, d, "transition-later", 1, 'z')
	later := d.AcquireSnapshot()
	if later == nil {
		t.Fatal("missing later snapshot")
	}
	t.Cleanup(func() { closeNoErr(t, later) })
	if err := transition.Validate(before, later); !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("intervening publication accepted: %v", err)
	}
	if err := transition.Validate(before, after); err != nil {
		t.Fatalf("later publication invalidated original views: %v", err)
	}
	closed := d.AcquireSnapshot()
	if closed == nil {
		t.Fatal("missing snapshot for closed control")
	}
	closeNoErr(t, closed)
	if err := transition.Validate(before, closed); !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("closed snapshot accepted: %v", err)
	}
}

func TestLeafGenerationPackTransitionUnownedPublicationHasNoAuthority(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)
	prepareLeafGenerationPackTestCandidate(t, d, leafLog, 1024)
	stats, err := d.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Force: true, Sync: true})
	if err != nil || !stats.Ran || stats.Pack.BytesCopied == 0 || !stats.Transition.IsZero() {
		t.Fatalf("unowned publication: stats=%+v error=%v", stats, err)
	}
}
