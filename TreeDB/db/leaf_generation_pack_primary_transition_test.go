package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const primaryPackTransitionDescriptor = "collections/root/primary-normalization/users/primary"

// Bounded SYSTEM-only setup: one1024-row image, then a512-row delta and one-row
// delta. The named collection itself uses pager leaves, so copying SYSTEM leaves
// must not change either that collection root or the user's PRIMARY DATA base.
func primaryPackTransitionSystemRows(t *testing.T, root uint64, start, end int, fill byte) memtable.Table {
	t.Helper()
	kvs := make([]any, 0, 2*(end-start)+2)
	if root != 0 {
		kvs = append(kvs, primaryPackTransitionDescriptor, encodeMaintenanceRootID(root))
	}
	for i := start; i < end; i++ {
		kvs = append(kvs, fmt.Sprintf("normalization-%04d", i), bytes.Repeat([]byte{fill}, 32))
	}
	return mustFrozenRawMemtable(t, kvs...)
}

func primaryPackTransitionDataBase(t *testing.T, snapshot *Snapshot) (node.PrimaryOperand, uint64, int) {
	t.Helper()
	if snapshot == nil || snapshot.state == nil || snapshot.idx == nil || snapshot.idx.primary == nil || !primaryarena.IsPage(snapshot.state.RootPageID) {
		t.Fatal("INVALID_PRIMARY_CONTROL actual captured root is not PRIMARY")
	}
	image, err := snapshot.idx.pager.Get(snapshot.state.RootPageID)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := node.DecodePrimaryDirectory(image)
	if err != nil {
		t.Fatal(err)
	}
	base, sequence := directory.Base()
	if base.Ref.Kind != page.ChildRefPage || base.Ref.Page == 0 || primaryarena.IsPage(base.Ref.Page) || base.Digest == ([32]byte{}) {
		t.Fatalf("INVALID_PRIMARY_CONTROL no actual DATA base: %+v", base)
	}
	return base, sequence, directory.Count()
}

// This supported opt-in PRIMARY control is separate from the fresh-DATA public
// graph witness. It requires actual finalizer normalization before asking for a
// transition; the current producer guard is intentionally NOT relaxed here.
// Neither this source preparation nor the expected guard failure is observed RED.
func TestLeafGenerationPackPrimaryNormalizationKeepsAcceptedTransition(t *testing.T) {
	for _, outcome := range []string{"success", "accepted_error"} {
		t.Run(outcome, func(t *testing.T) {
			requireLeafGenerationPackPromotionSupport(t)
			dir := t.TempDir()
			d, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast,
				IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed,
				DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
				LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true})
			if err != nil {
				t.Fatal(err)
			}
			leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
			leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
			d.SetLeafPageLog(leafLog)
			t.Cleanup(func() { closeNoErr(t, leafLog) })
			t.Cleanup(func() {
				// Consume the original once-only producer result, not another Close:
				// an expected publication error is not proof of physical disposal.
				closeOutcome, closeCleanup, closeCompleted := d.closeWithCleanupOutcome()
				t.Logf("PRIMARY_CONTROL_CHECKED_CLOSE outcome=%s original_error=%v cleanup_error=%v completed=%t", outcome, closeOutcome, closeCleanup, closeCompleted)
				if !closeCompleted || closeCleanup != nil {
					t.Errorf("INVALID_PRIMARY_CONTROL incomplete teardown or cleanup debt: completed=%t cleanup=%v", closeCompleted, closeCleanup)
				}
				if closeOutcome != nil && !(outcome == "accepted_error" && errors.Is(closeOutcome, errTestWriteMetaFailpoint)) {
					t.Errorf("original PRIMARY control Close outcome: %v", closeOutcome)
				}
			})
			if d.idx.Load().primary == nil {
				t.Fatal("INVALID_PRIMARY_CONTROL explicit PRIMARY option not active")
			}
			if err := d.SetSync([]byte("unchanged-user"), []byte("original-user-value")); err != nil {
				t.Fatal(err)
			}
			_, roots, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
				Iter: mustFrozenRawMemtable(t, "doc", []byte("unchanged-collection-value")).NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves,
			}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
				return primaryPackTransitionSystemRows(t, roots[0], 0, 1024, 'a').NewIterator(nil, nil), nil
			})
			if err != nil || len(roots) != 1 {
				t.Fatalf("initial SYSTEM publication: roots=%v error=%v", roots, err)
			}
			collectionRoot := roots[0]
			_, sourceID := currentLeafSegmentOrFatal(t, leafLog)
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			// Ordinary public SYSTEM deltas preserve omitted entries and create
			// genuine dead/live bytes; the second fixed delta advances commit age.
			for _, delta := range []struct {
				start, end int
				fill       byte
			}{{0, 512, 'b'}, {0, 1, 'c'}} {
				_, _, err := d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
					return primaryPackTransitionSystemRows(t, 0, delta.start, delta.end, delta.fill).NewIterator(nil, nil), nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			owner := transactionowner.New()
			if err := d.BindPrivateCOWOwner(owner); err != nil {
				t.Fatal(err)
			}
			plan, err := d.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{MinPublishedAgeCommits: 1, MinCandidateGenerations: 1})
			if err != nil {
				t.Fatal(err)
			}
			candidate := findLeafGenerationByFileID(t, loadLeafGenerationManifestOrFatal(t, dir), page.ValueLogSegmentID(sourceID))
			eligible := false
			for _, gen := range plan.Candidates {
				eligible = eligible || gen.GenerationID == candidate.GenerationID && gen.Eligible && gen.BytesLive > 0 && gen.BytesDead > 0 && gen.AgeCommits >= 1
			}
			if !eligible {
				t.Fatalf("INVALID_PRIMARY_CONTROL no natural partly-live SYSTEM candidate: generation%d plan%+v", candidate.GenerationID, plan)
			}
			before := d.AcquireSnapshot()
			if before == nil {
				t.Fatal("missing original PRIMARY snapshot")
			}
			defer func() { closeNoErr(t, before) }()
			beforeBase, beforeSequence, beforeCount := primaryPackTransitionDataBase(t, before)
			var after *Snapshot
			defer func() {
				if after != nil {
					closeNoErr(t, after)
				}
			}()
			calls, accepted := 0, false
			unregister := d.RegisterCollectionRootRelocation(func(_ *Snapshot, _ *pager.Pager, relocated map[uint64]uint64) (func(bool), error) {
				if relocated[collectionRoot] != collectionRoot {
					return nil, errors.New("SYSTEM-only control changed pager collection root")
				}
				return func(actual bool) {
					calls++
					accepted = actual
					if actual {
						after = d.AcquireSnapshot() // exact accepted view before error poisoning
					}
				}, nil
			})
			defer unregister()
			restore := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
				if outcome == "accepted_error" && event.Phase == leafGenerationPackBeforeMetaWrite {
					d.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
					d.testFailWriteMeta.Store(true)
				}
				return nil
			})
			defer restore()
			defer d.testFailWriteMeta.Store(false)
			stats, packErr := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), LeafGenerationPackFromPlanOptions{Sync: true, MaxGenerations: 1, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1})
			d.testFailWriteMeta.Store(false)
			t.Logf("PRIMARY_NORMALIZATION_ORIGINAL_OUTCOME outcome=%s calls=%d accepted=%t ran=%t transition_zero=%t error=%v", outcome, calls, accepted, stats.Ran, stats.Transition.IsZero(), packErr)
			if outcome == "accepted_error" {
				if !errors.Is(packErr, errTestWriteMetaFailpoint) || !CommitPublicationAccepted(packErr) || stats.Ran {
					t.Fatalf("original accepted error not preserved: stats%+v error%v", stats, packErr)
				}
			} else if packErr != nil || !stats.Ran || stats.Pack.BytesCopied == 0 {
				t.Fatalf("original pack completion: stats%+v error%v", stats, packErr)
			}
			if calls != 1 || !accepted || after == nil || len(stats.Selection.GenerationIDs) != 1 || stats.Selection.GenerationIDs[0] != candidate.GenerationID {
				t.Fatalf("INVALID_PRIMARY_CONTROL actual selected/accepted endpoints: calls%d accepted%t after%p selection%v", calls, accepted, after, stats.Selection.GenerationIDs)
			}
			afterBase, afterSequence, afterCount := primaryPackTransitionDataBase(t, after)
			beforeToken, beforeOK := before.StateToken()
			afterToken, afterOK := after.StateToken()
			t.Logf("PRIMARY_NORMALIZATION_ENDPOINTS outcome=%s before=%+v after=%+v before_data_base=%+v after_data_base=%+v before_base_sequence=%d after_base_sequence=%d before_components=%d after_components=%d transition_zero=%t original_error=%v", outcome, beforeToken, afterToken, beforeBase, afterBase, beforeSequence, afterSequence, beforeCount, afterCount, stats.Transition.IsZero(), packErr)
			if !beforeOK || !afterOK || beforeToken.RootPageID == afterToken.RootPageID || beforeToken.SystemRootPageID == afterToken.SystemRootPageID || beforeBase != afterBase || beforeSequence != afterSequence || beforeCount != afterCount {
				t.Fatal("INVALID_PRIMARY_CONTROL did not observe SYSTEM-only PRIMARY replacement with unchanged DATA base/components")
			}
			for _, snap := range []*Snapshot{before, after} {
				if value, err := snap.Get([]byte("unchanged-user")); err != nil || !bytes.Equal(value, []byte("original-user-value")) {
					t.Fatalf("held/current user value=%q error%v", value, err)
				}
				if value, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte("normalization-0000")); err != nil || !bytes.Equal(value, bytes.Repeat([]byte{'c'}, 32)) {
					t.Fatalf("SYSTEM value=%q error%v", value, err)
				}
			}
			if stats.Transition.IsZero() || stats.Transition.before != beforeToken || stats.Transition.after != afterToken {
				t.Fatalf("PRIMARY normalization lost exact accepted transition: before%+v after%+v transition%+v original_error%v", beforeToken, afterToken, stats.Transition, packErr)
			}
			if err := stats.Transition.Validate(before, after); err != nil {
				t.Fatalf("actual normalized PRIMARY endpoint refused: %v original_error%v", err, packErr)
			}
		})
	}
}
