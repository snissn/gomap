//go:build !windows

package caching

import (
	"context"
	"errors"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// Called on the real dictionary fixture under its existing flush fence. No
// writer, semantic state, root, pin or dictionary definition is substituted.
func cowRequireDictionaryLeafOwnerContract(t *testing.T, log *cowDictionaryLeafLog) {
	t.Helper()
	selected, ok := log.LeafPageLogLane(0)
	if !ok || selected != log {
		t.Fatal("supported dictionary lane changed append owner")
	}
	for _, index := range []int{-1, 1, 2} {
		if selected, ok := log.LeafPageLogLane(index); ok || selected != nil {
			t.Fatalf("unsupported dictionary worker %d supplied an appender", index)
		}
	}
	// Ordinary LeafPageLog methods remain this fixture's dictionary methods;
	// prepared caching methods must not bypass those methods via promotion.
	switch any(log).(type) {
	case backenddb.LeafPagePreparedLog, backenddb.LeafPagePreparedStableLog,
		backenddb.LeafPagePreparedBatchLog, backenddb.LeafPagePreparedStableBatchLog,
		backenddb.LeafPagePreparedChildRefBatchLog, backenddb.LeafPagePreparedChildRefStableBatchLog,
		interface{ PreparedLeafPageAppends() bool }, interface{ PreparedLeafPageBatchAppends() bool }:
		t.Fatal("dictionary fixture exposed a prepared append bypass")
	}
	owner := log.cowDictionaryLeafLifecycle
	group, ok := owner.(*cachingLeafPageLogGroup)
	if !ok || group.db != log.db {
		t.Fatal("dictionary lifecycle no longer belongs to the original cache group")
	}
	wantRoots, wantSystem, wantVersion := owner.ProtectedLeafGenerationRootIDPairSnapshot()
	gotRoots, gotSystem, gotVersion := log.ProtectedLeafGenerationRootIDPairSnapshot()
	if !slices.Equal(gotRoots, wantRoots) || !slices.Equal(gotSystem, wantSystem) || gotVersion != wantVersion {
		t.Fatal("dictionary appender changed original versioned protected roots")
	}
	pairRoots, pairSystem := log.ProtectedLeafGenerationRootIDPair()
	if !slices.Equal(log.ProtectedLeafGenerationRootIDs(), wantRoots) ||
		!slices.Equal(log.ProtectedLeafGenerationSystemRootIDs(), wantSystem) ||
		!slices.Equal(pairRoots, wantRoots) || !slices.Equal(pairSystem, wantSystem) {
		t.Fatal("dictionary appender changed original protected-root capabilities")
	}
	_, _, afterVersion := owner.ProtectedLeafGenerationRootIDPairSnapshot()
	if afterVersion != wantVersion {
		t.Fatal("protected-root parity crossed a publication")
	}

	lanes, sources := privateCOWActiveLeafSources(t, log.db)
	if len(sources) == 0 {
		t.Fatal("dictionary fixture has no actual active source writer")
	}
	paths := make([]string, len(lanes))
	rotations := make([]uint64, len(lanes))
	for i, lane := range lanes {
		paths[i], rotations[i] = lane.vlogPath, lane.vlogRotateTotal.Load()
	}
	before := log.handoffs.Load()
	for _, invalid := range []*transactionowner.Owner{nil, transactionowner.New()} {
		if err := log.HandoffPrivateCOWLeafPackSources(context.Background(), invalid, sources); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("dictionary owner handoff failed to refuse wrong owner: %v", err)
		}
	}
	wrong := append([]backenddb.LeafPageLogPackSource(nil), sources...)
	wrong[len(wrong)-1].Identity.ObjectID[0] ^= 1
	if err := log.HandoffPrivateCOWLeafPackSources(context.Background(), log.db.cow.txnOwner, wrong); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("dictionary owner handoff lost exact-identity refusal: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := log.HandoffPrivateCOWLeafPackSources(ctx, log.db.cow.txnOwner, sources); !errors.Is(err, context.Canceled) {
		t.Fatalf("dictionary owner handoff lost cancellation refusal: %v", err)
	}
	if log.handoffs.Load() != before {
		t.Fatal("refused handoff was counted as original-owner success")
	}
	for i, lane := range lanes {
		if lane.vlogPath != paths[i] || lane.vlogRotateTotal.Load() != rotations[i] {
			t.Fatal("dictionary owner refusal rotated an original source writer")
		}
	}
}

func TestCOWDictionaryLeafOwnerContract(t *testing.T) {
	// The unchanged fixture workload uses actual SetLeafPageLog installation,
	// validates the contract/refusals above and requires original handoff before
	// a successful real copy. Its existing dictionary record assertion remains.
	cowNativePackedDictionaryLeafFixtureWithEncoding(t, true)
}
