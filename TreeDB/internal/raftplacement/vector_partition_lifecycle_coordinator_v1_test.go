package raftplacement

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type lifecycleCoordinatorCommitterV1 struct {
	authority *CatalogMetaAuthorityV1
	index     uint64
	fail      error
}

type lifecycleTestBuilderV1 struct {
	ready VectorPartitionLifecycleGroupReadyV1
	err   error
	call  *bool
}

func (b lifecycleTestBuilderV1) BuildAndStageVectorPartitionGroupV1(context.Context, VectorPartitionLifecycleIdentityV1, raftcluster.GroupID) (VectorPartitionLifecycleGroupReadyV1, error) {
	if b.call != nil {
		*b.call = true
	}
	return b.ready, b.err
}

func TestVectorPartitionLifecycleWorkflowFailsClosedWhenUnconfiguredV1(t *testing.T) {
	identity := VectorPartitionLifecycleIdentityV1{}
	ready := VectorPartitionLifecycleGroupReadyV1{}
	refs := VectorPartitionLifecycleReferencesV1{}
	for _, coordinator := range []VectorPartitionLifecycleCoordinatorV1{
		{},
		{Authority: NewCatalogMetaAuthorityV1()},
	} {
		builderCalled := false
		checks := []func() error{
			func() error { _, err := coordinator.BeginBuildV1(t.Context(), identity, nil, 0, 0); return err },
			func() error { _, err := coordinator.RecordGroupReadyV1(t.Context(), identity, ready); return err },
			func() error {
				_, err := coordinator.BuildAndRecordGroupReadyV1(t.Context(), lifecycleTestBuilderV1{call: &builderCalled}, identity, "group-a")
				return err
			},
			func() error { _, err := coordinator.PrepareV1(t.Context(), identity); return err },
			func() error { _, err := coordinator.ActivateV1(t.Context(), identity); return err },
			func() error { _, err := coordinator.AbortV1(t.Context(), identity, "abort"); return err },
			func() error { _, err := coordinator.RetireV1(t.Context(), identity); return err },
			func() error { _, err := coordinator.MarkCleanableV1(t.Context(), identity, refs); return err },
			func() error { _, err := coordinator.RecordGroupCleanupV1(t.Context(), identity, "group-a"); return err },
			func() error { _, err := coordinator.CompleteCleanupV1(t.Context(), identity); return err },
		}
		for i, check := range checks {
			if err := check(); !errors.Is(err, ErrCatalogMetaUnavailable) {
				t.Fatalf("coordinator=%+v check=%d err=%v want catalog-meta unavailable", coordinator, i, err)
			}
		}
		if builderCalled {
			t.Fatalf("coordinator=%+v invoked builder before configuration validation", coordinator)
		}
	}
}

func (c *lifecycleCoordinatorCommitterV1) SubmitCatalogMetaCommandV1(_ context.Context, raw []byte) (uint64, uint64, error) {
	if c.fail != nil {
		return 0, 0, c.fail
	}
	c.index++
	if _, err := c.authority.applyCommittedCatalogMetaV1(raw, c.index); err != nil {
		return 0, 0, err
	}
	return 1, c.index, nil
}

func TestVectorPartitionLifecycleCoordinatorInvalidatesBeforeRelevantMutationV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	active := catalogMetaLifecycleBuildPreparedV1(t, authority, &committer.index, identity, 0, 9)
	active = catalogMetaLifecycleApplyV1(t, authority, &committer.index, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 9
	}))

	proof, err := coordinator.InvalidateBeforeRelevantMutationV1(t.Context(), identity.Index, "vector field update")
	if err != nil {
		t.Fatalf("InvalidateBeforeRelevantMutationV1: %v", err)
	}
	if proof.ActiveGeneration != identity.Generation || proof.InvalidationEpoch != active.MutationEpoch+1 {
		t.Fatalf("proof=%+v", proof)
	}
	record, ok := authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok || record.State != VectorPartitionLifecycleInvalidatedV1 {
		t.Fatalf("record=%+v available=%v", record, ok)
	}
	if err := record.CanCommitRelevantMutation(proof); err != nil {
		t.Fatalf("proof did not admit mutation after durable invalidation: %v", err)
	}

	// A concurrent retry cannot reuse a pending fence: only the data commit
	// path may confirm it, so a second mutation is refused rather than sharing
	// the first mutation's invalidation proof.
	before := committer.index
	retry, err := coordinator.InvalidateBeforeRelevantMutationV1(t.Context(), identity.Index, "vector field update")
	if !errors.Is(err, ErrVectorPartitionLifecycleGuard) || committer.index != before {
		t.Fatalf("retry proof=%+v err=%v index=%d", retry, err, committer.index)
	}
}

func TestVectorPartitionCollectionMutationBarrierClosesFirstGenerationSourceRaceV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	collection := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11).Index.Collection
	before, err := coordinator.BuildSourceMutationEpochV1(collection)
	if err != nil || before != 1 {
		t.Fatalf("initial build epoch=%d err=%v", before, err)
	}
	opA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	barrier, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil || !barrier.Pending || barrier.Epoch != before+1 {
		t.Fatalf("barrier=%+v err=%v", barrier, err)
	}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	if _, err := coordinator.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-a"}, 0, before); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("pending barrier admitted stale first-generation build: %v", err)
	}
	if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), collection, opA); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-a"}, 0, before); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("completed barrier admitted stale captured source: %v", err)
	}
	if _, err := coordinator.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-a"}, 0, barrier.Epoch); err != nil {
		t.Fatalf("fresh source build: %v", err)
	}
	opB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opB); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("mutation raced source capture: %v", err)
	}
}

func TestVectorPartitionCollectionMutationBarrierRetryConflictAndRestoreV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	collection := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11).Index.Collection
	opA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	opB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	first, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil {
		t.Fatal(err)
	}
	beforeRetry := committer.index
	retry, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil || retry != first || committer.index != beforeRetry {
		t.Fatalf("exact retry=%+v err=%v applied=%d", retry, err, committer.index)
	}
	if _, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opB); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("distinct concurrent operation err=%v", err)
	}
	snapshot, err := authority.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if _, err := restored.installCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatal(err)
	}
	restoredCommitter := &lifecycleCoordinatorCommitterV1{authority: restored, index: snapshot.AppliedIndex}
	restoredCoordinator := VectorPartitionLifecycleCoordinatorV1{Authority: restored, Committer: restoredCommitter}
	if _, err := restoredCoordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opB); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("restored pending barrier admitted distinct operation: %v", err)
	}
	if err := restoredCoordinator.ConfirmRelevantCollectionMutationV1(t.Context(), collection, opA); err != nil {
		t.Fatal(err)
	}
	completedReplay, err := restoredCoordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil || completedReplay.Pending || completedReplay.Epoch != first.Epoch {
		t.Fatalf("completed replay=%+v err=%v", completedReplay, err)
	}
}

func TestVectorPartitionCollectionMutationBarrierRetainsOlderCompletedRetryV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	collection := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11).Index.Collection
	opA := fmt.Sprintf("%064x", 1)
	opB := fmt.Sprintf("%064x", 2)

	first, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), collection, opA); err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opB)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), collection, opB); err != nil {
		t.Fatal(err)
	}
	if second.Epoch != first.Epoch+1 {
		t.Fatalf("mutation epochs first=%d second=%d", first.Epoch, second.Epoch)
	}

	beforeReplay := committer.index
	replay, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil || replay.Pending || replay.Epoch != first.Epoch || replay.OperationDigest != opA {
		t.Fatalf("older completed replay=%+v err=%v", replay, err)
	}
	if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), collection, opA); err != nil {
		t.Fatalf("older completed confirm: %v", err)
	}
	if committer.index != beforeReplay {
		t.Fatalf("older completed replay committed new catalog entries: before=%d after=%d", beforeReplay, committer.index)
	}

	snapshot, err := authority.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if _, err := restored.installCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatal(err)
	}
	restoredCommitter := &lifecycleCoordinatorCommitterV1{authority: restored, index: snapshot.AppliedIndex}
	restoredCoordinator := VectorPartitionLifecycleCoordinatorV1{Authority: restored, Committer: restoredCommitter}
	restoredReplay, err := restoredCoordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, opA)
	if err != nil || restoredReplay.Pending || restoredReplay.Epoch != first.Epoch || restoredCommitter.index != snapshot.AppliedIndex {
		t.Fatalf("restored older replay=%+v err=%v applied=%d", restoredReplay, err, restoredCommitter.index)
	}
}

func TestVectorPartitionCollectionMutationCompletedReplayWindowIsBoundedV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	collection := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11).Index.Collection
	operations := make([]string, maxVectorPartitionCollectionCompletedMutationsV1+2)
	for i := range operations {
		operations[i] = fmt.Sprintf("%064x", i+1)
		if _, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, operations[i]); err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), collection, operations[i]); err != nil {
			t.Fatalf("confirm %d: %v", i, err)
		}
	}

	barrier := authority.collectionMutationBarriers[collection]
	if len(barrier.Completed) != maxVectorPartitionCollectionCompletedMutationsV1 {
		t.Fatalf("completed history=%d want %d", len(barrier.Completed), maxVectorPartitionCollectionCompletedMutationsV1)
	}
	if _, found, err := authority.VectorPartitionCollectionMutationOperationV1(collection, operations[0]); err != nil || found {
		t.Fatalf("evicted operation found=%v err=%v", found, err)
	}
	retained, found, err := authority.VectorPartitionCollectionMutationOperationV1(collection, operations[2])
	if err != nil || !found || retained.Pending || retained.OperationDigest != operations[2] {
		t.Fatalf("oldest retained operation=%+v found=%v err=%v", retained, found, err)
	}
	if _, err := authority.ExportCatalogMetaSnapshotV1(); err != nil {
		t.Fatalf("bounded history snapshot: %v", err)
	}
}

func TestVectorPartitionCollectionMutationBarrierOwnsIndexInvalidationV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	active := catalogMetaLifecycleBuildPreparedV1(t, authority, &committer.index, identity, 0, 9)
	_ = catalogMetaLifecycleApplyV1(t, authority, &committer.index, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	op := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	barrier, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), identity.Index.Collection, op)
	if err != nil || barrier.Epoch != 10 {
		t.Fatalf("barrier=%+v err=%v", barrier, err)
	}
	proof, err := coordinator.InvalidateBeforeRelevantMutationAtEpochV1(t.Context(), identity.Index, "vector update", barrier.Epoch)
	if err != nil || proof.InvalidationEpoch != barrier.Epoch {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), identity.Index.Collection, op); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("collection barrier cleared before index confirmation: %v", err)
	}
	if err := coordinator.ConfirmRelevantMutationV1(t.Context(), proof); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfirmRelevantCollectionMutationV1(t.Context(), identity.Index.Collection, op); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionCollectionMutationBarriersAreBoundedBeforePublicationV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	authority.collectionMutationBarriers = make(map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1, maxVectorPartitionCollectionMutationBarriersV1)
	for i := 0; i < maxVectorPartitionCollectionMutationBarriersV1; i++ {
		collection := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: fmt.Sprintf("collection-%04d", i)}
		authority.collectionMutationBarriers[collection] = vectorPartitionCollectionMutationBarrierStateV1{Epoch: 2, OperationDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	}
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	collection := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11).Index.Collection
	if _, err := coordinator.BeginRelevantCollectionMutationV1(t.Context(), collection, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); !errors.Is(err, ErrVectorPartitionLifecycleLimit) {
		t.Fatalf("new barrier past cap err=%v", err)
	}
}

func TestSelectMutationProofRecordPrefersPendingGenerationDeterministicallyV1(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	older := identity
	older.Generation = 7
	newer := identity
	newer.Generation = 9
	prepared := identity
	prepared.Generation = 10
	records := map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{
		older:    {State: VectorPartitionLifecycleInvalidatedV1, Identity: older, Revision: 4, InvalidationEpoch: 12},
		newer:    {State: VectorPartitionLifecycleInvalidatedV1, Identity: newer, Revision: 8, InvalidationEpoch: 11, MutationConfirmed: true},
		prepared: {State: VectorPartitionLifecyclePreparedV1, Identity: prepared, Revision: 9},
	}
	for i := 0; i < 100; i++ {
		gotIdentity, gotRecord, ok := selectMutationProofRecordLockedV1(records, identity.Index)
		if !ok || gotIdentity != older || gotRecord.InvalidationEpoch != 12 || gotRecord.MutationConfirmed {
			t.Fatalf("selection %d identity=%+v record=%+v found=%v", i, gotIdentity, gotRecord, ok)
		}
	}
}

func TestVectorPartitionLifecycleWorkflowRetriesV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	c := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	r, err := c.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-a"}, 0, 9)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-b"}, 0, 9); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("conflicting begin retry err=%v", err)
	}
	if retry, err := c.BeginBuildV1(t.Context(), identity, []raftcluster.GroupID{"group-a"}, 0, 9); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("begin retry=%+v err=%v", retry, err)
	}
	ready := VectorPartitionLifecycleGroupReadyV1{GroupID: "group-a", AppliedIndex: 2, AssetSetDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	r, err = c.RecordGroupReadyV1(t.Context(), identity, ready)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.RecordGroupReadyV1(t.Context(), identity, ready); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("ready retry=%+v err=%v", retry, err)
	}
	r, err = c.PrepareV1(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.PrepareV1(t.Context(), identity); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("prepare retry=%+v err=%v", retry, err)
	}
	r, err = c.ActivateV1(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.ActivateV1(t.Context(), identity); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("activate retry=%+v err=%v", retry, err)
	}
	proof, err := c.InvalidateBeforeRelevantMutationV1(t.Context(), identity.Index, "test mutation")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ConfirmRelevantMutationV1(t.Context(), proof); err != nil {
		t.Fatal(err)
	}
	r, err = c.RetireV1(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.RetireV1(t.Context(), identity); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("retire retry=%+v err=%v", retry, err)
	}
	r, err = c.MarkCleanableV1(t.Context(), identity, VectorPartitionLifecycleReferencesV1{})
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.MarkCleanableV1(t.Context(), identity, VectorPartitionLifecycleReferencesV1{}); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("cleanable retry=%+v err=%v", retry, err)
	}
	r, err = c.RecordGroupCleanupV1(t.Context(), identity, "group-a")
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.RecordGroupCleanupV1(t.Context(), identity, "group-a"); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("cleanup retry=%+v err=%v", retry, err)
	}
	r, err = c.CompleteCleanupV1(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.CompleteCleanupV1(t.Context(), identity); err != nil || !reflect.DeepEqual(retry, r) {
		t.Fatalf("complete retry=%+v err=%v", retry, err)
	}
	if statuses, fences, err := c.RecoveryStatusV1(); err != nil || len(statuses) != 1 || len(fences) != 1 || fences[0].Pending {
		t.Fatalf("recovery statuses=%+v fences=%+v err=%v", statuses, fences, err)
	}
}

func TestVectorPartitionLifecycleWorkflowCutoverAbortAndBuilderV1(t *testing.T) {
	a, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: a, index: 1}
	c := VectorPartitionLifecycleCoordinatorV1{Authority: a, Committer: committer}
	oldID := catalogMetaLifecycleTestIdentityV1(catalog, 6, 10)
	old := catalogMetaLifecycleBuildPreparedV1(t, a, &committer.index, oldID, 0, 9)
	old = catalogMetaLifecycleApplyV1(t, a, &committer.index, catalogMetaLifecycleTestCommandV1(old, VectorPartitionLifecycleActivateV1, func(x *VectorPartitionLifecycleCommandV1) { x.MutationEpoch = 9 }))
	newID := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	new := catalogMetaLifecycleBuildPreparedV1(t, a, &committer.index, newID, oldID.Generation, 10)
	got, err := c.ActivateV1(t.Context(), newID)
	if err != nil || got.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("cutover=%+v err=%v", got, err)
	}
	if retry, err := c.ActivateV1(t.Context(), newID); err != nil || !reflect.DeepEqual(retry, got) {
		t.Fatalf("cutover retry=%+v err=%v", retry, err)
	}
	oldAfter, _ := a.VectorPartitionLifecycleRecordV1(oldID)
	if oldAfter.State != VectorPartitionLifecycleRetiredV1 {
		t.Fatalf("old=%q", oldAfter.State)
	}
	// A stale explicit predecessor cannot bypass the reducer guard.
	bad := catalogMetaLifecycleTestCommandV1(new, VectorPartitionLifecycleActivateV1, func(x *VectorPartitionLifecycleCommandV1) {
		x.PreviousActiveGeneration = oldID.Generation
		x.PreviousActiveRevision = old.Revision + 1
		x.MutationEpoch = 10
	})
	if _, err := a.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, bad), committer.index+1); !errors.Is(err, ErrVectorPartitionLifecycleStale) && !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("bad predecessor err=%v", err)
	}

	abortID := catalogMetaLifecycleTestIdentityV1(catalog, 8, 12)
	if _, err := c.BeginBuildV1(t.Context(), abortID, []raftcluster.GroupID{"group-a"}, 0, 11); err != nil {
		t.Fatal(err)
	}
	aborted, err := c.AbortV1(t.Context(), abortID, "cancelled")
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.AbortV1(t.Context(), abortID, "different"); err != nil || !reflect.DeepEqual(retry, aborted) {
		t.Fatalf("abort retry=%+v err=%v", retry, err)
	}

	buildID := catalogMetaLifecycleTestIdentityV1(catalog, 9, 13)
	if _, err := c.BeginBuildV1(t.Context(), buildID, []raftcluster.GroupID{"group-a"}, 0, 12); err != nil {
		t.Fatal(err)
	}
	badBuilder := lifecycleTestBuilderV1{ready: VectorPartitionLifecycleGroupReadyV1{GroupID: "wrong", AppliedIndex: 1, AssetSetDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}
	if _, err := c.BuildAndRecordGroupReadyV1(t.Context(), badBuilder, buildID, "group-a"); err == nil {
		t.Fatal("wrong group builder accepted")
	}
	if r, _ := a.VectorPartitionLifecycleRecordV1(buildID); len(r.ReadyGroups) != 0 {
		t.Fatal("bad builder published readiness")
	}
	good := lifecycleTestBuilderV1{ready: VectorPartitionLifecycleGroupReadyV1{GroupID: "group-a", AppliedIndex: 1, AssetSetDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}
	if _, err := c.BuildAndRecordGroupReadyV1(t.Context(), good, buildID, "group-a"); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkVectorPartitionLifecycleCommandEncodeV1(b *testing.B) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	command := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a"}, MutationEpoch: 9,
	}
	b.ReportAllocs()
	b.SetBytes(512)
	for b.Loop() {
		if _, err := EncodeVectorPartitionLifecycleCommandV1(command); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVectorPartitionLifecycleApplyBeginV1(b *testing.B) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	command := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a"}, MutationEpoch: 9,
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ApplyVectorPartitionLifecycleCommandV1(VectorPartitionLifecycleRecordV1{}, command); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVectorPartitionLifecycleReadySetDigestV1(b *testing.B) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	groups := []raftcluster.GroupID{"group-a"}
	ready := []VectorPartitionLifecycleGroupReadyV1{{GroupID: "group-a", AppliedIndex: 17, AssetSetDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	b.ReportAllocs()
	b.SetBytes(128)
	for b.Loop() {
		if _, err := VectorPartitionLifecycleReadySetDigestV1(identity, groups, ready); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVectorPartitionLifecycleMutationProofSelectV1(b *testing.B) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	pending := identity
	pending.Generation = 7
	prepared := identity
	prepared.Generation = 8
	records := map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{
		pending:  {State: VectorPartitionLifecycleInvalidatedV1, Identity: pending, Revision: 4, InvalidationEpoch: 10},
		prepared: {State: VectorPartitionLifecyclePreparedV1, Identity: prepared, Revision: 3},
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, ok := selectMutationProofRecordLockedV1(records, identity.Index); !ok {
			b.Fatal("no selected lifecycle record")
		}
	}
}

func TestVectorPartitionLifecycleCoordinatorFailsClosedForPreparedOrStaleIdentityV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	committer := &lifecycleCoordinatorCommitterV1{authority: authority, index: 1}
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: committer}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	prepared := catalogMetaLifecycleBuildPreparedV1(t, authority, &committer.index, identity, 0, 9)
	if _, err := coordinator.InvalidateBeforeRelevantMutationV1(t.Context(), identity.Index, "vector insert"); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("prepared admission err=%v", err)
	}

	active := catalogMetaLifecycleApplyV1(t, authority, &committer.index, catalogMetaLifecycleTestCommandV1(prepared, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 9
	}))
	stale := identity.Index
	stale.IndexEpoch++
	if _, err := coordinator.InvalidateBeforeRelevantMutationV1(t.Context(), stale, "index ddl"); !errors.Is(err, ErrVectorPartitionLifecycleIdentity) {
		t.Fatalf("stale identity admission err=%v", err)
	}
	still, _ := authority.VectorPartitionLifecycleRecordV1(identity)
	if still.State != VectorPartitionLifecycleActiveV1 || still.Revision != active.Revision {
		t.Fatalf("stale identity changed active state: %+v", still)
	}
}

func TestVectorPartitionLifecycleCoordinatorSubmitRequiresAppliedAuthorityV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	command := catalogMetaLifecycleTestBeginV1(identity, 0, 9)
	coordinator := VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: &lifecycleCoordinatorCommitterV1{authority: authority, index: 1, fail: errors.New("leader unavailable")}}
	if _, err := coordinator.Submit(t.Context(), command); err == nil {
		t.Fatal("Submit succeeded after rejected meta commit")
	}
}
