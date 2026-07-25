package raftplacement

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type lifecycleCoordinatorCommitterV1 struct {
	authority *CatalogMetaAuthorityV1
	index     uint64
	fail      error
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
