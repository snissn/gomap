package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func postmergeSnapshotBytesV1(t *testing.T, a *CatalogMetaAuthorityV1) []byte {
	t.Helper()
	raw, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func postmergeSnapshotRefusesV1(t *testing.T, before, forged []byte) {
	t.Helper()
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forged); err != nil {
		t.Fatalf("forged snapshot is not self-canonical: %v", err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forged); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unreachable snapshot accepted: %v", err)
	}
	if after := postmergeSnapshotBytesV1(t, follower); !bytes.Equal(before, after) {
		t.Fatal("refusal mutated authority")
	}
}

func postmergeRewriteSnapshotV1(t *testing.T, raw []byte, mutate func(*CatalogMetaSnapshotV1, *vectorPartitionLifecycleSnapshotV1)) []byte {
	t.Helper()
	var snapshot CatalogMetaSnapshotV1
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(snapshot.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	mutate(&snapshot, &lifecycle)
	var err error
	snapshot.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	result, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Locally committed preparation cannot disappear through a self-canonical
// terminal record whose revision and final command skip mandatory reducers.
func TestCatalogSnapshotKnownPreparationCleanupReachabilityV1(t *testing.T) {
	for _, state := range []VectorPartitionLifecycleStateV1{VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1, VectorPartitionLifecyclePreparedV1} {
		t.Run(string(state), func(t *testing.T) {
			leader, _, record := activeReplicaReplacementAuthorityV1(t, false)
			applied := leader.applied
			if state != VectorPartitionLifecycleBuildingV1 {
				record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
					c.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: "group-b", AppliedIndex: applied, AssetSetDigest: strings.Repeat("c", 64)}
				}))
			}
			if state == VectorPartitionLifecyclePreparedV1 {
				digest, err := VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
				if err != nil {
					t.Fatal(err)
				}
				record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(c *VectorPartitionLifecycleCommandV1) { c.ReadySetDigest = digest }))
			}
			before := postmergeSnapshotBytesV1(t, leader)
			oldRevision := record.Revision
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleAbortBuildV1, func(c *VectorPartitionLifecycleCommandV1) { c.Reason = "postmerge cleanup control" }))
			retired := postmergeSnapshotBytesV1(t, leader)
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleMarkCleanableV1, nil))
			cleanable := postmergeSnapshotBytesV1(t, leader)
			for _, group := range record.RequiredGroups {
				record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupCleanupV1, func(c *VectorPartitionLifecycleCommandV1) { c.GroupID = group }))
			}
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleCompleteCleanupV1, nil))
			absent := postmergeSnapshotBytesV1(t, leader)
			for _, control := range [][]byte{retired, cleanable, absent} {
				assertReplicaReplacementBudgetSnapshotV1(t, before, control)
			}
			for _, tc := range []struct {
				name     string
				raw      []byte
				revision uint64
			}{
				{"retired-final-digest", retired, oldRevision + 1},
				{"cleanable-skipped-revision", cleanable, oldRevision + 1},
				{"absent-one-revision", absent, oldRevision + 1},
				{"absent-final-digest", absent, record.Revision},
			} {
				t.Run(tc.name, func(t *testing.T) {
					forged := postmergeRewriteSnapshotV1(t, tc.raw, func(snapshot *CatalogMetaSnapshotV1, lifecycle *vectorPartitionLifecycleSnapshotV1) {
						lifecycle.Records[0].Revision = tc.revision
						lifecycle.Records[0].LastCommandDigest = strings.Repeat("e", 64)
						if tc.name == "absent-one-revision" {
							var baseline CatalogMetaSnapshotV1
							if err := json.Unmarshal(before, &baseline); err != nil {
								t.Fatal(err)
							}
							snapshot.AppliedIndex = baseline.AppliedIndex + 1
						}
					})
					postmergeSnapshotRefusesV1(t, before, forged)
				})
			}
		})
	}
}

func TestCatalogSnapshotIndependentSameIndexLifecycleBudgetV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	identity := active.Identity
	identity.Generation++
	applied := leader.applied
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, active.Identity.Generation, active.MutationEpoch+1)
	candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.PreviousActiveRevision = active.Identity.Generation, active.Revision
		c.MutationEpoch = candidate.MutationEpoch
	}))
	retired, ok := leader.VectorPartitionLifecycleRecordV1(active.Identity)
	if !ok {
		t.Fatal("cutover predecessor missing")
	}
	before := postmergeSnapshotBytesV1(t, leader)
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(retired, VectorPartitionLifecycleMarkCleanableV1, nil))
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "independent successor invalidation", candidate.MutationEpoch+1
	}))
	current := postmergeSnapshotBytesV1(t, leader)
	assertReplicaReplacementBudgetSnapshotV1(t, before, current, 1)
}

func postmergeCommitCollectionMutationV1(t *testing.T, a *CatalogMetaAuthorityV1, c vectorPartitionCollectionMutationCommandV1) {
	t.Helper()
	raw, err := encodeVectorPartitionCollectionMutationCommandV1(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedVectorPartitionCollectionMutationV1(raw, a.applied+1); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogSnapshotMixedLifecycleBarrierProgressV1(t *testing.T) {
	for _, mode := range []string{"collection-mutation", "direct-invalidation-jump"} {
		t.Run(mode, func(t *testing.T) {
			leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
			before := postmergeSnapshotBytesV1(t, leader)
			applied := leader.applied
			epoch := active.MutationEpoch + 1
			if mode == "direct-invalidation-jump" {
				epoch += 100
			}
			mutation := vectorPartitionCollectionMutationCommandV1{
				Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
				CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
				ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: epoch, OperationDigest: strings.Repeat("e", 64),
			}
			if mode == "collection-mutation" {
				postmergeCommitCollectionMutationV1(t, leader, mutation)
				applied = leader.applied
			}
			active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
				c.Reason, c.InvalidationEpoch = "mixed lifecycle control", epoch
			}))
			invalidated := postmergeSnapshotBytesV1(t, leader)
			active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = epoch }))
			if mode == "direct-invalidation-jump" {
				mutation.ExpectedMutationEpoch, mutation.MutationEpoch = epoch, epoch+1
				postmergeCommitCollectionMutationV1(t, leader, mutation)
			}
			mutation.Kind, mutation.ExpectedMutationEpoch = vectorPartitionConfirmCollectionMutationV1, mutation.MutationEpoch
			postmergeCommitCollectionMutationV1(t, leader, mutation)
			current := postmergeSnapshotBytesV1(t, leader)
			assertReplicaReplacementBudgetSnapshotV1(t, before, current, 3)
			forged := postmergeRewriteSnapshotV1(t, invalidated, func(snapshot *CatalogMetaSnapshotV1, lifecycle *vectorPartitionLifecycleSnapshotV1) {
				lifecycle.CollectionMutationBarriers = []vectorPartitionCollectionMutationBarrierV1{{
					Collection: active.Identity.Index.Collection, Epoch: math.MaxUint64, OperationDigest: strings.Repeat("f", 64),
					Completed: []vectorPartitionCollectionCompletedMutationV1{{Epoch: math.MaxUint64, OperationDigest: strings.Repeat("f", 64)}},
				}}
			})
			t.Run("confirmed-max-epoch", func(t *testing.T) { postmergeSnapshotRefusesV1(t, before, forged) })
		})
	}
}

// Ordinary catch-up may pass through activation and then erase its READY sets.
// These snapshots are produced by real authority commands, not projections.
func TestCatalogSnapshotKnownPreparationServingCatchupV1(t *testing.T) {
	for _, state := range []VectorPartitionLifecycleStateV1{VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1, VectorPartitionLifecyclePreparedV1} {
		t.Run(string(state), func(t *testing.T) {
			leader, _, record := activeReplicaReplacementAuthorityV1(t, false)
			applied := leader.applied
			var before []byte
			if state == VectorPartitionLifecycleBuildingV1 {
				before = postmergeSnapshotBytesV1(t, leader)
			}
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
				c.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: "group-b", AppliedIndex: applied, AssetSetDigest: strings.Repeat("c", 64)}
			}))
			if state == VectorPartitionLifecycleStagedV1 {
				before = postmergeSnapshotBytesV1(t, leader)
			}
			digest, err := VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
			if err != nil {
				t.Fatal(err)
			}
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(c *VectorPartitionLifecycleCommandV1) { c.ReadySetDigest = digest }))
			if state == VectorPartitionLifecyclePreparedV1 {
				before = postmergeSnapshotBytesV1(t, leader)
			}
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = record.MutationEpoch }))
			assertReplicaReplacementBudgetSnapshotV1(t, before, postmergeSnapshotBytesV1(t, leader))
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
				c.Reason, c.InvalidationEpoch = "serving catch-up", record.MutationEpoch+1
			}))
			assertReplicaReplacementBudgetSnapshotV1(t, before, postmergeSnapshotBytesV1(t, leader))
			record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = record.InvalidationEpoch }))
			assertReplicaReplacementBudgetSnapshotV1(t, before, postmergeSnapshotBytesV1(t, leader))
			for _, kind := range []VectorPartitionLifecycleCommandKindV1{VectorPartitionLifecycleRetireV1, VectorPartitionLifecycleMarkCleanableV1, VectorPartitionLifecycleRecordGroupCleanupV1, VectorPartitionLifecycleCompleteCleanupV1} {
				record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, kind, func(c *VectorPartitionLifecycleCommandV1) {
					if kind == VectorPartitionLifecycleRecordGroupCleanupV1 {
						c.GroupID = "group-b"
					}
				}))
				assertReplicaReplacementBudgetSnapshotV1(t, before, postmergeSnapshotBytesV1(t, leader))
			}
		})
	}
}

func TestCatalogSnapshotCompactedCutoverIndependentSuffixBudgetV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	identity := active.Identity
	identity.Generation++
	applied := leader.applied
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, active.Identity.Generation, active.MutationEpoch+1)
	before := postmergeSnapshotBytesV1(t, leader)
	candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.PreviousActiveRevision = active.Identity.Generation, active.Revision
		c.MutationEpoch = candidate.MutationEpoch
	}))
	retired, ok := leader.VectorPartitionLifecycleRecordV1(active.Identity)
	if !ok {
		t.Fatal("missing cutover predecessor")
	}
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(retired, VectorPartitionLifecycleMarkCleanableV1, nil))
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "independent post-cutover suffix", candidate.MutationEpoch+1
	}))
	current := postmergeSnapshotBytesV1(t, leader)
	assertReplicaReplacementBudgetSnapshotV1(t, before, current, 2)
	independent := NewCatalogMetaAuthorityV1()
	if err := independent.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	independentApplied := independent.applied
	invalidated := catalogMetaLifecycleApplyV1(t, independent, &independentApplied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "independent predecessor mutation", active.MutationEpoch+1
	}))
	invalidated = catalogMetaLifecycleApplyV1(t, independent, &independentApplied, catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = invalidated.InvalidationEpoch }))
	assertReplicaReplacementBudgetSnapshotV1(t, before, postmergeSnapshotBytesV1(t, independent))
	forged := postmergeRewriteSnapshotV1(t, current, func(snapshot *CatalogMetaSnapshotV1, lifecycle *vectorPartitionLifecycleSnapshotV1) {
		for i := range lifecycle.Records {
			if lifecycle.Records[i].Identity == active.Identity {
				lifecycle.Records[i] = invalidated
			}
		}
		// Spare budget cannot prove the omitted atomic predecessor retirement.
		snapshot.AppliedIndex += 10
	})
	t.Run("activation-without-predecessor-retirement", func(t *testing.T) { postmergeSnapshotRefusesV1(t, before, forged) })
}

// Legacy admission permits two source identities for one Index+generation.
// That history remains canonical, but cannot select an atomic pair by map order.
func TestCatalogSnapshotLegacyAmbiguousCutoverBudgetV1(t *testing.T) {
	leader, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	active := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, 0, 9)
	active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = active.MutationEpoch }))
	candidateIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 8, 12)
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, candidateIdentity, active.Identity.Generation, 10)
	aliasIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 8, 13)
	catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, aliasIdentity, active.Identity.Generation, 10)
	before := cloneVectorPartitionLifecycleRecordsV1(leader.lifecycle)
	beforeBytes := postmergeSnapshotBytesV1(t, leader)
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(beforeBytes); err != nil {
		t.Fatalf("legacy source aliases not admitted: %v", err)
	}
	if cost, err := knownVectorPartitionLifecycleSnapshotEntryCostV1(before, before, leader.mutationFences, 0); err != nil || cost != 0 {
		t.Fatalf("unchanged legacy history: cost=%d err=%v", cost, err)
	}
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.PreviousActiveRevision = active.Identity.Generation, active.Revision
		c.MutationEpoch = candidate.MutationEpoch
	}))
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(postmergeSnapshotBytesV1(t, leader)); err != nil {
		t.Fatalf("legacy cutover producer not canonical: %v", err)
	}
	for i := 0; i < 8; i++ {
		if cost, err := knownVectorPartitionLifecycleSnapshotEntryCostV1(before, leader.lifecycle, leader.mutationFences, 2); err != nil || cost != 2 {
			t.Fatalf("ambiguous pair selected: cost=%d err=%v", cost, err)
		}
		if _, err := knownVectorPartitionLifecycleSnapshotEntryCostV1(before, leader.lifecycle, leader.mutationFences, 1); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("ambiguous pair discounted: %v", err)
		}
	}
}

func TestCatalogSnapshotInitialActivationServingNameGuardV1(t *testing.T) {
	t.Run("unchanged higher-watermark ACTIVE", func(t *testing.T) {
		leader, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
		applied := uint64(1)
		active := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, catalogMetaLifecycleTestIdentityV1(catalog, 7, 11), 0, 20)
		active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = 20 }))
		candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, catalogMetaLifecycleTestIdentityV1(catalog, 8, 12), 0, 10)
		before := postmergeSnapshotBytesV1(t, leader)
		// The pure reducer can form these records; catalog ACTIVATE refuses them
		// while the known serving name remains ACTIVE. Confirmed fence 11 is below
		// old ACTIVE's captured watermark 20, so the forged snapshot is canonical.
		var err error
		for _, kind := range []VectorPartitionLifecycleCommandKindV1{VectorPartitionLifecycleActivateV1, VectorPartitionLifecycleInvalidateV1, VectorPartitionLifecycleConfirmMutationV1} {
			c := catalogMetaLifecycleTestCommandV1(candidate, kind, func(c *VectorPartitionLifecycleCommandV1) {
				switch kind {
				case VectorPartitionLifecycleActivateV1:
					c.MutationEpoch = 10
				case VectorPartitionLifecycleInvalidateV1:
					c.Reason, c.InvalidationEpoch = "forged initial activation", 11
				case VectorPartitionLifecycleConfirmMutationV1:
					c.MutationEpoch = 11
				}
			})
			candidate, err = ApplyVectorPartitionLifecycleCommandV1(candidate, c)
			if err != nil {
				t.Fatal(err)
			}
		}
		forged := postmergeRewriteSnapshotV1(t, before, func(snapshot *CatalogMetaSnapshotV1, lifecycle *vectorPartitionLifecycleSnapshotV1) {
			for i := range lifecycle.Records {
				if lifecycle.Records[i].Identity == candidate.Identity {
					lifecycle.Records[i] = candidate
				}
			}
			lifecycle.MutationFences = []vectorPartitionLifecycleMutationFenceV1{{Collection: active.Identity.Index.Collection, IndexName: active.Identity.Index.IndexName, Epoch: 11}}
			snapshot.AppliedIndex += 10
		})
		postmergeSnapshotRefusesV1(t, before, forged)
	})
	t.Run("vacated serving name", func(t *testing.T) {
		leader, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
		applied := uint64(1)
		active := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, catalogMetaLifecycleTestIdentityV1(catalog, 7, 11), 0, 9)
		active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = 9 }))
		candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, catalogMetaLifecycleTestIdentityV1(catalog, 8, 12), 0, 11)
		before := postmergeSnapshotBytesV1(t, leader)
		active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) { c.Reason, c.InvalidationEpoch = "vacate serving name", 10 }))
		unconfirmed := active
		active = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = 10 }))
		catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleRetireV1, nil))
		candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = 11 }))
		candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) { c.Reason, c.InvalidationEpoch = "new serving mutation", 12 }))
		catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = 12 }))
		current := postmergeSnapshotBytesV1(t, leader)
		assertReplicaReplacementBudgetSnapshotV1(t, before, current)
		t.Run("older retained unconfirmed debt", func(t *testing.T) {
			// The final confirmed fence at 12 makes this earlier unconfirmed
			// INVALIDATED record at 10 canonical. It cannot prove the serving
			// name was available to ACTIVATE while its mutation debt was pending.
			forged := postmergeRewriteSnapshotV1(t, current, func(snapshot *CatalogMetaSnapshotV1, lifecycle *vectorPartitionLifecycleSnapshotV1) {
				for i := range lifecycle.Records {
					if lifecycle.Records[i].Identity == unconfirmed.Identity {
						lifecycle.Records[i] = unconfirmed
					}
				}
				snapshot.AppliedIndex += 10
			})
			postmergeSnapshotRefusesV1(t, before, forged)
		})
	})
}

func TestCatalogSnapshotMixedBarrierInvalidationOrderingV1(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pairs        uint64
		earlyEpoch   uint64
		lateEpoch    uint64
		localPending bool
	}{
		{"late-above-final", 100, 0, 1000, false},
		{"late-between-retained-and-final", 100, 0, 70, false},
		{"late-below-retained-with-evicted-entry", 100, 0, 44, false},
		{"early-and-late-known-records", 65, 100, 1000, false},
		{"old-pending-confirmation-evicted-late", 100, 0, 1000, true},
		{"old-pending-confirmation-evicted-early", 65, 100, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
			applied := leader.applied
			late := active
			if tc.earlyEpoch != 0 && tc.lateEpoch != 0 {
				identity := active.Identity
				identity.Index.IndexName = "embedding-late"
				late = catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, 0, active.MutationEpoch)
				late = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(late, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = active.MutationEpoch }))
			}
			mutation := vectorPartitionCollectionMutationCommandV1{
				Collection:   active.Identity.Index.Collection,
				CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
			}
			localEpoch := active.MutationEpoch
			if tc.localPending {
				mutation.Kind = vectorPartitionBeginCollectionMutationV1
				mutation.ExpectedMutationEpoch, mutation.MutationEpoch = localEpoch, localEpoch+1
				mutation.OperationDigest = strings.Repeat("e", 64)
				postmergeCommitCollectionMutationV1(t, leader, mutation)
				localEpoch++
			}
			before := postmergeSnapshotBytesV1(t, leader)
			invalidate := func(record VectorPartitionLifecycleRecordV1, epoch uint64) {
				applied = leader.applied
				record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
					c.Reason, c.InvalidationEpoch = "retained barrier ordering", epoch
				}))
				catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) { c.MutationEpoch = epoch }))
			}
			knownEntries := uint64(0)
			effective := localEpoch
			if tc.earlyEpoch != 0 {
				invalidate(active, tc.earlyEpoch)
				knownEntries += 2
				effective = tc.earlyEpoch
			}
			if tc.localPending {
				mutation.Kind = vectorPartitionConfirmCollectionMutationV1
				mutation.ExpectedMutationEpoch = mutation.MutationEpoch
				postmergeCommitCollectionMutationV1(t, leader, mutation)
			}
			for i := uint64(0); i < tc.pairs; i++ {
				mutation.Kind = vectorPartitionBeginCollectionMutationV1
				mutation.ExpectedMutationEpoch, mutation.MutationEpoch = effective, effective+1
				mutation.OperationDigest = fmt.Sprintf("%064x", i+1)
				postmergeCommitCollectionMutationV1(t, leader, mutation)
				mutation.Kind, mutation.ExpectedMutationEpoch = vectorPartitionConfirmCollectionMutationV1, mutation.MutationEpoch
				postmergeCommitCollectionMutationV1(t, leader, mutation)
				effective++
			}
			if tc.lateEpoch != 0 {
				invalidate(late, tc.lateEpoch)
				knownEntries += 2
			}
			current := postmergeSnapshotBytesV1(t, leader)
			t.Run("genuine producer", func(t *testing.T) { assertReplicaReplacementBudgetSnapshotV1(t, before, current) })
			// The retained window contains the final 64 confirmations. A possible
			// preceding jump must be below its first new BEGIN. This is a minimum
			// compatible history, not authentication of the actual erased order.
			firstBegin := effective - maxVectorPartitionCollectionCompletedMutationsV1 + 1
			floor := localEpoch
			for _, epoch := range []uint64{tc.earlyEpoch, tc.lateEpoch} {
				if epoch < firstBegin && epoch > floor {
					floor = epoch
				}
			}
			required := knownEntries + 2*(effective-floor)
			if tc.localPending {
				required++ // The locally committed BEGIN still needs its CONFIRM.
			}
			t.Run("one missing compatible entry", func(t *testing.T) {
				var baseline CatalogMetaSnapshotV1
				if err := json.Unmarshal(before, &baseline); err != nil {
					t.Fatal(err)
				}
				forged := postmergeRewriteSnapshotV1(t, current, func(snapshot *CatalogMetaSnapshotV1, _ *vectorPartitionLifecycleSnapshotV1) {
					snapshot.AppliedIndex = baseline.AppliedIndex + required - 1
				})
				postmergeSnapshotRefusesV1(t, before, forged)
			})
		})
	}
}
