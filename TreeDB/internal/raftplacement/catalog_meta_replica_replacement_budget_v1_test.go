package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCatalogReplicaReplacementPendingRefusesNewCollectionMutationV1(t *testing.T) {
	a, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	mutation := vectorPartitionCollectionMutationCommandV1{
		Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
		CatalogEpoch: a.record.Epoch, CatalogDigest: a.record.Digest,
		ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: active.MutationEpoch + 1,
		OperationDigest: strings.Repeat("e", 64),
	}
	commit := func(command vectorPartitionCollectionMutationCommandV1) error {
		t.Helper()
		raw, err := encodeVectorPartitionCollectionMutationCommandV1(command)
		if err != nil {
			t.Fatal(err)
		}
		_, err = a.applyCommittedVectorPartitionCollectionMutationV1(raw, a.applied+1)
		return err
	}
	if err := commit(mutation); err != nil {
		t.Fatal(err)
	}
	confirm := mutation
	confirm.Kind, confirm.ExpectedMutationEpoch = vectorPartitionConfirmCollectionMutationV1, mutation.MutationEpoch
	if err := commit(confirm); err != nil {
		t.Fatal(err)
	}
	rawBegin, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(rawBegin, a.applied+1); err != nil {
		t.Fatal(err)
	}
	before, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	for _, retry := range []vectorPartitionCollectionMutationCommandV1{mutation, confirm} {
		if err := commit(retry); err != nil {
			t.Fatalf("exact %s retry: %v", retry.Kind, err)
		}
	}
	mutation.ExpectedMutationEpoch, mutation.MutationEpoch = mutation.MutationEpoch, mutation.MutationEpoch+1
	mutation.OperationDigest = strings.Repeat("f", 64)
	if err := commit(mutation); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("new mutation BEGIN during pending replacement: %v", err)
	}
	if after, err := a.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refusal or exact retry mutated authority: %v", err)
	}
	completeReplicaReplacementForTestV1(t, a, begin)
	mutation.CatalogEpoch, mutation.CatalogDigest = a.record.Epoch, a.record.Digest
	if err := commit(mutation); err != nil {
		t.Fatalf("new BEGIN after completion: %v", err)
	}
	confirm = mutation
	confirm.Kind, confirm.ExpectedMutationEpoch = vectorPartitionConfirmCollectionMutationV1, mutation.MutationEpoch
	if err := commit(confirm); err != nil {
		t.Fatalf("CONFIRM after completion: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotReservesEveryPhaseV1(t *testing.T) {
	for _, local := range []string{"unknown", "begun"} {
		for _, terminal := range []string{"removed", "completed", "completed-with-barrier"} {
			if local == "unknown" && terminal != "removed" {
				continue
			} // Completion requires an anchored BEGIN.
			t.Run(local+"/"+terminal, func(t *testing.T) {
				leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
				before, err := leader.ExportCatalogMetaSnapshotBytesV1()
				if err != nil {
					t.Fatal(err)
				}
				if local == "begun" {
					raw, err := EncodeReplicaReplacementBeginV1(begin)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := leader.applyCommittedCatalogMetaV1(raw, leader.applied+1); err != nil {
						t.Fatal(err)
					}
					before, err = leader.ExportCatalogMetaSnapshotBytesV1()
					if err != nil {
						t.Fatal(err)
					}
				}
				complete := removedReplicaReplacementForTestV1(t, leader, begin)
				if terminal != "removed" {
					raw, err := EncodeReplicaReplacementCompleteV1(complete)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := leader.applyCommittedCatalogMetaV1(raw, leader.applied+1); err != nil {
						t.Fatal(err)
					}
				}
				if terminal == "completed-with-barrier" {
					// A real new barrier begins only after replacement completion.
					mutation := vectorPartitionCollectionMutationCommandV1{
						Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
						CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
						ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: active.MutationEpoch + 1,
						OperationDigest: strings.Repeat("e", 64),
					}
					for _, kind := range []vectorPartitionCollectionMutationCommandKindV1{vectorPartitionBeginCollectionMutationV1, vectorPartitionConfirmCollectionMutationV1} {
						mutation.Kind = kind
						if kind == vectorPartitionConfirmCollectionMutationV1 {
							mutation.ExpectedMutationEpoch = mutation.MutationEpoch
						}
						raw, err := encodeVectorPartitionCollectionMutationCommandV1(mutation)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := leader.applyCommittedVectorPartitionCollectionMutationV1(raw, leader.applied+1); err != nil {
							t.Fatal(err)
						}
					}
				}
				current, err := leader.ExportCatalogMetaSnapshotBytesV1()
				if err != nil {
					t.Fatal(err)
				}
				follower := NewCatalogMetaAuthorityV1()
				if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
					t.Fatal(err)
				}
				if err := follower.installCatalogMetaSnapshotBytesV1(current); err != nil {
					t.Fatalf("genuine phase catch-up: %v", err)
				}
				var short CatalogMetaSnapshotV1
				if err := json.Unmarshal(current, &short); err != nil {
					t.Fatal(err)
				}
				short.AppliedIndex-- // One fewer than the mandatory entries in this exact producer history.
				shortRaw, err := json.Marshal(short)
				if err != nil {
					t.Fatal(err)
				}
				if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(shortRaw); err != nil {
					t.Fatalf("short fixture canonicality: %v", err)
				}
				refusing := NewCatalogMetaAuthorityV1()
				if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
					t.Fatal(err)
				}
				if err := refusing.installCatalogMetaSnapshotBytesV1(shortRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
					t.Fatalf("snapshot spent a required phase index twice: %v", err)
				}
				if after, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(before, after) {
					t.Fatalf("short phase snapshot refusal mutated authority: %v", err)
				}
			})
		}
	}
}

func cleanupReplicaReplacementBudgetRecordV1(t *testing.T, a *CatalogMetaAuthorityV1, record VectorPartitionLifecycleRecordV1) {
	t.Helper()
	applied := a.applied
	record = catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "budget control"
		command.InvalidationEpoch = record.MutationEpoch + 1
	}))
	record = catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = record.InvalidationEpoch
	}))
	for _, kind := range []VectorPartitionLifecycleCommandKindV1{VectorPartitionLifecycleRetireV1, VectorPartitionLifecycleMarkCleanableV1} {
		record = catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(record, kind, nil))
	}
	for _, group := range record.RequiredGroups {
		record = catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupCleanupV1, func(command *VectorPartitionLifecycleCommandV1) {
			command.GroupID = group
		}))
	}
	catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleCompleteCleanupV1, nil))
}

func assertReplicaReplacementBudgetSnapshotV1(t *testing.T, before, current []byte, shortGaps ...uint64) {
	t.Helper()
	var baseline, snapshot CatalogMetaSnapshotV1
	if err := json.Unmarshal(before, &baseline); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(current, &snapshot); err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(current); err != nil {
		t.Fatalf("genuine known lifecycle snapshot: %v", err)
	}
	for _, gap := range shortGaps {
		t.Run("short-gap-"+fmt.Sprint(gap), func(t *testing.T) {
			short := snapshot
			short.AppliedIndex = baseline.AppliedIndex + gap
			raw, err := json.Marshal(short)
			if err != nil {
				t.Fatal(err)
			}
			if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(raw); err != nil {
				t.Fatalf("short fixture canonicality: %v", err)
			}
			refusing := NewCatalogMetaAuthorityV1()
			if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
				t.Fatal(err)
			}
			if err := refusing.installCatalogMetaSnapshotBytesV1(raw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
				t.Fatalf("known lifecycle commands exceeded applied gap%d: %v", gap, err)
			}
			if after, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(after, before) {
				t.Fatalf("short lifecycle refusal mutated authority: %v", err)
			}
		})
	}
}

func TestCatalogReplicaReplacementSnapshotKnownLifecycleEntryBudgetV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	oldApplied := leader.applied
	cleanupReplicaReplacementBudgetRecordV1(t, leader, active)
	if leader.applied-oldApplied != 6 {
		t.Fatalf("cleanup used%d entries want6", leader.applied-oldApplied)
	}
	current, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	assertReplicaReplacementBudgetSnapshotV1(t, before, current, 1, 5)
}

func TestCatalogReplicaReplacementSnapshotIndependentLifecycleEntryBudgetsV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	identity := active.Identity
	identity.Index.IndexName = "other-embedding"
	applied := leader.applied
	other := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, 0, active.MutationEpoch)
	other = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(other, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = other.MutationEpoch
	}))
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	oldApplied := leader.applied
	cleanupReplicaReplacementBudgetRecordV1(t, leader, active)
	cleanupReplicaReplacementBudgetRecordV1(t, leader, other)
	if leader.applied-oldApplied != 12 {
		t.Fatalf("two index cleanups used%d entries want12", leader.applied-oldApplied)
	}
	current, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	assertReplicaReplacementBudgetSnapshotV1(t, before, current, 6, 11)
}

func TestCatalogReplicaReplacementSnapshotCutoverUsesOneLifecycleEntryV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	identity := active.Identity
	identity.Generation++
	applied := leader.applied
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, active.Identity.Generation, active.MutationEpoch+1)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	oldApplied := leader.applied
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration, command.PreviousActiveRevision = active.Identity.Generation, active.Revision
		command.MutationEpoch = candidate.MutationEpoch
	}))
	old, ok := leader.VectorPartitionLifecycleRecordV1(active.Identity)
	if !ok || old.Revision != active.Revision+1 {
		t.Fatal("cutover predecessor revision did not advance once")
	}
	newRecord, ok := leader.VectorPartitionLifecycleRecordV1(identity)
	if !ok || newRecord.Revision != candidate.Revision+1 || old.Identity.Index != newRecord.Identity.Index {
		t.Fatal("cutover candidate/Index fixture mismatch")
	}
	if leader.applied != oldApplied+1 {
		t.Fatal("cutover did not consume exactly one applied entry")
	}
	current, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	assertReplicaReplacementBudgetSnapshotV1(t, before, current)
}
