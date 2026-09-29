package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
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
