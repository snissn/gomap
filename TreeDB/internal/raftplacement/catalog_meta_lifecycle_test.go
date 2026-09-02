package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestCatalogMetaLifecycleFeatureFloorAndExactRetry(t *testing.T) {
	t.Run("missing feature floor refuses lifecycle", func(t *testing.T) {
		authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, false)
		identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
		command := catalogMetaLifecycleTestBeginV1(identity, 0, 10)
		raw := mustEncodeCatalogMetaLifecycleCommandV1(t, command)
		if _, err := authority.applyCommittedCatalogMetaV1(raw, 2); !errors.Is(err, ErrUnsupportedFeature) {
			t.Fatalf("lifecycle without feature floor err=%v", err)
		}
		if _, ok := authority.VectorPartitionLifecycleRecordV1(identity); ok {
			t.Fatal("feature-floor rejection published lifecycle state")
		}
		status, ok := authority.Status()
		if !ok || status.AppliedIndex != 1 {
			t.Fatalf("status after rejection=%+v available=%v", status, ok)
		}
	})

	t.Run("exact retry preserves lifecycle revision", func(t *testing.T) {
		authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
		identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
		raw := mustEncodeCatalogMetaLifecycleCommandV1(t, catalogMetaLifecycleTestBeginV1(identity, 0, 10))
		first, err := authority.applyCommittedCatalogMetaV1(raw, 2)
		if err != nil {
			t.Fatalf("first apply: %v", err)
		}
		before, ok := authority.VectorPartitionLifecycleRecordV1(identity)
		if !ok {
			t.Fatal("first apply did not publish lifecycle record")
		}
		retry, err := authority.applyCommittedCatalogMetaV1(raw, 3)
		if err != nil {
			t.Fatalf("exact retry: %v", err)
		}
		after, ok := authority.VectorPartitionLifecycleRecordV1(identity)
		if !ok || !reflect.DeepEqual(after, before) {
			t.Fatalf("retry record=%+v available=%v want %+v", after, ok, before)
		}
		if first.AppliedIndex != 2 || retry.AppliedIndex != 2 || after.Revision != 1 {
			t.Fatalf("first/retry/record=%+v / %+v / %+v", first, retry, after)
		}
	})
}

func TestCatalogMetaLifecycleRecordReturnsDeepCopy(t *testing.T) {
	authority := NewCatalogMetaAuthorityV1()
	identity := VectorPartitionLifecycleIdentityV1{Generation: 7}
	authority.lifecycle = make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1)
	authority.lifecycle[identity] = VectorPartitionLifecycleRecordV1{
		Identity:       identity,
		RequiredGroups: []raftcluster.GroupID{"group-a"},
		ReadyGroups: []VectorPartitionLifecycleGroupReadyV1{{
			GroupID:        "group-a",
			AppliedIndex:   11,
			AssetSetDigest: strings.Repeat("a", 64),
		}},
		CleanedGroups: []raftcluster.GroupID{"group-a"},
	}

	got, ok := authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok {
		t.Fatal("VectorPartitionLifecycleRecordV1 record unavailable")
	}
	got.RequiredGroups[0] = "tampered-required"
	got.ReadyGroups[0].GroupID = "tampered-ready"
	got.CleanedGroups[0] = "tampered-cleaned"

	retained, ok := authority.VectorPartitionLifecycleRecordV1(identity)
	if !ok {
		t.Fatal("VectorPartitionLifecycleRecordV1 retained record unavailable")
	}
	if retained.RequiredGroups[0] != "group-a" || retained.ReadyGroups[0].GroupID != "group-a" || retained.CleanedGroups[0] != "group-a" {
		t.Fatalf("returned slice mutation changed authority record: %+v", retained)
	}
}

func TestCatalogMetaLifecycleAtomicCutoverInvalidationCleanupAndStatus(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)

	oldIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 6, 10)
	old := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, oldIdentity, 0, 9)
	old = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(old, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	if old.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("old state=%q", old.State)
	}
	collisionIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	collisionIdentity.Index.IndexEpoch++
	collisionIdentity.Index.IndexDefinitionDigest = strings.Repeat("c", 64)
	collision := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, collisionIdentity, 0, 10)
	collisionActivate := catalogMetaLifecycleTestCommandV1(collision, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 10
	})
	if _, err := authority.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, collisionActivate), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("same serving-name activation err=%v", err)
	}

	newIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	candidate := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, newIdentity, oldIdentity.Generation, 10)
	cutover := catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration = oldIdentity.Generation
		command.PreviousActiveRevision = old.Revision
		command.MutationEpoch = 10
	})
	catalogMetaLifecycleApplyV1(t, authority, &applied, cutover)

	retired, ok := authority.VectorPartitionLifecycleRecordV1(oldIdentity)
	if !ok || retired.State != VectorPartitionLifecycleRetiredV1 ||
		retired.SupersededByGeneration != newIdentity.Generation || retired.InvalidationEpoch != 0 {
		t.Fatalf("retired record=%+v available=%v", retired, ok)
	}
	active, ok := authority.VectorPartitionLifecycleRecordV1(newIdentity)
	if !ok || active.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("active record=%+v available=%v", active, ok)
	}

	if err := catalogMetaLifecycleValidateSearchV1(authority, oldIdentity, retired.ReadySetDigest); !errors.Is(err, ErrVectorPartitionLifecycleIdentity) {
		t.Fatalf("superseded search err=%v", err)
	}
	if err := catalogMetaLifecycleValidateSearchV1(authority, newIdentity, active.ReadySetDigest); err != nil {
		t.Fatalf("active search: %v", err)
	}
	snapshot, err := authority.VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		context.Background(), applied, newIdentity.Index.Collection, newIdentity.Index.IndexName,
		newIdentity.Generation, newIdentity.Index.IndexDefinitionDigest, newIdentity.Source.Generation,
		newIdentity.Source.Checksum, newIdentity.Source.SchemaHash, newIdentity.Source.RowCount,
	)
	if err != nil || snapshot.Catalog.AppliedIndex != applied || snapshot.Identity != newIdentity || snapshot.Record.ReadySetDigest != active.ReadySetDigest {
		t.Fatalf("serving authority snapshot=%+v err=%v", snapshot, err)
	}
	if err := authority.ValidateVectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(context.Background(), applied, snapshot); err != nil {
		t.Fatalf("validate serving authority snapshot: %v", err)
	}
	changed := snapshot
	changed.Record.ReadyGroups = slices.Clone(snapshot.Record.ReadyGroups)
	changed.Record.ReadyGroups[0].AppliedIndex++
	if err := authority.ValidateVectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(context.Background(), applied, changed); !errors.Is(err, ErrVectorPartitionLifecycleIdentity) {
		t.Fatalf("changed serving authority snapshot err=%v", err)
	}
	if _, err := authority.VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		context.Background(), applied-1, newIdentity.Index.Collection, newIdentity.Index.IndexName,
		newIdentity.Generation, newIdentity.Index.IndexDefinitionDigest, newIdentity.Source.Generation,
		newIdentity.Source.Checksum, newIdentity.Source.SchemaHash, newIdentity.Source.RowCount,
	); !errors.Is(err, ErrCatalogMetaUnavailable) {
		t.Fatalf("mixed-index snapshot err=%v want ErrCatalogMetaUnavailable", err)
	}
	if _, err := authority.ValidateVectorPartitionGenerationSearchAtAppliedIndexV1(
		context.Background(), applied+1, newIdentity.Index.Collection, newIdentity.Index.IndexName,
		newIdentity.Generation, newIdentity.Index.IndexDefinitionDigest,
		newIdentity.Source.Generation, newIdentity.Source.Checksum, newIdentity.Source.SchemaHash,
		newIdentity.Source.RowCount,
	); !errors.Is(err, ErrCatalogMetaUnavailable) {
		t.Fatalf("search against stale local applied view err=%v want ErrCatalogMetaUnavailable", err)
	}
	if err := catalogMetaLifecycleValidateSearchV1(authority, newIdentity, strings.Repeat("f", 64)); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("wrong ready-set search err=%v", err)
	}

	statuses := authority.VectorPartitionLifecycleStatusesV1()
	if len(statuses) != 3 || statuses[0].Identity.Generation != 6 || statuses[1].Identity != newIdentity ||
		statuses[2].Identity != collisionIdentity || statuses[0].Active || !statuses[1].Active || statuses[2].Active {
		t.Fatalf("unsorted or dual-active statuses=%+v", statuses)
	}
	for _, status := range statuses {
		if status.RequiredGroups != 1 || status.ReadyGroups != 1 ||
			status.RetainedWireBytes == 0 || status.RetainedWireBytes > MaxVectorPartitionLifecycleRecordBytesV1 {
			t.Fatalf("unbounded/incomplete operator status=%+v", status)
		}
	}

	badInvalidate := catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant vector update"
		command.InvalidationEpoch = active.MutationEpoch
	})
	raw := mustEncodeCatalogMetaLifecycleCommandV1(t, badInvalidate)
	if _, err := authority.applyCommittedCatalogMetaV1(raw, applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("non-advancing invalidation err=%v", err)
	}
	stillActive, _ := authority.VectorPartitionLifecycleRecordV1(newIdentity)
	if stillActive.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("refused invalidation changed state=%q", stillActive.State)
	}

	active = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant vector update"
		command.InvalidationEpoch = active.MutationEpoch + 1
	}))
	if err := catalogMetaLifecycleValidateSearchV1(authority, newIdentity, active.ReadySetDigest); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("invalidated generation remained searchable: %v", err)
	}

	pinned := catalogMetaLifecycleTestCommandV1(retired, VectorPartitionLifecycleMarkCleanableV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.References.ReaderPins = 1
	})
	if _, err := authority.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, pinned), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("pinned superseded cleanup err=%v", err)
	}
	retired = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(retired, VectorPartitionLifecycleMarkCleanableV1, nil))
	if retired.State != VectorPartitionLifecycleCleanableV1 {
		t.Fatalf("superseded cleanable state=%q", retired.State)
	}
}

func TestCatalogMetaLifecycleSnapshotCanonicalRoundTripAndFailClosed(t *testing.T) {
	authority, oldIdentity, newIdentity := catalogMetaLifecycleTestAuthorityWithCutoverV1(t)
	first, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}
	second, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatalf("second export: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("snapshot bytes are not deterministic")
	}

	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(bytes.Clone(first)); err != nil {
		t.Fatalf("restore canonical snapshot: %v", err)
	}
	oldRecord, oldOK := restored.VectorPartitionLifecycleRecordV1(oldIdentity)
	newRecord, newOK := restored.VectorPartitionLifecycleRecordV1(newIdentity)
	if !oldOK || !newOK || oldRecord.State != VectorPartitionLifecycleRetiredV1 ||
		newRecord.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("restored old/new=%+v/%v %+v/%v", oldRecord, oldOK, newRecord, newOK)
	}
	if err := catalogMetaLifecycleValidateSearchV1(restored, newIdentity, newRecord.ReadySetDigest); err != nil {
		t.Fatalf("restored search admission: %v", err)
	}

	var outer CatalogMetaSnapshotV1
	if err := json.Unmarshal(first, &outer); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(outer.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	if len(lifecycle.Records) != 2 {
		t.Fatalf("snapshot lifecycle records=%d", len(lifecycle.Records))
	}

	t.Run("noncanonical order", func(t *testing.T) {
		candidate := outer
		payload := lifecycle
		payload.Records = append([]VectorPartitionLifecycleRecordV1(nil), lifecycle.Records...)
		payload.Records[0], payload.Records[1] = payload.Records[1], payload.Records[0]
		candidate.VectorPartitionLifecycle = mustJSONCatalogMetaLifecycleV1(t, payload)
		raw := mustJSONCatalogMetaLifecycleV1(t, candidate)
		target := NewCatalogMetaAuthorityV1()
		if err := target.installCatalogMetaSnapshotBytesV1(raw); !errors.Is(err, ErrInvalidVectorPartitionLifecycle) {
			t.Fatalf("noncanonical lifecycle err=%v", err)
		}
		if _, ok := target.Status(); ok {
			t.Fatal("noncanonical lifecycle snapshot published catalog state")
		}
	})

	t.Run("dual active", func(t *testing.T) {
		candidate := outer
		payload := lifecycle
		payload.Records = append([]VectorPartitionLifecycleRecordV1(nil), lifecycle.Records...)
		payload.Records[0].State = VectorPartitionLifecycleActiveV1
		payload.Records[0].SupersededByGeneration = 0
		candidate.VectorPartitionLifecycle = mustJSONCatalogMetaLifecycleV1(t, payload)
		raw := mustJSONCatalogMetaLifecycleV1(t, candidate)
		target := NewCatalogMetaAuthorityV1()
		if err := target.installCatalogMetaSnapshotBytesV1(raw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("dual-active lifecycle err=%v", err)
		}
		if _, ok := target.Status(); ok {
			t.Fatal("dual-active lifecycle snapshot published catalog state")
		}
	})

	t.Run("malformed", func(t *testing.T) {
		candidate := outer
		candidate.VectorPartitionLifecycle = []byte(`{"format":1,"records":[],"unknown":true}`)
		raw := mustJSONCatalogMetaLifecycleV1(t, candidate)
		target := NewCatalogMetaAuthorityV1()
		if err := target.installCatalogMetaSnapshotBytesV1(raw); !errors.Is(err, ErrInvalidVectorPartitionLifecycle) {
			t.Fatalf("malformed lifecycle err=%v", err)
		}
		if _, ok := target.Status(); ok {
			t.Fatal("malformed lifecycle snapshot published catalog state")
		}
	})
}

func TestCatalogMetaLifecycleMutationFenceRejectsRacingCandidateAfterRestoreV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	activeIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 6, 10)
	active := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, activeIdentity, 0, 9)
	active = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 9
	}))

	// This candidate captured the old source while the active generation was
	// still serving.  It is prepared before the data mutation admission races
	// through its durable invalidation.
	staleIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	stale := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, staleIdentity, activeIdentity.Generation, 9)
	active = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "relevant mutation", 10
	}))

	// Crash/failover after invalidation but before the data submit must retain
	// the fence.  A replayed stale activation cannot re-open serving.
	snapshot, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatalf("export fenced snapshot: %v", err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
		t.Fatalf("restore fenced snapshot: %v", err)
	}
	fences := restored.VectorPartitionLifecycleMutationFencesV1()
	if len(fences) != 1 || !fences[0].Pending || fences[0].Epoch != 10 || fences[0].Collection != activeIdentity.Index.Collection || fences[0].IndexName != activeIdentity.Index.IndexName {
		t.Fatalf("restored pending-fence operator state=%+v", fences)
	}
	retireWhilePending := catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleRetireV1, nil)
	if _, err := restored.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, retireWhilePending), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("cleanup during pending mutation err=%v", err)
	}
	activate := catalogMetaLifecycleTestCommandV1(stale, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.PreviousActiveRevision, c.MutationEpoch = activeIdentity.Generation, active.Revision, 9
	})
	if _, err := restored.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, activate), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("stale activation after restore err=%v", err)
	}
	// Equal-to-fence is adversarial too: the number alone cannot prove that
	// the data entry committed, so build/source capture remains blocked while
	// the durable mutation outcome is pending.
	equalIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 8, 12)
	equalBegin := catalogMetaLifecycleTestBeginV1(equalIdentity, 0, 10)
	if _, err := restored.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, equalBegin), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("equal-fence build during pending mutation err=%v", err)
	}
	// This models the only release path: the data Raft result is definitive,
	// then its exact invalidation proof is committed as confirmation.
	active = catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleConfirmMutationV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 10
	}))
	if !active.MutationConfirmed {
		t.Fatal("committed mutation did not release pending fence")
	}
	// Cleanup is allowed to reclaim the invalidated generation's assets, but
	// not the durable mutation fence.  A delayed/replayed candidate remains
	// refused after the source record has reached its terminal absent state.
	active = catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleRetireV1, nil))
	active = catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleMarkCleanableV1, nil))
	active = catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleRecordGroupCleanupV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupID = "group-a"
	}))
	catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleCompleteCleanupV1, nil))
	if _, err := restored.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, activate), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("stale activation after cleanup err=%v", err)
	}

	// A replacement built from the post-mutation source watermark is allowed;
	// this is the recovery path after a submit failure leaves the old generation
	// invalidated and the data writer retries/rebuilds.
	freshIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 9, 13)
	fresh := catalogMetaLifecycleBuildPreparedV1(t, restored, &applied, freshIdentity, 0, 10)
	fresh = catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(fresh, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 10
	}))
	if fresh.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("fresh post-fence generation state=%q", fresh.State)
	}
}

func TestCatalogMetaLifecycleSnapshotRejectsInconsistentPendingFenceV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 6, 10)
	active := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, identity, 0, 9)
	active = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 9
	}))
	invalidated := catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "relevant mutation", 10
	}))
	key := vectorPartitionLifecycleServingKeyV1{Collection: identity.Index.Collection, IndexName: identity.Index.IndexName}
	fences := map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1{
		key: {Epoch: 10, Pending: true},
	}
	wrongEpoch := invalidated
	wrongEpoch.InvalidationEpoch++
	tests := []struct {
		name    string
		records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1
	}{
		{name: "missing invalidated record", records: map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{}},
		{name: "active record", records: map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{identity: active}},
		{name: "mismatched invalidation epoch", records: map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{identity: wrongEpoch}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := encodeVectorPartitionLifecycleSnapshotV1(test.records, fences, nil)
			if err != nil {
				t.Fatalf("encode adversarial snapshot: %v", err)
			}
			if _, _, _, _, _, err := decodeVectorPartitionLifecycleSnapshotV1(raw, catalog); !errors.Is(err, ErrInvalidVectorPartitionLifecycle) {
				t.Fatalf("inconsistent pending fence err=%v", err)
			}
		})
	}
}

func TestCatalogMetaLifecycleSameCatalogSnapshotAdvancesButNeverRollsBack(t *testing.T) {
	source, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	target, _ := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	stale, err := target.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatalf("export stale snapshot: %v", err)
	}
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	command := catalogMetaLifecycleTestBeginV1(identity, 0, 10)
	if _, err := source.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, command), 2); err != nil {
		t.Fatalf("apply source lifecycle: %v", err)
	}
	advanced, err := source.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatalf("export advanced snapshot: %v", err)
	}
	if _, err := target.installCatalogMetaSnapshotV1(advanced); err != nil {
		t.Fatalf("install same-catalog advancement: %v", err)
	}
	if record, ok := target.VectorPartitionLifecycleRecordV1(identity); !ok || record.State != VectorPartitionLifecycleBuildingV1 {
		t.Fatalf("advanced lifecycle record=%+v available=%v", record, ok)
	}
	if _, err := target.installCatalogMetaSnapshotV1(stale); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("same-catalog rollback err=%v", err)
	}
	if record, ok := target.VectorPartitionLifecycleRecordV1(identity); !ok || record.State != VectorPartitionLifecycleBuildingV1 {
		t.Fatalf("rollback changed lifecycle record=%+v available=%v", record, ok)
	}
}

func TestCatalogMetaLifecycleGuardsCatalogTransitionUntilCleanup(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	record := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, identity, 0, 10)
	record = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 10
	}))

	nextCatalog := catalogMetaLifecycleCatalogV1(true)
	nextCatalog.Groups[0].LeaderHint = "node-c"
	nextCommand := mustCatalogMetaCommand(t, 1, 2, nextCatalog)
	if _, err := authority.applyCommittedCatalogMetaV1(nextCommand, applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("catalog transition with active lifecycle err=%v", err)
	}

	record = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "catalog transition"
		command.InvalidationEpoch = 11
	}))
	record = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 11
	}))
	record = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRetireV1, nil))
	record = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleMarkCleanableV1, nil))
	record = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupCleanupV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupID = "group-a"
	}))
	catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleCompleteCleanupV1, nil))

	status, err := authority.applyCommittedCatalogMetaV1(nextCommand, applied+1)
	if err != nil {
		t.Fatalf("catalog transition after cleanup: %v", err)
	}
	if status.Epoch != 2 {
		t.Fatalf("catalog epoch=%d want 2", status.Epoch)
	}
	if statuses := authority.VectorPartitionLifecycleStatusesV1(); len(statuses) != 0 {
		t.Fatalf("cleaned lifecycle tombstones survived catalog replacement: %+v", statuses)
	}
}

func TestCatalogMetaLifecycleRefusesFenceMismatchBeforeReducerPublicationV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	active := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, identity, 0, 9)
	active = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	key := vectorPartitionLifecycleServingKeyV1{Collection: identity.Index.Collection, IndexName: identity.Index.IndexName}
	authority.mu.Lock()
	authority.mutationFences[key] = vectorPartitionLifecycleMutationFenceStateV1{Epoch: 11, Pending: true}
	authority.mu.Unlock()

	invalidate := catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "stale invalidation"
		command.InvalidationEpoch = 10
	})
	if _, err := authority.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, invalidate), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("non-advancing durable invalidation err=%v", err)
	}
	if got, ok := authority.VectorPartitionLifecycleRecordV1(identity); !ok || !reflect.DeepEqual(got, active) {
		t.Fatalf("refused invalidation published record=%+v available=%v want %+v", got, ok, active)
	}
	if got := authority.VectorPartitionLifecycleMutationFencesV1(); len(got) != 1 || got[0].Epoch != 11 || !got[0].Pending {
		t.Fatalf("refused invalidation changed fence=%+v", got)
	}

	// A valid-looking record confirmation must also fail before reducer
	// publication when it does not own the exact durable fence.
	authority.mu.Lock()
	invalidated, err := ApplyVectorPartitionLifecycleCommandV1(active, invalidate)
	if err != nil {
		authority.mu.Unlock()
		t.Fatal(err)
	}
	authority.lifecycle[identity] = invalidated
	authority.clearVectorPartitionLifecycleActiveLockedV1(identity)
	authority.mu.Unlock()
	confirm := catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 10
	})
	if _, err := authority.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, confirm), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("non-owner confirmation err=%v", err)
	}
	if got, ok := authority.VectorPartitionLifecycleRecordV1(identity); !ok || !reflect.DeepEqual(got, invalidated) {
		t.Fatalf("refused confirmation published record=%+v available=%v want %+v", got, ok, invalidated)
	}
}

func TestCatalogMetaLifecycleCatalogTransitionRefusesPendingFenceWithoutRecordV1(t *testing.T) {
	authority, _ := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	collection := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "docs"}
	authority.mu.Lock()
	authority.mutationFences = map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1{
		{Collection: collection, IndexName: "embedding"}: {Epoch: 7, Pending: true},
	}
	authority.mu.Unlock()

	nextCatalog := catalogMetaLifecycleCatalogV1(true)
	nextCatalog.Groups[0].LeaderHint = "node-c"
	if _, err := authority.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 2, nextCatalog), 2); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("catalog transition with pending fence err=%v", err)
	}
	if status, ok := authority.Status(); !ok || status.Epoch != 1 {
		t.Fatalf("pending-fence refusal published status=%+v available=%v", status, ok)
	}
}

func TestVectorPartitionLifecycleIdentityComparatorIsTotalV1(t *testing.T) {
	_, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	base := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	tests := []struct {
		name string
		edit func(*VectorPartitionLifecycleIdentityV1)
	}{
		{"index definition digest", func(v *VectorPartitionLifecycleIdentityV1) { v.Index.IndexDefinitionDigest = strings.Repeat("f", 64) }},
		{"catalog digest", func(v *VectorPartitionLifecycleIdentityV1) { v.Index.CatalogDigest = strings.Repeat("f", 64) }},
		{"generation", func(v *VectorPartitionLifecycleIdentityV1) { v.Generation++ }},
		{"source generation", func(v *VectorPartitionLifecycleIdentityV1) { v.Source.Generation++ }},
		{"source checksum", func(v *VectorPartitionLifecycleIdentityV1) { v.Source.Checksum++ }},
		{"source schema", func(v *VectorPartitionLifecycleIdentityV1) { v.Source.SchemaHash++ }},
		{"source row count", func(v *VectorPartitionLifecycleIdentityV1) { v.Source.RowCount++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			other := base
			test.edit(&other)
			if vectorPartitionLifecycleIdentityLessV1(base, other) == vectorPartitionLifecycleIdentityLessV1(other, base) {
				t.Fatalf("distinct identities compare equal: base=%+v other=%+v", base, other)
			}
		})
	}
}

func TestCatalogMetaLifecycleProspectiveOuterSnapshotIsBoundedV1(t *testing.T) {
	authority, _ := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	// The decoded lifecycle payload is below 8 MiB, but base64 expansion makes
	// the complete catalog snapshot exceed the durable outer bound.
	lifecycle := bytes.Repeat([]byte{'x'}, MaxCatalogMetaSnapshotBytesV1*3/4)
	authority.mu.RLock()
	err := authority.validateProspectiveCatalogMetaSnapshotLockedV1(lifecycle, 2)
	authority.mu.RUnlock()
	if !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("prospective oversized outer snapshot err=%v", err)
	}
}

func TestCatalogMetaLifecycleRecordCapRefusesBeforePublicationV1(t *testing.T) {
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	base := catalogMetaLifecycleTestIdentityV1(catalog, 1, 1)
	records := make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, maxVectorPartitionLifecycleSnapshotRecordsV1)
	for i := 0; i < maxVectorPartitionLifecycleSnapshotRecordsV1; i++ {
		identity := base
		identity.Generation = uint64(i + 1)
		identity.Source.Generation = uint64(i + 1)
		record, err := ApplyVectorPartitionLifecycleCommandV1(VectorPartitionLifecycleRecordV1{}, catalogMetaLifecycleTestBeginV1(identity, 0, 1))
		if err != nil {
			t.Fatalf("seed lifecycle %d: %v", i, err)
		}
		records[identity] = record
	}
	authority.mu.Lock()
	authority.lifecycle = records
	authority.mu.Unlock()

	overflow := base
	overflow.Generation = maxVectorPartitionLifecycleSnapshotRecordsV1 + 1
	overflow.Source.Generation = overflow.Generation
	before, _ := authority.Status()
	if _, err := authority.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, catalogMetaLifecycleTestBeginV1(overflow, 0, 1)), before.AppliedIndex+1); !errors.Is(err, ErrVectorPartitionLifecycleLimit) {
		t.Fatalf("lifecycle record overflow err=%v", err)
	}
	if _, ok := authority.VectorPartitionLifecycleRecordV1(overflow); ok {
		t.Fatal("record overflow published the refused generation")
	}
	after, _ := authority.Status()
	if after.AppliedIndex != before.AppliedIndex || len(authority.VectorPartitionLifecycleStatusesV1()) != maxVectorPartitionLifecycleSnapshotRecordsV1 {
		t.Fatalf("record overflow changed authority before=%+v after=%+v records=%d", before, after, len(authority.VectorPartitionLifecycleStatusesV1()))
	}
}

func TestCatalogMetaLifecycleMutationFencesAreBoundedBeforePublicationV1(t *testing.T) {
	collection := CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "docs"}
	fences := make(map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1, maxVectorPartitionLifecycleMutationFencesV1+1)
	for i := 0; i <= maxVectorPartitionLifecycleMutationFencesV1; i++ {
		fences[vectorPartitionLifecycleServingKeyV1{Collection: collection, IndexName: fmt.Sprintf("idx-%04d", i)}] =
			vectorPartitionLifecycleMutationFenceStateV1{Epoch: 1}
	}
	if _, err := encodeVectorPartitionLifecycleSnapshotV1(nil, fences, nil); !errors.Is(err, ErrVectorPartitionLifecycleLimit) {
		t.Fatalf("oversized mutation-fence snapshot err=%v", err)
	}

	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	active := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, identity, 0, 9)
	active = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	authority.mu.Lock()
	authority.mutationFences = make(map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1, maxVectorPartitionLifecycleMutationFencesV1)
	for i := 0; i < maxVectorPartitionLifecycleMutationFencesV1; i++ {
		authority.mutationFences[vectorPartitionLifecycleServingKeyV1{Collection: collection, IndexName: fmt.Sprintf("idx-%04d", i)}] =
			vectorPartitionLifecycleMutationFenceStateV1{Epoch: 1}
	}
	authority.mu.Unlock()
	invalidate := catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "bounded fence test"
		command.InvalidationEpoch = 10
	})
	if _, err := authority.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, invalidate), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleLimit) {
		t.Fatalf("new mutation fence past cap err=%v", err)
	}
	if retained, ok := authority.VectorPartitionLifecycleRecordV1(identity); !ok || retained.State != VectorPartitionLifecycleActiveV1 || retained.Revision != active.Revision {
		t.Fatalf("fence-cap refusal mutated active record: %+v available=%v", retained, ok)
	}
}

func catalogMetaLifecycleTestAuthorityWithCutoverV1(t *testing.T) (*CatalogMetaAuthorityV1, VectorPartitionLifecycleIdentityV1, VectorPartitionLifecycleIdentityV1) {
	t.Helper()
	authority, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	oldIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 6, 10)
	old := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, oldIdentity, 0, 9)
	old = catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(old, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	newIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	candidate := catalogMetaLifecycleBuildPreparedV1(t, authority, &applied, newIdentity, oldIdentity.Generation, 10)
	catalogMetaLifecycleApplyV1(t, authority, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration = oldIdentity.Generation
		command.PreviousActiveRevision = old.Revision
		command.MutationEpoch = 10
	}))
	return authority, oldIdentity, newIdentity
}

func newCatalogMetaLifecycleTestAuthorityV1(t *testing.T, enabled bool) (*CatalogMetaAuthorityV1, CatalogMetaRecordV1) {
	t.Helper()
	catalog := catalogMetaLifecycleCatalogV1(enabled)
	record, err := NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatalf("catalog record: %v", err)
	}
	raw, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("catalog command: %v", err)
	}
	authority := NewCatalogMetaAuthorityV1()
	if _, err := authority.applyCommittedCatalogMetaV1(raw, 1); err != nil {
		t.Fatalf("install catalog: %v", err)
	}
	return authority, record
}

func catalogMetaLifecycleCatalogV1(enabled bool) CatalogV1 {
	catalog := validCatalog()
	catalog.Features = DefaultFeatureSet()
	if enabled {
		catalog.Features.Required = append(catalog.Features.Required, raftcluster.RequiredFeature{
			Name:    raftcluster.FeatureVectorPartitionLifecycle,
			Version: SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle],
		})
	}
	return catalog
}

func catalogMetaLifecycleTestIdentityV1(catalog CatalogMetaRecordV1, generation, sourceGeneration uint64) VectorPartitionLifecycleIdentityV1 {
	return VectorPartitionLifecycleIdentityV1{
		Index: VectorPartitionLifecycleIndexIdentityV1{
			Collection:            CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"},
			CollectionIncarnation: 3,
			IndexName:             "embedding",
			IndexDefinitionDigest: strings.Repeat("a", 64),
			IndexEpoch:            4,
			CatalogEpoch:          catalog.Epoch,
			CatalogDigest:         catalog.Digest,
		},
		Source: VectorPartitionLifecycleSourceIdentityV1{
			Generation: sourceGeneration,
			Checksum:   sourceGeneration + 1,
			SchemaHash: sourceGeneration + 2,
			RowCount:   100,
		},
		Generation: generation,
	}
}

func catalogMetaLifecycleTestBeginV1(identity VectorPartitionLifecycleIdentityV1, previousGeneration, mutationEpoch uint64) VectorPartitionLifecycleCommandV1 {
	return VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a"},
		PreviousActiveGeneration: previousGeneration, MutationEpoch: mutationEpoch,
	}
}

func catalogMetaLifecycleBuildPreparedV1(
	t *testing.T,
	authority *CatalogMetaAuthorityV1,
	applied *uint64,
	identity VectorPartitionLifecycleIdentityV1,
	previousGeneration uint64,
	mutationEpoch uint64,
) VectorPartitionLifecycleRecordV1 {
	t.Helper()
	record := catalogMetaLifecycleApplyV1(t, authority, applied, catalogMetaLifecycleTestBeginV1(identity, previousGeneration, mutationEpoch))
	record = catalogMetaLifecycleApplyV1(t, authority, applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupReady = VectorPartitionLifecycleGroupReadyV1{
			GroupID: "group-a", AppliedIndex: *applied, AssetSetDigest: strings.Repeat("b", 64),
		}
	}))
	digest, err := VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
	if err != nil {
		t.Fatalf("ready-set digest: %v", err)
	}
	return catalogMetaLifecycleApplyV1(t, authority, applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.ReadySetDigest = digest
	}))
}

func catalogMetaLifecycleApplyV1(
	t *testing.T,
	authority *CatalogMetaAuthorityV1,
	applied *uint64,
	command VectorPartitionLifecycleCommandV1,
) VectorPartitionLifecycleRecordV1 {
	t.Helper()
	raw := mustEncodeCatalogMetaLifecycleCommandV1(t, command)
	*applied = *applied + 1
	if _, err := authority.applyCommittedCatalogMetaV1(raw, *applied); err != nil {
		t.Fatalf("apply %q: %v", command.Kind, err)
	}
	record, ok := authority.VectorPartitionLifecycleRecordV1(command.Identity)
	if !ok {
		t.Fatalf("apply %q did not retain record", command.Kind)
	}
	return record
}

func catalogMetaLifecycleTestCommandV1(
	record VectorPartitionLifecycleRecordV1,
	kind VectorPartitionLifecycleCommandKindV1,
	mutate func(*VectorPartitionLifecycleCommandV1),
) VectorPartitionLifecycleCommandV1 {
	command := VectorPartitionLifecycleCommandV1{
		Kind: kind, ExpectedRevision: record.Revision, ExpectedState: record.State, Identity: record.Identity,
	}
	if mutate != nil {
		mutate(&command)
	}
	return command
}

func catalogMetaLifecycleValidateSearchV1(authority *CatalogMetaAuthorityV1, identity VectorPartitionLifecycleIdentityV1, readySetDigest string) error {
	appliedIndex, ok := authority.CatalogMetaAppliedIndexV1()
	if !ok {
		return ErrCatalogMetaUnavailable
	}
	got, err := authority.ValidateVectorPartitionGenerationSearchAtAppliedIndexV1(
		context.Background(), appliedIndex, identity.Index.Collection, identity.Index.IndexName,
		identity.Generation, identity.Index.IndexDefinitionDigest,
		identity.Source.Generation, identity.Source.Checksum, identity.Source.SchemaHash,
		identity.Source.RowCount,
	)
	if err != nil {
		return err
	}
	if got != readySetDigest {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("ready-set digest mismatch"))
	}
	return nil
}

func mustEncodeCatalogMetaLifecycleCommandV1(t *testing.T, command VectorPartitionLifecycleCommandV1) []byte {
	t.Helper()
	raw, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		t.Fatalf("encode %q: %v", command.Kind, err)
	}
	return raw
}

func mustJSONCatalogMetaLifecycleV1(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
