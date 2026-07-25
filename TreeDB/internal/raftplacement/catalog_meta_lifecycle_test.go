package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	activate := catalogMetaLifecycleTestCommandV1(stale, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.PreviousActiveRevision, c.MutationEpoch = activeIdentity.Generation, active.Revision, 9
	})
	if _, err := restored.applyCommittedCatalogMetaV1(mustEncodeCatalogMetaLifecycleCommandV1(t, activate), applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("stale activation after restore err=%v", err)
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
	freshIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 8, 12)
	fresh := catalogMetaLifecycleBuildPreparedV1(t, restored, &applied, freshIdentity, 0, 10)
	fresh = catalogMetaLifecycleApplyV1(t, restored, &applied, catalogMetaLifecycleTestCommandV1(fresh, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 10
	}))
	if fresh.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("fresh post-fence generation state=%q", fresh.State)
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
	return authority.ValidateVectorPartitionGenerationSearchV1(
		context.Background(), identity.Index.Collection, identity.Index.IndexName,
		identity.Generation, identity.Index.IndexDefinitionDigest,
		identity.Source.Generation, identity.Source.Checksum, identity.Source.SchemaHash,
		identity.Source.RowCount, readySetDigest,
	)
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
