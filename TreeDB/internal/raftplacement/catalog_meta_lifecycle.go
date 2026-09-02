package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	maxVectorPartitionLifecycleSnapshotRecordsV1 = 4096
	maxVectorPartitionLifecycleMutationFencesV1  = 4096
)

type vectorPartitionLifecycleServingKeyV1 struct {
	Collection CollectionRefV1
	IndexName  string
}

type vectorPartitionLifecycleSnapshotV1 struct {
	Format                     uint16                                       `json:"format"`
	Records                    []VectorPartitionLifecycleRecordV1           `json:"records"`
	MutationFences             []vectorPartitionLifecycleMutationFenceV1    `json:"mutation_fences"`
	CollectionMutationBarriers []vectorPartitionCollectionMutationBarrierV1 `json:"collection_mutation_barriers"`
}

// vectorPartitionLifecycleMutationFenceV1 is retained independently of the
// generation records.  Cleanup may remove an invalidated generation, but it
// must never remove the source watermark which prevents an older candidate
// from becoming active after a mutation.
type vectorPartitionLifecycleMutationFenceV1 struct {
	Collection CollectionRefV1 `json:"collection"`
	IndexName  string          `json:"index_name"`
	Epoch      uint64          `json:"epoch"`
	Pending    bool            `json:"pending"`
}

type vectorPartitionLifecycleMutationFenceStateV1 struct {
	Epoch   uint64
	Pending bool
}

// VectorPartitionLifecycleAuthorityStatusV1 is the bounded operator view of
// one replicated generation. Local reader/snapshot/backup pins remain inputs
// to the cleanup command and are not guessed by catalog authority.
type VectorPartitionLifecycleAuthorityStatusV1 struct {
	Identity               VectorPartitionLifecycleIdentityV1
	State                  VectorPartitionLifecycleStateV1
	Revision               uint64
	Active                 bool
	ReadyGroups            int
	RequiredGroups         int
	InvalidationReason     string
	InvalidationEpoch      uint64
	MutationConfirmed      bool
	SupersededByGeneration uint64
	CleanedGroups          int
	RetainedWireBytes      uint64
}

// VectorPartitionLifecycleMutationFenceStatusV1 exposes durable pending
// mutation recovery debt even after a generation record has been cleaned.
type VectorPartitionLifecycleMutationFenceStatusV1 struct {
	Collection CollectionRefV1
	IndexName  string
	Epoch      uint64
	Pending    bool
}

// VectorPartitionServingAuthoritySnapshotV1 is one atomic catalog/lifecycle
// identity captured at an exact applied index. Local serving snapshots bind
// their immutable router and partition assets to this value.
type VectorPartitionServingAuthoritySnapshotV1 struct {
	Catalog  CatalogMetaStatusV1
	Identity VectorPartitionLifecycleIdentityV1
	Record   VectorPartitionLifecycleRecordV1
}

func (a *CatalogMetaAuthorityV1) VectorPartitionLifecycleMutationFencesV1() []VectorPartitionLifecycleMutationFenceStatusV1 {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	statuses := make([]VectorPartitionLifecycleMutationFenceStatusV1, 0, len(a.mutationFences))
	for key, fence := range a.mutationFences {
		statuses = append(statuses, VectorPartitionLifecycleMutationFenceStatusV1{Collection: key.Collection, IndexName: key.IndexName, Epoch: fence.Epoch, Pending: fence.Pending})
	}
	sort.Slice(statuses, func(i, j int) bool {
		a, b := statuses[i], statuses[j]
		if a.Collection.Database != b.Collection.Database {
			return a.Collection.Database < b.Collection.Database
		}
		if a.Collection.Catalog != b.Collection.Catalog {
			return a.Collection.Catalog < b.Collection.Catalog
		}
		if a.Collection.Collection != b.Collection.Collection {
			return a.Collection.Collection < b.Collection.Collection
		}
		return a.IndexName < b.IndexName
	})
	return statuses
}

func vectorPartitionLifecycleCommandBytesV1(raw []byte) bool {
	if len(raw) == 0 || len(raw) > MaxVectorPartitionLifecycleCommandBytesV1 {
		return false
	}
	var envelope struct {
		Kind VectorPartitionLifecycleCommandKindV1 `json:"kind"`
	}
	return json.Unmarshal(raw, &envelope) == nil && envelope.Kind != ""
}

func (a *CatalogMetaAuthorityV1) applyCommittedVectorPartitionLifecycleV1(raw []byte, appliedIndex uint64) (CatalogMetaStatusV1, error) {
	command, err := DecodeVectorPartitionLifecycleCommandV1(raw)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if a == nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaUnavailable
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.record.Epoch == 0 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaUnavailable
	}
	if !catalogMetaFeatureEnabledV1(a.record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
		return CatalogMetaStatusV1{}, errors.Join(ErrUnsupportedFeature, fmt.Errorf("catalog does not require %s", raftcluster.FeatureVectorPartitionLifecycle))
	}
	if command.Identity.Index.CatalogEpoch != a.record.Epoch || command.Identity.Index.CatalogDigest != a.record.Digest {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleIdentity, fmt.Errorf("lifecycle catalog proof does not match current authority"))
	}
	if a.lifecycle == nil {
		a.lifecycle = make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1)
	}
	if a.active == nil {
		a.active = make(map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1)
	}
	if a.activeNames == nil {
		a.activeNames = make(map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1)
	}
	if a.mutationFences == nil {
		a.mutationFences = make(map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1)
	}

	identity := command.Identity
	servingKey := vectorPartitionLifecycleServingKeyV1{Collection: identity.Index.Collection, IndexName: identity.Index.IndexName}
	collectionBarrier := a.collectionMutationBarriers[identity.Index.Collection]
	// MutationEpoch is the immutable source watermark captured by the build.
	// A durable invalidation fence is set before a relevant data entry may be
	// submitted.  Checking both build and activation makes a candidate that
	// raced that entry fail closed, including after restore/replay and after
	// the invalidated record has been cleaned up.
	fence, fenceExists := a.mutationFences[servingKey]
	if command.Kind == VectorPartitionLifecycleInvalidateV1 && !fenceExists &&
		len(a.mutationFences) >= maxVectorPartitionLifecycleMutationFencesV1 {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleLimit,
			fmt.Errorf("mutation fences=%d", len(a.mutationFences)))
	}
	if (command.Kind == VectorPartitionLifecycleBeginBuildV1 || command.Kind == VectorPartitionLifecycleActivateV1) && fence.Pending {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("candidate source is blocked by pending mutation fence %d", fence.Epoch))
	}
	if (command.Kind == VectorPartitionLifecycleBeginBuildV1 || command.Kind == VectorPartitionLifecycleActivateV1) && collectionBarrier.Pending {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("candidate source is blocked by pending collection mutation barrier %d", collectionBarrier.Epoch))
	}
	if (command.Kind == VectorPartitionLifecycleBeginBuildV1 || command.Kind == VectorPartitionLifecycleActivateV1) &&
		command.MutationEpoch < collectionBarrier.Epoch {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard,
			fmt.Errorf("candidate source mutation epoch %d predates collection mutation barrier %d", command.MutationEpoch, collectionBarrier.Epoch))
	}
	if (command.Kind == VectorPartitionLifecycleBeginBuildV1 || command.Kind == VectorPartitionLifecycleActivateV1) &&
		command.MutationEpoch < fence.Epoch {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard,
			fmt.Errorf("candidate source mutation epoch %d predates durable fence %d", command.MutationEpoch, fence.Epoch))
	}
	current := a.lifecycle[identity]
	commandDigest := sha256HexVectorPartitionLifecycleV1(raw)
	if current.LastCommandDigest == commandDigest {
		return a.statusLocked(), nil
	}
	if command.Kind == VectorPartitionLifecycleInvalidateV1 && command.InvalidationEpoch <= fence.Epoch {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard,
			fmt.Errorf("invalidation epoch %d does not advance durable fence %d", command.InvalidationEpoch, fence.Epoch))
	}
	if command.Kind == VectorPartitionLifecycleConfirmMutationV1 && (fence.Epoch != command.MutationEpoch || !fence.Pending) {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("mutation confirmation does not own a pending fence"))
	}
	// A pending fence is the durable recovery debt for a data mutation whose
	// outcome has not yet been confirmed.  Lifecycle cleanup must not discard
	// its invalidated source record: confirmation needs that exact record and
	// proof, including after snapshot/rejoin.  Keep the pure per-record reducer
	// generic; this cross-record safety rule belongs to catalog authority.
	if fence.Pending && current.InvalidationEpoch != 0 {
		switch command.Kind {
		case VectorPartitionLifecycleRetireV1, VectorPartitionLifecycleMarkCleanableV1,
			VectorPartitionLifecycleRecordGroupCleanupV1, VectorPartitionLifecycleCompleteCleanupV1:
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard,
				fmt.Errorf("cleanup is blocked by pending mutation fence %d", fence.Epoch))
		}
	}
	if command.Kind == VectorPartitionLifecycleActivateV1 {
		if activeIdentity, ok := a.activeNames[servingKey]; ok && activeIdentity != identity &&
			(command.PreviousActiveGeneration == 0 || activeIdentity.Index != identity.Index || activeIdentity.Generation != command.PreviousActiveGeneration) {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("serving name is already active at generation %d", activeIdentity.Generation))
		}
	}
	originalLifecycle := cloneVectorPartitionLifecycleRecordsV1(a.lifecycle)
	originalActive := cloneVectorPartitionLifecycleActiveV1(a.active)
	originalActiveNames := cloneVectorPartitionLifecycleActiveNamesV1(a.activeNames)
	originalMutationFences := cloneVectorPartitionLifecycleMutationFencesV1(a.mutationFences)
	if command.Kind == VectorPartitionLifecycleActivateV1 && command.PreviousActiveGeneration != 0 {
		previousIdentity, previous, ok := findVectorPartitionLifecycleGenerationLockedV1(a.lifecycle, identity.Index, command.PreviousActiveGeneration)
		if !ok {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("previous active generation %d is missing", command.PreviousActiveGeneration))
		}
		if previous.LastCommandDigest == commandDigest && current.LastCommandDigest == commandDigest {
			return a.statusLocked(), nil
		}
		activeIdentity, active := a.active[identity.Index]
		if !active || (activeIdentity != previousIdentity && activeIdentity != identity) {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("cutover predecessor is not the sole active generation"))
		}
		retired, activated, err := ApplyVectorPartitionLifecycleCutoverV1(previous, current, command)
		if err != nil {
			return CatalogMetaStatusV1{}, err
		}
		a.lifecycle[previousIdentity] = retired
		a.lifecycle[identity] = activated
		a.setVectorPartitionLifecycleActiveLockedV1(identity)
	} else {
		if command.Kind == VectorPartitionLifecycleActivateV1 {
			if activeIdentity, ok := a.active[identity.Index]; ok && activeIdentity != identity {
				return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("activation requires an atomic cutover from generation %d", activeIdentity.Generation))
			}
		}
		next, err := ApplyVectorPartitionLifecycleCommandV1(current, command)
		if err != nil {
			return CatalogMetaStatusV1{}, err
		}
		a.lifecycle[identity] = next
		switch next.State {
		case VectorPartitionLifecycleActiveV1:
			a.setVectorPartitionLifecycleActiveLockedV1(identity)
		case VectorPartitionLifecycleInvalidatedV1, VectorPartitionLifecycleRetiredV1,
			VectorPartitionLifecycleCleanableV1, VectorPartitionLifecycleAbsentV1:
			a.clearVectorPartitionLifecycleActiveLockedV1(identity)
		}
	}
	if command.Kind == VectorPartitionLifecycleInvalidateV1 {
		a.mutationFences[servingKey] = vectorPartitionLifecycleMutationFenceStateV1{Epoch: command.InvalidationEpoch, Pending: true}
	}
	if command.Kind == VectorPartitionLifecycleConfirmMutationV1 {
		a.mutationFences[servingKey] = vectorPartitionLifecycleMutationFenceStateV1{Epoch: fence.Epoch}
	}
	snapshot, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		a.lifecycle = originalLifecycle
		a.active = originalActive
		a.activeNames = originalActiveNames
		a.mutationFences = originalMutationFences
		return CatalogMetaStatusV1{}, err
	}
	if err := a.validateProspectiveCatalogMetaSnapshotLockedV1(snapshot, appliedIndex); err != nil {
		a.lifecycle = originalLifecycle
		a.active = originalActive
		a.activeNames = originalActiveNames
		a.mutationFences = originalMutationFences
		return CatalogMetaStatusV1{}, err
	}
	a.applied = appliedIndex
	a.lifecycleBytes = uint64(len(snapshot))
	a.refusal = ""
	return a.statusLocked(), nil
}

func (a *CatalogMetaAuthorityV1) validateVectorPartitionLifecycleCatalogTransitionLockedV1() error {
	for collection, barrier := range a.collectionMutationBarriers {
		if barrier.Pending {
			return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("catalog transition blocked by collection %s/%s/%s mutation epoch %d", collection.Database, collection.Catalog, collection.Collection, barrier.Epoch))
		}
	}
	for key, fence := range a.mutationFences {
		if fence.Pending {
			return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("catalog transition blocked by collection %s/%s/%s index %q mutation epoch %d", key.Collection.Database, key.Collection.Catalog, key.Collection.Collection, key.IndexName, fence.Epoch))
		}
	}
	for identity, record := range a.lifecycle {
		if record.State != VectorPartitionLifecycleAbsentV1 {
			return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("catalog transition blocked by generation %d in state %q", identity.Generation, record.State))
		}
	}
	return nil
}

func (a *CatalogMetaAuthorityV1) clearVectorPartitionLifecycleLockedV1() {
	a.lifecycle = nil
	a.active = nil
	a.activeNames = nil
	a.mutationFences = nil
	a.collectionMutationBarriers = nil
	a.lifecycleBytes = 0
}

func (a *CatalogMetaAuthorityV1) setVectorPartitionLifecycleActiveLockedV1(identity VectorPartitionLifecycleIdentityV1) {
	a.active[identity.Index] = identity
	a.activeNames[vectorPartitionLifecycleServingKeyV1{Collection: identity.Index.Collection, IndexName: identity.Index.IndexName}] = identity
}

func (a *CatalogMetaAuthorityV1) clearVectorPartitionLifecycleActiveLockedV1(identity VectorPartitionLifecycleIdentityV1) {
	if a.active[identity.Index] == identity {
		delete(a.active, identity.Index)
	}
	key := vectorPartitionLifecycleServingKeyV1{Collection: identity.Index.Collection, IndexName: identity.Index.IndexName}
	if a.activeNames[key] == identity {
		delete(a.activeNames, key)
	}
}

func findVectorPartitionLifecycleGenerationLockedV1(
	records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	index VectorPartitionLifecycleIndexIdentityV1,
	generation uint64,
) (VectorPartitionLifecycleIdentityV1, VectorPartitionLifecycleRecordV1, bool) {
	for identity, record := range records {
		if identity.Index == index && identity.Generation == generation {
			return identity, record, true
		}
	}
	return VectorPartitionLifecycleIdentityV1{}, VectorPartitionLifecycleRecordV1{}, false
}

func catalogMetaFeatureEnabledV1(features raftcluster.FeatureSet, name raftcluster.FeatureName) bool {
	floor, known := SupportedFeatureFloors[name]
	if !known {
		return false
	}
	for _, required := range features.Required {
		if required.Name == name && required.Version.Major == floor.Major && required.Version.Minor >= floor.Minor {
			return true
		}
	}
	return false
}

// ValidateVectorPartitionGenerationSearchAtAppliedIndexV1 validates against a
// local catalog view that has caught up through requiredAppliedIndex. The
// required index must come from a fresh linearizable meta-Raft read fence; the
// local applied index by itself is not proof of quorum freshness.
//
// This deliberately does not implement nativewire's serving interface. That
// prevents a replica-local CatalogMetaAuthorityV1 from being wired directly
// into search without the linearizable fence adapter.
func (a *CatalogMetaAuthorityV1) ValidateVectorPartitionGenerationSearchAtAppliedIndexV1(
	ctx context.Context,
	requiredAppliedIndex uint64,
	collection CollectionRefV1,
	index string,
	generation uint64,
	indexDefinitionDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	sourceRowCount uint64,
) (string, error) {
	snapshot, err := a.vectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		ctx, requiredAppliedIndex, false, collection, index, generation, indexDefinitionDigest,
		sourceGeneration, sourceChecksum, sourceSchemaHash, sourceRowCount,
	)
	if err != nil {
		return "", err
	}
	return snapshot.Record.ReadySetDigest, nil
}

// VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1 captures the exact
// catalog and lifecycle identity installed at requiredAppliedIndex. It rejects
// both lagging and newer views so publication cannot mix generations.
func (a *CatalogMetaAuthorityV1) VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
	ctx context.Context,
	requiredAppliedIndex uint64,
	collection CollectionRefV1,
	index string,
	generation uint64,
	indexDefinitionDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	sourceRowCount uint64,
) (VectorPartitionServingAuthoritySnapshotV1, error) {
	return a.vectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
		ctx, requiredAppliedIndex, true, collection, index, generation, indexDefinitionDigest,
		sourceGeneration, sourceChecksum, sourceSchemaHash, sourceRowCount,
	)
}

// ValidateVectorPartitionServingAuthoritySnapshotAtAppliedIndexV1 validates a
// retained serving identity against the exact local applied view without
// cloning the lifecycle record. The required index must come from a retained
// linearizable read proof.
func (a *CatalogMetaAuthorityV1) ValidateVectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
	ctx context.Context,
	requiredAppliedIndex uint64,
	expected VectorPartitionServingAuthoritySnapshotV1,
) error {
	if a == nil {
		return ErrCatalogMetaUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	retainedWireBytes := uint64(len(a.recordBytes)+len(a.command)) + a.lifecycleBytes
	if requiredAppliedIndex == 0 || a.applied != requiredAppliedIndex || expected.Catalog.AppliedIndex != requiredAppliedIndex ||
		a.record.Epoch != expected.Catalog.Epoch || a.record.Digest != expected.Catalog.Digest ||
		a.record.Catalog.Features.ConfigVersion != expected.Catalog.Features.ConfigVersion ||
		!slices.Equal(a.record.Catalog.Features.Required, expected.Catalog.Features.Required) ||
		retainedWireBytes != expected.Catalog.RetainedWireBytes || a.refusal != expected.Catalog.Refusal {
		return errors.Join(ErrCatalogMetaUnavailable, fmt.Errorf("catalog serving authority changed"))
	}
	identity, ok := a.activeNames[vectorPartitionLifecycleServingKeyV1{
		Collection: expected.Identity.Index.Collection,
		IndexName:  expected.Identity.Index.IndexName,
	}]
	if !ok || identity != expected.Identity {
		return ErrVectorPartitionLifecycleIdentity
	}
	record, ok := a.lifecycle[identity]
	if !ok || !equalVectorPartitionLifecycleRecordV1(record, expected.Record) {
		return ErrVectorPartitionLifecycleIdentity
	}
	return record.CanSearch(VectorPartitionLifecycleSearchProofV1{
		Identity: identity, ReadySetDigest: expected.Record.ReadySetDigest,
	})
}

func (a *CatalogMetaAuthorityV1) vectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
	ctx context.Context,
	requiredAppliedIndex uint64,
	requireExact bool,
	collection CollectionRefV1,
	index string,
	generation uint64,
	indexDefinitionDigest string,
	sourceGeneration uint64,
	sourceChecksum uint64,
	sourceSchemaHash uint64,
	sourceRowCount uint64,
) (VectorPartitionServingAuthoritySnapshotV1, error) {
	if a == nil {
		return VectorPartitionServingAuthoritySnapshotV1{}, ErrCatalogMetaUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionServingAuthoritySnapshotV1{}, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if requiredAppliedIndex == 0 || a.applied < requiredAppliedIndex || requireExact && a.applied != requiredAppliedIndex {
		return VectorPartitionServingAuthoritySnapshotV1{}, errors.Join(ErrCatalogMetaUnavailable, fmt.Errorf("catalog applied index %d does not satisfy required linearizable index %d", a.applied, requiredAppliedIndex))
	}
	identity, ok := a.activeNames[vectorPartitionLifecycleServingKeyV1{Collection: collection, IndexName: index}]
	if !ok {
		return VectorPartitionServingAuthoritySnapshotV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("no active generation"))
	}
	record, ok := a.lifecycle[identity]
	if !ok || identity.Generation != generation || identity.Index.IndexDefinitionDigest != indexDefinitionDigest ||
		identity.Source.Generation != sourceGeneration || identity.Source.Checksum != sourceChecksum ||
		identity.Source.SchemaHash != sourceSchemaHash || identity.Source.RowCount != sourceRowCount {
		return VectorPartitionServingAuthoritySnapshotV1{}, ErrVectorPartitionLifecycleIdentity
	}
	if err := record.CanSearch(VectorPartitionLifecycleSearchProofV1{Identity: identity, ReadySetDigest: record.ReadySetDigest}); err != nil {
		return VectorPartitionServingAuthoritySnapshotV1{}, err
	}
	return VectorPartitionServingAuthoritySnapshotV1{
		Catalog: a.statusLocked(), Identity: identity, Record: cloneVectorPartitionLifecycleRecordV1(record),
	}, nil
}

func (a *CatalogMetaAuthorityV1) VectorPartitionLifecycleRecordV1(identity VectorPartitionLifecycleIdentityV1) (VectorPartitionLifecycleRecordV1, bool) {
	if a == nil {
		return VectorPartitionLifecycleRecordV1{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	record, ok := a.lifecycle[identity]
	return cloneVectorPartitionLifecycleRecordV1(record), ok
}

func (a *CatalogMetaAuthorityV1) VectorPartitionLifecycleStatusesV1() []VectorPartitionLifecycleAuthorityStatusV1 {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	statuses := make([]VectorPartitionLifecycleAuthorityStatusV1, 0, len(a.lifecycle))
	for identity, record := range a.lifecycle {
		raw, _ := EncodeVectorPartitionLifecycleRecordV1(record)
		statuses = append(statuses, VectorPartitionLifecycleAuthorityStatusV1{
			Identity: identity, State: record.State, Revision: record.Revision,
			Active:      a.active[identity.Index] == identity,
			ReadyGroups: len(record.ReadyGroups), RequiredGroups: len(record.RequiredGroups),
			InvalidationReason: record.InvalidationReason, InvalidationEpoch: record.InvalidationEpoch,
			MutationConfirmed:      record.MutationConfirmed,
			SupersededByGeneration: record.SupersededByGeneration,
			CleanedGroups:          len(record.CleanedGroups), RetainedWireBytes: uint64(len(raw)),
		})
	}
	sort.Slice(statuses, func(i, j int) bool {
		return vectorPartitionLifecycleIdentityLessV1(statuses[i].Identity, statuses[j].Identity)
	})
	return statuses
}

func encodeVectorPartitionLifecycleSnapshotV1(records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1, barriers map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1) ([]byte, error) {
	if len(records) == 0 && len(fences) == 0 && len(barriers) == 0 {
		return nil, nil
	}
	if len(records) > maxVectorPartitionLifecycleSnapshotRecordsV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("snapshot records=%d", len(records)))
	}
	if len(fences) > maxVectorPartitionLifecycleMutationFencesV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("snapshot mutation fences=%d", len(fences)))
	}
	if len(barriers) > maxVectorPartitionCollectionMutationBarriersV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("snapshot collection mutation barriers=%d", len(barriers)))
	}
	payload := vectorPartitionLifecycleSnapshotV1{Format: VectorPartitionLifecycleFormatV1, Records: make([]VectorPartitionLifecycleRecordV1, 0, len(records)), MutationFences: make([]vectorPartitionLifecycleMutationFenceV1, 0, len(fences)), CollectionMutationBarriers: make([]vectorPartitionCollectionMutationBarrierV1, 0, len(barriers))}
	for _, record := range records {
		raw, err := EncodeVectorPartitionLifecycleRecordV1(record)
		if err != nil {
			return nil, err
		}
		canonical, err := DecodeVectorPartitionLifecycleRecordV1(raw)
		if err != nil {
			return nil, err
		}
		payload.Records = append(payload.Records, canonical)
	}
	sort.Slice(payload.Records, func(i, j int) bool {
		return vectorPartitionLifecycleIdentityLessV1(payload.Records[i].Identity, payload.Records[j].Identity)
	})
	for key, fence := range fences {
		if err := validateCollectionRef(key.Collection); err != nil ||
			validateVectorPartitionLifecycleNameV1("index name", key.IndexName) != nil || fence.Epoch == 0 {
			return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid mutation fence"))
		}
		payload.MutationFences = append(payload.MutationFences, vectorPartitionLifecycleMutationFenceV1{Collection: key.Collection, IndexName: key.IndexName, Epoch: fence.Epoch, Pending: fence.Pending})
	}
	sort.Slice(payload.MutationFences, func(i, j int) bool {
		a, b := payload.MutationFences[i], payload.MutationFences[j]
		if a.Collection.Database != b.Collection.Database {
			return a.Collection.Database < b.Collection.Database
		}
		if a.Collection.Catalog != b.Collection.Catalog {
			return a.Collection.Catalog < b.Collection.Catalog
		}
		if a.Collection.Collection != b.Collection.Collection {
			return a.Collection.Collection < b.Collection.Collection
		}
		return a.IndexName < b.IndexName
	})
	for collection, barrier := range barriers {
		if err := validateCollectionRef(collection); err != nil || validateVectorPartitionCollectionMutationBarrierStateV1(barrier) != nil {
			return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid collection mutation barrier"))
		}
		payload.CollectionMutationBarriers = append(payload.CollectionMutationBarriers, vectorPartitionCollectionMutationBarrierV1{Collection: collection, Epoch: barrier.Epoch, Pending: barrier.Pending, OperationDigest: barrier.OperationDigest, Completed: append([]vectorPartitionCollectionCompletedMutationV1(nil), barrier.Completed...)})
	}
	sort.Slice(payload.CollectionMutationBarriers, func(i, j int) bool {
		a, b := payload.CollectionMutationBarriers[i].Collection, payload.CollectionMutationBarriers[j].Collection
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Catalog != b.Catalog {
			return a.Catalog < b.Catalog
		}
		return a.Collection < b.Collection
	})
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if len(raw) > MaxCatalogMetaSnapshotBytesV1 {
		return nil, errors.Join(ErrCatalogMetaLimit, fmt.Errorf("lifecycle snapshot is %d bytes", len(raw)))
	}
	return raw, nil
}

func decodeVectorPartitionLifecycleSnapshotV1(raw []byte, catalog CatalogMetaRecordV1) (
	map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1,
	map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1,
	map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1,
	map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1,
	error,
) {
	records := make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1)
	active := make(map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1)
	activeNames := make(map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1)
	fences := make(map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1)
	barriers := make(map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1)
	if len(raw) == 0 {
		return records, active, activeNames, fences, barriers, nil
	}
	if len(raw) > MaxCatalogMetaSnapshotBytesV1 {
		return nil, nil, nil, nil, nil, ErrCatalogMetaLimit
	}
	var payload vectorPartitionLifecycleSnapshotV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("trailing lifecycle snapshot data"))
	}
	if payload.Format != VectorPartitionLifecycleFormatV1 ||
		len(payload.Records) > maxVectorPartitionLifecycleSnapshotRecordsV1 ||
		len(payload.MutationFences) > maxVectorPartitionLifecycleMutationFencesV1 ||
		len(payload.CollectionMutationBarriers) > maxVectorPartitionCollectionMutationBarriersV1 {
		return nil, nil, nil, nil, nil, ErrVectorPartitionLifecycleLimit
	}
	for _, record := range payload.Records {
		encoded, err := EncodeVectorPartitionLifecycleRecordV1(record)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		record, err = DecodeVectorPartitionLifecycleRecordV1(encoded)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		identity := record.Identity
		if identity.Index.CatalogEpoch != catalog.Epoch || identity.Index.CatalogDigest != catalog.Digest {
			return nil, nil, nil, nil, nil, ErrVectorPartitionLifecycleIdentity
		}
		if _, duplicate := records[identity]; duplicate {
			return nil, nil, nil, nil, nil, ErrVectorPartitionLifecycleConflict
		}
		records[identity] = record
		if record.State == VectorPartitionLifecycleActiveV1 {
			if _, duplicate := active[identity.Index]; duplicate {
				return nil, nil, nil, nil, nil, errors.Join(ErrVectorPartitionLifecycleConflict, fmt.Errorf("multiple active generations for one index"))
			}
			servingKey := vectorPartitionLifecycleServingKeyV1{Collection: identity.Index.Collection, IndexName: identity.Index.IndexName}
			if _, duplicate := activeNames[servingKey]; duplicate {
				return nil, nil, nil, nil, nil, errors.Join(ErrVectorPartitionLifecycleConflict, fmt.Errorf("multiple active generations for one serving name"))
			}
			active[identity.Index] = identity
			activeNames[servingKey] = identity
		}
	}
	for _, fence := range payload.MutationFences {
		if err := validateCollectionRef(fence.Collection); err != nil ||
			validateVectorPartitionLifecycleNameV1("index name", fence.IndexName) != nil || fence.Epoch == 0 {
			return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid mutation fence"))
		}
		key := vectorPartitionLifecycleServingKeyV1{Collection: fence.Collection, IndexName: fence.IndexName}
		if _, duplicate := fences[key]; duplicate {
			return nil, nil, nil, nil, nil, ErrVectorPartitionLifecycleConflict
		}
		fences[key] = vectorPartitionLifecycleMutationFenceStateV1{Epoch: fence.Epoch, Pending: fence.Pending}
	}
	pendingMatches := make(map[vectorPartitionLifecycleServingKeyV1]int)
	for _, record := range records {
		key := vectorPartitionLifecycleServingKeyV1{Collection: record.Identity.Index.Collection, IndexName: record.Identity.Index.IndexName}
		fence := fences[key]
		if fence.Pending && record.State == VectorPartitionLifecycleInvalidatedV1 &&
			record.InvalidationEpoch == fence.Epoch && !record.MutationConfirmed {
			pendingMatches[key]++
		}
	}
	for key, fence := range fences {
		if !fence.Pending {
			continue
		}
		if _, serving := activeNames[key]; serving {
			return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle,
				fmt.Errorf("pending mutation fence %d accompanies an active generation", fence.Epoch))
		}
		matching := pendingMatches[key]
		if matching != 1 {
			return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle,
				fmt.Errorf("pending mutation fence %d has %d matching invalidated generation records", fence.Epoch, matching))
		}
	}
	for _, barrier := range payload.CollectionMutationBarriers {
		state := vectorPartitionCollectionMutationBarrierStateV1{Epoch: barrier.Epoch, Pending: barrier.Pending, OperationDigest: barrier.OperationDigest, Completed: append([]vectorPartitionCollectionCompletedMutationV1(nil), barrier.Completed...)}
		if err := validateCollectionRef(barrier.Collection); err != nil || validateVectorPartitionCollectionMutationBarrierStateV1(state) != nil {
			return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid collection mutation barrier"))
		}
		if _, duplicate := barriers[barrier.Collection]; duplicate {
			return nil, nil, nil, nil, nil, ErrVectorPartitionLifecycleConflict
		}
		barriers[barrier.Collection] = state
	}
	canonical, err := encodeVectorPartitionLifecycleSnapshotV1(records, fences, barriers)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if !bytes.Equal(raw, canonical) {
		return nil, nil, nil, nil, nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("lifecycle snapshot is not canonical"))
	}
	return records, active, activeNames, fences, barriers, nil
}

func vectorPartitionLifecycleIdentityLessV1(a, b VectorPartitionLifecycleIdentityV1) bool {
	if a.Index.Collection.Database != b.Index.Collection.Database {
		return a.Index.Collection.Database < b.Index.Collection.Database
	}
	if a.Index.Collection.Catalog != b.Index.Collection.Catalog {
		return a.Index.Collection.Catalog < b.Index.Collection.Catalog
	}
	if a.Index.Collection.Collection != b.Index.Collection.Collection {
		return a.Index.Collection.Collection < b.Index.Collection.Collection
	}
	if a.Index.IndexName != b.Index.IndexName {
		return a.Index.IndexName < b.Index.IndexName
	}
	if a.Index.CollectionIncarnation != b.Index.CollectionIncarnation {
		return a.Index.CollectionIncarnation < b.Index.CollectionIncarnation
	}
	if a.Index.IndexEpoch != b.Index.IndexEpoch {
		return a.Index.IndexEpoch < b.Index.IndexEpoch
	}
	if a.Index.CatalogEpoch != b.Index.CatalogEpoch {
		return a.Index.CatalogEpoch < b.Index.CatalogEpoch
	}
	if a.Index.IndexDefinitionDigest != b.Index.IndexDefinitionDigest {
		return a.Index.IndexDefinitionDigest < b.Index.IndexDefinitionDigest
	}
	if a.Index.CatalogDigest != b.Index.CatalogDigest {
		return a.Index.CatalogDigest < b.Index.CatalogDigest
	}
	if a.Generation != b.Generation {
		return a.Generation < b.Generation
	}
	if a.Source.Generation != b.Source.Generation {
		return a.Source.Generation < b.Source.Generation
	}
	if a.Source.Checksum != b.Source.Checksum {
		return a.Source.Checksum < b.Source.Checksum
	}
	if a.Source.SchemaHash != b.Source.SchemaHash {
		return a.Source.SchemaHash < b.Source.SchemaHash
	}
	return a.Source.RowCount < b.Source.RowCount
}

func cloneVectorPartitionLifecycleRecordsV1(source map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1) map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1 {
	clone := make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, len(source))
	for key, value := range source {
		clone[key] = cloneVectorPartitionLifecycleRecordV1(value)
	}
	return clone
}

func cloneVectorPartitionLifecycleRecordV1(record VectorPartitionLifecycleRecordV1) VectorPartitionLifecycleRecordV1 {
	record.RequiredGroups = slices.Clone(record.RequiredGroups)
	record.ReadyGroups = slices.Clone(record.ReadyGroups)
	record.CleanedGroups = slices.Clone(record.CleanedGroups)
	return record
}

func equalVectorPartitionLifecycleRecordV1(a, b VectorPartitionLifecycleRecordV1) bool {
	return a.Format == b.Format && a.Revision == b.Revision && a.State == b.State && a.Identity == b.Identity &&
		a.PreviousActiveGeneration == b.PreviousActiveGeneration && a.MutationEpoch == b.MutationEpoch &&
		slices.Equal(a.RequiredGroups, b.RequiredGroups) && slices.Equal(a.ReadyGroups, b.ReadyGroups) &&
		a.ReadySetDigest == b.ReadySetDigest && a.InvalidationReason == b.InvalidationReason &&
		a.InvalidationEpoch == b.InvalidationEpoch && a.MutationConfirmed == b.MutationConfirmed &&
		a.Aborted == b.Aborted && a.RetirementReason == b.RetirementReason &&
		a.SupersededByGeneration == b.SupersededByGeneration && slices.Equal(a.CleanedGroups, b.CleanedGroups) &&
		a.CleanupComplete == b.CleanupComplete && a.LastCommandDigest == b.LastCommandDigest
}

func cloneVectorPartitionLifecycleActiveV1(source map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1) map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1 {
	clone := make(map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneVectorPartitionLifecycleActiveNamesV1(source map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1) map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1 {
	clone := make(map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneVectorPartitionLifecycleMutationFencesV1(source map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1) map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1 {
	clone := make(map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
