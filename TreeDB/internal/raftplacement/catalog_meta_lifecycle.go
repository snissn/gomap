package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
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

	if command.Kind == VectorPartitionLifecycleBeginBuildV1 && command.Identity.SourceFormat == 2 {
		if err := validateCatalogSourceOwnersV2(a.record.Catalog, command.SourceOwners); err != nil {
			return CatalogMetaStatusV1{}, err
		}
	}
	if command.Kind == VectorPartitionLifecycleBeginBuildV1 {
		for existing := range a.lifecycle {
			if (existing.SourceFormat == 2 || command.Identity.SourceFormat == 2 ||
				existing.Immutable != (VectorPartitionLifecycleImmutableAuthorityV1{}) || command.Identity.Immutable != (VectorPartitionLifecycleImmutableAuthorityV1{})) &&
				existing.Index == command.Identity.Index && existing.Generation == command.Identity.Generation && existing != command.Identity {
				return CatalogMetaStatusV1{}, ErrVectorPartitionLifecycleConflict
			}
		}
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

// Replica replacement changes only one group's roster. An immutable ACTIVE
// generation may retain its source and READY evidence across that catalog
// change; ordinary catalog publication still uses the stricter guard above.
func (a *CatalogMetaAuthorityV1) validateReplicaReplacementLifecycleLockedV1(group raftcluster.GroupID) error {
	if !catalogMetaFeatureEnabledV1(a.record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
		return a.validateVectorPartitionLifecycleCatalogTransitionLockedV1()
	}
	for _, barrier := range a.collectionMutationBarriers {
		if barrier.Pending {
			return ErrVectorPartitionLifecycleGuard
		}
	}
	for _, fence := range a.mutationFences {
		if fence.Pending {
			return ErrVectorPartitionLifecycleGuard
		}
	}
	active := false
	for identity, record := range a.lifecycle {
		if record.Identity != identity {
			return ErrVectorPartitionLifecycleIdentity
		}
		if record.State == VectorPartitionLifecycleAbsentV1 {
			continue
		}
		if record.State != VectorPartitionLifecycleActiveV1 || identity.Immutable == (VectorPartitionLifecycleImmutableAuthorityV1{}) {
			return ErrVectorPartitionLifecycleGuard
		}
		placement, ok := a.resolved.Placement(identity.Index.Collection)
		if !ok || placement.GroupID == group {
			return ErrVectorPartitionLifecycleGuard
		}
		for _, partition := range placement.TokenPartitions {
			if partition.GroupID == group {
				return ErrVectorPartitionLifecycleGuard
			}
		}
		active = true
	}
	if !active {
		return errors.Join(ErrUnsupportedFeature, fmt.Errorf("lifecycle-bearing replica replacement requires an immutable ACTIVE generation"))
	}
	return nil
}

// Snapshot restore may compact several committed commands. A replacement
// operation absent from the local authority still needs the same lifecycle
// admission as a committed BEGIN, even when the catalog epoch is unchanged.
func (a *CatalogMetaAuthorityV1) validateReplicaReplacementLifecycleSnapshotAddsLockedV1(replacements map[raftcluster.GroupID][]byte) error {
	for group, raw := range replacements {
		next, err := decodeReplicaReplacementCurrentV1(raw)
		if err != nil {
			return err
		}
		if oldRaw := a.replacements[group]; len(oldRaw) != 0 {
			old, err := decodeReplicaReplacementCurrentV1(oldRaw)
			if err != nil {
				return err
			}
			if sameReplicaReplacementBeginV1(old.Begin, next.Begin) {
				continue
			}
		}
		if err := a.validateReplicaReplacementLifecycleLockedV1(group); err != nil {
			return err
		}
	}
	return nil
}

func (a *CatalogMetaAuthorityV1) rebindReplicaReplacementLifecycleLockedV1(next CatalogMetaRecordV1) (
	map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1,
	map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1,
	[]byte, error,
) {
	if len(a.lifecycle) == 0 {
		raw, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
		return a.lifecycle, a.active, a.activeNames, raw, err
	}
	records := make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, len(a.lifecycle))
	for _, original := range a.lifecycle {
		record := cloneVectorPartitionLifecycleRecordV1(original)
		record.Identity.Index.CatalogEpoch = next.Epoch
		record.Identity.Index.CatalogDigest = next.Digest
		if record.State == VectorPartitionLifecycleActiveV1 {
			var err error
			record.ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
			if err != nil {
				return nil, nil, nil, nil, err
			}
		}
		if _, duplicate := records[record.Identity]; duplicate {
			return nil, nil, nil, nil, ErrVectorPartitionLifecycleConflict
		}
		records[record.Identity] = record
	}
	raw, err := encodeVectorPartitionLifecycleSnapshotV1(records, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	validated, active, activeNames, _, _, err := decodeVectorPartitionLifecycleSnapshotV1(raw, next)
	return validated, active, activeNames, raw, err
}

func (a *CatalogMetaAuthorityV1) validateReplicaReplacementLifecycleSnapshotTransitionLockedV1(
	next CatalogMetaRecordV1, resolved ResolvedCatalogV1, replacements map[raftcluster.GroupID][]byte,
	records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1,
	barriers map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1,
) error {
	if !catalogMetaFeatureEnabledV1(a.record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
		return a.validateVectorPartitionLifecycleCatalogTransitionLockedV1()
	}
	active := false
	for _, record := range a.lifecycle {
		active = active || record.State == VectorPartitionLifecycleActiveV1
	}
	if !active {
		return a.validateVectorPartitionLifecycleCatalogTransitionLockedV1()
	}
	// The ordinary topology guard permits additions. A compacted replacement
	// snapshot cannot establish whether an added route preceded an ACTIVE
	// generation, so retain the exact old placement and group inventory.
	if len(resolved.Groups) != len(a.resolved.Groups) || len(resolved.Placements) != len(a.resolved.Placements) ||
		!reflect.DeepEqual(a.record.Catalog.Features, next.Catalog.Features) ||
		!reflect.DeepEqual(a.record.Catalog.Placements, next.Catalog.Placements) {
		return ErrVectorPartitionLifecycleGuard
	}
	finalReplacement := false
	for _, old := range a.resolved.Groups {
		current := resolved.groups[old.ID]
		rosterChanged := !equalCatalogMetaMembersV1(old.Members, current.Members)
		if !rosterChanged {
			if old.LeaderHint != current.LeaderHint {
				state, err := decodeReplicaReplacementCurrentV1(replacements[old.ID])
				if err != nil || current.LeaderHint != "" ||
					!equalCatalogMetaMembersV1(replicaReplacementPeerIDsV1(state.Peers), current.Members) {
					return ErrCatalogMetaConflict
				}
				if state.Phase == ReplicaReplacementCompletedV1 {
					if state.Result == nil || state.Result.Epoch <= a.record.Epoch || state.Result.Epoch > next.Epoch {
						return ErrCatalogMetaConflict
					}
				} else if state.Begin.ExpectedEpoch != next.Epoch || state.Begin.CatalogDigest != next.Digest {
					return ErrCatalogMetaConflict
				}
				if err := a.validateReplicaReplacementLifecycleLockedV1(old.ID); err != nil {
					return err
				}
			}
			continue
		}
		if current.LeaderHint != old.LeaderHint &&
			(current.LeaderHint != "" || slices.Contains(current.Members, old.LeaderHint)) {
			return ErrCatalogMetaConflict
		}
		state, err := decodeReplicaReplacementCurrentV1(replacements[old.ID])
		if err != nil || !equalCatalogMetaMembersV1(replicaReplacementPeerIDsV1(state.Peers), current.Members) {
			return ErrCatalogMetaConflict
		}
		if err := a.validateReplicaReplacementLifecycleLockedV1(old.ID); err != nil {
			return err
		}
		if state.Phase == ReplicaReplacementCompletedV1 {
			if state.Result == nil || state.Result.Epoch <= a.record.Epoch || state.Result.Epoch > next.Epoch {
				return ErrCatalogMetaConflict
			}
		} else if state.Begin.ExpectedEpoch != next.Epoch || state.Begin.CatalogDigest != next.Digest {
			return ErrCatalogMetaConflict
		}
	}
	for group, raw := range replacements {
		state, err := decodeReplicaReplacementCurrentV1(raw)
		if err != nil {
			return err
		}
		if state.Phase == ReplicaReplacementCompletedV1 {
			if state.Result == nil || state.Result.Epoch <= a.record.Epoch {
				continue
			}
			if err := a.validateReplicaReplacementLifecycleLockedV1(group); err != nil {
				return err
			}
			if state.Result.Epoch == next.Epoch && state.Result.Digest == next.Digest {
				finalReplacement = true
			}
			continue
		}
		if state.Begin.ExpectedEpoch <= a.record.Epoch {
			continue
		}
		if err := a.validateReplicaReplacementLifecycleLockedV1(group); err != nil {
			return err
		}
		if state.Begin.ExpectedEpoch == next.Epoch && state.Begin.CatalogDigest == next.Digest && len(state.Peers) != 0 &&
			equalCatalogMetaMembersV1(replicaReplacementPeerIDsV1(state.Peers), resolved.groups[group].Members) {
			finalReplacement = true
		}
	}
	if !finalReplacement {
		return ErrVectorPartitionLifecycleGuard
	}
	// Current-state replacement successors and the final rosters are checked by
	// the snapshot installer. History may be compacted: the follower need not
	// have seen BEGIN, nor every intervening completed catalog epoch.
	expected, _, _, _, err := a.rebindReplicaReplacementLifecycleLockedV1(next)
	if err != nil {
		return err
	}
	return a.validateVectorPartitionLifecycleSnapshotEvidenceLockedV1(expected, records, fences, barriers)
}

func (a *CatalogMetaAuthorityV1) validateVectorPartitionLifecycleSnapshotEvidenceLockedV1(
	expected, records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1,
	barriers map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1,
) error {
	for identity, old := range expected {
		incoming, ok := records[identity]
		if !ok || !replicaReplacementLifecycleRecordSnapshotSuccessorV1(old, incoming, records, fences) {
			return ErrVectorPartitionLifecycleConflict
		}
	}
	for identity, incoming := range records {
		if _, exists := expected[identity]; exists {
			continue
		}
		switch incoming.State {
		case VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1, VectorPartitionLifecyclePreparedV1:
			if !replicaReplacementNewLifecyclePreparationSnapshotV1(incoming) {
				return ErrVectorPartitionLifecycleConflict
			}
		case VectorPartitionLifecycleActiveV1:
			if !a.replicaReplacementNewLifecycleActiveSnapshotV1(incoming, expected, records, fences) {
				return ErrVectorPartitionLifecycleConflict
			}
		}
	}
	for key, old := range a.mutationFences {
		incoming, ok := fences[key]
		if !ok || incoming.Epoch < old.Epoch || incoming.Epoch == old.Epoch && !old.Pending && incoming.Pending {
			return ErrVectorPartitionLifecycleConflict
		}
	}
	for collection, old := range a.collectionMutationBarriers {
		incoming, ok := barriers[collection]
		if !ok || incoming.Epoch < old.Epoch || incoming.Epoch == old.Epoch &&
			(old.OperationDigest != incoming.OperationDigest || !old.Pending && incoming.Pending) ||
			!vectorPartitionCollectionMutationBarrierSnapshotSuccessorV1(old, incoming) {
			return ErrVectorPartitionLifecycleConflict
		}
	}
	return nil
}

// A new ACTIVE generation has no local record to compare against. Reconstruct
// its reducer preparation and activation command at this catalog identity.
// A direct cutover must also carry the same READY-bound command digest on the
// previously ACTIVE local record. Older cleaned cutovers have overwritten that
// digest, so their separate terminal-successor check remains the authority.
func (a *CatalogMetaAuthorityV1) replicaReplacementNewLifecycleActiveSnapshotV1(
	record VectorPartitionLifecycleRecordV1,
	expected, records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1,
) bool {
	key := vectorPartitionLifecycleServingKeyV1{Collection: record.Identity.Index.Collection, IndexName: record.Identity.Index.IndexName}
	if fence := fences[key]; fence.Pending || record.MutationEpoch < fence.Epoch {
		return false
	}
	if barrier := a.collectionMutationBarriers[record.Identity.Index.Collection]; barrier.Pending || record.MutationEpoch < barrier.Epoch {
		return false
	}
	begin := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: record.Identity, RequiredGroups: record.RequiredGroups,
		PreviousActiveGeneration: record.PreviousActiveGeneration, MutationEpoch: record.MutationEpoch,
		SourceOwners: record.SourceOwners, ANNOwners: record.ANNOwners,
	}
	current, err := ApplyVectorPartitionLifecycleCommandV1(VectorPartitionLifecycleRecordV1{}, begin)
	if err != nil {
		return false
	}
	for _, ready := range record.ReadyGroups {
		current, err = ApplyVectorPartitionLifecycleCommandV1(current, VectorPartitionLifecycleCommandV1{
			Kind: VectorPartitionLifecycleRecordGroupReadyV1, ExpectedRevision: current.Revision,
			ExpectedState: current.State, Identity: record.Identity, GroupReady: ready,
		})
		if err != nil {
			return false
		}
	}
	current, err = ApplyVectorPartitionLifecycleCommandV1(current, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecyclePrepareV1, ExpectedRevision: current.Revision,
		ExpectedState: current.State, Identity: record.Identity, ReadySetDigest: record.ReadySetDigest,
	})
	if err != nil || record.Revision != current.Revision+1 {
		return false
	}
	// The cutover command's previous revision is available only when the
	// predecessor remains the locally known ACTIVE generation.
	var previousRevision uint64
	if record.PreviousActiveGeneration != 0 {
		_, incomingPrevious, found := findVectorPartitionLifecycleGenerationLockedV1(records, record.Identity.Index, record.PreviousActiveGeneration)
		if !found || incomingPrevious.SupersededByGeneration != record.Identity.Generation ||
			(incomingPrevious.State != VectorPartitionLifecycleRetiredV1 && incomingPrevious.State != VectorPartitionLifecycleCleanableV1 &&
				incomingPrevious.State != VectorPartitionLifecycleAbsentV1) {
			return false
		}
		previousIdentity, previous, found := findVectorPartitionLifecycleGenerationLockedV1(expected, record.Identity.Index, record.PreviousActiveGeneration)
		if found && previous.State == VectorPartitionLifecycleActiveV1 {
			previousRevision = previous.Revision
			currentPrevious, ok := records[previousIdentity]
			if !ok || currentPrevious.State != VectorPartitionLifecycleRetiredV1 || currentPrevious.Revision != previous.Revision+1 {
				previousRevision = 0
			}
		}
	}
	if record.PreviousActiveGeneration != 0 && previousRevision == 0 {
		// A later compacted cutover can leave only terminal predecessors. Its
		// provenance remains bounded by the existing terminal successor check
		// for a locally known ACTIVE generation.
		for _, old := range expected {
			if old.Identity.Index == record.Identity.Index && old.State == VectorPartitionLifecycleActiveV1 {
				return true
			}
		}
		return false
	}
	command := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleActivateV1, ExpectedRevision: current.Revision,
		ExpectedState: current.State, Identity: record.Identity,
		PreviousActiveGeneration: record.PreviousActiveGeneration,
		PreviousActiveRevision:   previousRevision, MutationEpoch: record.MutationEpoch,
		ReadySetDigest: record.ReadySetDigest,
	}
	raw, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil || sha256HexVectorPartitionLifecycleV1(raw) != record.LastCommandDigest {
		return false
	}
	if previousRevision != 0 {
		previousIdentity, _, _ := findVectorPartitionLifecycleGenerationLockedV1(expected, record.Identity.Index, record.PreviousActiveGeneration)
		return records[previousIdentity].LastCommandDigest == record.LastCommandDigest
	}
	return true
}

// A snapshot can compact the commands that created a new candidate. Rebuild
// its preparation through the pure reducer to reject records that no command
// sequence could produce. Replacement completion excludes non-ACTIVE
// candidates, so a newly introduced preparation starts at the incoming
// catalog identity. This checks shape and command digests, not whether opaque
// source or asset attestations were actually committed.
func replicaReplacementNewLifecyclePreparationSnapshotV1(record VectorPartitionLifecycleRecordV1) bool {
	begin := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: record.Identity, RequiredGroups: record.RequiredGroups,
		PreviousActiveGeneration: record.PreviousActiveGeneration, MutationEpoch: record.MutationEpoch,
		SourceOwners: record.SourceOwners, ANNOwners: record.ANNOwners,
	}
	building, err := ApplyVectorPartitionLifecycleCommandV1(VectorPartitionLifecycleRecordV1{}, begin)
	if err != nil {
		return false
	}
	if record.State == VectorPartitionLifecycleBuildingV1 {
		return equalVectorPartitionLifecycleRecordV1(building, record)
	}
	current := building
	for _, ready := range record.ReadyGroups {
		current, err = ApplyVectorPartitionLifecycleCommandV1(current, VectorPartitionLifecycleCommandV1{
			Kind: VectorPartitionLifecycleRecordGroupReadyV1, ExpectedRevision: current.Revision,
			ExpectedState: current.State, Identity: record.Identity, GroupReady: ready,
		})
		if err != nil {
			return false
		}
	}
	if record.State == VectorPartitionLifecyclePreparedV1 {
		current, err = ApplyVectorPartitionLifecycleCommandV1(current, VectorPartitionLifecycleCommandV1{
			Kind: VectorPartitionLifecyclePrepareV1, ExpectedRevision: current.Revision,
			ExpectedState: current.State, Identity: record.Identity, ReadySetDigest: record.ReadySetDigest,
		})
		if err != nil {
			return false
		}
		return equalVectorPartitionLifecycleRecordV1(current, record)
	}
	// READY commands may arrive in any order. Replaying once proves the
	// resulting STAGED fields; one of the possible final READY commands must
	// also match the retained last-command digest.
	current.LastCommandDigest = record.LastCommandDigest
	if !equalVectorPartitionLifecycleRecordV1(current, record) {
		return false
	}
	expectedState := VectorPartitionLifecycleStagedV1
	if len(record.ReadyGroups) == 1 {
		expectedState = VectorPartitionLifecycleBuildingV1
	}
	for _, ready := range record.ReadyGroups {
		command, err := EncodeVectorPartitionLifecycleCommandV1(VectorPartitionLifecycleCommandV1{
			Kind: VectorPartitionLifecycleRecordGroupReadyV1, ExpectedRevision: record.Revision - 1,
			ExpectedState: expectedState, Identity: record.Identity, GroupReady: ready,
		})
		if err == nil && sha256HexVectorPartitionLifecycleV1(command) == record.LastCommandDigest {
			return true
		}
	}
	return false
}

func vectorPartitionCollectionMutationBarrierSnapshotSuccessorV1(old, next vectorPartitionCollectionMutationBarrierStateV1) bool {
	if next.Epoch < old.Epoch {
		return false
	}
	// Mutation epochs can jump to a lifecycle invalidation epoch. Count only
	// receipts visible after the old barrier; each confirmation appends one.
	firstNew := 0
	for firstNew < len(next.Completed) && next.Completed[firstNew].Epoch < old.Epoch {
		firstNew++
	}
	if !old.Pending && firstNew < len(next.Completed) && next.Completed[firstNew].Epoch == old.Epoch {
		firstNew++
	}
	completed := len(next.Completed) - firstNew
	retained := len(old.Completed)
	if completed >= maxVectorPartitionCollectionCompletedMutationsV1 {
		retained = 0
	} else if retained > maxVectorPartitionCollectionCompletedMutationsV1-completed {
		retained = maxVectorPartitionCollectionCompletedMutationsV1 - completed
	}
	if firstNew != retained || !slices.Equal(old.Completed[len(old.Completed)-retained:], next.Completed[:retained]) {
		return false
	}
	if old.Pending {
		if next.Epoch == old.Epoch && next.Pending {
			return completed == 0
		}
		// Confirming the pending operation precedes the next BEGIN. Its receipt
		// may be absent only when 64 later receipts have displaced it.
		if completed < maxVectorPartitionCollectionCompletedMutationsV1 || next.Completed[firstNew].Epoch == old.Epoch {
			if completed == 0 || next.Completed[firstNew] != (vectorPartitionCollectionCompletedMutationV1{
				Epoch: old.Epoch, OperationDigest: old.OperationDigest,
			}) {
				return false
			}
		}
	}
	return true
}

// A compacted snapshot may include committed non-serving lifecycle commands
// after an ACTIVE catalog rebind or subsequent cleanup. The immutable source
// and READY receipts may not be replaced; only reducer-reachable
// invalidation/retirement/cleanup fields may advance. Same-revision records
// must still match exactly.
func replicaReplacementLifecycleRecordSnapshotSuccessorV1(
	old, next VectorPartitionLifecycleRecordV1,
	records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1,
	fences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1,
) bool {
	if next.Revision == old.Revision {
		return reflect.DeepEqual(old, next)
	}
	if next.Revision < old.Revision || next.State == VectorPartitionLifecycleActiveV1 ||
		old.State == VectorPartitionLifecycleAbsentV1 || next.Aborted != old.Aborted {
		return false
	}
	if old.State != VectorPartitionLifecycleActiveV1 &&
		(old.State != VectorPartitionLifecycleInvalidatedV1 && old.State != VectorPartitionLifecycleRetiredV1 && old.State != VectorPartitionLifecycleCleanableV1 ||
			next.InvalidationEpoch != old.InvalidationEpoch || next.InvalidationReason != old.InvalidationReason ||
			old.MutationConfirmed && !next.MutationConfirmed || next.SupersededByGeneration != old.SupersededByGeneration ||
			next.RetirementReason != old.RetirementReason) {
		return false
	}
	// The reducer can only advance through invalidation, retirement, and
	// cleanup. A compacted snapshot may skip intermediate commands.
	switch old.State {
	case VectorPartitionLifecycleActiveV1:
		if next.State != VectorPartitionLifecycleInvalidatedV1 && next.State != VectorPartitionLifecycleRetiredV1 &&
			next.State != VectorPartitionLifecycleCleanableV1 && next.State != VectorPartitionLifecycleAbsentV1 {
			return false
		}
	case VectorPartitionLifecycleInvalidatedV1:
		if next.State != VectorPartitionLifecycleInvalidatedV1 && next.State != VectorPartitionLifecycleRetiredV1 &&
			next.State != VectorPartitionLifecycleCleanableV1 && next.State != VectorPartitionLifecycleAbsentV1 {
			return false
		}
	case VectorPartitionLifecycleRetiredV1:
		if next.State != VectorPartitionLifecycleCleanableV1 && next.State != VectorPartitionLifecycleAbsentV1 {
			return false
		}
	case VectorPartitionLifecycleCleanableV1:
		if next.State != VectorPartitionLifecycleCleanableV1 && next.State != VectorPartitionLifecycleAbsentV1 {
			return false
		}
	default:
		return false
	}
	// Every committed reducer transition advances the revision once. A
	// compacted snapshot can omit intermediate states, but cannot reach a
	// later state with a different revision distance: retries do not advance
	// revisions, and each committed transition advances exactly once.
	required := uint64(len(old.RequiredGroups))
	cleaned := uint64(len(old.CleanedGroups))
	nextCleaned := uint64(len(next.CleanedGroups))
	var minimum uint64
	switch old.State {
	case VectorPartitionLifecycleActiveV1:
		switch next.State {
		case VectorPartitionLifecycleInvalidatedV1:
			minimum = 1
			if next.MutationConfirmed {
				minimum++
			}
		case VectorPartitionLifecycleRetiredV1:
			minimum = 1 // Atomic cutover.
			if next.SupersededByGeneration == 0 {
				minimum = 3 // Invalidate, confirm, retire.
			}
		case VectorPartitionLifecycleCleanableV1:
			minimum = 2 + nextCleaned // Cutover, mark cleanable, group cleanup.
			if next.SupersededByGeneration == 0 {
				minimum += 2 // Invalidate and confirm before retirement.
			}
		case VectorPartitionLifecycleAbsentV1:
			minimum = 3 + required // Cutover, mark cleanable, cleanup, complete.
			if next.SupersededByGeneration == 0 {
				minimum += 2
			}
		}
	case VectorPartitionLifecycleInvalidatedV1:
		confirmation := uint64(0)
		if !old.MutationConfirmed {
			confirmation = 1
		}
		switch next.State {
		case VectorPartitionLifecycleInvalidatedV1:
			if confirmation == 0 || !next.MutationConfirmed {
				return false
			}
			minimum = confirmation
		case VectorPartitionLifecycleRetiredV1:
			minimum = confirmation + 1
		case VectorPartitionLifecycleCleanableV1:
			minimum = confirmation + 2 + nextCleaned
		case VectorPartitionLifecycleAbsentV1:
			minimum = confirmation + 3 + required
		}
	case VectorPartitionLifecycleRetiredV1:
		if next.State == VectorPartitionLifecycleCleanableV1 {
			minimum = 1 + nextCleaned
		} else {
			minimum = 2 + required
		}
	case VectorPartitionLifecycleCleanableV1:
		if next.State == VectorPartitionLifecycleCleanableV1 {
			if nextCleaned <= cleaned {
				return false
			}
			minimum = nextCleaned - cleaned
		} else {
			minimum = 1 + required - cleaned
		}
	}
	if next.Revision-old.Revision != minimum {
		return false
	}
	if next.State == VectorPartitionLifecycleCleanableV1 {
		if old.State == VectorPartitionLifecycleCleanableV1 && len(next.CleanedGroups) <= len(old.CleanedGroups) {
			return false
		}
		for _, group := range old.CleanedGroups {
			if !containsVectorPartitionLifecycleGroupV1(next.CleanedGroups, group) {
				return false
			}
		}
	}
	if next.Aborted {
		if old.State != VectorPartitionLifecycleRetiredV1 && old.State != VectorPartitionLifecycleCleanableV1 {
			return false
		}
	} else if next.InvalidationEpoch == 0 {
		if next.SupersededByGeneration == 0 || next.State == VectorPartitionLifecycleInvalidatedV1 {
			return false
		}
		previous := next
		generation := next.SupersededByGeneration
		proved := false
		for steps := 0; steps < len(records); steps++ {
			_, successor, found := findVectorPartitionLifecycleGenerationLockedV1(records, next.Identity.Index, generation)
			if !found || generation <= previous.Identity.Generation ||
				successor.PreviousActiveGeneration != previous.Identity.Generation ||
				successor.Aborted || successor.Identity.SourceFormat == 2 {
				return false
			}
			switch successor.State {
			case VectorPartitionLifecycleActiveV1:
				// Immediate cutover gives both records the same command digest.
				if previous.State == VectorPartitionLifecycleRetiredV1 && previous.LastCommandDigest != successor.LastCommandDigest {
					return false
				}
				proved = true
			case VectorPartitionLifecycleInvalidatedV1, VectorPartitionLifecycleRetiredV1,
				VectorPartitionLifecycleCleanableV1, VectorPartitionLifecycleAbsentV1:
				if successor.InvalidationEpoch != 0 {
					key := vectorPartitionLifecycleServingKeyV1{Collection: successor.Identity.Index.Collection, IndexName: successor.Identity.Index.IndexName}
					fence, ok := fences[key]
					if !ok || successor.SupersededByGeneration != 0 ||
						successor.InvalidationEpoch <= successor.MutationEpoch || successor.InvalidationReason == "" ||
						fence.Epoch < successor.InvalidationEpoch ||
						(successor.State != VectorPartitionLifecycleInvalidatedV1 && !successor.MutationConfirmed) ||
						(fence.Epoch == successor.InvalidationEpoch && fence.Pending == successor.MutationConfirmed) {
						return false
					}
					proved = true
				} else if successor.SupersededByGeneration != 0 &&
					successor.State != VectorPartitionLifecycleInvalidatedV1 && successor.InvalidationReason == "" {
					previous = successor
					generation = successor.SupersededByGeneration
				} else {
					return false
				}
			default:
				return false
			}
			if proved {
				break
			}
		}
		if !proved {
			return false
		}
	} else {
		key := vectorPartitionLifecycleServingKeyV1{Collection: next.Identity.Index.Collection, IndexName: next.Identity.Index.IndexName}
		fence, ok := fences[key]
		if !ok || fence.Epoch < next.InvalidationEpoch ||
			(next.State != VectorPartitionLifecycleInvalidatedV1 && !next.MutationConfirmed) ||
			(fence.Epoch == next.InvalidationEpoch && fence.Pending == next.MutationConfirmed) {
			return false
		}
	}
	// Project the fields that the committed reducer can change back onto the
	// normalized old record, then compare every remaining source/asset fact.
	comparison := cloneVectorPartitionLifecycleRecordV1(next)
	comparison.Revision = old.Revision
	comparison.State = old.State
	comparison.LastCommandDigest = old.LastCommandDigest
	comparison.InvalidationReason = old.InvalidationReason
	comparison.InvalidationEpoch = old.InvalidationEpoch
	comparison.MutationConfirmed = old.MutationConfirmed
	comparison.SupersededByGeneration = old.SupersededByGeneration
	comparison.CleanedGroups = old.CleanedGroups
	comparison.CleanupComplete = old.CleanupComplete
	if next.State == VectorPartitionLifecycleAbsentV1 {
		comparison.RequiredGroups = old.RequiredGroups
		comparison.ReadyGroups = old.ReadyGroups
		comparison.ReadySetDigest = old.ReadySetDigest
	}
	return reflect.DeepEqual(old, comparison)
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
		if identity.SourceFormat == 2 {
			if err := validateCatalogSourceOwnersV2(catalog.Catalog, record.SourceOwners); err != nil {
				return nil, nil, nil, nil, nil, err
			}
		}
		for existing := range records {
			if (existing.SourceFormat == 2 || identity.SourceFormat == 2 ||
				existing.Immutable != (VectorPartitionLifecycleImmutableAuthorityV1{}) || identity.Immutable != (VectorPartitionLifecycleImmutableAuthorityV1{})) &&
				existing.Index == identity.Index && existing.Generation == identity.Generation && existing != identity {
				return nil, nil, nil, nil, nil, ErrVectorPartitionLifecycleConflict
			}
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
	if a.Source.RowCount != b.Source.RowCount {
		return a.Source.RowCount < b.Source.RowCount
	}
	if a.SourceFormat != b.SourceFormat {
		return a.SourceFormat < b.SourceFormat
	}
	return vectorPartitionSourceIdentityLessV2(a.SourceV2, b.SourceV2)
}

func cloneVectorPartitionLifecycleRecordsV1(source map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1) map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1 {
	clone := make(map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, len(source))
	for key, value := range source {
		clone[key] = cloneVectorPartitionLifecycleRecordV1(value)
	}
	return clone
}

func cloneVectorPartitionLifecycleRecordV1(record VectorPartitionLifecycleRecordV1) VectorPartitionLifecycleRecordV1 {
	record.SourceOwners = slices.Clone(record.SourceOwners)
	record.ANNOwners = slices.Clone(record.ANNOwners)
	record.RequiredGroups = slices.Clone(record.RequiredGroups)
	record.ReadyGroups = slices.Clone(record.ReadyGroups)
	record.CleanedGroups = slices.Clone(record.CleanedGroups)
	return record
}

func equalVectorPartitionLifecycleRecordV1(a, b VectorPartitionLifecycleRecordV1) bool {
	return a.Format == b.Format && a.Revision == b.Revision && a.State == b.State && a.Identity == b.Identity &&
		a.PreviousActiveGeneration == b.PreviousActiveGeneration && a.MutationEpoch == b.MutationEpoch &&
		slices.Equal(a.SourceOwners, b.SourceOwners) && slices.Equal(a.ANNOwners, b.ANNOwners) && slices.Equal(a.RequiredGroups, b.RequiredGroups) && slices.Equal(a.ReadyGroups, b.ReadyGroups) &&
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
