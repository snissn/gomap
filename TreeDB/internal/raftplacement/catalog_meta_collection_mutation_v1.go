package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	maxVectorPartitionCollectionMutationBarriersV1   = 4096
	maxVectorPartitionCollectionCompletedMutationsV1 = 64
)

type vectorPartitionCollectionMutationCommandKindV1 string

const (
	vectorPartitionBeginCollectionMutationV1   vectorPartitionCollectionMutationCommandKindV1 = "begin_collection_mutation_v1"
	vectorPartitionConfirmCollectionMutationV1 vectorPartitionCollectionMutationCommandKindV1 = "confirm_collection_mutation_v1"
)

type vectorPartitionCollectionMutationCommandV1 struct {
	Format                uint16                                         `json:"format"`
	Kind                  vectorPartitionCollectionMutationCommandKindV1 `json:"kind"`
	Collection            CollectionRefV1                                `json:"collection"`
	CatalogEpoch          uint64                                         `json:"catalog_epoch"`
	CatalogDigest         string                                         `json:"catalog_digest"`
	ExpectedMutationEpoch uint64                                         `json:"expected_mutation_epoch"`
	MutationEpoch         uint64                                         `json:"mutation_epoch"`
	OperationDigest       string                                         `json:"operation_digest"`
}

type vectorPartitionCollectionMutationBarrierV1 struct {
	Collection      CollectionRefV1                                `json:"collection"`
	Epoch           uint64                                         `json:"epoch"`
	Pending         bool                                           `json:"pending"`
	OperationDigest string                                         `json:"operation_digest"`
	Completed       []vectorPartitionCollectionCompletedMutationV1 `json:"completed,omitempty"`
}

type vectorPartitionCollectionCompletedMutationV1 struct {
	Epoch           uint64 `json:"epoch"`
	OperationDigest string `json:"operation_digest"`
}

type vectorPartitionCollectionMutationBarrierStateV1 struct {
	Epoch           uint64
	Pending         bool
	OperationDigest string
	Completed       []vectorPartitionCollectionCompletedMutationV1
}

// VectorPartitionCollectionMutationBarrierStatusV1 is the durable collection-
// wide source-capture fence. Epoch is retained after confirmation so a source
// captured before the mutation cannot later begin or activate a generation.
type VectorPartitionCollectionMutationBarrierStatusV1 struct {
	Collection      CollectionRefV1
	Epoch           uint64
	Pending         bool
	OperationDigest string
}

func cloneVectorPartitionCollectionMutationBarrierStateV1(state vectorPartitionCollectionMutationBarrierStateV1) vectorPartitionCollectionMutationBarrierStateV1 {
	state.Completed = append([]vectorPartitionCollectionCompletedMutationV1(nil), state.Completed...)
	return state
}

func findVectorPartitionCollectionCompletedMutationV1(state vectorPartitionCollectionMutationBarrierStateV1, operationDigest string) (vectorPartitionCollectionCompletedMutationV1, bool) {
	for _, completed := range state.Completed {
		if completed.OperationDigest == operationDigest {
			return completed, true
		}
	}
	return vectorPartitionCollectionCompletedMutationV1{}, false
}

func appendVectorPartitionCollectionCompletedMutationV1(state *vectorPartitionCollectionMutationBarrierStateV1, completed vectorPartitionCollectionCompletedMutationV1) {
	if _, exists := findVectorPartitionCollectionCompletedMutationV1(*state, completed.OperationDigest); exists {
		return
	}
	state.Completed = append(state.Completed, completed)
	if len(state.Completed) > maxVectorPartitionCollectionCompletedMutationsV1 {
		state.Completed = append([]vectorPartitionCollectionCompletedMutationV1(nil), state.Completed[len(state.Completed)-maxVectorPartitionCollectionCompletedMutationsV1:]...)
	}
}

func validateVectorPartitionCollectionMutationBarrierStateV1(state vectorPartitionCollectionMutationBarrierStateV1) error {
	if state.Epoch == 0 || !isSHA256HexVectorPartitionV1(state.OperationDigest) || len(state.Completed) > maxVectorPartitionCollectionCompletedMutationsV1 {
		return ErrInvalidVectorPartitionLifecycle
	}
	seen := make(map[string]struct{}, len(state.Completed))
	previousEpoch := uint64(0)
	for _, completed := range state.Completed {
		if completed.Epoch == 0 || completed.Epoch <= previousEpoch || completed.Epoch > state.Epoch || !isSHA256HexVectorPartitionV1(completed.OperationDigest) {
			return ErrInvalidVectorPartitionLifecycle
		}
		if _, duplicate := seen[completed.OperationDigest]; duplicate {
			return ErrVectorPartitionLifecycleConflict
		}
		seen[completed.OperationDigest] = struct{}{}
		previousEpoch = completed.Epoch
	}
	if state.Pending {
		if len(state.Completed) > 0 && state.Completed[len(state.Completed)-1].Epoch >= state.Epoch {
			return ErrInvalidVectorPartitionLifecycle
		}
		if _, duplicate := seen[state.OperationDigest]; duplicate {
			return ErrVectorPartitionLifecycleConflict
		}
		return nil
	}
	if len(state.Completed) == 0 {
		return ErrInvalidVectorPartitionLifecycle
	}
	latest := state.Completed[len(state.Completed)-1]
	if latest.Epoch != state.Epoch || latest.OperationDigest != state.OperationDigest {
		return ErrInvalidVectorPartitionLifecycle
	}
	return nil
}

func encodeVectorPartitionCollectionMutationCommandV1(command vectorPartitionCollectionMutationCommandV1) ([]byte, error) {
	command.Format = VectorPartitionLifecycleFormatV1
	if err := validateCollectionRef(command.Collection); err != nil {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if command.CatalogEpoch == 0 || !isSHA256HexVectorPartitionV1(command.CatalogDigest) ||
		command.MutationEpoch == 0 || !isSHA256HexVectorPartitionV1(command.OperationDigest) {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid collection mutation proof"))
	}
	switch command.Kind {
	case vectorPartitionBeginCollectionMutationV1:
		if command.MutationEpoch != command.ExpectedMutationEpoch+1 {
			return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("collection mutation epoch is not contiguous"))
		}
	case vectorPartitionConfirmCollectionMutationV1:
		if command.ExpectedMutationEpoch != command.MutationEpoch {
			return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("collection mutation confirmation epoch mismatch"))
		}
	default:
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("unknown collection mutation command kind %q", command.Kind))
	}
	raw, err := json.Marshal(command)
	if err != nil {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if len(raw) > MaxVectorPartitionLifecycleCommandBytesV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("collection mutation command is %d bytes", len(raw)))
	}
	return raw, nil
}

func decodeVectorPartitionCollectionMutationCommandV1(raw []byte) (vectorPartitionCollectionMutationCommandV1, error) {
	if len(raw) == 0 || len(raw) > MaxVectorPartitionLifecycleCommandBytesV1 {
		return vectorPartitionCollectionMutationCommandV1{}, ErrVectorPartitionLifecycleLimit
	}
	var command vectorPartitionCollectionMutationCommandV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		return vectorPartitionCollectionMutationCommandV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return vectorPartitionCollectionMutationCommandV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("trailing collection mutation command data"))
	}
	canonical, err := encodeVectorPartitionCollectionMutationCommandV1(command)
	if err != nil {
		return vectorPartitionCollectionMutationCommandV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return vectorPartitionCollectionMutationCommandV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("collection mutation command is not canonical"))
	}
	return command, nil
}

func vectorPartitionCollectionMutationCommandBytesV1(raw []byte) bool {
	var envelope struct {
		Kind vectorPartitionCollectionMutationCommandKindV1 `json:"kind"`
	}
	if len(raw) == 0 || len(raw) > MaxVectorPartitionLifecycleCommandBytesV1 || json.Unmarshal(raw, &envelope) != nil {
		return false
	}
	return envelope.Kind == vectorPartitionBeginCollectionMutationV1 || envelope.Kind == vectorPartitionConfirmCollectionMutationV1
}

func (a *CatalogMetaAuthorityV1) effectiveCollectionMutationEpochLockedV1(collection CollectionRefV1) uint64 {
	epoch := uint64(1) // epoch one is the no-mutation source-capture baseline.
	if barrier := a.collectionMutationBarriers[collection]; barrier.Epoch > epoch {
		epoch = barrier.Epoch
	}
	for identity, record := range a.lifecycle {
		if identity.Index.Collection != collection {
			continue
		}
		if record.MutationEpoch > epoch {
			epoch = record.MutationEpoch
		}
		if record.InvalidationEpoch > epoch {
			epoch = record.InvalidationEpoch
		}
	}
	return epoch
}

func (a *CatalogMetaAuthorityV1) applyCommittedVectorPartitionCollectionMutationV1(raw []byte, appliedIndex uint64) (CatalogMetaStatusV1, error) {
	command, err := decodeVectorPartitionCollectionMutationCommandV1(raw)
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
	if command.CatalogEpoch != a.record.Epoch || command.CatalogDigest != a.record.Digest {
		return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleIdentity, fmt.Errorf("collection mutation catalog proof does not match current authority"))
	}
	if a.collectionMutationBarriers == nil {
		a.collectionMutationBarriers = make(map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1)
	}
	current, exists := a.collectionMutationBarriers[command.Collection]
	current = cloneVectorPartitionCollectionMutationBarrierStateV1(current)
	original := cloneVectorPartitionCollectionMutationBarrierStateV1(current)
	switch command.Kind {
	case vectorPartitionBeginCollectionMutationV1:
		if exists && current.OperationDigest == command.OperationDigest {
			if current.Epoch != command.MutationEpoch {
				return CatalogMetaStatusV1{}, ErrVectorPartitionLifecycleConflict
			}
			return a.statusLocked(), nil
		}
		if completed, ok := findVectorPartitionCollectionCompletedMutationV1(current, command.OperationDigest); ok {
			if completed.Epoch != command.MutationEpoch {
				return CatalogMetaStatusV1{}, ErrVectorPartitionLifecycleConflict
			}
			return a.statusLocked(), nil
		}
		if current.Pending {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("collection mutation epoch %d is pending", current.Epoch))
		}
		if !exists && len(a.collectionMutationBarriers) >= maxVectorPartitionCollectionMutationBarriersV1 {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("collection mutation barriers=%d", len(a.collectionMutationBarriers)))
		}
		effective := a.effectiveCollectionMutationEpochLockedV1(command.Collection)
		if command.ExpectedMutationEpoch != effective || command.MutationEpoch != effective+1 {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleStale, fmt.Errorf("collection mutation epoch=%d/%d want %d/%d", command.ExpectedMutationEpoch, command.MutationEpoch, effective, effective+1))
		}
		for identity, record := range a.lifecycle {
			if identity.Index.Collection != command.Collection {
				continue
			}
			switch record.State {
			case VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1, VectorPartitionLifecyclePreparedV1:
				return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("generation %d source capture is in progress", identity.Generation))
			}
		}
		a.collectionMutationBarriers[command.Collection] = vectorPartitionCollectionMutationBarrierStateV1{Epoch: command.MutationEpoch, Pending: true, OperationDigest: command.OperationDigest, Completed: current.Completed}
	case vectorPartitionConfirmCollectionMutationV1:
		if completed, ok := findVectorPartitionCollectionCompletedMutationV1(current, command.OperationDigest); ok {
			if completed.Epoch != command.MutationEpoch {
				return CatalogMetaStatusV1{}, ErrVectorPartitionLifecycleConflict
			}
			return a.statusLocked(), nil
		}
		if !exists || current.Epoch != command.MutationEpoch || current.OperationDigest != command.OperationDigest {
			return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("collection mutation confirmation does not own the barrier"))
		}
		if !current.Pending {
			return a.statusLocked(), nil
		}
		for key, fence := range a.mutationFences {
			if key.Collection == command.Collection && fence.Pending {
				return CatalogMetaStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("index %q mutation fence %d remains pending", key.IndexName, fence.Epoch))
			}
		}
		appendVectorPartitionCollectionCompletedMutationV1(&current, vectorPartitionCollectionCompletedMutationV1{Epoch: current.Epoch, OperationDigest: current.OperationDigest})
		current.Pending = false
		a.collectionMutationBarriers[command.Collection] = current
	}
	snapshot, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		if exists {
			a.collectionMutationBarriers[command.Collection] = original
		} else {
			delete(a.collectionMutationBarriers, command.Collection)
		}
		return CatalogMetaStatusV1{}, err
	}
	if err := a.validateProspectiveCatalogMetaSnapshotLockedV1(snapshot, appliedIndex); err != nil {
		if exists {
			a.collectionMutationBarriers[command.Collection] = original
		} else {
			delete(a.collectionMutationBarriers, command.Collection)
		}
		return CatalogMetaStatusV1{}, err
	}
	a.applied = appliedIndex
	a.lifecycleBytes = uint64(len(snapshot))
	a.refusal = ""
	return a.statusLocked(), nil
}

// VectorPartitionCollectionMutationOperationV1 resolves an exact operation
// from the current barrier or the bounded durable completion window.
func (a *CatalogMetaAuthorityV1) VectorPartitionCollectionMutationOperationV1(collection CollectionRefV1, operationDigest string) (VectorPartitionCollectionMutationBarrierStatusV1, bool, error) {
	if a == nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, ErrCatalogMetaUnavailable
	}
	if err := validateCollectionRef(collection); err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, err
	}
	if !isSHA256HexVectorPartitionV1(operationDigest) {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid mutation operation digest"))
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.record.Epoch == 0 {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, ErrCatalogMetaUnavailable
	}
	barrier, ok := a.collectionMutationBarriers[collection]
	if !ok {
		return VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection}, false, nil
	}
	if barrier.OperationDigest == operationDigest {
		return VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection, Epoch: barrier.Epoch, Pending: barrier.Pending, OperationDigest: barrier.OperationDigest}, true, nil
	}
	if completed, found := findVectorPartitionCollectionCompletedMutationV1(barrier, operationDigest); found {
		return VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection, Epoch: completed.Epoch, Pending: false, OperationDigest: completed.OperationDigest}, true, nil
	}
	return VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection, Epoch: barrier.Epoch, Pending: barrier.Pending}, false, nil
}

func (a *CatalogMetaAuthorityV1) VectorPartitionCollectionMutationBarrierV1(collection CollectionRefV1) (VectorPartitionCollectionMutationBarrierStatusV1, bool, error) {
	if a == nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, ErrCatalogMetaUnavailable
	}
	if err := validateCollectionRef(collection); err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.record.Epoch == 0 {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, false, ErrCatalogMetaUnavailable
	}
	barrier, ok := a.collectionMutationBarriers[collection]
	if !ok {
		return VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection, Epoch: a.effectiveCollectionMutationEpochLockedV1(collection)}, false, nil
	}
	return VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection, Epoch: barrier.Epoch, Pending: barrier.Pending, OperationDigest: barrier.OperationDigest}, true, nil
}

func (a *CatalogMetaAuthorityV1) VectorPartitionCollectionMutationBarriersV1() []VectorPartitionCollectionMutationBarrierStatusV1 {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	statuses := make([]VectorPartitionCollectionMutationBarrierStatusV1, 0, len(a.collectionMutationBarriers))
	for collection, barrier := range a.collectionMutationBarriers {
		statuses = append(statuses, VectorPartitionCollectionMutationBarrierStatusV1{Collection: collection, Epoch: barrier.Epoch, Pending: barrier.Pending, OperationDigest: barrier.OperationDigest})
	}
	sort.Slice(statuses, func(i, j int) bool {
		a, b := statuses[i].Collection, statuses[j].Collection
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Catalog != b.Catalog {
			return a.Catalog < b.Catalog
		}
		return a.Collection < b.Collection
	})
	return statuses
}

// BuildSourceMutationEpochV1 must be read before capturing the group source
// proofs. BeginBuildV1 rechecks the returned epoch after capture, while a
// committed begin-build blocks a later distinct mutation until build abort or
// cutover. The two checks close both sides of the source-capture race.
func (c VectorPartitionLifecycleCoordinatorV1) BuildSourceMutationEpochV1(collection CollectionRefV1) (uint64, error) {
	if c.Authority == nil {
		return 0, ErrCatalogMetaUnavailable
	}
	status, _, err := c.Authority.VectorPartitionCollectionMutationBarrierV1(collection)
	if err != nil {
		return 0, err
	}
	if status.Pending {
		return 0, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("collection mutation epoch %d is pending", status.Epoch))
	}
	return status.Epoch, nil
}

func (c VectorPartitionLifecycleCoordinatorV1) BeginRelevantCollectionMutationV1(ctx context.Context, collection CollectionRefV1, operationDigest string) (VectorPartitionCollectionMutationBarrierStatusV1, error) {
	if c.Authority == nil || c.Committer == nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, ErrCatalogMetaUnavailable
	}
	if !isSHA256HexVectorPartitionV1(operationDigest) {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid mutation operation digest"))
	}
	exact, found, err := c.Authority.VectorPartitionCollectionMutationOperationV1(collection, operationDigest)
	if err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, err
	}
	if found {
		return exact, nil
	}
	current, _, err := c.Authority.VectorPartitionCollectionMutationBarrierV1(collection)
	if err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, err
	}
	if current.Pending {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("different collection mutation epoch %d is pending", current.Epoch))
	}
	proof, err := c.Authority.CurrentCatalogProof(ctx)
	if err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, err
	}
	command := vectorPartitionCollectionMutationCommandV1{Kind: vectorPartitionBeginCollectionMutationV1, Collection: collection, CatalogEpoch: proof.Epoch, CatalogDigest: proof.Digest, ExpectedMutationEpoch: current.Epoch, MutationEpoch: current.Epoch + 1, OperationDigest: operationDigest}
	raw, err := encodeVectorPartitionCollectionMutationCommandV1(command)
	if err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, err
	}
	if _, _, err := c.Committer.SubmitCatalogMetaCommandV1(ctx, raw); err != nil {
		return VectorPartitionCollectionMutationBarrierStatusV1{}, err
	}
	status, _, err := c.Authority.VectorPartitionCollectionMutationBarrierV1(collection)
	if err != nil || status.OperationDigest != operationDigest || status.Epoch != command.MutationEpoch {
		if err != nil {
			return VectorPartitionCollectionMutationBarrierStatusV1{}, err
		}
		return VectorPartitionCollectionMutationBarrierStatusV1{}, ErrCatalogMetaUnavailable
	}
	return status, nil
}

func (c VectorPartitionLifecycleCoordinatorV1) ConfirmRelevantCollectionMutationV1(ctx context.Context, collection CollectionRefV1, operationDigest string) error {
	if c.Authority == nil || c.Committer == nil {
		return ErrCatalogMetaUnavailable
	}
	current, exists, err := c.Authority.VectorPartitionCollectionMutationOperationV1(collection, operationDigest)
	if err != nil {
		return err
	}
	if !exists || current.OperationDigest != operationDigest {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("collection mutation confirmation has no matching barrier"))
	}
	if !current.Pending {
		return nil
	}
	proof, err := c.Authority.CurrentCatalogProof(ctx)
	if err != nil {
		return err
	}
	command := vectorPartitionCollectionMutationCommandV1{Kind: vectorPartitionConfirmCollectionMutationV1, Collection: collection, CatalogEpoch: proof.Epoch, CatalogDigest: proof.Digest, ExpectedMutationEpoch: current.Epoch, MutationEpoch: current.Epoch, OperationDigest: operationDigest}
	raw, err := encodeVectorPartitionCollectionMutationCommandV1(command)
	if err != nil {
		return err
	}
	_, _, err = c.Committer.SubmitCatalogMetaCommandV1(ctx, raw)
	return err
}
