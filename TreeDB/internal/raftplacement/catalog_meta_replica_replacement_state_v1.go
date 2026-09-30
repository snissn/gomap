package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const ReplicaReplacementAdvanceKindV1 = "replica-replacement-advance-v1"

type ReplicaReplacementPhaseV1 string

const (
	ReplicaReplacementBegunV1         ReplicaReplacementPhaseV1 = "begun"
	ReplicaReplacementSeededV1        ReplicaReplacementPhaseV1 = "seeded"
	ReplicaReplacementInstalledV1     ReplicaReplacementPhaseV1 = "installed"
	ReplicaReplacementAddIntentV1     ReplicaReplacementPhaseV1 = "add-intent"
	ReplicaReplacementPromoteIntentV1 ReplicaReplacementPhaseV1 = "promote-intent"
	ReplicaReplacementPromotedV1      ReplicaReplacementPhaseV1 = "promoted"
	ReplicaReplacementRemoveIntentV1  ReplicaReplacementPhaseV1 = "remove-intent"
	ReplicaReplacementRemovedV1       ReplicaReplacementPhaseV1 = "removed"
	ReplicaReplacementCompletedV1     ReplicaReplacementPhaseV1 = "completed"
)

// ReplicaReplacementStateV1 is one bounded current operation per group, not an
// event history. Seed bytes originate at the production native snapshot boundary;
// applying this record binds them but cannot itself prove remote installation.
// Only the trusted runtime may advance after checking that receiver boundary.
type ReplicaReplacementStateV1 struct {
	Format uint16                                 `json:"format"`
	Kind   string                                 `json:"kind"`
	Begin  ReplicaReplacementBeginV1              `json:"begin"`
	Phase  ReplicaReplacementPhaseV1              `json:"phase"`
	Seed   *raftcluster.ReplacementSnapshotSeedV1 `json:"seed,omitempty"`
	Tail   *raftcluster.ReplacementTailV1         `json:"tail,omitempty"`
	// Peers is current catalog membership with transport addresses. Nil means the
	// original anchored roster; completion always persists an explicit roster.
	Peers        []raftcluster.Peer          `json:"peers,omitempty"`
	RemovalIndex uint64                      `json:"removal_index,omitempty"`
	Result       *ReplicaReplacementResultV1 `json:"result,omitempty"`
}

func replacementPhaseOrdinalV1(phase ReplicaReplacementPhaseV1) int {
	switch phase {
	case ReplicaReplacementBegunV1:
		return 0
	case ReplicaReplacementSeededV1:
		return 1
	case ReplicaReplacementInstalledV1:
		return 2
	case ReplicaReplacementAddIntentV1:
		return 3
	case ReplicaReplacementPromoteIntentV1:
		return 4
	case ReplicaReplacementPromotedV1:
		return 5
	case ReplicaReplacementRemoveIntentV1:
		return 6
	case ReplicaReplacementRemovedV1:
		return 7
	case ReplicaReplacementCompletedV1:
		return 8
	default:
		return -1
	}
}

func EncodeReplicaReplacementStateV1(state ReplicaReplacementStateV1) ([]byte, error) {
	state.Format, state.Kind = CatalogMetaFormatV1, ReplicaReplacementAdvanceKindV1
	if err := validateReplicaReplacementBeginV1(state.Begin); err != nil {
		return nil, err
	}
	ordinal := replacementPhaseOrdinalV1(state.Phase)
	if ordinal < 0 || validateReplicaReplacementPeersV1(state.Peers) != nil {
		return nil, ErrInvalidCatalogMeta
	}
	if ordinal == 0 {
		if state.Seed != nil {
			return nil, ErrInvalidCatalogMeta
		}
	} else if state.Seed == nil || state.Seed.Validate() != nil || state.Seed.Manifest.GroupID != state.Begin.GroupID || state.Seed.SourceNodeID == state.Begin.NewPeer.ID {
		return nil, ErrInvalidCatalogMeta
	}
	if replacementPhaseOrdinalV1(state.Phase) >= 4 {
		_, seedCommandIndex := state.Seed.Manifest.CommandBoundaryV1()
		if state.Tail == nil || state.Tail.Validate() != nil || state.Tail.GroupID != state.Begin.GroupID || state.Tail.Progress.EntryID.Index < seedCommandIndex || state.Tail.ConfigurationIndex <= state.Seed.ConfigurationIndex {
			return nil, ErrInvalidCatalogMeta
		}
	} else if state.Tail != nil {
		return nil, ErrInvalidCatalogMeta
	}
	if ordinal >= 6 {
		if state.RemovalIndex <= state.Tail.ConfigurationIndex {
			return nil, ErrInvalidCatalogMeta
		}
	} else if state.RemovalIndex != 0 {
		return nil, ErrInvalidCatalogMeta
	}
	if ordinal >= 7 {
		if state.Result == nil || state.Result.ConfigurationIndex <= state.RemovalIndex {
			return nil, ErrInvalidCatalogMeta
		}
		if ordinal == 8 {
			if state.Result.Epoch != state.Begin.ExpectedEpoch+1 || !validReplicaReplacementDigestV1(state.Result.Digest) || len(state.Peers) == 0 {
				return nil, ErrInvalidCatalogMeta
			}
		} else if state.Result.Epoch != 0 || state.Result.Digest != "" {
			return nil, ErrInvalidCatalogMeta
		}
	} else if state.Result != nil {
		return nil, ErrInvalidCatalogMeta
	}
	raw, err := json.Marshal(state)
	if len(raw) > maxReplicaReplacementCommandBytesV1 {
		return nil, ErrCatalogMetaLimit
	}
	return raw, err
}
func DecodeReplicaReplacementStateV1(raw []byte) (ReplicaReplacementStateV1, error) {
	var state ReplicaReplacementStateV1
	if len(raw) == 0 || len(raw) > maxReplicaReplacementCommandBytesV1 {
		return state, ErrCatalogMetaLimit
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return state, err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return state, ErrInvalidCatalogMeta
	}
	if state.Format != CatalogMetaFormatV1 || state.Kind != ReplicaReplacementAdvanceKindV1 {
		return state, ErrInvalidCatalogMeta
	}
	canonical, err := EncodeReplicaReplacementStateV1(state)
	if err != nil {
		return state, err
	}
	if !bytes.Equal(raw, canonical) {
		return state, ErrInvalidCatalogMeta
	}
	return state, nil
}
func decodeReplicaReplacementCurrentV1(raw []byte) (ReplicaReplacementStateV1, error) {
	var envelope struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ReplicaReplacementStateV1{}, err
	}
	if envelope.Kind == ReplicaReplacementBeginKindV1 {
		begin, err := DecodeReplicaReplacementBeginV1(raw)
		return ReplicaReplacementStateV1{Format: 1, Kind: ReplicaReplacementAdvanceKindV1, Begin: begin, Phase: ReplicaReplacementBegunV1}, err
	}
	return DecodeReplicaReplacementStateV1(raw)
}
func sameReplicaReplacementBeginV1(a, b ReplicaReplacementBeginV1) bool {
	left, err := EncodeReplicaReplacementBeginV1(a)
	if err != nil {
		return false
	}
	right, err := EncodeReplicaReplacementBeginV1(b)
	return err == nil && bytes.Equal(left, right)
}
func replicaReplacementStateExtendsV1(old, next ReplicaReplacementStateV1) bool {
	if !sameReplicaReplacementBeginV1(old.Begin, next.Begin) || replacementPhaseOrdinalV1(next.Phase) < replacementPhaseOrdinalV1(old.Phase) {
		return false
	}
	if !reflect.DeepEqual(old.Peers, next.Peers) || old.RemovalIndex != 0 && old.RemovalIndex != next.RemovalIndex || old.Result != nil && (next.Result == nil || *old.Result != *next.Result) {
		return false
	}
	if old.Tail != nil && (next.Tail == nil || *old.Tail != *next.Tail) {
		return false
	}
	return old.Seed == nil || next.Seed != nil && raftcluster.SameReplacementSnapshotSeedV1(*old.Seed, *next.Seed)
}
func replicaReplacementSnapshotExtendsV1(old, next []byte) bool {
	a, err := decodeReplicaReplacementCurrentV1(old)
	if err != nil {
		return false
	}
	b, err := decodeReplicaReplacementCurrentV1(next)
	return err == nil && replicaReplacementStateExtendsV1(a, b)
}

// Snapshot admission already validates identity and successor evidence. This
// lower bound reserves the entries required by the sequential phase reducer,
// using budget subtraction so several operations cannot overflow the total.
func replicaReplacementSnapshotEntryCostV1(old, next map[raftcluster.GroupID][]byte, budget uint64) (uint64, error) {
	remaining := budget
	for group, raw := range next {
		if bytes.Equal(raw, old[group]) {
			continue
		}
		incoming, err := decodeReplicaReplacementCurrentV1(raw)
		if err != nil {
			return 0, err
		}
		// A newly observed operation requires BEGIN and all earlier phases.
		entries := replacementPhaseOrdinalV1(incoming.Phase) + 1
		if priorRaw := old[group]; len(priorRaw) != 0 {
			prior, err := decodeReplicaReplacementCurrentV1(priorRaw)
			if err != nil {
				return 0, err
			}
			if sameReplicaReplacementBeginV1(prior.Begin, incoming.Begin) {
				entries = replacementPhaseOrdinalV1(incoming.Phase) - replacementPhaseOrdinalV1(prior.Phase)
			}
		}
		if entries < 0 || uint64(entries) > remaining {
			return 0, ErrVectorPartitionLifecycleConflict
		}
		remaining -= uint64(entries)
	}
	return budget - remaining, nil
}

func (a *CatalogMetaAuthorityV1) ReplicaReplacementStateV1(group raftcluster.GroupID) (ReplicaReplacementStateV1, error) {
	if a == nil {
		return ReplicaReplacementStateV1{}, ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.replacements[group]) == 0 {
		return ReplicaReplacementStateV1{}, ErrCatalogMetaUnavailable
	}
	return decodeReplicaReplacementCurrentV1(a.replacements[group])
}

func (a *CatalogMetaAuthorityV1) applyCommittedReplicaReplacementAdvanceV1(raw []byte, index uint64) (CatalogMetaStatusV1, error) {
	next, err := DecodeReplicaReplacementStateV1(raw)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if next.Phase == ReplicaReplacementCompletedV1 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := validateReplicaReplacementOwnerPreparationPhaseV1(a.record, a.lifecycle, next); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if err := validateReplicaReplacementCatalogV1(next.Begin, a.record); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	old, err := decodeReplicaReplacementCurrentV1(a.replacements[next.Begin.GroupID])
	if err != nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if !replicaReplacementStateExtendsV1(old, next) {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if old.Phase == next.Phase {
		return a.statusLocked(), nil
	}
	if replacementPhaseOrdinalV1(next.Phase) != replacementPhaseOrdinalV1(old.Phase)+1 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	candidate := make(map[raftcluster.GroupID][]byte, len(a.replacements))
	for group, value := range a.replacements {
		candidate[group] = value
	}
	candidate[next.Begin.GroupID] = bytes.Clone(raw)
	return a.installReplicaReplacementCandidateLockedV1(candidate, index)
}

func (a *CatalogMetaAuthorityV1) installReplicaReplacementCandidateLockedV1(candidate map[raftcluster.GroupID][]byte, index uint64) (CatalogMetaStatusV1, error) {
	if err := validateReplicaReplacementOwnerPreparationPhasesV1(a.record, a.resolved, a.lifecycle, candidate, a.mutationFences, a.collectionMutationBarriers); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	encoded, err := encodeReplicaReplacementSnapshotV1(candidate)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	lifecycle, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	snapshot := CatalogMetaSnapshotV1{Format: CatalogMetaFormatV1, AppliedIndex: index, Record: a.recordBytes, LastCommand: a.command, VectorPartitionLifecycle: lifecycle, ReplicaReplacements: encoded}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if len(raw) > MaxCatalogMetaSnapshotBytesV1 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaLimit
	}
	a.replacements, a.replacementBytes, a.applied = candidate, uint64(len(encoded)), index
	return a.statusLocked(), nil
}
