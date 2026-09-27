package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const ReplicaReplacementCompleteKindV1 = "replica-replacement-complete-v1"

// ReplicaReplacementResultV1 is the latest terminal result, not operation
// history. The next operation on this group supersedes it.
type ReplicaReplacementResultV1 struct {
	ConfigurationIndex uint64 `json:"configuration_index"`
	Epoch              uint64 `json:"epoch,omitempty"`
	Digest             string `json:"digest,omitempty"`
}

type ReplicaReplacementCompleteV1 struct {
	Format  uint16                    `json:"format"`
	Kind    string                    `json:"kind"`
	State   ReplicaReplacementStateV1 `json:"state"`
	Catalog CatalogMetaCommandV1      `json:"catalog"`
}

func validateReplicaReplacementPeersV1(peers []raftcluster.Peer) error {
	if len(peers) > MaxCatalogMetaMembersPerGroupV1 {
		return ErrCatalogMetaLimit
	}
	for i, p := range peers {
		if !validReplicaReplacementStringV1(string(p.ID)) || !validReplicaReplacementAddressV1(p.Address) || p.Capabilities.ConfigVersion != (raftcluster.Version{}) || len(p.Capabilities.Required) != 0 {
			return ErrInvalidCatalogMeta
		}
		if i > 0 && peers[i-1].ID >= p.ID {
			return ErrInvalidCatalogMeta
		}
		for _, q := range peers[:i] {
			if q.Address == p.Address {
				return ErrInvalidCatalogMeta
			}
		}
	}
	return nil
}

func replicaReplacementPeerIDsV1(peers []raftcluster.Peer) []raftcluster.NodeID {
	ids := make([]raftcluster.NodeID, len(peers))
	for i, p := range peers {
		ids[i] = p.ID
	}
	return ids
}

func validateReplicaReplacementStateCatalogV1(state ReplicaReplacementStateV1, record CatalogMetaRecordV1) error {
	if state.Phase != ReplicaReplacementCompletedV1 {
		if err := validateReplicaReplacementCatalogV1(state.Begin, record); err != nil {
			return err
		}
	} else {
		if state.Result == nil || state.Result.Epoch > record.Epoch || state.Result.Epoch == record.Epoch && state.Result.Digest != record.Digest || catalogMetaFeatureEnabledV1(record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
			return ErrCatalogMetaConflict
		}
	}
	if len(state.Peers) > 0 {
		for _, g := range record.Catalog.Groups {
			if g.ID == state.Begin.GroupID {
				if !equalCatalogMetaMembersV1(replicaReplacementPeerIDsV1(state.Peers), g.Members) {
					return ErrCatalogMetaConflict
				}
				return nil
			}
		}
		return ErrCatalogMetaConflict
	}
	return nil
}

func EncodeReplicaReplacementCompleteV1(command ReplicaReplacementCompleteV1) ([]byte, error) {
	command.Format, command.Kind = CatalogMetaFormatV1, ReplicaReplacementCompleteKindV1
	state, err := EncodeReplicaReplacementStateV1(command.State)
	if err != nil {
		return nil, err
	}
	command.State, err = DecodeReplicaReplacementStateV1(state)
	if err != nil {
		return nil, err
	}
	if command.State.Phase != ReplicaReplacementCompletedV1 || command.State.Result == nil {
		return nil, ErrInvalidCatalogMeta
	}
	catalog, err := EncodeCatalogMetaCommandV1(command.Catalog)
	if err != nil {
		return nil, err
	}
	command.Catalog, err = DecodeCatalogMetaCommandV1(catalog)
	if err != nil {
		return nil, err
	}
	if command.Catalog.ExpectedEpoch != command.State.Begin.ExpectedEpoch || command.Catalog.Record.Epoch != command.State.Result.Epoch || command.Catalog.Record.Digest != command.State.Result.Digest {
		return nil, ErrCatalogMetaConflict
	}
	raw, err := json.Marshal(command)
	if len(raw) > MaxCatalogMetaCommandBytesV1 {
		return nil, ErrCatalogMetaLimit
	}
	return raw, err
}

func DecodeReplicaReplacementCompleteV1(raw []byte) (ReplicaReplacementCompleteV1, error) {
	var command ReplicaReplacementCompleteV1
	if len(raw) == 0 || len(raw) > MaxCatalogMetaCommandBytesV1 {
		return command, ErrCatalogMetaLimit
	}
	var envelope struct {
		Format  uint16          `json:"format"`
		Kind    string          `json:"kind"`
		State   json.RawMessage `json:"state"`
		Catalog json.RawMessage `json:"catalog"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&envelope); err != nil {
		return command, err
	}
	if err := d.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return command, ErrInvalidCatalogMeta
	}
	if envelope.Format != CatalogMetaFormatV1 || envelope.Kind != ReplicaReplacementCompleteKindV1 {
		return command, ErrInvalidCatalogMeta
	}
	var err error
	command.Format, command.Kind = envelope.Format, envelope.Kind
	// The existing catalog decoder preflights every nested count before slices.
	command.Catalog, err = DecodeCatalogMetaCommandV1(envelope.Catalog)
	if err != nil {
		return command, err
	}
	command.State, err = DecodeReplicaReplacementStateV1(envelope.State)
	if err != nil {
		return command, err
	}
	canonical, err := EncodeReplicaReplacementCompleteV1(command)
	if err != nil {
		return command, err
	}
	if !bytes.Equal(raw, canonical) {
		return command, ErrInvalidCatalogMeta
	}
	return command, nil
}

// ReplacementCompletionV1 derives the sole permitted catalog edit from the
// committed removed phase. The runtime must independently verify the native
// final configuration before submitting this command.
func (a *CatalogMetaAuthorityV1) ReplacementCompletionV1(group raftcluster.GroupID, peers []raftcluster.Peer) (ReplicaReplacementCompleteV1, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	state, err := decodeReplicaReplacementCurrentV1(a.replacements[group])
	if err != nil {
		return ReplicaReplacementCompleteV1{}, err
	}
	if state.Phase != ReplicaReplacementRemovedV1 {
		return ReplicaReplacementCompleteV1{}, ErrCatalogMetaConflict
	}
	catalog, _, err := canonicalCatalogMetaCatalogV1(a.record.Catalog)
	if err != nil {
		return ReplicaReplacementCompleteV1{}, err
	}
	found := false
	for i := range catalog.Groups {
		if catalog.Groups[i].ID == group {
			found = true
			members := catalog.Groups[i].Members
			for j, id := range members {
				if id == state.Begin.OldNodeID {
					members[j] = state.Begin.NewPeer.ID
				}
			}
			slices.Sort(members)
			if catalog.Groups[i].LeaderHint == state.Begin.OldNodeID {
				catalog.Groups[i].LeaderHint = ""
			}
		}
	}
	if !found {
		return ReplicaReplacementCompleteV1{}, ErrCatalogMetaConflict
	}
	record, err := NewCatalogMetaRecordV1(a.record.Epoch+1, catalog)
	if err != nil {
		return ReplicaReplacementCompleteV1{}, err
	}
	state.Phase = ReplicaReplacementCompletedV1
	state.Peers = slices.Clone(peers)
	result := *state.Result
	result.Epoch, result.Digest = record.Epoch, record.Digest
	state.Result = &result
	command := ReplicaReplacementCompleteV1{State: state, Catalog: CatalogMetaCommandV1{ExpectedEpoch: a.record.Epoch, Record: record}}
	raw, err := EncodeReplicaReplacementCompleteV1(command)
	if err != nil {
		return command, err
	}
	return DecodeReplicaReplacementCompleteV1(raw)
}

func (a *CatalogMetaAuthorityV1) applyCommittedReplicaReplacementCompleteV1(raw []byte, index uint64) (CatalogMetaStatusV1, error) {
	command, err := DecodeReplicaReplacementCompleteV1(raw)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	state := command.State
	old, err := decodeReplicaReplacementCurrentV1(a.replacements[state.Begin.GroupID])
	if err != nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if old.Phase == ReplicaReplacementCompletedV1 {
		oldRaw, _ := EncodeReplicaReplacementStateV1(old)
		nextRaw, _ := EncodeReplicaReplacementStateV1(state)
		if bytes.Equal(oldRaw, nextRaw) && validateReplicaReplacementStateCatalogV1(state, a.record) == nil {
			return a.statusLocked(), nil
		}
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if old.Phase != ReplicaReplacementRemovedV1 || validateReplicaReplacementCatalogV1(state.Begin, a.record) != nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	// Compare immutable operation evidence before permitting only the terminal
	// roster/result change. The removed result retains its native configuration.
	compare := state
	compare.Phase = old.Phase
	compare.Peers = old.Peers
	compare.Result = old.Result
	if !reflect.DeepEqual(compare, old) {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if state.Result.ConfigurationIndex != old.Result.ConfigurationIndex || !replicaReplacementCompletedPeersV1(old, state.Peers) {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	catalog, _, err := canonicalCatalogMetaCatalogV1(a.record.Catalog)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	for i := range catalog.Groups {
		if catalog.Groups[i].ID == state.Begin.GroupID {
			for j, id := range catalog.Groups[i].Members {
				if id == state.Begin.OldNodeID {
					catalog.Groups[i].Members[j] = state.Begin.NewPeer.ID
				}
			}
			slices.Sort(catalog.Groups[i].Members)
			if catalog.Groups[i].LeaderHint == state.Begin.OldNodeID {
				catalog.Groups[i].LeaderHint = ""
			}
		}
	}
	expected, err := NewCatalogMetaRecordV1(a.record.Epoch+1, catalog)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if !reflect.DeepEqual(expected, command.Catalog.Record) || command.Catalog.ExpectedEpoch != a.record.Epoch || validateReplicaReplacementStateCatalogV1(state, expected) != nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if err := a.validateVectorPartitionLifecycleCatalogTransitionLockedV1(); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	nextRaw, _ := EncodeReplicaReplacementStateV1(state)
	records := make(map[raftcluster.GroupID][]byte, len(a.replacements))
	for g, v := range a.replacements {
		records[g] = v
	}
	records[state.Begin.GroupID] = nextRaw
	replacements, err := encodeReplicaReplacementSnapshotV1(records)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	recordRaw, err := encodeCatalogMetaRecordV1(expected)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	last, err := EncodeCatalogMetaCommandV1(command.Catalog)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	lifecycle, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	snapshot := CatalogMetaSnapshotV1{Format: CatalogMetaFormatV1, AppliedIndex: index, Record: recordRaw, LastCommand: last, ReplicaReplacements: replacements, VectorPartitionLifecycle: lifecycle}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if len(encoded) > MaxCatalogMetaSnapshotBytesV1 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaLimit
	}
	if err := preflightCatalogMetaSnapshotJSONV1(encoded); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	_, resolved, err := canonicalCatalogMetaCatalogV1(expected.Catalog)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	a.record, a.recordBytes, a.command, a.resolved = expected, recordRaw, last, resolved
	a.replacements, a.replacementBytes, a.applied = records, uint64(len(replacements)), index
	return a.statusLocked(), nil
}

func replicaReplacementSnapshotSuccessorV1(oldRaw, nextRaw []byte) bool {
	old, err := decodeReplicaReplacementCurrentV1(oldRaw)
	if err != nil {
		return false
	}
	next, err := decodeReplicaReplacementCurrentV1(nextRaw)
	if err != nil {
		return false
	}
	if !sameReplicaReplacementBeginV1(old.Begin, next.Begin) {
		// A committed snapshot may compact completion and several later operations.
		// Its current roster/pending fence is validated independently; no event
		// history is required. Same-epoch identity substitution remains refused.
		return next.Begin.GroupID == old.Begin.GroupID && next.Begin.ConfigDigest == old.Begin.ConfigDigest && next.Begin.ExpectedEpoch > old.Begin.ExpectedEpoch
	}
	if next.Phase == ReplicaReplacementCompletedV1 && old.Phase != ReplicaReplacementCompletedV1 {
		if old.Result != nil && old.Result.ConfigurationIndex != next.Result.ConfigurationIndex || !replicaReplacementCompletedPeersV1(old, next.Peers) {
			return false
		}
		compare := next
		compare.Peers = old.Peers
		compare.Result = old.Result
		if old.RemovalIndex == 0 {
			compare.RemovalIndex = 0
		}
		return replicaReplacementStateExtendsV1(old, compare)
	}
	return replicaReplacementStateExtendsV1(old, next)
}

func (a *CatalogMetaAuthorityV1) hasPendingReplicaReplacementLockedV1() bool {
	for _, raw := range a.replacements {
		state, err := decodeReplicaReplacementCurrentV1(raw)
		if err != nil || state.Phase != ReplicaReplacementCompletedV1 {
			return true
		}
	}
	return false
}

// Snapshot topology is authorized by bounded current rosters, not historical
// BEGIN epochs. Ordinary catalog publication continues to forbid member edits.
func validateReplicaReplacementSnapshotTopologyV1(current, next ResolvedCatalogV1, records map[raftcluster.GroupID][]byte) error {
	projected := current
	projected.Groups = slices.Clone(current.Groups)
	for i, g := range projected.Groups {
		ng, ok := next.groups[g.ID]
		if !ok {
			return ErrCatalogMetaTopologyChange
		}
		if equalCatalogMetaMembersV1(g.Members, ng.Members) {
			continue
		}
		state, err := decodeReplicaReplacementCurrentV1(records[g.ID])
		if err != nil || len(state.Peers) == 0 || !equalCatalogMetaMembersV1(replicaReplacementPeerIDsV1(state.Peers), ng.Members) {
			return ErrCatalogMetaTopologyChange
		}
		projected.Groups[i].Members = slices.Clone(ng.Members)
	}
	return validateCatalogMetaTopologyTransitionV1(projected, next)
}

// On first completion the runtime proves anchored survivor addresses. Later
// operations also bind them here through the committed current roster.
func replicaReplacementCompletedPeersV1(old ReplicaReplacementStateV1, peers []raftcluster.Peer) bool {
	found := false
	for _, peer := range peers {
		if peer.ID == old.Begin.OldNodeID {
			return false
		}
		if peer.ID == old.Begin.NewPeer.ID {
			if peer.Address != old.Begin.NewPeer.Address {
				return false
			}
			found = true
		}
	}
	if !found {
		return false
	}
	if len(old.Peers) == 0 {
		return true
	}
	if len(old.Peers) != len(peers) {
		return false
	}
	for _, prior := range old.Peers {
		if prior.ID == old.Begin.OldNodeID {
			continue
		}
		match := false
		for _, peer := range peers {
			if peer.ID == prior.ID && peer.Address == prior.Address {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
}

// CurrentReplicaGroupV1 returns one coherent bounded roster selection. Callers
// must first fence this authority through the committed catalog provider.
func (a *CatalogMetaAuthorityV1) CurrentReplicaGroupV1(group raftcluster.GroupID) (GroupV1, *ReplicaReplacementStateV1, CatalogMetaStatusV1, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, g := range a.record.Catalog.Groups {
		if g.ID != group {
			continue
		}
		g.Members = slices.Clone(g.Members)
		var state *ReplicaReplacementStateV1
		if raw := a.replacements[group]; len(raw) > 0 {
			decoded, err := decodeReplicaReplacementCurrentV1(raw)
			if err != nil {
				return GroupV1{}, nil, CatalogMetaStatusV1{}, err
			}
			state = &decoded
		}
		return g, state, a.statusLocked(), nil
	}
	return GroupV1{}, nil, CatalogMetaStatusV1{}, ErrCatalogMetaUnavailable
}
