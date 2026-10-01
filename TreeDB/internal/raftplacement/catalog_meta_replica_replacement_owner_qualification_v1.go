package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"io"
)

const ReplicaReplacementOwnerQualificationKindV1 = "replica-replacement-owner-qualification-v1"

// ReplicaReplacementOwnerQualificationReceiptV1 records one historical private
// ANN execution. It grants no serving, READY, membership or promotion authority.
// Raw privileged in-process commands and trusted backups cannot prove execution.
type ReplicaReplacementOwnerQualificationReceiptV1 struct {
	QueryDigest        string                        `json:"query_digest"`
	ResultDigest       string                        `json:"result_digest"`
	ReadySetDigest     string                        `json:"ready_set_digest"`
	IssuerNode         raftcluster.NodeID            `json:"issuer_node"`
	ReadTerm           uint64                        `json:"read_term"`
	ReadIndex          uint64                        `json:"read_index"`
	TargetAppliedIndex uint64                        `json:"target_applied_index"`
	Tail               raftcluster.ReplacementTailV1 `json:"tail"`
}

type ReplicaReplacementOwnerQualificationCommandV1 struct {
	Format uint16                    `json:"format"`
	Kind   string                    `json:"kind"`
	State  ReplicaReplacementStateV1 `json:"state"`
}

func validateReplicaReplacementOwnerQualificationReceiptV1(state ReplicaReplacementStateV1) error {
	receipt := state.OwnerQualification
	if receipt == nil || state.Begin.OwnerPreparation == nil || state.Seed == nil ||
		replacementPhaseOrdinalV1(state.Phase) < replacementPhaseOrdinalV1(ReplicaReplacementAddIntentV1) ||
		!validReplicaReplacementDigestV1(receipt.QueryDigest) || !validReplicaReplacementDigestV1(receipt.ResultDigest) ||
		!validReplicaReplacementDigestV1(receipt.ReadySetDigest) || receipt.IssuerNode == "" || receipt.IssuerNode == state.Begin.NewPeer.ID ||
		receipt.ReadTerm == 0 || receipt.ReadIndex == 0 || receipt.TargetAppliedIndex < receipt.ReadIndex ||
		receipt.Tail.Validate() != nil || receipt.Tail.GroupID != state.Begin.GroupID ||
		receipt.Tail.CommitIndex < receipt.ReadIndex || receipt.ReadTerm > receipt.Tail.LeaderTerm || receipt.Tail.ConfigurationIndex <= state.Seed.ConfigurationIndex {
		return ErrInvalidCatalogMeta
	}
	_, seedIndex := state.Seed.Manifest.CommandBoundaryV1()
	if receipt.Tail.Progress.EntryID.Index < seedIndex {
		return ErrInvalidCatalogMeta
	}
	return nil
}

func EncodeReplicaReplacementOwnerQualificationCommandV1(command ReplicaReplacementOwnerQualificationCommandV1) ([]byte, error) {
	command.Format, command.Kind = CatalogMetaFormatV1, ReplicaReplacementOwnerQualificationKindV1
	if command.State.Phase != ReplicaReplacementAddIntentV1 || command.State.OwnerQualification == nil {
		return nil, ErrInvalidCatalogMeta
	}
	state, err := EncodeReplicaReplacementStateV1(command.State)
	if err != nil {
		return nil, err
	}
	command.State, err = DecodeReplicaReplacementStateV1(state)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(command)
	if len(raw) > maxReplicaReplacementCommandBytesV1 {
		return nil, ErrCatalogMetaLimit
	}
	return raw, err
}

func DecodeReplicaReplacementOwnerQualificationCommandV1(raw []byte) (ReplicaReplacementOwnerQualificationCommandV1, error) {
	var command ReplicaReplacementOwnerQualificationCommandV1
	if len(raw) == 0 || len(raw) > maxReplicaReplacementCommandBytesV1 {
		return command, ErrCatalogMetaLimit
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		return command, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return command, ErrInvalidCatalogMeta
	}
	if command.Format != CatalogMetaFormatV1 || command.Kind != ReplicaReplacementOwnerQualificationKindV1 {
		return command, ErrInvalidCatalogMeta
	}
	canonical, err := EncodeReplicaReplacementOwnerQualificationCommandV1(command)
	if err != nil {
		return command, err
	}
	if !bytes.Equal(raw, canonical) {
		return command, ErrInvalidCatalogMeta
	}
	return command, nil
}

func (a *CatalogMetaAuthorityV1) applyCommittedReplicaReplacementOwnerQualificationV1(raw []byte, index uint64) (CatalogMetaStatusV1, error) {
	command, err := DecodeReplicaReplacementOwnerQualificationCommandV1(raw)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	next := command.State
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := validateReplicaReplacementCatalogV1(next.Begin, a.record); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if err := validateReplicaReplacementOwnerPreparationPhaseV1(a.record, a.lifecycle, next); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	old, err := decodeReplicaReplacementCurrentV1(a.replacements[next.Begin.GroupID])
	if err != nil || old.Phase != ReplicaReplacementAddIntentV1 || !replicaReplacementStateExtendsV1(old, next) {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	// Receipt is the sole mutation; caller bytes cannot replace seed/roster.
	oldWithout, nextWithout := old, next
	oldWithout.OwnerQualification, nextWithout.OwnerQualification = nil, nil
	left, err := EncodeReplicaReplacementStateV1(oldWithout)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	right, err := EncodeReplicaReplacementStateV1(nextWithout)
	if err != nil || !bytes.Equal(left, right) {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	if old.OwnerQualification != nil {
		if *old.OwnerQualification != *next.OwnerQualification {
			return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
		}
		return a.statusLocked(), nil
	}
	if index <= a.applied {
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	encoded, err := EncodeReplicaReplacementStateV1(next)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	candidate := make(map[raftcluster.GroupID][]byte, len(a.replacements))
	for group, value := range a.replacements {
		candidate[group] = value
	}
	candidate[next.Begin.GroupID] = bytes.Clone(encoded)
	return a.installReplicaReplacementCandidateLockedV1(candidate, index)
}
