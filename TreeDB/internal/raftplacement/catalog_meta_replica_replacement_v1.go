package raftplacement

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	ReplicaReplacementBeginKindV1        = "replica-replacement-begin-v1"
	maxReplicaReplacementCommandBytesV1  = 8 << 10
	maxReplicaReplacementSnapshotBytesV1 = MaxCatalogMetaGroupsV1 * maxReplicaReplacementCommandBytesV1
)

// ReplicaReplacementBeginV1 authorizes preparation only. It neither changes
// catalog ownership nor certifies learner suffrage, snapshot installation, or
// durable tail readiness. The production caller must derive the new peer from
// the immutable node/security configuration before submitting this command.
// Membership and readiness remain separate, subsequently verified operations.
type ReplicaReplacementBeginV1 struct {
	Format        uint16              `json:"format"`
	Kind          string              `json:"kind"`
	OperationID   string              `json:"operation_id"`
	ConfigDigest  string              `json:"config_digest"`
	ExpectedEpoch uint64              `json:"expected_epoch"`
	CatalogDigest string              `json:"catalog_digest"`
	GroupID       raftcluster.GroupID `json:"group_id"`
	OldNodeID     raftcluster.NodeID  `json:"old_node_id"`
	NewPeer       raftcluster.Peer    `json:"new_peer"`
}

func replicaReplacementCommandBytesV1(raw []byte) bool {
	if len(raw) == 0 || len(raw) > MaxCatalogMetaCommandBytesV1 {
		return false
	}
	var envelope struct {
		Kind string `json:"kind"`
	}
	return json.Unmarshal(raw, &envelope) == nil && strings.HasPrefix(envelope.Kind, "replica-replacement-")
}

func validReplicaReplacementStringV1(value string) bool {
	return value != "" && len(value) <= MaxCatalogMetaStringBytesV1 && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func validReplicaReplacementDigestV1(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func validateReplicaReplacementBeginV1(command ReplicaReplacementBeginV1) error {
	if command.Format != CatalogMetaFormatV1 || command.Kind != ReplicaReplacementBeginKindV1 || command.ExpectedEpoch == 0 ||
		!validReplicaReplacementStringV1(command.OperationID) || !validReplicaReplacementStringV1(string(command.GroupID)) ||
		!validReplicaReplacementStringV1(string(command.OldNodeID)) || !validReplicaReplacementStringV1(string(command.NewPeer.ID)) ||
		command.OldNodeID == command.NewPeer.ID || !validReplicaReplacementDigestV1(command.ConfigDigest) || !validReplicaReplacementDigestV1(command.CatalogDigest) {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("invalid replica replacement identity"))
	}
	host, port, err := net.SplitHostPort(command.NewPeer.Address)
	portNumber, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || portErr != nil || portNumber == 0 || host == "" || !validReplicaReplacementStringV1(command.NewPeer.Address) {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("invalid replica replacement address"))
	}
	// Capabilities come from the anchored runtime configuration, never from a
	// caller's assertion inside a replacement command.
	if command.NewPeer.Capabilities.ConfigVersion != (raftcluster.Version{}) || len(command.NewPeer.Capabilities.Required) != 0 {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("replacement peer capabilities must be derived from configuration"))
	}
	return nil
}

func EncodeReplicaReplacementBeginV1(command ReplicaReplacementBeginV1) ([]byte, error) {
	command.Format, command.Kind = CatalogMetaFormatV1, ReplicaReplacementBeginKindV1
	if err := validateReplicaReplacementBeginV1(command); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxReplicaReplacementCommandBytesV1 {
		return nil, ErrCatalogMetaLimit
	}
	return raw, nil
}

func DecodeReplicaReplacementBeginV1(raw []byte) (ReplicaReplacementBeginV1, error) {
	var command ReplicaReplacementBeginV1
	if len(raw) == 0 || len(raw) > maxReplicaReplacementCommandBytesV1 {
		return command, ErrCatalogMetaLimit
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		return command, errors.Join(ErrInvalidCatalogMeta, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return command, ErrInvalidCatalogMeta
	}
	if err := validateReplicaReplacementBeginV1(command); err != nil {
		return command, err
	}
	canonical, err := EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return command, err
	}
	if !bytes.Equal(canonical, raw) {
		return command, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("replacement command is not canonical"))
	}
	return command, nil
}

func validateReplicaReplacementCatalogV1(command ReplicaReplacementBeginV1, record CatalogMetaRecordV1) error {
	if command.ExpectedEpoch != record.Epoch || command.CatalogDigest != record.Digest {
		return ErrCatalogMetaStaleEpoch
	}
	if catalogMetaFeatureEnabledV1(record.Catalog.Features, raftcluster.FeatureVectorPartitionLifecycle) {
		return errors.Join(ErrUnsupportedFeature, fmt.Errorf("lifecycle-bearing replica replacement is not activated"))
	}
	for _, group := range record.Catalog.Groups {
		if group.ID != command.GroupID {
			continue
		}
		old := false
		for _, member := range group.Members {
			if member == command.NewPeer.ID {
				return errors.Join(ErrCatalogMetaConflict, fmt.Errorf("replacement target is already a member"))
			}
			old = old || member == command.OldNodeID
		}
		if old {
			return nil
		}
		return errors.Join(ErrCatalogMetaConflict, fmt.Errorf("old replica is not a member"))
	}
	return errors.Join(ErrCatalogMetaConflict, fmt.Errorf("replacement group is absent"))
}

func (a *CatalogMetaAuthorityV1) applyCommittedReplicaReplacementV1(raw []byte, appliedIndex uint64) (CatalogMetaStatusV1, error) {
	command, err := DecodeReplicaReplacementBeginV1(raw)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.record.Epoch == 0 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaUnavailable
	}
	if err := validateReplicaReplacementCatalogV1(command, a.record); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if err := a.validateVectorPartitionLifecycleCatalogTransitionLockedV1(); err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if existing := a.replacements[command.GroupID]; existing != nil {
		if bytes.Equal(existing, raw) {
			return a.statusLocked(), nil
		}
		return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
	}
	for _, existing := range a.replacements {
		previous, err := DecodeReplicaReplacementBeginV1(existing)
		if err != nil {
			return CatalogMetaStatusV1{}, err
		}
		if previous.OperationID == command.OperationID || previous.ConfigDigest != command.ConfigDigest {
			return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
		}
	}
	if len(a.replacements) >= MaxCatalogMetaGroupsV1 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaLimit
	}
	candidate := make(map[raftcluster.GroupID][]byte, len(a.replacements)+1)
	for group, value := range a.replacements {
		candidate[group] = value
	}
	candidate[command.GroupID] = bytes.Clone(raw)
	encoded, err := encodeReplicaReplacementSnapshotV1(candidate)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	lifecycle, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	snapshot := CatalogMetaSnapshotV1{Format: CatalogMetaFormatV1, AppliedIndex: appliedIndex, Record: a.recordBytes, LastCommand: a.command, VectorPartitionLifecycle: lifecycle, ReplicaReplacements: encoded}
	if raw, err := json.Marshal(snapshot); err != nil {
		return CatalogMetaStatusV1{}, err
	} else if len(raw) > MaxCatalogMetaSnapshotBytesV1 {
		return CatalogMetaStatusV1{}, ErrCatalogMetaLimit
	}
	a.replacements, a.replacementBytes, a.applied = candidate, uint64(len(encoded)), appliedIndex
	return a.statusLocked(), nil
}

// ReplicaReplacementBeginsV1 returns owned, bounded immutable preparation
// records. Callers still need a current quorum/catalog proof before acting.
func (a *CatalogMetaAuthorityV1) ReplicaReplacementBeginsV1() ([]ReplicaReplacementBeginV1, error) {
	if a == nil {
		return nil, ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	raw, err := encodeReplicaReplacementSnapshotV1(a.replacements)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var commands []ReplicaReplacementBeginV1
	if err := json.Unmarshal(raw, &commands); err != nil {
		return nil, err
	}
	return commands, nil
}

func encodeReplicaReplacementSnapshotV1(records map[raftcluster.GroupID][]byte) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) > MaxCatalogMetaGroupsV1 {
		return nil, ErrCatalogMetaLimit
	}
	groups := make([]raftcluster.GroupID, 0, len(records))
	for group := range records {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	commands := make([]json.RawMessage, 0, len(groups))
	for _, group := range groups {
		commands = append(commands, json.RawMessage(records[group]))
	}
	raw, err := json.Marshal(commands)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxReplicaReplacementSnapshotBytesV1 {
		return nil, ErrCatalogMetaLimit
	}
	return raw, nil
}

func decodeReplicaReplacementSnapshotV1(raw []byte, record CatalogMetaRecordV1) (map[raftcluster.GroupID][]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > maxReplicaReplacementSnapshotBytesV1 {
		return nil, ErrCatalogMetaLimit
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return nil, ErrInvalidCatalogMeta
	}
	records := make(map[raftcluster.GroupID][]byte)
	operationIDs := make(map[string]bool)
	var configDigest string
	for decoder.More() {
		if len(records) >= MaxCatalogMetaGroupsV1 {
			return nil, ErrCatalogMetaLimit
		}
		var commandBytes json.RawMessage
		if err := decoder.Decode(&commandBytes); err != nil {
			return nil, err
		}
		command, err := DecodeReplicaReplacementBeginV1(commandBytes)
		if err != nil {
			return nil, err
		}
		if err := validateReplicaReplacementCatalogV1(command, record); err != nil {
			return nil, err
		}
		if records[command.GroupID] != nil || operationIDs[command.OperationID] || (configDigest != "" && configDigest != command.ConfigDigest) {
			return nil, ErrCatalogMetaConflict
		}
		configDigest = command.ConfigDigest
		records[command.GroupID], operationIDs[command.OperationID] = bytes.Clone(commandBytes), true
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
		return nil, ErrInvalidCatalogMeta
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidCatalogMeta
	}
	canonical, err := encodeReplicaReplacementSnapshotV1(records)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, raw) {
		return nil, ErrInvalidCatalogMeta
	}
	return records, nil
}
