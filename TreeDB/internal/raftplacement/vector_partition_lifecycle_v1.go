package raftplacement

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	VectorPartitionLifecycleFormatV1          uint16 = 1
	MaxVectorPartitionLifecycleCommandBytesV1        = 1 << 20
	MaxVectorPartitionLifecycleRecordBytesV1         = 1 << 20
	MaxVectorPartitionLifecycleGroupsV1              = MaxCatalogMetaGroupsV1
	MaxVectorPartitionLifecycleReasonBytesV1         = 256
)

var (
	ErrInvalidVectorPartitionLifecycle  = errors.New("raftplacement: invalid vector partition lifecycle")
	ErrVectorPartitionLifecycleLimit    = errors.New("raftplacement: vector partition lifecycle limit")
	ErrVectorPartitionLifecycleState    = errors.New("raftplacement: vector partition lifecycle state transition")
	ErrVectorPartitionLifecycleIdentity = errors.New("raftplacement: vector partition lifecycle identity mismatch")
	ErrVectorPartitionLifecycleStale    = errors.New("raftplacement: stale vector partition lifecycle command")
	ErrVectorPartitionLifecycleConflict = errors.New("raftplacement: vector partition lifecycle conflict")
	ErrVectorPartitionLifecycleGuard    = errors.New("raftplacement: vector partition lifecycle guard refused")
)

// VectorPartitionLifecycleStateV1 is the replicated state of one derived
// generation. Absent is the zero state and the terminal state after cleanup.
type VectorPartitionLifecycleStateV1 string

const (
	VectorPartitionLifecycleAbsentV1      VectorPartitionLifecycleStateV1 = "absent"
	VectorPartitionLifecycleBuildingV1    VectorPartitionLifecycleStateV1 = "building"
	VectorPartitionLifecycleStagedV1      VectorPartitionLifecycleStateV1 = "staged"
	VectorPartitionLifecyclePreparedV1    VectorPartitionLifecycleStateV1 = "prepared"
	VectorPartitionLifecycleActiveV1      VectorPartitionLifecycleStateV1 = "active"
	VectorPartitionLifecycleInvalidatedV1 VectorPartitionLifecycleStateV1 = "invalidated"
	VectorPartitionLifecycleRetiredV1     VectorPartitionLifecycleStateV1 = "retired"
	VectorPartitionLifecycleCleanableV1   VectorPartitionLifecycleStateV1 = "cleanable"
)

type VectorPartitionLifecycleCommandKindV1 string

const (
	VectorPartitionLifecycleBeginBuildV1         VectorPartitionLifecycleCommandKindV1 = "begin_build"
	VectorPartitionLifecycleRecordGroupReadyV1   VectorPartitionLifecycleCommandKindV1 = "record_group_ready"
	VectorPartitionLifecyclePrepareV1            VectorPartitionLifecycleCommandKindV1 = "prepare"
	VectorPartitionLifecycleActivateV1           VectorPartitionLifecycleCommandKindV1 = "activate"
	VectorPartitionLifecycleAbortBuildV1         VectorPartitionLifecycleCommandKindV1 = "abort_build"
	VectorPartitionLifecycleInvalidateV1         VectorPartitionLifecycleCommandKindV1 = "invalidate"
	VectorPartitionLifecycleConfirmMutationV1    VectorPartitionLifecycleCommandKindV1 = "confirm_mutation"
	VectorPartitionLifecycleRetireV1             VectorPartitionLifecycleCommandKindV1 = "retire"
	VectorPartitionLifecycleMarkCleanableV1      VectorPartitionLifecycleCommandKindV1 = "mark_cleanable"
	VectorPartitionLifecycleRecordGroupCleanupV1 VectorPartitionLifecycleCommandKindV1 = "record_group_cleanup"
	VectorPartitionLifecycleCompleteCleanupV1    VectorPartitionLifecycleCommandKindV1 = "complete_cleanup"
)

// VectorPartitionLifecycleIndexIdentityV1 distinguishes collection recreation,
// index replacement, and catalog replacement. Every field participates in
// equality; no name-only fallback is permitted.
type VectorPartitionLifecycleIndexIdentityV1 struct {
	Collection            CollectionRefV1 `json:"collection"`
	CollectionIncarnation uint64          `json:"collection_incarnation"`
	IndexName             string          `json:"index_name"`
	IndexDefinitionDigest string          `json:"index_definition_digest"`
	IndexEpoch            uint64          `json:"index_epoch"`
	CatalogEpoch          uint64          `json:"catalog_epoch"`
	CatalogDigest         string          `json:"catalog_digest"`
}

type VectorPartitionLifecycleSourceIdentityV1 struct {
	Generation uint64 `json:"generation"`
	Checksum   uint64 `json:"checksum"`
	SchemaHash uint64 `json:"schema_hash"`
	RowCount   uint64 `json:"row_count"`
}

// VectorPartitionLifecycleIdentityV1 is the exact identity of one derived
// generation and its immutable source.
type VectorPartitionLifecycleIdentityV1 struct {
	Index      VectorPartitionLifecycleIndexIdentityV1  `json:"index"`
	Source     VectorPartitionLifecycleSourceIdentityV1 `json:"source"`
	Generation uint64                                   `json:"generation"`
}

// VectorPartitionLifecycleGroupReadyV1 is one bounded group-level aggregate.
// AssetSetDigest binds all partition/router assets declared for that group;
// the lifecycle record never carries an unbounded per-asset vector.
type VectorPartitionLifecycleGroupReadyV1 struct {
	GroupID        raftcluster.GroupID `json:"group_id"`
	AppliedIndex   uint64              `json:"applied_index"`
	AssetSetDigest string              `json:"asset_set_digest"`
}

// VectorPartitionLifecycleReferencesV1 is a point-in-time conservative
// reachability observation used only to move a retired generation to cleanable.
type VectorPartitionLifecycleReferencesV1 struct {
	ReaderPins         uint64 `json:"reader_pins"`
	SnapshotReferences uint64 `json:"snapshot_references"`
	BackupReferences   uint64 `json:"backup_references"`
	CatalogReferences  uint64 `json:"catalog_references"`
}

// VectorPartitionLifecycleSearchProofV1 is intentionally O(1): the local M5/M6
// admission path validates selected assets, while this guard binds that local
// evidence to the single replicated active ready-set.
type VectorPartitionLifecycleSearchProofV1 struct {
	Identity       VectorPartitionLifecycleIdentityV1 `json:"identity"`
	ReadySetDigest string                             `json:"ready_set_digest"`
}

// VectorPartitionLifecycleMutationProofV1 proves either that no generation is
// active or that the named active generation was durably invalidated first.
type VectorPartitionLifecycleMutationProofV1 struct {
	IndexIdentity     VectorPartitionLifecycleIndexIdentityV1 `json:"index_identity"`
	ActiveGeneration  uint64                                  `json:"active_generation"`
	InvalidationEpoch uint64                                  `json:"invalidation_epoch"`
}

// VectorPartitionLifecycleRecordV1 is a bounded, O(1)-friendly admission
// record. RequiredGroups and ReadyGroups are scanned only on build/cleanup
// transitions; steady-state guards compare fixed-size identity and digest data.
type VectorPartitionLifecycleRecordV1 struct {
	Format                   uint16                                 `json:"format"`
	Revision                 uint64                                 `json:"revision"`
	State                    VectorPartitionLifecycleStateV1        `json:"state"`
	Identity                 VectorPartitionLifecycleIdentityV1     `json:"identity"`
	PreviousActiveGeneration uint64                                 `json:"previous_active_generation"`
	MutationEpoch            uint64                                 `json:"mutation_epoch"`
	RequiredGroups           []raftcluster.GroupID                  `json:"required_groups"`
	ReadyGroups              []VectorPartitionLifecycleGroupReadyV1 `json:"ready_groups"`
	ReadySetDigest           string                                 `json:"ready_set_digest"`
	InvalidationReason       string                                 `json:"invalidation_reason"`
	InvalidationEpoch        uint64                                 `json:"invalidation_epoch"`
	MutationConfirmed        bool                                   `json:"mutation_confirmed"`
	Aborted                  bool                                   `json:"aborted"`
	RetirementReason         string                                 `json:"retirement_reason"`
	SupersededByGeneration   uint64                                 `json:"superseded_by_generation"`
	CleanedGroups            []raftcluster.GroupID                  `json:"cleaned_groups"`
	CleanupComplete          bool                                   `json:"cleanup_complete"`
	LastCommandDigest        string                                 `json:"last_command_digest"`
}

// VectorPartitionLifecycleCommandV1 is a single deterministic envelope. Fields
// irrelevant to Kind must be zero, preventing ambiguous alternate encodings.
type VectorPartitionLifecycleCommandV1 struct {
	Format                   uint16                                `json:"format"`
	Kind                     VectorPartitionLifecycleCommandKindV1 `json:"kind"`
	ExpectedRevision         uint64                                `json:"expected_revision"`
	ExpectedState            VectorPartitionLifecycleStateV1       `json:"expected_state"`
	Identity                 VectorPartitionLifecycleIdentityV1    `json:"identity"`
	RequiredGroups           []raftcluster.GroupID                 `json:"required_groups"`
	PreviousActiveGeneration uint64                                `json:"previous_active_generation"`
	PreviousActiveRevision   uint64                                `json:"previous_active_revision"`
	MutationEpoch            uint64                                `json:"mutation_epoch"`
	GroupReady               VectorPartitionLifecycleGroupReadyV1  `json:"group_ready"`
	ReadySetDigest           string                                `json:"ready_set_digest"`
	Reason                   string                                `json:"reason"`
	InvalidationEpoch        uint64                                `json:"invalidation_epoch"`
	References               VectorPartitionLifecycleReferencesV1  `json:"references"`
	GroupID                  raftcluster.GroupID                   `json:"group_id"`
}

func EncodeVectorPartitionLifecycleCommandV1(command VectorPartitionLifecycleCommandV1) ([]byte, error) {
	canonical, err := canonicalVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if len(b) > MaxVectorPartitionLifecycleCommandBytesV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("command is %d bytes", len(b)))
	}
	return b, nil
}

func DecodeVectorPartitionLifecycleCommandV1(raw []byte) (VectorPartitionLifecycleCommandV1, error) {
	if len(raw) == 0 || len(raw) > MaxVectorPartitionLifecycleCommandBytesV1 {
		return VectorPartitionLifecycleCommandV1{}, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("command is %d bytes", len(raw)))
	}
	var command VectorPartitionLifecycleCommandV1
	if err := decodeVectorPartitionLifecycleJSONV1(raw, &command); err != nil {
		return VectorPartitionLifecycleCommandV1{}, err
	}
	canonical, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		return VectorPartitionLifecycleCommandV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return VectorPartitionLifecycleCommandV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("command is not canonical"))
	}
	return command, nil
}

func EncodeVectorPartitionLifecycleRecordV1(record VectorPartitionLifecycleRecordV1) ([]byte, error) {
	canonical, err := canonicalVectorPartitionLifecycleRecordV1(record)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(canonical)
	if err != nil {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if len(b) > MaxVectorPartitionLifecycleRecordBytesV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("record is %d bytes", len(b)))
	}
	return b, nil
}

func DecodeVectorPartitionLifecycleRecordV1(raw []byte) (VectorPartitionLifecycleRecordV1, error) {
	if len(raw) == 0 || len(raw) > MaxVectorPartitionLifecycleRecordBytesV1 {
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("record is %d bytes", len(raw)))
	}
	var record VectorPartitionLifecycleRecordV1
	if err := decodeVectorPartitionLifecycleJSONV1(raw, &record); err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	canonical, err := EncodeVectorPartitionLifecycleRecordV1(record)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("record is not canonical"))
	}
	return record, nil
}

// ApplyVectorPartitionLifecycleCommandV1 is a deterministic pure reducer.
// Exact retry of the last committed command is a no-op. Every other command is
// revision-, state-, and identity-guarded before any state is changed.
func ApplyVectorPartitionLifecycleCommandV1(record VectorPartitionLifecycleRecordV1, command VectorPartitionLifecycleCommandV1) (VectorPartitionLifecycleRecordV1, error) {
	current, err := canonicalVectorPartitionLifecycleRecordV1(record)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	commandBytes, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	command, err = DecodeVectorPartitionLifecycleCommandV1(commandBytes)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	commandDigest := sha256HexVectorPartitionLifecycleV1(commandBytes)
	if current.LastCommandDigest != "" && current.LastCommandDigest == commandDigest {
		return current, nil
	}
	if current.Revision != command.ExpectedRevision {
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleStale, fmt.Errorf("revision=%d want %d", command.ExpectedRevision, current.Revision))
	}
	if current.State != command.ExpectedState {
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleState, fmt.Errorf("state=%q want %q", current.State, command.ExpectedState))
	}
	if command.Kind != VectorPartitionLifecycleBeginBuildV1 && current.Identity != command.Identity {
		return VectorPartitionLifecycleRecordV1{}, ErrVectorPartitionLifecycleIdentity
	}

	next := current
	switch command.Kind {
	case VectorPartitionLifecycleBeginBuildV1:
		if !zeroVectorPartitionLifecycleIdentityV1(current.Identity) || current.Revision != 0 {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleConflict, fmt.Errorf("build slot is not empty"))
		}
		next = VectorPartitionLifecycleRecordV1{
			Format:                   VectorPartitionLifecycleFormatV1,
			State:                    VectorPartitionLifecycleBuildingV1,
			Identity:                 command.Identity,
			PreviousActiveGeneration: command.PreviousActiveGeneration,
			MutationEpoch:            command.MutationEpoch,
			RequiredGroups:           append([]raftcluster.GroupID(nil), command.RequiredGroups...),
			ReadyGroups:              []VectorPartitionLifecycleGroupReadyV1{},
			CleanedGroups:            []raftcluster.GroupID{},
		}
	case VectorPartitionLifecycleRecordGroupReadyV1:
		if !containsVectorPartitionLifecycleGroupV1(current.RequiredGroups, command.GroupReady.GroupID) {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleIdentity, fmt.Errorf("group %q is not required", command.GroupReady.GroupID))
		}
		if i, ok := findVectorPartitionLifecycleReadyGroupV1(current.ReadyGroups, command.GroupReady.GroupID); ok {
			if current.ReadyGroups[i] == command.GroupReady {
				return current, nil
			}
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleConflict, fmt.Errorf("group %q readiness changed", command.GroupReady.GroupID))
		}
		next.ReadyGroups = append(append([]VectorPartitionLifecycleGroupReadyV1(nil), current.ReadyGroups...), command.GroupReady)
		sort.Slice(next.ReadyGroups, func(i, j int) bool { return next.ReadyGroups[i].GroupID < next.ReadyGroups[j].GroupID })
		next.State = VectorPartitionLifecycleStagedV1
	case VectorPartitionLifecyclePrepareV1:
		if err := current.CanPrepare(command.Identity, command.ReadySetDigest); err != nil {
			return VectorPartitionLifecycleRecordV1{}, err
		}
		next.State = VectorPartitionLifecyclePreparedV1
		next.ReadySetDigest = command.ReadySetDigest
	case VectorPartitionLifecycleActivateV1:
		if command.PreviousActiveGeneration != 0 {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("activation with a previous active generation requires atomic cutover"))
		}
		if err := current.CanActivate(command.Identity, command.PreviousActiveGeneration, command.MutationEpoch); err != nil {
			return VectorPartitionLifecycleRecordV1{}, err
		}
		next.State = VectorPartitionLifecycleActiveV1
	case VectorPartitionLifecycleAbortBuildV1:
		next.State = VectorPartitionLifecycleRetiredV1
		next.Aborted = true
		next.RetirementReason = command.Reason
	case VectorPartitionLifecycleInvalidateV1:
		if command.InvalidationEpoch <= current.MutationEpoch {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("invalidation epoch %d does not follow captured mutation epoch %d", command.InvalidationEpoch, current.MutationEpoch))
		}
		next.State = VectorPartitionLifecycleInvalidatedV1
		next.InvalidationReason = command.Reason
		next.InvalidationEpoch = command.InvalidationEpoch
	case VectorPartitionLifecycleConfirmMutationV1:
		if current.InvalidationEpoch == 0 || current.InvalidationEpoch != command.MutationEpoch {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("mutation confirmation does not match invalidation fence"))
		}
		next.MutationConfirmed = true
	case VectorPartitionLifecycleRetireV1:
		next.State = VectorPartitionLifecycleRetiredV1
	case VectorPartitionLifecycleMarkCleanableV1:
		if err := current.CanClean(command.References); err != nil {
			return VectorPartitionLifecycleRecordV1{}, err
		}
		next.State = VectorPartitionLifecycleCleanableV1
	case VectorPartitionLifecycleRecordGroupCleanupV1:
		if !containsVectorPartitionLifecycleGroupV1(current.RequiredGroups, command.GroupID) {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleIdentity, fmt.Errorf("group %q is not required", command.GroupID))
		}
		if containsVectorPartitionLifecycleGroupV1(current.CleanedGroups, command.GroupID) {
			return current, nil
		}
		next.CleanedGroups = append(append([]raftcluster.GroupID(nil), current.CleanedGroups...), command.GroupID)
		sort.Slice(next.CleanedGroups, func(i, j int) bool { return next.CleanedGroups[i] < next.CleanedGroups[j] })
	case VectorPartitionLifecycleCompleteCleanupV1:
		if len(current.CleanedGroups) != len(current.RequiredGroups) {
			return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("cleaned groups=%d required=%d", len(current.CleanedGroups), len(current.RequiredGroups)))
		}
		next.State = VectorPartitionLifecycleAbsentV1
		next.RequiredGroups = []raftcluster.GroupID{}
		next.ReadyGroups = []VectorPartitionLifecycleGroupReadyV1{}
		next.ReadySetDigest = ""
		next.CleanedGroups = []raftcluster.GroupID{}
		next.CleanupComplete = true
	default:
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("unknown command kind %q", command.Kind))
	}
	next.Revision = current.Revision + 1
	next.LastCommandDigest = commandDigest
	return canonicalVectorPartitionLifecycleRecordV1(next)
}

// ApplyVectorPartitionLifecycleCutoverV1 is the only activation path when a
// previous generation is active. The authority must publish both returned
// records in one meta-log replacement: the candidate becomes active exactly
// when the previous record becomes retired. Supersession is not mutation
// invalidation and cannot authorize a relevant mutation.
func ApplyVectorPartitionLifecycleCutoverV1(previous, candidate VectorPartitionLifecycleRecordV1, command VectorPartitionLifecycleCommandV1) (VectorPartitionLifecycleRecordV1, VectorPartitionLifecycleRecordV1, error) {
	oldRecord, err := canonicalVectorPartitionLifecycleRecordV1(previous)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}
	newRecord, err := canonicalVectorPartitionLifecycleRecordV1(candidate)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}
	commandBytes, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}
	command, err = DecodeVectorPartitionLifecycleCommandV1(commandBytes)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}
	if command.Kind != VectorPartitionLifecycleActivateV1 || command.PreviousActiveGeneration == 0 || command.PreviousActiveRevision == 0 {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("cutover requires an activate command with a previous active guard"))
	}
	commandDigest := sha256HexVectorPartitionLifecycleV1(commandBytes)
	if oldRecord.LastCommandDigest == commandDigest && newRecord.LastCommandDigest == commandDigest &&
		oldRecord.State == VectorPartitionLifecycleRetiredV1 &&
		oldRecord.SupersededByGeneration == newRecord.Identity.Generation &&
		newRecord.State == VectorPartitionLifecycleActiveV1 {
		return oldRecord, newRecord, nil
	}
	if newRecord.Revision != command.ExpectedRevision {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleStale, fmt.Errorf("candidate revision=%d want %d", command.ExpectedRevision, newRecord.Revision))
	}
	if newRecord.State != command.ExpectedState {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleState, fmt.Errorf("candidate state=%q want %q", newRecord.State, command.ExpectedState))
	}
	if newRecord.Identity != command.Identity || oldRecord.Identity.Index != newRecord.Identity.Index {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, ErrVectorPartitionLifecycleIdentity
	}
	if oldRecord.State != VectorPartitionLifecycleActiveV1 ||
		oldRecord.Identity.Generation != command.PreviousActiveGeneration ||
		oldRecord.Revision != command.PreviousActiveRevision {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, errors.Join(ErrVectorPartitionLifecycleStale, fmt.Errorf("previous active generation or revision changed"))
	}
	if err := newRecord.CanActivate(command.Identity, command.PreviousActiveGeneration, command.MutationEpoch); err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}

	nextOld := oldRecord
	nextOld.State = VectorPartitionLifecycleRetiredV1
	nextOld.SupersededByGeneration = newRecord.Identity.Generation
	nextOld.Revision++
	nextOld.LastCommandDigest = commandDigest
	nextOld, err = canonicalVectorPartitionLifecycleRecordV1(nextOld)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}
	nextNew := newRecord
	nextNew.State = VectorPartitionLifecycleActiveV1
	nextNew.Revision++
	nextNew.LastCommandDigest = commandDigest
	nextNew, err = canonicalVectorPartitionLifecycleRecordV1(nextNew)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleRecordV1{}, err
	}
	return nextOld, nextNew, nil
}

func (r VectorPartitionLifecycleRecordV1) CanPrepare(identity VectorPartitionLifecycleIdentityV1, readySetDigest string) error {
	if r.State != VectorPartitionLifecycleStagedV1 {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("prepare requires staged state"))
	}
	if r.Identity != identity {
		return ErrVectorPartitionLifecycleIdentity
	}
	if len(r.ReadyGroups) != len(r.RequiredGroups) {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("ready groups=%d required=%d", len(r.ReadyGroups), len(r.RequiredGroups)))
	}
	digest, err := VectorPartitionLifecycleReadySetDigestV1(r.Identity, r.RequiredGroups, r.ReadyGroups)
	if err != nil {
		return err
	}
	if readySetDigest == "" || readySetDigest != digest {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("ready-set digest mismatch"))
	}
	return nil
}

func (r VectorPartitionLifecycleRecordV1) CanActivate(identity VectorPartitionLifecycleIdentityV1, previousActiveGeneration, mutationEpoch uint64) error {
	if r.State != VectorPartitionLifecyclePreparedV1 {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("activate requires prepared state"))
	}
	if r.Identity != identity {
		return ErrVectorPartitionLifecycleIdentity
	}
	if r.PreviousActiveGeneration != previousActiveGeneration || r.MutationEpoch != mutationEpoch {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("activation predecessor or mutation epoch changed"))
	}
	if r.ReadySetDigest == "" {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("prepared ready-set digest is missing"))
	}
	return nil
}

func (r VectorPartitionLifecycleRecordV1) CanSearch(proof VectorPartitionLifecycleSearchProofV1) error {
	if r.State != VectorPartitionLifecycleActiveV1 || r.InvalidationEpoch != 0 {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("search requires non-invalidated active state"))
	}
	if r.Identity != proof.Identity {
		return ErrVectorPartitionLifecycleIdentity
	}
	if r.ReadySetDigest == "" || r.ReadySetDigest != proof.ReadySetDigest {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("active ready-set digest mismatch"))
	}
	return nil
}

func (r VectorPartitionLifecycleRecordV1) CanCommitRelevantMutation(proof VectorPartitionLifecycleMutationProofV1) error {
	if err := validateVectorPartitionLifecycleIndexIdentityV1(proof.IndexIdentity); err != nil {
		return err
	}
	if r.State == VectorPartitionLifecycleAbsentV1 && zeroVectorPartitionLifecycleIdentityV1(r.Identity) {
		if proof.ActiveGeneration == 0 && proof.InvalidationEpoch == 0 {
			return nil
		}
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("absent lifecycle does not permit a named active generation"))
	}
	if indexIdentityVectorPartitionLifecycleV1(r.Identity) != proof.IndexIdentity {
		return ErrVectorPartitionLifecycleIdentity
	}
	switch r.State {
	case VectorPartitionLifecycleInvalidatedV1, VectorPartitionLifecycleRetiredV1, VectorPartitionLifecycleCleanableV1:
		if r.Aborted || proof.ActiveGeneration != r.Identity.Generation || r.InvalidationEpoch == 0 || proof.InvalidationEpoch != r.InvalidationEpoch {
			return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("active generation lacks exact durable invalidation"))
		}
		return nil
	case VectorPartitionLifecycleAbsentV1:
		if proof.ActiveGeneration == 0 && proof.InvalidationEpoch == 0 {
			return nil
		}
	default:
	}
	return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("generation state %q does not prove invalidation-before-mutation", r.State))
}

func (r VectorPartitionLifecycleRecordV1) CanClean(references VectorPartitionLifecycleReferencesV1) error {
	if r.State != VectorPartitionLifecycleRetiredV1 {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("cleanup requires retired state"))
	}
	if references.ReaderPins != 0 || references.SnapshotReferences != 0 ||
		references.BackupReferences != 0 || references.CatalogReferences != 0 {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("generation remains referenced"))
	}
	return nil
}

// VectorPartitionLifecycleReadySetDigestV1 returns the SHA-256 of a canonical,
// complete required-group/readiness payload. Inputs may be unordered; duplicate,
// missing, extra, or invalid readiness fails closed.
func VectorPartitionLifecycleReadySetDigestV1(identity VectorPartitionLifecycleIdentityV1, required []raftcluster.GroupID, ready []VectorPartitionLifecycleGroupReadyV1) (string, error) {
	if err := validateVectorPartitionLifecycleIdentityV1(identity); err != nil {
		return "", err
	}
	groups, err := canonicalVectorPartitionLifecycleGroupsV1(required)
	if err != nil {
		return "", err
	}
	readiness, err := canonicalVectorPartitionLifecycleReadyGroupsV1(ready)
	if err != nil {
		return "", err
	}
	if len(groups) != len(readiness) {
		return "", errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("ready groups=%d required=%d", len(readiness), len(groups)))
	}
	for i := range groups {
		if readiness[i].GroupID != groups[i] {
			return "", errors.Join(ErrVectorPartitionLifecycleIdentity, fmt.Errorf("ready group %q does not match required group %q", readiness[i].GroupID, groups[i]))
		}
	}
	payload := struct {
		Format         uint16                                 `json:"format"`
		Identity       VectorPartitionLifecycleIdentityV1     `json:"identity"`
		RequiredGroups []raftcluster.GroupID                  `json:"required_groups"`
		ReadyGroups    []VectorPartitionLifecycleGroupReadyV1 `json:"ready_groups"`
	}{
		Format: VectorPartitionLifecycleFormatV1, Identity: identity,
		RequiredGroups: groups, ReadyGroups: readiness,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	return sha256HexVectorPartitionLifecycleV1(b), nil
}

func canonicalVectorPartitionLifecycleCommandV1(command VectorPartitionLifecycleCommandV1) (VectorPartitionLifecycleCommandV1, error) {
	command.Format = VectorPartitionLifecycleFormatV1
	if err := validateVectorPartitionLifecycleIdentityV1(command.Identity); err != nil {
		return VectorPartitionLifecycleCommandV1{}, err
	}
	if !validVectorPartitionLifecycleStateV1(command.ExpectedState) {
		return VectorPartitionLifecycleCommandV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid expected state %q", command.ExpectedState))
	}
	command.RequiredGroups = append([]raftcluster.GroupID(nil), command.RequiredGroups...)
	if command.RequiredGroups == nil {
		command.RequiredGroups = []raftcluster.GroupID{}
	}
	var err error
	if len(command.RequiredGroups) != 0 {
		command.RequiredGroups, err = canonicalVectorPartitionLifecycleGroupsV1(command.RequiredGroups)
		if err != nil {
			return VectorPartitionLifecycleCommandV1{}, err
		}
	}
	if err := validateVectorPartitionLifecycleCommandShapeV1(command); err != nil {
		return VectorPartitionLifecycleCommandV1{}, err
	}
	return command, nil
}

func validateVectorPartitionLifecycleCommandShapeV1(c VectorPartitionLifecycleCommandV1) error {
	zeroReady := VectorPartitionLifecycleGroupReadyV1{}
	zeroRefs := VectorPartitionLifecycleReferencesV1{}
	if c.Kind != VectorPartitionLifecycleActivateV1 && c.PreviousActiveRevision != 0 {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("previous active revision is only valid for activate"))
	}
	otherZero := func(previous, mutation uint64, ready VectorPartitionLifecycleGroupReadyV1, digest, reason string, invalidation uint64, refs VectorPartitionLifecycleReferencesV1, group raftcluster.GroupID) bool {
		return len(c.RequiredGroups) == 0 && previous == 0 && mutation == 0 && ready == zeroReady &&
			digest == "" && reason == "" && invalidation == 0 && refs == zeroRefs && group == ""
	}
	switch c.Kind {
	case VectorPartitionLifecycleBeginBuildV1:
		if c.ExpectedRevision != 0 || c.ExpectedState != VectorPartitionLifecycleAbsentV1 || len(c.RequiredGroups) == 0 ||
			c.MutationEpoch == 0 || (c.PreviousActiveGeneration != 0 && c.Identity.Generation <= c.PreviousActiveGeneration) ||
			c.GroupReady != zeroReady || c.ReadySetDigest != "" || c.Reason != "" || c.InvalidationEpoch != 0 || c.References != zeroRefs || c.GroupID != "" {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid begin-build command shape"))
		}
	case VectorPartitionLifecycleRecordGroupReadyV1:
		if c.ExpectedState != VectorPartitionLifecycleBuildingV1 && c.ExpectedState != VectorPartitionLifecycleStagedV1 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("group readiness requires building or staged expected state"))
		}
		if err := validateVectorPartitionLifecycleGroupReadyV1(c.GroupReady); err != nil {
			return err
		}
		if !otherZero(c.PreviousActiveGeneration, c.MutationEpoch, zeroReady, c.ReadySetDigest, c.Reason, c.InvalidationEpoch, c.References, c.GroupID) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid group-ready command shape"))
		}
	case VectorPartitionLifecyclePrepareV1:
		if c.ExpectedState != VectorPartitionLifecycleStagedV1 || !isSHA256HexVectorPartitionV1(c.ReadySetDigest) ||
			!otherZero(c.PreviousActiveGeneration, c.MutationEpoch, c.GroupReady, "", c.Reason, c.InvalidationEpoch, c.References, c.GroupID) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid prepare command shape"))
		}
	case VectorPartitionLifecycleActivateV1:
		if c.ExpectedState != VectorPartitionLifecyclePreparedV1 || c.MutationEpoch == 0 ||
			(c.PreviousActiveGeneration == 0) != (c.PreviousActiveRevision == 0) ||
			c.GroupReady != zeroReady || c.ReadySetDigest != "" || c.Reason != "" || c.InvalidationEpoch != 0 || c.References != zeroRefs || c.GroupID != "" || len(c.RequiredGroups) != 0 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid activate command shape"))
		}
	case VectorPartitionLifecycleAbortBuildV1:
		if c.ExpectedState != VectorPartitionLifecycleBuildingV1 && c.ExpectedState != VectorPartitionLifecycleStagedV1 && c.ExpectedState != VectorPartitionLifecyclePreparedV1 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("abort requires building, staged, or prepared expected state"))
		}
		if err := validateVectorPartitionLifecycleReasonV1(c.Reason); err != nil ||
			!otherZero(c.PreviousActiveGeneration, c.MutationEpoch, c.GroupReady, c.ReadySetDigest, "", c.InvalidationEpoch, c.References, c.GroupID) {
			if err != nil {
				return err
			}
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid abort command shape"))
		}
	case VectorPartitionLifecycleInvalidateV1:
		if c.ExpectedState != VectorPartitionLifecycleActiveV1 || c.InvalidationEpoch == 0 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalidate requires active state and epoch"))
		}
		if err := validateVectorPartitionLifecycleReasonV1(c.Reason); err != nil ||
			!otherZero(c.PreviousActiveGeneration, c.MutationEpoch, c.GroupReady, c.ReadySetDigest, "", 0, c.References, c.GroupID) {
			if err != nil {
				return err
			}
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid invalidate command shape"))
		}
	case VectorPartitionLifecycleConfirmMutationV1:
		if c.ExpectedState != VectorPartitionLifecycleInvalidatedV1 || c.MutationEpoch == 0 ||
			!otherZero(c.PreviousActiveGeneration, 0, c.GroupReady, c.ReadySetDigest, c.Reason, c.InvalidationEpoch, c.References, c.GroupID) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid mutation-confirm command shape"))
		}
	case VectorPartitionLifecycleRetireV1:
		if c.ExpectedState != VectorPartitionLifecycleInvalidatedV1 ||
			!otherZero(c.PreviousActiveGeneration, c.MutationEpoch, c.GroupReady, c.ReadySetDigest, c.Reason, c.InvalidationEpoch, c.References, c.GroupID) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid retire command shape"))
		}
	case VectorPartitionLifecycleMarkCleanableV1:
		if c.ExpectedState != VectorPartitionLifecycleRetiredV1 ||
			len(c.RequiredGroups) != 0 || c.PreviousActiveGeneration != 0 || c.MutationEpoch != 0 || c.GroupReady != zeroReady ||
			c.ReadySetDigest != "" || c.Reason != "" || c.InvalidationEpoch != 0 || c.GroupID != "" {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid mark-cleanable command shape"))
		}
	case VectorPartitionLifecycleRecordGroupCleanupV1:
		if c.ExpectedState != VectorPartitionLifecycleCleanableV1 || c.GroupID == "" ||
			!otherZero(c.PreviousActiveGeneration, c.MutationEpoch, c.GroupReady, c.ReadySetDigest, c.Reason, c.InvalidationEpoch, c.References, "") {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid group-cleanup command shape"))
		}
		if err := validateID("group id", string(c.GroupID)); err != nil {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, err)
		}
		if len(c.GroupID) > MaxCatalogMetaStringBytesV1 {
			return errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("group id is %d bytes", len(c.GroupID)))
		}
	case VectorPartitionLifecycleCompleteCleanupV1:
		if c.ExpectedState != VectorPartitionLifecycleCleanableV1 ||
			!otherZero(c.PreviousActiveGeneration, c.MutationEpoch, c.GroupReady, c.ReadySetDigest, c.Reason, c.InvalidationEpoch, c.References, c.GroupID) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid complete-cleanup command shape"))
		}
	default:
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("unknown command kind %q", c.Kind))
	}
	return nil
}

func canonicalVectorPartitionLifecycleRecordV1(record VectorPartitionLifecycleRecordV1) (VectorPartitionLifecycleRecordV1, error) {
	if record.Format == 0 && record.Revision == 0 && record.State == "" &&
		zeroVectorPartitionLifecycleIdentityV1(record.Identity) &&
		record.PreviousActiveGeneration == 0 && record.MutationEpoch == 0 &&
		len(record.RequiredGroups) == 0 && len(record.ReadyGroups) == 0 &&
		record.ReadySetDigest == "" && record.InvalidationReason == "" &&
		record.InvalidationEpoch == 0 && !record.Aborted &&
		record.RetirementReason == "" && record.SupersededByGeneration == 0 &&
		len(record.CleanedGroups) == 0 &&
		!record.CleanupComplete && record.LastCommandDigest == "" {
		return VectorPartitionLifecycleRecordV1{
			Format: VectorPartitionLifecycleFormatV1, State: VectorPartitionLifecycleAbsentV1,
			RequiredGroups: []raftcluster.GroupID{}, ReadyGroups: []VectorPartitionLifecycleGroupReadyV1{},
			CleanedGroups: []raftcluster.GroupID{},
		}, nil
	}
	if record.Format != VectorPartitionLifecycleFormatV1 {
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("record format=%d", record.Format))
	}
	if err := validateVectorPartitionLifecycleIdentityV1(record.Identity); err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	var err error
	record.RequiredGroups, err = canonicalVectorPartitionLifecycleGroupsAllowEmptyV1(record.RequiredGroups)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	record.ReadyGroups, err = canonicalVectorPartitionLifecycleReadyGroupsAllowEmptyV1(record.ReadyGroups)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	record.CleanedGroups, err = canonicalVectorPartitionLifecycleGroupsAllowEmptyV1(record.CleanedGroups)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	if err := validateVectorPartitionLifecycleRecordV1(record); err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	return record, nil
}

func validateVectorPartitionLifecycleRecordV1(r VectorPartitionLifecycleRecordV1) error {
	if r.Revision == 0 || !isSHA256HexVectorPartitionV1(r.LastCommandDigest) || r.MutationEpoch == 0 {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("committed record revision, mutation epoch, or command digest is invalid"))
	}
	if !validVectorPartitionLifecycleStateV1(r.State) {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalid state %q", r.State))
	}
	if r.PreviousActiveGeneration != 0 && r.Identity.Generation <= r.PreviousActiveGeneration {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("generation does not follow previous active generation"))
	}
	if r.SupersededByGeneration != 0 {
		if r.SupersededByGeneration <= r.Identity.Generation || r.Aborted ||
			r.InvalidationEpoch != 0 || r.InvalidationReason != "" {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("supersession identity or provenance is invalid"))
		}
		if r.State != VectorPartitionLifecycleRetiredV1 && r.State != VectorPartitionLifecycleCleanableV1 &&
			r.State != VectorPartitionLifecycleAbsentV1 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("superseded generation has state %q", r.State))
		}
	}
	if r.State == VectorPartitionLifecycleAbsentV1 {
		if !r.CleanupComplete || len(r.RequiredGroups) != 0 || len(r.ReadyGroups) != 0 || len(r.CleanedGroups) != 0 || r.ReadySetDigest != "" {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("terminal absent record is not cleaned"))
		}
		return nil
	}
	if r.CleanupComplete || len(r.RequiredGroups) == 0 {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("live record has no required groups or is already cleaned"))
	}
	for _, ready := range r.ReadyGroups {
		if !containsVectorPartitionLifecycleGroupV1(r.RequiredGroups, ready.GroupID) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("ready group %q is not required", ready.GroupID))
		}
	}
	for _, group := range r.CleanedGroups {
		if !containsVectorPartitionLifecycleGroupV1(r.RequiredGroups, group) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("cleaned group %q is not required", group))
		}
	}
	switch r.State {
	case VectorPartitionLifecycleBuildingV1:
		if len(r.ReadyGroups) != 0 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("building record has ready groups"))
		}
	case VectorPartitionLifecycleStagedV1:
		if len(r.ReadyGroups) == 0 || len(r.ReadyGroups) > len(r.RequiredGroups) {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("staged readiness is invalid"))
		}
	case VectorPartitionLifecyclePreparedV1, VectorPartitionLifecycleActiveV1, VectorPartitionLifecycleInvalidatedV1:
		if err := validateCompleteVectorPartitionLifecycleReadySetV1(r); err != nil {
			return err
		}
	case VectorPartitionLifecycleRetiredV1, VectorPartitionLifecycleCleanableV1:
		if !r.Aborted {
			if err := validateCompleteVectorPartitionLifecycleReadySetV1(r); err != nil {
				return err
			}
		}
	}
	needsInvalidation := r.State == VectorPartitionLifecycleInvalidatedV1 ||
		((r.State == VectorPartitionLifecycleRetiredV1 || r.State == VectorPartitionLifecycleCleanableV1) &&
			!r.Aborted && r.SupersededByGeneration == 0)
	if needsInvalidation {
		if r.InvalidationEpoch <= r.MutationEpoch || r.InvalidationReason == "" {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("invalidated generation lacks durable invalidation"))
		}
	} else if r.SupersededByGeneration == 0 && (r.InvalidationEpoch != 0 || r.InvalidationReason != "") {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("non-invalidated generation carries invalidation"))
	}
	if r.Aborted {
		if r.State != VectorPartitionLifecycleRetiredV1 && r.State != VectorPartitionLifecycleCleanableV1 {
			return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("aborted generation has state %q", r.State))
		}
		if err := validateVectorPartitionLifecycleReasonV1(r.RetirementReason); err != nil {
			return err
		}
	} else if r.RetirementReason != "" {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("non-aborted generation has retirement reason"))
	}
	if r.State != VectorPartitionLifecycleCleanableV1 && len(r.CleanedGroups) != 0 {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("non-cleanable generation has cleanup progress"))
	}
	return nil
}

func validateCompleteVectorPartitionLifecycleReadySetV1(r VectorPartitionLifecycleRecordV1) error {
	digest, err := VectorPartitionLifecycleReadySetDigestV1(r.Identity, r.RequiredGroups, r.ReadyGroups)
	if err != nil {
		return err
	}
	if r.ReadySetDigest != digest {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("record ready-set digest mismatch"))
	}
	return nil
}

func validateVectorPartitionLifecycleIdentityV1(identity VectorPartitionLifecycleIdentityV1) error {
	if err := validateVectorPartitionLifecycleIndexIdentityV1(identity.Index); err != nil {
		return err
	}
	if identity.Generation == 0 || identity.Source.Generation == 0 || identity.Source.Checksum == 0 ||
		identity.Source.SchemaHash == 0 || identity.Source.RowCount == 0 {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("identity contains zero generation or source field"))
	}
	return nil
}

func validateVectorPartitionLifecycleIndexIdentityV1(identity VectorPartitionLifecycleIndexIdentityV1) error {
	if err := validateCollectionRef(identity.Collection); err != nil {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if identity.CollectionIncarnation == 0 || identity.IndexEpoch == 0 || identity.CatalogEpoch == 0 {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("index identity contains zero incarnation or epoch"))
	}
	if err := validateVectorPartitionLifecycleNameV1("index name", identity.IndexName); err != nil {
		return err
	}
	if !isSHA256HexVectorPartitionV1(identity.IndexDefinitionDigest) || !isSHA256HexVectorPartitionV1(identity.CatalogDigest) {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("index or catalog digest is not canonical sha256"))
	}
	return nil
}

func validateVectorPartitionLifecycleGroupReadyV1(ready VectorPartitionLifecycleGroupReadyV1) error {
	if err := validateID("group id", string(ready.GroupID)); err != nil {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if len(ready.GroupID) > MaxCatalogMetaStringBytesV1 {
		return errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("group id is %d bytes", len(ready.GroupID)))
	}
	if ready.AppliedIndex == 0 || !isSHA256HexVectorPartitionV1(ready.AssetSetDigest) {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("group %q has invalid applied index or asset digest", ready.GroupID))
	}
	return nil
}

func canonicalVectorPartitionLifecycleGroupsV1(groups []raftcluster.GroupID) ([]raftcluster.GroupID, error) {
	if len(groups) == 0 {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("at least one required group is required"))
	}
	return canonicalVectorPartitionLifecycleGroupsAllowEmptyV1(groups)
}

func canonicalVectorPartitionLifecycleGroupsAllowEmptyV1(groups []raftcluster.GroupID) ([]raftcluster.GroupID, error) {
	if len(groups) > MaxVectorPartitionLifecycleGroupsV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("groups=%d", len(groups)))
	}
	out := append([]raftcluster.GroupID(nil), groups...)
	if out == nil {
		out = []raftcluster.GroupID{}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	for i, group := range out {
		if err := validateID("group id", string(group)); err != nil {
			return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, err)
		}
		if len(group) > MaxCatalogMetaStringBytesV1 {
			return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("group id is %d bytes", len(group)))
		}
		if i != 0 && out[i-1] == group {
			return nil, errors.Join(ErrVectorPartitionLifecycleConflict, fmt.Errorf("duplicate group %q", group))
		}
	}
	return out, nil
}

func canonicalVectorPartitionLifecycleReadyGroupsV1(groups []VectorPartitionLifecycleGroupReadyV1) ([]VectorPartitionLifecycleGroupReadyV1, error) {
	if len(groups) == 0 {
		return nil, errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("at least one ready group is required"))
	}
	return canonicalVectorPartitionLifecycleReadyGroupsAllowEmptyV1(groups)
}

func canonicalVectorPartitionLifecycleReadyGroupsAllowEmptyV1(groups []VectorPartitionLifecycleGroupReadyV1) ([]VectorPartitionLifecycleGroupReadyV1, error) {
	if len(groups) > MaxVectorPartitionLifecycleGroupsV1 {
		return nil, errors.Join(ErrVectorPartitionLifecycleLimit, fmt.Errorf("ready groups=%d", len(groups)))
	}
	out := append([]VectorPartitionLifecycleGroupReadyV1(nil), groups...)
	if out == nil {
		out = []VectorPartitionLifecycleGroupReadyV1{}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	for i, ready := range out {
		if err := validateVectorPartitionLifecycleGroupReadyV1(ready); err != nil {
			return nil, err
		}
		if i != 0 && out[i-1].GroupID == ready.GroupID {
			return nil, errors.Join(ErrVectorPartitionLifecycleConflict, fmt.Errorf("duplicate ready group %q", ready.GroupID))
		}
	}
	return out, nil
}

func validateVectorPartitionLifecycleReasonV1(reason string) error {
	if reason == "" || len(reason) > MaxVectorPartitionLifecycleReasonBytesV1 || !utf8.ValidString(reason) || strings.TrimSpace(reason) != reason {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("reason is empty, too long, non-canonical, or invalid utf-8"))
	}
	return nil
}

func validateVectorPartitionLifecycleNameV1(label, value string) error {
	if value == "" || len(value) > MaxCatalogMetaStringBytesV1 || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("%s is invalid", label))
	}
	return nil
}

func validVectorPartitionLifecycleStateV1(state VectorPartitionLifecycleStateV1) bool {
	switch state {
	case VectorPartitionLifecycleAbsentV1, VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1,
		VectorPartitionLifecyclePreparedV1, VectorPartitionLifecycleActiveV1, VectorPartitionLifecycleInvalidatedV1,
		VectorPartitionLifecycleRetiredV1, VectorPartitionLifecycleCleanableV1:
		return true
	default:
		return false
	}
}

func indexIdentityVectorPartitionLifecycleV1(identity VectorPartitionLifecycleIdentityV1) VectorPartitionLifecycleIndexIdentityV1 {
	return identity.Index
}

func zeroVectorPartitionLifecycleIdentityV1(identity VectorPartitionLifecycleIdentityV1) bool {
	return identity == (VectorPartitionLifecycleIdentityV1{})
}

func containsVectorPartitionLifecycleGroupV1(groups []raftcluster.GroupID, group raftcluster.GroupID) bool {
	i := sort.Search(len(groups), func(i int) bool { return groups[i] >= group })
	return i < len(groups) && groups[i] == group
}

func findVectorPartitionLifecycleReadyGroupV1(groups []VectorPartitionLifecycleGroupReadyV1, group raftcluster.GroupID) (int, bool) {
	i := sort.Search(len(groups), func(i int) bool { return groups[i].GroupID >= group })
	return i, i < len(groups) && groups[i].GroupID == group
}

func sha256HexVectorPartitionLifecycleV1(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func decodeVectorPartitionLifecycleJSONV1(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.Join(ErrInvalidVectorPartitionLifecycle, fmt.Errorf("trailing json data"))
	}
	return nil
}
