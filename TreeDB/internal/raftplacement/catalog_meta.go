package raftplacement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// The meta payload is deliberately small: it is replicated as one Raft entry,
// retained in snapshots, and read on every routed request. Limits are checked
// at the wire boundary before JSON decoding and again after decode.
const (
	CatalogMetaFormatV1                    uint16 = 1
	MaxCatalogMetaCommandBytesV1                  = 1 << 20
	MaxCatalogMetaSnapshotBytesV1                 = 8 << 20
	MaxCatalogMetaStringBytesV1                   = 128
	MaxCatalogMetaDigestBytesV1                   = sha256.Size * 2
	MaxCatalogMetaNestingDepthV1                  = 8
	MaxCatalogMetaGroupsV1                        = 128
	MaxCatalogMetaMembersPerGroupV1               = 64
	MaxCatalogMetaPlacementsV1                    = 4096
	MaxCatalogMetaPartitionsV1                    = 16384
	MaxCatalogMetaPartitionsPerPlacementV1        = MaxCatalogMetaPartitionsV1
	MaxCatalogMetaFeaturesV1                      = 64
)

var (
	ErrInvalidCatalogMeta        = errors.New("raftplacement: invalid catalog meta")
	ErrCatalogMetaLimit          = errors.New("raftplacement: catalog meta limit")
	ErrCatalogMetaUnavailable    = errors.New("raftplacement: catalog meta unavailable")
	ErrCatalogMetaStaleEpoch     = errors.New("raftplacement: stale catalog meta epoch")
	ErrCatalogMetaSkippedEpoch   = errors.New("raftplacement: skipped catalog meta epoch")
	ErrCatalogMetaConflict       = errors.New("raftplacement: conflicting catalog meta epoch")
	ErrCatalogMetaTopologyChange = errors.New("raftplacement: catalog meta topology change requires migration")
	ErrCatalogMetaDigestMismatch = errors.New("raftplacement: catalog meta digest mismatch")
	ErrCatalogMetaProofMissing   = errors.New("raftplacement: catalog meta proof missing")
	ErrCatalogMetaRouteMismatch  = errors.New("raftplacement: catalog meta route mismatch")
)

// CatalogMetaRecordV1 is the complete, immutable generation installed by the
// declared meta Raft group. Digest is the SHA-256 of the canonical record
// payload without Digest.
type CatalogMetaRecordV1 struct {
	Format  uint16    `json:"format"`
	Epoch   uint64    `json:"epoch"`
	Catalog CatalogV1 `json:"catalog"`
	Digest  string    `json:"digest"`
}

// CatalogMetaCommandV1 is the only mutation envelope accepted by the local
// meta-state apply seam. ExpectedEpoch makes stale writers fail before a newer
// generation can be made visible.
type CatalogMetaCommandV1 struct {
	Format        uint16              `json:"format"`
	ExpectedEpoch uint64              `json:"expected_epoch"`
	Record        CatalogMetaRecordV1 `json:"record"`
}

// CatalogProofV1 binds a routed read, write, or lifecycle request to an exact
// catalog generation. Callers must obtain it from the status/read path; zero
// values fail closed.
type CatalogProofV1 struct {
	Epoch  uint64
	Digest string
}

type CatalogMetaStatusV1 struct {
	Epoch             uint64
	Digest            string
	AppliedIndex      uint64
	Features          raftcluster.FeatureSet
	RetainedWireBytes uint64
	Refusal           string
}

// CatalogMetaSnapshotV1 is an all-or-nothing snapshot payload. It contains a
// canonical record rather than a local cache serialization, so rejoin and
// backup/restore share the same validation path as Raft replay.
type CatalogMetaSnapshotV1 struct {
	Format                   uint16 `json:"format"`
	AppliedIndex             uint64 `json:"applied_index"`
	Record                   []byte `json:"record"`
	LastCommand              []byte `json:"last_command"`
	VectorPartitionLifecycle []byte `json:"vector_partition_lifecycle,omitempty"`
}

// ApplyCatalogMetaCommittedV1, ExportCatalogMetaSnapshotBytesV1, and
// InstallCatalogMetaSnapshotBytesV1 are the narrow dependency-inverted hooks
// used by raftcluster's HashiCorp meta-group adapter. They ensure the adapter
// cannot mutate a catalog except through committed log Apply or Restore.
func (a *CatalogMetaAuthorityV1) ApplyCatalogMetaCommittedV1(capability raftcluster.CatalogMetaApplyCapabilityV1, raw []byte, appliedIndex uint64) error {
	if !capability.Granted() {
		return ErrCatalogMetaUnavailable
	}
	_, err := a.applyCommittedCatalogMetaV1(raw, appliedIndex)
	return err
}

// CatalogMetaAppliedIndexV1 returns the last catalog command index installed
// in this local applied view. Raft read fences use it after a quorum-verified
// barrier; callers must not treat the value alone as linearizability proof.
func (a *CatalogMetaAuthorityV1) CatalogMetaAppliedIndexV1() (uint64, bool) {
	if a == nil {
		return 0, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.applied, true
}

func (a *CatalogMetaAuthorityV1) ExportCatalogMetaSnapshotBytesV1() ([]byte, error) {
	s, err := a.ExportCatalogMetaSnapshotV1()
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, errors.Join(ErrInvalidCatalogMeta, err)
	}
	if len(b) > MaxCatalogMetaSnapshotBytesV1 {
		return nil, errors.Join(ErrCatalogMetaLimit, fmt.Errorf("snapshot is %d bytes", len(b)))
	}
	if err := preflightCatalogMetaSnapshotJSONV1(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (a *CatalogMetaAuthorityV1) InstallCatalogMetaSnapshotBytesV1(capability raftcluster.CatalogMetaRestoreCapabilityV1, raw []byte) error {
	if !capability.Granted() {
		return ErrCatalogMetaUnavailable
	}
	return a.installCatalogMetaSnapshotBytesV1(raw)
}

// ValidateCatalogMetaSnapshotBytesV1 fully decodes and validates a snapshot
// without publishing it. The Raft backup adapter uses this before entering
// HashiCorp Raft's external Restore path, whose FSM restore callback is
// intentionally fatal on error after the snapshot has been copied.
func (a *CatalogMetaAuthorityV1) ValidateCatalogMetaSnapshotBytesV1(raw []byte) error {
	if a == nil {
		return ErrCatalogMetaUnavailable
	}
	return NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(raw)
}

// ValidateCatalogMetaBackupRestoreTargetV1 restricts external backup restore
// to a fresh disaster-recovery authority. Normal epoch changes must flow
// through committed catalog commands; restoring over live state could roll a
// cluster back and violates HashiCorp Raft's own Restore safety contract.
func (a *CatalogMetaAuthorityV1) ValidateCatalogMetaBackupRestoreTargetV1() error {
	if a == nil {
		return ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.record.Epoch != 0 {
		return errors.Join(ErrCatalogMetaConflict, fmt.Errorf("backup restore target already has catalog epoch %d", a.record.Epoch))
	}
	return nil
}

func (a *CatalogMetaAuthorityV1) installCatalogMetaSnapshotBytesV1(raw []byte) error {
	if len(raw) == 0 || len(raw) > MaxCatalogMetaSnapshotBytesV1 {
		return errors.Join(ErrCatalogMetaLimit, fmt.Errorf("snapshot is %d bytes", len(raw)))
	}
	if err := preflightCatalogMetaSnapshotJSONV1(raw); err != nil {
		return err
	}
	var snapshot CatalogMetaSnapshotV1
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&snapshot); err != nil {
		return errors.Join(ErrInvalidCatalogMeta, err)
	}
	if err := d.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("trailing snapshot data"))
	}
	canonical, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Join(ErrInvalidCatalogMeta, err)
	}
	if !bytes.Equal(raw, canonical) {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("snapshot is not canonical"))
	}
	_, err = a.installCatalogMetaSnapshotV1(snapshot)
	return err
}

// CatalogMetaAuthorityV1 is the local applied view of one replicated meta
// group. It never activates a catalog from a file or constructor argument:
// only capability-bearing Raft Apply or Restore callbacks may publish a
// generation. Reads take an RLock and do not contact the meta leader.
type CatalogMetaAuthorityV1 struct {
	mu             sync.RWMutex
	record         CatalogMetaRecordV1
	resolved       ResolvedCatalogV1
	recordBytes    []byte
	command        []byte
	applied        uint64
	refusal        string
	lifecycleBytes uint64
	lifecycle      map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1
	active         map[VectorPartitionLifecycleIndexIdentityV1]VectorPartitionLifecycleIdentityV1
	activeNames    map[vectorPartitionLifecycleServingKeyV1]VectorPartitionLifecycleIdentityV1
	// mutationFences is a durable per-serving-name watermark.  An
	// invalidation advances it before the corresponding data mutation is
	// admitted, so a generation built from an older source cannot activate
	// after a crash, replay, or a cleanup of the invalidated record.
	mutationFences map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1
	// collectionMutationBarriers serialize relevant data mutations against
	// first-generation source capture. They are replicated and snapshotted;
	// process-local admission locks are intentionally insufficient here.
	collectionMutationBarriers map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1
}

func NewCatalogMetaAuthorityV1() *CatalogMetaAuthorityV1 { return &CatalogMetaAuthorityV1{} }

func NewCatalogMetaRecordV1(epoch uint64, catalog CatalogV1) (CatalogMetaRecordV1, error) {
	if epoch == 0 {
		return CatalogMetaRecordV1{}, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("epoch is required"))
	}
	canonical, resolved, err := canonicalCatalogMetaCatalogV1(catalog)
	if err != nil {
		return CatalogMetaRecordV1{}, err
	}
	_ = resolved
	record := CatalogMetaRecordV1{Format: CatalogMetaFormatV1, Epoch: epoch, Catalog: canonical}
	if err := preflightCatalogMetaRecordValueV1(record); err != nil {
		return CatalogMetaRecordV1{}, err
	}
	digest, err := catalogMetaDigestV1(record)
	if err != nil {
		return CatalogMetaRecordV1{}, err
	}
	record.Digest = digest
	return record, nil
}

func EncodeCatalogMetaCommandV1(command CatalogMetaCommandV1) ([]byte, error) {
	command.Format = CatalogMetaFormatV1
	record, err := validateCatalogMetaRecordV1(command.Record)
	if err != nil {
		return nil, err
	}
	command.Record = record
	b, err := json.Marshal(command)
	if err != nil {
		return nil, errors.Join(ErrInvalidCatalogMeta, err)
	}
	if len(b) > MaxCatalogMetaCommandBytesV1 {
		return nil, errors.Join(ErrCatalogMetaLimit, fmt.Errorf("command is %d bytes", len(b)))
	}
	if err := preflightCatalogMetaCommandJSONV1(b); err != nil {
		return nil, err
	}
	return b, nil
}

func DecodeCatalogMetaCommandV1(raw []byte) (CatalogMetaCommandV1, error) {
	if len(raw) == 0 || len(raw) > MaxCatalogMetaCommandBytesV1 {
		return CatalogMetaCommandV1{}, errors.Join(ErrCatalogMetaLimit, fmt.Errorf("command is %d bytes", len(raw)))
	}
	if err := preflightCatalogMetaCommandJSONV1(raw); err != nil {
		return CatalogMetaCommandV1{}, err
	}
	var command CatalogMetaCommandV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		return CatalogMetaCommandV1{}, errors.Join(ErrInvalidCatalogMeta, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return CatalogMetaCommandV1{}, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("trailing command data"))
	}
	if command.Format != CatalogMetaFormatV1 {
		return CatalogMetaCommandV1{}, errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedVersion, fmt.Errorf("format %d", command.Format))
	}
	record, err := validateCatalogMetaRecordV1(command.Record)
	if err != nil {
		return CatalogMetaCommandV1{}, err
	}
	command.Record = record
	canonical, err := EncodeCatalogMetaCommandV1(command)
	if err != nil {
		return CatalogMetaCommandV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return CatalogMetaCommandV1{}, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("command is not canonical"))
	}
	return command, nil
}

func (a *CatalogMetaAuthorityV1) applyCommittedCatalogMetaV1(raw []byte, appliedIndex uint64) (CatalogMetaStatusV1, error) {
	if a == nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaUnavailable
	}
	if vectorPartitionCollectionMutationCommandBytesV1(raw) {
		return a.applyCommittedVectorPartitionCollectionMutationV1(raw, appliedIndex)
	}
	if vectorPartitionLifecycleCommandBytesV1(raw) {
		return a.applyCommittedVectorPartitionLifecycleV1(raw, appliedIndex)
	}
	command, err := DecodeCatalogMetaCommandV1(raw)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.record.Epoch != 0 && bytes.Equal(a.command, raw) {
		return a.statusLocked(), nil
	}
	if a.record.Epoch == 0 {
		if command.ExpectedEpoch != 0 || command.Record.Epoch != 1 {
			return CatalogMetaStatusV1{}, errors.Join(ErrCatalogMetaSkippedEpoch, fmt.Errorf("initial expected/record epoch %d/%d", command.ExpectedEpoch, command.Record.Epoch))
		}
	} else {
		if command.Record.Epoch == a.record.Epoch {
			return CatalogMetaStatusV1{}, errors.Join(ErrCatalogMetaConflict, fmt.Errorf("epoch %d differs from committed bytes", command.Record.Epoch))
		}
		if command.ExpectedEpoch != a.record.Epoch {
			return CatalogMetaStatusV1{}, errors.Join(ErrCatalogMetaStaleEpoch, fmt.Errorf("expected %d current %d", command.ExpectedEpoch, a.record.Epoch))
		}
		if command.Record.Epoch != a.record.Epoch+1 {
			return CatalogMetaStatusV1{}, errors.Join(ErrCatalogMetaSkippedEpoch, fmt.Errorf("record %d current %d", command.Record.Epoch, a.record.Epoch))
		}
	}
	_, resolved, err := canonicalCatalogMetaCatalogV1(command.Record.Catalog)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if a.record.Epoch != 0 {
		if err := validateCatalogMetaTopologyTransitionV1(a.resolved, resolved); err != nil {
			return CatalogMetaStatusV1{}, err
		}
		if err := a.validateVectorPartitionLifecycleCatalogTransitionLockedV1(); err != nil {
			return CatalogMetaStatusV1{}, err
		}
	}
	a.record = command.Record
	a.resolved = resolved
	a.recordBytes, _ = encodeCatalogMetaRecordV1(command.Record)
	a.command = bytes.Clone(raw)
	a.applied = appliedIndex
	a.refusal = ""
	a.clearVectorPartitionLifecycleLockedV1()
	return a.statusLocked(), nil
}

func (a *CatalogMetaAuthorityV1) Status() (CatalogMetaStatusV1, bool) {
	if a == nil {
		return CatalogMetaStatusV1{Refusal: ErrCatalogMetaUnavailable.Error()}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.record.Epoch == 0 {
		return CatalogMetaStatusV1{Refusal: ErrCatalogMetaUnavailable.Error()}, false
	}
	return a.statusLocked(), true
}

func (a *CatalogMetaAuthorityV1) CurrentCatalogVersion(context.Context) (uint64, bool, error) {
	status, ok := a.Status()
	if !ok {
		return 0, false, nil
	}
	return status.Epoch, true, nil
}

// CurrentCatalogProof returns the exact locally applied proof without
// contacting the meta leader. An authority without a committed generation
// fails closed.
func (a *CatalogMetaAuthorityV1) CurrentCatalogProof(context.Context) (CatalogProofV1, error) {
	status, ok := a.Status()
	if !ok {
		return CatalogProofV1{}, ErrCatalogMetaUnavailable
	}
	return CatalogProofV1{Epoch: status.Epoch, Digest: status.Digest}, nil
}

func (a *CatalogMetaAuthorityV1) Route(_ context.Context, proof CatalogProofV1, request RouteRequestV1) (RouteDecisionV1, error) {
	if a == nil {
		return RouteDecisionV1{}, ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.admitLocked(proof); err != nil {
		return RouteDecisionV1{}, err
	}
	return a.resolved.Route(request)
}

func (a *CatalogMetaAuthorityV1) RouteDocumentToken(_ context.Context, proof CatalogProofV1, collection CollectionRefV1, token uint64) (RouteDecisionV1, error) {
	if a == nil {
		return RouteDecisionV1{}, ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.admitLocked(proof); err != nil {
		return RouteDecisionV1{}, err
	}
	return a.resolved.RouteDocumentToken(collection.Database, collection.Catalog, collection.Collection, token)
}

func (a *CatalogMetaAuthorityV1) ClassifyDocumentTokenBatch(_ context.Context, proof CatalogProofV1, collection CollectionRefV1, tokens []uint64) (RouteTokenBatchDecisionV1, error) {
	if a == nil {
		return RouteTokenBatchDecisionV1{}, ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.admitLocked(proof); err != nil {
		return RouteTokenBatchDecisionV1{}, err
	}
	return a.resolved.ClassifyDocumentTokenBatch(collection.Database, collection.Catalog, collection.Collection, tokens)
}

func (a *CatalogMetaAuthorityV1) ValidateCatalogMetaProof(_ context.Context, epoch uint64, digest string) error {
	if a == nil {
		return ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.admitLocked(CatalogProofV1{Epoch: epoch, Digest: digest})
}

// ValidateCatalogRouteMetadata re-resolves request-only route metadata against
// the exact locally applied catalog generation. It is the final owner-dispatch
// guard: carrying a valid epoch/digest alone cannot redirect a command to a
// different group, placement, partition, or member set.
func (a *CatalogMetaAuthorityV1) ValidateCatalogRouteMetadata(_ context.Context, metadata raftentry.RequestMetadataV1) error {
	if a == nil {
		return ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.admitLocked(CatalogProofV1{Epoch: metadata.CatalogMetaEpoch, Digest: metadata.CatalogMetaDigest}); err != nil {
		return err
	}
	if !metadata.ClusterRouteKnown {
		return errors.Join(ErrCatalogMetaRouteMismatch, fmt.Errorf("route metadata is missing"))
	}
	request := RouteRequestV1{
		Collection: CollectionRefV1{
			Database:   metadata.ClusterRouteDatabase,
			Catalog:    metadata.ClusterRouteCatalog,
			Collection: metadata.ClusterRouteCollection,
		},
		Shape: RouteShapeV1(metadata.ClusterRouteShape),
	}
	if metadata.ClusterRouteTokenKnown {
		token := metadata.ClusterRouteToken
		request.Token = &token
	}
	decision, err := a.resolved.Route(request)
	if err != nil {
		return errors.Join(ErrCatalogMetaRouteMismatch, err)
	}
	if metadata.ClusterRouteGroupID != string(decision.GroupID()) ||
		metadata.ClusterRouteLeaderHint != string(decision.LeaderHint()) ||
		metadata.ClusterRoutePlacementMode != string(decision.PlacementMode) ||
		metadata.ClusterRouteKey != string(decision.RouteKey) ||
		metadata.ClusterRouteTokenKnown != decision.Token.Present ||
		(metadata.ClusterRouteTokenKnown && metadata.ClusterRouteToken != decision.Token.Token) ||
		metadata.ClusterRoutePartitionID != catalogMetaRoutePartitionIDV1(decision) ||
		!catalogMetaRouteMembersEqualV1(metadata.ClusterRouteMembers, decision.Group.Members) {
		return errors.Join(ErrCatalogMetaRouteMismatch, fmt.Errorf("route metadata differs from committed catalog decision"))
	}
	return nil
}

func catalogMetaRoutePartitionIDV1(decision RouteDecisionV1) string {
	if !decision.Token.Present {
		return ""
	}
	return string(decision.Token.Partition.ID)
}

func catalogMetaRouteMembersEqualV1(got []string, want []raftcluster.NodeID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != string(want[i]) {
			return false
		}
	}
	return true
}

func (a *CatalogMetaAuthorityV1) admitLocked(proof CatalogProofV1) error {
	if a.record.Epoch == 0 {
		return ErrCatalogMetaUnavailable
	}
	if proof.Epoch == 0 || proof.Digest == "" {
		return ErrCatalogMetaProofMissing
	}
	if proof.Epoch != a.record.Epoch {
		return errors.Join(ErrCatalogMetaStaleEpoch, fmt.Errorf("proof %d current %d", proof.Epoch, a.record.Epoch))
	}
	if proof.Digest != a.record.Digest {
		return ErrCatalogMetaDigestMismatch
	}
	return nil
}

func (a *CatalogMetaAuthorityV1) ExportCatalogMetaSnapshotV1() (CatalogMetaSnapshotV1, error) {
	if a == nil {
		return CatalogMetaSnapshotV1{}, ErrCatalogMetaUnavailable
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.record.Epoch == 0 {
		return CatalogMetaSnapshotV1{}, ErrCatalogMetaUnavailable
	}
	lifecycle, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	if err != nil {
		return CatalogMetaSnapshotV1{}, err
	}
	return CatalogMetaSnapshotV1{Format: CatalogMetaFormatV1, AppliedIndex: a.applied, Record: bytes.Clone(a.recordBytes), LastCommand: bytes.Clone(a.command), VectorPartitionLifecycle: lifecycle}, nil
}

// validateProspectiveCatalogMetaSnapshotLockedV1 proves that a lifecycle
// mutation leaves the complete outer catalog snapshot exportable. The
// lifecycle payload is itself encoded as base64 by encoding/json, so checking
// only its decoded length is insufficient.
func (a *CatalogMetaAuthorityV1) validateProspectiveCatalogMetaSnapshotLockedV1(lifecycle []byte, appliedIndex uint64) error {
	snapshot := CatalogMetaSnapshotV1{
		Format:                   CatalogMetaFormatV1,
		AppliedIndex:             appliedIndex,
		Record:                   a.recordBytes,
		LastCommand:              a.command,
		VectorPartitionLifecycle: lifecycle,
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Join(ErrInvalidCatalogMeta, err)
	}
	if len(raw) > MaxCatalogMetaSnapshotBytesV1 {
		return errors.Join(ErrCatalogMetaLimit, fmt.Errorf("snapshot is %d bytes", len(raw)))
	}
	return preflightCatalogMetaSnapshotJSONV1(raw)
}

func (a *CatalogMetaAuthorityV1) installCatalogMetaSnapshotV1(snapshot CatalogMetaSnapshotV1) (CatalogMetaStatusV1, error) {
	if a == nil {
		return CatalogMetaStatusV1{}, ErrCatalogMetaUnavailable
	}
	if snapshot.Format != CatalogMetaFormatV1 {
		return CatalogMetaStatusV1{}, errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedVersion)
	}
	record, err := decodeCatalogMetaRecordV1(snapshot.Record)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	_, resolved, err := canonicalCatalogMetaCatalogV1(record.Catalog)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	if len(snapshot.LastCommand) == 0 {
		return CatalogMetaStatusV1{}, errors.Join(ErrInvalidCatalogMeta, ErrCatalogMetaConflict, fmt.Errorf("snapshot last command is required"))
	}
	command, err := DecodeCatalogMetaCommandV1(snapshot.LastCommand)
	if err != nil {
		return CatalogMetaStatusV1{}, errors.Join(ErrInvalidCatalogMeta, ErrCatalogMetaConflict, fmt.Errorf("decode snapshot last command: %w", err))
	}
	commandRecord, err := encodeCatalogMetaRecordV1(command.Record)
	if err != nil {
		return CatalogMetaStatusV1{}, errors.Join(ErrInvalidCatalogMeta, ErrCatalogMetaConflict, fmt.Errorf("encode snapshot last command record: %w", err))
	}
	if command.ExpectedEpoch != record.Epoch-1 ||
		command.Record.Epoch != record.Epoch ||
		command.Record.Digest != record.Digest ||
		!bytes.Equal(snapshot.Record, commandRecord) {
		return CatalogMetaStatusV1{}, errors.Join(ErrInvalidCatalogMeta, ErrCatalogMetaConflict, fmt.Errorf("snapshot last command does not exactly install its record"))
	}
	lifecycle, active, activeNames, mutationFences, collectionMutationBarriers, err := decodeVectorPartitionLifecycleSnapshotV1(snapshot.VectorPartitionLifecycle, record)
	if err != nil {
		return CatalogMetaStatusV1{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.record.Epoch > record.Epoch {
		return CatalogMetaStatusV1{}, ErrCatalogMetaStaleEpoch
	}
	if a.record.Epoch == record.Epoch && a.record.Epoch != 0 {
		if a.record.Digest != record.Digest || !bytes.Equal(a.recordBytes, snapshot.Record) {
			return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
		}
		if snapshot.AppliedIndex < a.applied {
			return CatalogMetaStatusV1{}, ErrCatalogMetaStaleEpoch
		}
		if snapshot.AppliedIndex == a.applied {
			currentLifecycle, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
			if err != nil {
				return CatalogMetaStatusV1{}, err
			}
			if !bytes.Equal(currentLifecycle, snapshot.VectorPartitionLifecycle) {
				return CatalogMetaStatusV1{}, ErrCatalogMetaConflict
			}
			return a.statusLocked(), nil
		}
		a.lifecycle = lifecycle
		a.active = active
		a.activeNames = activeNames
		a.mutationFences = mutationFences
		a.collectionMutationBarriers = collectionMutationBarriers
		a.lifecycleBytes = uint64(len(snapshot.VectorPartitionLifecycle))
		a.applied = snapshot.AppliedIndex
		a.refusal = ""
		return a.statusLocked(), nil
	}
	if a.record.Epoch != 0 {
		if err := validateCatalogMetaTopologyTransitionV1(a.resolved, resolved); err != nil {
			return CatalogMetaStatusV1{}, err
		}
		if err := a.validateVectorPartitionLifecycleCatalogTransitionLockedV1(); err != nil {
			return CatalogMetaStatusV1{}, err
		}
	}
	a.record = record
	a.resolved = resolved
	a.recordBytes = bytes.Clone(snapshot.Record)
	a.command = bytes.Clone(snapshot.LastCommand)
	a.applied = snapshot.AppliedIndex
	a.refusal = ""
	a.lifecycle = lifecycle
	a.active = active
	a.activeNames = activeNames
	a.mutationFences = mutationFences
	a.collectionMutationBarriers = collectionMutationBarriers
	a.lifecycleBytes = uint64(len(snapshot.VectorPartitionLifecycle))
	return a.statusLocked(), nil
}

func (a *CatalogMetaAuthorityV1) statusLocked() CatalogMetaStatusV1 {
	return CatalogMetaStatusV1{
		Epoch:             a.record.Epoch,
		Digest:            a.record.Digest,
		AppliedIndex:      a.applied,
		Features:          cloneFeatureSet(a.record.Catalog.Features),
		RetainedWireBytes: uint64(len(a.recordBytes)+len(a.command)) + a.lifecycleBytes,
		Refusal:           a.refusal,
	}
}

func validateCatalogMetaRecordV1(record CatalogMetaRecordV1) (CatalogMetaRecordV1, error) {
	if record.Format != CatalogMetaFormatV1 || record.Epoch == 0 {
		return CatalogMetaRecordV1{}, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("format/epoch %d/%d", record.Format, record.Epoch))
	}
	canonical, _, err := canonicalCatalogMetaCatalogV1(record.Catalog)
	if err != nil {
		return CatalogMetaRecordV1{}, err
	}
	record.Catalog = canonical
	digest, err := catalogMetaDigestV1(record)
	if err != nil {
		return CatalogMetaRecordV1{}, err
	}
	if len(record.Digest) != sha256.Size*2 || record.Digest != digest {
		return CatalogMetaRecordV1{}, ErrCatalogMetaDigestMismatch
	}
	return record, nil
}

func encodeCatalogMetaRecordV1(record CatalogMetaRecordV1) ([]byte, error) {
	record, err := validateCatalogMetaRecordV1(record)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(record)
	if err != nil {
		return nil, errors.Join(ErrInvalidCatalogMeta, err)
	}
	if len(b) > MaxCatalogMetaCommandBytesV1 {
		return nil, errors.Join(ErrCatalogMetaLimit, fmt.Errorf("record is %d bytes", len(b)))
	}
	if err := preflightCatalogMetaRecordJSONV1(b); err != nil {
		return nil, err
	}
	return b, nil
}

func preflightCatalogMetaRecordValueV1(record CatalogMetaRecordV1) error {
	b, err := json.Marshal(record)
	if err != nil {
		return errors.Join(ErrInvalidCatalogMeta, err)
	}
	if len(b) > MaxCatalogMetaCommandBytesV1 {
		return errors.Join(ErrCatalogMetaLimit, fmt.Errorf("record is %d bytes", len(b)))
	}
	return preflightCatalogMetaRecordJSONV1(b)
}

func decodeCatalogMetaRecordV1(raw []byte) (CatalogMetaRecordV1, error) {
	if len(raw) == 0 || len(raw) > MaxCatalogMetaCommandBytesV1 {
		return CatalogMetaRecordV1{}, ErrCatalogMetaLimit
	}
	if err := preflightCatalogMetaRecordJSONV1(raw); err != nil {
		return CatalogMetaRecordV1{}, err
	}
	var record CatalogMetaRecordV1
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&record); err != nil {
		return CatalogMetaRecordV1{}, errors.Join(ErrInvalidCatalogMeta, err)
	}
	if err := d.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return CatalogMetaRecordV1{}, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("trailing record data"))
	}
	record, err := validateCatalogMetaRecordV1(record)
	if err != nil {
		return CatalogMetaRecordV1{}, err
	}
	canonical, err := encodeCatalogMetaRecordV1(record)
	if err != nil {
		return CatalogMetaRecordV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return CatalogMetaRecordV1{}, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("record is not canonical"))
	}
	return record, nil
}
func catalogMetaDigestV1(record CatalogMetaRecordV1) (string, error) {
	// Keep the digest input structurally separate from the wire record. Merely
	// clearing record.Digest would still serialize `"digest":""`, contrary to
	// the public record contract and independent implementations.
	b, err := json.Marshal(struct {
		Format  uint16    `json:"format"`
		Epoch   uint64    `json:"epoch"`
		Catalog CatalogV1 `json:"catalog"`
	}{
		Format:  record.Format,
		Epoch:   record.Epoch,
		Catalog: record.Catalog,
	})
	if err != nil {
		return "", errors.Join(ErrInvalidCatalogMeta, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalCatalogMetaCatalogV1(c CatalogV1) (CatalogV1, ResolvedCatalogV1, error) {
	if len(c.Groups) > MaxCatalogMetaGroupsV1 || len(c.Placements) > MaxCatalogMetaPlacementsV1 || len(c.Features.Required) > MaxCatalogMetaFeaturesV1 {
		return CatalogV1{}, ResolvedCatalogV1{}, ErrCatalogMetaLimit
	}
	for _, g := range c.Groups {
		if len(g.Members) > MaxCatalogMetaMembersPerGroupV1 {
			return CatalogV1{}, ResolvedCatalogV1{}, ErrCatalogMetaLimit
		}
	}
	partitions := 0
	for _, p := range c.Placements {
		partitions += len(p.TokenPartitions)
	}
	if partitions > MaxCatalogMetaPartitionsV1 {
		return CatalogV1{}, ResolvedCatalogV1{}, ErrCatalogMetaLimit
	}
	c.Groups = append([]GroupV1(nil), c.Groups...)
	c.Placements = append([]CollectionPlacementV1(nil), c.Placements...)
	c.Features.Required = append([]raftcluster.RequiredFeature(nil), c.Features.Required...)
	for i := range c.Groups {
		c.Groups[i].Members = append([]raftcluster.NodeID(nil), c.Groups[i].Members...)
		sort.Slice(c.Groups[i].Members, func(a, b int) bool { return c.Groups[i].Members[a] < c.Groups[i].Members[b] })
	}
	for i := range c.Placements {
		c.Placements[i].TokenPartitions = append([]TokenPartitionV1(nil), c.Placements[i].TokenPartitions...)
		sort.Slice(c.Placements[i].TokenPartitions, func(a, b int) bool {
			return c.Placements[i].TokenPartitions[a].ID < c.Placements[i].TokenPartitions[b].ID
		})
	}
	sort.Slice(c.Groups, func(i, j int) bool { return c.Groups[i].ID < c.Groups[j].ID })
	sort.Slice(c.Placements, func(i, j int) bool {
		a, b := c.Placements[i].Collection, c.Placements[j].Collection
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Catalog != b.Catalog {
			return a.Catalog < b.Catalog
		}
		return a.Collection < b.Collection
	})
	sort.Slice(c.Features.Required, func(i, j int) bool { return c.Features.Required[i].Name < c.Features.Required[j].Name })
	resolved, err := Validate(c)
	if err != nil {
		return CatalogV1{}, ResolvedCatalogV1{}, errors.Join(ErrInvalidCatalogMeta, err)
	}
	canonical := CatalogV1{
		Features:   cloneFeatureSet(resolved.Features),
		Groups:     make([]GroupV1, len(resolved.Groups)),
		Placements: make([]CollectionPlacementV1, len(resolved.Placements)),
	}
	for i, group := range resolved.Groups {
		canonical.Groups[i] = GroupV1{
			ID:         group.ID,
			Members:    append([]raftcluster.NodeID(nil), group.Members...),
			LeaderHint: group.LeaderHint,
		}
	}
	for i, placement := range resolved.Placements {
		canonical.Placements[i] = CollectionPlacementV1{
			Collection: placement.Collection,
			GroupID:    placement.GroupID,
			Mode:       placement.Mode,
			RouteKey:   placement.RouteKey,
		}
		if len(placement.TokenPartitions) != 0 {
			canonical.Placements[i].TokenPartitions = make([]TokenPartitionV1, len(placement.TokenPartitions))
			for j, partition := range placement.TokenPartitions {
				canonical.Placements[i].TokenPartitions[j] = TokenPartitionV1{
					ID:      partition.ID,
					GroupID: partition.GroupID,
					Start:   partition.Start,
					End:     partition.End,
				}
			}
		}
	}
	return canonical, resolved, nil
}

// validateCatalogMetaTopologyTransitionV1 enforces the M4A no-migration
// boundary. Existing ownership and membership may not change until an
// explicit migration workflow can transfer data, apply progress, and
// idempotency state before publishing the new route. Metadata-only updates and
// new groups or placements remain valid.
func validateCatalogMetaTopologyTransitionV1(current, next ResolvedCatalogV1) error {
	for _, group := range current.Groups {
		nextGroup, ok := next.groups[group.ID]
		if !ok {
			return catalogMetaTopologyChangeV1("group %q was removed", group.ID)
		}
		if !equalCatalogMetaMembersV1(group.Members, nextGroup.Members) {
			return catalogMetaTopologyChangeV1("group %q members changed", group.ID)
		}
	}
	for _, placement := range current.Placements {
		nextPlacement, ok := next.placements[placement.Collection]
		if !ok {
			return catalogMetaTopologyChangeV1(
				"placement %s/%s/%s was removed",
				placement.Collection.Database,
				placement.Collection.Catalog,
				placement.Collection.Collection,
			)
		}
		if placement.Mode != nextPlacement.Mode {
			return catalogMetaTopologyChangeV1(
				"placement %s/%s/%s mode changed from %q to %q",
				placement.Collection.Database,
				placement.Collection.Catalog,
				placement.Collection.Collection,
				placement.Mode,
				nextPlacement.Mode,
			)
		}
		if placement.GroupID != nextPlacement.GroupID {
			return catalogMetaTopologyChangeV1(
				"placement %s/%s/%s owner changed from %q to %q",
				placement.Collection.Database,
				placement.Collection.Catalog,
				placement.Collection.Collection,
				placement.GroupID,
				nextPlacement.GroupID,
			)
		}
		if placement.RouteKey != nextPlacement.RouteKey {
			return catalogMetaTopologyChangeV1(
				"placement %s/%s/%s route key changed from %q to %q",
				placement.Collection.Database,
				placement.Collection.Catalog,
				placement.Collection.Collection,
				placement.RouteKey,
				nextPlacement.RouteKey,
			)
		}
		if !equalCatalogMetaPartitionsV1(placement.TokenPartitions, nextPlacement.TokenPartitions) {
			return catalogMetaTopologyChangeV1(
				"placement %s/%s/%s partitions changed",
				placement.Collection.Database,
				placement.Collection.Catalog,
				placement.Collection.Collection,
			)
		}
	}
	return nil
}

func catalogMetaTopologyChangeV1(format string, args ...any) error {
	return errors.Join(ErrCatalogMetaConflict, ErrCatalogMetaTopologyChange, fmt.Errorf(format, args...))
}

func equalCatalogMetaMembersV1(a, b []raftcluster.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalCatalogMetaPartitionsV1(a, b []ResolvedTokenPartitionV1) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
