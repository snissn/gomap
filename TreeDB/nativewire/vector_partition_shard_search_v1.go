package nativewire

// This file is the M5 generation-pinned shard search boundary. It deliberately
// composes the generic #3474 routed read-index/apply contract with M3's
// no-document partition searcher. It is not a second routing protocol and it
// does not forward, fan out, fetch documents, or mutate collection state.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

const VectorPartitionShardSearchVersionV1 uint32 = 1

const (
	vectorPartitionShardSearchResponseEnvelopeBytesV1 uint64 = 256
	vectorPartitionShardSearchPartialEnvelopeBytesV1  uint64 = 128
	vectorPartitionDuplicateLinearThresholdV1                = 16
)

const (
	vectorPartitionShardSearchProofReadIndexV1      = "read_index"
	vectorPartitionShardSearchProofStrictSnapshotV1 = "immutable_snapshot_capability"
)

type VectorPartitionShardSearchMetricV1 string
type VectorPartitionShardSearchModeV1 string
type VectorPartitionShardSearchConsistencyV1 string
type VectorPartitionShardSearchStatsModeV1 string

const (
	VectorPartitionShardSearchMetricCosineV1 VectorPartitionShardSearchMetricV1 = "cosine"
	// V1 returns stable IDs and authoritative FP32 scores without fetching
	// documents. The immutable local asset decides whether candidate discovery
	// is native HNSW or an exact in-memory fallback, and every partial reports
	// that actual route.
	VectorPartitionShardSearchModeNoDocumentV1 VectorPartitionShardSearchModeV1 = "no_document_partition"

	// V1 freshness is the named immutable vector generation after a
	// quorum-backed read-index/apply proof. It is not a claim that the vector
	// generation contains the latest committed document mutation.
	VectorPartitionShardSearchConsistencySnapshotV1 VectorPartitionShardSearchConsistencyV1 = "linearizable_generation_snapshot"

	VectorPartitionShardSearchStatsNoneV1  VectorPartitionShardSearchStatsModeV1 = "none"
	VectorPartitionShardSearchStatsBasicV1 VectorPartitionShardSearchStatsModeV1 = "basic"
)

var (
	ErrVectorPartitionShardSearchInvalidRequest         = errors.New("nativewire: invalid vector partition shard search request")
	ErrVectorPartitionShardSearchUnsupportedConsistency = errors.New("nativewire: unsupported vector partition shard search consistency")
	ErrVectorPartitionShardSearchRouteMismatch          = errors.New("nativewire: vector partition shard search route mismatch")
	ErrVectorPartitionShardSearchRemoteOwner            = errors.New("nativewire: vector partition shard search owner is remote")
	ErrVectorPartitionShardSearchGenerationMismatch     = errors.New("nativewire: vector partition shard search generation mismatch")
	ErrVectorPartitionShardSearchAssetsUnavailable      = errors.New("nativewire: vector partition shard search assets unavailable")
	ErrVectorPartitionShardSearchResponseTooLarge       = errors.New("nativewire: vector partition shard search response too large")
	ErrVectorPartitionShardSearchCanceled               = errors.New("nativewire: vector partition shard search canceled")
	ErrVectorPartitionShardSearchDeadline               = errors.New("nativewire: vector partition shard search deadline exceeded")
)

type VectorPartitionShardSearchErrorCodeV1 string

const (
	VectorPartitionShardSearchErrorInvalidRequestV1         VectorPartitionShardSearchErrorCodeV1 = "invalid_request"
	VectorPartitionShardSearchErrorUnsupportedConsistencyV1 VectorPartitionShardSearchErrorCodeV1 = "unsupported_consistency"
	VectorPartitionShardSearchErrorMissingOwnerV1           VectorPartitionShardSearchErrorCodeV1 = "missing_owner"
	VectorPartitionShardSearchErrorUnknownOwnerV1           VectorPartitionShardSearchErrorCodeV1 = "unknown_owner"
	VectorPartitionShardSearchErrorRemoteOwnerV1            VectorPartitionShardSearchErrorCodeV1 = "remote_owner"
	VectorPartitionShardSearchErrorRouteMismatchV1          VectorPartitionShardSearchErrorCodeV1 = "route_mismatch"
	VectorPartitionShardSearchErrorNotLeaderV1              VectorPartitionShardSearchErrorCodeV1 = "not_leader"
	VectorPartitionShardSearchErrorGroupUnavailableV1       VectorPartitionShardSearchErrorCodeV1 = "group_unavailable"
	VectorPartitionShardSearchErrorGenerationMismatchV1     VectorPartitionShardSearchErrorCodeV1 = "generation_mismatch"
	VectorPartitionShardSearchErrorAssetsUnavailableV1      VectorPartitionShardSearchErrorCodeV1 = "assets_unavailable"
	VectorPartitionShardSearchErrorResponseTooLargeV1       VectorPartitionShardSearchErrorCodeV1 = "response_too_large"
	VectorPartitionShardSearchErrorCanceledV1               VectorPartitionShardSearchErrorCodeV1 = "canceled"
	VectorPartitionShardSearchErrorDeadlineV1               VectorPartitionShardSearchErrorCodeV1 = "deadline_exceeded"
)

// VectorPartitionShardSearchErrorV1 provides a stable machine-readable class
// while preserving the generic routed-read or M3 cause for errors.Is.
type VectorPartitionShardSearchErrorV1 struct {
	Code       VectorPartitionShardSearchErrorCodeV1
	GroupID    raftcluster.GroupID
	LeaderHint raftcluster.NodeID
	Err        error
}

func (e *VectorPartitionShardSearchErrorV1) Error() string {
	if e == nil {
		return "nativewire: vector partition shard search failed"
	}
	message := "nativewire: vector partition shard search " + string(e.Code)
	if e.GroupID != "" {
		message += fmt.Sprintf(" group=%q", e.GroupID)
	}
	if e.LeaderHint != "" {
		message += fmt.Sprintf(" leader_hint=%q", e.LeaderHint)
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *VectorPartitionShardSearchErrorV1) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type VectorPartitionShardSearchLimitsV1 struct {
	MaxDimensions, MaxQueryBytes, MaxPartitions, MaxTopK, MaxEfSearch int
	MaxRequestBytes, MaxCandidateBytes, MaxResponseBytes              uint64
	MaxIdentityBytes, MaxStableIDBytes                                int
}

func DefaultVectorPartitionShardSearchLimitsV1() VectorPartitionShardSearchLimitsV1 {
	return VectorPartitionShardSearchLimitsV1{
		MaxDimensions:     4096,
		MaxQueryBytes:     16 << 10,
		MaxPartitions:     32,
		MaxTopK:           256,
		MaxEfSearch:       4096,
		MaxRequestBytes:   64 << 10,
		MaxCandidateBytes: 64 << 20,
		MaxResponseBytes:  64 << 20,
		MaxIdentityBytes:  4096,
		MaxStableIDBytes:  4096,
	}
}

type VectorPartitionShardSearchRequestV1 struct {
	Version uint32

	RequestID, CancellationID        string
	Database, Catalog, Collection    string
	IndexName, IndexDefinitionDigest string
	ReadySetDigest                   string

	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	PartitionGeneration, RouterGeneration                              uint64

	TargetGroupID raftcluster.GroupID
	TargetNodeID  raftcluster.NodeID
	PartitionIDs  []uint32

	Query       []float32
	Metric      VectorPartitionShardSearchMetricV1
	Mode        VectorPartitionShardSearchModeV1
	Consistency VectorPartitionShardSearchConsistencyV1
	StatsMode   VectorPartitionShardSearchStatsModeV1
	TopK        int
	EfSearch    int

	DeadlineUnixNano    int64
	RequestBytesLimit   uint64
	CandidateBytesLimit uint64
	ResponseBytesLimit  uint64

	StrictCapability *vectorPartitionStrictSearchCapabilityV1
}

type VectorPartitionShardSearchNeighborV1 struct {
	ID    string
	Score float32
}

type VectorPartitionShardSearchPartialV1 struct {
	PartitionID       uint32
	Neighbors         []VectorPartitionShardSearchNeighborV1
	Candidates, Edges uint64
	SearchRoute       string
	PackBytes         uint64
	MappedBytes       uint64
	HeapBytes         uint64
	OpenNanos         uint64
}

type VectorPartitionShardSearchProofV1 struct {
	Kind                                                               string
	ServingNode, LeaderNode                                            raftcluster.NodeID
	GroupID                                                            raftcluster.GroupID
	ReadySetDigest                                                     string
	ServingIdentityDigest                                              string
	ReadTerm, ReadIndex                                                uint64
	AppliedTerm, AppliedIndex                                          uint64
	CatalogAppliedIndex, GroupAppliedIndex                             uint64
	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	PartitionGeneration, RouterGeneration                              uint64
}

type VectorPartitionShardSearchTimingV1 struct {
	RouteOwnerNanos, ReadIndexApplyNanos, GenerationOpenNanos uint64
	SearchNanos, ResponseCopyNanos, TotalNanos                uint64
}

type VectorPartitionShardSearchResponseV1 struct {
	Version                                                uint32
	RequestID                                              string
	Proof                                                  VectorPartitionShardSearchProofV1
	Partials                                               []VectorPartitionShardSearchPartialV1
	Partitions, ReadProofs, GenerationPins, PartitionOpens uint64
	Candidates                                             uint64
	Edges                                                  uint64
	ResponseBytes                                          uint64
	Timing                                                 VectorPartitionShardSearchTimingV1
}

// MeasureVectorPartitionShardSearchResponseBytesV1 applies the M5 v1 logical
// response accounting used by service and transport-neutral dispatchers. It
// measures the decoded envelope, not a specific network framing.
func MeasureVectorPartitionShardSearchResponseBytesV1(partials []VectorPartitionShardSearchPartialV1) (uint64, error) {
	partialBytes, ok := mulUint64V1(uint64(len(partials)), vectorPartitionShardSearchPartialEnvelopeBytesV1)
	if !ok {
		return 0, ErrVectorPartitionShardSearchResponseTooLarge
	}
	responseBytes, ok := addUint64V1(vectorPartitionShardSearchResponseEnvelopeBytesV1, partialBytes)
	if !ok {
		return 0, ErrVectorPartitionShardSearchResponseTooLarge
	}
	for _, partial := range partials {
		for _, neighbor := range partial.Neighbors {
			responseBytes, ok = addUint64V1(responseBytes, uint64(len(neighbor.ID)+16))
			if !ok {
				return 0, ErrVectorPartitionShardSearchResponseTooLarge
			}
		}
	}
	return responseBytes, nil
}

// VectorPartitionPinnedManifestV1 is the bounded identity/placement subset M5
// needs from M1. Memberships and asset descriptors stay inside the local
// generation source so warm requests never clone a potentially 1M-row
// manifest.
type VectorPartitionPinnedManifestV1 struct {
	State, Collection, IndexName, IndexDefinitionDigest, IntegrityDigest string
	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount   uint64
	Generation, RouterGeneration                                         uint64
	ReadySetDigest                                                       string
	PartitionCount                                                       uint32
	Placements                                                           []collections.VectorPartitionPlacementV1
}

// VectorPartitionPartitionSearchLeaseV1 gives one request a searcher without
// deciding whether the underlying immutable mapped pack is request-owned or
// shared by a generation cache. Close is idempotent.
type VectorPartitionPartitionSearchLeaseV1 struct {
	Searcher *collections.VectorPartitionLocalSearcherV1
	CacheHit bool

	closeOnce sync.Once
	closeFn   func() error
	closeErr  error
}

func NewVectorPartitionPartitionSearchLeaseV1(searcher *collections.VectorPartitionLocalSearcherV1, cacheHit bool, closeFn func() error) (*VectorPartitionPartitionSearchLeaseV1, error) {
	if searcher == nil || closeFn == nil {
		return nil, ErrVectorPartitionShardSearchAssetsUnavailable
	}
	return &VectorPartitionPartitionSearchLeaseV1{Searcher: searcher, CacheHit: cacheHit, closeFn: closeFn}, nil
}

func (l *VectorPartitionPartitionSearchLeaseV1) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeErr = l.closeFn()
	})
	return l.closeErr
}

// VectorPartitionPinnedGenerationV1 holds the exact generation pin across all
// partition opens/searches. Close and every returned partition lease Close
// must be idempotent. The underlying M3 searcher defers resource release while
// a concurrent SearchWithOptionsV1 call is active.
type VectorPartitionPinnedGenerationV1 interface {
	Manifest() VectorPartitionPinnedManifestV1
	OpenPartition(context.Context, uint32) (*VectorPartitionPartitionSearchLeaseV1, error)
	Close() error
}

// vectorPartitionPinnedManifestViewV1 is implemented only by the package-owned
// immutable cache lease. External generation sources retain the defensive-copy
// Manifest contract.
type vectorPartitionPinnedManifestViewV1 interface {
	immutableManifestViewV1() VectorPartitionPinnedManifestV1
}

type VectorPartitionGenerationSourceV1 interface {
	PinVectorPartitionGenerationV1(context.Context, string, uint64) (VectorPartitionPinnedGenerationV1, error)
}

type VectorPartitionShardSearchServiceOptionsV1 struct {
	Catalog          raftplacement.ResolvedCatalogV1
	Placement        raftplacement.VectorPartitionPlacementRecordV1
	LocalNodeID      raftcluster.NodeID
	LocalGroupID     raftcluster.GroupID
	ReadCoordinator  raftcluster.RoutedReadIndexCoordinator
	GenerationSource VectorPartitionGenerationSourceV1
	Limits           VectorPartitionShardSearchLimitsV1
}

type vectorPartitionShardSearchRouteV1 struct {
	placement raftplacement.VectorPartitionPlacementRecordV1
	owners    map[uint32]raftcluster.GroupID
	hints     map[raftcluster.GroupID]raftcluster.NodeID
}

type VectorPartitionShardSearchServiceV1 struct {
	localNodeID      raftcluster.NodeID
	localGroup       raftcluster.GroupID
	readCoordinator  raftcluster.RoutedReadIndexCoordinator
	generationSource VectorPartitionGenerationSourceV1
	limits           VectorPartitionShardSearchLimitsV1
	route            vectorPartitionShardSearchRouteV1
	stats            vectorPartitionShardSearchStatsAccumulatorV1
	servingSnapshot  *VectorPartitionServingSnapshotPublisherV1
	strictKey        []byte

	// Narrow package-test seams for cancellation and timing at the response boundary.
	testBeforePartialMaterialization func()
	testBeforeResponseCopy           func()
	testSearchPartition              func(context.Context, *collections.VectorPartitionLocalSearcherV1, []float32, collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error)
}

func (s *VectorPartitionShardSearchServiceV1) bindServingSnapshotV1(publisher *VectorPartitionServingSnapshotPublisherV1, key []byte) error {
	if s == nil || publisher == nil || len(key) != sha256.Size || s.servingSnapshot != nil {
		return ErrVectorPartitionShardSearchInvalidRequest
	}
	s.servingSnapshot = publisher
	s.strictKey = slices.Clone(key)
	return nil
}

func NewVectorPartitionShardSearchServiceV1(opts VectorPartitionShardSearchServiceOptionsV1) (*VectorPartitionShardSearchServiceV1, error) {
	limits, err := normalizeVectorPartitionShardSearchLimitsV1(opts.Limits)
	if err != nil {
		return nil, err
	}
	if opts.LocalNodeID == "" || opts.LocalGroupID == "" || opts.ReadCoordinator == nil || opts.GenerationSource == nil {
		return nil, fmt.Errorf("%w: incomplete service dependencies", ErrVectorPartitionShardSearchInvalidRequest)
	}
	if err := opts.Catalog.ValidateVectorPartitionPlacementV1(opts.Placement); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionShardSearchRouteMismatch, err)
	}
	if _, ok := opts.Catalog.Placement(opts.Placement.Collection); !ok {
		return nil, fmt.Errorf("%w: collection is absent from the resolved catalog", ErrVectorPartitionShardSearchRouteMismatch)
	}
	localGroup, ok := opts.Catalog.Group(opts.LocalGroupID)
	if !ok {
		return nil, fmt.Errorf("%w: local group %q is absent from the resolved catalog", ErrVectorPartitionShardSearchRouteMismatch, opts.LocalGroupID)
	}
	if !slices.Contains(localGroup.Members, opts.LocalNodeID) {
		return nil, fmt.Errorf("%w: local node %q is not a member of group %q", ErrVectorPartitionShardSearchRouteMismatch, opts.LocalNodeID, opts.LocalGroupID)
	}
	owners := make(map[uint32]raftcluster.GroupID, len(opts.Placement.Partitions))
	for _, part := range opts.Placement.Partitions {
		owners[part.PartitionID] = part.GroupID
	}
	hints := make(map[raftcluster.GroupID]raftcluster.NodeID, len(opts.Catalog.Groups))
	for _, group := range opts.Catalog.Groups {
		hints[group.ID] = group.LeaderHint
	}
	placement := opts.Placement
	placement.Partitions = slices.Clone(opts.Placement.Partitions)
	return &VectorPartitionShardSearchServiceV1{
		localNodeID:      opts.LocalNodeID,
		localGroup:       opts.LocalGroupID,
		readCoordinator:  opts.ReadCoordinator,
		generationSource: opts.GenerationSource,
		limits:           limits,
		route: vectorPartitionShardSearchRouteV1{
			placement: placement,
			owners:    owners,
			hints:     hints,
		},
	}, nil
}

func normalizeVectorPartitionShardSearchLimitsV1(limits VectorPartitionShardSearchLimitsV1) (VectorPartitionShardSearchLimitsV1, error) {
	if limits == (VectorPartitionShardSearchLimitsV1{}) {
		limits = DefaultVectorPartitionShardSearchLimitsV1()
	}
	if err := validateVectorPartitionShardSearchLimitsV1(limits); err != nil {
		return VectorPartitionShardSearchLimitsV1{}, err
	}
	return limits, nil
}

func validateVectorPartitionShardSearchLimitsV1(l VectorPartitionShardSearchLimitsV1) error {
	if l.MaxDimensions < 1 || l.MaxDimensions > 4096 ||
		l.MaxQueryBytes < 4 || l.MaxPartitions < 1 || l.MaxTopK < 1 || l.MaxEfSearch < l.MaxTopK ||
		l.MaxRequestBytes == 0 || l.MaxCandidateBytes == 0 || l.MaxResponseBytes == 0 ||
		l.MaxIdentityBytes < 1 || l.MaxStableIDBytes < 1 {
		return fmt.Errorf("%w: invalid service limits", ErrVectorPartitionShardSearchInvalidRequest)
	}
	return nil
}

func (s *VectorPartitionShardSearchServiceV1) Search(ctx context.Context, request VectorPartitionShardSearchRequestV1) (response VectorPartitionShardSearchResponseV1, resultErr error) {
	started := time.Now()
	if s == nil {
		return response, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, Err: ErrVectorPartitionShardSearchAssetsUnavailable}
	}
	s.stats.begin()
	defer func() {
		total := elapsedNanosV1(started)
		total = max(total,
			response.Timing.RouteOwnerNanos+response.Timing.ReadIndexApplyNanos+
				response.Timing.GenerationOpenNanos+response.Timing.SearchNanos+
				response.Timing.ResponseCopyNanos)
		response.Timing.TotalNanos = total
		if resultErr != nil {
			s.stats.fail(classifyVectorPartitionShardSearchErrorV1(resultErr), total)
			response = VectorPartitionShardSearchResponseV1{}
			return
		}
		s.stats.succeed(response, total)
	}()

	ctx, cancel, err := vectorPartitionShardSearchContextV1(ctx, request.DeadlineUnixNano)
	if err != nil {
		return response, s.wrapError(err, "")
	}
	defer cancel()
	if err := s.validateRequest(request); err != nil {
		return response, s.wrapError(err, "")
	}

	routeStarted := time.Now()
	groupID, leaderHint, err := s.resolveOwner(request)
	response.Timing.RouteOwnerNanos = elapsedNanosV1(routeStarted)
	if err != nil {
		return response, s.wrapError(err, groupID)
	}
	s.stats.ownerRoute()
	if err := ctx.Err(); err != nil {
		return response, s.wrapError(err, groupID)
	}

	openStarted := time.Now()
	var proof raftcluster.ReadIndexProof
	var progress raftcluster.AppliedProgress
	var pinned VectorPartitionPinnedGenerationV1
	var strictSnapshot *VectorPartitionServingSnapshotLeaseV1
	ownedAssets := true
	if s.servingSnapshot != nil {
		if err := validateVectorPartitionStrictSearchCapabilityV1(request, s.strictKey); err != nil {
			return response, s.wrapError(fmt.Errorf("%w: strict capability: %v", ErrVectorPartitionShardSearchGenerationMismatch, err), groupID)
		}
		strictSnapshot, err = s.servingSnapshot.AcquireV1()
		if err != nil {
			return response, s.wrapError(fmt.Errorf("%w: strict serving snapshot: %v", ErrVectorPartitionShardSearchGenerationMismatch, err), groupID)
		}
		defer func() {
			if closeErr := strictSnapshot.Close(); closeErr != nil && resultErr == nil {
				resultErr = s.wrapError(closeErr, groupID)
			}
		}()
		identity := strictSnapshot.IdentityV1()
		capability := request.StrictCapability
		if identity.ServingIdentityDigest != capability.ServingIdentityDigest || identity.CatalogEpoch != capability.CatalogEpoch ||
			identity.CatalogDigest != capability.CatalogDigest || identity.CatalogAppliedIndex < capability.CatalogAppliedIndex {
			return response, s.wrapError(fmt.Errorf("%w: strict serving identity", ErrVectorPartitionShardSearchGenerationMismatch), groupID)
		}
		groupReady := uint64(0)
		for _, ready := range identity.ReadyGroups {
			if ready.GroupID == groupID {
				groupReady = ready.AppliedIndex
				break
			}
		}
		if groupReady < capability.GroupAppliedIndex {
			return response, s.wrapError(fmt.Errorf("%w: strict group applied index", ErrVectorPartitionShardSearchGenerationMismatch), groupID)
		}
		pinned = strictSnapshot.snapshot.generations[groupID]
		ownedAssets = false
	} else {
		if request.StrictCapability != nil {
			return response, s.wrapError(ErrVectorPartitionShardSearchGenerationMismatch, groupID)
		}
		readStarted := time.Now()
		proof, progress, err = s.readCoordinator.CoordinateRoutedReadIndex(ctx, raftcluster.ReadIndexBarrier{NodeID: request.TargetNodeID, GroupID: groupID})
		response.Timing.ReadIndexApplyNanos = elapsedNanosV1(readStarted)
		if err != nil {
			return response, s.wrapErrorWithHint(err, groupID, leaderHint)
		}
		target := raftcluster.ReadIndexBarrier{NodeID: request.TargetNodeID, GroupID: groupID}
		if err := target.Check(proof); err != nil {
			return response, s.wrapErrorWithHint(err, groupID, leaderHint)
		}
		if err := proof.AppliedIndexBarrier().Check(progress); err != nil {
			return response, s.wrapErrorWithHint(err, groupID, leaderHint)
		}
		if proof.NodeID != s.localNodeID || proof.GroupID != s.localGroup || progress.NodeID != s.localNodeID || progress.GroupID != s.localGroup {
			return response, s.wrapErrorWithHint(fmt.Errorf("%w: proof served by node=%q group=%q applied_node=%q applied_group=%q", ErrVectorPartitionShardSearchRouteMismatch, proof.NodeID, proof.GroupID, progress.NodeID, progress.GroupID), groupID, leaderHint)
		}
		s.stats.readProof()
		response.ReadProofs = 1
		pinned, err = s.generationSource.PinVectorPartitionGenerationV1(ctx, request.IndexName, request.PartitionGeneration)
		if err != nil {
			return response, s.wrapError(err, groupID)
		}
		if pinned == nil {
			return response, s.wrapError(fmt.Errorf("%w: nil generation lease", ErrVectorPartitionShardSearchAssetsUnavailable), groupID)
		}
		response.GenerationPins = 1
		defer func() {
			if closeErr := pinned.Close(); closeErr != nil && resultErr == nil {
				resultErr = s.wrapError(fmt.Errorf("%w: close generation pin: %v", ErrVectorPartitionShardSearchAssetsUnavailable, closeErr), groupID)
			}
		}()
	}
	if pinned == nil {
		return response, s.wrapError(ErrVectorPartitionShardSearchAssetsUnavailable, groupID)
	}
	var manifest VectorPartitionPinnedManifestV1
	if view, ok := pinned.(vectorPartitionPinnedManifestViewV1); ok {
		manifest = view.immutableManifestViewV1()
	} else {
		manifest = pinned.Manifest()
	}
	if err := s.validatePinnedManifest(request, manifest); err != nil {
		response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
		return response, s.wrapError(err, groupID)
	}

	searchers := make([]*VectorPartitionPartitionSearchLeaseV1, 0, len(request.PartitionIDs))
	var openedCandidateBytes uint64
	defer func() {
		if !ownedAssets {
			return
		}
		for i := len(searchers) - 1; i >= 0; i-- {
			if closeErr := searchers[i].Close(); closeErr != nil && resultErr == nil {
				resultErr = s.wrapError(fmt.Errorf("%w: close partition %d: %v", ErrVectorPartitionShardSearchAssetsUnavailable, request.PartitionIDs[i], closeErr), groupID)
			}
		}
	}()
	for _, partitionID := range request.PartitionIDs {
		if err := ctx.Err(); err != nil {
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			return response, s.wrapError(err, groupID)
		}
		var lease *VectorPartitionPartitionSearchLeaseV1
		var openErr error
		if ownedAssets {
			lease, openErr = pinned.OpenPartition(ctx, partitionID)
			if openErr == nil {
				response.PartitionOpens++
			}
		} else {
			lease = strictSnapshot.snapshot.partitions[groupID][partitionID]
		}
		if openErr != nil {
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			if errors.Is(openErr, context.Canceled) || errors.Is(openErr, context.DeadlineExceeded) {
				return response, s.wrapError(fmt.Errorf("partition %d: %w", partitionID, openErr), groupID)
			}
			return response, s.wrapError(fmt.Errorf("%w: partition %d: %w", ErrVectorPartitionShardSearchAssetsUnavailable, partitionID, openErr), groupID)
		}
		if lease == nil || lease.Searcher == nil {
			if lease != nil && ownedAssets {
				_ = lease.Close()
			}
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			return response, s.wrapError(fmt.Errorf("%w: partition %d returned a nil search lease", ErrVectorPartitionShardSearchAssetsUnavailable, partitionID), groupID)
		}
		searcher := lease.Searcher
		status, scratchBytes, scratchErr := searcher.SearchPreflightV1(collections.VectorPartitionSearchOptionsV1{
			TopK:             request.TopK,
			EfSearch:         request.EfSearch,
			MaxStableIDBytes: s.limits.MaxStableIDBytes,
		})
		if status.Generation != request.PartitionGeneration || status.PartitionID != partitionID || status.Retired {
			if ownedAssets {
				_ = lease.Close()
			}
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			return response, s.wrapError(fmt.Errorf("%w: opened partition %d generation=%d retired=%t", ErrVectorPartitionShardSearchGenerationMismatch, status.PartitionID, status.Generation, status.Retired), groupID)
		}
		if status.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 &&
			status.SearchRoute != collections.VectorPartitionSearchRouteExactFP32ScanV1 {
			if ownedAssets {
				_ = lease.Close()
			}
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			return response, s.wrapError(fmt.Errorf("%w: partition %d unknown search route %q", ErrVectorPartitionShardSearchAssetsUnavailable, partitionID, status.SearchRoute), groupID)
		}
		if scratchErr != nil {
			if ownedAssets {
				_ = lease.Close()
			}
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			return response, s.wrapError(fmt.Errorf("%w: partition %d scratch bound: %v", ErrVectorPartitionShardSearchAssetsUnavailable, partitionID, scratchErr), groupID)
		}
		candidateCeiling := uint64(request.EfSearch)
		if status.SearchRoute == collections.VectorPartitionSearchRouteExactFP32ScanV1 {
			candidateCeiling = uint64(status.HomeMemberships + status.OverlapMemberships)
		}
		partitionCandidateBytes, ok := mulUint64V1(candidateCeiling, 64)
		if scratchBytes > partitionCandidateBytes {
			partitionCandidateBytes = scratchBytes
		}
		if ok {
			openedCandidateBytes, ok = addUint64V1(openedCandidateBytes, partitionCandidateBytes)
		}
		if !ok || openedCandidateBytes > request.CandidateBytesLimit || openedCandidateBytes > s.limits.MaxCandidateBytes {
			if ownedAssets {
				_ = lease.Close()
			}
			response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)
			return response, s.wrapError(fmt.Errorf("%w: opened candidate ceiling", ErrVectorPartitionShardSearchInvalidRequest), groupID)
		}
		searchers = append(searchers, lease)
		if ownedAssets {
			s.stats.open(status, lease.CacheHit)
		}
	}
	response.Timing.GenerationOpenNanos = elapsedNanosV1(openStarted)

	responseWorkStarted := time.Now()
	partials := make([]VectorPartitionShardSearchPartialV1, len(searchers))
	var totalCandidates, totalEdges, actualCandidateBytes, searchNanos uint64
	for i, lease := range searchers {
		searcher := lease.Searcher
		search := searchVectorPartitionWithContextV1
		if searcher.Status().SearchRoute == collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
			// The native pack polls the caller context throughout its bounded
			// traversal. Avoid a channel/goroutine on the performance path.
			search = searchVectorPartitionDirectV1
		}
		if s.testSearchPartition != nil {
			search = s.testSearchPartition
		}
		partitionSearchStarted := time.Now()
		results, metrics, searchErr := search(ctx, searcher, request.Query, collections.VectorPartitionSearchOptionsV1{
			TopK:             request.TopK,
			EfSearch:         request.EfSearch,
			MaxStableIDBytes: s.limits.MaxStableIDBytes,
		})
		searchNanos += elapsedNanosV1(partitionSearchStarted)
		response.Timing.SearchNanos = searchNanos
		if searchErr != nil {
			return response, s.wrapError(searchErr, groupID)
		}
		if s.testBeforePartialMaterialization != nil {
			s.testBeforePartialMaterialization()
		}
		if err := ctx.Err(); err != nil {
			return response, s.wrapError(err, groupID)
		}
		status := searcher.Status()
		if metrics.Route != status.SearchRoute {
			return response, s.wrapError(fmt.Errorf("%w: partition %d reported route %q after opening as %q", ErrVectorPartitionShardSearchAssetsUnavailable, request.PartitionIDs[i], metrics.Route, status.SearchRoute), groupID)
		}
		var ok bool
		totalCandidates, ok = addUint64V1(totalCandidates, metrics.Candidates)
		if !ok {
			return response, s.wrapError(ErrVectorPartitionShardSearchResponseTooLarge, groupID)
		}
		totalEdges, ok = addUint64V1(totalEdges, metrics.Edges)
		if !ok {
			return response, s.wrapError(ErrVectorPartitionShardSearchResponseTooLarge, groupID)
		}
		partitionCandidateBytes, ok := mulUint64V1(metrics.Candidates, 64)
		if ok {
			actualCandidateBytes, ok = addUint64V1(actualCandidateBytes, partitionCandidateBytes)
		}
		if !ok || actualCandidateBytes > request.CandidateBytesLimit || actualCandidateBytes > s.limits.MaxCandidateBytes {
			return response, s.wrapError(fmt.Errorf("%w: actual candidate bytes", ErrVectorPartitionShardSearchInvalidRequest), groupID)
		}
		partials[i] = VectorPartitionShardSearchPartialV1{
			PartitionID: request.PartitionIDs[i],
			Neighbors:   make([]VectorPartitionShardSearchNeighborV1, len(results)),
			SearchRoute: metrics.Route,
			PackBytes:   status.PackBytes,
			MappedBytes: status.MappedBytes,
			HeapBytes:   status.HeapBytes,
			OpenNanos:   status.OpenNanos,
		}
		if request.StatsMode == VectorPartitionShardSearchStatsBasicV1 {
			partials[i].Candidates = metrics.Candidates
			partials[i].Edges = metrics.Edges
		}
		for j, result := range results {
			partials[i].Neighbors[j] = VectorPartitionShardSearchNeighborV1{ID: result.ID, Score: result.Score}
		}
	}

	if s.testBeforeResponseCopy != nil {
		s.testBeforeResponseCopy()
	}
	responseBytes, err := s.validateResponse(ctx, request, partials)
	responseWorkNanos := elapsedNanosV1(responseWorkStarted)
	if responseWorkNanos > searchNanos {
		response.Timing.ResponseCopyNanos = responseWorkNanos - searchNanos
	}
	if err != nil {
		return response, s.wrapError(err, groupID)
	}
	responseProof := VectorPartitionShardSearchProofV1{
		Kind:        vectorPartitionShardSearchProofReadIndexV1,
		ServingNode: proof.NodeID, LeaderNode: proof.NodeID, GroupID: proof.GroupID,
		ReadySetDigest: request.ReadySetDigest,
		ReadTerm:       proof.Term, ReadIndex: proof.Index, AppliedTerm: progress.Term, AppliedIndex: progress.Index,
		SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum,
		SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount,
		PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration,
	}
	if strictSnapshot != nil {
		capability := request.StrictCapability
		responseProof = VectorPartitionShardSearchProofV1{
			Kind:        vectorPartitionShardSearchProofStrictSnapshotV1,
			ServingNode: s.localNodeID, GroupID: groupID, ReadySetDigest: request.ReadySetDigest,
			ServingIdentityDigest: capability.ServingIdentityDigest,
			CatalogAppliedIndex:   capability.CatalogAppliedIndex, GroupAppliedIndex: capability.GroupAppliedIndex,
			SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum,
			SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount,
			PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration,
		}
	}
	response = VectorPartitionShardSearchResponseV1{
		Version:       VectorPartitionShardSearchVersionV1,
		RequestID:     request.RequestID,
		Proof:         responseProof,
		Partials:      partials,
		Partitions:    uint64(len(partials)),
		Candidates:    totalCandidates,
		Edges:         totalEdges,
		ResponseBytes: responseBytes,
		Timing:        response.Timing,
	}
	return response, nil
}

type vectorPartitionSearchAsyncResultV1 struct {
	results []collections.VectorPartitionSearchResultV1
	metrics collections.VectorPartitionSearchMetricsV1
	err     error
}

func searchVectorPartitionDirectV1(ctx context.Context, searcher *collections.VectorPartitionLocalSearcherV1, query []float32, opts collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error) {
	return searcher.SearchWithOptionsV1(ctx, query, opts)
}

func searchVectorPartitionWithContextV1(ctx context.Context, searcher *collections.VectorPartitionLocalSearcherV1, query []float32, opts collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error) {
	return runVectorPartitionOwnedSearchWithContextV1(ctx, query, opts, searcher.SearchWithOptionsV1)
}

func runVectorPartitionOwnedSearchWithContextV1(
	ctx context.Context,
	query []float32,
	opts collections.VectorPartitionSearchOptionsV1,
	search func(context.Context, []float32, collections.VectorPartitionSearchOptionsV1) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error),
) ([]collections.VectorPartitionSearchResultV1, collections.VectorPartitionSearchMetricsV1, error) {
	result := make(chan vectorPartitionSearchAsyncResultV1, 1)
	ownedQuery := slices.Clone(query)
	go func() {
		results, metrics, err := search(ctx, ownedQuery, opts)
		result <- vectorPartitionSearchAsyncResultV1{results: results, metrics: metrics, err: err}
	}()
	select {
	case <-ctx.Done():
		// Join the bounded exact scan before returning so the request cannot
		// outlive its generation lease. SearchWithOptionsV1 polls ctx while
		// scanning; owning the query also prevents caller reuse from racing
		// with any work between cancellation and the join.
		<-result
		return nil, collections.VectorPartitionSearchMetricsV1{}, ctx.Err()
	case completed := <-result:
		return completed.results, completed.metrics, completed.err
	}
}

func vectorPartitionShardSearchContextV1(ctx context.Context, deadlineUnixNano int64) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if deadlineUnixNano == 0 {
		// The caller context already carries connection/request cancellation.
		// Avoid allocating a redundant child on the warm path when the wire
		// request does not add its own deadline.
		return ctx, func() {}, nil
	}
	deadline := time.Unix(0, deadlineUnixNano)
	if !deadline.After(time.Now()) {
		return nil, func() {}, context.DeadlineExceeded
	}
	child, cancel := context.WithDeadline(ctx, deadline)
	return child, cancel, nil
}

func (s *VectorPartitionShardSearchServiceV1) validateRequest(r VectorPartitionShardSearchRequestV1) error {
	l := s.limits
	if r.Version != VectorPartitionShardSearchVersionV1 ||
		r.RequestID == "" || r.CancellationID == "" ||
		r.Database == "" || r.Catalog == "" || r.Collection == "" || r.IndexName == "" ||
		!isVectorPartitionShardSearchDigestV1(r.IndexDefinitionDigest) ||
		!isVectorPartitionShardSearchDigestV1(r.ReadySetDigest) ||
		r.SourceGeneration == 0 || r.SourceRowCount == 0 || r.PartitionGeneration == 0 || r.RouterGeneration == 0 ||
		len(r.PartitionIDs) < 1 || len(r.PartitionIDs) > l.MaxPartitions ||
		len(r.Query) < 1 || len(r.Query) > l.MaxDimensions ||
		r.TopK < 1 || r.TopK > l.MaxTopK || r.EfSearch < r.TopK || r.EfSearch > l.MaxEfSearch {
		return ErrVectorPartitionShardSearchInvalidRequest
	}
	if r.TargetGroupID == "" {
		return errors.Join(ErrVectorPartitionShardSearchInvalidRequest, raftcluster.ErrRouteTargetMissing)
	}
	for _, identity := range []string{r.RequestID, r.CancellationID, r.Database, r.Catalog, r.Collection, r.IndexName, r.IndexDefinitionDigest, r.ReadySetDigest, string(r.TargetGroupID), string(r.TargetNodeID)} {
		if len(identity) > l.MaxIdentityBytes {
			return fmt.Errorf("%w: identity bytes", ErrVectorPartitionShardSearchInvalidRequest)
		}
	}
	if r.Metric != VectorPartitionShardSearchMetricCosineV1 || r.Mode != VectorPartitionShardSearchModeNoDocumentV1 {
		return fmt.Errorf("%w: metric=%q mode=%q", ErrVectorPartitionShardSearchInvalidRequest, r.Metric, r.Mode)
	}
	if r.Consistency != VectorPartitionShardSearchConsistencySnapshotV1 {
		return fmt.Errorf("%w: %q", ErrVectorPartitionShardSearchUnsupportedConsistency, r.Consistency)
	}
	if r.StatsMode != VectorPartitionShardSearchStatsNoneV1 && r.StatsMode != VectorPartitionShardSearchStatsBasicV1 {
		return fmt.Errorf("%w: stats mode %q", ErrVectorPartitionShardSearchInvalidRequest, r.StatsMode)
	}
	queryBytes := uint64(len(r.Query)) * 4
	if queryBytes > uint64(l.MaxQueryBytes) {
		return fmt.Errorf("%w: query bytes", ErrVectorPartitionShardSearchInvalidRequest)
	}
	var norm float64
	for _, value := range r.Query {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("%w: nonfinite query", ErrVectorPartitionShardSearchInvalidRequest)
		}
		norm += float64(value) * float64(value)
	}
	if norm == 0 {
		return fmt.Errorf("%w: zero query", ErrVectorPartitionShardSearchInvalidRequest)
	}
	for i, partitionID := range r.PartitionIDs {
		if i > 0 && partitionID <= r.PartitionIDs[i-1] {
			return fmt.Errorf("%w: partition ids must be strictly increasing", ErrVectorPartitionShardSearchInvalidRequest)
		}
	}
	requestBytes, requestBytesErr := vectorPartitionCoordinatorShardRequestBytesV1(r)
	if requestBytesErr != nil || r.RequestBytesLimit == 0 || r.RequestBytesLimit > l.MaxRequestBytes || requestBytes > r.RequestBytesLimit {
		return fmt.Errorf("%w: request bytes=%d limit=%d", ErrVectorPartitionShardSearchInvalidRequest, requestBytes, r.RequestBytesLimit)
	}
	candidateBytes, ok := mulUint64V1(uint64(len(r.PartitionIDs)), uint64(r.EfSearch))
	if ok {
		candidateBytes, ok = mulUint64V1(candidateBytes, 64)
	}
	if !ok || r.CandidateBytesLimit == 0 || r.CandidateBytesLimit > l.MaxCandidateBytes || candidateBytes > r.CandidateBytesLimit {
		return fmt.Errorf("%w: candidate bytes", ErrVectorPartitionShardSearchInvalidRequest)
	}
	responseBytes, ok := mulUint64V1(uint64(len(r.PartitionIDs)), vectorPartitionShardSearchPartialEnvelopeBytesV1)
	if ok {
		responseBytes, ok = addUint64V1(vectorPartitionShardSearchResponseEnvelopeBytesV1, responseBytes)
	}
	resultCount, resultCountOK := mulUint64V1(uint64(len(r.PartitionIDs)), uint64(r.TopK))
	if resultCountOK {
		resultCount, resultCountOK = mulUint64V1(resultCount, uint64(l.MaxStableIDBytes+16))
	}
	if resultCountOK {
		responseBytes, resultCountOK = addUint64V1(responseBytes, resultCount)
	}
	if !ok || !resultCountOK || r.ResponseBytesLimit == 0 || r.ResponseBytesLimit > l.MaxResponseBytes || responseBytes > r.ResponseBytesLimit {
		return fmt.Errorf("%w: response budget", ErrVectorPartitionShardSearchResponseTooLarge)
	}
	return nil
}

func isVectorPartitionShardSearchDigestV1(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}

func (s *VectorPartitionShardSearchServiceV1) resolveOwner(r VectorPartitionShardSearchRequestV1) (raftcluster.GroupID, raftcluster.NodeID, error) {
	p := s.route.placement
	if r.Database != p.Collection.Database || r.Catalog != p.Collection.Catalog || r.Collection != p.Collection.Collection ||
		r.IndexName != p.IndexName || r.IndexDefinitionDigest != p.IndexDefinitionDigest ||
		r.SourceGeneration != p.SourceGeneration || r.SourceChecksum != p.SourceChecksum ||
		r.SourceSchemaHash != p.SourceSchemaHash || r.SourceRowCount != p.SourceRowCount ||
		r.PartitionGeneration != p.PartitionGeneration || r.RouterGeneration != p.PartitionGeneration {
		return "", "", ErrVectorPartitionShardSearchGenerationMismatch
	}
	var groupID raftcluster.GroupID
	for _, partitionID := range r.PartitionIDs {
		owner, ok := s.route.owners[partitionID]
		if !ok {
			return "", "", fmt.Errorf("%w: partition %d", raftcluster.ErrRouteTargetUnknown, partitionID)
		}
		if groupID == "" {
			groupID = owner
		} else if owner != groupID {
			return groupID, s.route.hints[groupID], fmt.Errorf("%w: partitions span groups %q and %q", ErrVectorPartitionShardSearchRouteMismatch, groupID, owner)
		}
	}
	if r.TargetGroupID != groupID {
		return groupID, s.route.hints[groupID], fmt.Errorf("%w: target group %q owner group %q", ErrVectorPartitionShardSearchRouteMismatch, r.TargetGroupID, groupID)
	}
	if groupID != s.localGroup {
		return groupID, s.route.hints[groupID], fmt.Errorf("%w: local group %q", ErrVectorPartitionShardSearchRemoteOwner, s.localGroup)
	}
	return groupID, s.route.hints[groupID], nil
}

func (s *VectorPartitionShardSearchServiceV1) validatePinnedManifest(r VectorPartitionShardSearchRequestV1, m VectorPartitionPinnedManifestV1) error {
	if m.State != "ready" || m.Collection != r.Collection || m.IndexName != r.IndexName ||
		m.IndexDefinitionDigest != r.IndexDefinitionDigest ||
		m.SourceGeneration != r.SourceGeneration || m.SourceChecksum != r.SourceChecksum ||
		m.SourceSchemaHash != r.SourceSchemaHash || m.SourceRowCount != r.SourceRowCount ||
		m.Generation != r.PartitionGeneration || m.RouterGeneration != r.RouterGeneration ||
		m.ReadySetDigest != r.ReadySetDigest {
		return ErrVectorPartitionShardSearchGenerationMismatch
	}
	for _, partitionID := range r.PartitionIDs {
		if uint64(partitionID) >= uint64(len(m.Placements)) {
			return fmt.Errorf("%w: manifest partition %d owner=%q target=%q", ErrVectorPartitionShardSearchRouteMismatch, partitionID, "", r.TargetGroupID)
		}
		index := int(partitionID)
		if m.Placements[index].PartitionID != partitionID ||
			m.Placements[index].GroupID != string(r.TargetGroupID) {
			owner := ""
			if m.Placements[index].PartitionID == partitionID {
				owner = m.Placements[index].GroupID
			}
			return fmt.Errorf("%w: manifest partition %d owner=%q target=%q", ErrVectorPartitionShardSearchRouteMismatch, partitionID, owner, r.TargetGroupID)
		}
	}
	return nil
}

func (s *VectorPartitionShardSearchServiceV1) validateResponse(ctx context.Context, r VectorPartitionShardSearchRequestV1, partials []VectorPartitionShardSearchPartialV1) (uint64, error) {
	if len(partials) != len(r.PartitionIDs) {
		return 0, fmt.Errorf("%w: partial count", ErrVectorPartitionShardSearchAssetsUnavailable)
	}
	responseBytes, err := MeasureVectorPartitionShardSearchResponseBytesV1(partials)
	if err != nil || responseBytes > r.ResponseBytesLimit || responseBytes > s.limits.MaxResponseBytes {
		return 0, ErrVectorPartitionShardSearchResponseTooLarge
	}
	for i, partial := range partials {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if partial.PartitionID != r.PartitionIDs[i] || len(partial.Neighbors) > r.TopK ||
			partial.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 &&
				partial.SearchRoute != collections.VectorPartitionSearchRouteExactFP32ScanV1 {
			return 0, fmt.Errorf("%w: malformed partition envelope", ErrVectorPartitionShardSearchAssetsUnavailable)
		}
		var seenIDs map[string]struct{}
		if len(partial.Neighbors) > vectorPartitionDuplicateLinearThresholdV1 {
			seenIDs = make(map[string]struct{}, len(partial.Neighbors))
		}
		var seenSmallBloom uint64
		for neighborIndex, neighbor := range partial.Neighbors {
			if neighbor.ID == "" || len(neighbor.ID) > s.limits.MaxStableIDBytes || math.IsNaN(float64(neighbor.Score)) || math.IsInf(float64(neighbor.Score), 0) {
				return 0, fmt.Errorf("%w: malformed partition result", ErrVectorPartitionShardSearchAssetsUnavailable)
			}
			if seenIDs != nil {
				if _, duplicate := seenIDs[neighbor.ID]; duplicate {
					return 0, fmt.Errorf("%w: duplicate stable ID", ErrVectorPartitionShardSearchAssetsUnavailable)
				}
				seenIDs[neighbor.ID] = struct{}{}
			} else {
				bit := vectorPartitionStableIDBloomBitV1(neighbor.ID)
				if seenSmallBloom&bit != 0 {
					for previous := 0; previous < neighborIndex; previous++ {
						if partial.Neighbors[previous].ID == neighbor.ID {
							return 0, fmt.Errorf("%w: duplicate stable ID", ErrVectorPartitionShardSearchAssetsUnavailable)
						}
					}
				}
				seenSmallBloom |= bit
			}
		}
	}
	return responseBytes, nil
}

func vectorPartitionStableIDBloomBitV1(id string) uint64 {
	fingerprint := uint64(len(id))*0x9e3779b97f4a7c15 ^
		uint64(id[0])<<17 ^
		uint64(id[len(id)/2])<<9 ^
		uint64(id[len(id)-1])
	fingerprint ^= fingerprint >> 33
	return uint64(1) << (fingerprint & 63)
}

func addUint64V1(a, b uint64) (uint64, bool) {
	if b > ^uint64(0)-a {
		return 0, false
	}
	return a + b, true
}

func mulUint64V1(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, false
	}
	return a * b, true
}

func elapsedNanosV1(start time.Time) uint64 {
	nanos := time.Since(start).Nanoseconds()
	if nanos < 1 {
		return 1
	}
	return uint64(nanos)
}

func (s *VectorPartitionShardSearchServiceV1) wrapError(err error, groupID raftcluster.GroupID) error {
	return s.wrapErrorWithHint(err, groupID, s.route.hints[groupID])
}

func (s *VectorPartitionShardSearchServiceV1) wrapErrorWithHint(err error, groupID raftcluster.GroupID, hint raftcluster.NodeID) error {
	if err == nil {
		return nil
	}
	var existing *VectorPartitionShardSearchErrorV1
	if errors.As(err, &existing) {
		return err
	}
	return &VectorPartitionShardSearchErrorV1{
		Code:       classifyVectorPartitionShardSearchErrorV1(err),
		GroupID:    groupID,
		LeaderHint: hint,
		Err:        err,
	}
}

func classifyVectorPartitionShardSearchErrorV1(err error) VectorPartitionShardSearchErrorCodeV1 {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrVectorPartitionShardSearchDeadline):
		return VectorPartitionShardSearchErrorDeadlineV1
	case errors.Is(err, context.Canceled), errors.Is(err, ErrVectorPartitionShardSearchCanceled):
		return VectorPartitionShardSearchErrorCanceledV1
	case errors.Is(err, ErrVectorPartitionShardSearchUnsupportedConsistency):
		return VectorPartitionShardSearchErrorUnsupportedConsistencyV1
	case errors.Is(err, ErrVectorPartitionShardSearchRemoteOwner):
		return VectorPartitionShardSearchErrorRemoteOwnerV1
	case errors.Is(err, raftcluster.ErrRouteTargetMissing):
		return VectorPartitionShardSearchErrorMissingOwnerV1
	case errors.Is(err, raftcluster.ErrRouteTargetUnknown):
		return VectorPartitionShardSearchErrorUnknownOwnerV1
	case errors.Is(err, ErrVectorPartitionShardSearchRouteMismatch), errors.Is(err, raftcluster.ErrReadBarrierTargetMismatch), errors.Is(err, raftcluster.ErrRouteGroupMismatch):
		return VectorPartitionShardSearchErrorRouteMismatchV1
	case errors.Is(err, raftcluster.ErrNotLeader):
		return VectorPartitionShardSearchErrorNotLeaderV1
	case errors.Is(err, raftcluster.ErrReadBarrierNotSatisfied), errors.Is(err, raftcluster.ErrAdmissionUnavailable):
		return VectorPartitionShardSearchErrorGroupUnavailableV1
	case errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch), errors.Is(err, collections.ErrVectorPartitionManifestInvalid):
		return VectorPartitionShardSearchErrorGenerationMismatchV1
	case errors.Is(err, ErrVectorPartitionShardSearchResponseTooLarge):
		return VectorPartitionShardSearchErrorResponseTooLargeV1
	case errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable), errors.Is(err, collections.ErrVectorPartitionSearchUnavailable):
		return VectorPartitionShardSearchErrorAssetsUnavailableV1
	default:
		return VectorPartitionShardSearchErrorInvalidRequestV1
	}
}

type VectorPartitionShardSearchServiceStatsV1 struct {
	Requests, Successes, Errors, OwnerRoutes, ReadProofs, Partitions uint64
	Candidates, ResponseBytes, Opens, MappedOpens, HeapOpens         uint64
	CacheHits, CacheMisses                                           uint64
	Invalid, UnsupportedConsistency, MissingOwner, UnknownOwner      uint64
	RemoteOwner, RouteMismatch, NotLeader, Unavailable               uint64
	GenerationMismatch, AssetsUnavailable, ResponseTooLarge          uint64
	Canceled, TimedOut                                               uint64
	RouteOwnerNanos, ReadIndexApplyNanos, GenerationOpenNanos        uint64
	SearchNanos, ResponseCopyNanos, TotalNanos                       uint64
}

type vectorPartitionShardSearchStatsAccumulatorV1 struct {
	mu    sync.Mutex
	value VectorPartitionShardSearchServiceStatsV1
}

func (a *vectorPartitionShardSearchStatsAccumulatorV1) begin() {
	a.mu.Lock()
	a.value.Requests++
	a.mu.Unlock()
}

func (a *vectorPartitionShardSearchStatsAccumulatorV1) ownerRoute() {
	a.mu.Lock()
	a.value.OwnerRoutes++
	a.mu.Unlock()
}

func (a *vectorPartitionShardSearchStatsAccumulatorV1) readProof() {
	a.mu.Lock()
	a.value.ReadProofs++
	a.mu.Unlock()
}

func (a *vectorPartitionShardSearchStatsAccumulatorV1) open(status collections.VectorPartitionSearchStatusV1, cacheHit bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value.Opens++
	if cacheHit {
		a.value.CacheHits++
	} else {
		a.value.CacheMisses++
	}
	if status.MappedBytes != 0 {
		a.value.MappedOpens++
	}
	if status.HeapBytes != 0 {
		a.value.HeapOpens++
	}
}

func (a *vectorPartitionShardSearchStatsAccumulatorV1) fail(code VectorPartitionShardSearchErrorCodeV1, total uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value.Errors++
	a.value.TotalNanos += total
	switch code {
	case VectorPartitionShardSearchErrorUnsupportedConsistencyV1:
		a.value.UnsupportedConsistency++
	case VectorPartitionShardSearchErrorMissingOwnerV1:
		a.value.MissingOwner++
	case VectorPartitionShardSearchErrorUnknownOwnerV1:
		a.value.UnknownOwner++
	case VectorPartitionShardSearchErrorRemoteOwnerV1:
		a.value.RemoteOwner++
	case VectorPartitionShardSearchErrorRouteMismatchV1:
		a.value.RouteMismatch++
	case VectorPartitionShardSearchErrorNotLeaderV1:
		a.value.NotLeader++
	case VectorPartitionShardSearchErrorGroupUnavailableV1:
		a.value.Unavailable++
	case VectorPartitionShardSearchErrorGenerationMismatchV1:
		a.value.GenerationMismatch++
	case VectorPartitionShardSearchErrorAssetsUnavailableV1:
		a.value.AssetsUnavailable++
	case VectorPartitionShardSearchErrorResponseTooLargeV1:
		a.value.ResponseTooLarge++
	case VectorPartitionShardSearchErrorCanceledV1:
		a.value.Canceled++
	case VectorPartitionShardSearchErrorDeadlineV1:
		a.value.TimedOut++
	default:
		a.value.Invalid++
	}
}

func (a *vectorPartitionShardSearchStatsAccumulatorV1) succeed(response VectorPartitionShardSearchResponseV1, total uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value.Successes++
	a.value.Partitions += response.Partitions
	a.value.Candidates += response.Candidates
	a.value.ResponseBytes += response.ResponseBytes
	a.value.RouteOwnerNanos += response.Timing.RouteOwnerNanos
	a.value.ReadIndexApplyNanos += response.Timing.ReadIndexApplyNanos
	a.value.GenerationOpenNanos += response.Timing.GenerationOpenNanos
	a.value.SearchNanos += response.Timing.SearchNanos
	a.value.ResponseCopyNanos += response.Timing.ResponseCopyNanos
	a.value.TotalNanos += total
}

func (s *VectorPartitionShardSearchServiceV1) Stats() VectorPartitionShardSearchServiceStatsV1 {
	if s == nil {
		return VectorPartitionShardSearchServiceStatsV1{}
	}
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	return s.stats.value
}
