package nativewire

// This file is the bounded M6 coordinator. It composes one pinned M4 router
// with transport-neutral M5 shard dispatch. It does not activate generations,
// fetch documents, choose serving replicas, or claim a production network path.

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

const VectorPartitionCoordinatorVersionV1 uint32 = 1

var (
	ErrVectorPartitionCoordinatorInvalidRequest     = errors.New("nativewire: invalid vector partition coordinator request")
	ErrVectorPartitionCoordinatorGenerationMismatch = errors.New("nativewire: vector partition coordinator generation mismatch")
	ErrVectorPartitionCoordinatorRouteMismatch      = errors.New("nativewire: vector partition coordinator route mismatch")
	ErrVectorPartitionCoordinatorBudgetExceeded     = errors.New("nativewire: vector partition coordinator budget exceeded")
	ErrVectorPartitionCoordinatorMalformedResponse  = errors.New("nativewire: malformed vector partition coordinator shard response")
	ErrVectorPartitionCoordinatorUnavailable        = errors.New("nativewire: vector partition coordinator unavailable")
)

type VectorPartitionCoordinatorErrorCodeV1 string

const (
	VectorPartitionCoordinatorErrorInvalidRequestV1     VectorPartitionCoordinatorErrorCodeV1 = "invalid_request"
	VectorPartitionCoordinatorErrorGenerationMismatchV1 VectorPartitionCoordinatorErrorCodeV1 = "generation_mismatch"
	VectorPartitionCoordinatorErrorRouteMismatchV1      VectorPartitionCoordinatorErrorCodeV1 = "route_mismatch"
	VectorPartitionCoordinatorErrorBudgetExceededV1     VectorPartitionCoordinatorErrorCodeV1 = "budget_exceeded"
	VectorPartitionCoordinatorErrorMalformedResponseV1  VectorPartitionCoordinatorErrorCodeV1 = "malformed_response"
	VectorPartitionCoordinatorErrorNotLeaderV1          VectorPartitionCoordinatorErrorCodeV1 = "not_leader"
	VectorPartitionCoordinatorErrorUnavailableV1        VectorPartitionCoordinatorErrorCodeV1 = "unavailable"
	VectorPartitionCoordinatorErrorCanceledV1           VectorPartitionCoordinatorErrorCodeV1 = "canceled"
	VectorPartitionCoordinatorErrorDeadlineV1           VectorPartitionCoordinatorErrorCodeV1 = "deadline_exceeded"
)

type VectorPartitionCoordinatorErrorV1 struct {
	Code    VectorPartitionCoordinatorErrorCodeV1
	GroupID raftcluster.GroupID
	Err     error

	// Counters and Timing retain resource evidence for failed requests without
	// exposing neighbors or routing results. Search still returns a zero
	// response on every error.
	Counters VectorPartitionCoordinatorCountersV1
	Timing   VectorPartitionCoordinatorTimingV1
}

func (e *VectorPartitionCoordinatorErrorV1) Error() string {
	if e == nil {
		return "nativewire: vector partition coordinator failed"
	}
	message := "nativewire: vector partition coordinator " + string(e.Code)
	if e.GroupID != "" {
		message += fmt.Sprintf(" group=%q", e.GroupID)
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *VectorPartitionCoordinatorErrorV1) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type VectorPartitionCoordinatorLimitsV1 struct {
	MaxSelectedPartitions, MaxGroups, MaxRequests, MaxConcurrentRequests int
	MaxRetries, MaxRedirects, MaxRouterCandidates                        int
	MaxQueryBytes, MaxTopK, MaxEfSearch, MaxPartitionsPerRequest         int
	MaxIdentityBytes, MaxStableIDBytes, MaxMergeEntries                  int
	MaxRequestBytes, MaxCandidateBytes, MaxResponseBytes                 uint64
	MaxWallClock                                                         time.Duration
}

func DefaultVectorPartitionCoordinatorLimitsV1() VectorPartitionCoordinatorLimitsV1 {
	shard := DefaultVectorPartitionShardSearchLimitsV1()
	return VectorPartitionCoordinatorLimitsV1{
		MaxSelectedPartitions:   256,
		MaxGroups:               64,
		MaxRequests:             256,
		MaxConcurrentRequests:   8,
		MaxRetries:              1,
		MaxRedirects:            1,
		MaxRouterCandidates:     1_000_000,
		MaxQueryBytes:           shard.MaxQueryBytes,
		MaxTopK:                 shard.MaxTopK,
		MaxEfSearch:             shard.MaxEfSearch,
		MaxPartitionsPerRequest: shard.MaxPartitions,
		MaxIdentityBytes:        shard.MaxIdentityBytes,
		MaxStableIDBytes:        shard.MaxStableIDBytes,
		MaxMergeEntries:         256 * shard.MaxTopK,
		MaxRequestBytes:         4 << 20,
		MaxCandidateBytes:       shard.MaxCandidateBytes,
		MaxResponseBytes:        shard.MaxResponseBytes,
		MaxWallClock:            30 * time.Second,
	}
}

// VectorPartitionCoordinatorRouterV1 is the exact pinned M4 generation used by
// one coordinator request.
type VectorPartitionCoordinatorRouterV1 interface {
	SearchWithContextV1(context.Context, []float32, collections.VectorPartitionRouterSearchOptionsV1) (collections.VectorPartitionRouterSearchResultV1, error)
	Status() collections.VectorPartitionRouterRuntimeStatusV1
	Close() error
}

type VectorPartitionCoordinatorRouterSourceV1 interface {
	OpenVectorPartitionCoordinatorRouterV1(context.Context, string, uint64) (VectorPartitionCoordinatorRouterV1, error)
}

// CollectionVectorPartitionCoordinatorRouterSourceV1 adapts the real M4
// collection path without exposing collection storage to a transport.
type CollectionVectorPartitionCoordinatorRouterSourceV1 struct {
	Collection *collections.Collection
}

func (s CollectionVectorPartitionCoordinatorRouterSourceV1) OpenVectorPartitionCoordinatorRouterV1(ctx context.Context, index string, generation uint64) (VectorPartitionCoordinatorRouterV1, error) {
	if s.Collection == nil {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	router, _, err := s.Collection.OpenPreparedVectorPartitionRouterForGenerationWithContextV1(ctx, index, generation)
	if err != nil {
		return nil, err
	}
	if router == nil {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	return router, nil
}

// VectorPartitionShardSearchDispatcherV1 owns transport and connection
// cancellation. A local registry may implement it for tests and M6 evidence;
// production network evidence remains an M8 concern.
type VectorPartitionShardSearchDispatcherV1 interface {
	DispatchVectorPartitionShardSearchV1(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)
}

type VectorPartitionShardSearchDispatcherFuncV1 func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)

func (f VectorPartitionShardSearchDispatcherFuncV1) DispatchVectorPartitionShardSearchV1(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	return f(ctx, request)
}

type VectorPartitionCoordinatorOptionsV1 struct {
	Catalog                    raftplacement.ResolvedCatalogV1
	Placement                  raftplacement.VectorPartitionPlacementRecordV1
	RouterSource               VectorPartitionCoordinatorRouterSourceV1
	Dispatcher                 VectorPartitionShardSearchDispatcherV1
	ReplicatedLifecycle        VectorPartitionReplicatedLifecycleAuthorityV1
	RequireReplicatedLifecycle bool
	Limits                     VectorPartitionCoordinatorLimitsV1
	ShardLimits                VectorPartitionShardSearchLimitsV1
}

// VectorPartitionCoordinatorTopologyV1 is the public, transport-neutral M1
// topology projection needed to construct a coordinator outside TreeDB's
// internal Raft packages.
type VectorPartitionCoordinatorTopologyV1 struct {
	Database, Catalog, Collection    string
	CollectionGroupID                string
	IndexName, IndexDefinitionDigest string

	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	PartitionGeneration                                                uint64
	Groups                                                             []VectorPartitionCoordinatorTopologyGroupV1
	Partitions                                                         []VectorPartitionCoordinatorTopologyPartitionV1
}

type VectorPartitionCoordinatorTopologyGroupV1 struct {
	ID, LeaderHint string
	Members        []string
}

type VectorPartitionCoordinatorTopologyPartitionV1 struct {
	PartitionID uint32
	GroupID     string
}

type VectorPartitionCoordinatorRequestV1 struct {
	Version uint32

	RequestID, CancellationID        string
	Database, Catalog, Collection    string
	IndexName, IndexDefinitionDigest string

	Query                 []float32
	Metric                VectorPartitionShardSearchMetricV1
	RouterMode            string
	RouterCandidateBudget int
	PartitionProbes       int
	Consistency           VectorPartitionShardSearchConsistencyV1
	StatsMode             VectorPartitionShardSearchStatsModeV1
	TopK, EfSearch        int
	DeadlineUnixNano      int64
	RequestBytesLimit     uint64
	CandidateBytesLimit   uint64
	ResponseBytesLimit    uint64
	MergeEntriesLimit     int
}

type VectorPartitionCoordinatorNeighborV1 struct {
	ID    string
	Score float32
}

type VectorPartitionCoordinatorCountersV1 struct {
	SelectedPartitions, SelectedGroups                       uint64
	Requests, RPCs, Retries, Redirects                       uint64
	SnapshotPins, ReadProofs, GenerationPins, PartitionOpens uint64
	Cancellations, Failures                                  uint64
	QueryBytes, RequestBytes                                 uint64
	ResponseBytes, CandidateBytes                            uint64
	MaxShardPartitions                                       uint64
	MaxShardRequestBytes                                     uint64
	MaxShardResponseBytes                                    uint64
	MaxShardCandidateBytes                                   uint64
	Candidates, Edges, MergeEntries                          uint64
	Duplicates, ScoreDisagreements                           uint64
}

type VectorPartitionCoordinatorTimingV1 struct {
	RouterOpenNanos, RouterSearchNanos, PlacementNanos uint64
	LifecycleNanos, DispatchNanos                      uint64
	QueueNanos, RPCNanos, NetworkNanos                 uint64
	ReadIndexApplyNanos, GenerationOpenNanos           uint64
	ShardSearchNanos, ResponseNanos                    uint64
	DedupeNanos, MergeNanos, TotalNanos                uint64
}

type VectorPartitionCoordinatorResponseV1 struct {
	Version   uint32
	RequestID string

	SourceGeneration, SourceChecksum, SourceSchemaHash, SourceRowCount uint64
	PartitionGeneration, RouterGeneration                              uint64
	RouterModelDigest, ReadySetDigest                                  string
	Consistency                                                        VectorPartitionShardSearchConsistencyV1

	Neighbors        []VectorPartitionCoordinatorNeighborV1
	ProbedPartitions []uint32
	ProbedGroups     []raftcluster.GroupID
	Counters         VectorPartitionCoordinatorCountersV1
	Timing           VectorPartitionCoordinatorTimingV1
}

type VectorPartitionCoordinatorStatsV1 struct {
	Requests, Successes, Errors, Canceled, TimedOut uint64
	SelectedPartitions, SelectedGroups, RPCs        uint64
	Retries, Redirects, Duplicates, Disagreements   uint64
	TotalNanos                                      uint64
	RouterSessions                                  []VectorPartitionCoordinatorRouterSessionStatsV1
}

type vectorPartitionCoordinatorStatsAccumulatorV1 struct {
	mu    sync.Mutex
	value VectorPartitionCoordinatorStatsV1
}

// VectorPartitionCoordinatorRouterSessionIdentityV1 identifies the immutable
// M1/M4 epoch a coordinator is allowed to retain. A coordinator is immutable
// for this identity; a catalog or generation replacement constructs a new
// coordinator and drains the old one.
type VectorPartitionCoordinatorRouterSessionIdentityV1 struct {
	Database              string `json:"database"`
	Catalog               string `json:"catalog"`
	Collection            string `json:"collection"`
	IndexName             string `json:"index_name"`
	IndexDefinitionDigest string `json:"index_definition_digest"`
	SourceGeneration      uint64 `json:"source_generation"`
	SourceChecksum        uint64 `json:"source_checksum"`
	SourceSchemaHash      uint64 `json:"source_schema_hash"`
	SourceRowCount        uint64 `json:"source_row_count"`
	PartitionGeneration   uint64 `json:"partition_generation"`
	ReadySetDigest        string `json:"ready_set_digest"`
	RouterModelDigest     string `json:"router_model_digest"`
}

// VectorPartitionCoordinatorRouterSessionStatsV1 is a deterministic
// cumulative accounting record for one accepted router identity. Reader pins
// are the underlying persistent-generation pins; lease counters describe
// request-level concurrent use of that pin.
type VectorPartitionCoordinatorRouterSessionStatsV1 struct {
	Identity             VectorPartitionCoordinatorRouterSessionIdentityV1 `json:"identity"`
	ColdOpens            uint64                                            `json:"cold_opens"`
	ManifestOpenAttempts uint64                                            `json:"manifest_open_attempts"`
	Misses               uint64                                            `json:"misses"`
	Hits                 uint64                                            `json:"hits"`
	OpenFailures         uint64                                            `json:"open_failures"`
	ReaderPins           uint64                                            `json:"reader_pins"`
	ReaderReleases       uint64                                            `json:"reader_releases"`
	LeasePins            uint64                                            `json:"lease_pins"`
	LeaseReleases        uint64                                            `json:"lease_releases"`
	Invalidations        uint64                                            `json:"invalidations"`
	Closes               uint64                                            `json:"closes"`
}

type VectorPartitionCoordinatorV1 struct {
	placement           raftplacement.VectorPartitionPlacementRecordV1
	routerSource        VectorPartitionCoordinatorRouterSourceV1
	dispatcher          VectorPartitionShardSearchDispatcherV1
	replicatedLifecycle VectorPartitionReplicatedLifecycleAuthorityV1
	limits              VectorPartitionCoordinatorLimitsV1
	shardLimits         VectorPartitionShardSearchLimitsV1
	groups              map[raftcluster.GroupID]raftplacement.ResolvedGroupV1
	stats               vectorPartitionCoordinatorStatsAccumulatorV1
	sessionMu           sync.Mutex
	sessionCond         *sync.Cond
	sessions            map[vectorPartitionCoordinatorRouterKeyV1]*vectorPartitionCoordinatorRouterSessionV1
	loads               map[vectorPartitionCoordinatorRouterKeyV1]*vectorPartitionCoordinatorRouterLoadV1
	sessionStats        map[vectorPartitionCoordinatorRouterKeyV1]*vectorPartitionCoordinatorRouterSessionStatsV1
	closing             uint64
	leases              uint64
	closed              bool
	closeOnce           sync.Once
	closeErr            error
	routerCloseErr      error
}

type vectorPartitionCoordinatorStrictSearchV1 struct {
	snapshot *VectorPartitionServingSnapshotLeaseV1
	proof    vectorPartitionServingAuthorityProofV1
	key      []byte
}

// A router session owns one generation-pinned M4 router for the lifetime of a
// coordinator. Individual searches hold leases, so Close can reject new work,
// drain in-flight requests, and only then release the router's generation pin.
type vectorPartitionCoordinatorRouterKeyV1 struct {
	database, catalog, collection    string
	index, indexDefinitionDigest     string
	sourceGeneration, sourceChecksum uint64
	sourceSchemaHash, sourceRowCount uint64
	generation                       uint64
}

type vectorPartitionCoordinatorRouterSessionV1 struct {
	router           VectorPartitionCoordinatorRouterV1
	stats            *vectorPartitionCoordinatorRouterSessionStatsV1
	refs             uint64
	retired, closing bool
}

// vectorPartitionCoordinatorRouterSessionStatsV1 aggregates every reopen for
// the coordinator's one immutable placement identity. Keying this accounting
// by the bounded placement key prevents repeated lifecycle rejection from
// retaining an unbounded session history.
type vectorPartitionCoordinatorRouterSessionStatsV1 struct {
	accepted bool
	value    VectorPartitionCoordinatorRouterSessionStatsV1
}

type vectorPartitionCoordinatorRouterLoadV1 struct {
	ready chan struct{}
	err   error
}

type vectorPartitionCoordinatorRouterLeaseV1 struct {
	coordinator *VectorPartitionCoordinatorV1
	key         vectorPartitionCoordinatorRouterKeyV1
	session     *vectorPartitionCoordinatorRouterSessionV1
	once        sync.Once
	closeErr    error
}

func NewVectorPartitionCoordinatorV1(opts VectorPartitionCoordinatorOptionsV1) (*VectorPartitionCoordinatorV1, error) {
	limits, err := normalizeVectorPartitionCoordinatorLimitsV1(opts.Limits)
	if err != nil {
		return nil, err
	}
	shardLimits, err := normalizeVectorPartitionShardSearchLimitsV1(opts.ShardLimits)
	if err != nil {
		return nil, err
	}
	limits.MaxQueryBytes = min(limits.MaxQueryBytes, shardLimits.MaxQueryBytes)
	limits.MaxQueryBytes = min(limits.MaxQueryBytes, shardLimits.MaxDimensions*4)
	limits.MaxTopK = min(limits.MaxTopK, shardLimits.MaxTopK)
	limits.MaxEfSearch = min(limits.MaxEfSearch, shardLimits.MaxEfSearch)
	limits.MaxPartitionsPerRequest = min(limits.MaxPartitionsPerRequest, shardLimits.MaxPartitions)
	limits.MaxIdentityBytes = min(limits.MaxIdentityBytes, shardLimits.MaxIdentityBytes)
	limits.MaxStableIDBytes = min(limits.MaxStableIDBytes, shardLimits.MaxStableIDBytes)
	if opts.RouterSource == nil || opts.Dispatcher == nil {
		return nil, fmt.Errorf("%w: incomplete coordinator dependencies", ErrVectorPartitionCoordinatorInvalidRequest)
	}
	if opts.RequireReplicatedLifecycle && opts.ReplicatedLifecycle == nil {
		return nil, fmt.Errorf("%w: replicated lifecycle authority is required", ErrVectorPartitionCoordinatorUnavailable)
	}
	if err := opts.Catalog.ValidateVectorPartitionPlacementV1(opts.Placement); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorPartitionCoordinatorRouteMismatch, err)
	}
	groups := make(map[raftcluster.GroupID]raftplacement.ResolvedGroupV1, len(opts.Catalog.Groups))
	for _, group := range opts.Catalog.Groups {
		group.Members = slices.Clone(group.Members)
		groups[group.ID] = group
	}
	placement := opts.Placement
	placement.Partitions = slices.Clone(opts.Placement.Partitions)
	coordinator := &VectorPartitionCoordinatorV1{
		placement: placement, routerSource: opts.RouterSource,
		dispatcher: opts.Dispatcher, replicatedLifecycle: opts.ReplicatedLifecycle,
		limits: limits, shardLimits: shardLimits, groups: groups,
		sessions:     make(map[vectorPartitionCoordinatorRouterKeyV1]*vectorPartitionCoordinatorRouterSessionV1),
		loads:        make(map[vectorPartitionCoordinatorRouterKeyV1]*vectorPartitionCoordinatorRouterLoadV1),
		sessionStats: make(map[vectorPartitionCoordinatorRouterKeyV1]*vectorPartitionCoordinatorRouterSessionStatsV1),
	}
	coordinator.sessionCond = sync.NewCond(&coordinator.sessionMu)
	return coordinator, nil
}

// NewVectorPartitionCoordinatorForTopologyV1 validates a public topology into
// the canonical M1 catalog/placement representation before constructing M6.
func NewVectorPartitionCoordinatorForTopologyV1(
	topology VectorPartitionCoordinatorTopologyV1,
	routerSource VectorPartitionCoordinatorRouterSourceV1,
	dispatcher VectorPartitionShardSearchDispatcherV1,
	limits VectorPartitionCoordinatorLimitsV1,
) (*VectorPartitionCoordinatorV1, error) {
	ref := raftplacement.CollectionRefV1{
		Database: topology.Database, Catalog: topology.Catalog, Collection: topology.Collection,
	}
	groups := make([]raftplacement.GroupV1, len(topology.Groups))
	for i, input := range topology.Groups {
		members := make([]raftcluster.NodeID, len(input.Members))
		for j, member := range input.Members {
			members[j] = raftcluster.NodeID(member)
		}
		groups[i] = raftplacement.GroupV1{
			ID: raftcluster.GroupID(input.ID), Members: members, LeaderHint: raftcluster.NodeID(input.LeaderHint),
		}
	}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{
		Groups: groups,
		Placements: []raftplacement.CollectionPlacementV1{{
			Collection: ref, GroupID: raftcluster.GroupID(topology.CollectionGroupID),
			Mode: raftplacement.PlacementModeCollectionV1,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: topology catalog: %v", ErrVectorPartitionCoordinatorRouteMismatch, err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: topology.IndexName,
		IndexDefinitionDigest: topology.IndexDefinitionDigest,
		SourceGeneration:      topology.SourceGeneration, SourceChecksum: topology.SourceChecksum,
		SourceSchemaHash: topology.SourceSchemaHash, SourceRowCount: topology.SourceRowCount,
		PartitionGeneration: topology.PartitionGeneration,
		PartitionCount:      uint32(len(topology.Partitions)),
		Partitions:          make([]raftplacement.VectorPartitionGroupV1, len(topology.Partitions)),
	}
	for i, input := range topology.Partitions {
		placement.Partitions[i] = raftplacement.VectorPartitionGroupV1{
			PartitionID: input.PartitionID, GroupID: raftcluster.GroupID(input.GroupID),
		}
	}
	return NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{
		Catalog: catalog, Placement: placement, RouterSource: routerSource,
		Dispatcher: dispatcher, Limits: limits,
	})
}

func (c *VectorPartitionCoordinatorV1) acquireRouterSessionV1(ctx context.Context, index string, generation uint64) (*vectorPartitionCoordinatorRouterLeaseV1, error) {
	if c == nil || index == "" || generation == 0 {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	key, ok := c.routerSessionKeyV1(index, generation)
	if !ok {
		return nil, ErrVectorPartitionCoordinatorUnavailable
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.sessionMu.Lock()
		if c.closed {
			c.sessionMu.Unlock()
			return nil, fmt.Errorf("%w: coordinator closed", ErrVectorPartitionCoordinatorUnavailable)
		}
		if session := c.sessions[key]; session != nil {
			session.refs++
			c.leases++
			session.stats.value.Hits++
			session.stats.value.LeasePins++
			c.sessionMu.Unlock()
			return &vectorPartitionCoordinatorRouterLeaseV1{coordinator: c, key: key, session: session}, nil
		}
		if load := c.loads[key]; load != nil {
			c.sessionMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-load.ready:
				// A canceled cold open belongs only to its initiating request. Other
				// callers retry their own open rather than inheriting cancellation.
				// Every other failure is shared by the cold-open cohort so corrupt or
				// otherwise invalid assets are not repeatedly reconstructed.
				if load.err != nil && !errors.Is(load.err, context.Canceled) && !errors.Is(load.err, context.DeadlineExceeded) {
					return nil, load.err
				}
				continue
			}
		}
		load := &vectorPartitionCoordinatorRouterLoadV1{ready: make(chan struct{})}
		c.loads[key] = load
		stats := c.newRouterSessionStatsLocked(key)
		stats.value.ColdOpens++
		stats.value.ManifestOpenAttempts++
		stats.value.Misses++
		c.sessionMu.Unlock()

		router, err := c.routerSource.OpenVectorPartitionCoordinatorRouterV1(ctx, index, generation)
		c.sessionMu.Lock()
		delete(c.loads, key)
		if err == nil && router == nil {
			err = ErrVectorPartitionCoordinatorUnavailable
		}
		if c.closed && err == nil {
			err = fmt.Errorf("%w: coordinator closed", ErrVectorPartitionCoordinatorUnavailable)
		}
		if err == nil {
			session := &vectorPartitionCoordinatorRouterSessionV1{router: router, stats: stats, refs: 1}
			c.sessions[key] = session
			c.leases++
			stats.value.ReaderPins++
			stats.value.LeasePins++
			close(load.ready)
			c.sessionCond.Broadcast()
			c.sessionMu.Unlock()
			return &vectorPartitionCoordinatorRouterLeaseV1{coordinator: c, key: key, session: session}, nil
		}
		load.err = err
		close(load.ready)
		c.sessionCond.Broadcast()
		stats.value.OpenFailures++
		if router != nil {
			stats.value.ReaderPins++
			stats.value.Closes++
			c.closing++
		}
		c.sessionMu.Unlock()
		if router != nil {
			closeErr := c.closeRouterSessionV1(stats, router)
			err = errors.Join(err, closeErr)
		}
		return nil, err
	}
}

func (c *VectorPartitionCoordinatorV1) routerSessionKeyV1(index string, generation uint64) (vectorPartitionCoordinatorRouterKeyV1, bool) {
	if c == nil || index != c.placement.IndexName || generation != c.placement.PartitionGeneration {
		return vectorPartitionCoordinatorRouterKeyV1{}, false
	}
	p := c.placement
	return vectorPartitionCoordinatorRouterKeyV1{
		database: p.Collection.Database, catalog: p.Collection.Catalog, collection: p.Collection.Collection,
		index: index, indexDefinitionDigest: p.IndexDefinitionDigest,
		sourceGeneration: p.SourceGeneration, sourceChecksum: p.SourceChecksum,
		sourceSchemaHash: p.SourceSchemaHash, sourceRowCount: p.SourceRowCount,
		generation: generation,
	}, true
}

func (c *VectorPartitionCoordinatorV1) newRouterSessionStatsLocked(key vectorPartitionCoordinatorRouterKeyV1) *vectorPartitionCoordinatorRouterSessionStatsV1 {
	if stats := c.sessionStats[key]; stats != nil {
		return stats
	}
	stats := &vectorPartitionCoordinatorRouterSessionStatsV1{value: VectorPartitionCoordinatorRouterSessionStatsV1{Identity: VectorPartitionCoordinatorRouterSessionIdentityV1{
		Database: key.database, Catalog: key.catalog, Collection: key.collection,
		IndexName: key.index, IndexDefinitionDigest: key.indexDefinitionDigest,
		SourceGeneration: key.sourceGeneration, SourceChecksum: key.sourceChecksum,
		SourceSchemaHash: key.sourceSchemaHash, SourceRowCount: key.sourceRowCount,
		PartitionGeneration: key.generation,
	}}}
	c.sessionStats[key] = stats
	return stats
}

func (c *VectorPartitionCoordinatorV1) recordRouterSessionIdentityV1(lease *vectorPartitionCoordinatorRouterLeaseV1, status collections.VectorPartitionRouterRuntimeStatusV1, readySetDigest string) error {
	if lease == nil || lease.coordinator != c || lease.session == nil {
		return ErrVectorPartitionCoordinatorUnavailable
	}
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	stats := lease.session.stats
	if stats == nil {
		return ErrVectorPartitionCoordinatorUnavailable
	}
	if !stats.accepted {
		stats.value.Identity.ReadySetDigest = readySetDigest
		stats.value.Identity.RouterModelDigest = status.ModelDigest
		stats.accepted = true
		return nil
	}
	if stats.value.Identity.ReadySetDigest != readySetDigest || stats.value.Identity.RouterModelDigest != status.ModelDigest {
		return ErrVectorPartitionCoordinatorGenerationMismatch
	}
	return nil
}

func (c *VectorPartitionCoordinatorV1) retireRouterSessionV1(lease *vectorPartitionCoordinatorRouterLeaseV1) {
	if lease == nil || lease.coordinator != c || lease.session == nil {
		return
	}
	c.sessionMu.Lock()
	session := lease.session
	if !session.retired {
		session.retired = true
		if c.sessions[lease.key] == session {
			delete(c.sessions, lease.key)
		}
		if session.stats != nil {
			session.stats.value.Invalidations++
		}
	}
	c.sessionCond.Broadcast()
	c.sessionMu.Unlock()
}

func (l *vectorPartitionCoordinatorRouterLeaseV1) Close() error {
	if l == nil || l.coordinator == nil || l.session == nil {
		return nil
	}
	l.once.Do(func() {
		c := l.coordinator
		c.sessionMu.Lock()
		if l.session.refs > 0 {
			l.session.refs--
		}
		if c.leases > 0 {
			c.leases--
		}
		if l.session.stats != nil {
			l.session.stats.value.LeaseReleases++
		}
		closeRouter := c.routerToCloseLocked(l.session)
		c.sessionCond.Broadcast()
		c.sessionMu.Unlock()
		if closeRouter != nil {
			l.closeErr = c.closeRouterSessionV1(l.session.stats, closeRouter)
		}
	})
	return l.closeErr
}

func (c *VectorPartitionCoordinatorV1) routerToCloseLocked(session *vectorPartitionCoordinatorRouterSessionV1) VectorPartitionCoordinatorRouterV1 {
	if session == nil || session.closing || session.refs != 0 || (!session.retired && !c.closed) || session.router == nil {
		return nil
	}
	session.closing = true
	c.closing++
	if session.stats != nil {
		session.stats.value.Closes++
	}
	return session.router
}

func (c *VectorPartitionCoordinatorV1) closeRouterSessionV1(stats *vectorPartitionCoordinatorRouterSessionStatsV1, router VectorPartitionCoordinatorRouterV1) error {
	err := router.Close()
	c.sessionMu.Lock()
	c.closing--
	if stats != nil {
		stats.value.ReaderReleases++
	}
	if err != nil && c.routerCloseErr == nil {
		// Every owning lease returns its own close error. Retain the first one
		// for coordinator shutdown diagnostics without growing an unbounded
		// joined-error chain across repeated rejected reopen attempts.
		c.routerCloseErr = err
	}
	c.sessionCond.Broadcast()
	c.sessionMu.Unlock()
	return err
}

// Close permanently rejects new searches, waits for in-flight leases and
// cold opens to leave the coordinator, then closes every retained router.
func (c *VectorPartitionCoordinatorV1) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.sessionMu.Lock()
		c.closed = true
		for len(c.loads) != 0 || c.closing != 0 || c.leases != 0 {
			c.sessionCond.Wait()
		}
		routers := make([]struct {
			router VectorPartitionCoordinatorRouterV1
			stats  *vectorPartitionCoordinatorRouterSessionStatsV1
		}, 0, len(c.sessions))
		for key, session := range c.sessions {
			delete(c.sessions, key)
			session.retired = true
			if router := c.routerToCloseLocked(session); router != nil {
				routers = append(routers, struct {
					router VectorPartitionCoordinatorRouterV1
					stats  *vectorPartitionCoordinatorRouterSessionStatsV1
				}{router: router, stats: session.stats})
			}
		}
		c.sessionMu.Unlock()
		for _, item := range routers {
			c.closeRouterSessionV1(item.stats, item.router)
		}
		c.sessionMu.Lock()
		c.closeErr = c.routerCloseErr
		c.sessionMu.Unlock()
	})
	return c.closeErr
}

func normalizeVectorPartitionCoordinatorLimitsV1(limits VectorPartitionCoordinatorLimitsV1) (VectorPartitionCoordinatorLimitsV1, error) {
	defaults := DefaultVectorPartitionCoordinatorLimitsV1()
	fillInt := func(value *int, fallback int) {
		if *value == 0 {
			*value = fallback
		}
	}
	fillUint := func(value *uint64, fallback uint64) {
		if *value == 0 {
			*value = fallback
		}
	}
	fillInt(&limits.MaxSelectedPartitions, defaults.MaxSelectedPartitions)
	fillInt(&limits.MaxGroups, defaults.MaxGroups)
	fillInt(&limits.MaxRequests, defaults.MaxRequests)
	fillInt(&limits.MaxConcurrentRequests, defaults.MaxConcurrentRequests)
	if limits.MaxRetries == 0 {
		limits.MaxRetries = defaults.MaxRetries
	}
	if limits.MaxRedirects == 0 {
		limits.MaxRedirects = defaults.MaxRedirects
	}
	fillInt(&limits.MaxRouterCandidates, defaults.MaxRouterCandidates)
	fillInt(&limits.MaxQueryBytes, defaults.MaxQueryBytes)
	fillInt(&limits.MaxTopK, defaults.MaxTopK)
	fillInt(&limits.MaxEfSearch, defaults.MaxEfSearch)
	fillInt(&limits.MaxPartitionsPerRequest, defaults.MaxPartitionsPerRequest)
	fillInt(&limits.MaxIdentityBytes, defaults.MaxIdentityBytes)
	fillInt(&limits.MaxStableIDBytes, defaults.MaxStableIDBytes)
	fillInt(&limits.MaxMergeEntries, defaults.MaxMergeEntries)
	fillUint(&limits.MaxRequestBytes, defaults.MaxRequestBytes)
	fillUint(&limits.MaxCandidateBytes, defaults.MaxCandidateBytes)
	fillUint(&limits.MaxResponseBytes, defaults.MaxResponseBytes)
	if limits.MaxWallClock == 0 {
		limits.MaxWallClock = defaults.MaxWallClock
	}
	if limits.MaxSelectedPartitions < 1 || limits.MaxGroups < 1 || limits.MaxRequests < 1 ||
		limits.MaxConcurrentRequests < 1 || limits.MaxConcurrentRequests > limits.MaxRequests ||
		limits.MaxRetries < 0 || limits.MaxRedirects < 0 || limits.MaxRouterCandidates < 1 ||
		limits.MaxQueryBytes < 4 ||
		limits.MaxTopK < 1 ||
		limits.MaxEfSearch < limits.MaxTopK ||
		limits.MaxPartitionsPerRequest < 1 ||
		limits.MaxIdentityBytes < 1 ||
		limits.MaxStableIDBytes < 1 ||
		limits.MaxMergeEntries < 1 || limits.MaxRequestBytes == 0 ||
		limits.MaxCandidateBytes == 0 ||
		limits.MaxResponseBytes == 0 ||
		limits.MaxWallClock < time.Millisecond {
		return VectorPartitionCoordinatorLimitsV1{}, fmt.Errorf("%w: invalid coordinator limits", ErrVectorPartitionCoordinatorInvalidRequest)
	}
	return limits, nil
}

func (c *VectorPartitionCoordinatorV1) Search(ctx context.Context, request VectorPartitionCoordinatorRequestV1) (response VectorPartitionCoordinatorResponseV1, resultErr error) {
	return c.searchV1(ctx, request, nil)
}

func (c *VectorPartitionCoordinatorV1) searchStrictV1(ctx context.Context, request VectorPartitionCoordinatorRequestV1, snapshot *VectorPartitionServingSnapshotLeaseV1, proof vectorPartitionServingAuthorityProofV1, key []byte) (VectorPartitionCoordinatorResponseV1, error) {
	if snapshot == nil || snapshot.snapshot == nil {
		return VectorPartitionCoordinatorResponseV1{}, c.wrapError(ErrVectorPartitionCoordinatorUnavailable, "")
	}
	return c.searchV1(ctx, request, &vectorPartitionCoordinatorStrictSearchV1{snapshot: snapshot, proof: proof, key: key})
}

func (c *VectorPartitionCoordinatorV1) searchV1(ctx context.Context, request VectorPartitionCoordinatorRequestV1, strict *vectorPartitionCoordinatorStrictSearchV1) (response VectorPartitionCoordinatorResponseV1, resultErr error) {
	started := time.Now()
	if c == nil {
		return response, &VectorPartitionCoordinatorErrorV1{
			Code: VectorPartitionCoordinatorErrorUnavailableV1, Err: ErrVectorPartitionCoordinatorUnavailable,
		}
	}
	c.stats.begin()
	defer func() {
		total := elapsedNanosV1(started)
		if resultErr != nil {
			response.Timing.TotalNanos = total
			var coordinatorErr *VectorPartitionCoordinatorErrorV1
			if errors.As(resultErr, &coordinatorErr) {
				coordinatorErr.Counters = response.Counters
				coordinatorErr.Timing = response.Timing
			}
			response = VectorPartitionCoordinatorResponseV1{}
			c.stats.fail(classifyVectorPartitionCoordinatorErrorV1(resultErr), total)
			return
		}
		response.Timing.TotalNanos = total
		c.stats.succeed(response, total)
	}()

	requestCtx, cancel, err := vectorPartitionCoordinatorContextV1(
		ctx, request.DeadlineUnixNano, c.limits.MaxWallClock,
	)
	if err != nil {
		return response, c.wrapError(err, "")
	}
	defer cancel()
	if err := c.validateRequest(request); err != nil {
		return response, c.wrapError(err, "")
	}
	request.Query = slices.Clone(request.Query)
	if deadline, ok := requestCtx.Deadline(); ok {
		request.DeadlineUnixNano = deadline.UnixNano()
	}

	openStarted := time.Now()
	var routerLease *vectorPartitionCoordinatorRouterLeaseV1
	var router VectorPartitionCoordinatorRouterV1
	if strict == nil {
		routerLease, err = c.acquireRouterSessionV1(requestCtx, request.IndexName, c.placement.PartitionGeneration)
		if err != nil {
			return response, c.wrapError(err, "")
		}
		defer func() {
			if closeErr := routerLease.Close(); closeErr != nil {
				resultErr = errors.Join(resultErr, c.wrapError(fmt.Errorf("%w: close router: %w", ErrVectorPartitionCoordinatorUnavailable, closeErr), ""))
			}
		}()
		router = routerLease.session.router
	} else {
		router = strict.snapshot.snapshot.router.session.router
	}
	response.Timing.RouterOpenNanos = elapsedNanosV1(openStarted)
	if router == nil {
		return response, c.wrapError(ErrVectorPartitionCoordinatorUnavailable, "")
	}
	status := router.Status()
	if err := c.validateRouterStatus(status); err != nil {
		if strict == nil && vectorPartitionCoordinatorRouterStatusInvalidatesSessionV1(err) {
			c.retireRouterSessionV1(routerLease)
		}
		return response, c.wrapError(err, "")
	}
	replicatedReadySetDigest := ""
	if strict == nil {
		lifecycleStarted := time.Now()
		replicatedReadySetDigest, err = c.validateReplicatedLifecycle(
			raftcluster.WithCatalogMetaReadSourceV1(requestCtx, raftcluster.CatalogMetaReadSourceCoordinatorLifecycleV1), status,
		)
		response.Timing.LifecycleNanos = elapsedNanosV1(lifecycleStarted)
		if err != nil {
			if vectorPartitionCoordinatorReplicatedLifecycleInvalidatesSessionV1(err) {
				c.retireRouterSessionV1(routerLease)
			}
			return response, c.wrapError(err, "")
		}
		if err := c.recordRouterSessionIdentityV1(routerLease, status, replicatedReadySetDigest); err != nil {
			c.retireRouterSessionV1(routerLease)
			return response, c.wrapError(err, "")
		}
	} else {
		identity := strict.snapshot.IdentityV1()
		if identity.ManifestIntegrityDigest != status.Manifest.IntegrityDigest {
			return response, c.wrapError(fmt.Errorf("%w: strict manifest integrity", ErrVectorPartitionCoordinatorGenerationMismatch), "")
		}
		if identity.RouterModelDigest != status.ModelDigest {
			return response, c.wrapError(fmt.Errorf("%w: strict router model", ErrVectorPartitionCoordinatorGenerationMismatch), "")
		}
		replicatedReadySetDigest = identity.ReadySetDigest
	}
	if err := validateVectorPartitionCoordinatorRouterRequestV1(request, status); err != nil {
		return response, c.wrapError(err, "")
	}

	routerStarted := time.Now()
	routed, err := router.SearchWithContextV1(requestCtx, request.Query, collections.VectorPartitionRouterSearchOptionsV1{
		Mode: request.RouterMode, CandidateBudget: request.RouterCandidateBudget, PartitionProbes: request.PartitionProbes,
	})
	response.Timing.RouterSearchNanos = elapsedNanosV1(routerStarted)
	if err != nil {
		return response, c.wrapError(err, "")
	}
	if routed.Status.Selected != uint64(len(routed.Partitions)) ||
		len(routed.Partitions) != request.PartitionProbes {
		return response, c.wrapError(fmt.Errorf("%w: router selected=%d want=%d", ErrVectorPartitionCoordinatorMalformedResponse, len(routed.Partitions), request.PartitionProbes), "")
	}

	placementStarted := time.Now()
	tasks, selectedPartitions, selectedGroups, budget, err := c.plan(requestCtx, request, status, replicatedReadySetDigest, routed.Partitions, strict)
	response.Timing.PlacementNanos = elapsedNanosV1(placementStarted)
	if err != nil {
		return response, c.wrapError(err, "")
	}
	if err := requestCtx.Err(); err != nil {
		return response, c.wrapError(err, "")
	}

	counters := VectorPartitionCoordinatorCountersV1{
		SelectedPartitions: uint64(len(selectedPartitions)), SelectedGroups: uint64(len(selectedGroups)),
		Requests: uint64(len(tasks)), QueryBytes: uint64(len(request.Query)) * 4,
		RequestBytes: budget.requestBytes,
	}
	for _, task := range tasks {
		counters.MaxShardPartitions = max(counters.MaxShardPartitions, uint64(len(task.partitionIDs)))
	}
	response.Counters = counters
	dispatchStarted := time.Now()
	taskResults, dispatchErr := c.dispatch(requestCtx, tasks)
	response.Timing.DispatchNanos = elapsedNanosV1(dispatchStarted)
	for _, result := range taskResults {
		if !accumulateVectorPartitionCoordinatorResponseCountersV1(&counters, result.response) {
			return response, c.wrapError(ErrVectorPartitionCoordinatorBudgetExceeded, "")
		}
		if !accumulateVectorPartitionCoordinatorTimingV1(&response.Timing, result) {
			return response, c.wrapError(ErrVectorPartitionCoordinatorMalformedResponse, "")
		}
		counters.RPCs += result.rpcs
		counters.Retries += result.retries
		counters.Redirects += result.redirects
		counters.MaxShardRequestBytes = max(counters.MaxShardRequestBytes, result.maxRequestBytes)
	}
	response.Counters = counters
	if dispatchErr != nil {
		return response, dispatchErr
	}
	if err := requestCtx.Err(); err != nil {
		return response, c.wrapError(err, "")
	}
	if counters.ResponseBytes > request.ResponseBytesLimit ||
		counters.CandidateBytes > request.CandidateBytesLimit {
		return response, c.wrapError(ErrVectorPartitionCoordinatorBudgetExceeded, "")
	}

	dedupeStarted := time.Now()
	unique, duplicates, disagreements, err := c.dedupe(requestCtx, request, taskResults)
	response.Timing.DedupeNanos = elapsedNanosV1(dedupeStarted)
	if err != nil {
		return response, c.wrapError(err, "")
	}
	counters.Duplicates = duplicates
	counters.ScoreDisagreements = disagreements
	counters.MergeEntries = uint64(len(unique))

	mergeStarted := time.Now()
	neighbors, err := topVectorPartitionCoordinatorNeighborsV1(requestCtx, unique, request.TopK)
	response.Timing.MergeNanos = elapsedNanosV1(mergeStarted)
	if err != nil {
		return response, c.wrapError(err, "")
	}
	response.Version = VectorPartitionCoordinatorVersionV1
	response.RequestID = request.RequestID
	response.SourceGeneration = status.Manifest.SourceGeneration
	response.SourceChecksum = status.Manifest.SourceChecksum
	response.SourceSchemaHash = status.Manifest.SourceSchemaHash
	response.SourceRowCount = status.Manifest.SourceRowCount
	response.PartitionGeneration = status.Manifest.Generation
	response.RouterGeneration = status.Manifest.RouterGeneration
	response.RouterModelDigest = status.ModelDigest
	response.ReadySetDigest = replicatedReadySetDigest
	response.Consistency = VectorPartitionShardSearchConsistencySnapshotV1
	response.Neighbors = neighbors
	response.ProbedPartitions = selectedPartitions
	response.ProbedGroups = selectedGroups
	response.Counters = counters
	return response, nil
}

// validateReplicatedLifecycle binds the coordinator's locally opened router to
// the single active catalog generation on every request. It is deliberately
// after local manifest validation and before router search/dispatch: prepared,
// invalidated, or failover-stale state cannot select partitions or issue shard
// RPCs. A nil authority preserves the standalone M6 test harness; M7 cluster
// construction supplies the replicated authority and therefore fails closed
// when it is unavailable.
func (c *VectorPartitionCoordinatorV1) validateReplicatedLifecycle(ctx context.Context, status collections.VectorPartitionRouterRuntimeStatusV1) (string, error) {
	if c == nil || c.replicatedLifecycle == nil {
		return status.Manifest.ReadySetDigest, nil
	}
	m := status.Manifest
	return c.replicatedLifecycle.ValidateVectorPartitionGenerationSearchV1(
		ctx, c.placement.Collection, m.IndexName, m.Generation,
		m.IndexDefinitionDigest, m.SourceGeneration, m.SourceChecksum,
		m.SourceSchemaHash, m.SourceRowCount,
	)
}

func vectorPartitionCoordinatorReplicatedLifecycleInvalidatesSessionV1(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) &&
		(errors.Is(err, raftplacement.ErrVectorPartitionLifecycleGuard) ||
			errors.Is(err, raftplacement.ErrVectorPartitionLifecycleIdentity))
}

func accumulateVectorPartitionCoordinatorResponseCountersV1(
	counters *VectorPartitionCoordinatorCountersV1,
	response VectorPartitionShardSearchResponseV1,
) bool {
	if counters == nil {
		return false
	}
	responseBytes, responseBytesOK := addUint64V1(counters.ResponseBytes, response.ResponseBytes)
	candidates, candidatesOK := addUint64V1(counters.Candidates, response.Candidates)
	edges, edgesOK := addUint64V1(counters.Edges, response.Edges)
	readProofs, readProofsOK := addUint64V1(counters.ReadProofs, response.ReadProofs)
	generationPins, generationPinsOK := addUint64V1(counters.GenerationPins, response.GenerationPins)
	partitionOpens, partitionOpensOK := addUint64V1(counters.PartitionOpens, response.PartitionOpens)
	candidateBytes, candidateBytesOK := mulUint64V1(response.Candidates, 64)
	if !responseBytesOK || !candidatesOK || !edgesOK || !readProofsOK || !generationPinsOK || !partitionOpensOK || !candidateBytesOK {
		return false
	}
	shardCandidateBytes := candidateBytes
	candidateBytes, candidateBytesOK = addUint64V1(counters.CandidateBytes, candidateBytes)
	if !candidateBytesOK {
		return false
	}
	counters.ResponseBytes = responseBytes
	counters.Candidates = candidates
	counters.Edges = edges
	counters.ReadProofs = readProofs
	counters.GenerationPins = generationPins
	counters.PartitionOpens = partitionOpens
	counters.CandidateBytes = candidateBytes
	counters.MaxShardResponseBytes = max(counters.MaxShardResponseBytes, response.ResponseBytes)
	counters.MaxShardCandidateBytes = max(counters.MaxShardCandidateBytes, shardCandidateBytes)
	return true
}

func accumulateVectorPartitionCoordinatorTimingV1(
	timing *VectorPartitionCoordinatorTimingV1,
	result vectorPartitionCoordinatorTaskResultV1,
) bool {
	if timing == nil {
		return false
	}
	next := *timing
	var ok bool
	if next.QueueNanos, ok = addUint64V1(next.QueueNanos, result.queueNanos); !ok {
		return false
	}
	if next.RPCNanos, ok = addUint64V1(next.RPCNanos, result.rpcNanos); !ok {
		return false
	}
	if next.NetworkNanos, ok = addUint64V1(next.NetworkNanos, result.networkNanos); !ok {
		return false
	}
	if next.ReadIndexApplyNanos, ok = addUint64V1(next.ReadIndexApplyNanos, result.response.Timing.ReadIndexApplyNanos); !ok {
		return false
	}
	if next.GenerationOpenNanos, ok = addUint64V1(next.GenerationOpenNanos, result.response.Timing.GenerationOpenNanos); !ok {
		return false
	}
	if next.ShardSearchNanos, ok = addUint64V1(next.ShardSearchNanos, result.response.Timing.SearchNanos); !ok {
		return false
	}
	if next.ResponseNanos, ok = addUint64V1(next.ResponseNanos, result.response.Timing.ResponseCopyNanos); !ok {
		return false
	}
	*timing = next
	return true
}

func vectorPartitionCoordinatorContextV1(
	ctx context.Context,
	deadlineUnixNano int64,
	maxWallClock time.Duration,
) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, func() {}, err
	}
	now := time.Now()
	deadline := now.Add(maxWallClock)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if deadlineUnixNano != 0 {
		requestDeadline := time.Unix(0, deadlineUnixNano)
		if !requestDeadline.After(now) {
			return nil, func() {}, context.DeadlineExceeded
		}
		if requestDeadline.Before(deadline) {
			deadline = requestDeadline
		}
	}
	if !deadline.After(now) {
		return nil, func() {}, context.DeadlineExceeded
	}
	child, cancel := context.WithDeadline(ctx, deadline)
	return child, cancel, nil
}

func (c *VectorPartitionCoordinatorV1) validateRequest(request VectorPartitionCoordinatorRequestV1) error {
	l := c.limits
	p := c.placement
	if request.Version != VectorPartitionCoordinatorVersionV1 ||
		request.RequestID == "" || request.CancellationID == "" ||
		request.Database != p.Collection.Database || request.Catalog != p.Collection.Catalog ||
		request.Collection != p.Collection.Collection || request.IndexName != p.IndexName ||
		request.IndexDefinitionDigest != p.IndexDefinitionDigest ||
		len(request.Query) < 1 || len(request.Query)*4 > l.MaxQueryBytes ||
		request.PartitionProbes < 1 || request.PartitionProbes > l.MaxSelectedPartitions ||
		request.PartitionProbes > int(p.PartitionCount) ||
		request.RouterCandidateBudget < 1 || request.RouterCandidateBudget > l.MaxRouterCandidates ||
		request.RouterMode == collections.VectorPartitionRouterModeApproxV1 &&
			request.RouterCandidateBudget < request.PartitionProbes ||
		request.TopK < 1 || request.TopK > l.MaxTopK ||
		request.EfSearch < request.TopK || request.EfSearch > l.MaxEfSearch ||
		request.MergeEntriesLimit < 1 || request.MergeEntriesLimit > l.MaxMergeEntries {
		return ErrVectorPartitionCoordinatorInvalidRequest
	}
	for _, identity := range []string{
		request.RequestID, request.CancellationID, request.Database, request.Catalog,
		request.Collection, request.IndexName, request.IndexDefinitionDigest,
	} {
		if len(identity) > l.MaxIdentityBytes {
			return fmt.Errorf("%w: identity bytes", ErrVectorPartitionCoordinatorInvalidRequest)
		}
	}
	if !isVectorPartitionShardSearchDigestV1(request.IndexDefinitionDigest) ||
		request.Metric != VectorPartitionShardSearchMetricCosineV1 ||
		request.RouterMode != collections.VectorPartitionRouterModeExactV1 &&
			request.RouterMode != collections.VectorPartitionRouterModeApproxV1 ||
		request.Consistency != VectorPartitionShardSearchConsistencySnapshotV1 ||
		request.StatsMode != VectorPartitionShardSearchStatsBasicV1 {
		return ErrVectorPartitionCoordinatorInvalidRequest
	}
	var norm float64
	for _, value := range request.Query {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("%w: nonfinite query", ErrVectorPartitionCoordinatorInvalidRequest)
		}
		norm += float64(value) * float64(value)
	}
	if norm == 0 || math.IsNaN(norm) || math.IsInf(norm, 0) {
		return fmt.Errorf("%w: invalid query norm", ErrVectorPartitionCoordinatorInvalidRequest)
	}
	invNorm := 1 / math.Sqrt(norm)
	if invNorm > math.MaxFloat32 || math.IsNaN(invNorm) || math.IsInf(invNorm, 0) {
		return fmt.Errorf("%w: query inverse norm out of range", ErrVectorPartitionCoordinatorInvalidRequest)
	}
	mergeEntries, ok := mulUint64V1(uint64(request.PartitionProbes), uint64(request.TopK))
	if !ok || mergeEntries > uint64(request.MergeEntriesLimit) {
		return fmt.Errorf("%w: merge entries", ErrVectorPartitionCoordinatorBudgetExceeded)
	}
	if request.RequestBytesLimit == 0 || request.RequestBytesLimit > l.MaxRequestBytes ||
		request.CandidateBytesLimit == 0 || request.CandidateBytesLimit > l.MaxCandidateBytes ||
		request.ResponseBytesLimit == 0 || request.ResponseBytesLimit > l.MaxResponseBytes {
		return ErrVectorPartitionCoordinatorBudgetExceeded
	}
	return nil
}

func (c *VectorPartitionCoordinatorV1) validateRouterStatus(status collections.VectorPartitionRouterRuntimeStatusV1) error {
	m := status.Manifest
	p := c.placement
	if m.State != "ready" || m.Collection != p.Collection.Collection ||
		m.IndexName != p.IndexName || m.IndexDefinitionDigest != p.IndexDefinitionDigest ||
		m.SourceGeneration != p.SourceGeneration || m.SourceChecksum != p.SourceChecksum ||
		m.SourceSchemaHash != p.SourceSchemaHash || m.SourceRowCount != p.SourceRowCount ||
		m.Generation != p.PartitionGeneration || m.RouterGeneration != p.PartitionGeneration ||
		m.PartitionCount != p.PartitionCount || len(m.Placements) != len(p.Partitions) ||
		!isVectorPartitionShardSearchDigestV1(m.ReadySetDigest) ||
		!isVectorPartitionShardSearchDigestV1(status.ModelDigest) {
		return ErrVectorPartitionCoordinatorGenerationMismatch
	}
	if status.Partitions != uint64(m.PartitionCount) || status.Representatives == 0 {
		return ErrVectorPartitionCoordinatorGenerationMismatch
	}
	for i := range p.Partitions {
		if m.Placements[i].PartitionID != p.Partitions[i].PartitionID ||
			m.Placements[i].GroupID != string(p.Partitions[i].GroupID) {
			return fmt.Errorf("%w: partition %d placement", ErrVectorPartitionCoordinatorRouteMismatch, i)
		}
	}
	return nil
}

func validateVectorPartitionCoordinatorRouterRequestV1(request VectorPartitionCoordinatorRequestV1, status collections.VectorPartitionRouterRuntimeStatusV1) error {
	if request.RouterMode == collections.VectorPartitionRouterModeExactV1 &&
		request.RouterCandidateBudget < int(status.Representatives) {
		return fmt.Errorf("%w: exact router candidate budget", ErrVectorPartitionCoordinatorBudgetExceeded)
	}
	return nil
}

func vectorPartitionCoordinatorRouterStatusInvalidatesSessionV1(err error) bool {
	return errors.Is(err, ErrVectorPartitionCoordinatorGenerationMismatch) ||
		errors.Is(err, ErrVectorPartitionCoordinatorRouteMismatch)
}

type vectorPartitionCoordinatorBudgetV1 struct {
	requestBytes uint64
}

type vectorPartitionCoordinatorTaskV1 struct {
	index         int
	group         raftplacement.ResolvedGroupV1
	partitionIDs  []uint32
	candidateRows []uint64
	request       VectorPartitionShardSearchRequestV1
	queuedAt      time.Time
}

func (c *VectorPartitionCoordinatorV1) plan(ctx context.Context, request VectorPartitionCoordinatorRequestV1, status collections.VectorPartitionRouterRuntimeStatusV1, readySetDigest string, routed []collections.VectorPartitionRouterPartitionScoreV1, strict *vectorPartitionCoordinatorStrictSearchV1) ([]vectorPartitionCoordinatorTaskV1, []uint32, []raftcluster.GroupID, vectorPartitionCoordinatorBudgetV1, error) {
	var zero vectorPartitionCoordinatorBudgetV1
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, zero, err
	}
	if len(routed) < 1 || len(routed) > c.limits.MaxSelectedPartitions {
		return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	if !isVectorPartitionShardSearchDigestV1(readySetDigest) {
		return nil, nil, nil, zero, ErrVectorPartitionCoordinatorGenerationMismatch
	}
	selected := make([]uint32, len(routed))
	byGroup := make(map[raftcluster.GroupID][]uint32)
	seen := make(map[uint32]struct{}, len(routed))
	for i, score := range routed {
		if score.PartitionID >= c.placement.PartitionCount ||
			math.IsNaN(score.Distance) || math.IsInf(score.Distance, 0) {
			return nil, nil, nil, zero, ErrVectorPartitionCoordinatorMalformedResponse
		}
		if _, duplicate := seen[score.PartitionID]; duplicate {
			return nil, nil, nil, zero, ErrVectorPartitionCoordinatorMalformedResponse
		}
		seen[score.PartitionID] = struct{}{}
		selected[i] = score.PartitionID
		groupID := c.placement.Partitions[score.PartitionID].GroupID
		if _, ok := c.groups[groupID]; !ok {
			return nil, nil, nil, zero, ErrVectorPartitionCoordinatorRouteMismatch
		}
		byGroup[groupID] = append(byGroup[groupID], score.PartitionID)
	}
	if len(byGroup) > c.limits.MaxGroups {
		return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	groupIDs := make([]raftcluster.GroupID, 0, len(byGroup))
	for groupID := range byGroup {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	taskCount := 0
	for _, groupID := range groupIDs {
		sort.Slice(byGroup[groupID], func(i, j int) bool { return byGroup[groupID][i] < byGroup[groupID][j] })
		taskCount += (len(byGroup[groupID]) + c.limits.MaxPartitionsPerRequest - 1) / c.limits.MaxPartitionsPerRequest
	}
	if taskCount < 1 || taskCount > c.limits.MaxRequests {
		return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
	}

	shardLimits := c.shardLimits
	tasks := make([]vectorPartitionCoordinatorTaskV1, 0, taskCount)
	var totalRequestBytes, totalResponseReservation uint64
	var ok bool
	candidateRows, totalCandidateWeight, err := vectorPartitionCoordinatorCandidateRowsV1(
		ctx, status.Manifest, selected,
	)
	if err != nil {
		return nil, nil, nil, zero, err
	}
	candidateFloors, totalCandidateFloor, err := vectorPartitionCoordinatorCandidateFloorsV1(
		candidateRows, selected, len(request.Query), request.TopK, request.EfSearch,
	)
	if err != nil || totalCandidateFloor > request.CandidateBytesLimit {
		return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	candidateSurplus := request.CandidateBytesLimit - totalCandidateFloor
	var candidateWeightCursor uint64
	for _, groupID := range groupIDs {
		group := c.groups[groupID]
		partitions := byGroup[groupID]
		for start := 0; start < len(partitions); start += c.limits.MaxPartitionsPerRequest {
			end := min(start+c.limits.MaxPartitionsPerRequest, len(partitions))
			ids := slices.Clone(partitions[start:end])
			rows := make([]uint64, len(ids))
			taskIndex := len(tasks)
			target := group.LeaderHint
			if target == "" {
				target = group.Members[0]
			}
			requestID := fmt.Sprintf("%s/%04d", request.RequestID, taskIndex)
			if len(requestID) > c.limits.MaxIdentityBytes ||
				len(groupID) > c.limits.MaxIdentityBytes ||
				len(target) > c.limits.MaxIdentityBytes {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			longestTarget := target
			for _, member := range group.Members {
				if len(member) > c.limits.MaxIdentityBytes {
					return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
				}
				if len(member) > len(longestTarget) {
					longestTarget = member
				}
			}
			shardRequest := VectorPartitionShardSearchRequestV1{
				Version:   VectorPartitionShardSearchVersionV1,
				RequestID: requestID, CancellationID: request.CancellationID,
				Database: request.Database, Catalog: request.Catalog, Collection: request.Collection,
				IndexName: request.IndexName, IndexDefinitionDigest: request.IndexDefinitionDigest,
				SourceGeneration: status.Manifest.SourceGeneration, SourceChecksum: status.Manifest.SourceChecksum,
				SourceSchemaHash: status.Manifest.SourceSchemaHash, SourceRowCount: status.Manifest.SourceRowCount,
				PartitionGeneration: status.Manifest.Generation, RouterGeneration: status.Manifest.RouterGeneration,
				ReadySetDigest: readySetDigest,
				TargetGroupID:  groupID, TargetNodeID: target, PartitionIDs: ids,
				Query: request.Query, Metric: request.Metric, Mode: VectorPartitionShardSearchModeNoDocumentV1,
				Consistency: request.Consistency, StatsMode: VectorPartitionShardSearchStatsBasicV1,
				TopK: request.TopK, EfSearch: request.EfSearch, DeadlineUnixNano: request.DeadlineUnixNano,
			}
			responseReservation, err := vectorPartitionCoordinatorShardResponseReservationV1(
				len(ids), request.TopK, shardLimits.MaxStableIDBytes,
			)
			if err != nil || responseReservation > shardLimits.MaxResponseBytes {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			totalResponseReservation, ok = addUint64V1(totalResponseReservation, responseReservation)
			if !ok || totalResponseReservation > request.ResponseBytesLimit {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			shardRequest.ResponseBytesLimit = responseReservation
			var taskCandidateWeight uint64
			var baseline uint64
			for i, partitionID := range ids {
				rows[i] = candidateRows[partitionID]
				taskCandidateWeight, ok = addUint64V1(taskCandidateWeight, candidateRows[partitionID])
				if ok {
					baseline, ok = addUint64V1(baseline, candidateFloors[partitionID])
				}
				if !ok {
					return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
				}
			}
			weightedSurplus := vectorPartitionCoordinatorWeightedBudgetShareV1(
				candidateSurplus, totalCandidateWeight, candidateWeightCursor, taskCandidateWeight,
			)
			candidateShare, ok := addUint64V1(baseline, weightedSurplus)
			if !ok {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			candidateWeightCursor, ok = addUint64V1(candidateWeightCursor, taskCandidateWeight)
			if !ok {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			if baseline > shardLimits.MaxCandidateBytes {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			candidateShare = min(candidateShare, shardLimits.MaxCandidateBytes)
			shardRequest.CandidateBytesLimit = candidateShare
			if strict != nil {
				identity := strict.snapshot.IdentityV1()
				groupApplied := uint64(0)
				for _, ready := range identity.ReadyGroups {
					if ready.GroupID == groupID {
						groupApplied = ready.AppliedIndex
						break
					}
				}
				shardRequest.StrictCapability, err = prepareVectorPartitionStrictSearchCapabilityV1(shardRequest, identity, strict.proof.read, groupApplied, strict.key)
				if err != nil {
					return nil, nil, nil, zero, err
				}
			}
			budgetRequest := shardRequest
			budgetRequest.TargetNodeID = longestTarget
			requestBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(budgetRequest)
			if err != nil || requestBytes > shardLimits.MaxRequestBytes {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			shardRequest.RequestBytesLimit = requestBytes
			if strict != nil {
				shardRequest.StrictCapability.MAC, err = vectorPartitionStrictSearchCapabilityMACV1(shardRequest, *shardRequest.StrictCapability, strict.key)
				if err != nil {
					return nil, nil, nil, zero, err
				}
			}
			totalRequestBytes, ok = addUint64V1(totalRequestBytes, requestBytes)
			if !ok || totalRequestBytes > request.RequestBytesLimit {
				return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
			}
			tasks = append(tasks, vectorPartitionCoordinatorTaskV1{
				index: taskIndex, group: group, partitionIDs: ids, candidateRows: rows, request: shardRequest,
			})
		}
	}
	mergeEntries, ok := mulUint64V1(uint64(len(selected)), uint64(request.TopK))
	if !ok || mergeEntries > uint64(request.MergeEntriesLimit) {
		return nil, nil, nil, zero, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	return tasks, selected, groupIDs, vectorPartitionCoordinatorBudgetV1{requestBytes: totalRequestBytes}, nil
}

func vectorPartitionCoordinatorCandidateRowsV1(
	ctx context.Context,
	manifest collections.VectorPartitionManifestV1,
	selected []uint32,
) ([]uint64, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if manifest.PartitionCount == 0 {
		return nil, 0, ErrVectorPartitionCoordinatorGenerationMismatch
	}
	weights := make([]uint64, manifest.PartitionCount)
	selectedSet := make([]bool, manifest.PartitionCount)
	for _, partitionID := range selected {
		if partitionID >= manifest.PartitionCount {
			return nil, 0, ErrVectorPartitionCoordinatorGenerationMismatch
		}
		selectedSet[partitionID] = true
	}
	countMembership := func(memberships []collections.VectorPartitionMembershipV1) error {
		for ordinal, membership := range memberships {
			if ordinal&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if membership.PartitionID >= manifest.PartitionCount {
				return ErrVectorPartitionCoordinatorGenerationMismatch
			}
			if !selectedSet[membership.PartitionID] {
				continue
			}
			count, ok := addUint64V1(weights[membership.PartitionID], 1)
			if !ok {
				return ErrVectorPartitionCoordinatorBudgetExceeded
			}
			weights[membership.PartitionID] = count
		}
		return nil
	}
	if err := countMembership(manifest.Memberships); err != nil {
		return nil, 0, err
	}
	if err := countMembership(manifest.OverlapMemberships); err != nil {
		return nil, 0, err
	}
	var total uint64
	for _, partitionID := range selected {
		var ok bool
		total, ok = addUint64V1(total, weights[partitionID])
		if !ok {
			return nil, 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	return weights, total, nil
}

func vectorPartitionCoordinatorCandidateFloorsV1(
	rows []uint64,
	selected []uint32,
	dimensions, topK, efSearch int,
) ([]uint64, uint64, error) {
	floors := make([]uint64, len(rows))
	var total uint64
	for _, partitionID := range selected {
		if int(partitionID) >= len(rows) || rows[partitionID] > uint64(math.MaxInt) {
			return nil, 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
		scratchBytes, err := collections.VectorPartitionConservativeSearchScratchBytesV1(
			int(rows[partitionID]),
			dimensions,
			collections.VectorPartitionSearchOptionsV1{TopK: topK, EfSearch: efSearch},
		)
		if err != nil {
			return nil, 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
		candidateBytes, ok := mulUint64V1(rows[partitionID], 64)
		if !ok {
			return nil, 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
		traversalBytes, ok := mulUint64V1(uint64(efSearch), 64)
		if !ok {
			return nil, 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
		floors[partitionID] = max(candidateBytes, scratchBytes, traversalBytes)
		total, ok = addUint64V1(total, floors[partitionID])
		if !ok {
			return nil, 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
	}
	return floors, total, nil
}

func vectorPartitionCoordinatorWeightedBudgetShareV1(total, totalWeight, startWeight, weight uint64) uint64 {
	if totalWeight == 0 || weight == 0 || startWeight > totalWeight || weight > totalWeight-startWeight {
		return 0
	}
	perWeight := total / totalWeight
	remainder := total % totalWeight
	share := perWeight * weight
	extraStart := min(startWeight, remainder)
	extraEnd := min(startWeight+weight, remainder)
	return share + extraEnd - extraStart
}

func vectorPartitionCoordinatorShardRequestBytesV1(request VectorPartitionShardSearchRequestV1) (uint64, error) {
	size, ok := addUint64V1(vectorPartitionShardSearchTCPRequestFixedBytesV1, uint64(len(request.Query))*4)
	if ok {
		size, ok = addUint64V1(size, uint64(len(request.PartitionIDs))*4)
	}
	if !ok {
		return 0, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	for _, identity := range []string{
		request.RequestID, request.CancellationID, request.Database, request.Catalog,
		request.Collection, request.IndexName, request.IndexDefinitionDigest, request.ReadySetDigest,
		string(request.TargetGroupID), string(request.TargetNodeID),
	} {
		size, ok = addUint64V1(size, uint64(len(identity)))
		if !ok {
			return 0, ErrVectorPartitionCoordinatorBudgetExceeded
		}
	}
	capabilityBytes, err := vectorPartitionStrictSearchCapabilityBytesV1(request.StrictCapability)
	if err != nil {
		return 0, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	size, ok = addUint64V1(size, capabilityBytes)
	if !ok {
		return 0, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	return size, nil
}

func vectorPartitionCoordinatorShardResponseReservationV1(partitions, topK, maxStableIDBytes int) (uint64, error) {
	partialBytes, ok := mulUint64V1(uint64(partitions), vectorPartitionShardSearchPartialEnvelopeBytesV1)
	if ok {
		partialBytes, ok = addUint64V1(vectorPartitionShardSearchResponseEnvelopeBytesV1, partialBytes)
	}
	results, resultsOK := mulUint64V1(uint64(partitions), uint64(topK))
	if resultsOK {
		results, resultsOK = mulUint64V1(results, uint64(maxStableIDBytes+16))
	}
	if resultsOK {
		partialBytes, resultsOK = addUint64V1(partialBytes, results)
	}
	if !ok || !resultsOK {
		return 0, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	return partialBytes, nil
}

type vectorPartitionCoordinatorTaskResultV1 struct {
	response                           VectorPartitionShardSearchResponseV1
	rpcs, retries, redirects           uint64
	maxRequestBytes                    uint64
	queueNanos, rpcNanos, networkNanos uint64
}

func (c *VectorPartitionCoordinatorV1) dispatch(ctx context.Context, tasks []vectorPartitionCoordinatorTaskV1) ([]vectorPartitionCoordinatorTaskResultV1, error) {
	if len(tasks) < 1 || len(tasks) > c.limits.MaxRequests {
		return nil, c.wrapError(ErrVectorPartitionCoordinatorBudgetExceeded, "")
	}
	queuedAt := time.Now()
	for i := range tasks {
		tasks[i].queuedAt = queuedAt
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int, len(tasks))
	for i := range tasks {
		jobs <- i
	}
	close(jobs)
	results := make([]vectorPartitionCoordinatorTaskResultV1, len(tasks))
	var firstErr error
	var errorOnce sync.Once
	workers := min(len(tasks), c.limits.MaxConcurrentRequests)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for taskIndex := range jobs {
				if err := child.Err(); err != nil {
					continue
				}
				result, err := c.dispatchTask(child, tasks[taskIndex])
				results[taskIndex] = result
				if err != nil {
					errorOnce.Do(func() {
						firstErr = err
						cancel()
					})
					continue
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return results, firstErr
	}
	if err := ctx.Err(); err != nil {
		return results, c.wrapError(err, "")
	}
	for i := range results {
		if results[i].response.Version == 0 {
			return results, c.wrapError(ErrVectorPartitionCoordinatorUnavailable, tasks[i].group.ID)
		}
	}
	return results, nil
}

func (c *VectorPartitionCoordinatorV1) dispatchTask(ctx context.Context, task vectorPartitionCoordinatorTaskV1) (vectorPartitionCoordinatorTaskResultV1, error) {
	result := vectorPartitionCoordinatorTaskResultV1{queueNanos: elapsedNanosV1(task.queuedAt)}
	request := task.request
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return result, c.wrapError(err, task.group.ID)
		}
		requestBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(request)
		if err != nil {
			return result, c.wrapError(err, task.group.ID)
		}
		if requestBytes > request.RequestBytesLimit {
			return result, c.wrapError(ErrVectorPartitionCoordinatorBudgetExceeded, task.group.ID)
		}
		result.maxRequestBytes = max(result.maxRequestBytes, requestBytes)
		started := time.Now()
		response, err := c.dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request)
		elapsed := elapsedNanosV1(started)
		result.rpcs++
		result.rpcNanos += elapsed
		if err == nil {
			if err := c.validateShardResponse(ctx, task, request, response); err != nil {
				return result, c.wrapError(err, task.group.ID)
			}
			if elapsed > response.Timing.TotalNanos {
				result.networkNanos += elapsed - response.Timing.TotalNanos
			}
			result.response = response
			return result, nil
		}
		var shardErr *VectorPartitionShardSearchErrorV1
		if !errors.As(err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorNotLeaderV1 ||
			attempt >= c.limits.MaxRetries {
			return result, c.wrapError(err, task.group.ID)
		}
		if result.redirects >= uint64(c.limits.MaxRedirects) ||
			shardErr.LeaderHint == "" || !slices.Contains(task.group.Members, shardErr.LeaderHint) {
			return result, c.wrapError(err, task.group.ID)
		}
		result.retries++
		if request.TargetNodeID != shardErr.LeaderHint {
			result.redirects++
		}
		request.TargetNodeID = shardErr.LeaderHint
	}
}

func (c *VectorPartitionCoordinatorV1) validateShardResponse(ctx context.Context, task vectorPartitionCoordinatorTaskV1, request VectorPartitionShardSearchRequestV1, response VectorPartitionShardSearchResponseV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	proof := response.Proof
	if response.Version != VectorPartitionShardSearchVersionV1 || response.RequestID != request.RequestID ||
		response.Partitions != uint64(len(task.partitionIDs)) || len(response.Partials) != len(task.partitionIDs) ||
		len(task.candidateRows) != len(task.partitionIDs) ||
		proof.GroupID != task.group.ID ||
		proof.SourceGeneration != request.SourceGeneration || proof.SourceChecksum != request.SourceChecksum ||
		proof.SourceSchemaHash != request.SourceSchemaHash || proof.SourceRowCount != request.SourceRowCount ||
		proof.PartitionGeneration != request.PartitionGeneration || proof.RouterGeneration != request.RouterGeneration ||
		proof.ReadySetDigest != request.ReadySetDigest ||
		proof.ServingNode == "" ||
		!slices.Contains(task.group.Members, proof.ServingNode) ||
		request.TargetNodeID != "" && proof.ServingNode != request.TargetNodeID {
		return ErrVectorPartitionCoordinatorMalformedResponse
	}
	if request.StrictCapability != nil {
		capability := request.StrictCapability
		if proof.Kind != vectorPartitionShardSearchProofStrictSnapshotV1 || proof.LeaderNode != "" ||
			proof.ReadTerm != 0 || proof.ReadIndex != 0 || proof.AppliedTerm != 0 || proof.AppliedIndex != 0 ||
			proof.ServingIdentityDigest != capability.ServingIdentityDigest ||
			proof.CatalogAppliedIndex != capability.CatalogAppliedIndex || proof.GroupAppliedIndex != capability.GroupAppliedIndex {
			return ErrVectorPartitionCoordinatorMalformedResponse
		}
	} else {
		// M5 may prove a current-term read while the latest applied TreeDB
		// command remains in an older term across a command-free gap.
		if proof.Kind != "" && proof.Kind != vectorPartitionShardSearchProofReadIndexV1 ||
			proof.ReadTerm == 0 || proof.ReadIndex == 0 || proof.AppliedTerm == 0 || proof.AppliedIndex < proof.ReadIndex ||
			proof.LeaderNode == "" || proof.LeaderNode != proof.ServingNode || !slices.Contains(task.group.Members, proof.LeaderNode) {
			return ErrVectorPartitionCoordinatorMalformedResponse
		}
	}
	var timingSubtotal uint64
	for _, component := range [...]uint64{
		response.Timing.RouteOwnerNanos,
		response.Timing.ReadIndexApplyNanos,
		response.Timing.GenerationOpenNanos,
		response.Timing.SearchNanos,
		response.Timing.ResponseCopyNanos,
	} {
		var ok bool
		timingSubtotal, ok = addUint64V1(timingSubtotal, component)
		if !ok || timingSubtotal > response.Timing.TotalNanos {
			return ErrVectorPartitionCoordinatorMalformedResponse
		}
	}
	var candidates, edges uint64
	for i, partial := range response.Partials {
		if err := ctx.Err(); err != nil {
			return err
		}
		if partial.PartitionID != task.partitionIDs[i] ||
			partial.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 &&
				partial.SearchRoute != collections.VectorPartitionSearchRouteExactFP32ScanV1 {
			return ErrVectorPartitionCoordinatorMalformedResponse
		}
		// Exact scan visits every manifest membership. Bind its candidate count
		// to the coordinator's pinned manifest rather than trusting a shard to
		// lower both Candidates and the required neighbor count consistently.
		if partial.SearchRoute == collections.VectorPartitionSearchRouteExactFP32ScanV1 &&
			partial.Candidates != task.candidateRows[i] {
			return ErrVectorPartitionCoordinatorMalformedResponse
		}
		expectedNeighbors := uint64(request.TopK)
		if partial.Candidates < expectedNeighbors {
			expectedNeighbors = partial.Candidates
		}
		if uint64(len(partial.Neighbors)) != expectedNeighbors {
			return ErrVectorPartitionCoordinatorMalformedResponse
		}
		candidatesNext, ok := addUint64V1(candidates, partial.Candidates)
		if !ok {
			return ErrVectorPartitionCoordinatorBudgetExceeded
		}
		candidates = candidatesNext
		edgesNext, ok := addUint64V1(edges, partial.Edges)
		if !ok {
			return ErrVectorPartitionCoordinatorBudgetExceeded
		}
		edges = edgesNext
		seen := make(map[string]struct{}, len(partial.Neighbors))
		for neighborIndex, neighbor := range partial.Neighbors {
			if neighbor.ID == "" || len(neighbor.ID) > c.limits.MaxStableIDBytes ||
				math.IsNaN(float64(neighbor.Score)) || math.IsInf(float64(neighbor.Score), 0) {
				return ErrVectorPartitionCoordinatorMalformedResponse
			}
			if _, duplicate := seen[neighbor.ID]; duplicate {
				return ErrVectorPartitionCoordinatorMalformedResponse
			}
			seen[neighbor.ID] = struct{}{}
			if neighborIndex > 0 {
				previous := partial.Neighbors[neighborIndex-1]
				if neighbor.Score > previous.Score ||
					neighbor.Score == previous.Score && neighbor.ID < previous.ID {
					return ErrVectorPartitionCoordinatorMalformedResponse
				}
			}
		}
	}
	responseBytes, err := MeasureVectorPartitionShardSearchResponseBytesV1(response.Partials)
	if err != nil {
		return ErrVectorPartitionCoordinatorBudgetExceeded
	}
	if response.ResponseBytes != responseBytes ||
		response.Candidates != candidates || response.Edges != edges {
		return ErrVectorPartitionCoordinatorMalformedResponse
	}
	candidateBytes, ok := mulUint64V1(response.Candidates, 64)
	if !ok || candidateBytes > request.CandidateBytesLimit ||
		responseBytes > request.ResponseBytesLimit {
		return ErrVectorPartitionCoordinatorBudgetExceeded
	}
	return nil
}

func (c *VectorPartitionCoordinatorV1) dedupe(ctx context.Context, request VectorPartitionCoordinatorRequestV1, results []vectorPartitionCoordinatorTaskResultV1) (map[string]float32, uint64, uint64, error) {
	capacity := request.PartitionProbes * request.TopK
	if capacity < 0 || capacity > request.MergeEntriesLimit || capacity > c.limits.MaxMergeEntries {
		return nil, 0, 0, ErrVectorPartitionCoordinatorBudgetExceeded
	}
	unique := make(map[string]float32, capacity)
	var duplicates, disagreements uint64
	for _, result := range results {
		for _, partial := range result.response.Partials {
			if err := ctx.Err(); err != nil {
				return nil, 0, 0, err
			}
			for _, neighbor := range partial.Neighbors {
				current, exists := unique[neighbor.ID]
				if !exists {
					if len(unique) >= request.MergeEntriesLimit || len(unique) >= c.limits.MaxMergeEntries {
						return nil, 0, 0, ErrVectorPartitionCoordinatorBudgetExceeded
					}
					unique[neighbor.ID] = neighbor.Score
					continue
				}
				duplicates++
				if math.Float32bits(current) != math.Float32bits(neighbor.Score) {
					disagreements++
				}
				if neighbor.Score > current {
					unique[neighbor.ID] = neighbor.Score
				}
			}
		}
	}
	return unique, duplicates, disagreements, nil
}

type vectorPartitionCoordinatorHeapV1 []VectorPartitionCoordinatorNeighborV1

func (h vectorPartitionCoordinatorHeapV1) Len() int { return len(h) }
func (h vectorPartitionCoordinatorHeapV1) Less(i, j int) bool {
	return vectorPartitionCoordinatorNeighborWorseV1(h[i], h[j])
}
func (h vectorPartitionCoordinatorHeapV1) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *vectorPartitionCoordinatorHeapV1) Push(value any) {
	*h = append(*h, value.(VectorPartitionCoordinatorNeighborV1))
}
func (h *vectorPartitionCoordinatorHeapV1) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func vectorPartitionCoordinatorNeighborBetterV1(left, right VectorPartitionCoordinatorNeighborV1) bool {
	return left.Score > right.Score || left.Score == right.Score && left.ID < right.ID
}

func vectorPartitionCoordinatorNeighborWorseV1(left, right VectorPartitionCoordinatorNeighborV1) bool {
	return left.Score < right.Score || left.Score == right.Score && left.ID > right.ID
}

func topVectorPartitionCoordinatorNeighborsV1(ctx context.Context, unique map[string]float32, topK int) ([]VectorPartitionCoordinatorNeighborV1, error) {
	if topK < 1 {
		return nil, ErrVectorPartitionCoordinatorInvalidRequest
	}
	h := make(vectorPartitionCoordinatorHeapV1, 0, min(topK, len(unique)))
	heap.Init(&h)
	seen := 0
	for id, score := range unique {
		if seen&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		seen++
		candidate := VectorPartitionCoordinatorNeighborV1{ID: strings.Clone(id), Score: score}
		if len(h) < topK {
			heap.Push(&h, candidate)
		} else if vectorPartitionCoordinatorNeighborBetterV1(candidate, h[0]) {
			h[0] = candidate
			heap.Fix(&h, 0)
		}
	}
	out := make([]VectorPartitionCoordinatorNeighborV1, len(h))
	copy(out, h)
	sort.Slice(out, func(i, j int) bool {
		return vectorPartitionCoordinatorNeighborBetterV1(out[i], out[j])
	})
	return out, ctx.Err()
}

func (c *VectorPartitionCoordinatorV1) wrapError(err error, groupID raftcluster.GroupID) error {
	if err == nil {
		return nil
	}
	var existing *VectorPartitionCoordinatorErrorV1
	if errors.As(err, &existing) {
		return err
	}
	return &VectorPartitionCoordinatorErrorV1{
		Code:    classifyVectorPartitionCoordinatorErrorV1(err),
		GroupID: groupID,
		Err:     err,
	}
}

func classifyVectorPartitionCoordinatorErrorV1(err error) VectorPartitionCoordinatorErrorCodeV1 {
	var coordinatorErr *VectorPartitionCoordinatorErrorV1
	if errors.As(err, &coordinatorErr) {
		return coordinatorErr.Code
	}
	var shardErr *VectorPartitionShardSearchErrorV1
	if errors.As(err, &shardErr) {
		switch shardErr.Code {
		case VectorPartitionShardSearchErrorDeadlineV1:
			return VectorPartitionCoordinatorErrorDeadlineV1
		case VectorPartitionShardSearchErrorCanceledV1:
			return VectorPartitionCoordinatorErrorCanceledV1
		case VectorPartitionShardSearchErrorNotLeaderV1:
			return VectorPartitionCoordinatorErrorNotLeaderV1
		case VectorPartitionShardSearchErrorGenerationMismatchV1:
			return VectorPartitionCoordinatorErrorGenerationMismatchV1
		case VectorPartitionShardSearchErrorMissingOwnerV1,
			VectorPartitionShardSearchErrorUnknownOwnerV1,
			VectorPartitionShardSearchErrorRemoteOwnerV1,
			VectorPartitionShardSearchErrorRouteMismatchV1:
			return VectorPartitionCoordinatorErrorRouteMismatchV1
		case VectorPartitionShardSearchErrorResponseTooLargeV1:
			return VectorPartitionCoordinatorErrorBudgetExceededV1
		case VectorPartitionShardSearchErrorInvalidRequestV1,
			VectorPartitionShardSearchErrorUnsupportedConsistencyV1:
			return VectorPartitionCoordinatorErrorMalformedResponseV1
		default:
			return VectorPartitionCoordinatorErrorUnavailableV1
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return VectorPartitionCoordinatorErrorDeadlineV1
	case errors.Is(err, context.Canceled):
		return VectorPartitionCoordinatorErrorCanceledV1
	case errors.Is(err, ErrVectorPartitionCoordinatorGenerationMismatch),
		errors.Is(err, collections.ErrVectorPartitionManifestInvalid):
		return VectorPartitionCoordinatorErrorGenerationMismatchV1
	case errors.Is(err, ErrVectorPartitionCoordinatorRouteMismatch):
		return VectorPartitionCoordinatorErrorRouteMismatchV1
	case errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded):
		return VectorPartitionCoordinatorErrorBudgetExceededV1
	case errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse):
		return VectorPartitionCoordinatorErrorMalformedResponseV1
	case errors.Is(err, ErrVectorPartitionCoordinatorInvalidRequest):
		return VectorPartitionCoordinatorErrorInvalidRequestV1
	default:
		return VectorPartitionCoordinatorErrorUnavailableV1
	}
}

func (a *vectorPartitionCoordinatorStatsAccumulatorV1) begin() {
	a.mu.Lock()
	a.value.Requests++
	a.mu.Unlock()
}

func (a *vectorPartitionCoordinatorStatsAccumulatorV1) fail(code VectorPartitionCoordinatorErrorCodeV1, total uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value.Errors++
	a.value.TotalNanos += total
	switch code {
	case VectorPartitionCoordinatorErrorCanceledV1:
		a.value.Canceled++
	case VectorPartitionCoordinatorErrorDeadlineV1:
		a.value.TimedOut++
	}
}

func (a *vectorPartitionCoordinatorStatsAccumulatorV1) succeed(response VectorPartitionCoordinatorResponseV1, total uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value.Successes++
	a.value.SelectedPartitions += response.Counters.SelectedPartitions
	a.value.SelectedGroups += response.Counters.SelectedGroups
	a.value.RPCs += response.Counters.RPCs
	a.value.Retries += response.Counters.Retries
	a.value.Redirects += response.Counters.Redirects
	a.value.Duplicates += response.Counters.Duplicates
	a.value.Disagreements += response.Counters.ScoreDisagreements
	a.value.TotalNanos += total
}

// Stats returns a concurrency-safe cumulative coordinator status snapshot.
func (c *VectorPartitionCoordinatorV1) Stats() VectorPartitionCoordinatorStatsV1 {
	if c == nil {
		return VectorPartitionCoordinatorStatsV1{}
	}
	c.stats.mu.Lock()
	stats := c.stats.value
	c.stats.mu.Unlock()
	c.sessionMu.Lock()
	sessions := make([]VectorPartitionCoordinatorRouterSessionStatsV1, 0, len(c.sessionStats))
	for _, session := range c.sessionStats {
		sessions = append(sessions, session.value)
	}
	c.sessionMu.Unlock()
	sort.Slice(sessions, func(i, j int) bool {
		left, right := sessions[i].Identity, sessions[j].Identity
		if left.Database != right.Database {
			return left.Database < right.Database
		}
		if left.Catalog != right.Catalog {
			return left.Catalog < right.Catalog
		}
		if left.Collection != right.Collection {
			return left.Collection < right.Collection
		}
		if left.IndexName != right.IndexName {
			return left.IndexName < right.IndexName
		}
		if left.IndexDefinitionDigest != right.IndexDefinitionDigest {
			return left.IndexDefinitionDigest < right.IndexDefinitionDigest
		}
		if left.SourceGeneration != right.SourceGeneration {
			return left.SourceGeneration < right.SourceGeneration
		}
		if left.SourceChecksum != right.SourceChecksum {
			return left.SourceChecksum < right.SourceChecksum
		}
		if left.SourceSchemaHash != right.SourceSchemaHash {
			return left.SourceSchemaHash < right.SourceSchemaHash
		}
		if left.SourceRowCount != right.SourceRowCount {
			return left.SourceRowCount < right.SourceRowCount
		}
		if left.PartitionGeneration != right.PartitionGeneration {
			return left.PartitionGeneration < right.PartitionGeneration
		}
		if left.ReadySetDigest != right.ReadySetDigest {
			return left.ReadySetDigest < right.ReadySetDigest
		}
		if left.RouterModelDigest != right.RouterModelDigest {
			return left.RouterModelDigest < right.RouterModelDigest
		}
		return false
	})
	stats.RouterSessions = sessions
	return stats
}
