package benchsupport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

// ProductionRouteProofOptions scopes the internal production route scaffold.
// Public token/ring mutations remain fail closed until authoritative collection
// and index metadata is bound to the exact owner route proof.
type ProductionRouteProofOptions struct {
	GroupCount           int
	PartitionCount       int
	RemoteOwnerExecution bool
}

// ProductionRouteProofSnapshot is a legacy internal-scaffold counter snapshot.
// It is not evidence that the current public token/ring path can route writes.
type ProductionRouteProofSnapshot struct {
	RealRoutedCommits        bool
	RouteAttemptsTotal       int64
	RouteLocalOwnerHits      int64
	RouteRemoteRedirects     int64
	RouteRemoteForwards      int64
	RouteUnknownOwnerRejects int64
	RouteGroupHits           map[string]int
	RouteLeaderHits          map[string]int
	RouteTokenPartitionHits  map[string]int
	CommitGroupHits          map[string]int
	AppliedGroupHits         map[string]int
	FanoutSplitAttempts      int64
	FanoutSplitFailures      int64
	DirectLocalBypassRejects int64
}

// ProductionRouteProofHarness owns the internal route-topology scaffold and
// counters used by mongo_gateway_bench fail-closed policy checks.
type ProductionRouteProofHarness struct {
	fsm        *raftfsm.FSM
	fsms       []*raftfsm.FSM
	recorder   *productionRouteProofRecorder
	dispatcher *raftcluster.GroupRoutedSubmitter
}

// ConfigureProductionRouteProofHarness wires server.ClusterSubmitter to an
// internal routed dispatcher with a local group and, when opted in, registered
// in-process remote groups. Public token/ring mutations reject before this
// scaffold can provide enabled-path evidence; it is not a multi-node scaleout
// benchmark.
func ConfigureProductionRouteProofHarness(opts ProductionRouteProofOptions, db *backenddb.DB, manager *collections.CollectionManager, server *mongogateway.Server) (*ProductionRouteProofHarness, error) {
	if server == nil {
		return nil, errors.New("internal production route scaffold requires Mongo gateway server")
	}
	if opts.GroupCount < 1 {
		opts.GroupCount = 1
	}
	cluster := productionRouteProofClusterConfig(db, 0)
	forwardGroups := make([]string, 0, max(0, opts.GroupCount-1))
	if opts.RemoteOwnerExecution {
		for group := 1; group < opts.GroupCount; group++ {
			forwardGroups = append(forwardGroups, productionRouteProofGroupID(group))
		}
	}
	recorder := newProductionRouteProofRecorderWithForwardGroups(opts.GroupCount, opts.PartitionCount, string(cluster.GroupID), forwardGroups...)
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:      db,
		Cluster: cluster,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		return nil, err
	}
	catalogVersion := newProductionRouteProofCatalogVersion(db)
	entries := make([]raftcluster.GroupSubmitterV1, 0, max(1, opts.GroupCount))
	var openedFSMs []*raftfsm.FSM
	applier := &productionRouteProofApplier{
		inner:          fsm,
		recorder:       recorder,
		catalogVersion: catalogVersion,
	}
	bridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
		Cluster:                cluster,
		AdmissionProvider:      raftcluster.StaticAdmissionProvider{Status: raftcluster.LeaderAdmission()},
		CommitSource:           newProductionRouteProofCommitSource(cluster, recorder),
		Preflight:              fsm,
		Applier:                applier,
		CatalogVersionProvider: catalogVersion,
	})
	if err != nil {
		_ = fsm.Close()
		return nil, err
	}
	openedFSMs = append(openedFSMs, fsm)
	entries = append(entries, raftcluster.GroupSubmitterV1{GroupID: cluster.GroupID, Submitter: bridge})
	if opts.RemoteOwnerExecution {
		for group := 1; group < opts.GroupCount; group++ {
			remoteCluster := productionRouteProofClusterConfig(db, group)
			boundary := newProductionRouteProofMemoryApplyBoundary(db, remoteCluster, recorder, catalogVersion)
			remoteBridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
				Cluster:                remoteCluster,
				AdmissionProvider:      raftcluster.StaticAdmissionProvider{Status: raftcluster.LeaderAdmission()},
				CommitSource:           newProductionRouteProofCommitSource(remoteCluster, recorder),
				Preflight:              boundary,
				Applier:                boundary,
				CatalogVersionProvider: catalogVersion,
			})
			if err != nil {
				_ = closeProductionRouteProofFSMs(openedFSMs)
				return nil, err
			}
			entries = append(entries, raftcluster.GroupSubmitterV1{GroupID: remoteCluster.GroupID, Submitter: remoteBridge})
		}
	}
	registry, err := raftcluster.NewGroupSubmitterRegistryV1(entries)
	if err != nil {
		_ = closeProductionRouteProofFSMs(openedFSMs)
		return nil, err
	}
	// This legacy benchmark scaffold has no replicated catalog authority.
	// Keep the production composition fail closed: it can exercise route
	// topology counters, but it cannot admit a routed mutation from a
	// constructor-local catalog or synthetic proof.
	dispatcher, err := raftcluster.NewCatalogMetaGroupRoutedSubmitter(registry, productionRouteProofFailClosedCatalogValidator{})
	if err != nil {
		_ = closeProductionRouteProofFSMs(openedFSMs)
		return nil, err
	}
	server.ClusterSubmitter = nativewire.NewRoutedRaftClusterSubmitter(dispatcher, recorder, manager)
	server.ClusterCatalogVersion = catalogVersion.ServerCatalogVersion
	return &ProductionRouteProofHarness{
		fsm:        fsm,
		fsms:       openedFSMs,
		recorder:   recorder,
		dispatcher: dispatcher,
	}, nil
}

type productionRouteProofFailClosedCatalogValidator struct{}

func (productionRouteProofFailClosedCatalogValidator) ValidateCatalogRouteMetadata(context.Context, raftentry.RequestMetadataV1) error {
	return raftplacement.ErrCatalogMetaUnavailable
}

func (h *ProductionRouteProofHarness) Close() error {
	if h == nil {
		return nil
	}
	if len(h.fsms) != 0 {
		return closeProductionRouteProofFSMs(h.fsms)
	}
	if h.fsm == nil {
		return nil
	}
	return h.fsm.Close()
}

func (h *ProductionRouteProofHarness) Snapshot() ProductionRouteProofSnapshot {
	if h == nil || h.recorder == nil {
		return ProductionRouteProofSnapshot{}
	}
	return h.recorder.snapshot()
}

func (h *ProductionRouteProofHarness) ResetCounters() {
	if h == nil || h.recorder == nil {
		return
	}
	h.recorder.resetCounters()
}

func LocalProductionRouteProofGroupID() string {
	return productionRouteProofGroupID(0)
}

func ProductionRouteProofGroupIDForDocumentID(groupCount, partitionCount int, documentID []byte) string {
	recorder := newProductionRouteProofRecorder(groupCount, partitionCount)
	token := raftplacement.DocumentIDTokenV1(documentID)
	partition := recorder.partitionForToken(token)
	return productionRouteProofGroupID(recorder.groupForPartition(partition))
}

func (h *ProductionRouteProofHarness) ProbeUnknownOwnerReject(ctx context.Context, database, collection string) error {
	if h == nil || h.dispatcher == nil || h.recorder == nil {
		return errors.New("internal production route scaffold dispatcher is not configured")
	}
	target := h.recorder.unknownOwnerProbeTarget()
	unknown := nativewire.ClusterRequestMetadata{
		AckPolicy:                 nativewire.AckVisible,
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      database,
		ClusterRouteCatalog:       "default",
		ClusterRouteCollection:    collection,
		ClusterRouteShape:         string(nativewire.ClusterRouteShapeToken),
		ClusterRouteGroupID:       target.GroupID,
		ClusterRouteMembers:       target.Members,
		ClusterRouteLeaderHint:    target.LeaderHint,
		ClusterRoutePlacementMode: "ring",
		ClusterRouteKey:           "_id",
		ClusterRouteTokenKnown:    true,
		ClusterRouteToken:         0,
		ClusterRoutePartitionID:   "token-999999",
	}
	if _, err := h.dispatcher.SubmitCommandEntryV1(ctx, nil, unknown); !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
		return fmt.Errorf("production route catalog-authority guardrail err=%v want %v", err, raftplacement.ErrCatalogMetaUnavailable)
	}
	h.recorder.recordUnknownOwnerReject()
	return nil
}

func (h *ProductionRouteProofHarness) ProbeDirectLocalBypassReject(ctx context.Context) error {
	if h == nil || h.dispatcher == nil {
		return errors.New("internal production route scaffold dispatcher is not configured")
	}
	direct := nativewire.ClusterRequestMetadata{AckPolicy: nativewire.AckVisible}
	if _, err := h.dispatcher.SubmitCommandEntryV1(ctx, nil, direct); !errors.Is(err, raftcluster.ErrRouteTargetMissing) {
		return fmt.Errorf("production route direct-local-bypass guardrail err=%v want %v", err, raftcluster.ErrRouteTargetMissing)
	}
	h.recorder.recordDirectLocalBypassReject()
	return nil
}

func productionRouteProofClusterConfig(db *backenddb.DB, group int) raftcluster.Config {
	dir := ""
	if db != nil {
		dir = db.Dir()
	}
	groupID := productionRouteProofGroupID(group)
	basePort := 9701 + group*10
	return raftcluster.Config{
		Dir:     dir,
		NodeID:  raftcluster.NodeID(fmt.Sprintf("node-%02d-a", group)),
		GroupID: raftcluster.GroupID(groupID),
		Peers: []raftcluster.Peer{
			{ID: raftcluster.NodeID(fmt.Sprintf("node-%02d-a", group)), Address: fmt.Sprintf("127.0.0.1:%d", basePort)},
			{ID: raftcluster.NodeID(fmt.Sprintf("node-%02d-b", group)), Address: fmt.Sprintf("127.0.0.1:%d", basePort+1)},
			{ID: raftcluster.NodeID(fmt.Sprintf("node-%02d-c", group)), Address: fmt.Sprintf("127.0.0.1:%d", basePort+2)},
		},
	}
}

func closeProductionRouteProofFSMs(fsms []*raftfsm.FSM) error {
	var errs []error
	for _, fsm := range fsms {
		if fsm != nil {
			errs = append(errs, fsm.Close())
		}
	}
	return errors.Join(errs...)
}

func newProductionRouteProofCommitSource(cluster raftcluster.Config, recorder *productionRouteProofRecorder) *productionRouteProofCommitSource {
	return &productionRouteProofCommitSource{
		inner: raftcluster.NewSequencedCommitSource(raftcluster.SequencedCommitSourceOptions{
			GroupID:             cluster.GroupID,
			NodeID:              cluster.NodeID,
			LeaderID:            cluster.NodeID,
			EvidenceKind:        raftcluster.CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		recorder: recorder,
	}
}

type productionRouteProofCommitSource struct {
	inner    raftcluster.CommitSource
	recorder *productionRouteProofRecorder
}

func (s *productionRouteProofCommitSource) CommitCommandEntryV1(ctx context.Context, req raftcluster.CommitCommandEntryV1Request) (raftcluster.CommitCommandEntryV1Result, error) {
	if s == nil || s.inner == nil {
		return raftcluster.CommitCommandEntryV1Result{}, raftcluster.ErrInvalidSubmitter
	}
	result, err := s.inner.CommitCommandEntryV1(ctx, req)
	if err != nil {
		return result, err
	}
	groupID := string(result.Evidence.GroupID)
	if groupID == "" {
		groupID = string(req.GroupID)
	}
	if s.recorder != nil {
		s.recorder.recordCommitGroup(groupID)
	}
	return result, nil
}

type productionRouteProofApplier struct {
	inner          raftcluster.CommittedCommandApplierV1
	recorder       *productionRouteProofRecorder
	catalogVersion *productionRouteProofCatalogVersion
}

func (a *productionRouteProofApplier) ApplyCommittedCommandEntryV1(ctx context.Context, entry raftcluster.CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	if a == nil || a.inner == nil {
		return raftentry.ApplyResultV1{}, raftcluster.ErrInvalidSubmitter
	}
	result, err := a.inner.ApplyCommittedCommandEntryV1(ctx, entry)
	if err != nil {
		return result, err
	}
	if result.Status == raftentry.ApplyStatusApplied || result.Status == raftentry.ApplyStatusAlreadyApplied {
		groupID := entry.RequestMetadata.ClusterRouteGroupID
		if groupID == "" {
			groupID = productionRouteProofGroupID(0)
		}
		if a.recorder != nil {
			a.recorder.recordAppliedGroup(groupID)
		}
		if a.catalogVersion != nil {
			a.catalogVersion.Refresh()
		}
	}
	return result, nil
}

func (a *productionRouteProofApplier) AllowsInitialIndexGapV1() bool {
	if a == nil || a.inner == nil {
		return false
	}
	support, ok := a.inner.(raftcluster.InitialIndexGapSupportV1)
	return ok && support.AllowsInitialIndexGapV1()
}

type productionRouteProofMemoryApplyBoundary struct {
	db             *backenddb.DB
	cluster        raftcluster.Config
	recorder       *productionRouteProofRecorder
	catalogVersion *productionRouteProofCatalogVersion
	progress       *raftapply.MemoryApplyProgressStore
	results        *raftapply.MemoryApplyResultStore
}

func newProductionRouteProofMemoryApplyBoundary(db *backenddb.DB, cluster raftcluster.Config, recorder *productionRouteProofRecorder, catalogVersion *productionRouteProofCatalogVersion) *productionRouteProofMemoryApplyBoundary {
	return &productionRouteProofMemoryApplyBoundary{
		db:             db,
		cluster:        cluster,
		recorder:       recorder,
		catalogVersion: catalogVersion,
		progress:       raftapply.NewMemoryApplyProgressStore(raftentry.MaxProgressRecordsV1, 0),
		results:        raftapply.NewMemoryApplyResultStore(raftentry.MaxProgressRecordsV1),
	}
}

func (b *productionRouteProofMemoryApplyBoundary) PreflightCommandEntryV1(ctx context.Context, req raftcluster.CommandEntryPreflightRequestV1) (raftcluster.CommandEntryPreflightResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return raftcluster.CommandEntryPreflightResultV1{}, ctx.Err()
	default:
	}
	if b == nil || b.db == nil {
		return raftcluster.CommandEntryPreflightResultV1{}, raftcluster.ErrInvalidSubmitter
	}
	if req.GroupID != "" && req.GroupID != b.cluster.GroupID {
		return raftcluster.CommandEntryPreflightResultV1{}, fmt.Errorf("internal production route scaffold preflight group %q does not match local group %q", req.GroupID, b.cluster.GroupID)
	}
	if req.NodeID != "" && req.NodeID != b.cluster.NodeID {
		return raftcluster.CommandEntryPreflightResultV1{}, fmt.Errorf("internal production route scaffold preflight node %q does not match local node %q", req.NodeID, b.cluster.NodeID)
	}
	if len(req.EntryBytes) == 0 {
		return raftcluster.CommandEntryPreflightResultV1{}, errors.New("internal production route scaffold preflight empty command entry")
	}
	meta := productionRouteProofApplyMetadata(req.CurrentCatalogVersion, req.HasCurrentCatalogVersion, req.SyncLocalCommandWAL, req.RequestMetadata, req.ExpectedTarget)
	opts := raftapply.Options{ResultStore: b.results}
	var result raftapply.PreflightResultV1
	var err error
	if len(req.DecodedEntry.Bytes) != 0 {
		if !bytes.Equal(req.DecodedEntry.Bytes, req.EntryBytes) {
			return raftcluster.CommandEntryPreflightResultV1{}, errors.New("internal production route scaffold preflight decoded command entry does not match raw entry bytes")
		}
		result, err = raftapply.PreflightDecodedCommandEntryV1(b.db, req.DecodedEntry, meta, opts)
	} else {
		result, err = raftapply.PreflightCommandEntryV1(b.db, req.EntryBytes, meta, opts)
	}
	if err != nil {
		return raftcluster.CommandEntryPreflightResultV1{}, err
	}
	return raftcluster.CommandEntryPreflightResultV1{KnownIdempotencyReplay: result.KnownIdempotencyReplay}, nil
}

func (b *productionRouteProofMemoryApplyBoundary) ApplyCommittedCommandEntryV1(ctx context.Context, entry raftcluster.CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return raftentry.ApplyResultV1{}, ctx.Err()
	default:
	}
	if b == nil || b.db == nil {
		return raftentry.ApplyResultV1{}, raftcluster.ErrInvalidSubmitter
	}
	if entry.RequestMetadata.ClusterRouteGroupID != "" && entry.RequestMetadata.ClusterRouteGroupID != string(b.cluster.GroupID) {
		return raftentry.ApplyResultV1{}, fmt.Errorf("internal production route scaffold apply route group %q does not match local group %q", entry.RequestMetadata.ClusterRouteGroupID, b.cluster.GroupID)
	}
	meta := productionRouteProofApplyMetadata(entry.CurrentCatalogVersion, entry.HasCurrentCatalogVersion, entry.SyncLocalCommandWAL, entry.RequestMetadata, entry.ExpectedTarget)
	meta.EntryID = raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
	result, err := raftapply.ApplyCommittedEntryV1(b.db, entry.Bytes, meta, raftapply.Options{
		ProgressStore: b.progress,
		ResultStore:   b.results,
	})
	if err != nil {
		return result, err
	}
	if result.Status == raftentry.ApplyStatusApplied || result.Status == raftentry.ApplyStatusAlreadyApplied {
		groupID := entry.RequestMetadata.ClusterRouteGroupID
		if groupID == "" {
			groupID = string(b.cluster.GroupID)
		}
		if b.recorder != nil {
			b.recorder.recordAppliedGroup(groupID)
		}
		if b.catalogVersion != nil {
			b.catalogVersion.Refresh()
		}
	}
	return result, nil
}

func productionRouteProofApplyMetadata(catalogVersion uint64, hasCatalogVersion, syncLocalWAL bool, metadata raftentry.RequestMetadataV1, expectedTarget *raftentry.TargetIdentityV1) raftapply.ApplyMetadataV1 {
	return raftapply.ApplyMetadataV1{
		LocalDurabilityBoundary:  raftapply.LocalDurabilityCommandWALV1,
		SyncLocalCommandWAL:      syncLocalWAL,
		CurrentCatalogVersion:    catalogVersion,
		HasCurrentCatalogVersion: hasCatalogVersion,
		RequestMetadata:          metadata,
		ExpectedTarget:           expectedTarget,
	}
}

type productionRouteProofCatalogVersion struct {
	db    *backenddb.DB
	known atomic.Bool
	value atomic.Uint64
}

func newProductionRouteProofCatalogVersion(db *backenddb.DB) *productionRouteProofCatalogVersion {
	p := &productionRouteProofCatalogVersion{db: db}
	p.Refresh()
	return p
}

func (p *productionRouteProofCatalogVersion) CurrentCatalogVersion(ctx context.Context) (uint64, bool, error) {
	if p == nil {
		return 0, false, nil
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		default:
		}
	}
	p.Refresh()
	return p.value.Load(), p.known.Load(), nil
}

func (p *productionRouteProofCatalogVersion) ServerCatalogVersion(ctx context.Context) (uint64, error) {
	version, ok, err := p.CurrentCatalogVersion(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("missing DB state")
	}
	return version, nil
}

func (p *productionRouteProofCatalogVersion) Refresh() uint64 {
	if p == nil {
		return 0
	}
	if p.db == nil {
		p.value.Store(0)
		p.known.Store(false)
		return 0
	}
	state, ok := p.db.StateToken()
	if !ok {
		p.value.Store(0)
		p.known.Store(false)
		return 0
	}
	p.value.Store(state.CommitSeq)
	p.known.Store(true)
	return state.CommitSeq
}

type productionRouteProofRecorder struct {
	groupCount     int
	partitionCount int
	localGroups    map[string]struct{}
	forwardGroups  map[string]struct{}

	mu                       sync.Mutex
	routeAttemptsTotal       int64
	routeLocalOwnerHits      int64
	routeRemoteRedirects     int64
	routeRemoteForwards      int64
	routeUnknownOwnerRejects int64
	directLocalBypassRejects int64
	fanoutSplitAttempts      int64
	fanoutSplitFailures      int64
	routeGroupHits           map[string]int
	routeLeaderHits          map[string]int
	routeTokenPartitionHits  map[string]int
	commitGroupHits          map[string]int
	appliedGroupHits         map[string]int
}

func newProductionRouteProofRecorder(groupCount, partitionCount int, localGroupIDs ...string) *productionRouteProofRecorder {
	localGroupID := productionRouteProofGroupID(0)
	if len(localGroupIDs) != 0 {
		localGroupID = localGroupIDs[0]
	}
	return newProductionRouteProofRecorderWithForwardGroups(groupCount, partitionCount, localGroupID)
}

func newProductionRouteProofRecorderWithForwardGroups(groupCount, partitionCount int, localGroupID string, forwardGroupIDs ...string) *productionRouteProofRecorder {
	if groupCount < 1 {
		groupCount = 1
	}
	if partitionCount < groupCount {
		partitionCount = groupCount
	}
	if localGroupID == "" {
		localGroupID = productionRouteProofGroupID(0)
	}
	localGroups := map[string]struct{}{localGroupID: {}}
	forwardGroups := make(map[string]struct{}, len(forwardGroupIDs))
	for _, groupID := range forwardGroupIDs {
		if groupID != "" && groupID != localGroupID {
			forwardGroups[groupID] = struct{}{}
		}
	}
	return &productionRouteProofRecorder{
		groupCount:              groupCount,
		partitionCount:          partitionCount,
		localGroups:             localGroups,
		forwardGroups:           forwardGroups,
		routeGroupHits:          make(map[string]int, groupCount),
		routeLeaderHits:         make(map[string]int, groupCount),
		routeTokenPartitionHits: make(map[string]int, partitionCount),
		commitGroupHits:         make(map[string]int, groupCount),
		appliedGroupHits:        make(map[string]int, groupCount),
	}
}

func (r *productionRouteProofRecorder) ClusterRoute(_ context.Context, request nativewire.ClusterRouteRequest) (nativewire.ClusterRouteTarget, error) {
	if r == nil {
		return nativewire.ClusterRouteTarget{}, errors.New("missing internal production route scaffold recorder")
	}
	var target nativewire.ClusterRouteTarget
	switch request.Shape {
	case nativewire.ClusterRouteShapeToken:
		if !request.TokenKnown {
			return nativewire.ClusterRouteTarget{}, errors.New("missing token")
		}
		target = r.routeToken(request.Token)
	case nativewire.ClusterRouteShapeTokenBatch:
		if len(request.Tokens) == 0 {
			return nativewire.ClusterRouteTarget{}, errors.New("missing token(s)")
		}
		target = r.routeTokenBatch(request.Tokens)
	case nativewire.ClusterRouteShapeCollection:
		target = r.groupTarget(0)
		target.PlacementMode = "collection"
		target.Shape = nativewire.ClusterRouteShapeCollection
		target.RouteKey = "_id"
	default:
		return nativewire.ClusterRouteTarget{}, fmt.Errorf("unsupported route shape %q", request.Shape)
	}
	r.recordRouteSuccess(target)
	return target, nil
}

func (r *productionRouteProofRecorder) routeToken(token uint64) nativewire.ClusterRouteTarget {
	partition := r.partitionForToken(token)
	target := r.groupTarget(r.groupForPartition(partition))
	target.PlacementMode = "ring"
	target.RouteKey = "_id"
	target.Shape = nativewire.ClusterRouteShapeToken
	target.TokenKnown = true
	target.Token = token
	target.PartitionID = productionRouteProofPartitionID(partition)
	return target
}

func (r *productionRouteProofRecorder) routeTokenBatch(tokens []uint64) nativewire.ClusterRouteTarget {
	target := nativewire.ClusterRouteTarget{
		PlacementMode: "ring",
		RouteKey:      "_id",
		Shape:         nativewire.ClusterRouteShapeTokenBatch,
	}
	if len(tokens) == 0 {
		target.TokenBatchClass = "single_token"
		return target
	}
	firstPartition := r.partitionForToken(tokens[0])
	firstGroup := r.groupForPartition(firstPartition)
	samePartition := true
	sameGroup := true
	for _, token := range tokens[1:] {
		partition := r.partitionForToken(token)
		if partition != firstPartition {
			samePartition = false
		}
		if r.groupForPartition(partition) != firstGroup {
			sameGroup = false
		}
	}
	switch {
	case samePartition:
		target.TokenBatchClass = "same_partition"
	case sameGroup:
		target.TokenBatchClass = "same_group_multi_partition"
	default:
		target.TokenBatchClass = "fanout_required"
		r.recordFanoutSplitAttempt()
		return target
	}
	groupTarget := r.groupTarget(firstGroup)
	target.GroupID = groupTarget.GroupID
	target.Members = groupTarget.Members
	target.LeaderHint = groupTarget.LeaderHint
	return target
}

func (r *productionRouteProofRecorder) partitionForToken(token uint64) int {
	if r == nil || r.partitionCount <= 1 {
		return 0
	}
	hi, _ := bits.Mul64(token, uint64(r.partitionCount))
	partition := int(hi)
	if partition >= r.partitionCount {
		return r.partitionCount - 1
	}
	return partition
}

func (r *productionRouteProofRecorder) groupForPartition(partition int) int {
	if r == nil || r.groupCount <= 1 {
		return 0
	}
	return partition % r.groupCount
}

func (r *productionRouteProofRecorder) groupTarget(group int) nativewire.ClusterRouteTarget {
	groupID := productionRouteProofGroupID(group)
	leader := fmt.Sprintf("node-%02d-a", group)
	return nativewire.ClusterRouteTarget{
		GroupID:    groupID,
		Members:    []string{leader, fmt.Sprintf("node-%02d-b", group), fmt.Sprintf("node-%02d-c", group)},
		LeaderHint: leader,
	}
}

func (r *productionRouteProofRecorder) unknownOwnerProbeTarget() nativewire.ClusterRouteTarget {
	group := 99
	if r != nil {
		group = max(group, r.groupCount)
	}
	return r.groupTarget(group)
}

func (r *productionRouteProofRecorder) recordRouteSuccess(target nativewire.ClusterRouteTarget) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routeAttemptsTotal++
	if _, ok := r.localGroups[target.GroupID]; ok {
		r.routeLocalOwnerHits++
	} else if _, ok := r.forwardGroups[target.GroupID]; ok {
		r.routeRemoteForwards++
	} else if target.GroupID != "" {
		r.routeRemoteRedirects++
	}
	if target.GroupID != "" {
		r.routeGroupHits[target.GroupID]++
	}
	if target.LeaderHint != "" {
		r.routeLeaderHits[target.LeaderHint]++
	}
	if target.PartitionID != "" {
		r.routeTokenPartitionHits[target.PartitionID]++
	}
}

func (r *productionRouteProofRecorder) recordCommitGroup(groupID string) {
	if r == nil || groupID == "" {
		return
	}
	r.mu.Lock()
	r.commitGroupHits[groupID]++
	r.mu.Unlock()
}

func (r *productionRouteProofRecorder) recordAppliedGroup(groupID string) {
	if r == nil || groupID == "" {
		return
	}
	r.mu.Lock()
	r.appliedGroupHits[groupID]++
	r.mu.Unlock()
}

func (r *productionRouteProofRecorder) recordUnknownOwnerReject() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.routeUnknownOwnerRejects++
	r.mu.Unlock()
}

func (r *productionRouteProofRecorder) recordDirectLocalBypassReject() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.directLocalBypassRejects++
	r.mu.Unlock()
}

func (r *productionRouteProofRecorder) recordFanoutSplitAttempt() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.fanoutSplitAttempts++
	r.fanoutSplitFailures++
	r.mu.Unlock()
}

func (r *productionRouteProofRecorder) resetCounters() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routeAttemptsTotal = 0
	r.routeLocalOwnerHits = 0
	r.routeRemoteRedirects = 0
	r.routeRemoteForwards = 0
	r.routeUnknownOwnerRejects = 0
	r.directLocalBypassRejects = 0
	r.fanoutSplitAttempts = 0
	r.fanoutSplitFailures = 0
	r.routeGroupHits = make(map[string]int, r.groupCount)
	r.routeLeaderHits = make(map[string]int, r.groupCount)
	r.routeTokenPartitionHits = make(map[string]int, r.partitionCount)
	r.commitGroupHits = make(map[string]int, r.groupCount)
	r.appliedGroupHits = make(map[string]int, r.groupCount)
}

func (r *productionRouteProofRecorder) snapshot() ProductionRouteProofSnapshot {
	if r == nil {
		return ProductionRouteProofSnapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	commitHits := cloneStringIntMap(r.commitGroupHits)
	applyHits := cloneStringIntMap(r.appliedGroupHits)
	commitTotal := int64(sumIntMap(commitHits))
	applyTotal := int64(sumIntMap(applyHits))
	routedOwnerHits := r.routeLocalOwnerHits + r.routeRemoteForwards
	return ProductionRouteProofSnapshot{
		RealRoutedCommits:        r.routeAttemptsTotal > 0 && routedOwnerHits == r.routeAttemptsTotal && r.routeRemoteRedirects == 0 && commitTotal == r.routeAttemptsTotal && applyTotal == r.routeAttemptsTotal,
		RouteAttemptsTotal:       r.routeAttemptsTotal,
		RouteLocalOwnerHits:      r.routeLocalOwnerHits,
		RouteRemoteRedirects:     r.routeRemoteRedirects,
		RouteRemoteForwards:      r.routeRemoteForwards,
		RouteUnknownOwnerRejects: r.routeUnknownOwnerRejects,
		RouteGroupHits:           cloneStringIntMap(r.routeGroupHits),
		RouteLeaderHits:          cloneStringIntMap(r.routeLeaderHits),
		RouteTokenPartitionHits:  cloneStringIntMap(r.routeTokenPartitionHits),
		CommitGroupHits:          commitHits,
		AppliedGroupHits:         applyHits,
		FanoutSplitAttempts:      r.fanoutSplitAttempts,
		FanoutSplitFailures:      r.fanoutSplitFailures,
		DirectLocalBypassRejects: r.directLocalBypassRejects,
	}
}

func productionRouteProofGroupID(group int) string {
	return fmt.Sprintf("group-%02d", group)
}

func productionRouteProofPartitionID(partition int) string {
	return fmt.Sprintf("token-%06d", partition)
}

func cloneStringIntMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]int, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func sumIntMap(values map[string]int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}
