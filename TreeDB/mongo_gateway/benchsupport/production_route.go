package benchsupport

import (
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
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

// ProductionRouteProofOptions scopes the bench-only local-owner routed commit
// proof. It intentionally models only locally configured groups; it does not
// perform remote forwarding or fanout execution.
type ProductionRouteProofOptions struct {
	GroupCount     int
	PartitionCount int
}

// ProductionRouteProofSnapshot is the bench-facing counter snapshot proving
// that routed submit/apply went through the production cluster submitter path.
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

// ProductionRouteProofHarness owns the local raft FSM and route counters used
// by mongo_gateway_bench production-route correctness evidence.
type ProductionRouteProofHarness struct {
	fsm        *raftfsm.FSM
	recorder   *productionRouteProofRecorder
	dispatcher *raftcluster.GroupRoutedSubmitter
}

// ConfigureProductionRouteProofHarness wires server.ClusterSubmitter to a
// single local group routed dispatcher. The commit source is deterministic and
// in-process, so this is correctness/instrumentation proof for the gateway
// route boundary, not a multi-node scaleout benchmark.
func ConfigureProductionRouteProofHarness(opts ProductionRouteProofOptions, db *backenddb.DB, manager *collections.CollectionManager, server *mongogateway.Server) (*ProductionRouteProofHarness, error) {
	if server == nil {
		return nil, errors.New("production route proof harness requires Mongo gateway server")
	}
	cluster := productionRouteProofClusterConfig(db)
	recorder := newProductionRouteProofRecorder(opts.GroupCount, opts.PartitionCount)
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
	commit := &productionRouteProofCommitSource{
		inner: raftcluster.NewSequencedCommitSource(raftcluster.SequencedCommitSourceOptions{
			GroupID:             cluster.GroupID,
			NodeID:              cluster.NodeID,
			LeaderID:            cluster.NodeID,
			EvidenceKind:        raftcluster.CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		recorder: recorder,
	}
	applier := &productionRouteProofApplier{
		inner:          fsm,
		recorder:       recorder,
		catalogVersion: catalogVersion,
	}
	bridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
		Cluster:                cluster,
		AdmissionProvider:      raftcluster.StaticAdmissionProvider{Status: raftcluster.LeaderAdmission()},
		CommitSource:           commit,
		Preflight:              fsm,
		Applier:                applier,
		CatalogVersionProvider: catalogVersion,
	})
	if err != nil {
		_ = fsm.Close()
		return nil, err
	}
	registry, err := raftcluster.NewGroupSubmitterRegistryV1([]raftcluster.GroupSubmitterV1{
		{GroupID: cluster.GroupID, Submitter: bridge},
	})
	if err != nil {
		_ = fsm.Close()
		return nil, err
	}
	dispatcher, err := raftcluster.NewGroupRoutedSubmitter(raftcluster.GroupRoutedSubmitterOptions{Registry: registry})
	if err != nil {
		_ = fsm.Close()
		return nil, err
	}
	server.ClusterSubmitter = nativewire.NewRoutedRaftClusterSubmitter(dispatcher, recorder, manager)
	server.ClusterCatalogVersion = catalogVersion.ServerCatalogVersion
	return &ProductionRouteProofHarness{
		fsm:        fsm,
		recorder:   recorder,
		dispatcher: dispatcher,
	}, nil
}

func (h *ProductionRouteProofHarness) Close() error {
	if h == nil || h.fsm == nil {
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

func (h *ProductionRouteProofHarness) ProbeUnknownOwnerReject(ctx context.Context, database, collection string) error {
	if h == nil || h.dispatcher == nil {
		return errors.New("production route proof dispatcher is not configured")
	}
	unknown := nativewire.ClusterRequestMetadata{
		AckPolicy:                 nativewire.AckVisible,
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      database,
		ClusterRouteCatalog:       "default",
		ClusterRouteCollection:    collection,
		ClusterRouteShape:         string(nativewire.ClusterRouteShapeToken),
		ClusterRouteGroupID:       "group-99",
		ClusterRouteMembers:       []string{"node-99-a"},
		ClusterRouteLeaderHint:    "node-99-a",
		ClusterRoutePlacementMode: "ring",
		ClusterRouteKey:           "_id",
		ClusterRouteTokenKnown:    true,
		ClusterRouteToken:         0,
		ClusterRoutePartitionID:   "token-999999",
	}
	if _, err := h.dispatcher.SubmitCommandEntryV1(ctx, nil, unknown); !errors.Is(err, raftcluster.ErrRouteTargetUnknown) {
		return fmt.Errorf("production route unknown-owner guardrail err=%v want %v", err, raftcluster.ErrRouteTargetUnknown)
	}
	h.recorder.recordUnknownOwnerReject()
	return nil
}

func (h *ProductionRouteProofHarness) ProbeDirectLocalBypassReject(ctx context.Context) error {
	if h == nil || h.dispatcher == nil {
		return errors.New("production route proof dispatcher is not configured")
	}
	direct := nativewire.ClusterRequestMetadata{AckPolicy: nativewire.AckVisible}
	if _, err := h.dispatcher.SubmitCommandEntryV1(ctx, nil, direct); !errors.Is(err, raftcluster.ErrRouteTargetMissing) {
		return fmt.Errorf("production route direct-local-bypass guardrail err=%v want %v", err, raftcluster.ErrRouteTargetMissing)
	}
	h.recorder.recordDirectLocalBypassReject()
	return nil
}

func productionRouteProofClusterConfig(db *backenddb.DB) raftcluster.Config {
	dir := ""
	if db != nil {
		dir = db.Dir()
	}
	return raftcluster.Config{
		Dir:     dir,
		NodeID:  "node-00-a",
		GroupID: raftcluster.GroupID(productionRouteProofGroupID(0)),
		Peers: []raftcluster.Peer{
			{ID: "node-00-a", Address: "127.0.0.1:9701"},
			{ID: "node-00-b", Address: "127.0.0.1:9702"},
			{ID: "node-00-c", Address: "127.0.0.1:9703"},
		},
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
	state := p.db.State()
	if state == nil {
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

	mu                       sync.Mutex
	routeAttemptsTotal       int64
	routeLocalOwnerHits      int64
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

func newProductionRouteProofRecorder(groupCount, partitionCount int) *productionRouteProofRecorder {
	if groupCount < 1 {
		groupCount = 1
	}
	if partitionCount < groupCount {
		partitionCount = groupCount
	}
	localGroups := make(map[string]struct{}, groupCount)
	for group := 0; group < groupCount; group++ {
		localGroups[productionRouteProofGroupID(group)] = struct{}{}
	}
	return &productionRouteProofRecorder{
		groupCount:              groupCount,
		partitionCount:          partitionCount,
		localGroups:             localGroups,
		routeGroupHits:          make(map[string]int, groupCount),
		routeLeaderHits:         make(map[string]int, groupCount),
		routeTokenPartitionHits: make(map[string]int, partitionCount),
		commitGroupHits:         make(map[string]int, groupCount),
		appliedGroupHits:        make(map[string]int, groupCount),
	}
}

func (r *productionRouteProofRecorder) ClusterRoute(_ context.Context, request nativewire.ClusterRouteRequest) (nativewire.ClusterRouteTarget, error) {
	if r == nil {
		return nativewire.ClusterRouteTarget{}, errors.New("missing production route proof recorder")
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

func (r *productionRouteProofRecorder) recordRouteSuccess(target nativewire.ClusterRouteTarget) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routeAttemptsTotal++
	if _, ok := r.localGroups[target.GroupID]; ok {
		r.routeLocalOwnerHits++
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
	return ProductionRouteProofSnapshot{
		RealRoutedCommits:        r.routeAttemptsTotal > 0 && r.routeLocalOwnerHits == r.routeAttemptsTotal && commitTotal == r.routeAttemptsTotal && applyTotal == r.routeAttemptsTotal,
		RouteAttemptsTotal:       r.routeAttemptsTotal,
		RouteLocalOwnerHits:      r.routeLocalOwnerHits,
		RouteRemoteRedirects:     0,
		RouteRemoteForwards:      0,
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
