package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
)

// Catalog snapshots contain independently bounded catalog command/record and
// vector-lifecycle byte fields encoded as base64 in a JSON envelope. Keep this
// transport boundary aligned with raftplacement.MaxCatalogMetaSnapshotBytesV1
// without importing the schema package back into raftcluster.
const catalogMetaRaftSnapshotMaxBytesV1 = 8 << 20

// CatalogMetaCommittedStateV1 is implemented by raftplacement's authority.
// The dependency inversion keeps raftcluster independent of catalog schema
// while ensuring only committed Raft Apply and Restore can publish state.
type CatalogMetaCommittedStateV1 interface {
	ApplyCatalogMetaCommittedV1(CatalogMetaApplyCapabilityV1, []byte, uint64) error
	ExportCatalogMetaSnapshotBytesV1() ([]byte, error)
	InstallCatalogMetaSnapshotBytesV1(CatalogMetaRestoreCapabilityV1, []byte) error
}

// CatalogMetaBackupStateV1 is the additional fail-closed contract required by
// external backup/restore. Validation must be side-effect free, and restore
// targets must reject any already-published catalog generation before
// HashiCorp Raft enters its fatal-on-FSM-restore-error path.
type CatalogMetaBackupStateV1 interface {
	CatalogMetaCommittedStateV1
	ValidateCatalogMetaSnapshotBytesV1([]byte) error
	ValidateCatalogMetaBackupRestoreTargetV1() error
}

// CatalogMetaAppliedStateV1 exposes the last catalog command installed in the
// local applied view. A value is safe for serving only when returned after the
// provider's quorum-verified linearizable read fence.
type CatalogMetaAppliedStateV1 interface {
	CatalogMetaAppliedIndexV1() (uint64, bool)
}

type CatalogMetaApplyCapabilityV1 struct{ granted bool }
type CatalogMetaRestoreCapabilityV1 struct{ granted bool }

func catalogMetaApplyCapabilityV1() CatalogMetaApplyCapabilityV1 {
	return CatalogMetaApplyCapabilityV1{granted: true}
}
func (c CatalogMetaApplyCapabilityV1) Granted() bool { return c.granted }
func catalogMetaRestoreCapabilityV1() CatalogMetaRestoreCapabilityV1 {
	return CatalogMetaRestoreCapabilityV1{granted: true}
}
func (c CatalogMetaRestoreCapabilityV1) Granted() bool { return c.granted }

type CatalogMetaRaftProviderOptionsV1 struct {
	Cluster        Config
	State          CatalogMetaCommittedStateV1
	Transport      hraft.Transport
	RaftConfig     *hraft.Config
	LogStore       hraft.LogStore
	StableStore    hraft.StableStore
	SnapshotStore  hraft.SnapshotStore
	Bootstrap      bool
	ApplyTimeout   time.Duration
	SnapshotRetain int
}

// CatalogMetaRaftProviderV1 is the declared single Raft authority for catalog
// commands. It has no local Set API: a generation becomes visible only when
// the committed log invokes State.ApplyCatalogMetaCommittedV1.
type CatalogMetaRaftProviderV1 struct {
	cluster      ResolvedConfig
	raft         *hraft.Raft
	logStore     hraft.LogStore
	state        CatalogMetaCommittedStateV1
	snapshots    hraft.SnapshotStore
	owned        []io.Closer
	applyTimeout time.Duration
	proofLease   time.Duration
	mutationMu   sync.Mutex
	readStats    catalogMetaLinearizableReadStatsV1
}

type CatalogMetaReadSourceV1 string

const (
	CatalogMetaReadSourceUnknownV1              CatalogMetaReadSourceV1 = "unknown"
	CatalogMetaReadSourceOperationsHealthV1     CatalogMetaReadSourceV1 = "operations_health"
	CatalogMetaReadSourceStrictSearchV1         CatalogMetaReadSourceV1 = "strict_search"
	CatalogMetaReadSourceServingRefreshV1       CatalogMetaReadSourceV1 = "serving_refresh"
	CatalogMetaReadSourceCoordinatorLifecycleV1 CatalogMetaReadSourceV1 = "coordinator_lifecycle"
	CatalogMetaReadSourceShardLifecycleV1       CatalogMetaReadSourceV1 = "shard_lifecycle"
)

// CatalogMetaReadProofV1 is the local no-log proof that the meta leader
// observed a current-term quorum and applied the committed prefix before
// reading the catalog state. The monotonic lease is intentionally process
// local; #4096 owns authenticated cross-process propagation.
type CatalogMetaReadProofV1 struct {
	NodeID               NodeID  `json:"node_id"`
	GroupID              GroupID `json:"group_id"`
	LeaderTerm           uint64  `json:"leader_term"`
	CommitIndex          uint64  `json:"commit_index"`
	RaftAppliedIndex     uint64  `json:"raft_applied_index"`
	CatalogAppliedIndex  uint64  `json:"catalog_applied_index"`
	IssuedAtUnixNano     int64   `json:"issued_at_unix_nano"`
	ValidThroughUnixNano int64   `json:"valid_through_unix_nano"`
	QuorumVerified       bool    `json:"quorum_verified"`
	validThrough         time.Time
}

// ValidThroughV1 returns the provider-owned lease deadline, including its
// process-local monotonic clock reading.
func (p CatalogMetaReadProofV1) ValidThroughV1() time.Time {
	return p.validThrough
}

type CatalogMetaLinearizableReadStageStatsV1 struct {
	Reads             uint64 `json:"reads"`
	Successes         uint64 `json:"successes"`
	Failures          uint64 `json:"failures"`
	VerifyLeaderCalls uint64 `json:"verify_leader_calls"`
	LogBarriers       uint64 `json:"log_barriers"`
	NoLogProofs       uint64 `json:"no_log_proofs"`
	TotalNanos        uint64 `json:"total_nanos"`
	AdmissionNanos    uint64 `json:"admission_nanos"`
	VerifyLeaderNanos uint64 `json:"verify_leader_nanos"`
	BarrierNanos      uint64 `json:"barrier_nanos"`
	CurrentTermNanos  uint64 `json:"current_term_nanos"`
	RaftApplyNanos    uint64 `json:"raft_apply_nanos"`
	AppliedReadNanos  uint64 `json:"applied_read_nanos"`
}

type CatalogMetaLinearizableReadStatsV1 struct {
	Total                CatalogMetaLinearizableReadStageStatsV1 `json:"total"`
	OperationsHealth     CatalogMetaLinearizableReadStageStatsV1 `json:"operations_health"`
	StrictSearch         CatalogMetaLinearizableReadStageStatsV1 `json:"strict_search"`
	ServingRefresh       CatalogMetaLinearizableReadStageStatsV1 `json:"serving_refresh"`
	CoordinatorLifecycle CatalogMetaLinearizableReadStageStatsV1 `json:"coordinator_lifecycle"`
	ShardLifecycle       CatalogMetaLinearizableReadStageStatsV1 `json:"shard_lifecycle"`
	Unknown              CatalogMetaLinearizableReadStageStatsV1 `json:"unknown"`
	LastTerm             uint64                                  `json:"last_term"`
	LastCatalogApplied   uint64                                  `json:"last_catalog_applied_index"`
	LastRaftApplied      uint64                                  `json:"last_raft_applied_index"`
	LastRaftLog          uint64                                  `json:"last_raft_log_index"`
}

type catalogMetaLinearizableReadStageStatsV1 struct {
	reads, successes, failures, verifyLeaderCalls, logBarriers, noLogProofs atomic.Uint64
	totalNanos, admissionNanos, verifyLeaderNanos                           atomic.Uint64
	barrierNanos, currentTermNanos, raftApplyNanos, appliedReadNanos        atomic.Uint64
}

type catalogMetaLinearizableReadStatsV1 struct {
	total, operationsHealth, strictSearch, servingRefresh, coordinatorLifecycle, shardLifecycle, unknown catalogMetaLinearizableReadStageStatsV1
	lastTerm, lastCatalogApplied, lastRaftApplied, lastRaftLog                                           atomic.Uint64
}

type catalogMetaReadSourceContextKeyV1 struct{}

func WithCatalogMetaReadSourceV1(ctx context.Context, source CatalogMetaReadSourceV1) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, catalogMetaReadSourceContextKeyV1{}, source)
}

func catalogMetaReadSourceV1(ctx context.Context) CatalogMetaReadSourceV1 {
	if ctx != nil {
		if source, ok := ctx.Value(catalogMetaReadSourceContextKeyV1{}).(CatalogMetaReadSourceV1); ok {
			return source
		}
	}
	return CatalogMetaReadSourceUnknownV1
}

func (s *catalogMetaLinearizableReadStatsV1) source(source CatalogMetaReadSourceV1) *catalogMetaLinearizableReadStageStatsV1 {
	switch source {
	case CatalogMetaReadSourceOperationsHealthV1:
		return &s.operationsHealth
	case CatalogMetaReadSourceStrictSearchV1:
		return &s.strictSearch
	case CatalogMetaReadSourceServingRefreshV1:
		return &s.servingRefresh
	case CatalogMetaReadSourceCoordinatorLifecycleV1:
		return &s.coordinatorLifecycle
	case CatalogMetaReadSourceShardLifecycleV1:
		return &s.shardLifecycle
	default:
		return &s.unknown
	}
}

func (s *catalogMetaLinearizableReadStatsV1) begin(source CatalogMetaReadSourceV1) (*catalogMetaLinearizableReadStageStatsV1, time.Time) {
	s.total.reads.Add(1)
	stage := s.source(source)
	stage.reads.Add(1)
	return stage, time.Now()
}

func (s *catalogMetaLinearizableReadStatsV1) finish(stage *catalogMetaLinearizableReadStageStatsV1, started time.Time, success bool) {
	nanos := uint64(time.Since(started))
	s.total.totalNanos.Add(nanos)
	stage.totalNanos.Add(nanos)
	if success {
		s.total.successes.Add(1)
		stage.successes.Add(1)
		return
	}
	s.total.failures.Add(1)
	stage.failures.Add(1)
}

func catalogMetaLinearizableReadStageSnapshotV1(s *catalogMetaLinearizableReadStageStatsV1) CatalogMetaLinearizableReadStageStatsV1 {
	return CatalogMetaLinearizableReadStageStatsV1{
		Reads: s.reads.Load(), Successes: s.successes.Load(), Failures: s.failures.Load(),
		VerifyLeaderCalls: s.verifyLeaderCalls.Load(), LogBarriers: s.logBarriers.Load(), NoLogProofs: s.noLogProofs.Load(),
		TotalNanos: s.totalNanos.Load(), AdmissionNanos: s.admissionNanos.Load(),
		VerifyLeaderNanos: s.verifyLeaderNanos.Load(), BarrierNanos: s.barrierNanos.Load(), CurrentTermNanos: s.currentTermNanos.Load(), RaftApplyNanos: s.raftApplyNanos.Load(), AppliedReadNanos: s.appliedReadNanos.Load(),
	}
}

func (p *CatalogMetaRaftProviderV1) CatalogMetaLinearizableReadStatsV1() CatalogMetaLinearizableReadStatsV1 {
	if p == nil {
		return CatalogMetaLinearizableReadStatsV1{}
	}
	return CatalogMetaLinearizableReadStatsV1{
		Total:                catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.total),
		OperationsHealth:     catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.operationsHealth),
		StrictSearch:         catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.strictSearch),
		ServingRefresh:       catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.servingRefresh),
		CoordinatorLifecycle: catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.coordinatorLifecycle),
		ShardLifecycle:       catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.shardLifecycle),
		Unknown:              catalogMetaLinearizableReadStageSnapshotV1(&p.readStats.unknown),
		LastTerm:             p.readStats.lastTerm.Load(), LastCatalogApplied: p.readStats.lastCatalogApplied.Load(),
		LastRaftApplied: p.readStats.lastRaftApplied.Load(), LastRaftLog: p.readStats.lastRaftLog.Load(),
	}
}

func (p *CatalogMetaRaftProviderV1) Config() ResolvedConfig {
	if p == nil {
		return ResolvedConfig{}
	}
	return p.cluster
}

func OpenCatalogMetaRaftProviderV1(opts CatalogMetaRaftProviderOptionsV1) (*CatalogMetaRaftProviderV1, error) {
	cluster, err := Validate(opts.Cluster)
	if err != nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, err)
	}
	if opts.State == nil || opts.Transport == nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("catalog meta state and transport are required"))
	}
	for _, peer := range cluster.Peers {
		if !featureSetSupportsV1(peer.Capabilities, FeatureCatalogMetaAuthority) {
			return nil, errors.Join(ErrInvalidHashicorpRaftProvider, ErrUnsupportedFeature, fmt.Errorf("catalog meta voter %q lacks %s", peer.ID, FeatureCatalogMetaAuthority))
		}
	}
	address, ok := localPeerAddress(cluster)
	if !ok || string(opts.Transport.LocalAddr()) != address {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("local peer address does not match transport"))
	}
	stores, err := openHashicorpRaftStores(cluster.Layout, HashicorpRaftProviderOptions{LogStore: opts.LogStore, StableStore: opts.StableStore, SnapshotStore: opts.SnapshotStore, SnapshotRetain: opts.SnapshotRetain})
	if err != nil {
		return nil, err
	}
	defer stores.closeOnError(&err)
	conf := hashicorpRaftConfig(cluster.NodeID, opts.RaftConfig)
	if opts.Bootstrap {
		has, hasStateErr := hraft.HasExistingState(stores.log, stores.stable, stores.snapshots)
		if hasStateErr != nil {
			err = errors.Join(ErrInvalidHashicorpRaftProvider, hasStateErr)
			return nil, err
		}
		if !has {
			if bootstrapErr := hraft.BootstrapCluster(conf, stores.log, stores.stable, stores.snapshots, opts.Transport, hashicorpRaftConfiguration(cluster)); bootstrapErr != nil {
				err = errors.Join(ErrInvalidHashicorpRaftProvider, bootstrapErr)
				return nil, err
			}
		}
	}
	r, err := hraft.NewRaft(conf, catalogMetaRaftFSMV1{state: opts.State}, stores.log, stores.stable, stores.snapshots, opts.Transport)
	if err != nil {
		return nil, err
	}
	timeout := opts.ApplyTimeout
	if timeout <= 0 {
		timeout = hashicorpRaftDefaultApplyTimeout
	}
	return &CatalogMetaRaftProviderV1{
		cluster:      cluster,
		raft:         r,
		logStore:     stores.log,
		state:        opts.State,
		snapshots:    stores.snapshots,
		owned:        stores.owned,
		applyTimeout: timeout,
		proofLease:   conf.LeaderLeaseTimeout / 2,
	}, nil
}

func featureSetSupportsV1(features FeatureSet, name FeatureName) bool {
	floor, known := SupportedFeatureFloors[name]
	if !known {
		return false
	}
	for _, required := range features.Required {
		if required.Name == name &&
			required.Version.Major == floor.Major &&
			required.Version.Minor >= floor.Minor {
			return true
		}
	}
	return false
}

func (p *CatalogMetaRaftProviderV1) ClusterAdmissionStatus(ctx context.Context) (AdmissionStatus, error) {
	if p == nil || p.raft == nil {
		return AdmissionStatus{}, ErrInvalidHashicorpRaftProvider
	}
	state := p.raft.State()
	if state == hraft.Leader {
		return LeaderAdmission(), nil
	}
	_, leader := p.raft.LeaderWithID()
	if state == hraft.Shutdown {
		return UnavailableAdmission("catalog meta raft shutdown"), nil
	}
	if leader != "" {
		return FollowerAdmission(NodeID(leader), "catalog meta raft follower"), nil
	}
	return UnavailableAdmission("catalog meta leader unavailable"), nil
}

// LinearizableCatalogMetaReadProofV1 verifies current leadership with the voter
// quorum, waits for the current-term committed prefix to apply locally, and
// returns the exact catalog view without appending a Raft log entry.
func (p *CatalogMetaRaftProviderV1) LinearizableCatalogMetaReadProofV1(ctx context.Context) (CatalogMetaReadProofV1, error) {
	if p == nil || p.raft == nil || p.state == nil {
		return CatalogMetaReadProofV1{}, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CatalogMetaReadProofV1{}, err
	}
	stage, started := p.readStats.begin(catalogMetaReadSourceV1(ctx))
	success := false
	defer func() { p.readStats.finish(stage, started, success) }()

	admissionStarted := time.Now()
	status, err := p.ClusterAdmissionStatus(ctx)
	admissionNanos := uint64(time.Since(admissionStarted))
	p.readStats.total.admissionNanos.Add(admissionNanos)
	stage.admissionNanos.Add(admissionNanos)
	if err != nil {
		return CatalogMetaReadProofV1{}, err
	}
	if status.Unavailable {
		return CatalogMetaReadProofV1{}, ErrHashicorpRaftUnavailable
	}
	if !status.Leader {
		return CatalogMetaReadProofV1{}, ErrNotLeader
	}
	verifiedTerm := p.raft.CurrentTerm()
	if verifiedTerm == 0 {
		return CatalogMetaReadProofV1{}, ErrReadBarrierNotSatisfied
	}
	verifyStarted := time.Now()
	p.readStats.total.verifyLeaderCalls.Add(1)
	stage.verifyLeaderCalls.Add(1)
	verifyErr := waitHashicorpRaftFuture(ctx, p.raft.VerifyLeader())
	verifyNanos := uint64(time.Since(verifyStarted))
	p.readStats.total.verifyLeaderNanos.Add(verifyNanos)
	stage.verifyLeaderNanos.Add(verifyNanos)
	if verifyErr != nil {
		return CatalogMetaReadProofV1{}, mapCatalogMetaLinearizableReadErrorV1(verifyErr)
	}
	verifiedAt := time.Now()
	if err := p.requireCatalogMetaReadLeaderTermV1(verifiedTerm); err != nil {
		return CatalogMetaReadProofV1{}, err
	}

	currentTermStarted := time.Now()
	term, commitIndex, err := waitHashicorpCurrentTermCommitV1(ctx, p.raft, p.logStore, p.applyTimeout, p.requireCatalogMetaReadLeaderV1)
	currentTermNanos := uint64(time.Since(currentTermStarted))
	p.readStats.total.currentTermNanos.Add(currentTermNanos)
	stage.currentTermNanos.Add(currentTermNanos)
	if err != nil {
		return CatalogMetaReadProofV1{}, mapCatalogMetaLinearizableReadErrorV1(err)
	}
	if term != verifiedTerm {
		return CatalogMetaReadProofV1{}, errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog leader term changed during quorum proof"))
	}

	raftApplyStarted := time.Now()
	err = p.waitCatalogMetaRaftAppliedV1(ctx, term, commitIndex)
	raftApplyNanos := uint64(time.Since(raftApplyStarted))
	p.readStats.total.raftApplyNanos.Add(raftApplyNanos)
	stage.raftApplyNanos.Add(raftApplyNanos)
	if err != nil {
		return CatalogMetaReadProofV1{}, err
	}

	appliedStarted := time.Now()
	defer func() {
		appliedNanos := uint64(time.Since(appliedStarted))
		p.readStats.total.appliedReadNanos.Add(appliedNanos)
		stage.appliedReadNanos.Add(appliedNanos)
	}()
	if err := p.requireCatalogMetaReadLeaderTermV1(term); err != nil {
		return CatalogMetaReadProofV1{}, err
	}
	appliedState, ok := p.state.(CatalogMetaAppliedStateV1)
	if !ok {
		return CatalogMetaReadProofV1{}, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("catalog state does not expose applied index"))
	}
	applied, ok := appliedState.CatalogMetaAppliedIndexV1()
	if !ok {
		return CatalogMetaReadProofV1{}, ErrHashicorpRaftUnavailable
	}
	if err := p.requireCatalogMetaReadLeaderTermV1(term); err != nil {
		return CatalogMetaReadProofV1{}, err
	}
	validThrough := verifiedAt.Add(p.proofLease)
	if !time.Now().Before(validThrough) {
		return CatalogMetaReadProofV1{}, errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog read proof lease expired during capture"))
	}
	proof := CatalogMetaReadProofV1{
		NodeID: p.cluster.NodeID, GroupID: p.cluster.GroupID,
		LeaderTerm: term, CommitIndex: commitIndex, RaftAppliedIndex: p.raft.AppliedIndex(), CatalogAppliedIndex: applied,
		IssuedAtUnixNano: verifiedAt.UnixNano(), ValidThroughUnixNano: validThrough.UnixNano(), QuorumVerified: true,
		validThrough: validThrough,
	}
	p.readStats.total.noLogProofs.Add(1)
	stage.noLogProofs.Add(1)
	p.readStats.lastTerm.Store(term)
	p.readStats.lastCatalogApplied.Store(applied)
	p.readStats.lastRaftApplied.Store(proof.RaftAppliedIndex)
	p.readStats.lastRaftLog.Store(p.raft.LastIndex())
	success = true
	return proof, nil
}

// LinearizableCatalogMetaAppliedIndexV1 preserves the existing caller contract
// while using the no-log proof path.
func (p *CatalogMetaRaftProviderV1) LinearizableCatalogMetaAppliedIndexV1(ctx context.Context) (uint64, error) {
	proof, err := p.LinearizableCatalogMetaReadProofV1(ctx)
	return proof.CatalogAppliedIndex, err
}

// ValidateCatalogMetaReadProofLeaseV1 validates a proof using only local
// in-memory Raft/catalog state. It performs no network or log I/O.
func (p *CatalogMetaRaftProviderV1) ValidateCatalogMetaReadProofLeaseV1(proof CatalogMetaReadProofV1) error {
	if p == nil || p.raft == nil || p.state == nil || proof.validThrough.IsZero() || !proof.QuorumVerified ||
		proof.NodeID != p.cluster.NodeID || proof.GroupID != p.cluster.GroupID || proof.LeaderTerm == 0 ||
		proof.CommitIndex == 0 || proof.RaftAppliedIndex < proof.CommitIndex || proof.CatalogAppliedIndex == 0 {
		return ErrReadBarrierNotSatisfied
	}
	if !time.Now().Before(proof.validThrough) {
		return errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog read proof lease expired"))
	}
	if err := p.requireCatalogMetaReadLeaderTermV1(proof.LeaderTerm); err != nil {
		return err
	}
	if p.raft.AppliedIndex() < proof.CommitIndex {
		return errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog raft applied index is behind proof"))
	}
	appliedState, ok := p.state.(CatalogMetaAppliedStateV1)
	if !ok {
		return errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("catalog state does not expose applied index"))
	}
	applied, ok := appliedState.CatalogMetaAppliedIndexV1()
	if !ok || applied != proof.CatalogAppliedIndex {
		return errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog applied identity changed"))
	}
	return nil
}

func (p *CatalogMetaRaftProviderV1) requireCatalogMetaReadLeaderV1() error {
	if p == nil || p.raft == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	if state := p.raft.State(); state != hraft.Leader {
		if state == hraft.Shutdown {
			return ErrHashicorpRaftUnavailable
		}
		return ErrNotLeader
	}
	return nil
}

func (p *CatalogMetaRaftProviderV1) requireCatalogMetaReadLeaderTermV1(term uint64) error {
	if err := p.requireCatalogMetaReadLeaderV1(); err != nil {
		return err
	}
	if got := p.raft.CurrentTerm(); got != term {
		return errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog leader term changed from %d to %d", term, got))
	}
	return nil
}

func (p *CatalogMetaRaftProviderV1) waitCatalogMetaRaftAppliedV1(ctx context.Context, term, minIndex uint64) error {
	timeout := p.applyTimeout
	if timeout <= 0 {
		timeout = hashicorpRaftDefaultApplyTimeout
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	poll := time.NewTicker(hashicorpRaftReadIndexInitialPoll)
	defer poll.Stop()
	for {
		if err := p.requireCatalogMetaReadLeaderTermV1(term); err != nil {
			return err
		}
		if p.raft.AppliedIndex() >= minIndex {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.Join(ErrReadBarrierNotSatisfied, fmt.Errorf("catalog raft applied index %d is behind commit %d", p.raft.AppliedIndex(), minIndex))
		case <-poll.C:
		}
	}
}

func mapCatalogMetaLinearizableReadErrorV1(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, hraft.ErrNotLeader),
		errors.Is(err, hraft.ErrLeadershipLost),
		errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		return errors.Join(ErrNotLeader, err)
	case errors.Is(err, hraft.ErrRaftShutdown),
		errors.Is(err, hraft.ErrEnqueueTimeout),
		errors.Is(err, hraft.ErrAbortedByRestore):
		return errors.Join(ErrHashicorpRaftUnavailable, err)
	default:
		return errors.Join(ErrHashicorpRaftUnavailable, err)
	}
}
func (p *CatalogMetaRaftProviderV1) Close() error {
	if p == nil {
		return nil
	}
	var es []error
	if p.raft != nil {
		es = append(es, p.raft.Shutdown().Error())
	}
	for _, c := range p.owned {
		es = append(es, c.Close())
	}
	return errors.Join(es...)
}
func (p *CatalogMetaRaftProviderV1) SubmitCatalogMetaCommandV1(ctx context.Context, command []byte) (uint64, uint64, error) {
	if p == nil || p.raft == nil {
		return 0, 0, ErrInvalidHashicorpRaftProvider
	}
	p.mutationMu.Lock()
	defer p.mutationMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	status, err := p.ClusterAdmissionStatus(ctx)
	if err != nil {
		return 0, 0, err
	}
	if status.Unavailable {
		return 0, 0, ErrAdmissionUnavailable
	}
	if !status.Leader {
		return 0, 0, ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		// Nothing has been enqueued yet; this is an ordinary caller
		// cancellation, not an ambiguous commit outcome.
		return 0, 0, err
	}
	f := p.raft.Apply(bytes.Clone(command), p.applyTimeout)
	if err := waitHashicorpRaftFuture(ctx, f); err != nil {
		return 0, 0, mapHashicorpRaftApplyError(err)
	}
	response, ok := f.Response().(catalogMetaRaftApplyResponseV1)
	if !ok {
		return 0, 0, ErrHashicorpRaftLogEntry
	}
	if response.err != nil {
		return 0, 0, response.err
	}
	return response.term, response.index, nil
}
func (p *CatalogMetaRaftProviderV1) SnapshotCatalogMetaV1(ctx context.Context) error {
	if p == nil || p.raft == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	p.mutationMu.Lock()
	defer p.mutationMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	return waitHashicorpRaftFuture(ctx, p.raft.Snapshot())
}

type catalogMetaRaftApplyResponseV1 struct {
	term, index uint64
	err         error
}
type catalogMetaRaftFSMV1 struct{ state CatalogMetaCommittedStateV1 }

func (f catalogMetaRaftFSMV1) Apply(log *hraft.Log) interface{} {
	if log == nil || log.Type != hraft.LogCommand || f.state == nil {
		return catalogMetaRaftApplyResponseV1{err: ErrHashicorpRaftLogEntry}
	}
	return catalogMetaRaftApplyResponseV1{term: log.Term, index: log.Index, err: f.state.ApplyCatalogMetaCommittedV1(catalogMetaApplyCapabilityV1(), bytes.Clone(log.Data), log.Index)}
}
func (f catalogMetaRaftFSMV1) Snapshot() (hraft.FSMSnapshot, error) {
	if f.state == nil {
		return nil, ErrRaftSnapshotUnsupported
	}
	b, e := f.state.ExportCatalogMetaSnapshotBytesV1()
	if e != nil {
		return nil, e
	}
	return catalogMetaRaftSnapshotV1{bytes: b}, nil
}
func (f catalogMetaRaftFSMV1) Restore(src io.ReadCloser) error {
	if src == nil || f.state == nil {
		return ErrRaftSnapshotUnsupported
	}
	defer src.Close()
	b, e := io.ReadAll(io.LimitReader(src, catalogMetaRaftSnapshotMaxBytesV1+1))
	if e != nil {
		return e
	}
	if len(b) > catalogMetaRaftSnapshotMaxBytesV1 {
		return ErrHashicorpRaftLogEntry
	}
	return f.state.InstallCatalogMetaSnapshotBytesV1(catalogMetaRestoreCapabilityV1(), b)
}

type catalogMetaRaftSnapshotV1 struct{ bytes []byte }

func (s catalogMetaRaftSnapshotV1) Persist(sink hraft.SnapshotSink) error {
	if sink == nil {
		return ErrRaftSnapshotUnsupported
	}
	if _, e := sink.Write(s.bytes); e != nil {
		_ = sink.Cancel()
		return e
	}
	return sink.Close()
}
func (s catalogMetaRaftSnapshotV1) Release() {}
