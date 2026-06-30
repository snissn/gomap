package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

var (
	ErrInvalidSubmitter          = errors.New("raftcluster: invalid submitter")
	ErrAdmissionUnavailable      = errors.New("raftcluster: admission unavailable")
	ErrNotLeader                 = errors.New("raftcluster: not leader")
	ErrCommitNotProven           = errors.New("raftcluster: commit not proven")
	ErrLocalApplyNotRecoverable  = errors.New("raftcluster: local apply not recoverable")
	ErrInvalidCommittedEntry     = errors.New("raftcluster: invalid committed entry")
	ErrUnsupportedSubmitAck      = errors.New("raftcluster: unsupported submit ack")
	ErrMissingCatalogVersion     = errors.New("raftcluster: missing catalog version")
	ErrUnexpectedCommittedTarget = errors.New("raftcluster: committed target mismatch")
)

// AdmissionProvider exposes the single-group write-admission state. Returning
// the zero AdmissionStatus fails closed as not-leader.
type AdmissionProvider interface {
	ClusterAdmissionStatus(context.Context) (AdmissionStatus, error)
}

// AdmissionStatus reports whether this node may submit single-group writes.
type AdmissionStatus struct {
	Leader      bool
	Unavailable bool
	LeaderHint  NodeID
	Reason      string
}

func LeaderAdmission() AdmissionStatus {
	return AdmissionStatus{Leader: true}
}

func FollowerAdmission(leaderHint NodeID, reason string) AdmissionStatus {
	return AdmissionStatus{LeaderHint: leaderHint, Reason: reason}
}

func UnavailableAdmission(reason string) AdmissionStatus {
	return AdmissionStatus{Unavailable: true, Reason: reason}
}

// StaticAdmissionProvider is a small deterministic AdmissionProvider useful
// for tests and single-node smoke wiring. Production providers should surface
// live leader/follower/unavailable state from the selected consensus adapter.
type StaticAdmissionProvider struct {
	Status AdmissionStatus
	Err    error
}

func (p StaticAdmissionProvider) ClusterAdmissionStatus(context.Context) (AdmissionStatus, error) {
	if p.Err != nil {
		return AdmissionStatus{}, p.Err
	}
	return p.Status, nil
}

// CatalogVersionProvider returns the local catalog version used for
// deterministic catalog guards. ok=false fails closed before commit.
type CatalogVersionProvider interface {
	CurrentCatalogVersion(context.Context) (version uint64, ok bool, err error)
}

type CatalogVersionProviderFunc func(context.Context) (uint64, bool, error)

func (f CatalogVersionProviderFunc) CurrentCatalogVersion(ctx context.Context) (uint64, bool, error) {
	if f == nil {
		return 0, false, nil
	}
	return f(ctx)
}

// CommittedCommandEntryV1 is a deterministic command entry after the
// single-group commit source has assigned a Raft log identity.
type CommittedCommandEntryV1 struct {
	Term  uint64
	Index uint64
	Bytes []byte

	CurrentCatalogVersion    uint64
	HasCurrentCatalogVersion bool
	SyncLocalCommandWAL      bool
	RequestMetadata          raftentry.RequestMetadataV1
	ExpectedTarget           *raftentry.TargetIdentityV1
}

func (e CommittedCommandEntryV1) EntryID() raftentry.ApplyEntryID {
	return raftentry.ApplyEntryID{Term: e.Term, Index: e.Index}
}

func (e CommittedCommandEntryV1) Clone() CommittedCommandEntryV1 {
	e.Bytes = bytes.Clone(e.Bytes)
	e.RequestMetadata = cloneRequestMetadataV1(e.RequestMetadata)
	e.ExpectedTarget = cloneExpectedTargetV1(e.ExpectedTarget)
	return e
}

// CommitCommandEntryV1Request is the provider boundary between submit
// admission and a concrete single-group commit source. The entry bytes are the
// deterministic native-wire CommandEntryV1 payload already validated by the
// submitter.
type CommitCommandEntryV1Request struct {
	GroupID GroupID
	NodeID  NodeID

	EntryBytes []byte

	CurrentCatalogVersion    uint64
	HasCurrentCatalogVersion bool
	SyncLocalCommandWAL      bool
	RequestMetadata          raftentry.RequestMetadataV1
	ExpectedTarget           *raftentry.TargetIdentityV1
}

func (r CommitCommandEntryV1Request) Clone() CommitCommandEntryV1Request {
	r.EntryBytes = bytes.Clone(r.EntryBytes)
	r.RequestMetadata = cloneRequestMetadataV1(r.RequestMetadata)
	r.ExpectedTarget = cloneExpectedTargetV1(r.ExpectedTarget)
	return r
}

type CommitEvidenceKindV1 string

const (
	// CommitEvidenceProductionConsensusV1 is the only evidence kind that may
	// satisfy AckRaftCommitted. The selected consensus adapter is responsible
	// for setting ProductionConsensus=true only after quorum commitment.
	CommitEvidenceProductionConsensusV1 CommitEvidenceKindV1 = "production-consensus-v1"

	// CommitEvidenceDeterministicHarnessV1 records deterministic local/test
	// commit ordering. It never proves production quorum commitment.
	CommitEvidenceDeterministicHarnessV1 CommitEvidenceKindV1 = "deterministic-harness-v1"
)

type CommitEvidenceV1 struct {
	Kind                CommitEvidenceKindV1
	GroupID             GroupID
	NodeID              NodeID
	LeaderID            NodeID
	Term                uint64
	Index               uint64
	Committed           bool
	ProductionConsensus bool
}

func (e CommitEvidenceV1) EntryID() raftentry.ApplyEntryID {
	return raftentry.ApplyEntryID{Term: e.Term, Index: e.Index}
}

func (e CommitEvidenceV1) ProvesProductionConsensus() bool {
	return e.Kind == CommitEvidenceProductionConsensusV1 &&
		e.Committed &&
		e.ProductionConsensus &&
		e.Term != 0 &&
		e.Index != 0
}

type CommitCommandEntryV1Result struct {
	Entry    CommittedCommandEntryV1
	Evidence CommitEvidenceV1
}

// CommitSource commits deterministic command entries and returns the committed
// log identity plus evidence. A test/harness source may implement this
// interface, but only production-consensus evidence can satisfy raft_committed.
type CommitSource interface {
	CommitCommandEntryV1(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error)
}

type CommitSourceFunc func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error)

func (f CommitSourceFunc) CommitCommandEntryV1(ctx context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
	if f == nil {
		return CommitCommandEntryV1Result{}, ErrInvalidSubmitter
	}
	return f(ctx, req)
}

// CommittedCommandApplierV1 is implemented by the local raftfsm adapter.
type CommittedCommandApplierV1 interface {
	ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error)
}

type CommittedCommandApplierFunc func(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error)

func (f CommittedCommandApplierFunc) ApplyCommittedCommandEntryV1(ctx context.Context, entry CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	if f == nil {
		return raftentry.ApplyResultV1{}, ErrInvalidSubmitter
	}
	return f(ctx, entry)
}

type SingleGroupSubmitterOptions struct {
	Cluster                Config
	AdmissionProvider      AdmissionProvider
	CommitSource           CommitSource
	Applier                CommittedCommandApplierV1
	CatalogVersionProvider CatalogVersionProvider

	DecodeLimits  iwire.Limits
	ScopeRule     raftentry.ScopeRuleV1
	DatabaseScope string
	CatalogScope  string

	// DisableLocalCommandWALSync is for tests that intentionally model weaker
	// local recovery. The default bridge syncs local command-WAL coverage before
	// reporting CommittedRecoverable.
	DisableLocalCommandWALSync bool
}

type SingleGroupSubmitter struct {
	cluster         ResolvedConfig
	admission       AdmissionProvider
	commit          CommitSource
	applier         CommittedCommandApplierV1
	catalogProvider CatalogVersionProvider

	decodeLimits iwire.Limits
	scopeRule    raftentry.ScopeRuleV1
	database     string
	catalog      string
	syncLocalWAL bool
}

type SubmitResultV1 struct {
	ActualAck            iwire.AckPolicy
	CommittedRecoverable bool
	DecodedEntry         raftentry.CommandEntryV1
	ApplyResult          raftentry.ApplyResultV1
	CommittedEntry       CommittedCommandEntryV1
	Evidence             CommitEvidenceV1
	CatalogVersion       uint64
	HasCatalogVersion    bool
}

func NewSingleGroupSubmitter(opts SingleGroupSubmitterOptions) (*SingleGroupSubmitter, error) {
	cluster, err := Validate(opts.Cluster)
	if err != nil {
		return nil, errors.Join(ErrInvalidSubmitter, err)
	}
	if opts.AdmissionProvider == nil {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("admission provider is required"))
	}
	if opts.CommitSource == nil {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("commit source is required"))
	}
	if opts.Applier == nil {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("committed command applier is required"))
	}
	if opts.CatalogVersionProvider == nil {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("catalog version provider is required"))
	}
	return &SingleGroupSubmitter{
		cluster:         cluster,
		admission:       opts.AdmissionProvider,
		commit:          opts.CommitSource,
		applier:         opts.Applier,
		catalogProvider: opts.CatalogVersionProvider,
		decodeLimits:    opts.DecodeLimits,
		scopeRule:       opts.ScopeRule,
		database:        opts.DatabaseScope,
		catalog:         opts.CatalogScope,
		syncLocalWAL:    !opts.DisableLocalCommandWALSync,
	}, nil
}

func (s *SingleGroupSubmitter) Config() ResolvedConfig {
	if s == nil {
		return ResolvedConfig{}
	}
	return s.cluster
}

func (s *SingleGroupSubmitter) ClusterAdmissionStatus(ctx context.Context) (AdmissionStatus, error) {
	if s == nil || s.admission == nil {
		return AdmissionStatus{}, ErrInvalidSubmitter
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.admission.ClusterAdmissionStatus(ctx)
}

func (s *SingleGroupSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (SubmitResultV1, error) {
	if s == nil || s.commit == nil || s.applier == nil || s.catalogProvider == nil {
		return SubmitResultV1{}, ErrInvalidSubmitter
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.admit(ctx); err != nil {
		return SubmitResultV1{}, err
	}
	catalogVersion, hasCatalogVersion, err := s.catalogProvider.CurrentCatalogVersion(ctx)
	if err != nil {
		return SubmitResultV1{}, errors.Join(ErrMissingCatalogVersion, err)
	}
	if !hasCatalogVersion {
		return SubmitResultV1{}, ErrMissingCatalogVersion
	}
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{
		Limits:          s.decodeLimits,
		ScopeRule:       s.scopeRule,
		DatabaseScope:   s.database,
		CatalogScope:    s.catalog,
		RequestMetadata: cloneRequestMetadataV1(metadata),
	})
	if err != nil {
		return SubmitResultV1{}, err
	}
	actualAck, err := actualSubmitAck(metadata.AckPolicy)
	if err != nil {
		return SubmitResultV1{}, err
	}
	expectedTarget := decoded.Target.Clone()
	request := CommitCommandEntryV1Request{
		GroupID:                  s.cluster.GroupID,
		NodeID:                   s.cluster.NodeID,
		EntryBytes:               bytes.Clone(entry),
		CurrentCatalogVersion:    catalogVersion,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      s.syncLocalWAL,
		RequestMetadata:          cloneRequestMetadataV1(metadata),
		ExpectedTarget:           &expectedTarget,
	}
	commitResult, err := s.commit.CommitCommandEntryV1(ctx, request.Clone())
	if err != nil {
		return SubmitResultV1{}, err
	}
	committed, err := s.validateCommitResult(request, commitResult)
	if err != nil {
		return SubmitResultV1{}, err
	}
	applyResult, err := s.applier.ApplyCommittedCommandEntryV1(ctx, committed.Clone())
	if err != nil {
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, err)
	}
	if applyResult.Status != raftentry.ApplyStatusApplied && applyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, fmt.Errorf("apply status %s", applyResult.Status))
	}
	postCatalogVersion, postHasCatalogVersion, err := s.catalogProvider.CurrentCatalogVersion(ctx)
	if err != nil {
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, err)
	}
	if !postHasCatalogVersion {
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, ErrMissingCatalogVersion)
	}
	return SubmitResultV1{
		ActualAck:            actualAck,
		CommittedRecoverable: actualAck == iwire.AckRaftCommitted,
		DecodedEntry:         decoded,
		ApplyResult:          applyResult,
		CommittedEntry:       committed,
		Evidence:             commitResult.Evidence,
		CatalogVersion:       postCatalogVersion,
		HasCatalogVersion:    postHasCatalogVersion,
	}, nil
}

func (s *SingleGroupSubmitter) admit(ctx context.Context) error {
	status, err := s.ClusterAdmissionStatus(ctx)
	if err != nil {
		return errors.Join(ErrAdmissionUnavailable, err)
	}
	if status.Unavailable {
		if status.Reason == "" {
			return ErrAdmissionUnavailable
		}
		return errors.Join(ErrAdmissionUnavailable, fmt.Errorf("%s", status.Reason))
	}
	if !status.Leader {
		if status.Reason == "" && status.LeaderHint == "" {
			return ErrNotLeader
		}
		msg := status.Reason
		if msg == "" {
			msg = "not leader"
		}
		if status.LeaderHint != "" {
			msg += "; leader_hint=" + string(status.LeaderHint)
		}
		return errors.Join(ErrNotLeader, fmt.Errorf("%s", msg))
	}
	return nil
}

func (s *SingleGroupSubmitter) validateCommitResult(request CommitCommandEntryV1Request, result CommitCommandEntryV1Result) (CommittedCommandEntryV1, error) {
	if !result.Evidence.ProvesProductionConsensus() {
		return CommittedCommandEntryV1{}, ErrCommitNotProven
	}
	if result.Evidence.GroupID != "" && result.Evidence.GroupID != s.cluster.GroupID {
		return CommittedCommandEntryV1{}, errors.Join(ErrCommitNotProven, fmt.Errorf("evidence group %q does not match local group %q", result.Evidence.GroupID, s.cluster.GroupID))
	}
	if result.Evidence.NodeID != "" && result.Evidence.NodeID != s.cluster.NodeID {
		return CommittedCommandEntryV1{}, errors.Join(ErrCommitNotProven, fmt.Errorf("evidence node %q does not match local node %q", result.Evidence.NodeID, s.cluster.NodeID))
	}
	committed := result.Entry.Clone()
	if committed.Term == 0 || committed.Index == 0 {
		return CommittedCommandEntryV1{}, errors.Join(ErrInvalidCommittedEntry, fmt.Errorf("missing term/index"))
	}
	if committed.Term != result.Evidence.Term || committed.Index != result.Evidence.Index {
		return CommittedCommandEntryV1{}, errors.Join(ErrCommitNotProven, fmt.Errorf("committed entry id %d/%d does not match evidence %d/%d", committed.Term, committed.Index, result.Evidence.Term, result.Evidence.Index))
	}
	if !bytes.Equal(committed.Bytes, request.EntryBytes) {
		return CommittedCommandEntryV1{}, errors.Join(ErrInvalidCommittedEntry, fmt.Errorf("committed entry bytes differ from submitted entry"))
	}
	if committed.CurrentCatalogVersion != request.CurrentCatalogVersion || committed.HasCurrentCatalogVersion != request.HasCurrentCatalogVersion {
		return CommittedCommandEntryV1{}, errors.Join(ErrInvalidCommittedEntry, fmt.Errorf("committed catalog guard differs from submit request"))
	}
	if committed.SyncLocalCommandWAL != request.SyncLocalCommandWAL {
		return CommittedCommandEntryV1{}, errors.Join(ErrInvalidCommittedEntry, fmt.Errorf("committed local command-WAL sync flag differs from submit request"))
	}
	if !requestMetadataEqual(committed.RequestMetadata, request.RequestMetadata) {
		return CommittedCommandEntryV1{}, errors.Join(ErrInvalidCommittedEntry, fmt.Errorf("committed request metadata differs from submit request"))
	}
	if !expectedTargetsEqual(committed.ExpectedTarget, request.ExpectedTarget) {
		return CommittedCommandEntryV1{}, errors.Join(ErrUnexpectedCommittedTarget, fmt.Errorf("committed target differs from submit request"))
	}
	return committed, nil
}

func actualSubmitAck(requested iwire.AckPolicy) (iwire.AckPolicy, error) {
	switch requested {
	case 0:
		return iwire.AckVisible, nil
	case iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced, iwire.AckRaftCommitted:
		return requested, nil
	default:
		return 0, errors.Join(ErrUnsupportedSubmitAck, fmt.Errorf("ack policy %d", requested))
	}
}

// SequencedCommitSource is a deterministic in-process CommitSource. By
// default it returns deterministic-harness evidence; ProductionConsensus must
// be set by a caller that is wrapping an actual quorum source or by tests that
// are explicitly exercising downstream raft_committed gates.
type SequencedCommitSource struct {
	mu sync.Mutex

	groupID             GroupID
	nodeID              NodeID
	leaderID            NodeID
	term                uint64
	nextIndex           uint64
	evidenceKind        CommitEvidenceKindV1
	productionConsensus bool
}

type SequencedCommitSourceOptions struct {
	GroupID             GroupID
	NodeID              NodeID
	LeaderID            NodeID
	Term                uint64
	FirstIndex          uint64
	EvidenceKind        CommitEvidenceKindV1
	ProductionConsensus bool
}

func NewSequencedCommitSource(opts SequencedCommitSourceOptions) *SequencedCommitSource {
	term := opts.Term
	if term == 0 {
		term = 1
	}
	firstIndex := opts.FirstIndex
	if firstIndex == 0 {
		firstIndex = 1
	}
	kind := opts.EvidenceKind
	if kind == "" {
		kind = CommitEvidenceDeterministicHarnessV1
	}
	return &SequencedCommitSource{
		groupID:             opts.GroupID,
		nodeID:              opts.NodeID,
		leaderID:            opts.LeaderID,
		term:                term,
		nextIndex:           firstIndex,
		evidenceKind:        kind,
		productionConsensus: opts.ProductionConsensus,
	}
}

func (s *SequencedCommitSource) CommitCommandEntryV1(ctx context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
	if s == nil {
		return CommitCommandEntryV1Result{}, ErrInvalidSubmitter
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return CommitCommandEntryV1Result{}, ctx.Err()
	default:
	}
	request := req.Clone()
	s.mu.Lock()
	term := s.term
	index := s.nextIndex
	s.nextIndex++
	groupID := s.groupID
	if groupID == "" {
		groupID = request.GroupID
	}
	nodeID := s.nodeID
	if nodeID == "" {
		nodeID = request.NodeID
	}
	leaderID := s.leaderID
	if leaderID == "" {
		leaderID = nodeID
	}
	kind := s.evidenceKind
	productionConsensus := s.productionConsensus
	s.mu.Unlock()

	entry := CommittedCommandEntryV1{
		Term:                     term,
		Index:                    index,
		Bytes:                    bytes.Clone(request.EntryBytes),
		CurrentCatalogVersion:    request.CurrentCatalogVersion,
		HasCurrentCatalogVersion: request.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      request.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataV1(request.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(request.ExpectedTarget),
	}
	return CommitCommandEntryV1Result{
		Entry: entry,
		Evidence: CommitEvidenceV1{
			Kind:                kind,
			GroupID:             groupID,
			NodeID:              nodeID,
			LeaderID:            leaderID,
			Term:                term,
			Index:               index,
			Committed:           true,
			ProductionConsensus: productionConsensus,
		},
	}, nil
}

func cloneRequestMetadataV1(meta raftentry.RequestMetadataV1) raftentry.RequestMetadataV1 {
	meta.TraceContext = bytes.Clone(meta.TraceContext)
	meta.ClusterRouteMembers = slices.Clone(meta.ClusterRouteMembers)
	return meta
}

func cloneExpectedTargetV1(target *raftentry.TargetIdentityV1) *raftentry.TargetIdentityV1 {
	if target == nil {
		return nil
	}
	cloned := target.Clone()
	return &cloned
}

func expectedTargetsEqual(a, b *raftentry.TargetIdentityV1) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}

func requestMetadataEqual(a, b raftentry.RequestMetadataV1) bool {
	return a.RequestID == b.RequestID &&
		a.AckPolicy == b.AckPolicy &&
		a.DeadlineUnixNanos == b.DeadlineUnixNanos &&
		bytes.Equal(a.TraceContext, b.TraceContext) &&
		a.Compression == b.Compression &&
		a.OmitResultIDs == b.OmitResultIDs &&
		a.OmitResponseMeta == b.OmitResponseMeta &&
		a.ClusterRouteKnown == b.ClusterRouteKnown &&
		a.ClusterRouteDatabase == b.ClusterRouteDatabase &&
		a.ClusterRouteCatalog == b.ClusterRouteCatalog &&
		a.ClusterRouteCollection == b.ClusterRouteCollection &&
		a.ClusterRouteShape == b.ClusterRouteShape &&
		a.ClusterRouteGroupID == b.ClusterRouteGroupID &&
		slices.Equal(a.ClusterRouteMembers, b.ClusterRouteMembers) &&
		a.ClusterRouteLeaderHint == b.ClusterRouteLeaderHint &&
		a.ClusterRoutePlacementMode == b.ClusterRoutePlacementMode &&
		a.ClusterRouteTokenKnown == b.ClusterRouteTokenKnown &&
		a.ClusterRouteToken == b.ClusterRouteToken &&
		a.ClusterRoutePartitionID == b.ClusterRoutePartitionID
}
