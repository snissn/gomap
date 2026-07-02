package raftcluster

import (
	"bytes"
	"context"
	"encoding/binary"
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
	ErrCommitAmbiguous           = errors.New("raftcluster: commit ambiguous")
	ErrCommitNotProven           = errors.New("raftcluster: commit not proven")
	ErrLocalApplyNotRecoverable  = errors.New("raftcluster: local apply not recoverable")
	ErrLocalAckUnavailable       = errors.New("raftcluster: local ack policy unavailable")
	ErrInvalidCommittedEntry     = errors.New("raftcluster: invalid committed entry")
	ErrUnsupportedSubmitAck      = errors.New("raftcluster: unsupported submit ack")
	ErrMissingCatalogVersion     = errors.New("raftcluster: missing catalog version")
	ErrCatalogVersionMismatch    = errors.New("raftcluster: catalog version mismatch")
	ErrUnexpectedCommittedTarget = errors.New("raftcluster: committed target mismatch")
	ErrRouteGroupMismatch        = errors.New("raftcluster: route group mismatch")
	ErrRouteTargetMissing        = errors.New("raftcluster: route target missing")
	ErrRouteTargetUnknown        = errors.New("raftcluster: route target unknown")
	ErrRouteTargetUnsupported    = errors.New("raftcluster: route target unsupported")
	ErrRouteFanoutRequired       = errors.New("raftcluster: route fanout required")
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

// CommandEntryPreflightRequestV1 asks the local deterministic apply provider
// to reject commands that cannot apply against the current local catalog and
// collection state before the commit source assigns a Raft log identity.
type CommandEntryPreflightRequestV1 struct {
	GroupID GroupID
	NodeID  NodeID

	EntryBytes   []byte
	DecodedEntry raftentry.CommandEntryV1

	CurrentCatalogVersion    uint64
	HasCurrentCatalogVersion bool
	SyncLocalCommandWAL      bool
	RequestMetadata          raftentry.RequestMetadataV1
	ExpectedTarget           *raftentry.TargetIdentityV1
}

func (r CommandEntryPreflightRequestV1) Clone() CommandEntryPreflightRequestV1 {
	r.EntryBytes = bytes.Clone(r.EntryBytes)
	r.DecodedEntry = cloneCommandEntryV1(r.DecodedEntry)
	r.RequestMetadata = cloneRequestMetadataV1(r.RequestMetadata)
	r.ExpectedTarget = cloneExpectedTargetV1(r.ExpectedTarget)
	return r
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

// CommandEntryPreflightV1 validates deterministic/catalog apply acceptability
// before the entry is admitted to the commit source. Implementations must treat
// the request as read-only and clone any data retained after return.
type CommandEntryPreflightV1 interface {
	PreflightCommandEntryV1(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error)
}

type CommandEntryPreflightResultV1 struct {
	KnownIdempotencyReplay bool
}

type CommandEntryPreflightFunc func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error)

func (f CommandEntryPreflightFunc) PreflightCommandEntryV1(ctx context.Context, req CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
	if f == nil {
		return CommandEntryPreflightResultV1{}, ErrInvalidSubmitter
	}
	return f(ctx, req)
}

// CommittedCommandApplierV1 is implemented by the local raftfsm adapter.
type CommittedCommandApplierV1 interface {
	ApplyCommittedCommandEntryV1(context.Context, CommittedCommandEntryV1) (raftentry.ApplyResultV1, error)
}

// InitialIndexGapSupportV1 is implemented by durable appliers that can report
// whether their first applied Raft command may start above index 1.
type InitialIndexGapSupportV1 interface {
	AllowsInitialIndexGapV1() bool
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
	Preflight              CommandEntryPreflightV1
	Applier                CommittedCommandApplierV1
	CatalogVersionProvider CatalogVersionProvider

	DecodeLimits  iwire.Limits
	ScopeRule     raftentry.ScopeRuleV1
	DatabaseScope string
	CatalogScope  string

	// DisableLocalCommandWALSync is for tests that intentionally model weaker
	// local recovery. The default bridge syncs local command-WAL coverage before
	// reporting CommittedRecoverable; when disabled, AckRaftCommitted is rejected.
	DisableLocalCommandWALSync bool
}

type SingleGroupSubmitter struct {
	submitMu sync.Mutex

	cluster         ResolvedConfig
	admission       AdmissionProvider
	commit          CommitSource
	preflight       CommandEntryPreflightV1
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

// CommandSubmitterV1 is the in-process boundary for submitting deterministic
// command entries. Single-group submitters and group-routed dispatchers both
// implement this interface.
type CommandSubmitterV1 interface {
	SubmitCommandEntryV1(context.Context, []byte, raftentry.RequestMetadataV1) (SubmitResultV1, error)
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
	if opts.Preflight == nil {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("command preflight provider is required"))
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
		preflight:       opts.Preflight,
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
	if s == nil || s.commit == nil || s.preflight == nil || s.applier == nil || s.catalogProvider == nil {
		return SubmitResultV1{}, ErrInvalidSubmitter
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.admit(ctx); err != nil {
		return SubmitResultV1{}, err
	}
	if err := s.checkRouteBinding(metadata); err != nil {
		return SubmitResultV1{}, err
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
	actualAck, err := actualSubmitAck(metadata.AckPolicy, s.syncLocalWAL)
	if err != nil {
		return SubmitResultV1{}, err
	}
	expectedTarget := decoded.Target.Clone()

	s.submitMu.Lock()
	catalogVersion, hasCatalogVersion, err := s.catalogProvider.CurrentCatalogVersion(ctx)
	if err != nil {
		s.submitMu.Unlock()
		return SubmitResultV1{}, errors.Join(ErrMissingCatalogVersion, err)
	}
	if !hasCatalogVersion {
		s.submitMu.Unlock()
		return SubmitResultV1{}, ErrMissingCatalogVersion
	}
	guardErr := checkSubmitCatalogGuardV1(decoded, catalogVersion)
	allowStaleCreateRetry := allowPreflightIdempotentCreateRetryV1(decoded, guardErr)
	if guardErr != nil && !allowStaleCreateRetry {
		s.submitMu.Unlock()
		return SubmitResultV1{}, guardErr
	}
	preflightRequest := CommandEntryPreflightRequestV1{
		GroupID:                  s.cluster.GroupID,
		NodeID:                   s.cluster.NodeID,
		EntryBytes:               entry,
		DecodedEntry:             decoded,
		CurrentCatalogVersion:    catalogVersion,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      s.syncLocalWAL,
		RequestMetadata:          metadata,
		ExpectedTarget:           &expectedTarget,
	}
	preflightResult, err := s.preflight.PreflightCommandEntryV1(ctx, preflightRequest)
	if err != nil {
		s.submitMu.Unlock()
		if allowStaleCreateRetry {
			return SubmitResultV1{}, guardErr
		}
		return SubmitResultV1{}, err
	}
	if allowStaleCreateRetry && !preflightResult.KnownIdempotencyReplay {
		s.submitMu.Unlock()
		return SubmitResultV1{}, guardErr
	}
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
		s.submitMu.Unlock()
		return SubmitResultV1{}, err
	}
	committed, err := s.validateCommitResult(request, commitResult)
	if err != nil {
		s.submitMu.Unlock()
		return SubmitResultV1{}, err
	}
	applyResult, err := s.applier.ApplyCommittedCommandEntryV1(ctx, committed.Clone())
	if err != nil {
		s.submitMu.Unlock()
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, err)
	}
	if applyResult.Status != raftentry.ApplyStatusApplied && applyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		s.submitMu.Unlock()
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, fmt.Errorf("apply status %s", applyResult.Status))
	}
	postCatalogVersion, postHasCatalogVersion, err := s.catalogProvider.CurrentCatalogVersion(ctx)
	if err != nil {
		s.submitMu.Unlock()
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, err)
	}
	if !postHasCatalogVersion {
		s.submitMu.Unlock()
		return SubmitResultV1{ApplyResult: applyResult, CommittedEntry: committed, Evidence: commitResult.Evidence}, errors.Join(ErrLocalApplyNotRecoverable, ErrMissingCatalogVersion)
	}
	result := SubmitResultV1{
		ActualAck:            actualAck,
		CommittedRecoverable: actualAck == iwire.AckRaftCommitted && s.syncLocalWAL,
		DecodedEntry:         decoded,
		ApplyResult:          applyResult,
		CommittedEntry:       committed,
		Evidence:             commitResult.Evidence,
		CatalogVersion:       postCatalogVersion,
		HasCatalogVersion:    postHasCatalogVersion,
	}
	s.submitMu.Unlock()
	return result, nil
}

func allowPreflightIdempotentCreateRetryV1(entry raftentry.CommandEntryV1, guardErr error) bool {
	return guardErr != nil &&
		errors.Is(guardErr, ErrCatalogVersionMismatch) &&
		canPreflightIdempotentCreateRetryV1(entry)
}

func canPreflightIdempotentCreateRetryV1(entry raftentry.CommandEntryV1) bool {
	return entry.Target.CommandID == iwire.CommandCreateCollection &&
		entry.Idempotency == raftentry.IdempotencyRequiredV1 &&
		len(entry.IdempotencyKey) > 0
}

func checkSubmitCatalogGuardV1(entry raftentry.CommandEntryV1, catalogVersion uint64) error {
	expected, err := decodeSubmitExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
	if err != nil {
		return err
	}
	if catalogVersion != expected {
		return errors.Join(ErrCatalogVersionMismatch, fmt.Errorf("catalog version %d does not match expected %d", catalogVersion, expected))
	}
	return nil
}

func decodeSubmitExpectedCatalogVersionV1(raw []byte) (uint64, error) {
	if len(raw) == 0 {
		return 0, errors.Join(ErrInvalidCommittedEntry, &raftentry.ValidationError{Code: raftentry.ErrorMissingGuardV1, Err: fmt.Errorf("raftcluster: missing expected catalog version")})
	}
	value, n := binary.Uvarint(raw)
	if n <= 0 {
		return 0, errors.Join(ErrInvalidCommittedEntry, &raftentry.ValidationError{Code: raftentry.ErrorMalformedEntryV1, Err: fmt.Errorf("raftcluster: malformed expected catalog version")})
	}
	if n != len(raw) {
		return 0, errors.Join(ErrInvalidCommittedEntry, &raftentry.ValidationError{Code: raftentry.ErrorMalformedEntryV1, Err: fmt.Errorf("raftcluster: expected catalog version has %d trailing bytes", len(raw)-n)})
	}
	return value, nil
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

func (s *SingleGroupSubmitter) checkRouteBinding(metadata raftentry.RequestMetadataV1) error {
	if !metadata.ClusterRouteKnown {
		return nil
	}
	localGroup := string(s.cluster.GroupID)
	if metadata.ClusterRouteGroupID == "" {
		return errors.Join(ErrRouteGroupMismatch, fmt.Errorf("route metadata missing group id for local group %q", localGroup))
	}
	if metadata.ClusterRouteGroupID != localGroup {
		return errors.Join(ErrRouteGroupMismatch, routeErrorWithLeaderHint(metadata, "route group %q does not match local group %q", metadata.ClusterRouteGroupID, localGroup))
	}
	return nil
}

func routeErrorWithLeaderHint(metadata raftentry.RequestMetadataV1, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if metadata.ClusterRouteLeaderHint == "" {
		return errors.New(msg)
	}
	return &routeLeaderHintError{message: msg, leaderHint: metadata.ClusterRouteLeaderHint}
}

type routeLeaderHintError struct {
	message    string
	leaderHint string
}

func (e *routeLeaderHintError) Error() string {
	if e == nil {
		return ""
	}
	return e.message + "; leader_hint=" + e.leaderHint
}

func (e *routeLeaderHintError) ClusterLeaderHint() string {
	if e == nil {
		return ""
	}
	return e.leaderHint
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

func actualSubmitAck(requested iwire.AckPolicy, syncLocalWAL bool) (iwire.AckPolicy, error) {
	switch requested {
	case 0:
		return iwire.AckVisible, nil
	case iwire.AckVisible:
		return requested, nil
	case iwire.AckRaftCommitted:
		if !syncLocalWAL {
			return 0, errors.Join(ErrLocalAckUnavailable, fmt.Errorf("ack policy %d requires local command-WAL sync", requested))
		}
		return requested, nil
	case iwire.AckFlushed, iwire.AckSynced:
		return 0, errors.Join(ErrLocalAckUnavailable, fmt.Errorf("ack policy %d requires a local flush/sync barrier", requested))
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

func cloneCommandEntryV1(entry raftentry.CommandEntryV1) raftentry.CommandEntryV1 {
	entry.Bytes = bytes.Clone(entry.Bytes)
	entry.Decoded.Sections = cloneSections(entry.Decoded.Sections)
	entry.Target = entry.Target.Clone()
	entry.IdempotencyKey = bytes.Clone(entry.IdempotencyKey)
	return entry
}

func cloneSections(sections []iwire.Section) []iwire.Section {
	if len(sections) == 0 {
		return nil
	}
	out := make([]iwire.Section, len(sections))
	for i := range sections {
		out[i] = sections[i]
		out[i].Bytes = bytes.Clone(sections[i].Bytes)
	}
	return out
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
		a.ClusterRouteKey == b.ClusterRouteKey &&
		a.ClusterRouteTokenKnown == b.ClusterRouteTokenKnown &&
		a.ClusterRouteToken == b.ClusterRouteToken &&
		a.ClusterRoutePartitionID == b.ClusterRoutePartitionID
}
