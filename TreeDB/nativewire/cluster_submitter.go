package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// ClusterRequestMetadata carries request-scoped fields that influence response
// policy but are intentionally excluded from deterministic command entry bytes.
type ClusterRequestMetadata = raftentry.RequestMetadataV1

// ClusterSubmitter accepts deterministic native-wire CommandEntryV1 bytes for
// cluster-owned mutation admission. Submitters must copy entry bytes or response
// sections before retaining them after SubmitCommandEntryV1 returns.
type ClusterSubmitter interface {
	SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata ClusterRequestMetadata) (ClusterSubmitResult, error)
}

// ClusterSubmitterWithPreCommitV1 is the M7-safe submit extension. The callback
// must run after the data layer's deterministic preflight succeeds and before
// the command is offered to consensus, under the same serialization boundary.
type ClusterSubmitterWithPreCommitV1 interface {
	SubmitCommandEntryWithPreCommitV1(ctx context.Context, entry []byte, metadata ClusterRequestMetadata, preCommit func(context.Context) error) (ClusterSubmitResult, error)
}

const committedVectorPartitionMutationConfirmationTimeoutV1 = 30 * time.Second

// ClusterRouteProvider is an optional ClusterSubmitter extension for
// collection-level route preflight. Providers should use catalog-derived route
// decisions only; leader hints are metadata and are not live leadership proof.
type ClusterRouteProvider interface {
	ClusterRoute(ctx context.Context, request ClusterRouteRequest) (ClusterRouteTarget, error)
}

type ClusterRouteShape string

const (
	ClusterRouteShapeCollection ClusterRouteShape = "collection"
	ClusterRouteShapeToken      ClusterRouteShape = "token"
	ClusterRouteShapeTokenBatch ClusterRouteShape = "token_batch"
	ClusterRouteShapeQuery      ClusterRouteShape = "query"
)

type ClusterRouteRequest struct {
	Database    string
	Catalog     string
	Collection  string
	CommandID   iwire.CommandID
	CommandName string
	Shape       ClusterRouteShape
	TokenKnown  bool
	Token       uint64
	Tokens      []uint64
}

type ClusterRouteTarget struct {
	GroupID           string
	Members           []string
	LeaderHint        string
	PlacementMode     string
	RouteKey          string
	Reason            string
	Shape             ClusterRouteShape
	TokenKnown        bool
	Token             uint64
	PartitionID       string
	TokenBatchClass   string
	CatalogMetaEpoch  uint64
	CatalogMetaDigest string
}

// ClusterAdmissionProvider exposes the node write-admission state backing a
// ClusterSubmitter. A configured cluster submitter that does not implement
// this interface fails closed as admission-unavailable.
type ClusterAdmissionProvider interface {
	ClusterAdmissionStatus(ctx context.Context) (ClusterAdmissionStatus, error)
}

// VectorPartitionMutationAdmissionProviderV1 is the M7 extension point for
// submitters that own both a catalog/meta lifecycle coordinator and collection
// schema knowledge. It receives the already registry-validated, bounded
// native-wire command before deterministic entry encoding or Raft submission.
// Implementations classify vector-field and DDL effects conservatively and
// must return only after invalidation-before-mutation is durably proven.
//
// The interface deliberately receives command sections instead of a decoded
// document graph: classification remains at the shared submit boundary used by
// nativewire, Mongo, retry, and replay callers, while the registry's existing
// bounds remain the sole wire-size authority.
type VectorPartitionMutationAdmissionProviderV1 interface {
	AdmitVectorPartitionMutationV1(context.Context, iwire.CommandID, []iwire.Section) error
}

// VectorPartitionMutationCommitProviderV1 is called only after the data
// command has definitively committed and completed deterministic local apply.
// It releases the replicated lifecycle freeze installed by admission;
// ambiguous or failed submissions deliberately do not call it.
type VectorPartitionMutationCommitProviderV1 interface {
	ConfirmVectorPartitionMutationV1(context.Context, iwire.CommandID, []iwire.Section) error
}

// VectorPartitionMutationLifecycleProviderV1 is the complete M7 mutation
// lifecycle contract. Production wiring must provide both admission and
// post-commit confirmation so a committed data mutation cannot strand its
// replicated lifecycle fence in the pending state.
type VectorPartitionMutationLifecycleProviderV1 interface {
	VectorPartitionMutationAdmissionProviderV1
	VectorPartitionMutationCommitProviderV1
}

// VectorPartitionMutationLifecycleReadinessV1 lets a concrete submitter fail
// closed when it exposes lifecycle methods but their backing coordinator is
// not configured.
type VectorPartitionMutationLifecycleReadinessV1 interface {
	ValidateVectorPartitionMutationLifecycleV1() error
}

// VectorPartitionMutationAdmissionRequiredV1 marks an M7-enabled shared
// submitter. It prevents a wiring error from degrading an active lifecycle to
// the legacy optional path: a required submitter without an admission provider
// is rejected before deterministic entry encoding.
type VectorPartitionMutationAdmissionRequiredV1 interface {
	RequiresVectorPartitionMutationAdmissionV1(context.Context) (bool, error)
}

// AdmitVectorPartitionMutationV1 invokes the optional M7 admission extension.
// A plain cluster submitter remains valid for non-vector deployments; an M7
// deployment installs this on its shared submitter so no frontend can bypass
// classification or durable invalidation.
func AdmitVectorPartitionMutationV1(ctx context.Context, submitter ClusterSubmitter, command iwire.CommandID, sections []iwire.Section) error {
	if submitter == nil {
		return protocolError(iwire.ErrInvalidCommand, "nativewire cluster submitter is not configured")
	}
	if required, ok := submitter.(VectorPartitionMutationAdmissionRequiredV1); ok {
		enabled, err := required.RequiresVectorPartitionMutationAdmissionV1(ctx)
		if err != nil {
			return err
		}
		if !enabled {
			return nil
		}
	}
	admitter, ok := submitter.(VectorPartitionMutationAdmissionProviderV1)
	if !ok {
		if _, required := submitter.(VectorPartitionMutationAdmissionRequiredV1); required {
			return protocolError(iwire.ErrReadOnly, "vector partition lifecycle admission is required but not configured")
		}
		return nil
	}
	return admitter.AdmitVectorPartitionMutationV1(ctx, command, cloneSections(sections))
}

// SubmitCommandEntryWithVectorPartitionAdmissionV1 orders lifecycle admission
// inside the data submitter's deterministic preflight-to-commit boundary for
// an M7-required deployment. A definite data-preflight rejection therefore
// cannot publish a pending lifecycle barrier. Legacy optional admission remains
// supported for non-M7 test and integration submitters.
func SubmitCommandEntryWithVectorPartitionAdmissionV1(ctx context.Context, submitter ClusterSubmitter, command iwire.CommandID, sections []iwire.Section, entry []byte, metadata ClusterRequestMetadata) (ClusterSubmitResult, error) {
	if submitter == nil {
		return ClusterSubmitResult{}, protocolError(iwire.ErrInvalidCommand, "nativewire cluster submitter is not configured")
	}
	required, hasRequirement := submitter.(VectorPartitionMutationAdmissionRequiredV1)
	if !hasRequirement {
		if err := AdmitVectorPartitionMutationV1(ctx, submitter, command, sections); err != nil {
			return ClusterSubmitResult{}, err
		}
		return submitter.SubmitCommandEntryV1(ctx, entry, metadata)
	}
	enabled, err := required.RequiresVectorPartitionMutationAdmissionV1(ctx)
	if err != nil {
		return ClusterSubmitResult{}, err
	}
	if !enabled {
		return submitter.SubmitCommandEntryV1(ctx, entry, metadata)
	}
	if readiness, ok := submitter.(VectorPartitionMutationLifecycleReadinessV1); ok {
		if err := readiness.ValidateVectorPartitionMutationLifecycleV1(); err != nil {
			return ClusterSubmitResult{}, err
		}
	}
	if _, ok := submitter.(VectorPartitionMutationLifecycleProviderV1); !ok {
		return ClusterSubmitResult{}, protocolError(iwire.ErrReadOnly, "vector partition lifecycle admission and confirmation are required but not configured")
	}
	atomicSubmitter, ok := submitter.(ClusterSubmitterWithPreCommitV1)
	if !ok {
		return ClusterSubmitResult{}, protocolError(iwire.ErrReadOnly, "vector partition lifecycle requires serialized data preflight and admission")
	}
	return atomicSubmitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, func(callbackCtx context.Context) error {
		return AdmitVectorPartitionMutationV1(callbackCtx, submitter, command, sections)
	})
}

func ConfirmVectorPartitionMutationV1(ctx context.Context, submitter ClusterSubmitter, command iwire.CommandID, sections []iwire.Section) error {
	if required, ok := submitter.(VectorPartitionMutationAdmissionRequiredV1); ok {
		enabled, err := required.RequiresVectorPartitionMutationAdmissionV1(ctx)
		if err != nil {
			return err
		}
		if !enabled {
			return nil
		}
	}
	confirmer, ok := submitter.(VectorPartitionMutationCommitProviderV1)
	if !ok {
		if _, required := submitter.(VectorPartitionMutationAdmissionRequiredV1); required {
			return protocolError(iwire.ErrReadOnly, "vector partition lifecycle mutation confirmation is required but not configured")
		}
		return nil
	}
	return confirmer.ConfirmVectorPartitionMutationV1(ctx, command, cloneSections(sections))
}

// ConfirmCommittedVectorPartitionMutationV1 completes the lifecycle fence
// after the data command is known committed and applied. At that point client
// cancellation must not strand durable admission state, so confirmation uses
// an internally bounded context detached from the request cancellation and
// deadline while retaining request values for observability.
func ConfirmCommittedVectorPartitionMutationV1(ctx context.Context, submitter ClusterSubmitter, command iwire.CommandID, sections []iwire.Section) error {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	confirmCtx, cancel := context.WithTimeout(base, committedVectorPartitionMutationConfirmationTimeoutV1)
	defer cancel()
	return ConfirmVectorPartitionMutationV1(confirmCtx, submitter, command, sections)
}

// ClusterAdmissionStatus describes whether this node may accept cluster-owned
// writes. A provider must set Leader for write admission; the zero value fails
// closed as not-leader when returned by a configured provider.
type ClusterAdmissionStatus struct {
	Leader      bool
	Unavailable bool
	LeaderHint  string
	Reason      string
}

// ClusterLeaderAdmission returns an admitted leader status.
func ClusterLeaderAdmission() ClusterAdmissionStatus {
	return ClusterAdmissionStatus{Leader: true}
}

// ClusterFollowerAdmission returns a fail-closed not-leader status. leaderHint
// is optional and may carry a redirect target for callers that can use one.
func ClusterFollowerAdmission(leaderHint, reason string) ClusterAdmissionStatus {
	return ClusterAdmissionStatus{LeaderHint: leaderHint, Reason: reason}
}

// ClusterUnavailableAdmission returns a fail-closed cluster-unavailable status.
func ClusterUnavailableAdmission(reason string) ClusterAdmissionStatus {
	return ClusterAdmissionStatus{Unavailable: true, Reason: reason}
}

// ClusterSubmitResult is the submitter-owned response for an admitted command.
// CommittedRecoverable must only be true when AckRaftCommitted reflects a real
// consensus commit plus the serving node's selected local recoverability gate.
type ClusterSubmitResult struct {
	ActualAck            AckPolicy
	CommittedRecoverable bool
	// CommittedApplied is submitter-owned post-commit/apply evidence used only
	// for lifecycle confirmation. Unlike CommittedRecoverable it does not
	// depend on the acknowledgment policy selected by the client.
	CommittedApplied bool
	ResponseSections []iwire.Section
	// CatalogVersion is internal post-apply evidence. It lets response shaping
	// omit response_meta without hiding catalog-cache progress from the server.
	CatalogVersion    uint64
	HasCatalogVersion bool
}

func (s *Server) handleClusterMutation(ctx context.Context, header iwire.Header, cmd iwire.ValidatedCommand) (sections []iwire.Section, err error) {
	start := time.Now()
	var actualAck AckPolicy
	if s != nil {
		s.counters.inc("cluster_submit.requests_total")
		defer func() {
			if recovered := recover(); recovered != nil {
				s.recordClusterSubmitOutcome(fmt.Errorf("cluster submit panic: %v", recovered), actualAck, time.Since(start))
				panic(recovered)
			}
			s.recordClusterSubmitOutcome(err, actualAck, time.Since(start))
		}()
	}
	if s == nil || s.clusterSubmitter == nil {
		return nil, protocolError(iwire.ErrInvalidCommand, "nativewire cluster submitter is not configured")
	}
	ack, err := ackPolicyFromSections(cmd.Known, s.defaultAckPolicy)
	if err != nil {
		return nil, err
	}
	if err := AdmitClusterMutation(ctx, s.clusterSubmitter); err != nil {
		return nil, err
	}
	row := raftentry.ClassifyNativeWireCommandV1(cmd.Header.ID)
	if !row.Known || row.Decision != raftentry.DecisionAccepted {
		return nil, unsupportedClusterMutationError(cmd, row)
	}
	metadata, err := clusterRequestMetadata(header, cmd.Header, cmd.Known, ack)
	if err != nil {
		return nil, err
	}
	entry, err := iwire.AppendDeterministicEntryWithLimits(nil, cmd, s.limits)
	if err != nil {
		return nil, err
	}
	if _, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{
		Limits:          s.limits,
		RequestMetadata: metadata,
	}); err != nil {
		return nil, nativeErrorForRaftEntryValidation(err)
	}
	var routeReq ClusterRouteRequest
	var route ClusterRouteTarget
	var routed bool
	if _, ok := s.clusterSubmitter.(ClusterRouteProvider); ok {
		var err error
		routeReq, err = clusterMutationRouteRequest(cmd, s.limits)
		if err != nil {
			return nil, err
		}
		route, routed, err = PreflightClusterRoute(ctx, s.clusterSubmitter, routeReq)
		if err != nil {
			return nil, err
		}
	}
	if routed {
		if err := s.rejectClusterTokenRouteIndexedMutation(cmd.Header.ID, routeReq, route); err != nil {
			return nil, err
		}
		ApplyClusterRouteMetadata(&metadata, routeReq, route)
	}
	// M7-required admission runs inside the submitter after deterministic data
	// preflight and immediately before commit. This prevents a definite
	// idempotency/schema rejection from stranding a replicated lifecycle fence.
	result, submitErr := SubmitCommandEntryWithVectorPartitionAdmissionV1(ctx, s.clusterSubmitter, cmd.Header.ID, cmd.Known, entry, metadata)
	if result.CommittedApplied {
		if err := ConfirmCommittedVectorPartitionMutationV1(ctx, s.clusterSubmitter, cmd.Header.ID, cmd.Known); err != nil {
			return nil, errors.Join(submitErr, err)
		}
	}
	if submitErr != nil {
		return nil, submitErr
	}
	if err := validateClusterSubmitResult(metadata, result); err != nil {
		return nil, err
	}
	if err := s.updateCatalogVersionFromClusterSubmitResult(result); err != nil {
		return nil, err
	}
	actualAck = result.ActualAck
	return cloneSections(result.ResponseSections), nil
}

func (s *Server) recordClusterSubmitOutcome(err error, actualAck AckPolicy, elapsed time.Duration) {
	if s == nil {
		return
	}
	if elapsed > 0 {
		s.counters.add("cluster_submit.nanos_total", uint64(elapsed.Nanoseconds()))
	}
	if err != nil {
		s.counters.inc("cluster_submit.errors_total")
		switch errorCodeFor(err) {
		case iwire.ErrReadOnly:
			s.counters.inc("cluster_submit.read_only_total")
		case iwire.ErrDurabilityUnavailable:
			s.counters.inc("cluster_submit.durability_unavailable_total")
		case iwire.ErrCommitAmbiguous:
			s.counters.inc("cluster_submit.commit_ambiguous_total")
		}
		return
	}
	s.counters.inc("cluster_submit.success_total")
	switch actualAck {
	case iwire.AckVisible:
		s.counters.inc("cluster_submit.ack_visible_total")
	case iwire.AckFlushed:
		s.counters.inc("cluster_submit.ack_flushed_total")
	case iwire.AckSynced:
		s.counters.inc("cluster_submit.ack_synced_total")
	case iwire.AckRaftCommitted:
		s.counters.inc("cluster_submit.ack_raft_committed_total")
	}
}

func (s *Server) updateCatalogVersionFromClusterSubmitResult(result ClusterSubmitResult) error {
	if result.HasCatalogVersion {
		s.advanceCatalogVersion(result.CatalogVersion)
		return nil
	}
	version, ok, err := catalogVersionFromResponseMeta(result.ResponseSections)
	if err != nil {
		return err
	}
	if ok {
		s.advanceCatalogVersion(version)
	}
	return nil
}

func (s *Server) advanceCatalogVersion(version uint64) {
	if s == nil {
		return
	}
	for {
		current := s.catalogVersion.Load()
		if version <= current {
			return
		}
		if s.catalogVersion.CompareAndSwap(current, version) {
			return
		}
	}
}

// AdmitClusterMutation fails closed unless submitter is configured and, when
// it exposes admission state, the node is currently the leader.
func AdmitClusterMutation(ctx context.Context, submitter ClusterSubmitter) error {
	if submitter == nil {
		return protocolError(iwire.ErrInvalidCommand, "cluster submitter is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	provider, ok := submitter.(ClusterAdmissionProvider)
	if !ok {
		return protocolError(iwire.ErrDurabilityUnavailable, "cluster admission provider is not configured")
	}
	status, err := provider.ClusterAdmissionStatus(ctx)
	if err != nil {
		return protocolError(iwire.ErrDurabilityUnavailable, "cluster admission unavailable: %v", err)
	}
	if status.Unavailable {
		reason := status.Reason
		if reason == "" {
			reason = "cluster admission is unavailable"
		}
		return protocolError(iwire.ErrDurabilityUnavailable, "%s", reason)
	}
	if status.Leader {
		return nil
	}
	message := "not cluster leader"
	if status.Reason != "" {
		message = status.Reason
	}
	if status.LeaderHint != "" {
		message += "; leader_hint=" + status.LeaderHint
	}
	return protocolError(iwire.ErrReadOnly, "%s", message)
}

// PreflightClusterRoute checks the optional cluster route provider. A missing
// provider preserves existing submitter behavior; a configured provider must
// return a supported route target with a group ID. Token/ring targets require
// an exactly-one-ID token route request; multi-ID token batches are classified
// only so adapters can fail closed until split/fanout execution exists. Query
// routes fail closed until bounded scatter/read-index execution is implemented.
func PreflightClusterRoute(ctx context.Context, submitter ClusterSubmitter, request ClusterRouteRequest) (ClusterRouteTarget, bool, error) {
	provider, ok := submitter.(ClusterRouteProvider)
	if !ok {
		return ClusterRouteTarget{}, false, nil
	}
	request.Shape = normalizeClusterRouteShape(request.Shape)
	switch request.Shape {
	case ClusterRouteShapeCollection:
	case ClusterRouteShapeToken:
		if !request.TokenKnown {
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "cluster token route request missing document token")
		}
	case ClusterRouteShapeTokenBatch:
		if len(request.Tokens) < 2 {
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "cluster token batch route request requires multiple document tokens")
		}
		request.Tokens = append([]uint64(nil), request.Tokens...)
	case ClusterRouteShapeQuery:
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "cluster query route shape is not supported without bounded scatter/read-index routing")
	default:
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "cluster route shape %q is not supported", request.Shape)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	target, err := provider.ClusterRoute(ctx, request)
	if err != nil {
		return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
			iwire.ErrReadOnly,
			request,
			ClusterRouteTarget{},
			"route_provider_rejected",
			"cluster route provider rejected request",
		)
	}
	if target.PlacementMode == "" {
		return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
			iwire.ErrReadOnly,
			request,
			target,
			"invalid_target",
			"cluster route target missing collection placement mode",
		)
	}
	target.Shape = normalizeClusterRouteTargetShape(target.Shape, target.PlacementMode)
	if target.PlacementMode == "token" || target.PlacementMode == "ring" {
		if request.Shape == ClusterRouteShapeTokenBatch {
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", clusterTokenBatchRouteRejection(request, target))
		}
	}
	if target.GroupID == "" {
		return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
			iwire.ErrReadOnly,
			request,
			target,
			"missing_owner",
			"cluster route target missing group id",
		)
	}
	switch target.PlacementMode {
	case "collection":
		if target.Shape != ClusterRouteShapeCollection {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster collection route target must use collection route shape",
			)
		}
	case "token", "ring":
		if request.Shape != ClusterRouteShapeToken || !request.TokenKnown {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster token/ring route target requires exactly one document id",
			)
		}
		if target.Shape != ClusterRouteShapeToken {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster token/ring route target must use token route shape",
			)
		}
		if target.RouteKey == "" {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster token/ring route target missing route key",
			)
		}
		if target.RouteKey != string(raftplacement.RouteKeyDocumentIDV1) {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				fmt.Sprintf("cluster token/ring route target uses unsupported route key %q; requires _id", target.RouteKey),
			)
		}
		if !target.TokenKnown {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster token/ring route target missing document token",
			)
		}
		if target.Token != request.Token {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster token/ring route target token does not match request token",
			)
		}
		if target.PartitionID == "" {
			return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
				iwire.ErrReadOnly,
				request,
				target,
				"invalid_target",
				"cluster token/ring route target missing token partition id",
			)
		}
	default:
		return ClusterRouteTarget{}, true, clusterRouteTargetProtocolError(
			iwire.ErrReadOnly,
			request,
			target,
			"invalid_target",
			"cluster route target placement mode "+target.PlacementMode+" is not supported",
		)
	}
	target.Members = append([]string(nil), target.Members...)
	return target, true, nil
}

func clusterTokenBatchRouteRejection(request ClusterRouteRequest, target ClusterRouteTarget) string {
	class := target.TokenBatchClass
	if class == "" {
		class = "unclassified"
	}
	action := "command split"
	if class == string(raftplacement.TokenBatchRouteFanoutRequiredV1) {
		action = "fanout"
	}
	return "cluster token/ring multi-id write requires " + action + " before submit: route_class=" + class + " token_count=" + strconv.Itoa(len(request.Tokens))
}

func clusterRouteTargetProtocolError(code iwire.ErrorCode, request ClusterRouteRequest, target ClusterRouteTarget, class, reason string) error {
	route := redactClusterRouteErrorMetadata(clusterRouteErrorMetadataFromTarget(request, target, class))
	if fields := clusterRouteErrorMetadataFields(route); fields != "" {
		reason += "; " + fields
	}
	return &clusterRouteProtocolError{
		err:        protocolError(code, "%s", reason),
		leaderHint: route.LeaderHint,
		route:      route,
		hasRoute:   true,
	}
}

func clusterRouteErrorMetadataFromTarget(request ClusterRouteRequest, target ClusterRouteTarget, class string) ClusterRouteErrorMetadata {
	shape := string(target.Shape)
	if shape == "" {
		shape = string(request.Shape)
	}
	tokenKnown := target.TokenKnown
	token := target.Token
	if !tokenKnown && request.TokenKnown {
		tokenKnown = true
		token = request.Token
	}
	return ClusterRouteErrorMetadata{
		Class:         class,
		Database:      request.Database,
		Catalog:       request.Catalog,
		Collection:    request.Collection,
		Shape:         shape,
		GroupID:       target.GroupID,
		Members:       append([]string(nil), target.Members...),
		LeaderHint:    target.LeaderHint,
		PlacementMode: target.PlacementMode,
		RouteKey:      target.RouteKey,
		TokenKnown:    tokenKnown,
		Token:         token,
		PartitionID:   target.PartitionID,
	}
}

func normalizeClusterRouteShape(shape ClusterRouteShape) ClusterRouteShape {
	if shape == "" {
		return ClusterRouteShapeCollection
	}
	return shape
}

func normalizeClusterRouteTargetShape(shape ClusterRouteShape, placementMode string) ClusterRouteShape {
	if shape != "" {
		return shape
	}
	switch placementMode {
	case "token", "ring":
		return ClusterRouteShapeToken
	default:
		return ClusterRouteShapeCollection
	}
}

func (s *Server) rejectClusterLocalMutation(command string) error {
	if s == nil || s.clusterSubmitter == nil {
		return nil
	}
	return protocolError(iwire.ErrReadOnly, "nativewire cluster submitter mode requires %s through the cluster submitter", command)
}

func unsupportedClusterMutationError(cmd iwire.ValidatedCommand, row raftentry.CommandRowV1) error {
	name := "<unknown>"
	if cmd.Schema != nil {
		name = cmd.Schema.Name
	}
	reason := "command is not accepted by R3a v1"
	if row.Reason != "" {
		reason = row.Reason
	}
	return protocolError(iwire.ErrUnsupportedFeature, "cluster submitter does not support %s: %s", name, reason)
}

func clusterMutationRouteRequest(cmd iwire.ValidatedCommand, limits iwire.Limits) (ClusterRouteRequest, error) {
	name := ""
	if cmd.Schema != nil {
		name = cmd.Schema.Name
	}
	collection, err := clusterMutationRouteCollection(cmd)
	if err != nil {
		return ClusterRouteRequest{}, err
	}
	req := ClusterRouteRequest{
		Database:    "default",
		Catalog:     "default",
		Collection:  collection,
		CommandID:   cmd.Header.ID,
		CommandName: name,
		Shape:       ClusterRouteShapeCollection,
	}
	tokens, ok, err := clusterMutationDocumentTokens(cmd, limits)
	if err != nil {
		return ClusterRouteRequest{}, err
	}
	if ok && len(tokens) == 1 {
		req.Shape = ClusterRouteShapeToken
		req.TokenKnown = true
		req.Token = tokens[0]
	} else if len(tokens) > 1 {
		req.Shape = ClusterRouteShapeTokenBatch
		req.Tokens = tokens
	}
	return req, nil
}

func (s *Server) rejectClusterTokenRouteIndexedMutation(command iwire.CommandID, request ClusterRouteRequest, target ClusterRouteTarget) error {
	switch target.PlacementMode {
	case string(raftplacement.PlacementModeTokenV1), string(raftplacement.PlacementModeRingV1):
	default:
		return nil
	}
	switch command {
	case iwire.CommandInsertBatch,
		iwire.CommandReplaceBatch,
		iwire.CommandDeleteBatch,
		iwire.CommandUpdateBSONSet:
	default:
		return nil
	}
	return clusterRouteTargetProtocolError(
		iwire.ErrReadOnly,
		request,
		target,
		"index_policy_unbound",
		"cluster token/ring mutation is disabled until authoritative collection and index metadata is bound to the owner route proof",
	)
}

func (s *Server) rejectClusterRoutedLocalMetadataRead(command iwire.CommandID) error {
	if s == nil || s.clusterSubmitter == nil {
		return nil
	}
	if _, ok := s.clusterSubmitter.(ClusterRouteProvider); !ok {
		return nil
	}
	switch command {
	case iwire.CommandListCollections, iwire.CommandListIndexes, iwire.CommandOpenCollection:
		return protocolError(
			iwire.ErrReadOnly,
			"nativewire routed-cluster metadata read is disabled until authoritative catalog metadata is bound to the route provider",
		)
	default:
		return nil
	}
}

func clusterMutationDocumentToken(cmd iwire.ValidatedCommand, limits iwire.Limits) (uint64, bool, error) {
	tokens, ok, err := clusterMutationDocumentTokens(cmd, limits)
	if err != nil || !ok || len(tokens) != 1 {
		return 0, false, err
	}
	return tokens[0], true, nil
}

func clusterMutationDocumentTokens(cmd iwire.ValidatedCommand, limits iwire.Limits) ([]uint64, bool, error) {
	switch cmd.Header.ID {
	case iwire.CommandInsertBatch, iwire.CommandReplaceBatch, iwire.CommandDeleteBatch, iwire.CommandUpdateBSONSet:
	default:
		return nil, false, nil
	}
	raw, err := metadataSection(cmd.Known, iwire.SectionDocumentIDs)
	if err != nil {
		return nil, false, err
	}
	ids, err := iwire.DecodeByteVectorItems(raw, limits)
	if err != nil {
		return nil, false, err
	}
	if len(ids) == 0 {
		return nil, false, nil
	}
	tokens := make([]uint64, len(ids))
	for i, id := range ids {
		tokens[i] = raftplacement.DocumentIDTokenV1(id)
	}
	return tokens, true, nil
}

func clusterMutationRouteCollection(cmd iwire.ValidatedCommand) (string, error) {
	switch cmd.Header.ID {
	case iwire.CommandCreateCollection:
		raw, err := metadataSection(cmd.Known, iwire.SectionCollectionMeta)
		if err != nil {
			return "", err
		}
		meta, err := decodeCollectionMeta(raw)
		if err != nil {
			return "", err
		}
		return meta.Name, nil
	default:
		raw, err := metadataSection(cmd.Known, iwire.SectionCollectionRef)
		if err != nil {
			return "", err
		}
		name, wasHandle, err := decodeCollectionRef(nil, raw)
		if err != nil {
			return "", err
		}
		if wasHandle {
			return "", protocolError(iwire.ErrInvalidCommand, "cluster route preflight requires collection name ref")
		}
		return name, nil
	}
}

func clusterRequestMetadata(header iwire.Header, command iwire.CommandHeader, sections []iwire.Section, ack AckPolicy) (ClusterRequestMetadata, error) {
	deadline, err := deadlineUnixNanosFromSections(sections)
	if err != nil {
		return ClusterRequestMetadata{}, err
	}
	trace, err := optionalSingletonSectionBytes(sections, iwire.SectionTraceContext)
	if err != nil {
		return ClusterRequestMetadata{}, err
	}
	compression, err := optionalSingletonSectionBytes(sections, iwire.SectionCompression)
	if err != nil {
		return ClusterRequestMetadata{}, err
	}
	return ClusterRequestMetadata{
		RequestID:         header.RequestID,
		AckPolicy:         ack,
		DeadlineUnixNanos: deadline,
		TraceContext:      bytes.Clone(trace),
		Compression:       string(compression),
		OmitResultIDs:     command.Flags&iwire.CommandFlagOmitResultIDs != 0,
		OmitResponseMeta:  command.Flags&iwire.CommandFlagOmitResponseMeta != 0,
	}, nil
}

// ApplyClusterRouteMetadata copies one preflighted route and its catalog proof
// into request-only submit metadata. Native-wire and adapter callers share this
// mapper so new binding fields cannot silently drift between submit paths.
func ApplyClusterRouteMetadata(metadata *ClusterRequestMetadata, request ClusterRouteRequest, target ClusterRouteTarget) {
	if metadata == nil {
		return
	}
	metadata.ClusterRouteKnown = true
	metadata.ClusterRouteDatabase = request.Database
	metadata.ClusterRouteCatalog = request.Catalog
	metadata.ClusterRouteCollection = request.Collection
	metadata.ClusterRouteShape = string(target.Shape)
	metadata.ClusterRouteGroupID = target.GroupID
	metadata.ClusterRouteMembers = append([]string(nil), target.Members...)
	metadata.ClusterRouteLeaderHint = target.LeaderHint
	metadata.ClusterRoutePlacementMode = target.PlacementMode
	metadata.ClusterRouteKey = target.RouteKey
	metadata.ClusterRouteTokenKnown = target.TokenKnown
	metadata.ClusterRouteToken = target.Token
	metadata.ClusterRoutePartitionID = target.PartitionID
	metadata.CatalogMetaEpoch = target.CatalogMetaEpoch
	metadata.CatalogMetaDigest = target.CatalogMetaDigest
}

func deadlineUnixNanosFromSections(sections []iwire.Section) (int64, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionDeadline)
	if err != nil || !ok {
		return 0, err
	}
	value, n, err := readUvarint(raw)
	if err != nil {
		return 0, err
	}
	if n != len(raw) {
		return 0, protocolError(iwire.ErrMalformedFrame, "deadline has trailing bytes")
	}
	if value > uint64(1<<63-1) {
		return 0, protocolError(iwire.ErrInvalidCommand, "deadline exceeds int64 nanoseconds")
	}
	return int64(value), nil
}

func optionalSingletonSectionBytes(sections []iwire.Section, id iwire.SectionID) ([]byte, error) {
	raw, ok, err := singletonSection(sections, id)
	if err != nil || !ok {
		return nil, err
	}
	return raw, nil
}

func validateClusterSubmitResult(metadata ClusterRequestMetadata, result ClusterSubmitResult) error {
	switch result.ActualAck {
	case iwire.AckVisible, iwire.AckFlushed, iwire.AckSynced, iwire.AckRaftCommitted:
	default:
		return protocolError(iwire.ErrInternal, "cluster submitter returned unsupported actual ack policy %d", result.ActualAck)
	}
	if metadata.AckPolicy == iwire.AckRaftCommitted {
		if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable {
			return protocolError(iwire.ErrDurabilityUnavailable, "cluster submitter did not prove raft_committed durability")
		}
	} else if result.ActualAck == iwire.AckRaftCommitted {
		return protocolError(iwire.ErrDurabilityUnavailable, "cluster submitter returned raft_committed without proving requested local ack policy %d", metadata.AckPolicy)
	} else if metadata.AckPolicy != 0 && result.ActualAck < metadata.AckPolicy {
		return protocolError(iwire.ErrDurabilityUnavailable, "cluster submitter actual ack policy %d is below requested policy %d", result.ActualAck, metadata.AckPolicy)
	}
	if ack, ok, err := responseMetaAckPolicy(result.ResponseSections); err != nil {
		return err
	} else if ok && ack != result.ActualAck {
		return protocolError(iwire.ErrInternal, "cluster submitter response ack policy %d does not match result ack policy %d", ack, result.ActualAck)
	}
	if result.HasCatalogVersion {
		version, ok, err := catalogVersionFromResponseMeta(result.ResponseSections)
		if err != nil {
			return err
		}
		if ok && version != result.CatalogVersion {
			return protocolError(iwire.ErrInternal, "cluster submitter response catalog version %d does not match result catalog version %d", version, result.CatalogVersion)
		}
	}
	return nil
}

func responseMetaAckPolicy(sections []iwire.Section) (AckPolicy, bool, error) {
	raw, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil || !ok {
		return 0, ok, err
	}
	values, err := decodeStringMap(raw)
	if err != nil {
		return 0, true, err
	}
	rawAck, ok := values["actual_ack_policy"]
	if !ok {
		return 0, true, protocolError(iwire.ErrMalformedFrame, "response_meta missing actual_ack_policy")
	}
	value, err := strconv.ParseUint(rawAck, 10, 64)
	if err != nil {
		return 0, true, protocolError(iwire.ErrMalformedFrame, "response_meta actual_ack_policy is not a uint64")
	}
	return AckPolicy(value), true, nil
}

func nativeErrorForRaftEntryValidation(err error) error {
	code, ok := raftentry.ErrorCodeOf(err)
	if !ok {
		return err
	}
	switch code {
	case raftentry.ErrorUnsupportedCommandV1, raftentry.ErrorReadOnlyV1, raftentry.ErrorUnsupportedFeatureV1:
		return protocolError(iwire.ErrUnsupportedFeature, "%v", err)
	case raftentry.ErrorUnsupportedVersionV1:
		return protocolError(iwire.ErrUnsupportedVersion, "%v", err)
	case raftentry.ErrorResourceExhaustedV1:
		return protocolError(iwire.ErrResourceExhausted, "%v", err)
	case raftentry.ErrorUnsafeDurabilityModeV1:
		return protocolError(iwire.ErrDurabilityUnavailable, "%v", err)
	default:
		return protocolError(iwire.ErrInvalidCommand, "%v", err)
	}
}
