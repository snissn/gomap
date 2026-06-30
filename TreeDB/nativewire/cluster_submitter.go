package nativewire

import (
	"bytes"
	"context"
	"strconv"

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
}

type ClusterRouteTarget struct {
	GroupID       string
	Members       []string
	LeaderHint    string
	PlacementMode string
	Reason        string
	Shape         ClusterRouteShape
	TokenKnown    bool
	Token         uint64
	PartitionID   string
}

// ClusterAdmissionProvider exposes the node write-admission state backing a
// ClusterSubmitter. A configured cluster submitter that does not implement
// this interface fails closed as admission-unavailable.
type ClusterAdmissionProvider interface {
	ClusterAdmissionStatus(ctx context.Context) (ClusterAdmissionStatus, error)
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
	ResponseSections     []iwire.Section
}

func (s *Server) handleClusterMutation(ctx context.Context, header iwire.Header, cmd iwire.ValidatedCommand) ([]iwire.Section, error) {
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
		applyClusterRouteMetadata(&metadata, routeReq, route)
	}
	result, err := s.clusterSubmitter.SubmitCommandEntryV1(ctx, entry, metadata)
	if err != nil {
		return nil, err
	}
	if err := validateClusterSubmitResult(metadata, result); err != nil {
		return nil, err
	}
	return cloneSections(result.ResponseSections), nil
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
// an exactly-one-ID token route request.
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
	default:
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "cluster route shape %q is not supported", request.Shape)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	target, err := provider.ClusterRoute(ctx, request)
	if err != nil {
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "cluster route rejected %s/%s/%s: %v", request.Database, request.Catalog, request.Collection, err)
	}
	if target.GroupID == "" {
		reason := target.Reason
		if reason == "" {
			reason = "cluster route target missing group id"
		}
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
	}
	if target.PlacementMode == "" {
		reason := target.Reason
		if reason == "" {
			reason = "cluster route target missing collection placement mode"
		}
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
	}
	target.Shape = normalizeClusterRouteTargetShape(target.Shape, target.PlacementMode)
	switch target.PlacementMode {
	case "collection":
		if target.Shape != ClusterRouteShapeCollection {
			reason := target.Reason
			if reason == "" {
				reason = "cluster collection route target must use collection route shape"
			}
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
		}
	case "token", "ring":
		if request.Shape != ClusterRouteShapeToken || !request.TokenKnown {
			reason := target.Reason
			if reason == "" {
				reason = "cluster token/ring route target requires exactly one document id"
			}
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
		}
		if target.Shape != ClusterRouteShapeToken {
			reason := target.Reason
			if reason == "" {
				reason = "cluster token/ring route target must use token route shape"
			}
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
		}
		if !target.TokenKnown {
			reason := target.Reason
			if reason == "" {
				reason = "cluster token/ring route target missing document token"
			}
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
		}
		if target.Token != request.Token {
			reason := target.Reason
			if reason == "" {
				reason = "cluster token/ring route target token does not match request token"
			}
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
		}
		if target.PartitionID == "" {
			reason := target.Reason
			if reason == "" {
				reason = "cluster token/ring route target missing token partition id"
			}
			return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
		}
	default:
		reason := target.Reason
		if reason == "" {
			reason = "cluster route target placement mode " + target.PlacementMode + " is not supported"
		}
		return ClusterRouteTarget{}, true, protocolError(iwire.ErrReadOnly, "%s", reason)
	}
	target.Members = append([]string(nil), target.Members...)
	return target, true, nil
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
	token, ok, err := clusterMutationDocumentToken(cmd, limits)
	if err != nil {
		return ClusterRouteRequest{}, err
	}
	if ok {
		req.Shape = ClusterRouteShapeToken
		req.TokenKnown = true
		req.Token = token
	}
	return req, nil
}

func clusterMutationDocumentToken(cmd iwire.ValidatedCommand, limits iwire.Limits) (uint64, bool, error) {
	switch cmd.Header.ID {
	case iwire.CommandInsertBatch, iwire.CommandReplaceBatch, iwire.CommandDeleteBatch, iwire.CommandUpdateBSONSet:
	default:
		return 0, false, nil
	}
	raw, err := metadataSection(cmd.Known, iwire.SectionDocumentIDs)
	if err != nil {
		return 0, false, err
	}
	ids, err := iwire.DecodeByteVectorItems(raw, limits)
	if err != nil {
		return 0, false, err
	}
	if len(ids) != 1 {
		return 0, false, nil
	}
	return raftplacement.DocumentIDTokenV1(ids[0]), true, nil
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

func applyClusterRouteMetadata(metadata *ClusterRequestMetadata, request ClusterRouteRequest, target ClusterRouteTarget) {
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
	metadata.ClusterRouteTokenKnown = target.TokenKnown
	metadata.ClusterRouteToken = target.Token
	metadata.ClusterRoutePartitionID = target.PartitionID
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
