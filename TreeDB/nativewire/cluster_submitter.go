package nativewire

import (
	"bytes"
	"context"
	"strconv"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
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
