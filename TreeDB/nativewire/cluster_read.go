package nativewire

import (
	"context"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// ClusterReadCoordinator proves cluster read barriers for native-wire reads.
// Implementations must fail closed: returning a strong ActualConsistency is the
// proof that the requested barrier was satisfied before the server reads local
// state.
type ClusterReadCoordinator interface {
	CoordinateRead(ctx context.Context, request ClusterReadRequest) (ClusterReadResult, error)
}

type ClusterReadRequest struct {
	Policy      ConsistencyPolicy
	CommandID   iwire.CommandID
	CommandName string
	RequestID   uint64
	StreamID    uint64
}

type ClusterReadResult struct {
	ActualConsistency ConsistencyPolicy
	ServingNode       string
	LeaderNode        string
	AppliedIndex      uint64
	HasAppliedIndex   bool
}

func (s *Server) readMetadataForCommand(ctx context.Context, header iwire.Header, cmd iwire.ValidatedCommand) (ReadMetadata, error) {
	policy, err := consistencyPolicyFromSections(cmd.Known)
	if err != nil {
		return ReadMetadata{}, err
	}
	if policy == iwire.ConsistencyLocalStale {
		return ReadMetadata{Valid: true, ActualConsistency: iwire.ConsistencyLocalStale}, nil
	}
	if s == nil || s.clusterReadCoordinator == nil {
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "consistency policy %s requires a cluster read coordinator", consistencyPolicyName(policy))
	}
	commandName := ""
	if cmd.Schema != nil {
		commandName = cmd.Schema.Name
	}
	result, err := s.clusterReadCoordinator.CoordinateRead(ctx, ClusterReadRequest{
		Policy:      policy,
		CommandID:   cmd.Header.ID,
		CommandName: commandName,
		RequestID:   header.RequestID,
		StreamID:    header.StreamID,
	})
	if err != nil {
		if _, ok := iwire.ErrorCodeOf(err); ok {
			return ReadMetadata{}, err
		}
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster read coordinator failed: %v", err)
	}
	return validateClusterReadResult(policy, result)
}

func validateClusterReadResult(requested ConsistencyPolicy, result ClusterReadResult) (ReadMetadata, error) {
	if !validConsistencyPolicy(result.ActualConsistency) {
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster read coordinator returned unsupported actual consistency policy %d", result.ActualConsistency)
	}
	if !readConsistencySatisfies(requested, result.ActualConsistency) {
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster read coordinator actual consistency %s does not satisfy requested %s", consistencyPolicyName(result.ActualConsistency), consistencyPolicyName(requested))
	}
	return ReadMetadata{
		Valid:             true,
		ActualConsistency: result.ActualConsistency,
		ServingNode:       result.ServingNode,
		LeaderNode:        result.LeaderNode,
		AppliedIndex:      result.AppliedIndex,
		HasAppliedIndex:   result.HasAppliedIndex,
	}, nil
}

func readConsistencySatisfies(requested, actual ConsistencyPolicy) bool {
	switch requested {
	case iwire.ConsistencyLocalStale:
		return validConsistencyPolicy(actual)
	case iwire.ConsistencyLeaderRead:
		return actual == iwire.ConsistencyLeaderRead ||
			actual == iwire.ConsistencyLinearizable ||
			actual == iwire.ConsistencyLeaseRead
	case iwire.ConsistencyLinearizable:
		return actual == iwire.ConsistencyLinearizable
	case iwire.ConsistencyLeaseRead:
		return actual == iwire.ConsistencyLeaseRead ||
			actual == iwire.ConsistencyLinearizable
	default:
		return false
	}
}
