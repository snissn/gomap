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

// ClusterRoutedReadCoordinator is the explicit opt-in for catalog-routed
// reads. Implementations must coordinate the owner in request.RouteTarget;
// implementing only ClusterReadCoordinator never authorizes a routed local
// observation. This contract selects an owner barrier, not a remote data plane:
// the bounded implementation assumes the statically routed in-process groups
// expose the same applied collection store to the serving nativewire server.
type ClusterRoutedReadCoordinator interface {
	CoordinateRoutedRead(ctx context.Context, request ClusterReadRequest) (ClusterReadResult, error)
}

type ClusterReadRequest struct {
	Policy      ConsistencyPolicy
	CommandID   iwire.CommandID
	CommandName string
	RequestID   uint64
	StreamID    uint64
	RouteKnown  bool
	RouteTarget ClusterRouteTarget
}

type ClusterReadResult struct {
	ActualConsistency ConsistencyPolicy
	ServingGroup      string
	ServingNode       string
	LeaderNode        string
	AppliedIndex      uint64
	HasAppliedIndex   bool
}

func (s *Server) readMetadataForCommand(ctx context.Context, header iwire.Header, cmd iwire.ValidatedCommand, route ClusterRouteTarget, routed bool) (ReadMetadata, error) {
	policy, err := consistencyPolicyFromSections(cmd.Known)
	if err != nil {
		return ReadMetadata{}, err
	}
	if policy == iwire.ConsistencyLocalStale {
		if routed {
			if s != nil {
				s.counters.inc("cluster_read_route.stale_rejected_total")
			}
			return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster routed _id read requires linearizable consistency; requested %s", consistencyPolicyName(policy))
		}
		return ReadMetadata{Valid: true, ActualConsistency: iwire.ConsistencyLocalStale}, nil
	}
	if routed && policy != iwire.ConsistencyLinearizable {
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster routed _id read requires linearizable consistency; requested %s", consistencyPolicyName(policy))
	}
	if s == nil || s.clusterReadCoordinator == nil {
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "consistency policy %s requires a cluster read coordinator", consistencyPolicyName(policy))
	}
	commandName := ""
	if cmd.Schema != nil {
		commandName = cmd.Schema.Name
	}
	request := ClusterReadRequest{
		Policy:      policy,
		CommandID:   cmd.Header.ID,
		CommandName: commandName,
		RequestID:   header.RequestID,
		StreamID:    header.StreamID,
		RouteKnown:  routed,
		RouteTarget: route,
	}
	var result ClusterReadResult
	if routed {
		coordinator, ok := s.clusterReadCoordinator.(ClusterRoutedReadCoordinator)
		if !ok {
			return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster read coordinator does not support routed owner reads")
		}
		result, err = coordinator.CoordinateRoutedRead(ctx, request)
	} else {
		result, err = s.clusterReadCoordinator.CoordinateRead(ctx, request)
	}
	if err != nil {
		if _, ok := iwire.ErrorCodeOf(err); ok {
			return ReadMetadata{}, err
		}
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster read coordinator failed: %v", err)
	}
	if routed && result.ServingGroup != route.GroupID {
		return ReadMetadata{}, protocolError(iwire.ErrConsistencyUnavailable, "cluster routed read coordinator served group %q, want owner group %q", result.ServingGroup, route.GroupID)
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
