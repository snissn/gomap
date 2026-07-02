package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const (
	clusterRouteShapeCollectionV1 = "collection"
	clusterRouteShapeTokenV1      = "token"
	clusterRouteShapeTokenBatchV1 = "token_batch"

	clusterRoutePlacementCollectionV1 = "collection"
	clusterRoutePlacementTokenV1      = "token"
	clusterRoutePlacementRingV1       = "ring"

	clusterRouteKeyDocumentIDV1 = "_id"
)

// GroupSubmitterV1 binds a local Raft group ID to the in-process submitter for
// that group.
type GroupSubmitterV1 struct {
	GroupID   GroupID
	Submitter CommandSubmitterV1
}

// GroupSubmitterRegistryV1 is an immutable registry of local group submitters.
// It is deliberately in-process only; it does not forward to remote leaders.
type GroupSubmitterRegistryV1 struct {
	byGroup map[GroupID]CommandSubmitterV1
	groups  []GroupID
}

// NewGroupSubmitterRegistryV1 validates and copies the configured local group
// submitters. If a submitter exposes Config(), its configured group must match
// the registry key.
func NewGroupSubmitterRegistryV1(entries []GroupSubmitterV1) (GroupSubmitterRegistryV1, error) {
	if len(entries) == 0 {
		return GroupSubmitterRegistryV1{}, errors.Join(ErrInvalidSubmitter, fmt.Errorf("at least one group submitter is required"))
	}
	byGroup := make(map[GroupID]CommandSubmitterV1, len(entries))
	groups := make([]GroupID, 0, len(entries))
	for i, entry := range entries {
		if entry.GroupID == "" {
			return GroupSubmitterRegistryV1{}, errors.Join(ErrInvalidSubmitter, fmt.Errorf("group submitter[%d] missing group id", i))
		}
		if entry.Submitter == nil {
			return GroupSubmitterRegistryV1{}, errors.Join(ErrInvalidSubmitter, fmt.Errorf("group submitter[%d] for group %q is nil", i, entry.GroupID))
		}
		if _, exists := byGroup[entry.GroupID]; exists {
			return GroupSubmitterRegistryV1{}, errors.Join(ErrInvalidSubmitter, fmt.Errorf("group submitter for group %q is duplicated", entry.GroupID))
		}
		if provider, ok := entry.Submitter.(Provider); ok {
			cfg := provider.Config()
			if cfg.GroupID != "" && cfg.GroupID != entry.GroupID {
				return GroupSubmitterRegistryV1{}, errors.Join(ErrInvalidSubmitter, fmt.Errorf("group submitter key %q does not match submitter group %q", entry.GroupID, cfg.GroupID))
			}
		}
		byGroup[entry.GroupID] = entry.Submitter
		groups = append(groups, entry.GroupID)
	}
	slices.Sort(groups)
	return GroupSubmitterRegistryV1{byGroup: byGroup, groups: groups}, nil
}

func (r GroupSubmitterRegistryV1) Lookup(groupID GroupID) (CommandSubmitterV1, bool) {
	if r.byGroup == nil || groupID == "" {
		return nil, false
	}
	submitter, ok := r.byGroup[groupID]
	return submitter, ok
}

func (r GroupSubmitterRegistryV1) GroupIDs() []GroupID {
	return slices.Clone(r.groups)
}

func (r GroupSubmitterRegistryV1) empty() bool {
	return len(r.byGroup) == 0
}

// GroupRoutedSubmitterOptions configures an in-process dispatcher over local
// group submitters.
type GroupRoutedSubmitterOptions struct {
	Registry GroupSubmitterRegistryV1
}

// GroupRoutedSubmitter dispatches collection and single-token routed writes to
// the configured local group submitter. Unsupported, missing, unknown, and
// fanout-required route metadata is rejected before any group submitter sees
// the entry.
type GroupRoutedSubmitter struct {
	registry GroupSubmitterRegistryV1
}

func NewGroupRoutedSubmitter(opts GroupRoutedSubmitterOptions) (*GroupRoutedSubmitter, error) {
	if opts.Registry.empty() {
		return nil, errors.Join(ErrInvalidSubmitter, fmt.Errorf("group submitter registry is required"))
	}
	return &GroupRoutedSubmitter{registry: opts.Registry}, nil
}

func (s *GroupRoutedSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (SubmitResultV1, error) {
	if s == nil || s.registry.empty() {
		return SubmitResultV1{}, ErrInvalidSubmitter
	}
	target, err := routeDispatchTargetFromMetadata(metadata)
	if err != nil {
		return SubmitResultV1{}, err
	}
	submitter, ok := s.registry.Lookup(target.GroupID)
	if !ok {
		return SubmitResultV1{}, errors.Join(ErrRouteTargetUnknown, routeErrorWithLeaderHint(metadata, "route group %q is not configured locally", target.GroupID))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return submitter.SubmitCommandEntryV1(ctx, bytes.Clone(entry), cloneRequestMetadataV1(metadata))
}

func (s *GroupRoutedSubmitter) ClusterAdmissionStatus(ctx context.Context) (AdmissionStatus, error) {
	if s == nil || s.registry.empty() {
		return AdmissionStatus{}, ErrInvalidSubmitter
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var firstFollower AdmissionStatus
	var sawFollower bool
	var firstUnavailable AdmissionStatus
	var sawUnavailable bool
	var firstLeader AdmissionStatus
	var sawLeader bool
	var missingAdmission AdmissionStatus
	var sawMissingAdmission bool
	for _, groupID := range s.registry.GroupIDs() {
		submitter, ok := s.registry.Lookup(groupID)
		if !ok {
			continue
		}
		provider, ok := submitter.(AdmissionProvider)
		if !ok {
			if !sawMissingAdmission {
				missingAdmission = UnavailableAdmission(fmt.Sprintf("group %q admission provider is unavailable", groupID))
				sawMissingAdmission = true
			}
			continue
		}
		status, err := provider.ClusterAdmissionStatus(ctx)
		if err != nil {
			return AdmissionStatus{}, err
		}
		if status.Leader {
			if !sawLeader {
				firstLeader = status
				sawLeader = true
			}
			continue
		}
		if status.Unavailable {
			if !sawUnavailable {
				firstUnavailable = status
				sawUnavailable = true
			}
			continue
		}
		if !sawFollower {
			firstFollower = status
			sawFollower = true
		}
	}
	if sawMissingAdmission {
		return missingAdmission, nil
	}
	if sawLeader {
		return firstLeader, nil
	}
	if sawFollower {
		return firstFollower, nil
	}
	if sawUnavailable {
		return firstUnavailable, nil
	}
	return AdmissionStatus{Unavailable: true, Reason: "no configured group admission provider is available"}, nil
}

type routeDispatchTarget struct {
	GroupID GroupID
}

func routeDispatchTargetFromMetadata(metadata raftentry.RequestMetadataV1) (routeDispatchTarget, error) {
	if !metadata.ClusterRouteKnown {
		return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("cluster route metadata is required for group-routed submit"))
	}
	if metadata.ClusterRouteDatabase == "" || metadata.ClusterRouteCatalog == "" || metadata.ClusterRouteCollection == "" {
		return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("cluster route identity is incomplete"))
	}
	if metadata.ClusterRouteShape == "" {
		return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("cluster route shape is required"))
	}
	if metadata.ClusterRoutePlacementMode == "" {
		return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("cluster route placement mode is required"))
	}
	if metadata.ClusterRouteShape == clusterRouteShapeTokenBatchV1 {
		return routeDispatchTarget{}, errors.Join(ErrRouteFanoutRequired, fmt.Errorf("cluster route shape %q requires split/fanout before group dispatch", metadata.ClusterRouteShape))
	}
	if metadata.ClusterRouteGroupID == "" {
		return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("cluster route group id is required"))
	}
	switch metadata.ClusterRouteShape {
	case clusterRouteShapeCollectionV1:
		if metadata.ClusterRoutePlacementMode != clusterRoutePlacementCollectionV1 {
			return routeDispatchTarget{}, errors.Join(ErrRouteGroupMismatch, fmt.Errorf("collection route uses placement mode %q", metadata.ClusterRoutePlacementMode))
		}
	case clusterRouteShapeTokenV1:
		switch metadata.ClusterRoutePlacementMode {
		case clusterRoutePlacementTokenV1, clusterRoutePlacementRingV1:
		default:
			return routeDispatchTarget{}, errors.Join(ErrRouteGroupMismatch, fmt.Errorf("token route uses placement mode %q", metadata.ClusterRoutePlacementMode))
		}
		if !metadata.ClusterRouteTokenKnown {
			return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("token route missing document token"))
		}
		if metadata.ClusterRouteKey != clusterRouteKeyDocumentIDV1 {
			return routeDispatchTarget{}, errors.Join(ErrRouteTargetUnsupported, fmt.Errorf("token route uses route key %q", metadata.ClusterRouteKey))
		}
		if metadata.ClusterRoutePartitionID == "" {
			return routeDispatchTarget{}, errors.Join(ErrRouteTargetMissing, fmt.Errorf("token route missing partition id"))
		}
	default:
		return routeDispatchTarget{}, errors.Join(ErrRouteTargetUnsupported, fmt.Errorf("cluster route shape %q is not supported", metadata.ClusterRouteShape))
	}
	return routeDispatchTarget{GroupID: GroupID(metadata.ClusterRouteGroupID)}, nil
}
