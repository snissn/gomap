package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

// RaftClusterSubmitter adapts the internal single-group raftcluster bridge to
// the public nativewire ClusterSubmitter contract.
type RaftClusterSubmitter struct {
	Bridge      raftcluster.CommandSubmitterV1
	Collections *collections.CollectionManager
}

// RoutedRaftClusterSubmitter composes the concrete single-group Raft bridge
// with a catalog-backed route provider. The base RaftClusterSubmitter does not
// implement ClusterRouteProvider so existing no-provider behavior stays
// unchanged.
type RoutedRaftClusterSubmitter struct {
	*RaftClusterSubmitter
	RouteProvider ClusterRouteProvider
}

func NewRaftClusterSubmitter(bridge raftcluster.CommandSubmitterV1, managers ...*collections.CollectionManager) *RaftClusterSubmitter {
	submitter := &RaftClusterSubmitter{Bridge: bridge}
	if len(managers) > 0 {
		submitter.Collections = managers[0]
	}
	return submitter
}

func NewRoutedRaftClusterSubmitter(bridge raftcluster.CommandSubmitterV1, provider ClusterRouteProvider, managers ...*collections.CollectionManager) *RoutedRaftClusterSubmitter {
	return &RoutedRaftClusterSubmitter{
		RaftClusterSubmitter: NewRaftClusterSubmitter(bridge, managers...),
		RouteProvider:        provider,
	}
}

func (s *RoutedRaftClusterSubmitter) ClusterRoute(ctx context.Context, request ClusterRouteRequest) (ClusterRouteTarget, error) {
	if s == nil || s.RouteProvider == nil {
		return ClusterRouteTarget{}, protocolError(iwire.ErrReadOnly, "raft cluster route provider is not configured")
	}
	return s.RouteProvider.ClusterRoute(ctx, request)
}

func (s *RaftClusterSubmitter) ClusterAdmissionStatus(ctx context.Context) (ClusterAdmissionStatus, error) {
	if s == nil || s.Bridge == nil {
		return ClusterAdmissionStatus{}, raftcluster.ErrInvalidSubmitter
	}
	provider, ok := s.Bridge.(raftcluster.AdmissionProvider)
	if !ok {
		return ClusterAdmissionStatus{}, raftcluster.ErrAdmissionUnavailable
	}
	status, err := provider.ClusterAdmissionStatus(ctx)
	if err != nil {
		return ClusterAdmissionStatus{}, err
	}
	return ClusterAdmissionStatus{
		Leader:      status.Leader,
		Unavailable: status.Unavailable,
		LeaderHint:  string(status.LeaderHint),
		Reason:      status.Reason,
	}, nil
}

func (s *RaftClusterSubmitter) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata ClusterRequestMetadata) (ClusterSubmitResult, error) {
	if s == nil || s.Bridge == nil {
		return ClusterSubmitResult{}, protocolError(iwire.ErrInvalidCommand, "raft cluster submitter is not configured")
	}
	if s.Collections == nil {
		return ClusterSubmitResult{}, protocolError(iwire.ErrInvalidCommand, "raft cluster submitter collection manager is not configured")
	}
	result, err := s.Bridge.SubmitCommandEntryV1(ctx, entry, metadata)
	if err != nil {
		return ClusterSubmitResult{}, nativeErrorForRaftClusterSubmit(err)
	}
	sections, err := raftClusterResponseSections(result.DecodedEntry, metadata, result, s.Collections)
	if err != nil {
		return ClusterSubmitResult{}, err
	}
	return ClusterSubmitResult{
		ActualAck:            AckPolicy(result.ActualAck),
		CommittedRecoverable: result.CommittedRecoverable,
		ResponseSections:     sections,
		CatalogVersion:       result.CatalogVersion,
		HasCatalogVersion:    result.HasCatalogVersion,
	}, nil
}

func raftClusterResponseSections(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result raftcluster.SubmitResultV1, manager *collections.CollectionManager) ([]iwire.Section, error) {
	actualAck := AckPolicy(result.ActualAck)
	catalogVersion := result.CatalogVersion
	hasCatalogVersion := result.HasCatalogVersion
	affected, matched, err := raftClusterMatchedAndAffectedCounts(result.ApplyResult)
	if err != nil {
		return nil, err
	}
	switch entry.Decoded.CommandID {
	case iwire.CommandCreateCollection:
		rawMeta, err := raftClusterCreateCollectionResponseMeta(entry, manager)
		if err != nil {
			return nil, err
		}
		sections := []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: bytes.Clone(rawMeta)}}
		if !metadata.OmitResponseMeta {
			sections = append(sections, ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion))
		}
		return sections, nil
	case iwire.CommandInsertBatch:
		var sections []iwire.Section
		if !metadata.OmitResultIDs {
			rawIDs, err := metadataSection(entry.Decoded.Sections, iwire.SectionDocumentIDs)
			if err != nil {
				return nil, err
			}
			sections = append(sections, iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: bytes.Clone(rawIDs)})
		}
		if !metadata.OmitResponseMeta {
			sections = append(sections, ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion, responseMetaCount{key: "inserted_count", value: affected}))
		}
		return sections, nil
	case iwire.CommandReplaceBatch, iwire.CommandUpdateBSONSet:
		if metadata.OmitResponseMeta {
			return nil, nil
		}
		return []iwire.Section{ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion,
			responseMetaCount{key: "matched_count", value: matched},
			responseMetaCount{key: "modified_count", value: affected},
		)}, nil
	case iwire.CommandDeleteBatch:
		if metadata.OmitResponseMeta {
			return nil, nil
		}
		return []iwire.Section{ackMetaCountsVersion(actualAck, catalogVersion, hasCatalogVersion, responseMetaCount{key: "deleted_count", value: affected})}, nil
	default:
		return nil, protocolError(iwire.ErrUnsupportedFeature, "raft cluster submitter response does not support command %d", entry.Decoded.CommandID)
	}
}

func raftClusterCreateCollectionResponseMeta(entry raftentry.CommandEntryV1, manager *collections.CollectionManager) ([]byte, error) {
	rawMeta, err := metadataSection(entry.Decoded.Sections, iwire.SectionCollectionMeta)
	if err != nil {
		return nil, err
	}
	meta, err := decodeCollectionMeta(rawMeta)
	if err != nil {
		return nil, err
	}
	meta, err = normalizeClientCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, protocolError(iwire.ErrInternal, "raft cluster submitter cannot return applied create_collection metadata without a collection manager")
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		return nil, protocolError(iwire.ErrInternal, "raft cluster submitter applied create_collection but collection %q is not readable: %v", meta.Name, err)
	}
	return encodeCollectionMeta(collection.Meta()), nil
}

func raftClusterMatchedAndAffectedCounts(result raftentry.ApplyResultV1) (int, int, error) {
	if result.AffectedCount < 0 || result.AffectedCount > int64(maxInt) {
		return 0, 0, protocolError(iwire.ErrInternal, "raft cluster apply affected count %d is outside int range", result.AffectedCount)
	}
	if result.MatchedCount < 0 || result.MatchedCount > int64(maxInt) {
		return 0, 0, protocolError(iwire.ErrInternal, "raft cluster apply matched count %d is outside int range", result.MatchedCount)
	}
	return int(result.AffectedCount), int(result.MatchedCount), nil
}

func nativeErrorForRaftClusterSubmit(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raftcluster.ErrNotLeader):
		return protocolError(iwire.ErrReadOnly, "%v", err)
	case errors.Is(err, raftcluster.ErrRouteGroupMismatch),
		errors.Is(err, raftcluster.ErrRouteTargetMissing),
		errors.Is(err, raftcluster.ErrRouteTargetUnknown),
		errors.Is(err, raftcluster.ErrRouteTargetUnsupported),
		errors.Is(err, raftcluster.ErrRouteFanoutRequired):
		return clusterProtocolError(iwire.ErrReadOnly, err)
	case errors.Is(err, raftcluster.ErrCatalogVersionMismatch):
		return protocolError(iwire.ErrCatalogVersionMismatch, "%v", err)
	case errors.Is(err, raftcluster.ErrCommitAmbiguous):
		return protocolError(iwire.ErrCommitAmbiguous, "%v", err)
	case errors.Is(err, raftcluster.ErrAdmissionUnavailable),
		errors.Is(err, raftcluster.ErrCommitNotProven),
		errors.Is(err, raftcluster.ErrLocalApplyNotRecoverable),
		errors.Is(err, raftcluster.ErrLocalAckUnavailable),
		errors.Is(err, raftcluster.ErrInvalidSubmitter),
		errors.Is(err, raftcluster.ErrMissingCatalogVersion):
		return protocolError(iwire.ErrDurabilityUnavailable, "%v", err)
	case errors.Is(err, raftcluster.ErrUnsupportedSubmitAck),
		errors.Is(err, raftcluster.ErrInvalidCommittedEntry),
		errors.Is(err, raftcluster.ErrUnexpectedCommittedTarget):
		return protocolError(iwire.ErrInvalidCommand, "%v", err)
	}
	if code, ok := raftentry.ErrorCodeOf(err); ok {
		return nativeErrorForDeterministicCode(code, err)
	}
	if code, ok := raftfsm.ErrorCodeOf(err); ok {
		return nativeErrorForDeterministicCode(code, err)
	}
	return fmt.Errorf("raft cluster submitter: %w", err)
}

func clusterProtocolError(code iwire.ErrorCode, err error) error {
	protocolErr := protocolError(code, "%v", err)
	route, hasRoute := ClusterRouteErrorMetadataOf(err)
	leaderHint := clusterLeaderHint(err)
	if leaderHint == "" {
		leaderHint = route.LeaderHint
	}
	if hasRoute || leaderHint != "" {
		return &clusterRouteProtocolError{
			err:        protocolErr,
			leaderHint: leaderHint,
			route:      route,
			hasRoute:   hasRoute,
		}
	}
	return protocolErr
}

type clusterRouteProtocolError struct {
	err        error
	leaderHint string
	route      ClusterRouteErrorMetadata
	hasRoute   bool
}

func (e *clusterRouteProtocolError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *clusterRouteProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *clusterRouteProtocolError) ClusterLeaderHint() string {
	if e == nil {
		return ""
	}
	return e.leaderHint
}

func (e *clusterRouteProtocolError) ClusterRouteErrorMetadata() ClusterRouteErrorMetadata {
	if e == nil || !e.hasRoute {
		return ClusterRouteErrorMetadata{}
	}
	return e.route.Clone()
}

func clusterLeaderHint(err error) string {
	var hinted interface {
		ClusterLeaderHint() string
	}
	if errors.As(err, &hinted) {
		return hinted.ClusterLeaderHint()
	}
	return ""
}

func nativeErrorForDeterministicCode(code raftentry.DeterministicErrorCodeV1, err error) error {
	if wireCode, ok := nativeCollectionConflictCode(err); ok {
		return protocolError(wireCode, "%v", err)
	}
	switch code {
	case raftentry.ErrorUnsupportedCommandV1, raftentry.ErrorReadOnlyV1, raftentry.ErrorUnsupportedFeatureV1:
		return protocolError(iwire.ErrUnsupportedFeature, "%v", err)
	case raftentry.ErrorUnsupportedVersionV1:
		return protocolError(iwire.ErrUnsupportedVersion, "%v", err)
	case raftentry.ErrorResourceExhaustedV1:
		return protocolError(iwire.ErrResourceExhausted, "%v", err)
	case raftentry.ErrorUnsafeDurabilityModeV1, raftentry.ErrorResultReplayRequiredV1:
		return protocolError(iwire.ErrDurabilityUnavailable, "%v", err)
	default:
		return protocolError(iwire.ErrInvalidCommand, "%v", err)
	}
}

func nativeCollectionConflictCode(err error) (iwire.ErrorCode, bool) {
	switch {
	case errors.Is(err, collections.ErrDuplicateDocumentID):
		return iwire.ErrDuplicateDocumentID, true
	case errors.Is(err, collections.ErrDocumentExists):
		return iwire.ErrDocumentExists, true
	case errors.Is(err, collections.ErrUniqueIndexConflict):
		return iwire.ErrUniqueIndexConflict, true
	default:
		return 0, false
	}
}
