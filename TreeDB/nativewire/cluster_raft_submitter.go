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
	Bridge                   raftcluster.CommandSubmitterV1
	Collections              *collections.CollectionManager
	VectorPartitionAdmission VectorPartitionMutationLifecycleProviderV1
}

// NewRaftClusterSubmitterWithVectorPartitionAdmissionV1 is the production M7
// construction path. The supplied admission provider performs deterministic
// schema classification and calls the replicated lifecycle coordinator inside
// the data preflight-to-commit boundary.
func NewRaftClusterSubmitterWithVectorPartitionAdmissionV1(bridge raftcluster.CommandSubmitterV1, admission VectorPartitionMutationAdmissionProviderV1, managers ...*collections.CollectionManager) (*RaftClusterSubmitter, error) {
	if admission == nil {
		return nil, errors.New("nativewire: vector partition admission provider is required")
	}
	lifecycle, ok := admission.(VectorPartitionMutationLifecycleProviderV1)
	if !ok {
		return nil, errors.New("nativewire: vector partition mutation confirmation provider is required")
	}
	submitter := NewRaftClusterSubmitter(bridge, managers...)
	submitter.VectorPartitionAdmission = lifecycle
	return submitter, nil
}

func (s *RaftClusterSubmitter) RequiresVectorPartitionMutationAdmissionV1(context.Context) (bool, error) {
	if s == nil {
		return false, raftcluster.ErrInvalidSubmitter
	}
	if s.VectorPartitionAdmission != nil {
		return true, nil
	}
	if requirements, ok := s.Bridge.(raftcluster.FeatureRequirementProviderV1); ok {
		return requirements.RequiresFeatureV1(raftcluster.FeatureVectorPartitionLifecycle)
	}
	if provider, ok := s.Bridge.(raftcluster.Provider); ok {
		return raftcluster.FeatureSetRequiresV1(provider.Config().Features, raftcluster.FeatureVectorPartitionLifecycle), nil
	}
	return false, nil
}

func (s *RaftClusterSubmitter) AdmitVectorPartitionMutationV1(ctx context.Context, command iwire.CommandID, sections []iwire.Section) error {
	if s == nil || s.VectorPartitionAdmission == nil {
		return protocolError(iwire.ErrReadOnly, "vector partition lifecycle admission is not configured")
	}
	return s.VectorPartitionAdmission.AdmitVectorPartitionMutationV1(ctx, command, sections)
}

func (s *RaftClusterSubmitter) ConfirmVectorPartitionMutationV1(ctx context.Context, command iwire.CommandID, sections []iwire.Section) error {
	if s == nil || s.VectorPartitionAdmission == nil {
		return protocolError(iwire.ErrReadOnly, "vector partition lifecycle admission is not configured")
	}
	return s.VectorPartitionAdmission.ConfirmVectorPartitionMutationV1(ctx, command, sections)
}

func (s *RaftClusterSubmitter) ValidateVectorPartitionMutationLifecycleV1() error {
	if s == nil || s.VectorPartitionAdmission == nil {
		return protocolError(iwire.ErrReadOnly, "vector partition lifecycle admission is not configured")
	}
	return nil
}

// RoutedRaftClusterSubmitter composes the concrete single-group Raft bridge
// with a catalog-backed route provider. The base RaftClusterSubmitter does not
// implement ClusterRouteProvider so existing no-provider behavior stays
// unchanged.
type RoutedRaftClusterSubmitter struct {
	*RaftClusterSubmitter
	RouteProvider ClusterRouteProvider
}

func (s *RoutedRaftClusterSubmitter) RequiresVectorPartitionMutationAdmissionV1(ctx context.Context) (bool, error) {
	if s == nil {
		return false, raftcluster.ErrInvalidSubmitter
	}
	if requirements, ok := s.RouteProvider.(VectorPartitionMutationAdmissionRequiredV1); ok {
		required, err := requirements.RequiresVectorPartitionMutationAdmissionV1(ctx)
		if err != nil || required {
			return required, err
		}
	}
	return s.RaftClusterSubmitter.RequiresVectorPartitionMutationAdmissionV1(ctx)
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

// NewRoutedRaftClusterSubmitterWithVectorPartitionAdmissionV1 is the sole
// routed M7 composition: the same embedded submitter owns both the route
// proof and the shared pre-commit admission/after-apply confirmation hooks.
// Legacy routed construction remains valid only where the replicated catalog
// does not require the vector-partition lifecycle feature.
func NewRoutedRaftClusterSubmitterWithVectorPartitionAdmissionV1(bridge raftcluster.CommandSubmitterV1, provider ClusterRouteProvider, admission VectorPartitionMutationAdmissionProviderV1, managers ...*collections.CollectionManager) (*RoutedRaftClusterSubmitter, error) {
	base, err := NewRaftClusterSubmitterWithVectorPartitionAdmissionV1(bridge, admission, managers...)
	if err != nil {
		return nil, err
	}
	return &RoutedRaftClusterSubmitter{RaftClusterSubmitter: base, RouteProvider: provider}, nil
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
	return s.submitCommandEntryV1(ctx, entry, metadata, nil)
}

func (s *RaftClusterSubmitter) SubmitCommandEntryWithPreCommitV1(ctx context.Context, entry []byte, metadata ClusterRequestMetadata, preCommit func(context.Context) error) (ClusterSubmitResult, error) {
	if preCommit == nil {
		return ClusterSubmitResult{}, protocolError(iwire.ErrInvalidCommand, "raft cluster submitter pre-commit callback is required")
	}
	return s.submitCommandEntryV1(ctx, entry, metadata, preCommit)
}

func (s *RaftClusterSubmitter) submitCommandEntryV1(ctx context.Context, entry []byte, metadata ClusterRequestMetadata, preCommit func(context.Context) error) (ClusterSubmitResult, error) {
	if s == nil || s.Bridge == nil {
		return ClusterSubmitResult{}, protocolError(iwire.ErrInvalidCommand, "raft cluster submitter is not configured")
	}
	if s.Collections == nil {
		return ClusterSubmitResult{}, protocolError(iwire.ErrInvalidCommand, "raft cluster submitter collection manager is not configured")
	}
	var result raftcluster.SubmitResultV1
	var err error
	if preCommit == nil {
		result, err = s.Bridge.SubmitCommandEntryV1(ctx, entry, metadata)
	} else {
		atomicBridge, ok := s.Bridge.(raftcluster.CommandSubmitterWithPreCommitV1)
		if !ok {
			return ClusterSubmitResult{}, protocolError(iwire.ErrReadOnly, "raft bridge does not support serialized pre-commit callbacks")
		}
		result, err = atomicBridge.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, preCommit)
	}
	clusterResult := ClusterSubmitResult{
		ActualAck:            AckPolicy(result.ActualAck),
		CommittedRecoverable: result.CommittedRecoverable,
		CommittedApplied:     result.CommittedApplied,
		CatalogVersion:       result.CatalogVersion,
		HasCatalogVersion:    result.HasCatalogVersion,
	}
	if err != nil {
		return clusterResult, nativeErrorForRaftClusterSubmit(err)
	}
	sections, err := raftClusterResponseSections(result.DecodedEntry, metadata, result, s.Collections)
	if err != nil {
		return clusterResult, err
	}
	clusterResult.ResponseSections = sections
	return clusterResult, nil
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
	route, hasRoute := ClusterRouteErrorMetadataOf(err)
	route = redactClusterRouteErrorMetadata(route)
	protocolErr := protocolError(code, "%v", err)
	if hasRoute {
		reason := "cluster route rejected"
		if fields := clusterRouteErrorMetadataFields(route); fields != "" {
			reason += "; " + fields
		}
		protocolErr = protocolError(code, "%s", reason)
	}
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
