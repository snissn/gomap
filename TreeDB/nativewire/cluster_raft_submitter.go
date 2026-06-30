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
	Bridge *raftcluster.SingleGroupSubmitter
}

func NewRaftClusterSubmitter(bridge *raftcluster.SingleGroupSubmitter) *RaftClusterSubmitter {
	return &RaftClusterSubmitter{Bridge: bridge}
}

func (s *RaftClusterSubmitter) ClusterAdmissionStatus(ctx context.Context) (ClusterAdmissionStatus, error) {
	if s == nil || s.Bridge == nil {
		return ClusterAdmissionStatus{}, raftcluster.ErrInvalidSubmitter
	}
	status, err := s.Bridge.ClusterAdmissionStatus(ctx)
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
	result, err := s.Bridge.SubmitCommandEntryV1(ctx, entry, metadata)
	if err != nil {
		return ClusterSubmitResult{}, nativeErrorForRaftClusterSubmit(err)
	}
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{RequestMetadata: metadata})
	if err != nil {
		return ClusterSubmitResult{}, nativeErrorForRaftEntryValidation(err)
	}
	sections, err := raftClusterResponseSections(decoded, metadata, result)
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

func raftClusterResponseSections(entry raftentry.CommandEntryV1, metadata ClusterRequestMetadata, result raftcluster.SubmitResultV1) ([]iwire.Section, error) {
	actualAck := AckPolicy(result.ActualAck)
	catalogVersion := result.CatalogVersion
	hasCatalogVersion := result.HasCatalogVersion
	affected, matched, err := raftClusterMatchedAndAffectedCounts(result.ApplyResult)
	if err != nil {
		return nil, err
	}
	switch entry.Decoded.CommandID {
	case iwire.CommandCreateCollection:
		rawMeta, err := raftClusterCreateCollectionResponseMeta(entry)
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

func raftClusterCreateCollectionResponseMeta(entry raftentry.CommandEntryV1) ([]byte, error) {
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
	meta.Options.DataRootStoragePolicy = collections.RootStorageFast
	meta.Options.IndexStateStoragePolicy = collections.RootStorageFast
	for i := range meta.Indexes {
		meta.Indexes[i].StoragePolicy = collections.RootStorageFast
	}
	return encodeCollectionMeta(meta), nil
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
	case errors.Is(err, raftcluster.ErrAdmissionUnavailable),
		errors.Is(err, raftcluster.ErrCommitNotProven),
		errors.Is(err, raftcluster.ErrLocalApplyNotRecoverable),
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

func nativeErrorForDeterministicCode(code raftentry.DeterministicErrorCodeV1, err error) error {
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
