package collections

import (
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// reusedProducerAppender holds the SAME existing segment stripe through actual
// Size/projection/append/sync/capture. No FD, parent, shared file cursor or
// externally retained producer control is constructed on this path.
func (s *columnPhysicalAssetAppendSession) reusedProducerAppender(fileID uint32, incarnation uint64) (*columnPhysicalAssetSegmentAppender, error) {
	ref := ColumnAssetRef{Namespace: s.cfg.AssetManager.Namespace, FileID: fileID}
	path, err := columnAssetSegmentPath(s.rootDir, ref)
	if err != nil {
		return nil, err
	}
	namespace, err := columnAssetManagerNamespaceForRoot(s.rootDir, ref.Namespace)
	if err != nil {
		return nil, err
	}
	lock := columnAssetSegmentWriteLock(path)
	lock.Lock()
	size, err := s.producerDB.ColumnSegmentProducerSizeV1(ref.Namespace, fileID, incarnation, nil)
	if err != nil || size > uint64(1<<63-1) {
		lock.Unlock()
		if err == nil {
			err = rootpublication.ErrResourceConflict
		}
		return nil, err
	}
	return &columnPhysicalAssetSegmentAppender{cfg: s.cfg, namespace: namespace, fileID: fileID, assetPath: path,
		offset: int64(size), appendStart: int64(size), lock: lock, unlockLock: true, stableRegistry: s.stableRegistry,
		producerDB: s.producerDB, producerIncarnation: incarnation, producerReused: true, producerInstalled: true}, nil
}

func (a *columnPhysicalAssetSegmentAppender) installProducer(namespace *rootpublication.StableNamespaceToken) error {
	if namespace == nil || a.stableNamespaceRef == nil || a.producerDB == nil {
		return rootpublication.ErrResourceOwnership
	}
	ref := *a.stableNamespaceRef
	kind, reach, _, err := stableColumnAssetResourceClassification(ref.Kind)
	if err != nil {
		return err
	}
	// A fixed physical owner carries no logical obligations or closure callbacks.
	// DB attachment owns the one extra registry observation; output tokens hold
	// their own exact logical obligations and deletion-exclusion pins.
	token, err := NewStableColumnAssetResourceToken(rootpublication.StableResourceSpec{
		Kind: kind, LogicalLane: ref.Namespace, ResourceID: fmt.Sprint(ref.FileID),
		Generation: uint64(ref.FileID), DiagnosticPath: stableColumnAssetDiagnosticPath(ref), File: a.file,
		Frontier: rootpublication.DurableFrontier{Bytes: uint64(a.offset)}, Digest: stableColumnSegmentDigest(ref),
		Reachability: reach, Namespace: namespace, PinRegistry: a.stableRegistry, ContentSynced: true,
	})
	if err != nil {
		return err
	}
	owner, err := rootpublication.NewStableSegmentOwner(token)
	if err != nil {
		token.Release()
		return err
	}
	// Keep the constructor edge local until the existing complete-resource
	// validation and rollback decision finishes. An output token may retain this
	// owner, but a failed unpublished constructor never becomes writable DB state.
	a.pendingProducer = owner
	return nil
}
func (a *columnPhysicalAssetSegmentAppender) captureProducerResources() (*rootpublication.StableResourceSet, error) {
	builder := rootpublication.NewStableResourceSetBuilder()
	for _, ref := range a.stableRefs {
		_, reach, classification, err := stableColumnAssetResourceClassification(ref.Kind)
		if err != nil {
			builder.Abandon()
			return nil, err
		}
		if classification == "rebuildable-non-authoritative" {
			continue
		}
		obligation := stableColumnLogicalObligation(ref, reach)
		obligations := []rootpublication.StableLogicalObligation{obligation}
		// Preserve the actual existing capture fault semantics on the reused owner.
		if hook := columnAssetStableObligationHook(); hook != nil {
			switch hook(ref, obligation, nil) {
			case columnAssetStableCaptureOmitToken:
				continue
			case columnAssetStableCaptureOmitObligation:
				obligations = nil
			}
		}
		var token *rootpublication.StableResourceToken
		if a.pendingProducer != nil {
			token, err = a.pendingProducer.CaptureLogicalObligations(ref.Namespace, fmt.Sprint(ref.FileID), stableColumnAssetDiagnosticPath(ref),
				rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)}, reach, obligations, true, nil)
		} else {
			token, err = a.producerDB.CaptureColumnSegmentProducerV1(ref.Namespace, ref.FileID, a.producerIncarnation,
				ref.Namespace, fmt.Sprint(ref.FileID), stableColumnAssetDiagnosticPath(ref),
				rootpublication.DurableFrontier{Bytes: uint64(ref.Offset + ref.Length)}, reach, obligations, nil)
		}
		if err != nil {
			builder.Abandon()
			return nil, err
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			builder.Abandon()
			return nil, err
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
	}
	return set, err
}
func (a *columnPhysicalAssetSegmentAppender) closeReusedProducer(validate func(*rootpublication.StableResourceSet) error) error {
	defer a.releaseLock()
	a.closeStats = columnPhysicalAssetSegmentCloseStats{CloseCount: 1}
	var err error
	if a.failed {
		err = errors.New("collections: column physical asset appender is failed")
	}
	if err == nil {
		started := time.Now()
		a.closeStats.FileSyncCount = 1
		err = a.producerDB.SyncColumnSegmentProducerV1(a.cfg.AssetManager.Namespace, a.fileID, a.producerIncarnation, nil)
		a.closeStats.FileSync = time.Since(started)
	}
	if err == nil {
		a.stableResources, err = a.captureProducerResources()
	}
	if err == nil && validate != nil {
		err = validate(a.stableResources)
	}
	if err != nil {
		// Output remains unpublished. Roll back only the exact appended suffix;
		// an error retains the DB's installed physical owner and poisons success.
		if a.stableResources != nil {
			_ = a.stableResources.Release()
			a.stableResources = nil
		}
		rollbackErr := a.producerDB.RollbackColumnSegmentProducerV1(a.cfg.AssetManager.Namespace, a.fileID, a.producerIncarnation, uint64(a.offset), uint64(a.appendStart), nil)
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr, ErrRecoveryRequired)
		}
		return err
	}
	a.closeStats.SyncEpochCount = 1
	return nil
}
