package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// CatalogVectorPartitionMutationAdmissionV1 is the concrete M7 provider for
// RaftClusterSubmitter. It resolves the bounded active lifecycle set from the
// replicated catalog authority, rather than trusting a frontend-local index
// cache, then invalidates every active index for the affected collection before
// the data command is offered to consensus.
type CatalogVectorPartitionMutationAdmissionV1 struct {
	Authority   *raftplacement.CatalogMetaAuthorityV1
	Coordinator raftplacement.VectorPartitionLifecycleCoordinatorV1
	Database    string
	Catalog     string
}

func (a CatalogVectorPartitionMutationAdmissionV1) AdmitVectorPartitionMutationV1(ctx context.Context, command iwire.CommandID, sections []iwire.Section) error {
	if a.Authority == nil || a.Coordinator.Authority != a.Authority || a.Coordinator.Committer == nil {
		return errors.New("nativewire: vector partition lifecycle admission is incompletely configured")
	}
	if !vectorPartitionRelevantMutationCommandV1(command) {
		return fmt.Errorf("nativewire: unclassified cluster mutation command %d", command)
	}
	collection, err := vectorPartitionMutationCollectionV1(command, sections)
	if err != nil {
		return err
	}
	database, catalog := a.Database, a.Catalog
	if database == "" {
		database = raftplacement.DefaultDatabase
	}
	if catalog == "" {
		catalog = raftplacement.DefaultCatalog
	}
	operationDigest, err := vectorPartitionMutationOperationDigestV1(command, sections)
	if err != nil {
		return err
	}
	collectionRef := raftplacement.CollectionRefV1{Database: database, Catalog: catalog, Collection: collection}
	barrier, err := a.Coordinator.BeginRelevantCollectionMutationV1(ctx, collectionRef, operationDigest)
	if err != nil {
		return err
	}
	// A completed barrier with the same operation digest is an exact
	// idempotency replay. The data Raft layer will return the already-applied
	// result, so invalidating a generation built afterward would be spurious.
	if !barrier.Pending {
		return nil
	}
	for _, status := range a.Authority.VectorPartitionLifecycleStatusesV1() {
		identity := status.Identity
		if identity.Index.Collection.Database != database || identity.Index.Collection.Catalog != catalog || identity.Index.Collection.Collection != collection {
			continue
		}
		if status.State == raftplacement.VectorPartitionLifecycleInvalidatedV1 && !status.MutationConfirmed && status.InvalidationEpoch != barrier.Epoch {
			return errors.New("nativewire: a prior vector-relevant mutation is pending outcome recovery")
		}
		switch status.State {
		case raftplacement.VectorPartitionLifecycleBuildingV1, raftplacement.VectorPartitionLifecycleStagedV1, raftplacement.VectorPartitionLifecyclePreparedV1:
			return errors.New("nativewire: vector generation source capture is in progress; relevant mutation is frozen")
		}
		if !status.Active {
			continue
		}
		if _, err := a.Coordinator.InvalidateBeforeRelevantMutationAtEpochV1(ctx, identity.Index, "nativewire relevant mutation", barrier.Epoch); err != nil {
			return err
		}
	}
	return nil
}

// ConfirmVectorPartitionMutationV1 releases pending fences for this affected
// collection. The shared submitter calls this solely after the
// data Raft bridge proves commit plus deterministic local apply; a failed or
// ambiguous submit intentionally leaves the catalog frozen.
func (a CatalogVectorPartitionMutationAdmissionV1) ConfirmVectorPartitionMutationV1(ctx context.Context, command iwire.CommandID, sections []iwire.Section) error {
	if a.Authority == nil || a.Coordinator.Authority != a.Authority || a.Coordinator.Committer == nil {
		return errors.New("nativewire: vector partition lifecycle admission is incompletely configured")
	}
	if !vectorPartitionRelevantMutationCommandV1(command) {
		return fmt.Errorf("nativewire: unclassified cluster mutation command %d", command)
	}
	collection, err := vectorPartitionMutationCollectionV1(command, sections)
	if err != nil {
		return err
	}
	database, catalog := a.Database, a.Catalog
	if database == "" {
		database = raftplacement.DefaultDatabase
	}
	if catalog == "" {
		catalog = raftplacement.DefaultCatalog
	}
	operationDigest, err := vectorPartitionMutationOperationDigestV1(command, sections)
	if err != nil {
		return err
	}
	collectionRef := raftplacement.CollectionRefV1{Database: database, Catalog: catalog, Collection: collection}
	barrier, exists, err := a.Authority.VectorPartitionCollectionMutationOperationV1(collectionRef, operationDigest)
	if err != nil {
		return err
	}
	if !exists || barrier.OperationDigest != operationDigest {
		return errors.New("nativewire: vector mutation confirmation has no matching collection barrier")
	}
	if !barrier.Pending {
		return nil
	}
	for _, status := range a.Authority.VectorPartitionLifecycleStatusesV1() {
		identity := status.Identity
		if identity.Index.Collection.Database != database || identity.Index.Collection.Catalog != catalog || identity.Index.Collection.Collection != collection ||
			status.State != raftplacement.VectorPartitionLifecycleInvalidatedV1 || status.MutationConfirmed {
			continue
		}
		if status.InvalidationEpoch != barrier.Epoch {
			return errors.New("nativewire: vector mutation confirmation found a mismatched index fence")
		}
		if err := a.Coordinator.ConfirmRelevantMutationV1(ctx, raftplacement.VectorPartitionLifecycleMutationProofV1{
			IndexIdentity: identity.Index, ActiveGeneration: identity.Generation, InvalidationEpoch: status.InvalidationEpoch,
		}); err != nil {
			return err
		}
	}
	return a.Coordinator.ConfirmRelevantCollectionMutationV1(ctx, collectionRef, operationDigest)
}

func vectorPartitionMutationOperationDigestV1(command iwire.CommandID, sections []iwire.Section) (string, error) {
	idempotencyKey, ok, err := singletonSection(sections, iwire.SectionIdempotencyKey)
	if err != nil {
		return "", err
	}
	if !ok || len(idempotencyKey) == 0 {
		return "", errors.New("nativewire: vector-relevant mutation requires an idempotency key")
	}
	h := sha256.New()
	var commandBytes [8]byte
	binary.BigEndian.PutUint64(commandBytes[:], uint64(command))
	_, _ = h.Write(commandBytes[:])
	_, _ = h.Write(idempotencyKey)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func vectorPartitionRelevantMutationCommandV1(command iwire.CommandID) bool {
	switch command {
	case iwire.CommandCreateCollection, iwire.CommandInsertBatch, iwire.CommandReplaceBatch,
		iwire.CommandDeleteBatch, iwire.CommandUpdateBSONSet:
		return true
	default:
		return false
	}
}

func vectorPartitionMutationCollectionV1(command iwire.CommandID, sections []iwire.Section) (string, error) {
	if command == iwire.CommandCreateCollection {
		raw, err := metadataSection(sections, iwire.SectionCollectionMeta)
		if err != nil {
			return "", err
		}
		meta, err := decodeCollectionMeta(raw)
		if err != nil {
			return "", err
		}
		return meta.Name, nil
	}
	return collectionNameFromSections(sections)
}
