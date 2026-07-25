package nativewire

import (
	"context"
	"errors"
	"fmt"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// CatalogVectorPartitionMutationAdmissionV1 is the concrete M7 provider for
// RaftClusterSubmitter. It resolves the bounded active lifecycle set from the
// replicated catalog authority, rather than trusting a frontend-local index
// cache, then invalidates every active index for the affected collection before
// the shared submitter encodes its deterministic mutation entry.
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
	for _, status := range a.Authority.VectorPartitionLifecycleStatusesV1() {
		identity := status.Identity
		if !status.Active || identity.Index.Collection.Database != database || identity.Index.Collection.Catalog != catalog || identity.Index.Collection.Collection != collection {
			continue
		}
		if _, err := a.Coordinator.InvalidateBeforeRelevantMutationV1(ctx, identity.Index, "nativewire relevant mutation"); err != nil {
			return err
		}
	}
	return nil
}

func vectorPartitionRelevantMutationCommandV1(command iwire.CommandID) bool {
	switch command {
	case iwire.CommandCreateCollection, iwire.CommandDropCollection, iwire.CommandCreateIndex, iwire.CommandDropIndex,
		iwire.CommandInsertBatch, iwire.CommandReplaceBatch, iwire.CommandDeleteBatch, iwire.CommandUpdateBSONSet:
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
