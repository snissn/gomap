// Package vectorpartitionplacement binds durable collection manifests to the
// Raft placement catalog without making collections depend on raftcluster.
package vectorpartitionplacement

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func RecordV1(m collections.VectorPartitionManifestV1, collection raftplacement.CollectionRefV1) (raftplacement.VectorPartitionPlacementRecordV1, error) {
	if collection.Collection != m.Collection {
		return raftplacement.VectorPartitionPlacementRecordV1{}, fmt.Errorf("vectorpartitionplacement: collection mismatch")
	}
	parts := make([]raftplacement.VectorPartitionGroupV1, len(m.Placements))
	for i, p := range m.Placements {
		parts[i] = raftplacement.VectorPartitionGroupV1{PartitionID: p.PartitionID, GroupID: raftcluster.GroupID(p.GroupID)}
	}
	return raftplacement.VectorPartitionPlacementRecordV1{Collection: collection, IndexName: m.IndexName, IndexDefinitionDigest: m.IndexDefinitionDigest, SourceGeneration: m.SourceGeneration, SourceChecksum: m.SourceChecksum, SourceSchemaHash: m.SourceSchemaHash, SourceRowCount: m.SourceRowCount, PartitionGeneration: m.Generation, PartitionCount: m.PartitionCount, Partitions: parts}, nil
}

func ValidateReadyManifestV1(m collections.VectorPartitionManifestV1, collection raftplacement.CollectionRefV1, catalog raftplacement.ResolvedCatalogV1) error {
	if m.State != "ready" {
		return fmt.Errorf("vectorpartitionplacement: manifest is not ready")
	}
	record, err := RecordV1(m, collection)
	if err != nil {
		return err
	}
	return catalog.ValidateVectorPartitionPlacementV1(record)
}
