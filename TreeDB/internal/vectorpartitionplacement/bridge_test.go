package vectorpartitionplacement

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestValidateReadyManifestV1RejectsUnknownGroup(t *testing.T) {
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	m := collections.VectorPartitionManifestV1{Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: strings.Repeat("a", 64), SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 1, Generation: 1, RouterGeneration: 1, PartitionCount: 1, BalancePolicy: "disjoint_v1", Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "missing"}}, Memberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}}, Assets: []collections.VectorPartitionAssetV1{{ID: "p", PartitionID: 0, Checksum: strings.Repeat("b", 64), Bytes: 1, Ref: collections.ColumnAssetRef{Kind: collections.ColumnAssetKindTCS1PartImage, Namespace: "n", Generation: 1, PartID: 1, FileID: 1, Length: 1}}}, RouterAsset: collections.VectorPartitionAssetV1{ID: "r", Checksum: strings.Repeat("c", 64), Bytes: 1, Ref: collections.ColumnAssetRef{Kind: collections.ColumnAssetKindTCS1PartImage, Namespace: "n", Generation: 1, PartID: 2, FileID: 2, Length: 1}}}
	m.Canonicalize()
	if err := ValidateReadyManifestV1(m, raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}, catalog); err == nil {
		t.Fatal("unknown group accepted")
	}
}
