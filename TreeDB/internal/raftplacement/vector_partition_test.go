package raftplacement

import "testing"

func TestValidateVectorPartitionPlacementV1AllowsManyPartitionsPerGroup(t *testing.T) {
	c, err := Validate(validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	p := VectorPartitionPlacementRecordV1{Collection: CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}, IndexName: "embedding", IndexDefinitionDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 2, PartitionCount: 2, Partitions: []VectorPartitionGroupV1{{0, "group-a"}, {1, "group-a"}}}
	if err := c.ValidateVectorPartitionPlacementV1(p); err != nil {
		t.Fatal(err)
	}
	p.Partitions[1].GroupID = "missing"
	if err := c.ValidateVectorPartitionPlacementV1(p); err == nil {
		t.Fatal("unknown group accepted")
	}
	p.Partitions[1].GroupID = "group-a"
	p.Partitions[0], p.Partitions[1] = p.Partitions[1], p.Partitions[0]
	if err := c.ValidateVectorPartitionPlacementV1(p); err == nil {
		t.Fatal("noncanonical partition order accepted")
	}
}
