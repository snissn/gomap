package main

import "testing"

func TestValidateLocalHNSWQuerySplitPairV1(t *testing.T) {
	f := fixtureManifest{Checksum: "fixture", Queries: 4}
	c, h := localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: "fixture", TruthArtifactSHA256: "truth"}, localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: "fixture", TruthArtifactSHA256: "truth"}
	for i := 0; i < 4; i++ {
		if localHNSWCalibrationOrdinalV1(i) {
			c.Ordinals = append(c.Ordinals, i)
		} else {
			h.Ordinals = append(h.Ordinals, i)
		}
	}
	if err := validateLocalHNSWQuerySplitPairV1(c, h, f, "truth"); err != nil {
		t.Fatal(err)
	}
	h.Ordinals = append(h.Ordinals, 0)
	if err := validateLocalHNSWQuerySplitPairV1(c, h, f, "truth"); err == nil {
		t.Fatal("duplicate accepted")
	}
}
