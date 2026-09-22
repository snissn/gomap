package main

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionGraphEvidenceV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8HistoricalOverlayRetainedAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil {
		t.Fatal(err)
	}
	source.descriptor = &m3VariantDescriptorV1{PartitionLoads: make([]int, len(loads))}
	for i, load := range loads {
		source.descriptor.PartitionLoads[i] = int(load)
	}
	native, err := materializeHistoricalLocalHNSWVariantV1(source, collections.VectorPartitionLocalGraphVariantNativeV1, 9983)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	overlay, err := materializeHistoricalLocalHNSWVariantV1(source, collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 9984)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.Close()
	partitions, aggregate, err := localHNSWAttributionGraphEvidenceV1(source, native, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.Schema != localHNSWAttributionGraphSchemaV1 || len(partitions) != len(loads) || aggregate.TotalRows != uint64(len(vectors)) || aggregate.NativeReachableRows+aggregate.NativeUnreachableRows != aggregate.TotalRows || aggregate.FinalReachableRows+aggregate.FinalUnreachableRows != aggregate.TotalRows || aggregate.Root.Rows+aggregate.Internal.Rows+aggregate.Leaf.Rows != aggregate.TotalRows || aggregate.NativeReciprocalEdges == 0 || aggregate.FinalReciprocalEdges == 0 {
		t.Fatalf("partitions=%+v aggregate=%+v", partitions, aggregate)
	}
	for p, partition := range partitions {
		if partition.Schema != localHNSWAttributionGraphSchemaV1 || partition.Partition != uint32(p) || partition.Comparison.Native.Rows != loads[p] || partition.Comparison.Final.Rows != loads[p] || partition.Overlay.Checksum != source.manifest.Assets[p].Checksum || partition.Overlay.Bytes != source.manifest.Assets[p].Bytes || partition.Overlay.MembershipDigest != source.manifest.Assets[p].MembershipDigest {
			t.Fatalf("partition=%+v", partition)
		}
	}
	checksum := overlay.packAssets[0].Checksum
	overlay.packAssets[0].Checksum = strings.Repeat("0", 64)
	if _, _, err := localHNSWAttributionGraphEvidenceV1(source, native, overlay); err == nil {
		t.Fatal("overlay identity drift accepted")
	}
	overlay.packAssets[0].Checksum = checksum
}
