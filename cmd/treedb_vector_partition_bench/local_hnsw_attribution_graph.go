package main

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/snissn/gomap/TreeDB/collections"
)

const localHNSWAttributionGraphSchemaV1 = "treedb_local_hnsw_attribution_graph_v1"

type localHNSWAttributionGraphAssetV1 struct {
	ID               string `json:"id"`
	Checksum         string `json:"checksum"`
	MembershipDigest string `json:"membership_digest"`
	Bytes            uint64 `json:"bytes"`
}

type localHNSWAttributionGraphPartitionV1 struct {
	Schema     string                                            `json:"schema"`
	Partition  uint32                                            `json:"partition_id"`
	Native     localHNSWAttributionGraphAssetV1                  `json:"native_asset"`
	Overlay    localHNSWAttributionGraphAssetV1                  `json:"overlay_asset"`
	Comparison collections.VectorPartitionLocalGraphComparisonV1 `json:"comparison"`
}

type localHNSWAttributionGraphRoleTotalsV1 struct {
	Rows            uint64 `json:"rows"`
	NativeSaturated uint64 `json:"native_saturated_rows"`
	FinalSaturated  uint64 `json:"final_saturated_rows"`
}

type localHNSWAttributionGraphAggregateV1 struct {
	Schema                      string                                `json:"schema"`
	TotalRows                   uint64                                `json:"total_rows"`
	NativeReachableRows         uint64                                `json:"native_reachable_rows"`
	NativeUnreachableRows       uint64                                `json:"native_unreachable_rows"`
	NativeTraversalRoots        uint64                                `json:"native_traversal_roots"`
	NativeDisconnectedPacks     uint64                                `json:"native_disconnected_packs"`
	FinalReachableRows          uint64                                `json:"final_reachable_rows"`
	FinalUnreachableRows        uint64                                `json:"final_unreachable_rows"`
	FinalTraversalRoots         uint64                                `json:"final_traversal_roots"`
	FinalDisconnectedPacks      uint64                                `json:"final_disconnected_packs"`
	Root                        localHNSWAttributionGraphRoleTotalsV1 `json:"root"`
	Internal                    localHNSWAttributionGraphRoleTotalsV1 `json:"internal"`
	Leaf                        localHNSWAttributionGraphRoleTotalsV1 `json:"leaf"`
	OverlayEdges                uint64                                `json:"overlay_edges"`
	OverlayDuplicates           uint64                                `json:"overlay_duplicates"`
	DisplacedEdges              uint64                                `json:"displaced_edges"`
	DisplacedDistanceRankCounts map[int]uint64                        `json:"displaced_distance_rank_counts"`
	NativeReciprocalEdges       uint64                                `json:"native_reciprocal_edges"`
	FinalReciprocalEdges        uint64                                `json:"final_reciprocal_edges"`
}

func localHNSWAttributionGraphEvidenceV1(source *m8ProductionMultiGroupAssetsV1, native, overlay *localHNSWVariantHarnessV1) ([]localHNSWAttributionGraphPartitionV1, localHNSWAttributionGraphAggregateV1, error) {
	aggregate := localHNSWAttributionGraphAggregateV1{Schema: localHNSWAttributionGraphSchemaV1, DisplacedDistanceRankCounts: map[int]uint64{}}
	if source == nil || source.descriptor == nil || source.manifest.PartitionCount == 0 || !reflect.DeepEqual(source.manifest, source.status.Manifest) {
		return nil, aggregate, errors.New("invalid retained local HNSW graph source")
	}
	loads, err := m8PartitionLoadsV1(source.manifest)
	if err != nil {
		return nil, aggregate, fmt.Errorf("retained local HNSW partition loads: %w", err)
	}
	if len(source.descriptor.PartitionLoads) != len(loads) {
		return nil, aggregate, errors.New("retained local HNSW descriptor loads")
	}
	for partition, load := range loads {
		if source.descriptor.PartitionLoads[partition] < 0 || uint64(source.descriptor.PartitionLoads[partition]) != load {
			return nil, aggregate, errors.New("retained local HNSW descriptor loads")
		}
	}
	if err := localHNSWAttributionGraphHarnessV1(source, native); err != nil {
		return nil, aggregate, err
	}
	if err := localHNSWAttributionGraphHarnessV1(source, overlay); err != nil {
		return nil, aggregate, err
	}
	partitions := make([]localHNSWAttributionGraphPartitionV1, source.manifest.PartitionCount)
	for partition, retained := range source.manifest.Assets {
		nativeAsset, overlayAsset := native.packAssets[partition], overlay.packAssets[partition]
		if overlayAsset.PartitionID != retained.PartitionID || overlayAsset.Checksum != retained.Checksum || overlayAsset.Bytes != retained.Bytes || overlayAsset.MembershipDigest != retained.MembershipDigest {
			return nil, aggregate, errors.New("retained local HNSW overlay asset drift")
		}
		comparison, err := collections.CompareVectorPartitionLocalGraphPacksV1(native.searchers[partition], overlay.searchers[partition])
		if err != nil {
			return nil, aggregate, fmt.Errorf("retained local HNSW graph comparison partition %d: %w", partition, err)
		}
		if comparison.Schema != collections.VectorPartitionLocalGraphComparisonSchemaV1 || comparison.Native.Rows != loads[partition] || comparison.Final.Rows != loads[partition] || comparison.Native.ReachableRows > comparison.Native.Rows || comparison.Final.ReachableRows > comparison.Final.Rows || comparison.Native.TraversalRoots == 0 || comparison.Final.TraversalRoots == 0 || len(comparison.Rows) != int(comparison.Native.Rows) {
			return nil, aggregate, errors.New("retained local HNSW graph comparison")
		}
		for _, row := range comparison.Rows {
			if row.TreeRole != "root" && row.TreeRole != "internal" && row.TreeRole != "leaf" {
				return nil, aggregate, errors.New("retained local HNSW graph comparison")
			}
		}
		partitions[partition] = localHNSWAttributionGraphPartitionV1{Schema: localHNSWAttributionGraphSchemaV1, Partition: uint32(partition), Native: localHNSWAttributionGraphAssetV1{ID: nativeAsset.ID, Checksum: nativeAsset.Checksum, MembershipDigest: nativeAsset.MembershipDigest, Bytes: nativeAsset.Bytes}, Overlay: localHNSWAttributionGraphAssetV1{ID: overlayAsset.ID, Checksum: overlayAsset.Checksum, MembershipDigest: overlayAsset.MembershipDigest, Bytes: overlayAsset.Bytes}, Comparison: comparison}
		localHNSWAttributionGraphAggregateComparisonV1(&aggregate, comparison)
	}
	return partitions, aggregate, nil
}

func localHNSWAttributionGraphHarnessV1(source *m8ProductionMultiGroupAssetsV1, harness *localHNSWVariantHarnessV1) error {
	if harness == nil || harness.assets == nil || !reflect.DeepEqual(harness.assets.manifest, source.manifest) || len(harness.packAssets) != int(source.manifest.PartitionCount) || len(harness.searchers) != int(source.manifest.PartitionCount) || len(source.manifest.Assets) != int(source.manifest.PartitionCount) {
		return errors.New("retained local HNSW partition coverage")
	}
	for partition, retained := range source.manifest.Assets {
		asset := harness.packAssets[partition]
		if retained.PartitionID != uint32(partition) || asset.PartitionID != uint32(partition) || harness.searchers[partition] == nil || asset.ID == "" || asset.Bytes == 0 || !localHNSWAttributionSHA256V1(asset.Checksum) || !localHNSWAttributionSHA256V1(asset.MembershipDigest) {
			return errors.New("retained local HNSW partition asset")
		}
	}
	return nil
}

func localHNSWAttributionGraphAggregateComparisonV1(aggregate *localHNSWAttributionGraphAggregateV1, comparison collections.VectorPartitionLocalGraphComparisonV1) {
	aggregate.TotalRows += comparison.Native.Rows
	aggregate.NativeReachableRows += comparison.Native.ReachableRows
	aggregate.NativeUnreachableRows += comparison.Native.Rows - comparison.Native.ReachableRows
	aggregate.NativeTraversalRoots += comparison.Native.TraversalRoots
	aggregate.FinalReachableRows += comparison.Final.ReachableRows
	aggregate.FinalUnreachableRows += comparison.Final.Rows - comparison.Final.ReachableRows
	aggregate.FinalTraversalRoots += comparison.Final.TraversalRoots
	if comparison.Native.TraversalRoots > 1 {
		aggregate.NativeDisconnectedPacks++
	}
	if comparison.Final.TraversalRoots > 1 {
		aggregate.FinalDisconnectedPacks++
	}
	aggregate.NativeReciprocalEdges += comparison.NativeReciprocalEdges
	aggregate.FinalReciprocalEdges += comparison.FinalReciprocalEdges
	for _, row := range comparison.Rows {
		var totals *localHNSWAttributionGraphRoleTotalsV1
		switch row.TreeRole {
		case "root":
			totals = &aggregate.Root
		case "internal":
			totals = &aggregate.Internal
		case "leaf":
			totals = &aggregate.Leaf
		default:
			continue
		}
		totals.Rows++
		if row.NativeSaturated {
			totals.NativeSaturated++
		}
		if row.FinalSaturated {
			totals.FinalSaturated++
		}
		aggregate.OverlayEdges += uint64(row.OverlayEdges)
		aggregate.OverlayDuplicates += uint64(row.OverlayDuplicates)
		aggregate.DisplacedEdges += uint64(row.Displaced)
		for _, edge := range row.DisplacedEdges {
			aggregate.DisplacedDistanceRankCounts[edge.DistanceRank]++
		}
	}
}
