package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestVectorPartitionGenerationSearchOpenPlanV1IndexesAndOwnsInputs(t *testing.T) {
	manifest := VectorPartitionManifestV1{
		Collection:            "docs",
		IndexName:             "embedding",
		IndexDefinitionDigest: "definition",
		SourceGeneration:      11,
		SourceChecksum:        22,
		SourceSchemaHash:      33,
		Generation:            7,
		PartitionCount:        2,
		Assets: []VectorPartitionAssetV1{
			{ID: vectorPartitionLocalAssetIDV1(0), PartitionID: 0},
			{ID: vectorPartitionLocalAssetIDV1(1), PartitionID: 1},
		},
		Memberships: []VectorPartitionMembershipV1{
			{VectorOrdinal: 3, PartitionID: 1},
			{VectorOrdinal: 0, PartitionID: 0},
		},
		OverlapMemberships: []VectorPartitionMembershipV1{
			{VectorOrdinal: 2, PartitionID: 0},
			{VectorOrdinal: 1, PartitionID: 1},
		},
	}
	plan, err := NewVectorPartitionGenerationSearchOpenPlanWithContextV1(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Assets[0].ID = "mutated"
	manifest.Memberships[0].VectorOrdinal = 99
	asset, members, home, overlap, err := plan.partition(0)
	if err != nil {
		t.Fatal(err)
	}
	if asset.ID != vectorPartitionLocalAssetIDV1(0) || home != 1 || overlap != 1 || len(members) != 2 ||
		members[0] != (vectorPartitionMembershipSourceV1{ordinal: 0, kind: VectorPartitionMembershipHomeV1}) ||
		members[1] != (vectorPartitionMembershipSourceV1{ordinal: 2, kind: VectorPartitionMembershipOverlapV1}) {
		t.Fatalf("partition plan asset=%+v members=%+v home=%d overlap=%d", asset, members, home, overlap)
	}
	if _, _, _, _, err := plan.partition(2); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("out-of-range partition err=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewVectorPartitionGenerationSearchOpenPlanWithContextV1(canceled, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled plan err=%v", err)
	}
	manifest.Memberships = append(manifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: 4, PartitionID: 2})
	if _, err := NewVectorPartitionGenerationSearchOpenPlanWithContextV1(t.Context(), manifest); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("out-of-range membership err=%v", err)
	}
}

func TestAppendVectorPartitionMembershipsToPlanV1FailsClosedOnChangedInput(t *testing.T) {
	plan := &VectorPartitionGenerationSearchOpenPlanV1{
		partitionCount: 2,
		memberOffsets:  []int{0, 1, 2},
		members:        make([]vectorPartitionMembershipSourceV1, 2),
	}
	next := []int{0, 1}
	err := appendVectorPartitionMembershipsToPlanV1(t.Context(), plan, []VectorPartitionMembershipV1{
		{VectorOrdinal: 1, PartitionID: 0},
		{VectorOrdinal: 2, PartitionID: 0},
	}, VectorPartitionMembershipHomeV1, next)
	if !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("changed membership input err=%v", err)
	}
	if next[0] != 1 || plan.members[1] != (vectorPartitionMembershipSourceV1{}) {
		t.Fatalf("changed membership input spilled into adjacent run: next=%v members=%+v", next, plan.members)
	}

	err = appendVectorPartitionMembershipsToPlanV1(t.Context(), plan, []VectorPartitionMembershipV1{{
		VectorOrdinal: 3,
		PartitionID:   2,
	}}, VectorPartitionMembershipHomeV1, []int{0, 1})
	if !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("out-of-range changed input err=%v", err)
	}
}

func TestOpenVectorPartitionLocalSearcherForGenerationWithContextV1RejectsCanceledColdOpen(t *testing.T) {
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}})
	defer func() {
		if err := d.Close(); err != nil {
			t.Errorf("close DB: %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := col.OpenVectorPartitionLocalSearcherForGenerationWithContextV1(ctx, def.Name, 1, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cold open err=%v", err)
	}
}

func TestOpenVectorPartitionLocalSearcherForOfflineAssetV1FailsClosed(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, err := col.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testVectorPartitionManifestV1()
	manifest.State, manifest.RouterGeneration, manifest.RouterAsset, manifest.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	manifest.Collection, manifest.IndexName, manifest.IndexDefinitionDigest = col.name, def.Name, VectorIndexDefinitionDigestV1(def)
	manifest.Generation, manifest.PartitionCount = 91, 2
	manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, manifest.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	manifest.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1}}
	manifest.Canonicalize()
	inputs := []VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: def.Dimensions}, {Source: source, Generation: manifest.Generation, PartitionID: 1, Dimensions: def.Dimensions}}
	assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, manifest, 977, inputs, VectorPartitionLocalGraphVariantNativeV1)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	searcher, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, manifest, assets[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	members := vectorPartitionMembershipsForPartitionV1(manifest, assets[0].PartitionID)
	if _, err := col.openVectorPartitionLocalSearcherForPreparedPartitionWithContextV1(t.Context(), def.Name, manifest.Generation, assets[0].PartitionID, manifest.IndexDefinitionDigest, manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, &assets[0], members, len(members), 0, false); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("production open accepted native v2 pack: %v", err)
	}
	nativeManifest := manifest
	nativeManifest.Assets = assets
	nativeManifest.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(nativeManifest, nil); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("native offline pack publication err=%v", err)
	}
	wrongPartition := assets[0]
	wrongPartition.PartitionID = 1
	if _, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, manifest, wrongPartition); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("wrong partition err=%v", err)
	}
	wrongSource := manifest
	wrongSource.SourceChecksum++
	if _, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, wrongSource, assets[0]); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("wrong source err=%v", err)
	}
	wrongMembers := manifest
	wrongMembers.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 1, PartitionID: 0}, {VectorOrdinal: 0, PartitionID: 1}}
	wrongMembers.Canonicalize()
	if _, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, wrongMembers, assets[0]); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("wrong membership err=%v", err)
	}
	wrongAsset := assets[0]
	wrongAsset.ID = "wrong"
	if _, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, manifest, wrongAsset); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("wrong asset err=%v", err)
	}
}

func TestCompareVectorPartitionLocalGraphPacksV1RejectsNonOverlayNativePack(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	rows := make([]columnGraphRebuildInputRowV2A, 8)
	for i := range rows {
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("v%d", i), vector: []float32{float32(i + 1), float32(i%3 + 1), 1}}
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, err := col.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State = "building"
	m.Collection = col.name
	m.IndexName = def.Name
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	m.Generation = 92
	m.PartitionCount = 1
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	m.Memberships = make([]VectorPartitionMembershipV1, len(rows))
	for i := range rows {
		m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
	}
	m.Canonicalize()
	in := []VectorPartitionSearchAssetV1{{Source: source, Generation: m.Generation, PartitionID: 0, Dimensions: def.Dimensions}}
	native, nr, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 981, in, VectorPartitionLocalGraphVariantNativeV1)
	if err != nil {
		t.Fatal(err)
	}
	defer nr.Release()
	auxiliary, ar, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 983, in, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1)
	if err != nil {
		t.Fatal(err)
	}
	defer ar.Release()
	nativeRaw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), native[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	auxiliaryRaw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), auxiliary[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	auxiliaryAgain, ar2, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 984, in, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1)
	if err != nil {
		t.Fatal(err)
	}
	defer ar2.Release()
	auxiliaryAgainRaw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), auxiliaryAgain[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(auxiliaryRaw, auxiliaryAgainRaw) {
		t.Fatal("auxiliary rebuild bytes differ")
	}
	nativeDigest, err := decodeVectorPartitionMembershipDigestV1(native[0].MembershipDigest)
	if err != nil {
		t.Fatal(err)
	}
	auxiliaryDigest, err := decodeVectorPartitionMembershipDigestV1(auxiliary[0].MembershipDigest)
	if err != nil {
		t.Fatal(err)
	}
	nativePack, err := decodeColumnHNSWSearchPack(nativeRaw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: m.SourceGeneration, ManifestChecksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash}, ExpectedMembershipDigest: nativeDigest})
	if err != nil {
		t.Fatal(err)
	}
	auxiliaryPack, err := decodeColumnHNSWSearchPack(auxiliaryRaw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: m.SourceGeneration, ManifestChecksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash}, ExpectedMembershipDigest: auxiliaryDigest})
	if err != nil {
		t.Fatal(err)
	}
	if hnswPackU16(auxiliaryRaw, columnHNSWSearchPackHeaderVersionOffset) != columnHNSWSearchPackVersionV3 || !auxiliaryPack.Header.HasAuxiliaryNavigation || hnswPackU16(nativeRaw, columnHNSWSearchPackHeaderVersionOffset) != columnHNSWSearchPackVersionV2 {
		t.Fatalf("pack versions native=%d auxiliary=%d headers=%+v", hnswPackU16(nativeRaw, columnHNSWSearchPackHeaderVersionOffset), hnswPackU16(auxiliaryRaw, columnHNSWSearchPackHeaderVersionOffset), auxiliaryPack.Header)
	}
	if !reflect.DeepEqual(nativePack.AdjacencyLayers, auxiliaryPack.AdjacencyLayers) {
		t.Fatal("auxiliary default changed native adjacency")
	}
	seen := make([]bool, auxiliaryPack.Header.Rows)
	queue := []int{auxiliaryPack.Header.EntryOrdinal}
	seen[auxiliaryPack.Header.EntryOrdinal] = true
	for head := 0; head < len(queue); head++ {
		ordinal := queue[head]
		for _, layer := range []columnHNSWSearchPackLayer{auxiliaryPack.AdjacencyLayers[0], auxiliaryPack.AuxiliaryNavigation} {
			for _, neighbor := range layer.Neighbors[layer.Offsets[ordinal]:layer.Offsets[ordinal+1]] {
				if !seen[neighbor] {
					seen[neighbor] = true
					queue = append(queue, int(neighbor))
				}
			}
		}
	}
	for ordinal, reached := range seen {
		if !reached {
			t.Fatalf("native plus auxiliary did not reach ordinal=%d", ordinal)
		}
	}
	overlay, or, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 982, in, VectorPartitionLocalGraphVariantOverlayCurrentV1)
	if err != nil {
		t.Fatal(err)
	}
	defer or.Release()
	n, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, m, native[0])
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	o, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, m, overlay[0])
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	comparison, err := CompareVectorPartitionLocalGraphPacksV1(n, o)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.NativeReciprocalEdges == 0 || comparison.FinalReciprocalEdges == 0 {
		t.Fatalf("reciprocity native=%d final=%d", comparison.NativeReciprocalEdges, comparison.FinalReciprocalEdges)
	}
	if comparison.Schema != VectorPartitionLocalGraphComparisonSchemaV1 || comparison.EntryOrdinal != 0 || comparison.NativeDistances.Count == 0 || comparison.FinalDistances.Count == 0 || comparison.NativeDistances.Count != comparison.Native.EdgesByLayer[0] || comparison.FinalDistances.Count != comparison.Final.EdgesByLayer[0] {
		t.Fatalf("comparison summary=%+v", comparison)
	}
	for _, summary := range []VectorPartitionLocalGraphDistanceDistributionV1{comparison.NativeDistances, comparison.FinalDistances} {
		if math.IsNaN(summary.Mean) || math.IsInf(summary.Mean, 0) || summary.Min > summary.P50 || summary.P50 > summary.P95 || summary.P95 > summary.P99 || summary.P99 > summary.Max || summary.Mean < summary.Min || summary.Mean > summary.Max {
			t.Fatalf("distance summary=%+v", summary)
		}
	}
	found := false
	for _, row := range comparison.Rows {
		for _, edge := range row.DisplacedEdges {
			if edge.DistanceRank > 0 && edge.Distance >= 0 && edge.NativeReciprocal {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("missing scored reciprocal displaced edge: %+v", comparison.Rows)
	}
	query := []float32{1, 1, 1}
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 4}
	ordinary, ordinaryMetrics, err := o.SearchWithOptionsV1(t.Context(), query, opts)
	if err != nil {
		t.Fatal(err)
	}
	attributed, attributedMetrics, attribution, err := o.SearchWithAttributionV1(t.Context(), query, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ordinary, attributed) || ordinaryMetrics != attributedMetrics {
		t.Fatalf("attribution parity ordinary=%+v/%+v attributed=%+v/%+v", ordinary, ordinaryMetrics, attributed, attributedMetrics)
	}
	if attribution.Schema != "treedb_vector_partition_search_attribution_v1" || attribution.VisitedRows == 0 || attribution.VisitedRows != uint64(len(attribution.VisitedOrdinals)) || attribution.FrontierAdmissions == 0 || len(attribution.VisitedOrdinalsSHA256) != 64 {
		t.Fatalf("attribution=%+v", attribution)
	}
	h := sha256.New()
	h.Write([]byte("treedb_vector_partition_search_attribution_v1/visited/"))
	var raw [4]byte
	for i, x := range attribution.VisitedOrdinals {
		if i > 0 && attribution.VisitedOrdinals[i-1] >= x {
			t.Fatalf("visited=%v", attribution.VisitedOrdinals)
		}
		binary.LittleEndian.PutUint32(raw[:], x)
		h.Write(raw[:])
	}
	if hex.EncodeToString(h.Sum(nil)) != attribution.VisitedOrdinalsSHA256 {
		t.Fatalf("digest=%s", attribution.VisitedOrdinalsSHA256)
	}
	switch attribution.TerminationReason {
	case "candidate_limit", "frontier_empty_retained_full", "frontier_empty_no_seed", "distance_bound":
	default:
		t.Fatalf("termination=%q", attribution.TerminationReason)
	}
	if _, err = CompareVectorPartitionLocalGraphPacksV1(n, n); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("native pack accepted as overlay err=%v", err)
	}
}

func TestValidVectorPartitionLocalGraphL0V1RejectsFutureOffset(t *testing.T) {
	if !validVectorPartitionLocalGraphL0V1(3, columnHNSWSearchPackPreparedLayer{Offsets: []uint64{0, 1, 2, 2}, Neighbors: []uint32{2, 0}}) {
		t.Fatal("valid L0 rejected")
	}
	if validVectorPartitionLocalGraphL0V1(3, columnHNSWSearchPackPreparedLayer{Offsets: []uint64{0, 1, 3, 2}, Neighbors: []uint32{2, 0}}) {
		t.Fatal("decreasing future offset accepted")
	}
}

func TestVectorPartitionLocalGraphOverlayMutationChangesTraversalAndTopK(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	rows := make([]columnGraphRebuildInputRowV2A, 64)
	state := uint64(0x4105)
	for i := range rows {
		vector := make([]float32, 8)
		for d := range vector {
			state += 0x9e3779b97f4a7c15
			x := state
			x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
			x = (x ^ x>>27) * 0x94d049bb133111eb
			x ^= x >> 31
			vector[d] = float32(int32(x>>32)) / float32(1<<31)
		}
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("v%d", i), vector: vector}
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 8, 2, rows)
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, err := col.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.Collection, m.IndexName = "building", col.name, def.Name
	m.IndexDefinitionDigest, m.Generation, m.PartitionCount = VectorIndexDefinitionDigestV1(def), 93, 1
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	m.Memberships = make([]VectorPartitionMembershipV1, len(rows))
	for i := range rows {
		m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
	}
	m.Canonicalize()
	in := []VectorPartitionSearchAssetV1{{Source: source, Generation: m.Generation, PartitionID: 0, Dimensions: def.Dimensions}}
	native, nr, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 983, in, VectorPartitionLocalGraphVariantNativeV1)
	if err != nil {
		t.Fatal(err)
	}
	defer nr.Release()
	overlay, or, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 984, in, VectorPartitionLocalGraphVariantOverlayCurrentV1)
	if err != nil {
		t.Fatal(err)
	}
	defer or.Release()
	repaired, rr, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, 985, in, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1)
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	n, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, m, native[0])
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	o, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, m, overlay[0])
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	r, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, m, repaired[0])
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	comparison, err := CompareVectorPartitionLocalGraphPacksV1(n, o)
	if err != nil {
		t.Fatal(err)
	}
	displaced03 := false
	for _, edge := range comparison.Rows[0].DisplacedEdges {
		displaced03 = displaced03 || edge.NeighborOrdinal == 3
	}
	if !displaced03 {
		t.Fatalf("missing displaced native edge 0->3: %+v", comparison.Rows[0])
	}
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 1}
	nativeResults, nativeMetrics, nativeTrace, err := n.SearchWithAttributionV1(t.Context(), rows[0].vector, opts)
	if err != nil {
		t.Fatal(err)
	}
	overlayResults, overlayMetrics, overlayTrace, err := o.SearchWithAttributionV1(t.Context(), rows[0].vector, opts)
	if err != nil {
		t.Fatal(err)
	}
	repairedResults, repairedMetrics, repairedTrace, err := r.SearchWithAttributionV1(t.Context(), rows[0].vector, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(nativeResults) != 1 || nativeResults[0].ID != "v40" || len(overlayResults) != 1 || overlayResults[0].ID != "v0" {
		t.Fatalf("native=%+v overlay=%+v", nativeResults, overlayResults)
	}
	if len(repairedResults) != 1 || repairedResults[0].ID != nativeResults[0].ID || repairedMetrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 || !slices.Contains(repairedTrace.VisitedOrdinals, uint32(3)) {
		t.Fatalf("repaired results/metrics/trace=%+v/%+v/%+v", repairedResults, repairedMetrics, repairedTrace)
	}
	if nativeMetrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 || overlayMetrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 || nativeTrace.VisitedOrdinalsSHA256 == overlayTrace.VisitedOrdinalsSHA256 {
		t.Fatalf("native metrics/trace=%+v/%+v overlay=%+v/%+v", nativeMetrics, nativeTrace, overlayMetrics, overlayTrace)
	}
	if !slices.Contains(nativeTrace.VisitedOrdinals, uint32(3)) || slices.Contains(overlayTrace.VisitedOrdinals, uint32(3)) {
		t.Fatalf("displaced endpoint traversal native=%v overlay=%v", nativeTrace.VisitedOrdinals, overlayTrace.VisitedOrdinals)
	}
}

func TestVectorPartitionLocalDefaultMaterializationVariantV1(t *testing.T) {
	if got, want := vectorPartitionLocalDefaultGraphVariantV1, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1; got != want {
		t.Fatalf("default materialization variant=%q want %q", got, want)
	}
	def := VectorIndexDefinition{M: 16, EfConstruction: 128}
	buildDef, hasAuxiliaryNavigation, err := vectorPartitionLocalGraphVariantDefinitionV1(def, vectorPartitionLocalDefaultGraphVariantV1)
	if err != nil {
		t.Fatal(err)
	}
	if buildDef.M != 18 || buildDef.EfConstruction != 256 || !hasAuxiliaryNavigation {
		t.Fatalf("default materialization definition=%+v auxiliary=%t", buildDef, hasAuxiliaryNavigation)
	}
	var membership [sha256.Size]byte
	membership[0] = 1
	if got, want := vectorPartitionLocalGraphVariantMembershipDigestV1(membership, vectorPartitionLocalDefaultGraphVariantV1), vectorPartitionLocalGraphVariantMembershipDigestV1(membership, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1); got != want {
		t.Fatalf("default materialization membership identity=%x want M18/eFC256 domain=%x", got, want)
	}
	if got, m16 := vectorPartitionLocalGraphVariantMembershipDigestV1(membership, vectorPartitionLocalDefaultGraphVariantV1), vectorPartitionLocalGraphVariantMembershipDigestV1(membership, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1); got == m16 {
		t.Fatal("default materialization retained the M16 membership identity")
	}
}

func TestVectorPartitionOfflineAuxiliaryConstructionVariantsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	rows := make([]columnGraphRebuildInputRowV2A, 8)
	for i := range rows {
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("v%d", i), vector: []float32{float32(i + 1), float32(i%3 + 1), 1}}
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, err := col.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.Collection, m.IndexName = "building", col.name, def.Name
	m.IndexDefinitionDigest, m.Generation, m.PartitionCount = VectorIndexDefinitionDigestV1(def), 94, 1
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	m.Memberships = make([]VectorPartitionMembershipV1, len(rows))
	for i := range rows {
		m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
	}
	m.Canonicalize()
	in := []VectorPartitionSearchAssetV1{{Source: source, Generation: m.Generation, PartitionID: 0, Dimensions: def.Dimensions}}
	canonical, canonicalResources, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, m, 986, in)
	if err != nil {
		t.Fatal(err)
	}
	defer canonicalResources.Release()
	sourceReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sourceReader.Close()
	members, err := vectorPartitionMembershipsForPartitionWithContextV1(t.Context(), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	membershipDigest, err := vectorPartitionMembershipDigestV1(sourceReader, m.Generation, 0, members)
	if err != nil {
		t.Fatal(err)
	}
	canonicalDigest, err := decodeVectorPartitionMembershipDigestV1(canonical[0].MembershipDigest)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalDigest == vectorPartitionLocalGraphVariantMembershipDigestV1(membershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationV1) {
		t.Fatal("default materializer retained the M16 membership identity")
	}
	if got, want := canonicalDigest, vectorPartitionLocalGraphVariantMembershipDigestV1(membershipDigest, VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1); got != want {
		t.Fatalf("default materializer membership digest=%x want M18/eFC256 domain=%x", got, want)
	}
	canonicalRaw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), canonical[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	canonicalPack, err := decodeColumnHNSWSearchPack(canonicalRaw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: m.SourceGeneration, ManifestChecksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash}, ExpectedMembershipDigest: canonicalDigest})
	if err != nil || canonicalPack.Header.M != 18 || canonicalPack.Header.EfConstruction != 256 || !canonicalPack.Header.HasAuxiliaryNavigation {
		t.Fatalf("default materializer pack=%+v err=%v", canonicalPack.Header, err)
	}
	if got := col.Meta().VectorIndexes[0]; got.M != def.M || got.EfConstruction != 128 || VectorIndexDefinitionDigestV1(got) != m.IndexDefinitionDigest {
		t.Fatalf("authoritative definition drifted: %+v", got)
	}
	for _, test := range []struct {
		variant VectorPartitionLocalGraphVariantV1
		m       int
		ef      int
		fileID  uint32
	}{
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, def.M, 128, 994},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction256V1, def.M, 256, 987},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationEfConstruction512V1, def.M, 512, 988},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 18, 256, 989},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1, 20, 256, 990},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1, 22, 256, 991},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationM24EfConstruction256V1, 24, 256, 992},
		{VectorPartitionLocalGraphVariantAuxiliaryNavigationM32EfConstruction256V1, 32, 256, 993},
	} {
		assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(def.Name, m, test.fileID, in, test.variant)
		if err != nil {
			t.Fatal(err)
		}
		defer resources.Release()
		if test.variant == VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1 {
			if assets[0].MembershipDigest != canonical[0].MembershipDigest {
				t.Fatalf("variant=%s did not retain default M18/eFC256 membership digest", test.variant)
			}
		} else if assets[0].MembershipDigest == canonical[0].MembershipDigest {
			t.Fatalf("variant=%s retained default M18/eFC256 membership digest", test.variant)
		}
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), assets[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := decodeVectorPartitionMembershipDigestV1(assets[0].MembershipDigest)
		if err != nil {
			t.Fatal(err)
		}
		pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: m.SourceGeneration, ManifestChecksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash}, ExpectedMembershipDigest: digest})
		if err != nil || pack.Header.M != test.m || pack.Header.EfConstruction != test.ef || !pack.Header.HasAuxiliaryNavigation || hnswPackU16(raw, columnHNSWSearchPackHeaderVersionOffset) != columnHNSWSearchPackVersionV3 {
			t.Fatalf("variant=%s pack=%+v err=%v", test.variant, pack.Header, err)
		}
		searcher, err := col.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(t.Context(), def.Name, m, assets[0])
		if err != nil {
			t.Fatal(err)
		}
		diagnostics, err := searcher.PackDiagnosticsV1()
		closeErr := searcher.Close()
		if err != nil || closeErr != nil || diagnostics.CombinedReachableRows != diagnostics.Rows {
			t.Fatalf("variant=%s diagnostics=%+v err=%v close=%v", test.variant, diagnostics, err, closeErr)
		}
		members, err := vectorPartitionMembershipsForPartitionWithContextV1(t.Context(), m, 0)
		if err != nil {
			t.Fatal(err)
		}
		production, productionErr := col.openVectorPartitionLocalSearcherForPreparedPartitionWithContextV1(t.Context(), def.Name, m.Generation, 0, m.IndexDefinitionDigest, m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, &assets[0], members, len(members), 0, false)
		if test.variant == VectorPartitionLocalGraphVariantAuxiliaryNavigationV1 || test.variant == VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1 {
			if productionErr != nil {
				t.Fatalf("variant=%s production open err=%v", test.variant, productionErr)
			}
			if err := production.Close(); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(productionErr, ErrVectorPartitionSearchUnavailable) {
			t.Fatalf("variant=%s production open err=%v", test.variant, productionErr)
		}
	}
}

func TestVectorPartitionLocalSearcherV1AuxiliaryMetricsAndDiagnostics(t *testing.T) {
	input := testColumnHNSWSearchPackAuxiliaryInput4106()
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	searcher := &VectorPartitionLocalSearcherV1{
		asset:       VectorPartitionSearchAssetV1{Generation: 1, PartitionID: 0, Dimensions: 3},
		prepared:    view,
		opened:      1,
		searchRoute: VectorPartitionSearchRouteHNSWSearchPackV1,
	}
	defer searcher.Close()
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 1}
	ordinary, ordinaryMetrics, err := searcher.SearchWithOptionsV1(t.Context(), []float32{0, 0, 1}, opts)
	if err != nil {
		t.Fatal(err)
	}
	attributed, attributedMetrics, _, err := searcher.SearchWithAttributionV1(t.Context(), []float32{0, 0, 1}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordinary, attributed) || ordinaryMetrics != attributedMetrics || len(ordinary) != 1 || ordinary[0].ID != "doc-c" || ordinaryMetrics.AuxiliaryEdges == 0 || ordinaryMetrics.AuxiliaryCandidates == 0 || ordinaryMetrics.AuxiliaryAdmissions == 0 || ordinaryMetrics.AuxiliaryAdmissions > ordinaryMetrics.AuxiliaryCandidates {
		t.Fatalf("ordinary=%+v/%+v attributed=%+v/%+v", ordinary, ordinaryMetrics, attributed, attributedMetrics)
	}
	diagnostics, err := searcher.PackDiagnosticsV1()
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics.ReachableRows != 2 || diagnostics.TraversalRoots != 2 || diagnostics.CombinedReachableRows != 3 || diagnostics.AuxiliaryEdges != 2 || diagnostics.AuxiliaryCSRBytes != 40 || diagnostics.AuxiliaryMaxDegree != 1 {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
	status := searcher.Status()
	if status.AuxiliaryEdges != ordinaryMetrics.AuxiliaryEdges+attributedMetrics.AuxiliaryEdges || status.AuxiliaryCandidates != ordinaryMetrics.AuxiliaryCandidates+attributedMetrics.AuxiliaryCandidates || status.AuxiliaryAdmissions != ordinaryMetrics.AuxiliaryAdmissions+attributedMetrics.AuxiliaryAdmissions {
		t.Fatalf("status=%+v ordinary=%+v attributed=%+v", status, ordinaryMetrics, attributedMetrics)
	}
}

func TestVectorPartitionPersistentLocalSearcherReopenCorruptionAndPinsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	makeGeneration := func(g uint64, file uint32) VectorPartitionManifestV1 {
		m := testVectorPartitionManifestV1()
		m.State = "building"
		m.RouterGeneration = 0
		m.RouterAsset = VectorPartitionAssetV1{}
		m.ReadySetDigest = ""
		m.Generation = g
		m.IndexName = def.Name
		m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
		_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
		if err != nil {
			t.Fatal(err)
		}
		m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
		source := VectorPartitionSourceIdentityV1{Generation: m.SourceGeneration, Checksum: m.SourceChecksum, SchemaHash: m.SourceSchemaHash, RowCount: m.SourceRowCount}
		in := []VectorPartitionSearchAssetV1{{Source: source, ManifestChecksum: m.IntegrityDigest, Generation: g, PartitionID: 0, Dimensions: 3, IDs: []string{"a"}, Vectors: [][]float32{{1, 0, 0}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1}, Adjacency: [][]uint32{{}}}, {Source: source, ManifestChecksum: m.IntegrityDigest, Generation: g, PartitionID: 1, Dimensions: 3, IDs: []string{"b"}, Vectors: [][]float32{{0, 1, 0}}, Kinds: []VectorPartitionMembershipKindV1{VectorPartitionMembershipHomeV1}, Adjacency: [][]uint32{{}}}}
		assets, res, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, m, file, in)
		if err != nil {
			t.Fatal(err)
		}
		res.Release()
		m.Assets = assets
		m.Canonicalize()
		if err := col.PublishVectorPartitionManifestV1(m, nil); err != nil {
			t.Fatal(err)
		}
		return m
	}
	m1 := makeGeneration(41, 941)
	openPlan, err := NewVectorPartitionGenerationSearchOpenPlanWithContextV1(t.Context(), m1)
	if err != nil {
		t.Fatal(err)
	}
	generationPin, err := col.AcquireVectorPartitionReaderPinWithContextV1(t.Context(), def.Name, m1.Generation)
	if err != nil {
		t.Fatal(err)
	}
	foreignPlan := *openPlan
	foreignPlan.collection = "other"
	if _, err := col.OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(t.Context(), def.Name, m1.Generation, 0, &foreignPlan, generationPin); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("cross-collection plan err=%v", err)
	}
	// The generation cache must own an immutable index rather than retaining
	// mutable caller slices.
	m1.Assets[0].ID = "mutated-after-plan"
	m1.Memberships[0].VectorOrdinal = ^uint64(0)
	planned, err := col.OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(t.Context(), def.Name, m1.Generation, 0, openPlan, generationPin)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := planned.Search([]float32{1, 0, 0}, 1); err != nil || len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("planned search=%+v err=%v", got, err)
	}
	if err := planned.Close(); err != nil {
		t.Fatal(err)
	}
	concurrentOpen := make(chan error, 2)
	for partition := uint32(0); partition < 2; partition++ {
		go func() {
			searcher, err := col.OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(t.Context(), def.Name, m1.Generation, partition, openPlan, generationPin)
			if err == nil {
				err = searcher.Close()
			}
			concurrentOpen <- err
		}()
	}
	for range 2 {
		if err := <-concurrentOpen; err != nil {
			t.Fatalf("concurrent planned open: %v", err)
		}
	}
	generationPin.Release()
	if pins := vectorPartitionReaderPinCountV1(d.Dir(), col.name, def.Name, m1.Generation); pins != 0 {
		t.Fatalf("planned reader pins after close=%d", pins)
	}
	if _, err := col.OpenVectorPartitionLocalSearcherForGenerationSearchPlanWithContextV1(t.Context(), def.Name, m1.Generation, 0, openPlan, generationPin); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("released generation pin err=%v", err)
	}
	s, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Search([]float32{1, 0, 0}, 1); err != nil || len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("search=%+v err=%v", got, err)
	}
	if _, metrics, err := s.SearchWithMetrics([]float32{1, 0, 0}, 1); err != nil || metrics.Route != VectorPartitionSearchRouteHNSWSearchPackV1 || metrics.Candidates == 0 {
		t.Fatalf("search metrics=%+v err=%v", metrics, err)
	}
	searchStatus := s.Status()
	if searchStatus.SearchRoute != VectorPartitionSearchRouteHNSWSearchPackV1 || searchStatus.MaxStableIDBytes != 1 || searchStatus.PackBytes == 0 || searchStatus.MappedBytes+searchStatus.HeapBytes == 0 || searchStatus.OpenNanos == 0 || searchStatus.Candidates == 0 {
		t.Fatalf("search status=%+v", searchStatus)
	}
	if err := col.DeleteVectorPartitionGenerationV1(def.Name, m1.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("delete succeeded while local searcher pinned")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Close rejects new work but defers the M1 pin release until an explicit
	// local lease drains, so deletion cannot race an in-flight caller.
	lease, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Acquire(); err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	if err := col.DeleteVectorPartitionGenerationV1(def.Name, m1.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("delete succeeded while closing local lease pinned")
	}
	lease.Release()
	// Two complete immutable generations may be read concurrently; each opener
	// is bound to the requested manifest generation rather than an active alias.
	m2 := makeGeneration(42, 942)
	old, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m2.Generation, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := old.Search([]float32{1, 0, 0}, 1); err != nil || got[0].ID != "b" {
		t.Fatalf("old=%+v err=%v", got, err)
	}
	if got, err := newer.Search([]float32{0, 1, 0}, 1); err != nil || got[0].ID != "a" {
		t.Fatalf("new=%+v err=%v", got, err)
	}
	_ = old.Close()
	_ = newer.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openCollectionCommandWALDB(t, dir)
	var openErr error
	col, openErr = NewCollectionManager(d).OpenCollection("docs")
	if openErr != nil {
		t.Fatal(openErr)
	}
	s, err = col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := store.Open("docs", def.Name, m1.Generation)
	if err != nil {
		t.Fatal(err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), m.Assets[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, m.Assets[0].Ref.Offset); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, m1.Generation, 0); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("corrupt open err=%v", err)
	}
	_ = d.Close()
}

func TestVectorPartitionNativePackPreflightAndLayeredAdjacencyV1(t *testing.T) {
	if err := preflightVectorPartitionNativePackV1(1, 3, 2); err != nil {
		t.Fatal(err)
	}
	if err := preflightVectorPartitionNativePackV1(1_000_000, 4096, 16); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("over-cap preflight err=%v", err)
	}
	source := []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 3, 2, 3, 4, 2, 2, 4}
	got, err := remapVectorPartitionAdjacencyV1(source, map[int]int{2: 0, 4: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1, 1, 1, 1}
	if len(got) != len(want) {
		t.Fatalf("remapped adjacency=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("remapped adjacency=%v want %v", got, want)
		}
	}
}

func TestVectorPartitionLocalNavigationOverlayReservesNativeEdgeAtM2V1(t *testing.T) {
	rows := make([]columnVectorGraphAssetRow, 8)
	nativeEdges := []uint32{4, 5, 7, 0, 0, 1, 3, 4}
	for i := range rows {
		// Every value is outside this row's parent/child overlay set.
		rows[i].Adjacency = []uint32{nativeEdges[i]}
	}
	if err := addVectorPartitionLocalNavigationOverlayV1(rows, 4); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		layer0, _, err := vectorPartitionLayer0AdjacencySplitV1(rows[i].Adjacency)
		if err != nil || len(layer0) > 4 || len(layer0) == 0 {
			t.Fatalf("row=%d layer0=%v err=%v", i, layer0, err)
		}
		native := nativeEdges[i]
		if !slices.Contains(layer0, native) {
			t.Fatalf("row=%d lost reserved native edge=%d: %v", i, native, layer0)
		}
	}
	if got, err := vectorPartitionLayer0Reachability3999(rows); err != nil || got != len(rows) {
		t.Fatalf("M=2 overlay reachability=%d err=%v want=%d", got, err, len(rows))
	}
	m1a := make([]columnVectorGraphAssetRow, len(rows))
	m1b := make([]columnVectorGraphAssetRow, len(rows))
	for i := range rows {
		m1a[i].Adjacency = []uint32{nativeEdges[i]}
		m1b[i].Adjacency = []uint32{nativeEdges[i]}
	}
	if err := addVectorPartitionLocalNavigationOverlayV1(m1a, 2); err != nil {
		t.Fatalf("M=1 overlay: %v", err)
	}
	if err := addVectorPartitionLocalNavigationOverlayV1(m1b, 2); err != nil {
		t.Fatalf("repeat M=1 overlay: %v", err)
	}
	if got, err := vectorPartitionLayer0Reachability3999(m1a); err != nil || got != len(m1a) {
		t.Fatalf("M=1 overlay reachability=%d err=%v want=%d", got, err, len(m1a))
	}
	for i := range m1a {
		layer0, _, err := vectorPartitionLayer0AdjacencySplitV1(m1a[i].Adjacency)
		if err != nil || len(layer0) > 2 || !slices.Contains(layer0, nativeEdges[i]) || !slices.Equal(m1a[i].Adjacency, m1b[i].Adjacency) {
			t.Fatalf("M=1 row=%d layer0=%v err=%v repeat=%v", i, layer0, err, m1b[i].Adjacency)
		}
	}
}

func TestVectorPartitionLocalGraphDeltaAccountsSaturatedDisplacementV1(t *testing.T) {
	const degreeLimit = 4
	native := make([]columnVectorGraphAssetRow, 8)
	for row := range native {
		overlay, err := vectorPartitionLocalNavigationEdgesV1(row, len(native), degreeLimit)
		if err != nil {
			t.Fatal(err)
		}
		layer0 := make([]uint32, 0, degreeLimit)
		for neighbor := range native {
			if neighbor == row || slices.Contains(overlay, uint32(neighbor)) {
				continue
			}
			layer0 = append(layer0, uint32(neighbor))
			if len(layer0) == degreeLimit {
				break
			}
		}
		if len(layer0) != degreeLimit {
			t.Fatalf("row=%d native fixture degree=%d", row, len(layer0))
		}
		// The layered suffix makes a mutation above layer 0 observable. Every
		// native row is saturated, so the deterministic overlay must displace
		// some of its builder-selected edges.
		native[row].Adjacency = vectorPartitionLayer0AdjacencyJoinV1(layer0, []uint32{1, 1, 1, uint32((row + 5) % len(native))})
	}
	final := cloneColumnVectorGraphAssetRows3999(native)
	if err := addVectorPartitionLocalNavigationOverlayV1(final, degreeLimit); err != nil {
		t.Fatal(err)
	}
	deltas, err := vectorPartitionLocalGraphDeltaV1(native, final, degreeLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != len(native) {
		t.Fatalf("deltas=%d want=%d", len(deltas), len(native))
	}
	// Row 1 is an internal binary-tree node: parent 0 and children 3/4 occupy
	// three slots, leaving only one native edge under the shared cap.
	if got := deltas[1]; got.NativeDegree != 4 || got.FinalDegree != 4 || got.OverlayEdges != 3 || got.DisplacedNativeEdges != 3 {
		t.Fatalf("row 1 delta=%+v want saturated internal displacement", got)
	}
	if deltas[0].DisplacedNativeEdges == 0 {
		t.Fatalf("root delta=%+v want native displacement", deltas[0])
	}
	for row := range native {
		_, nativeSuffix, nativeErr := vectorPartitionLayer0AdjacencySplitV1(native[row].Adjacency)
		finalLayer0, finalSuffix, finalErr := vectorPartitionLayer0AdjacencySplitV1(final[row].Adjacency)
		if nativeErr != nil || finalErr != nil || !vectorPartitionUint32SlicesEqualV1(nativeSuffix, finalSuffix) || len(finalLayer0) > degreeLimit {
			t.Fatalf("row=%d suffix or degree changed native=%v final=%v degree=%d", row, nativeSuffix, finalSuffix, len(finalLayer0))
		}
	}
}

func TestVectorPartitionLocalGraphDeltaFailsClosedV1(t *testing.T) {
	rows := []columnVectorGraphAssetRow{{Adjacency: []uint32{1}}, {Adjacency: []uint32{0}}}
	if _, err := vectorPartitionLocalGraphDeltaV1(rows[:1], rows, 4); err == nil {
		t.Fatal("accepted mismatched graph rows")
	}
	corrupt := cloneColumnVectorGraphAssetRows3999(rows)
	corrupt[0].Adjacency = []uint32{2}
	if _, err := vectorPartitionLocalGraphDeltaV1(rows, corrupt, 4); err == nil {
		t.Fatal("accepted out-of-range final neighbor")
	}
}

func TestVectorPartitionLocalGraphVariantIdentityFailsClosedV1(t *testing.T) {
	native, err := VectorPartitionLocalGraphVariantIdentityV1(VectorPartitionLocalGraphVariantNativeV1)
	if err != nil {
		t.Fatal(err)
	}
	overlay, err := VectorPartitionLocalGraphVariantIdentityV1(VectorPartitionLocalGraphVariantOverlayCurrentV1)
	if err != nil || native == overlay {
		t.Fatalf("variant identities native=%q overlay=%q err=%v", native, overlay, err)
	}
	if _, err := VectorPartitionLocalGraphVariantIdentityV1("unknown"); err == nil {
		t.Fatal("accepted unknown graph variant")
	}
}

func TestVectorPartitionLocalNavigationOverlayM1ValidatesEveryNativeEdgeV1(t *testing.T) {
	rows := make([]columnVectorGraphAssetRow, 3)
	// The first edge is eligible for retention. The later invalid edge must
	// still be checked before M=1 chooses at most one native edge.
	rows[0].Adjacency = []uint32{1, 3}
	if err := addVectorPartitionLocalNavigationOverlayV1(rows, 2); err == nil {
		t.Fatal("accepted later invalid M=1 native edge")
	}
}

func TestVectorPartitionPackDiagnosticsMaxLayerFailsClosedV1(t *testing.T) {
	for _, tc := range []struct{ maxLayer, layers int }{{-1, 1}, {1, 1}} {
		if err := validateVectorPartitionPackDiagnosticsMaxLayerV1(tc.maxLayer, tc.layers); err == nil {
			t.Fatalf("maxLayer=%d layers=%d unexpectedly accepted", tc.maxLayer, tc.layers)
		}
	}
	if err := validateVectorPartitionPackDiagnosticsMaxLayerV1(0, 1); err != nil {
		t.Fatalf("valid max layer: %v", err)
	}
}

func TestVectorPartitionMaterializationBuildsPartitionLocalConnectedGraphV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	const sourceRows = 128
	rows := make([]columnGraphRebuildInputRowV2A, sourceRows)
	for i := range rows {
		angle := 2 * math.Pi * float64(i) / sourceRows
		rows[i] = columnGraphRebuildInputRowV2A{
			id:     fmt.Sprintf("doc-%03d", i),
			vector: []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), 0.25},
		}
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	source, authoritativeRows, err := col.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testVectorPartitionManifestV1()
	manifest.State, manifest.RouterGeneration, manifest.RouterAsset, manifest.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	manifest.Generation = source.Generation + 1
	manifest.IndexName = def.Name
	manifest.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, manifest.SourceRowCount = source.Generation, source.Checksum, source.SchemaHash, source.RowCount
	manifest.PartitionCount = 2
	manifest.Memberships = manifest.Memberships[:0]
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conflicts := make([][]bool, len(authoritativeRows))
	for i := range conflicts {
		conflicts[i] = make([]bool, len(authoritativeRows))
	}
	for i := range conflicts {
		adjacency, adjacencyErr := vectorPartitionSourceAdjacencyV1(reader, i, nil, false)
		if adjacencyErr != nil {
			reader.Close()
			t.Fatal(adjacencyErr)
		}
		maxLayer, layerErr := columnVectorGraphAdjacencyMaxLayer(adjacency)
		if layerErr != nil {
			reader.Close()
			t.Fatal(layerErr)
		}
		for layer := 0; layer <= maxLayer; layer++ {
			neighbors, layerErr := columnVectorGraphAdjacencyLayer(adjacency, layer)
			if layerErr != nil {
				reader.Close()
				t.Fatal(layerErr)
			}
			for _, neighbor := range neighbors {
				if int(neighbor) >= len(conflicts) {
					reader.Close()
					t.Fatalf("source neighbor %d out of range", neighbor)
				}
				conflicts[i][neighbor] = true
				conflicts[neighbor][i] = true
			}
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	selectedOrdinals := make([]bool, len(authoritativeRows))
	selectedCount := 0
	for ordinal := range authoritativeRows {
		allowed := true
		for prior, selected := range selectedOrdinals {
			if selected && conflicts[ordinal][prior] {
				allowed = false
				break
			}
		}
		if allowed {
			selectedOrdinals[ordinal] = true
			selectedCount++
		}
	}
	if selectedCount < 4 {
		t.Fatalf("independent source membership count=%d want at least 4", selectedCount)
	}
	selected := make(map[string][]float32)
	for _, row := range authoritativeRows {
		partition := uint32(1)
		if selectedOrdinals[row.VectorOrdinal] {
			partition = 0
			selected[string(row.DocumentID)] = append([]float32(nil), row.Values...)
		}
		manifest.Memberships = append(manifest.Memberships, VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: partition})
	}
	manifest.Canonicalize()
	inputs := []VectorPartitionSearchAssetV1{
		{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: def.Dimensions},
		{Source: source, Generation: manifest.Generation, PartitionID: 1, Dimensions: def.Dimensions},
	}
	assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 963, inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), assets[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := decodeVectorPartitionMembershipDigestV1(assets[0].MembershipDigest)
	if err != nil {
		t.Fatal(err)
	}
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{
		ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{
			ManifestGeneration: manifest.SourceGeneration,
			ManifestChecksum:   manifest.SourceChecksum,
			SchemaHash:         manifest.SourceSchemaHash,
		},
		ExpectedMembershipDigest: expectedDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	visited := make([]bool, pack.Header.Rows)
	queue := []int{pack.Header.EntryOrdinal}
	visited[pack.Header.EntryOrdinal] = true
	baseLayer := pack.AdjacencyLayers[0]
	for head := 0; head < len(queue); head++ {
		ordinal := queue[head]
		for _, neighbor := range baseLayer.Neighbors[baseLayer.Offsets[ordinal]:baseLayer.Offsets[ordinal+1]] {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, int(neighbor))
			}
		}
	}
	if len(queue) != pack.Header.Rows {
		t.Fatalf("partition-local graph reachable rows=%d want %d", len(queue), pack.Header.Rows)
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		t.Fatal(err)
	}
	searcher, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(def.Name, manifest.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer searcher.Close()
	diagnostics, err := searcher.PackDiagnosticsV1()
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics.Rows != uint64(len(selected)) || diagnostics.ReachableRows != diagnostics.Rows || diagnostics.TraversalRoots != 1 || len(diagnostics.RowsByLayer) == 0 || len(diagnostics.EdgesByLayer) == 0 {
		t.Fatalf("partition-local pack diagnostics=%+v", diagnostics)
	}
	for id, query := range selected {
		got, err := searcher.Search(query, 1)
		if err != nil {
			t.Fatalf("search %s: %v", id, err)
		}
		if len(got) != 1 || got[0].ID != id {
			t.Fatalf("partition-local search for %s returned %+v", id, got)
		}
	}
}

func TestVectorPartitionNativePackMembershipBindingRejectsCrossManifestMixV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	manifest := testVectorPartitionManifestV1()
	manifest.State, manifest.RouterGeneration, manifest.RouterAsset, manifest.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	manifest.Generation = 61
	manifest.IndexName = def.Name
	manifest.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, manifest.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	source := VectorPartitionSourceIdentityV1{Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum, SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount}
	inputs := []VectorPartitionSearchAssetV1{
		{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: def.Dimensions},
		{Source: source, Generation: manifest.Generation, PartitionID: 1, Dimensions: def.Dimensions},
	}
	assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 961, inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := col.validateVectorPartitionAssetMembershipBindingsV1(manifest); err != nil {
		t.Fatalf("valid bindings: %v", err)
	}
	for _, asset := range assets {
		expected, err := decodeVectorPartitionMembershipDigestV1(asset.MembershipDigest)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if got := hnswPackU16(raw, columnHNSWSearchPackHeaderVersionOffset); got != columnHNSWSearchPackVersionV3 {
			t.Fatalf("pack version=%d want %d", got, columnHNSWSearchPackVersionV3)
		}
		pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: manifest.SourceGeneration, ManifestChecksum: manifest.SourceChecksum, SchemaHash: manifest.SourceSchemaHash}, ExpectedMembershipDigest: expected})
		if err != nil || pack.Header.MembershipDigest != expected || !pack.Header.HasAuxiliaryNavigation {
			t.Fatalf("persisted membership header=%x expected=%x err=%v", pack.Header.MembershipDigest, expected, err)
		}
	}

	mixed := manifest
	mixed.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}, {VectorOrdinal: 1, PartitionID: 0}}
	mixed.Canonicalize()
	if err := col.validateVectorPartitionAssetMembershipBindingsV1(mixed); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("same-source cross-membership mix err=%v", err)
	}
	sourceReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range mixed.Assets {
		digest, err := vectorPartitionMembershipDigestV1(sourceReader, mixed.Generation, mixed.Assets[i].PartitionID, vectorPartitionMembershipsForPartitionV1(mixed, mixed.Assets[i].PartitionID))
		if err != nil {
			sourceReader.Close()
			t.Fatal(err)
		}
		mixed.Assets[i].MembershipDigest = hex.EncodeToString(digest[:])
	}
	if err := sourceReader.Close(); err != nil {
		t.Fatal(err)
	}
	mixed.Canonicalize()
	if err := col.validateVectorPartitionAssetMembershipBindingsV1(mixed); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("descriptor-resealed cross-membership header mix err=%v", err)
	}

	duplicate := manifest
	duplicate.OverlapMemberships = append(duplicate.OverlapMemberships, duplicate.Memberships[0])
	duplicate.Canonicalize()
	if err := duplicate.Validate(DefaultVectorPartitionManifestLimits()); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("duplicate home/overlap membership err=%v", err)
	}
}

func TestVectorPartitionNativePackExactCapRejectsWithoutDurableTraceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	manifest := testVectorPartitionManifestV1()
	manifest.State, manifest.RouterGeneration, manifest.RouterAsset, manifest.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	manifest.Generation = 62
	manifest.IndexName = def.Name
	manifest.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SourceGeneration, manifest.SourceChecksum, manifest.SourceSchemaHash, manifest.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	source := VectorPartitionSourceIdentityV1{Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum, SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount}
	input := []VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: def.Dimensions}}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	level, err := vectorPartitionSourceMaxLayerV1(reader, 0)
	if err != nil {
		reader.Close()
		t.Fatal(err)
	}
	id, ok := reader.documentIDForOrdinal(0)
	if !ok {
		reader.Close()
		t.Fatal("missing source stable ID")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	exactBytes, err := exactVectorPartitionLocalGraphPackBytesV1(1, def.Dimensions, make([]uint64, level+1), uint64(len(id)), 0, true, vectorPartitionSearchAssetMaxBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	before, err := vectorPartitionTestDirectoryBytesV1(d.ColumnAssetRootDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, resources, err := col.materializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 962, input, exactBytes-1); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("over-bound materialize err=%v", err)
	}
	after, err := vectorPartitionTestDirectoryBytesV1(d.ColumnAssetRootDir())
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("over-bound materialize left physical trace before=%d after=%d", before, after)
	}
	if store, err := OpenExistingVectorPartitionStoreV1(d.Dir()); err == nil {
		if _, err := store.Open(manifest.Collection, manifest.IndexName, manifest.Generation); err == nil {
			t.Fatal("over-bound materialize left manifest trace")
		}
	}
	assets, resources, err := col.materializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 962, input, exactBytes)
	if err != nil {
		t.Fatalf("exact-bound materialize: %v", err)
	}
	defer resources.Release()
	if len(assets) != 1 || int64(assets[0].Bytes) != exactBytes {
		t.Fatalf("exact-bound assets=%+v exact=%d", assets, exactBytes)
	}
}

func TestVectorPartitionNativePackKnownBytesPreflightUsesCallerCapV1(t *testing.T) {
	const rows, dimensions = 2, 3
	baseBytes, err := exactVectorPartitionLocalGraphPackBytesV1(rows, dimensions, []uint64{0}, 0, 0, true, vectorPartitionSearchAssetMaxBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	if err := preflightVectorPartitionNativePackKnownBytesV1(rows, dimensions, 1, baseBytes); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
		t.Fatalf("known-byte preflight err=%v want caller-cap rejection", err)
	}
	if err := preflightVectorPartitionNativePackKnownBytesV1(rows, dimensions, 0, baseBytes); err != nil {
		t.Fatalf("exact lower-bound preflight: %v", err)
	}
}

func vectorPartitionTestDirectoryBytesV1(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func TestVectorPartitionSourceOrdinalsBindNativeStableIDsV1(t *testing.T) {
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	identity, rows, err := col.VectorPartitionSourceOrdinalsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	if identity.RowCount != 2 || len(rows) != 2 {
		t.Fatalf("identity=%+v rows=%+v", identity, rows)
	}
	seen := make(map[string]uint64, len(rows))
	for ordinal, row := range rows {
		if row.Ordinal != uint64(ordinal) {
			t.Fatalf("row[%d]=%+v", ordinal, row)
		}
		seen[row.StableID] = row.Ordinal
	}
	if _, ok := seen["a"]; !ok {
		t.Fatalf("missing stable ID a: %+v", rows)
	}
	if _, ok := seen["b"]; !ok {
		t.Fatalf("missing stable ID b: %+v", rows)
	}
}

func TestVectorPartitionOverlapPolicyCanonicalV1(t *testing.T) {
	policy := VectorPartitionOverlapPolicyV1{Capacity: 100, Budget: 20, Realized: 17, Unspent: 3}
	raw, err := FormatVectorPartitionOverlapPolicyV1(policy)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := parseVectorPartitionOverlapPolicyV1(raw); !ok || got != policy {
		t.Fatalf("policy=%+v ok=%v want %+v", got, ok, policy)
	}
	for _, bad := range []string{
		"",
		raw + "junk",
		"m3_bounded_overlap_v1:capacity=0,budget=1,realized=1,unspent=0",
		"m3_bounded_overlap_v1:capacity=1,budget=1,realized=2,unspent=0",
		"m3_bounded_overlap_v1:capacity=1,budget=2,realized=1,unspent=2",
	} {
		if _, ok := parseVectorPartitionOverlapPolicyV1(bad); ok {
			t.Fatalf("accepted noncanonical policy %q", bad)
		}
	}
}

func TestVectorPartitionOverlapPolicyBuildIdentityCanonicalV1(t *testing.T) {
	policy := VectorPartitionOverlapPolicyV1{Capacity: 100, Budget: 20, Realized: 17, Unspent: 3, BuildIdentityDigest: strings.Repeat("a", 64)}
	raw, err := FormatVectorPartitionOverlapPolicyV1(policy)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := ParseVectorPartitionOverlapPolicyV1(raw); !ok || got != policy {
		t.Fatalf("policy round trip got=%+v ok=%t", got, ok)
	}
	for _, bad := range []string{raw + ",trailing=true", strings.Replace(raw, strings.Repeat("a", 64), "not-a-digest", 1)} {
		if _, ok := ParseVectorPartitionOverlapPolicyV1(bad); ok {
			t.Fatalf("accepted malformed build identity policy %q", bad)
		}
	}
}
