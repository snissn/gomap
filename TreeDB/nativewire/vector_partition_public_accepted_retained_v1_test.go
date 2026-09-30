package nativewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// Opt-in qualification of a NEW epoch-one copy of the accepted disjoint
// recipe. The historical epoch-zero DB is checksum-only and never opened.
// These512 previously examined queries are not held out; this is correctness
// and pinned recall qualification, not a performance benchmark.
func TestMultiOwnerTCPAcceptedDisjointRetained100KV1(t *testing.T) {
	if os.Getenv("GOMAP_SELECTED_LIVE_FIXTURE") == "" {
		t.Skip("requires root-approved fresh accepted disjoint fixture")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	in := liveLifecycleRetainedInputForV1(t)
	if in.Recipe != "graph-disjoint-v1" || in.Probes != 5 {
		t.Fatal("accepted public qualification requires explicit disjoint recipe/P5")
	}
	var descriptor struct {
		VariantID             string                         `json:"variant_id"`
		FixtureChecksum       string                         `json:"fixture_checksum"`
		GraphArtifactSHA256   string                         `json:"graph_artifact_sha256"`
		GraphBuildSHA256      string                         `json:"graph_build_sha256"`
		KaHIPPythonSHA256     string                         `json:"kahip_python_sha256"`
		KaHIPAdapterSHA256    string                         `json:"kahip_adapter_sha256"`
		IndexDefinitionDigest string                         `json:"index_definition_digest"`
		PartitionHNSWM        int                            `json:"partition_hnsw_m"`
		PartitionHNSWEfC      int                            `json:"partition_hnsw_ef_construction"`
		RouterConfig          vectorpartition.RouterConfigV1 `json:"router_config"`
	}
	if err := json.Unmarshal(liveLifecycleReadPinnedV1(t, in.Descriptor, in.DescriptorSHA256), &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.VariantID != "graph-disjoint-v1" || descriptor.FixtureChecksum != "f88fb41a40006b02ee228b373e86aa90271d40e001af74f2d79d8263747f629d" ||
		descriptor.GraphArtifactSHA256 != "c42c6a3437e4055178d3e41c86c8e1f82da9a4e228715ecda35191692a14fd33" || descriptor.GraphBuildSHA256 != "7a74fab1c748ecc42df7793e3074f4e8ad7bd7a9e59bd91053dfe8fa6f1eba22" ||
		descriptor.KaHIPPythonSHA256 != "a2f33a6e006989270f4340528eb61f8f97366e00a5d1b602ac8672ea44fc56ae" || descriptor.KaHIPAdapterSHA256 != "74ca1829a3be3ad7d7edcbcc6c566fc17b98e00d70e36742ebc5e0b29fd5627e" || descriptor.PartitionHNSWM != 32 || descriptor.PartitionHNSWEfC != 256 {
		t.Fatal("fresh descriptor does not reproduce the accepted source/assignment/local graph recipe")
	}
	seed, _, queries, _ := liveLifecycleOpenRetainedV1(t, "graph-disjoint-v1")
	t.Cleanup(func() { _ = seed.database.Close() })
	if descriptor.IndexDefinitionDigest != seed.manifest.IndexDefinitionDigest {
		t.Fatal("fresh descriptor definition binding differs from actual epoch-one collection")
	}
	cfg := descriptor.RouterConfig
	if cfg.Seed != 4017 || cfg.BranchFactor != 64 || cfg.LeafSize != 250 || cfg.RepresentativeBudget != 256 || cfg.MaxDepth != 8 || cfg.MaxIterations != 16 || cfg.MaxVectors != 120000 || cfg.MaxScalarWork != 50000000000 || cfg.MaxRouterBytes != 1073741824 {
		t.Fatal("accepted router construction recipe drift")
	}
	var truth struct {
		Truth [][]VectorPartitionCoordinatorNeighborV1
	}
	if err := json.Unmarshal(liveLifecycleReadPinnedV1(t, in.Truth, "347a618317d2fde97802c2c84593980d36563c27dd2a859ab46320ca674f5fc8"), &truth); err != nil {
		t.Fatal(err)
	}
	if len(truth.Truth) != 512 {
		t.Fatal("accepted pinned truth must contain512 queries")
	}
	historicalDefinition := seed.definition
	historicalDefinition.SchemaGeneration = 0
	if collections.VectorIndexDefinitionDigestV1(historicalDefinition) != "acb460d2a9c2fe4932668e7d01cce03ac791579dd750180b78c814c86c64b87e" {
		t.Fatal("historical source definition changed")
	}
	original := seed.manifest
	// The standard M3 domain sections already exist; only physical owner
	// separation requires a new global generation, never a legacy rebind.
	source, rows, err := seed.collection.ReadVectorPartitionRouterSourceRowsV1(seed.definition.Name)
	if err != nil {
		t.Fatal(err)
	}
	router, _, err := seed.collection.OpenVectorPartitionRouterV1(seed.definition.Name)
	if err != nil {
		t.Fatal(err)
	}
	route, routeErr := router.Search(queries[0], collections.VectorPartitionRouterSearchOptionsV3{Mode: collections.VectorPartitionRouterModeExactV1, ScoreBudget: 256, PartitionProbes: 5})
	closeErr := router.Close()
	if routeErr != nil || closeErr != nil || len(route.Partitions) != 5 {
		t.Fatalf("accepted first query route: %v %v", routeErr, closeErr)
	}
	// Preserve whole domains. Ensure query zero exercises both owners so its
	// existing owner-loss/refusal/reopen assertions cannot pass vacuously.
	ownerByAnchor := make(map[uint32]string)
	for _, pack := range original.DomainPacks {
		owner := "group-b"
		if pack.DomainID%2 != 0 {
			owner = "group-c"
		}
		ownerByAnchor[pack.DomainID] = owner
	}
	m := original
	m.State, m.IntegrityDigest, m.ReadySetDigest = "building", "", ""
	m.Generation++
	if m.Generation <= original.Generation {
		t.Fatal("generation overflow")
	}
	m.RouterGeneration, m.Representatives, m.Assets, m.RouterAsset = 0, nil, nil, collections.VectorPartitionAssetV1{}
	m.Placements = nil
	domainByPack := make(map[uint32]uint32)
	for _, pack := range m.DomainPacks {
		domainByPack[pack.PackID] = pack.DomainID
	}
	firstDomain, secondDomain := route.Partitions[0].PartitionID, route.Partitions[1].PartitionID
	if firstDomain >= m.DomainCount || secondDomain >= m.DomainCount || firstDomain == secondDomain {
		t.Fatal("router did not select distinct logical domains")
	}
	ownerByAnchor[firstDomain], ownerByAnchor[secondDomain] = "group-b", "group-c"
	for pack := uint32(0); pack < m.PartitionCount; pack++ {
		domain, ok := domainByPack[pack]
		if !ok {
			t.Fatal("missing domain pack")
		}
		m.Placements = append(m.Placements, collections.VectorPartitionPlacementV1{PartitionID: pack, GroupID: ownerByAnchor[domain]})
	}
	parts := make([]vectorpartition.RouterPartitionV1, m.PartitionCount)
	inputs := make([]collections.VectorPartitionSearchAssetV1, m.PartitionCount)
	byOrdinal := make(map[uint64]int, len(rows))
	for i, row := range rows {
		byOrdinal[row.VectorOrdinal] = i
	}
	for pack := range parts {
		parts[pack].PartitionID = uint32(pack)
		inputs[pack] = collections.VectorPartitionSearchAssetV1{Source: source, Generation: m.Generation, PartitionID: uint32(pack), Dimensions: seed.definition.Dimensions}
	}
	for _, member := range m.Memberships {
		i, ok := byOrdinal[member.VectorOrdinal]
		if !ok {
			t.Fatal("membership absent from source")
		}
		parts[member.PartitionID].Vectors = append(parts[member.PartitionID].Vectors, vectorpartition.RouterVectorV1{Ordinal: member.VectorOrdinal, Values: rows[i].Values, MembershipKind: string(collections.VectorPartitionMembershipHomeV1)})
	}
	for _, id := range []uint32{61001, 61002, 61003} {
		if err := filepath.WalkDir(seed.database.ColumnAssetRootDir(), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Name() == fmt.Sprintf("segment-%06d.tca", id) {
				return fmt.Errorf("reserved asset ID collision: %s", path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	m.Canonicalize()
	assets, resources, err := seed.collection.MaterializeVectorPartitionOwnerSeparatedLocalSearchAssetsV1(seed.definition.Name, m, map[string]uint32{"group-b": 61001, "group-c": 61003}, inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	m.Assets = assets
	m.Canonicalize()
	if err := seed.collection.PublishVectorPartitionManifestV1(m, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.collection.BuildAndPublishVectorPartitionRouterV1(ctx, m, parts, collections.VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 61002, AssetPartID: 1, M: 16, EfConstruction: 128, EfSearch: 128}); err != nil {
		t.Fatal(err)
	}
	seed.manifest, err = seed.collection.PreparedVectorPartitionManifestWithContextV1(ctx, seed.definition.Name, m.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLiveLifecycleRetainedGeometryV1(seed.manifest, "graph-disjoint-v1"); err != nil {
		t.Fatal(err)
	}
	if seed.manifest.SourceGeneration != original.SourceGeneration || seed.manifest.SourceChecksum != original.SourceChecksum || seed.manifest.SourceSchemaHash != original.SourceSchemaHash || seed.manifest.IndexDefinitionDigest != original.IndexDefinitionDigest || seed.manifest.Generation != original.Generation+1 || seed.manifest.RouterGeneration != seed.manifest.Generation || seed.manifest.ReadySetDigest == original.ReadySetDigest {
		t.Fatal("fresh owner-separated generation/source binding drift")
	}
	graphs := 0
	for _, asset := range seed.manifest.Assets {
		if asset.GraphVariant != "" {
			if asset.GraphVariant != string(collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1) {
				t.Fatal("owner-separated graph model drift")
			}
			graphs++
		}
	}
	if graphs != 16 {
		t.Fatalf("canonical graph descriptors=%d,want16", graphs)
	}
	retainLiveLifecycleJSONV1(t, "accepted-owner-separated-generation", struct {
		Descriptor      any
		Original, Fresh collections.VectorPartitionManifestV1
	}{descriptor, original, seed.manifest})
	t.Logf("accepted fresh public qualification: original_generation=%d new_generation=%d definition=%s D16/P64 source_M16/eFC128/eFS128 local_compat_M32/eFC256 VamanaR64/L256/alpha1.2 fixed_queries=512 P5/EF96/Merge256 warm=16", original.Generation, seed.manifest.Generation, seed.manifest.IndexDefinitionDigest)
	testMultiOwnerTCPDomainSearchWithQualificationV1(t, ctx, seed, queries, 10, 96, 256, 5, 16, truth.Truth, false, false, false, false, false)
}

// The same production fixture builder supplies64 placements/D16. This is a
// transport/geometry check, not a substitute for the pinned100K recall gate.
func TestMultiOwnerTCPAcceptedDisjointGeometryV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	definition := collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 768, M: 16, EfConstruction: 128, EfSearch: 128, Encoding: collections.VectorIndexEncodingFloat32, Strategy: collections.VectorIndexStrategyColumnGraph, SchemaGeneration: 1}
	documents := make([]vectorPartitionLiveDocumentV1, 64)
	for i := range documents {
		v := make([]float32, 768)
		for d := range v {
			v[d] = float32(((i+1)*(d+3)+i*i)%31-15) / 16
		}
		v[0] += float32(i) / 64
		documents[i] = vectorPartitionLiveDocumentV1{id: fmt.Sprintf("geometry-%02d", i), vector: v, home: uint32(i)}
	}
	seed := newVectorPartitionLiveNativewireDocumentsWithGeometryV1(t, documents, nil, [2]string{"group-b", "group-c"}, true, true, definition, 64, 16)
	t.Cleanup(func() { _ = seed.database.Close() })
	queries := [][]float32{documents[37].vector, documents[12].vector}
	testMultiOwnerTCPDomainSearchWithQualificationV1(t, ctx, seed, queries, 10, 96, 256, 5, 0, nil, false, false, false, false, false)
}
