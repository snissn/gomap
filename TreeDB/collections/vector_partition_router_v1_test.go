package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	internalcrc "github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestVerifyVectorPartitionRouterStableAssetV1StreamsExactRange(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "router-stable-asset-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	prefix := []byte("unhashed-prefix")
	payload := bytes.Repeat([]byte("bounded-stream"), (2<<20)/len("bounded-stream"))
	if _, err := file.Write(append(append([]byte(nil), prefix...), payload...)); err != nil {
		t.Fatal(err)
	}
	ref := ColumnAssetRef{
		Kind: ColumnAssetKindTCS1HNSWSearchPack, Namespace: "router-stream-test",
		Generation: 1, PartID: 1, FileID: 1,
		Offset: int64(len(prefix)), Length: int64(len(payload)), Checksum: internalcrc.Checksum(payload),
	}
	if err := verifyVectorPartitionRouterStableAssetV1(file, ref); err != nil {
		t.Fatalf("verify stable range: %v", err)
	}
	if _, err := file.WriteAt([]byte{payload[0] ^ 0xff}, ref.Offset); err != nil {
		t.Fatal(err)
	}
	if err := verifyVectorPartitionRouterStableAssetV1(file, ref); err == nil {
		t.Fatal("corrupt stable range passed streaming verification")
	}
}

func TestPartitionRouterBuildPublishSearchReopenAndPinsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	rows := []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, .01}},
		{id: "b", vector: []float32{1, -.01}},
		{id: "c", vector: []float32{.01, 1}},
		{id: "d", vector: []float32{-.01, 1}},
	}
	dir, database, collection, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	building := vectorPartitionRouterBuildingFixtureV1(t, database, collection, def, 91, rows)
	sourceIdentity, sourceRows, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	if sourceIdentity.Generation != building.SourceGeneration ||
		sourceIdentity.Checksum != building.SourceChecksum ||
		sourceIdentity.SchemaHash != building.SourceSchemaHash ||
		sourceIdentity.RowCount != building.SourceRowCount {
		t.Fatalf("router source identity=%+v differs from building=%+v", sourceIdentity, building)
	}
	partitions := make([]internalrouter.RouterPartitionV1, building.PartitionCount)
	for partition := range partitions {
		partitions[partition].PartitionID = uint32(partition)
	}
	for _, sourceRow := range sourceRows {
		membership := building.Memberships[sourceRow.VectorOrdinal]
		if membership.VectorOrdinal != sourceRow.VectorOrdinal {
			t.Fatalf("noncanonical test membership ordinal=%d source=%d", membership.VectorOrdinal, sourceRow.VectorOrdinal)
		}
		partitions[membership.PartitionID].Vectors = append(partitions[membership.PartitionID].Vectors, internalrouter.RouterVectorV1{
			Ordinal: sourceRow.VectorOrdinal,
			Values:  sourceRow.Values,
		})
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.RepresentativesPerPartition = 2
	cfg.MaxDepth = 4
	cfg.MaxIterations = 8
	cfg.MaxVectors = 100
	cfg.MaxDimensions = 8
	cfg.MaxRepresentatives = 100
	cfg.MaxScalarWork = 1_000_000
	model, err := internalrouter.BuildRouterV1(partitions, cfg)
	if err != nil {
		t.Fatal(err)
	}
	modelDigest, err := internalrouter.RouterDigestV1(model)
	if err != nil {
		t.Fatal(err)
	}
	searchDef := def
	searchDef.M, searchDef.EfConstruction, searchDef.EfSearch = 2, 8, 8
	searchDef, err = normalizeVectorIndexDefinition(searchDef)
	if err != nil {
		t.Fatal(err)
	}
	firstPack, err := buildVectorPartitionRouterPackV1(building, model, modelDigest, searchDef)
	if err != nil {
		t.Fatal(err)
	}
	secondPack, err := buildVectorPartitionRouterPackV1(building, model, modelDigest, searchDef)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstPack, secondPack) {
		t.Fatal("identical router model produced non-deterministic native HNSW pack bytes")
	}
	baseIdentity := columnHNSWSearchPackBaseIdentity{
		ManifestGeneration: building.SourceGeneration,
		ManifestChecksum:   building.SourceChecksum,
		SchemaHash:         building.SourceSchemaHash,
	}
	prepared, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, firstPack, mappedresource.SourceHeapCopy, baseIdentity)
	stale := building
	stale.Generation++
	if _, _, _, err := decodeVectorPartitionRouterModelV1(prepared, stale); err == nil {
		t.Fatal("router records from a stale generation were accepted")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	buildOptions := VectorPartitionRouterBuildOptionsV1{
		Config: cfg, AssetFileID: 991, AssetPartID: 19, M: 2, EfConstruction: 8, EfSearch: 8,
	}
	tampered := cloneVectorPartitionRouterInputsV1(partitions)
	tampered[0].Vectors[0].Values[0] = .5
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), building, tampered, buildOptions); err == nil {
		t.Fatal("caller-tampered router vector was accepted instead of authoritative typed source")
	}
	missing := cloneVectorPartitionRouterInputsV1(partitions)
	missing[0].Vectors = missing[0].Vectors[:1]
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), building, missing, buildOptions); err == nil {
		t.Fatal("missing primary router vector was accepted")
	}
	duplicate := cloneVectorPartitionRouterInputsV1(partitions)
	duplicate[0].Vectors[1] = duplicate[0].Vectors[0]
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), building, duplicate, buildOptions); err == nil {
		t.Fatal("duplicate primary router vector was accepted")
	}
	cappedOptions := buildOptions
	cappedOptions.Config.MaxRouterBytes = 1
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), building, partitions, cappedOptions); err == nil {
		t.Fatal("pre-allocation router byte cap was ignored")
	}
	store, err := OpenExistingVectorPartitionStoreV1(database.Dir())
	if err != nil {
		t.Fatal(err)
	}
	stillBuilding, err := store.Open(collection.name, def.Name, building.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if stillBuilding.State != "building" || stillBuilding.RouterAsset != (VectorPartitionAssetV1{}) || len(stillBuilding.Representatives) != 0 {
		t.Fatalf("failed router preflights left durable publication trace: %+v", stillBuilding)
	}
	build, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), building, partitions, buildOptions)
	if err != nil {
		t.Fatalf("build and publish: %v status=%+v", err, build)
	}
	if build.Representatives != 4 || build.RouterBytes == 0 || build.ModelDigest == "" {
		t.Fatalf("unexpected build status: %+v", build)
	}

	router, openStatus, err := collection.OpenVectorPartitionRouterV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	if openStatus.Generation != building.Generation || openStatus.Representatives != 4 || openStatus.ActiveHandles != 1 {
		t.Fatalf("unexpected open status: %+v", openStatus)
	}
	if manifest := router.Status().Manifest; len(manifest.Representatives) != 4 {
		t.Fatalf("ready manifest omitted representative mapping: %+v", manifest)
	}
	partitionStatus, err := collection.VectorPartitionStatusV1(def.Name, building.Generation)
	if err != nil || partitionStatus.ReaderPins != 1 {
		t.Fatalf("pinned status=%+v err=%v", partitionStatus, err)
	}
	var exactPartitionOrder []uint32
	for _, mode := range []string{VectorPartitionRouterModeExactV1, VectorPartitionRouterModeApproxV1} {
		result, err := router.Search([]float32{1, 0}, VectorPartitionRouterSearchOptionsV1{
			Mode: mode, CandidateBudget: 4, PartitionProbes: 2,
		})
		if err != nil {
			t.Fatalf("%s search: %v", mode, err)
		}
		if got := result.Partitions[0].PartitionID; got != 0 {
			t.Fatalf("%s first partition=%d result=%+v", mode, got, result)
		}
		if result.Status.Candidates > 4 || result.Status.Selected != 2 {
			t.Fatalf("%s status=%+v", mode, result.Status)
		}
		order := make([]uint32, len(result.Partitions))
		for ordinal := range result.Partitions {
			order[ordinal] = result.Partitions[ordinal].PartitionID
		}
		if mode == VectorPartitionRouterModeExactV1 {
			exactPartitionOrder = order
		} else if !reflect.DeepEqual(order, exactPartitionOrder) {
			t.Fatalf("full-candidate HNSW partition order=%v want exact=%v", order, exactPartitionOrder)
		}
	}
	if _, err := router.Search([]float32{1, 0}, VectorPartitionRouterSearchOptionsV1{
		Mode: VectorPartitionRouterModeExactV1, CandidateBudget: 3, PartitionProbes: 1,
	}); err == nil {
		t.Fatal("undersized exact candidate budget succeeded")
	}
	oversizedExact, err := router.Search([]float32{1, 0}, VectorPartitionRouterSearchOptionsV1{
		Mode: VectorPartitionRouterModeExactV1, CandidateBudget: 1024, PartitionProbes: 2,
	})
	if err != nil {
		t.Fatalf("oversized exact candidate budget: %v", err)
	}
	if oversizedExact.Status.CandidateBudget != 1024 || oversizedExact.Status.Candidates != 4 || oversizedExact.Status.Selected != 2 {
		t.Fatalf("oversized exact status=%+v", oversizedExact.Status)
	}
	if _, err := router.Search([]float32{1, 0}, VectorPartitionRouterSearchOptionsV1{
		Mode: VectorPartitionRouterModeApproxV1, CandidateBudget: 1024, PartitionProbes: 1,
	}); err == nil {
		t.Fatal("oversized approximate candidate budget succeeded")
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	partitionStatus, err = collection.VectorPartitionStatusV1(def.Name, building.Generation)
	if err != nil || partitionStatus.ReaderPins != 0 {
		t.Fatalf("released status=%+v err=%v", partitionStatus, err)
	}
	if _, err := router.Search([]float32{1, 0}, VectorPartitionRouterSearchOptionsV1{
		Mode: VectorPartitionRouterModeExactV1, CandidateBudget: 4, PartitionProbes: 1,
	}); err == nil {
		t.Fatal("closed router search succeeded")
	}

	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = openCollectionCommandWALDB(t, dir)
	defer database.Close()
	collection, err = NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	router, _, err = collection.OpenVectorPartitionRouterV1(def.Name)
	if err != nil {
		t.Fatalf("open router after DB reopen: %v", err)
	}
	defer router.Close()
	result, err := router.Search([]float32{0, 1}, VectorPartitionRouterSearchOptionsV1{
		Mode: VectorPartitionRouterModeExactV1, CandidateBudget: 4, PartitionProbes: 1,
	})
	if err != nil || result.Partitions[0].PartitionID != 1 {
		t.Fatalf("reopened exact result=%+v err=%v", result, err)
	}
}

func cloneVectorPartitionRouterInputsV1(input []internalrouter.RouterPartitionV1) []internalrouter.RouterPartitionV1 {
	cloned := make([]internalrouter.RouterPartitionV1, len(input))
	for partitionOrdinal, partition := range input {
		cloned[partitionOrdinal].PartitionID = partition.PartitionID
		cloned[partitionOrdinal].Vectors = make([]internalrouter.RouterVectorV1, len(partition.Vectors))
		for vectorOrdinal, vector := range partition.Vectors {
			cloned[partitionOrdinal].Vectors[vectorOrdinal] = internalrouter.RouterVectorV1{
				Ordinal: vector.Ordinal,
				Values:  append([]float32(nil), vector.Values...),
			}
		}
	}
	return cloned
}

func TestPartitionRouterRecordRejectsMalformedFiniteAndHierarchyV1(t *testing.T) {
	cfg := internalrouter.DefaultRouterConfigV1()
	record := vectorPartitionRouterRecordV1{
		RouterGeneration: 7,
		PartitionID:      1,
		SourceOrdinal:    4,
		LeafNodeID:       2,
		Depth:            1,
		MemberCount:      3,
		Config:           cfg,
		Metrics: internalrouter.RouterBuildMetricsV1{
			Partitions: 1, Vectors: 3, Representatives: 1, HierarchyNodes: 2,
		},
		HNSWM: 2, HNSWEfConstruction: 8, HNSWEfSearch: 8,
		Path: []vectorPartitionRouterPathNodeV1{{NodeID: 1, MemberCount: 3}, {NodeID: 2, MemberCount: 3}},
	}
	raw, err := encodeVectorPartitionRouterRecordV1(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeVectorPartitionRouterRecordV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.LeafNodeID != record.LeafNodeID || got.Config != cfg {
		t.Fatalf("record=%+v want=%+v", got, record)
	}
	for _, malformed := range [][]byte{nil, raw[:len(raw)-1], append(append([]byte(nil), raw...), 1)} {
		if _, err := decodeVectorPartitionRouterRecordV1(malformed); err == nil {
			t.Fatalf("malformed record of length %d succeeded", len(malformed))
		}
	}
	if _, err := normalizeVectorPartitionRouterQueryV1([]float32{float32(math.NaN()), 1}, 2); err == nil {
		t.Fatal("non-finite router query succeeded")
	}
}

func TestPartitionRouterCandidateLimitIsHardV1(t *testing.T) {
	input := testColumnHNSWSearchPackInput2312()
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(
		t, testColumnHNSWSearchPackRaw2312(t), mappedresource.SourceHeapCopy, input.BaseIdentity,
	)
	defer view.Close()
	var scratch columnVectorGraphNativeSearchScratch
	_, stats, err := view.searchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{
		TopK: 2, EfSearch: 4, CandidateLimit: 2,
	}, &scratch)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Candidates > 2 {
		t.Fatalf("candidates=%d exceeded hard limit=2", stats.Candidates)
	}
	if _, err := rankVectorPartitionRouterCandidatesV1(
		[]internalrouter.RouterRepresentativeV1{{PartitionID: 1}},
		[]vectorPartitionRouterCandidateV1{{ordinal: 1, score: 1}},
		1,
	); err == nil {
		t.Fatal("corrupt representative mapping was accepted")
	}
}

func vectorPartitionRouterBuildingFixtureV1(t *testing.T, database *backenddb.DB, collection *Collection, def VectorIndexDefinition, generation uint64, rows []columnGraphRebuildInputRowV2A) VectorPartitionManifestV1 {
	t.Helper()
	identity, err := collection.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	cfg := *collection.meta.Options.ColumnStore
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		database.ColumnAssetRootDir(), cfg, 990,
		[]StableColumnPhysicalAssetAppend{
			{Payload: []byte("partition-router-fixture-0"), Kind: ColumnAssetKindTCS1PartImage, Generation: identity.Generation, PartID: 1},
			{Payload: []byte("partition-router-fixture-1"), Kind: ColumnAssetKindTCS1PartImage, Generation: identity.Generation, PartID: 2},
		},
		database.StableResourceIdentityPinRegistry(), lease,
	)
	lease.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	manifest := VectorPartitionManifestV1{
		State: "building", Collection: collection.name, IndexName: def.Name,
		IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      identity.Generation, SourceChecksum: identity.Checksum,
		SourceSchemaHash: identity.SchemaHash, SourceRowCount: uint64(len(rows)),
		Generation: generation, PartitionCount: 2, BalancePolicy: "disjoint_v1",
		Placements: []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}, {PartitionID: 1, GroupID: "raft-b"}},
	}
	_, sourceRows, err := collection.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, sourceRow := range sourceRows {
		inputOrdinal := -1
		for ordinal, row := range rows {
			if string(sourceRow.DocumentID) == row.id {
				inputOrdinal = ordinal
				break
			}
		}
		if inputOrdinal < 0 {
			t.Fatalf("authoritative source document %q not found in fixture", sourceRow.DocumentID)
		}
		manifest.Memberships = append(manifest.Memberships, VectorPartitionMembershipV1{
			VectorOrdinal: sourceRow.VectorOrdinal, PartitionID: uint32(inputOrdinal / 2),
		})
	}
	for ordinal, ref := range refs {
		raw, err := readColumnPhysicalAssetFromManager(database.ColumnAssetRootDir(), ref)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		manifest.Assets = append(manifest.Assets, VectorPartitionAssetV1{
			ID: "partition/" + string(rune('0'+ordinal)), PartitionID: uint32(ordinal),
			Checksum: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw)), Ref: ref,
		})
	}
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		t.Fatal(err)
	}
	return manifest
}
