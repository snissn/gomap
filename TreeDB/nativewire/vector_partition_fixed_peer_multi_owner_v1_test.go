package nativewire

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// This is the deployment test, not a local topology simulation. In particular,
// a node may never receive another owner's graph segment during fixture setup.
func TestMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	if err := seed.collection.EnsureVectorPartitionLiveBindingV1(ctx, seed.manifest); err != nil {
		t.Fatal(err)
	}
	appliedCommandLSN := seed.database.State().AppliedCommandLSN
	if appliedCommandLSN == 0 {
		t.Fatal("trusted seed has no command-WAL coverage")
	}
	// Only the schema/index definition is copied into each node's trusted
	// genesis. The builder's document rows and full ColumnGraph stay behind.
	meta := seed.collection.MetaView()
	if meta.Options.ColumnStore == nil {
		t.Fatal("source fixture has no column-store definition")
	}
	columnStore := *meta.Options.ColumnStore
	columnStore.ActiveManifest = nil // the active source descriptor belongs only to group-d
	columnStore.RecoveryAuthoritativeManifest = nil
	columnStore.RecoveryAuthoritativeAppliedCommandLSN = 0
	meta.Options.ColumnStore = &columnStore
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, meta)
	// Search the same immutable generation locally before the source holder is
	// closed. This is a result reference, not a substitute for the hosted-only
	// process and wire assertions below.
	localFixture := seed
	resolvedCatalog, err := raftplacement.Validate(configs[0].Vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	localFixture.catalog = resolvedCatalog
	localFixture.placement = configs[0].Vector.Placement
	localServices, localSources := newVectorPartitionLiveProductionServicesV1(t, localFixture)
	localCoordinator, err := NewVectorPartitionCoordinatorForTopologyV1(
		vectorPartitionLiveCoordinatorTopologyV1(localFixture),
		CollectionVectorPartitionCoordinatorRouterSourceV1{Collection: seed.collection},
		&vectorPartitionLiveProductionDispatcherV1{services: localServices}, VectorPartitionCoordinatorLimitsV1{},
	)
	if err != nil {
		t.Fatal(err)
	}
	localRequest := configs[0].Vector.RequestBase
	localRequest.RequestID, localRequest.CancellationID = "local-parity", "local-parity-cancel"
	localRequest.Query = []float32{.7, .7}
	localRequest.DeadlineUnixNano = time.Now().Add(30 * time.Second).UnixNano()
	localResult, err := localCoordinator.Search(ctx, localRequest)
	if err != nil {
		t.Fatalf("same-generation local reference: %v", err)
	}
	if localResult.PartitionGeneration != seed.manifest.Generation || localResult.Counters.SelectedDomains != 2 ||
		localResult.Counters.SelectedPartitions != 2 || localResult.Counters.HNSWServedPartitions != 2 || len(localResult.Neighbors) == 0 {
		t.Fatalf("same-generation local reference omitted a domain: %+v", localResult)
	}
	if err := localCoordinator.Close(); err != nil {
		t.Fatal(err)
	}
	for _, source := range localSources {
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.database.Close(); err != nil {
		t.Fatal(err)
	}

	// A domain's sections must remain in one owner. Separate domains must not
	// share a physical segment, or copying just hosted assets is impossible.
	graphSegments := map[raftcluster.GroupID]map[uint32]bool{
		"group-b": {}, "group-c": {},
	}
	placements := make(map[uint32]raftcluster.GroupID, len(seed.manifest.Placements))
	for _, placement := range seed.manifest.Placements {
		if _, exists := placements[placement.PartitionID]; exists {
			t.Fatalf("duplicate placement for domain %d", placement.PartitionID)
		}
		placements[placement.PartitionID] = raftcluster.GroupID(placement.GroupID)
	}
	chunksInMultiPackDomain := 0
	for _, asset := range seed.manifest.Assets {
		group, exists := placements[asset.PartitionID]
		if !exists || graphSegments[group] == nil {
			t.Fatalf("asset %q has no valid owner placement for domain %d", asset.ID, asset.PartitionID)
		}
		graphSegments[group][asset.Ref.FileID] = true
		if asset.PartitionID == 0 && strings.Contains(asset.ID, "/chunk/") {
			chunksInMultiPackDomain++
		}
	}
	if chunksInMultiPackDomain < 2 {
		t.Fatalf("domain 0 has %d physical chunks, want at least two", chunksInMultiPackDomain)
	}
	for fileID := range graphSegments["group-b"] {
		if graphSegments["group-c"][fileID] {
			t.Fatalf("owner-local publication gap: domains owned by group-b and group-c share physical segment %d", fileID)
		}
	}

	hostedFiles := make(map[raftcluster.GroupID]map[string]bool)
	for _, config := range configs {
		if config.NodeID == "source-holder" {
			// The non-serving source holder alone retains the full builder DB.
			// It is the catalog leader for a later source-validated BUILD.
			root := filepath.Join(config.DataRoot, "group-d")
			if err := os.CopyFS(root, os.DirFS(seed.dir)); err != nil {
				t.Fatal(err)
			}
			if err := backenddb.RebindDurableRootSnapshotV1(root); err != nil {
				t.Fatal(err)
			}
			bootstrapFixedPeerVectorTrustedGenesisV1(t, config, appliedCommandLSN)
			fixedPeerAssertSourceDocumentCountV1(t, root, seed.manifest.Collection, int(seed.manifest.SourceRowCount))
			continue
		}
		group := raftcluster.GroupID("group-a")
		switch config.NodeID {
		case "owner-b":
			group = "group-b"
		case "owner-c":
			group = "group-c"
		}
		root := filepath.Join(config.DataRoot, string(group))
		hostedFiles[group] = fixedPeerCopyHostedVectorAssetsV1(t, seed.dir, root, seed.manifest, graphSegments[group], group == "group-a")
		fixedPeerBootstrapHostedVectorMetadataV1(t, config, meta, root)
		// Metadata creation must not materialize a foreign graph or the full
		// builder source before any serving process is opened.
		fixedPeerAssertHostedVectorFilesV1(t, root, hostedFiles[group], false)
		fixedPeerAssertSourceDocumentCountV1(t, root, seed.manifest.Collection, 0)
	}
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	for i, config := range configs {
		processes[i] = fixedPeerStartTestProcessV1(t, config)
	}
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, id := range []raftcluster.NodeID{"ingress", "owner-b", "owner-c", "source-holder"} {
			status, err := client.Status(ctx, id)
			if err != nil || status.CatalogRaft.LeaderID == "" || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, configs[0].Vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "source-holder", command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, id := range []raftcluster.NodeID{"ingress", "owner-b", "owner-c", "source-holder"} {
			status, err := client.Status(ctx, id)
			if err != nil || status.Catalog.AppliedIndex == 0 || status.Catalog.Digest != record.Digest {
				return false
			}
		}
		return true
	})
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("source-verified BUILD, hosted Stage, and ACTIVE: %v", err)
	}
	for _, node := range []raftcluster.NodeID{"ingress", "owner-b", "owner-c"} {
		peer, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses[node])
		if err != nil {
			t.Fatalf("public listener for %s: %v", node, err)
		}
		status, statusErr := peer.VectorStatusV1(ctx)
		closeErr := peer.Close()
		if err := errors.Join(statusErr, closeErr); err != nil {
			t.Fatalf("warmed %s status: %v", node, err)
		}
		if !status.Health.Ready || status.Health.Reason != "ready" {
			t.Fatalf("warmed %s is not ready: %+v", node, status.Health)
		}
	}
	publicClient, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses["ingress"])
	if err != nil {
		t.Fatal(err)
	}
	defer publicClient.Close()
	request := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: seed.manifest.IndexName, Generation: seed.manifest.Generation},
		Query: []float32{.7, .7}, Metric: public.MetricCosineV1, TopK: 4, Probes: 2, EfSearch: 8,
		Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits:      public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 16},
		Deadline:    time.Now().Add(30 * time.Second),
	}
	for _, node := range []raftcluster.NodeID{"owner-b", "owner-c", "source-holder"} {
		peer, err := DialContext(ctx, "tcp", configs[0].Vector.PublicAddresses[node])
		if err != nil {
			t.Fatalf("public listener for %s: %v", node, err)
		}
		_, searchErr := peer.VectorSearchStrictV1(ctx, request)
		closeErr := peer.Close()
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if !hasPublicVectorErrorCodeV1(searchErr, public.ErrorUnavailableV1) {
			t.Fatalf("nonrouter %s accepted public strict search: %v", node, searchErr)
		}
	}
	response, err := publicClient.VectorSearchStrictV1(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Counters.SelectedDomains != 2 || response.Counters.SelectedGroups != 2 || response.Counters.RPCs != 2 ||
		response.Counters.SelectedPartitions != 2 || response.Counters.HNSWServedPartitions != 2 || response.Counters.ExactScanPartitions != 0 {
		t.Fatalf("multi-owner search did not traverse each selected domain exactly once: %+v", response.Counters)
	}
	if len(response.Neighbors) == 0 || len(response.Neighbors) != len(localResult.Neighbors) {
		t.Fatalf("multi-owner result cardinality differs from same-generation local search: remote=%+v local=%+v", response.Neighbors, localResult.Neighbors)
	}
	for i, remote := range response.Neighbors {
		local := localResult.Neighbors[i]
		if remote.ID != local.ID || math.Abs(float64(remote.Score-local.Score)) > 1e-5 {
			t.Fatalf("same-generation local parity at rank %d: remote=%+v local=%+v", i, remote, local)
		}
	}
	// Losing one selected owner must fail the whole public request; no partial
	// hits from the surviving owner may escape. Reopen that owner on its original
	// hosted-only assets and require the same generation/result again.
	processes[1].stop(t)
	request.Deadline = time.Now().Add(12 * time.Second)
	partial, searchErr := publicClient.VectorSearchStrictV1(ctx, request)
	if searchErr == nil || len(partial.Neighbors) != 0 {
		t.Fatalf("missing selected owner returned partial result: response=%+v err=%v", partial, searchErr)
	}
	processes[1] = fixedPeerStartTestProcessV1(t, configs[1])
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := client.Status(ctx, "owner-b")
		return err == nil && status.CatalogRaft.LeaderID != "" && len(status.Groups) == 1 && status.Groups[0].LeaderID != "" && status.Catalog.AppliedIndex != 0
	})
	if _, err := client.EnsureImmutableVectorLifecycleV1(ctx); err != nil {
		t.Fatalf("reopened owner lifecycle: %v", err)
	}
	request.Deadline = time.Now().Add(12 * time.Second)
	reopened, err := publicClient.VectorSearchStrictV1(ctx, request)
	if err != nil {
		t.Fatalf("reopened owner search: %v", err)
	}
	if len(reopened.Neighbors) != len(response.Neighbors) || reopened.Counters.SelectedDomains != 2 || reopened.Counters.RPCs != 2 ||
		reopened.Counters.SelectedPartitions != 2 || reopened.Counters.HNSWServedPartitions != 2 || reopened.Counters.ExactScanPartitions != 0 {
		t.Fatalf("reopened owner lost a domain or result: before=%+v after=%+v", response, reopened)
	}
	for i, neighbor := range reopened.Neighbors {
		if neighbor.ID != response.Neighbors[i].ID || math.Abs(float64(neighbor.Score-response.Neighbors[i].Score)) > 1e-5 {
			t.Fatalf("reopened owner result at rank %d: before=%+v after=%+v", i, response.Neighbors[i], neighbor)
		}
	}
	if err := publicClient.Close(); err != nil {
		t.Fatal(err)
	}
	client.Close()
	for _, process := range processes {
		process.stop(t)
	}
	for _, config := range configs {
		if config.NodeID == "source-holder" {
			fixedPeerAssertSourceDocumentCountV1(t, filepath.Join(config.DataRoot, "group-d"), seed.manifest.Collection, int(seed.manifest.SourceRowCount))
			continue
		}
		group := raftcluster.GroupID("group-a")
		switch config.NodeID {
		case "owner-b":
			group = "group-b"
		case "owner-c":
			group = "group-c"
		}
		fixedPeerAssertHostedVectorFilesV1(t, filepath.Join(config.DataRoot, string(group)), hostedFiles[group], false)
		fixedPeerAssertSourceDocumentCountV1(t, filepath.Join(config.DataRoot, string(group)), seed.manifest.Collection, 0)
	}
}

func TestFixedPeerImmutableDefinitionAndMutationRefusalV1(t *testing.T) {
	definition := collections.VectorIndexDefinition{Name: "embedding", SchemaGeneration: 17}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection:            raftplacement.CollectionRefV1{Collection: "documents"},
			CollectionIncarnation: 1,
			IndexName:             "embedding",
			IndexEpoch:            17,
			IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition),
		},
		Immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{ManifestDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64)},
	}
	meta := collections.CollectionMeta{Name: "documents", VectorIndexes: []collections.VectorIndexDefinition{definition}}
	if err := fixedPeerVectorImmutableDefinitionV1(meta, identity); err != nil {
		t.Fatalf("current durable definition: %v", err)
	}
	for _, changed := range []struct {
		name string
		edit func(*raftplacement.VectorPartitionLifecycleIdentityV1)
	}{
		{name: "recreated-index-epoch", edit: func(i *raftplacement.VectorPartitionLifecycleIdentityV1) { i.Index.IndexEpoch++ }},
		{name: "unsupported-collection-incarnation", edit: func(i *raftplacement.VectorPartitionLifecycleIdentityV1) { i.Index.CollectionIncarnation++ }},
	} {
		t.Run(changed.name, func(t *testing.T) {
			stale := identity
			changed.edit(&stale)
			if !errors.Is(fixedPeerVectorImmutableDefinitionV1(meta, stale), ErrFixedPeerVectorProofStaleV1) {
				t.Fatal("stale immutable definition was admitted")
			}
		})
	}
	b := &fixedPeerVectorBackendV1{runtime: &fixedPeerVectorRuntimeV1{parent: &FixedPeerTCPRuntimeV1{config: FixedPeerTCPConfigV1{Vector: &FixedPeerTCPVectorConfigV1{Identity: identity}}}}}
	if _, err := b.InsertVectorPartitionV1(t.Context(), public.InsertRequestV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
		t.Fatalf("immutable insert should refuse before backend or wire: %v", err)
	}
	if _, err := b.PrepareVectorPartitionV1(t.Context(), public.GenerationIDV1{}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
		t.Fatalf("immutable lifecycle mutation should refuse before backend: %v", err)
	}
}

// This mirrors the existing fixed-peer trusted-genesis fixture, but creates
// only collection metadata. Every serving asset is copied separately above
// and remains subject to the hosted-file inventory assertions.
func fixedPeerBootstrapHostedVectorMetadataV1(t testing.TB, config FixedPeerTCPConfigV1, meta collections.CollectionMeta, root string) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: root, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(database)
	_, createErr := manager.CreateCollection(&meta)
	coverage := database.State().AppliedCommandLSN
	if err := errors.Join(createErr, database.Close()); err != nil {
		t.Fatal(err)
	}
	if coverage == 0 {
		t.Fatal("metadata-only trusted genesis has no command-WAL coverage")
	}
	bootstrapFixedPeerVectorTrustedGenesisV1(t, config, coverage)
}

// The source-holder retains the full builder rows. Every serving process must
// retain zero rows before startup and after search/reopen, so hosted graph
// assets cannot be mistaken for a copied full serving source.
func fixedPeerAssertSourceDocumentCountV1(t testing.TB, root, collectionName string, want int) {
	t.Helper()
	database, err := backenddb.Open(backenddb.Options{Dir: root, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	manager := collections.NewCollectionManager(database)
	all, err := manager.ListCollections()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Name != collectionName {
		t.Fatalf("process %q has %d collections, want only metadata for %q", root, len(all), collectionName)
	}
	for _, meta := range all {
		if meta.Name != collectionName {
			continue
		}
		collection, err := manager.OpenCollection(collectionName)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		if _, err := collection.ScanDocumentIDsFunc(want+1, func([]byte) (bool, error) {
			seen++
			return seen < want+1, nil
		}); err != nil {
			t.Fatal(err)
		}
		if seen != want {
			t.Fatalf("process %q has %d source documents, want %d", root, seen, want)
		}
	}
}

func fixedPeerMultiOwnerSearchConfigsV1(t testing.TB, manifest collections.VectorPartitionManifestV1, sourceMeta collections.CollectionMeta) []FixedPeerTCPConfigV1 {
	t.Helper()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		return listener.Addr().String()
	}
	ids := []raftcluster.NodeID{"ingress", "owner-b", "owner-c", "source-holder"}
	features := raftcluster.DefaultFeatureSet()
	features.Required = append(features.Required,
		raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]},
	)
	meta := FixedPeerTCPGroupV1{ID: "meta", BootstrapNode: "source-holder", Features: features}
	groups := []FixedPeerTCPGroupV1{
		{ID: "group-a", BootstrapNode: "ingress"},
		{ID: "group-b", BootstrapNode: "owner-b"},
		{ID: "group-c", BootstrapNode: "owner-c"},
		{ID: "group-d", BootstrapNode: "source-holder"},
	}
	nodes := make([]FixedPeerTCPNodeV1, 0, len(ids))
	publicAddresses := make(map[raftcluster.NodeID]string, len(ids))
	shardAddresses := map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-b": {}, "group-c": {}}
	for i, id := range ids {
		nodes = append(nodes, FixedPeerTCPNodeV1{ID: id, Address: address()})
		meta.Peers = append(meta.Peers, raftcluster.Peer{ID: id, Address: address(), Capabilities: features})
		groups[i].Peers = append(groups[i].Peers, raftcluster.Peer{ID: id, Address: address()})
		if i == 1 || i == 2 {
			shardAddresses[groups[i].ID][id] = address()
		}
		publicAddresses[id] = address()
	}
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: manifest.Collection}
	catalogFeatures := raftplacement.DefaultFeatureSet()
	catalogFeatures.Required = append(catalogFeatures.Required, raftcluster.RequiredFeature{
		Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle],
	})
	catalog := raftplacement.CatalogV1{
		Features: catalogFeatures,
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}, LeaderHint: "ingress"},
			{ID: "group-b", Members: []raftcluster.NodeID{"owner-b"}, LeaderHint: "owner-b"},
			{ID: "group-c", Members: []raftcluster.NodeID{"owner-c"}, LeaderHint: "owner-c"},
			{ID: "group-d", Members: []raftcluster.NodeID{"source-holder"}, LeaderHint: "source-holder"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-d", Mode: raftplacement.PlacementModeCollectionV1}},
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: manifest.IndexName, IndexDefinitionDigest: manifest.IndexDefinitionDigest,
		SourceGeneration: manifest.SourceGeneration, SourceChecksum: manifest.SourceChecksum,
		SourceSchemaHash: manifest.SourceSchemaHash, SourceRowCount: manifest.SourceRowCount,
		PartitionGeneration: manifest.Generation, PartitionCount: manifest.PartitionCount,
		Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-b"}, {PartitionID: 1, GroupID: "group-b"}, {PartitionID: 2, GroupID: "group-c"}},
	}
	var indexEpoch uint64
	for _, definition := range sourceMeta.VectorIndexes {
		if definition.Name == manifest.IndexName && collections.VectorIndexDefinitionDigestV1(definition) == manifest.IndexDefinitionDigest {
			indexEpoch = definition.SchemaGeneration
			break
		}
	}
	if indexEpoch == 0 {
		t.Fatal("source definition does not attest the immutable index epoch")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: ref, CollectionIncarnation: 1, IndexName: manifest.IndexName, IndexDefinitionDigest: manifest.IndexDefinitionDigest,
			IndexEpoch: indexEpoch, CatalogEpoch: record.Epoch, CatalogDigest: record.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum,
			SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount,
		},
		Generation: manifest.Generation,
	}
	manifestBytes, err := collections.EncodeVectorPartitionManifestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	identity.Immutable = raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
		ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(manifestBytes)), PlacementDigest: placementDigest,
	}
	requestBase := VectorPartitionCoordinatorRequestV1{
		Version: VectorPartitionCoordinatorVersionV1, RequestID: "multi-owner-search", CancellationID: "multi-owner-search-cancel",
		Database: ref.Database, Catalog: ref.Catalog, Collection: ref.Collection, IndexName: manifest.IndexName,
		IndexDefinitionDigest: manifest.IndexDefinitionDigest, Metric: VectorPartitionShardSearchMetricCosineV1,
		RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: max(1, len(manifest.Representatives)),
		PartitionProbes: 2, Consistency: VectorPartitionShardSearchConsistencySnapshotV1,
		StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 4, EfSearch: 8,
		RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20, ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 16,
	}
	vector := &FixedPeerTCPVectorConfigV1{
		Collection: ref, Catalog: catalog, Manifest: manifest, Placement: placement, Identity: identity,
		RouterNodeID:    "ingress",
		PublicAddresses: publicAddresses, ShardAddresses: shardAddresses, RequestBase: requestBase, IndexedThrough: 1,
	}
	root := t.TempDir()
	configs := make([]FixedPeerTCPConfigV1, len(ids))
	for i, id := range ids {
		configs[i] = FixedPeerTCPConfigV1{
			NodeID: id, DataRoot: filepath.Join(root, string(id), "data"), RaftRoot: filepath.Join(root, string(id), "raft"),
			ListenAddress: nodes[i].Address, Nodes: nodes, Catalog: meta, Groups: groups,
			RequestTimeout: 12 * time.Second, RaftTimeout: 300 * time.Millisecond, Vector: vector,
			RaftListen: map[raftcluster.GroupID]string{"meta": meta.Peers[i].Address, groups[i].ID: groups[i].Peers[0].Address},
		}
	}
	return configs
}

// The fixture transfers only declared immutable search assets. In particular,
// it cannot smuggle the builder's document DB, ColumnGraph or source WAL into
// an owner process and then mistake a full-local open for hosted-only serving.
func fixedPeerCopyHostedVectorAssetsV1(t testing.TB, source, target string, manifest collections.VectorPartitionManifestV1, hosted map[uint32]bool, router bool) map[string]bool {
	t.Helper()
	allowed := make(map[string]bool)
	copyAsset := func(asset collections.VectorPartitionAssetV1) {
		rel := filepath.Join(filepath.FromSlash(asset.Ref.Namespace), "assets", "segments", fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID))
		if allowed[rel] {
			return
		}
		from, to := filepath.Join(backenddb.ColumnAssetRootDirPath(source), rel), filepath.Join(backenddb.ColumnAssetRootDirPath(target), rel)
		if err := os.MkdirAll(filepath.Dir(to), 0700); err != nil {
			t.Fatal(err)
		}
		in, err := os.Open(from)
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			_ = in.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(out, in)
		if err := errors.Join(copyErr, in.Close(), out.Close()); err != nil {
			t.Fatal(err)
		}
		allowed[rel] = true
	}
	for _, asset := range manifest.Assets {
		if hosted[asset.Ref.FileID] {
			copyAsset(asset)
		}
	}
	if router {
		copyAsset(manifest.RouterAsset)
	}
	expectedFiles := len(hosted)
	if router {
		expectedFiles++
	}
	if len(allowed) != expectedFiles {
		t.Fatalf("hosted asset file coverage=%d want %d graph segments and router=%t", len(allowed), len(hosted), router)
	}
	fixedPeerAssertHostedVectorFilesV1(t, target, allowed, true)
	return allowed
}

func fixedPeerAssertHostedVectorFilesV1(t testing.TB, target string, allowed map[string]bool, allFiles bool) {
	t.Helper()
	walkRoot := backenddb.ColumnAssetRootDirPath(target)
	if allFiles {
		walkRoot = target
	}
	err := filepath.WalkDir(walkRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(backenddb.ColumnAssetRootDirPath(target), path)
		if err != nil {
			return err
		}
		if !allowed[rel] {
			return fmt.Errorf("non-hosted or source file in process root %q", rel)
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("invalid hosted asset file %q: %v", rel, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
