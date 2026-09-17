package nativewire

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type fixedPeerVectorReadyFixtureV1 struct {
	IngressPublicAddress string
	Generation           public.GenerationIDV1
	RemotePartition      uint32
	RemoteGroup          string
	configs              []FixedPeerTCPConfigV1
	processes            []*fixedPeerTestProcessV1
	client               *FixedPeerTCPClientV1
}

func TestFixedPeerVectorSubmitErrorMarksCommittedResultAmbiguousV1(t *testing.T) {
	cause := context.DeadlineExceeded
	committed := raftcluster.SubmitResultV1{CommittedEntry: raftcluster.CommittedCommandEntryV1{Term: 1, Index: 2}}
	if err := fixedPeerVectorSubmitErrorV1(committed, cause); !errors.Is(err, raftcluster.ErrCommitAmbiguous) || !errors.Is(err, cause) {
		t.Fatalf("committed error = %v", err)
	}
	if err := fixedPeerVectorSubmitErrorV1(raftcluster.SubmitResultV1{}, cause); !errors.Is(err, cause) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("uncommitted error = %v", err)
	}
}

func TestFixedPeerVectorRuntimeCloseReleasesPublicListenerV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	runtime := &fixedPeerVectorRuntimeV1{listener: listener, server: NewServer(ServerOptions{})}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("public listener was not released: %v", err)
	}
	_ = rebound.Close()
}

func fixedPeerVectorReadyV1(t testing.TB, ctx context.Context) fixedPeerVectorReadyFixtureV1 {
	t.Helper()
	seed := fixedPeerVectorSeedV1(t)
	configs := fixedPeerVectorTestConfigsV1(t, seed)
	for _, config := range configs {
		group := "group-b"
		if config.NodeID == "ingress" {
			group = "group-a"
		}
		if err := os.CopyFS(filepath.Join(config.DataRoot, group), os.DirFS(seed.dir)); err != nil {
			t.Fatal(err)
		}
		if err := backenddb.RebindDurableRootSnapshotV1(filepath.Join(config.DataRoot, group)); err != nil {
			t.Fatal(err)
		}
		bootstrapFixedPeerVectorTrustedGenesisV1(t, config, seed.appliedCommandLSN)
	}
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	for i := range configs {
		processes[i] = fixedPeerStartTestProcessV1(t, configs[i])
	}
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	fixedPeerWaitV1(t, ctx, func() bool {
		status, statusErr := client.Status(ctx, "owner-1")
		if statusErr != nil || status.CatalogRaft.State != "Follower" || status.CatalogRaft.LeaderID != "owner-2" || len(status.Groups) != 1 {
			return false
		}
		return status.Groups[0].GroupID == "group-b" && status.Groups[0].State == "Leader" && status.Groups[0].LeaderID == "owner-1"
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, seed.catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "owner-2", command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs {
			status, statusErr := client.Status(ctx, config.NodeID)
			if statusErr != nil || status.Catalog.Epoch != record.Epoch || status.Catalog.Digest != record.Digest || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	ownerPublic := configs[0].Vector.PublicAddresses["owner-1"]
	ownerClient, err := DialContext(ctx, "tcp", ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerClient.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		status, statusErr := ownerClient.VectorStatusV1(ctx)
		return statusErr == nil && status.Health.Ready && status.Health.Generation.Index == seed.manifest.IndexName && status.Health.Generation.Generation == seed.manifest.Generation
	})
	ingressPublic := configs[0].Vector.PublicAddresses["ingress"]
	ingressClient, err := DialContext(ctx, "tcp", ingressPublic)
	if err != nil {
		t.Fatal(err)
	}
	defer ingressClient.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		status, statusErr := ingressClient.VectorStatusV1(ctx)
		return statusErr == nil && status.Health.Ready && status.Health.Generation.Index == seed.manifest.IndexName && status.Health.Generation.Generation == seed.manifest.Generation
	})
	return fixedPeerVectorReadyFixtureV1{
		IngressPublicAddress: ingressPublic,
		Generation:           public.GenerationIDV1{Index: seed.manifest.IndexName, Generation: seed.manifest.Generation},
		RemotePartition:      0, RemoteGroup: "group-b", configs: configs, processes: processes, client: client,
	}
}

func (f fixedPeerVectorReadyFixtureV1) RequireOwnerReplication(t testing.TB, ctx context.Context, index uint64) {
	t.Helper()
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range []raftcluster.NodeID{"owner-1", "owner-2", "owner-3"} {
			status, err := f.client.Status(ctx, node)
			if err != nil || len(status.Groups) != 1 || status.Groups[0].GroupID != "group-b" || status.Groups[0].CommitIndex < index || status.Groups[0].RaftAppliedIndex < index || status.Groups[0].Applied.Index < index {
				return false
			}
		}
		return true
	})
}

func (f fixedPeerVectorReadyFixtureV1) RequireNoWrongGroupMutation(t testing.TB, _ context.Context, ids ...[]byte) {
	t.Helper()
	ingress := -1
	for i := range f.configs {
		if f.configs[i].NodeID == "ingress" {
			ingress = i
			break
		}
	}
	if ingress < 0 {
		t.Fatal("ingress config is missing")
	}
	f.processes[ingress].stop(t)
	database, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(f.configs[ingress].DataRoot, "group-a"), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	collection, err := collections.NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		document, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if document != nil {
			t.Fatalf("wrong-group mutation stored %q in ingress group-a", id)
		}
	}
}

func (f fixedPeerVectorReadyFixtureV1) RequireOwnerDocuments(t testing.TB, present []byte, absent ...[]byte) {
	t.Helper()
	owner := -1
	for i := range f.configs {
		if f.configs[i].NodeID == "owner-1" {
			owner = i
			break
		}
	}
	if owner < 0 {
		t.Fatal("owner-1 fixture process is missing")
	}
	f.processes[owner].stop(t)
	database, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(f.configs[owner].DataRoot, "group-b"), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	collection, err := collections.NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	document, err := collection.Get(present)
	if err != nil || document == nil {
		t.Fatalf("accepted owner document %q=(%q, %v)", present, document, err)
	}
	for _, id := range absent {
		document, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if document != nil {
			t.Fatalf("rejected owner document %q was stored", id)
		}
	}
}

func (f fixedPeerVectorReadyFixtureV1) SearchRequest(query []float32, topK int) public.SearchRequestV1 {
	return public.SearchRequestV1{Version: 1, Generation: f.Generation, Query: query, Metric: public.MetricCosineV1, TopK: topK, Probes: 1, EfSearch: 8, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 16}, Deadline: time.Now().Add(30 * time.Second)}
}

// The ingress process owns no partition selected by this vector. The write
// must therefore cross the fixed-peer TCP boundary, commit and apply on the
// three-voter owner group, advance that generation's live delta, and become
// visible through the public production search service backed by those same
// fixed-peer DB/provider/lifecycle instances.
func TestVectorPartitionSystemNativeFourDaemonRemoteWriteRoutesAppliesAndBecomesVisibleV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fixture := fixedPeerVectorReadyV1(t, ctx)
	client, err := DialContext(ctx, "tcp", fixture.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	stale := public.InsertRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: fixture.Generation.Index, Generation: fixture.Generation.Generation + 1},
		IdempotencyKey: []byte("reject-stale-generation"), ID: []byte("reject-stale-generation"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(30 * time.Second),
	}
	if _, err := client.VectorInsertV1(ctx, stale); !hasPublicVectorErrorCodeV1(err, public.ErrorGenerationMismatchV1) {
		t.Fatalf("stale generation error=%v", err)
	}
	mismatch := public.InsertRequestV1{
		Version: 1, Generation: fixture.Generation,
		IdempotencyKey: []byte("reject-document-mismatch"), ID: []byte("reject-document-mismatch"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[1,0]}`), Deadline: time.Now().Add(30 * time.Second),
	}
	if _, err := client.VectorInsertV1(ctx, mismatch); !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
		t.Fatalf("document mismatch error=%v", err)
	}
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &VectorPartitionRoutedInsertV1{}}, true); !errors.Is(err, ErrFixedPeerVectorProofMissingV1) {
		t.Fatalf("missing proof error=%v", err)
	}
	private := VectorPartitionRoutedInsertV1{
		Request: public.InsertRequestV1{
			Version: 1, Generation: fixture.Generation, IdempotencyKey: []byte("reject-stale-catalog"), ID: []byte("reject-stale-catalog"), Vector: []float32{0, 1},
			Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(30 * time.Second),
		},
		Identity: fixture.configs[0].Vector.Identity,
		CatalogProof: raftplacement.CatalogProofV1{
			Epoch: fixture.configs[0].Vector.Identity.Index.CatalogEpoch, Digest: "stale-catalog-digest",
		},
		ReadySetDigest: "proof-present", RouterModelDigest: "proof-present", PartitionID: fixture.RemotePartition, OwnerGroup: raftcluster.GroupID(fixture.RemoteGroup),
	}
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &private}, true); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("stale catalog proof error=%v", err)
	}
	private.Request.ID = []byte("reject-wrong-owner")
	private.CatalogProof.Digest = fixture.configs[0].Vector.Identity.Index.CatalogDigest
	private.OwnerGroup = "group-a"
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &private}, true); !errors.Is(err, ErrFixedPeerVectorWrongOwnerV1) {
		t.Fatalf("wrong ownership error=%v", err)
	}
	if _, err := client.VectorSearchStrictV1(ctx, fixture.SearchRequest([]float32{0, 1}, 4)); err != nil {
		t.Fatalf("pre-mutation ingress search: %v", err)
	}

	started := time.Now()
	result, err := client.VectorInsertV1(ctx, public.InsertRequestV1{
		Version:        1,
		Generation:     fixture.Generation,
		IdempotencyKey: []byte("remote-visible-attempt-1"),
		ID:             []byte("remote-visible"),
		Vector:         []float32{0, 1},
		Document:       []byte(`{"embedding":[0,1],"kind":"remote-visible"}`),
		Deadline:       time.Now().Add(30 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("routed mutation acknowledgement latency=%s", time.Since(started))
	if result.Generation != fixture.Generation || result.PartitionID != fixture.RemotePartition || result.OwnerGroup != fixture.RemoteGroup || result.CommitTerm == 0 || result.CommitIndex <= 1 || result.AppliedIndex < result.CommitIndex || !result.ProductionConsensus || result.LiveRevision == 0 || result.VisibilityGeneration != fixture.Generation || result.VisibleID != "remote-visible" {
		t.Fatalf("incomplete mutation evidence: %+v", result)
	}
	if result.Counters.Routes != 1 || result.Counters.Forwards != 1 || result.Counters.Commits != 1 || result.Counters.Replications != 1 || result.Counters.Applies != 1 || result.Counters.VisibilityProofs != 1 {
		t.Fatalf("mutation counters: %+v", result.Counters)
	}
	fixture.RequireOwnerReplication(t, ctx, result.CommitIndex)

	search, err := client.VectorSearchStrictV1(ctx, fixture.SearchRequest([]float32{0, 1}, 4))
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Neighbors) == 0 || search.Neighbors[0].ID != "remote-visible" {
		t.Fatalf("subsequent production search did not observe routed mutation: %+v", search)
	}
	fixture.RequireNoWrongGroupMutation(t, ctx,
		[]byte("remote-visible"), []byte("reject-stale-generation"), []byte("reject-document-mismatch"), []byte("reject-stale-catalog"), []byte("reject-wrong-owner"),
	)
	fixture.RequireOwnerDocuments(t, []byte("remote-visible"),
		[]byte("reject-stale-generation"), []byte("reject-document-mismatch"), []byte("reject-stale-catalog"), []byte("reject-wrong-owner"),
	)
}

func hasPublicVectorErrorCodeV1(err error, code public.ErrorCodeV1) bool {
	var publicErr *public.ErrorV1
	return errors.As(err, &publicErr) && publicErr.Code == code
}

type fixedPeerVectorSeedFixtureV1 struct {
	dir               string
	appliedCommandLSN uint64
	manifest          collections.VectorPartitionManifestV1
	catalog           raftplacement.CatalogV1
}

func fixedPeerVectorSeedV1(t testing.TB) fixedPeerVectorSeedFixtureV1 {
	t.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("vector partition namespace persistence unsupported on this platform")
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	// Direct construction is trusted fixture genesis. The opaque command-WAL
	// coverage is bound to Raft apply progress before any child is started; all
	// behavior under test is a later real committed command.
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	definition := collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph}
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}}, VectorIndexes: []collections.VectorIndexDefinition{definition}}
	manager := collections.NewCollectionManager(database)
	if _, err := manager.CreateCollection(&meta); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	for _, document := range []struct{ id, body string }{{"base-x", `{"embedding":[1,0],"kind":"base-x"}`}, {"base-diagonal", `{"embedding":[0.6,0.8],"kind":"base-diagonal"}`}} {
		if _, err := collection.Insert([]byte(document.id), []byte(document.body)); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	if _, err := collection.RebuildVectorIndex(definition.Name); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(definition.Name)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manifest := collections.VectorPartitionManifestV1{
		State: "building", Collection: meta.Name, IndexName: definition.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}, BalancePolicy: "disjoint_v1",
		Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-b"}},
	}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for _, row := range rows {
		manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(collections.VectorPartitionMembershipHomeV1)})
	}
	manifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(definition.Name, manifest, 4701, []collections.VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: definition.Dimensions}})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		resources.Release()
		_ = database.Close()
		t.Fatal(err)
	}
	routerConfig := internalrouter.DefaultRouterConfigV1()
	routerConfig.BranchFactor, routerConfig.LeafSize, routerConfig.RepresentativesPerPartition = 2, 1, 1
	routerConfig.MaxDepth, routerConfig.MaxIterations, routerConfig.MaxVectors = 4, 8, 8
	routerConfig.MaxDimensions, routerConfig.MaxRepresentatives, routerConfig.MaxScalarWork = 8, 32, 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), manifest, []internalrouter.RouterPartitionV1{partition}, collections.VectorPartitionRouterBuildOptionsV1{Config: routerConfig, AssetFileID: 4702, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		resources.Release()
		_ = database.Close()
		t.Fatal(err)
	}
	prepared, err := collection.PreparedVectorPartitionManifestWithContextV1(context.Background(), definition.Name, manifest.Generation)
	resources.Release()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	beforeLiveBindingLSN := database.State().AppliedCommandLSN
	if err := collection.EnsureVectorPartitionLiveBindingV1(context.Background(), prepared); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	appliedCommandLSN := database.State().AppliedCommandLSN
	if beforeLiveBindingLSN == 0 || appliedCommandLSN <= beforeLiveBindingLSN {
		_ = database.Close()
		t.Fatalf("trusted genesis live binding command-WAL coverage=%d, want greater than pre-binding %d", appliedCommandLSN, beforeLiveBindingLSN)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: meta.Name}
	catalog := raftplacement.CatalogV1{
		Features: features,
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}, LeaderHint: "ingress"},
			// Keep the placement hint deliberately different from the Raft
			// bootstrap node so the system proof cannot rely on static ownership.
			{ID: "group-b", Members: []raftcluster.NodeID{"owner-1", "owner-2", "owner-3"}, LeaderHint: "owner-3"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-b", Mode: raftplacement.PlacementModeCollectionV1}},
	}
	return fixedPeerVectorSeedFixtureV1{dir: dir, appliedCommandLSN: appliedCommandLSN, manifest: prepared, catalog: catalog}
}

// bootstrapFixedPeerVectorTrustedGenesisV1 records only the opaque local
// command-WAL coverage created by direct fixture construction. The 1/1 entry
// is a trusted genesis baseline; every mutation asserted by the system test is
// committed and applied through the real fixed-peer Raft group above it.
func bootstrapFixedPeerVectorTrustedGenesisV1(t testing.TB, config FixedPeerTCPConfigV1, coverage uint64) {
	t.Helper()
	if coverage == 0 {
		t.Fatal("trusted genesis requires command-WAL coverage")
	}
	validated, _, err := validateFixedPeerConfigV1(config)
	if err != nil {
		t.Fatalf("validate fixed-peer config: %v", err)
	}
	var local *FixedPeerTCPGroupV1
	for i := range validated.Groups {
		for _, peer := range validated.Groups[i].Peers {
			if peer.ID != validated.NodeID {
				continue
			}
			if local != nil {
				t.Fatalf("node %s unexpectedly hosts multiple data groups", validated.NodeID)
			}
			local = &validated.Groups[i]
		}
	}
	if local == nil {
		t.Fatalf("node %s has no local data group", validated.NodeID)
	}
	cluster, err := raftcluster.Validate(raftcluster.Config{
		Dir:               filepath.Join(validated.DataRoot, string(local.ID)),
		ClusterDir:        validated.RaftRoot,
		DisableSideStores: true,
		NodeID:            validated.NodeID,
		GroupID:           local.ID,
		Peers:             local.Peers,
		Features:          local.Features,
	})
	if err != nil {
		t.Fatalf("validate local data-group layout: %v", err)
	}
	database, err := backenddb.Open(backenddb.Options{Dir: cluster.Dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open trusted genesis DB: %v", err)
	}
	gotCoverage := database.State().AppliedCommandLSN
	if err := database.Close(); err != nil {
		t.Fatalf("close trusted genesis DB: %v", err)
	}
	if gotCoverage != coverage {
		t.Fatalf("trusted genesis coverage=%d, want seed coverage %d", gotCoverage, coverage)
	}
	progress, err := raftapply.OpenDurableApplyProgressStore(cluster.Layout.ApplyDir, raftapply.DurableApplyStoreOptions{DisableSync: true, AllowInitialIndexGap: true})
	if err != nil {
		t.Fatalf("open trusted genesis apply progress: %v", err)
	}
	recordErr := progress.RecordApplied(raftapply.ApplyProgressRecordV1{
		EntryID:           raftentry.ApplyEntryID{Term: 1, Index: 1},
		AppliedCommandLSN: coverage,
	})
	if err := errors.Join(recordErr, progress.Close()); err != nil {
		t.Fatalf("record trusted genesis apply progress: %v", err)
	}
}

func fixedPeerVectorTestConfigsV1(t testing.TB, seed fixedPeerVectorSeedFixtureV1) []FixedPeerTCPConfigV1 {
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
	nodeIDs := []raftcluster.NodeID{"ingress", "owner-1", "owner-2", "owner-3"}
	features := raftcluster.DefaultFeatureSet()
	features.Required = append(features.Required,
		raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]},
	)
	nodes := make([]FixedPeerTCPNodeV1, 0, len(nodeIDs))
	meta := FixedPeerTCPGroupV1{ID: "meta", BootstrapNode: "owner-2", Features: features}
	groupA := FixedPeerTCPGroupV1{ID: "group-a", BootstrapNode: "ingress"}
	groupB := FixedPeerTCPGroupV1{ID: "group-b", BootstrapNode: "owner-1"}
	publicAddresses := make(map[raftcluster.NodeID]string, len(nodeIDs))
	shardAddresses := map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-b": {}}
	for _, nodeID := range nodeIDs {
		nodes = append(nodes, FixedPeerTCPNodeV1{ID: nodeID, Address: address()})
		meta.Peers = append(meta.Peers, raftcluster.Peer{ID: nodeID, Address: address(), Capabilities: features})
		peer := raftcluster.Peer{ID: nodeID, Address: address()}
		if nodeID == "ingress" {
			groupA.Peers = append(groupA.Peers, peer)
		} else {
			groupB.Peers = append(groupB.Peers, peer)
			shardAddresses["group-b"][nodeID] = address()
		}
		publicAddresses[nodeID] = address()
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, seed.catalog)
	if err != nil {
		t.Fatal(err)
	}
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: seed.manifest.Collection}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: seed.manifest.IndexName, IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest,
		SourceGeneration: seed.manifest.SourceGeneration, SourceChecksum: seed.manifest.SourceChecksum, SourceSchemaHash: seed.manifest.SourceSchemaHash, SourceRowCount: seed.manifest.SourceRowCount,
		PartitionGeneration: seed.manifest.Generation, PartitionCount: 1,
		Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-b"}},
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: ref, CollectionIncarnation: 1, IndexName: seed.manifest.IndexName, IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest,
			IndexEpoch: 1, CatalogEpoch: record.Epoch, CatalogDigest: record.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: seed.manifest.SourceGeneration, Checksum: seed.manifest.SourceChecksum, SchemaHash: seed.manifest.SourceSchemaHash, RowCount: seed.manifest.SourceRowCount,
		},
		Generation: seed.manifest.Generation,
	}
	requestBase := VectorPartitionCoordinatorRequestV1{
		Version: VectorPartitionCoordinatorVersionV1, RequestID: "fixed-peer-vector", CancellationID: "fixed-peer-vector-cancel",
		Database: ref.Database, Catalog: ref.Catalog, Collection: ref.Collection, IndexName: seed.manifest.IndexName, IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest,
		Metric: VectorPartitionShardSearchMetricCosineV1, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidateBudget: 1, PartitionProbes: 1,
		Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
		TopK: 8, EfSearch: 8, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20, ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 8,
	}
	vector := &FixedPeerTCPVectorConfigV1{
		Collection: ref, Catalog: seed.catalog, Manifest: seed.manifest, Placement: placement, Identity: identity,
		PublicAddresses: publicAddresses, ShardAddresses: shardAddresses, RequestBase: requestBase,
		IndexedThrough: 1,
	}
	root := t.TempDir()
	configs := make([]FixedPeerTCPConfigV1, len(nodes))
	for i, node := range nodes {
		configs[i] = FixedPeerTCPConfigV1{
			NodeID: node.ID, DataRoot: filepath.Join(root, string(node.ID), "data"), RaftRoot: filepath.Join(root, string(node.ID), "raft"), ListenAddress: node.Address,
			Nodes: nodes, Catalog: meta, Groups: []FixedPeerTCPGroupV1{groupA, groupB}, RequestTimeout: 4 * time.Second, RaftTimeout: 300 * time.Millisecond, Vector: vector,
			RaftListen: map[raftcluster.GroupID]string{},
		}
		for _, group := range []FixedPeerTCPGroupV1{meta, groupA, groupB} {
			for _, peer := range group.Peers {
				if peer.ID == node.ID {
					configs[i].RaftListen[group.ID] = peer.Address
				}
			}
		}
		if _, _, err := validateFixedPeerConfigV1(configs[i]); err != nil {
			t.Fatalf("vector config %s: %v", node.ID, err)
		}
	}
	return configs
}

func TestFixedPeerVectorTestConfigIsFourDaemonsAndThreeOwnerVotersV1(t *testing.T) {
	seed := fixedPeerVectorSeedV1(t)
	configs := fixedPeerVectorTestConfigsV1(t, seed)
	if len(configs) != 4 || len(configs[0].Groups) != 2 || len(configs[0].Groups[1].Peers) != 3 {
		t.Fatalf("topology is not four daemons with three owner voters: %+v", configs)
	}
	for _, config := range configs {
		if config.Vector == nil || config.Vector.PublicAddresses[config.NodeID] == "" {
			t.Fatalf("missing vector endpoint for %s", config.NodeID)
		}
	}
}

func TestFixedPeerVectorConfigRequiresCatalogLifecycleFeatureV1(t *testing.T) {
	configs := fixedPeerVectorTestConfigsV1(t, fixedPeerVectorSeedV1(t))
	config := configs[0]
	config.Catalog.Features.Required = []raftcluster.RequiredFeature{{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]}}
	if _, _, err := validateFixedPeerConfigV1(config); err == nil {
		t.Fatal("vector config without catalog lifecycle feature was accepted")
	}
}

func TestFixedPeerVectorConfigRequiresOneOwnerGroupV1(t *testing.T) {
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}
	vector := &FixedPeerTCPVectorConfigV1{
		Collection:      ref,
		Manifest:        collections.VectorPartitionManifestV1{State: "ready", Collection: "docs", IndexName: "embedding", Generation: 1, IntegrityDigest: "integrity"},
		Placement:       raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", PartitionGeneration: 1},
		Identity:        raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: ref, IndexName: "embedding"}, Generation: 1},
		PublicAddresses: map[raftcluster.NodeID]string{"node": "127.0.0.1:10001"},
		ShardAddresses: map[raftcluster.GroupID]map[raftcluster.NodeID]string{
			"group-a": {"node": "127.0.0.1:10002"},
			"group-b": {"node": "127.0.0.1:10003"},
		},
		IndexedThrough: 1,
	}
	config := FixedPeerTCPConfigV1{
		NodeID: "node", Nodes: []FixedPeerTCPNodeV1{{ID: "node"}}, Vector: vector,
		Groups: []FixedPeerTCPGroupV1{
			{ID: "group-a", Peers: []raftcluster.Peer{{ID: "node"}}},
			{ID: "group-b", Peers: []raftcluster.Peer{{ID: "node"}}},
		},
	}
	config.Vector.Placement.Partitions = nil
	want := "fixed-peer vector runtime requires exactly one owner group"
	if err := validateFixedPeerVectorConfigV1(config, map[raftcluster.GroupID]bool{}); err == nil || err.Error() != want {
		t.Fatalf("zero-owner validation error=%v, want %q", err, want)
	}
	config.Vector.Placement.Partitions = []raftplacement.VectorPartitionGroupV1{
		{PartitionID: 0, GroupID: "group-a"},
		{PartitionID: 1, GroupID: "group-b"},
	}
	if err := validateFixedPeerVectorConfigV1(config, map[raftcluster.GroupID]bool{"group-a": true, "group-b": true}); err == nil || err.Error() != want {
		t.Fatalf("multi-owner validation error=%v, want %q", err, want)
	}
}

func TestFixedPeerVectorInsertEntryUsesCallerAttemptIdentityV1(t *testing.T) {
	request := VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7},
		IdempotencyKey: []byte("attempt-1"), ID: []byte("doc"), Vector: []float32{1}, Document: []byte(`{"embedding":[1]}`),
	}, Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: "embedding"}, Generation: 7}}
	first, _, err := fixedPeerVectorInsertEntryV1("docs", collections.DocumentFormatJSON, 1, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, _, err := fixedPeerVectorInsertEntryV1("docs", collections.DocumentFormatJSON, 1, request)
	if err != nil || !bytes.Equal(first, replay) {
		t.Fatalf("same attempt changed entry: err=%v", err)
	}
	request.Request.IdempotencyKey = []byte("attempt-2")
	next, _, err := fixedPeerVectorInsertEntryV1("docs", collections.DocumentFormatJSON, 1, request)
	if err != nil || bytes.Equal(first, next) {
		t.Fatalf("new attempt reused entry: err=%v", err)
	}
}
