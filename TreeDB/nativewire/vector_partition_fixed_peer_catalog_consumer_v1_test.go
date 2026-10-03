package nativewire

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestMultiOwnerTCPDomainSearchWithCatalogConsumerOwnerV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0},
		{id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2},
		{id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	testMultiOwnerTCPDomainSearchWithSeedModeV1(t, ctx, seed, []float32{.7, .7}, false, false, false, true, false)
}

func TestMultiOwnerTCPDomainSearchWithCatalogConsumerInvalidationV1(t *testing.T) {
	t.Setenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL", t.TempDir())
	TestMultiOwnerTCPDomainSearchWithCatalogConsumerOwnerV1(t)
}

// Compare this exact tests-only overlay on the baseline and candidate for the
// voter case. The consumer has no working baseline capability.
func TestFixedPeerImmutableOwnerReadCostV1(t *testing.T) {
	for _, consumer := range []bool{false, true} {
		name := "voter"
		if consumer {
			name = "consumer"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
				{id: "a", vector: []float32{1, 0}, home: 0}, {id: "b", vector: []float32{.8, .2}, home: 1},
				{id: "c", vector: []float32{0, 1}, home: 2}, {id: "d", vector: []float32{.2, .8}, home: 2},
			}, nil, [2]string{"group-b", "group-c"}, true, true)
			testMultiOwnerTCPDomainSearchWithSeedModeV1(t, ctx, seed, []float32{.7, .7}, false, false, false, consumer, true)
		})
	}
}

func fixedPeerImmutableOwnerReadCostV1(t testing.TB, ctx context.Context, configs []FixedPeerTCPConfigV1, router *Client, request public.SearchRequestV1, consumer bool) {
	t.Helper()
	owner, err := DialContext(ctx, "tcp", configs[1].Vector.PublicAddresses["owner-b"])
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	const samples = 10
	gomaxprocs := runtime.GOMAXPROCS(0)
	for _, scope := range []string{"owner_status", "public_search_two_owner_shards"} {
		operation := func() {
			if scope == "owner_status" {
				status, err := owner.VectorStatusV1(ctx)
				if err != nil || !status.Health.Ready {
					t.Fatalf("cost owner status: %+v %v", status, err)
				}
			} else {
				request.Deadline = time.Now().Add(10 * time.Second)
				response, err := router.VectorSearchStrictV1(ctx, request)
				if err != nil || len(response.Neighbors) == 0 || response.Counters.RPCs != 2 {
					t.Fatalf("cost public search: %+v %v", response, err)
				}
			}
		}
		// AllocsPerRun performs one warmup then exactly ten measured operations
		// with parent GOMAXPROCS=1; it excludes allocations in child processes.
		allocs := testing.AllocsPerRun(samples, operation)
		operation() // One untimed warmup before the independent wall/bytes sample.
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		for range samples {
			operation()
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		t.Logf("immutable_owner_read_cost consumer=%t scope=%s samples=%d alloc_warmup=1 timing_warmup=1 parent_gomaxprocs=%d alloc_sample_gomaxprocs=1 allocs/op=%.2f caller_global_B/op=%.2f ns/op=%.0f ops/sec=%.2f owner_allocations=unobserved background_parent_allocations=included", consumer, scope, samples, gomaxprocs, allocs, float64(after.TotalAlloc-before.TotalAlloc)/samples, float64(elapsed.Nanoseconds())/samples, float64(samples)/elapsed.Seconds())
	}
}

func fixedPeerCatalogConsumerOwnerConfigV1(t testing.TB, configs []FixedPeerTCPConfigV1) {
	t.Helper()
	meta := configs[0].Catalog
	meta.Peers = slices.DeleteFunc(slices.Clone(meta.Peers), func(peer raftcluster.Peer) bool { return peer.ID == "owner-b" })
	if len(meta.Peers) != 3 || meta.BootstrapNode != "source-holder" {
		t.Fatalf("consumer fixture catalog voters: %+v", meta)
	}
	for i := range configs {
		configs[i].Catalog = meta
		if configs[i].NodeID == "owner-b" {
			delete(configs[i].RaftListen, meta.ID)
		}
	}
}

func fixedPeerAssertCatalogConsumerOwnerV1(t testing.TB, ctx context.Context, control *FixedPeerTCPClientV1, config FixedPeerTCPConfigV1) {
	t.Helper()
	status, err := control.Status(ctx, config.NodeID)
	if err != nil || status.CatalogRole != "consumer" || status.Catalog.AppliedIndex != 0 || status.CatalogRaft.GroupID != "" ||
		len(status.Groups) != 1 || status.Groups[0].GroupID != "group-b" {
		t.Fatalf("owner-b acquired local catalog authority: status=%+v err=%v", status, err)
	}
	for _, root := range []string{config.DataRoot, config.RaftRoot} {
		if _, err := os.Stat(filepath.Join(root, string(config.Catalog.ID))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("consumer local catalog files under %s: %v", root, err)
		}
	}
}

// The control order is fixed: positive direct shard -> cached quorum loss ->
// cold restart without quorum -> restore/rewarm. The
// router and source-holder are voters throughout; only owner-b is a consumer.
func fixedPeerCatalogConsumerOwnerControlsV1(t *testing.T, ctx context.Context, configs []FixedPeerTCPConfigV1, processes []*fixedPeerTestProcessV1, control *FixedPeerTCPClientV1, publicClient *Client, request public.SearchRequestV1) {
	t.Helper()
	owner := configs[1]
	fixedPeerAssertCatalogConsumerOwnerV1(t, ctx, control, owner)
	reply, err := control.call(ctx, "source-holder", "vector-catalog-read", fixedPeerRequestV1{
		VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorCatalogActiveV1},
	}, false)
	if err != nil || reply.VectorCatalog == nil {
		t.Fatalf("current owner decision: %+v err=%v", reply, err)
	}
	v := owner.Vector
	search := func() (vectorPartitionShardSearchTCPFrameV1, error) {
		return fixedPeerCatalogConsumerShardSearchV1(t, ctx, configs, request, reply.VectorCatalog.ReadySetDigest)
	}
	assertRefused := func(label string) {
		t.Helper()
		frame, err := search()
		if (err == nil && frame.Error == nil) || fixedPeerCatalogConsumerShardHasHitsV1(frame.Response) {
			t.Fatalf("%s returned consumer candidates: frame=%+v err=%v", label, frame, err)
		}
		if report, err := control.ReadinessV1(ctx, "owner-b"); err == nil || report.Ready {
			t.Fatalf("%s advertised READY: report=%+v err=%v", label, report, err)
		}
		peer, err := DialContext(ctx, "tcp", v.PublicAddresses["owner-b"])
		if err != nil {
			t.Fatal(err)
		}
		status, statusErr := peer.VectorStatusV1(ctx)
		_ = peer.Close()
		if statusErr == nil && status.Health.Ready {
			t.Fatalf("%s public status advertised READY: %+v", label, status)
		}
	}
	if frame, err := search(); err != nil || frame.Error != nil || !fixedPeerCatalogConsumerShardHasHitsV1(frame.Response) {
		t.Fatalf("positive direct consumer shard: frame=%+v err=%v", frame, err)
	}
	// Remove two catalog voters, while both serving owner data groups remain
	// live. A router socket failure alone would not prove owner admission.
	processes[0].stop(t)
	processes[3].stop(t)
	assertRefused("cached catalog quorum loss")
	processes[1].stop(t)
	processes[1] = fixedPeerStartTestProcessV1(t, owner)
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := control.Status(ctx, "owner-b")
		return err == nil && len(status.Groups) == 1 && status.Groups[0].LeaderID == "owner-b"
	})
	fixedPeerAssertColdCatalogConsumerOwnerV1(t, ctx, configs, control)
	_, warmErr := control.call(ctx, "owner-b", "vector-lifecycle", fixedPeerRequestV1{
		VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorLifecycleWarmImmutableV1},
	}, true)
	if warmErr == nil {
		t.Fatal("cold consumer warmed from durable assets without catalog quorum")
	}
	assertRefused("cold catalog quorum loss")
	fixedPeerAssertCatalogConsumerOwnerV1(t, ctx, control, owner)
	processes[0] = fixedPeerStartTestProcessV1(t, configs[0])
	processes[3] = fixedPeerStartTestProcessV1(t, configs[3])
	fixedPeerWaitV1(t, ctx, func() bool {
		_, err := control.EnsureImmutableVectorLifecycleV1(ctx)
		return err == nil
	})
	// The public ingress connection belonged to the old router process.
	_ = publicClient.Close()
	restored, err := DialContext(ctx, "tcp", v.PublicAddresses["ingress"])
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	request.Deadline = time.Now().Add(20 * time.Second)
	if response, err := restored.VectorSearchStrictV1(ctx, request); err != nil || len(response.Neighbors) == 0 {
		t.Fatalf("restored consumer public search: response=%+v err=%v", response, err)
	}
	if frame, err := search(); err != nil || frame.Error != nil || !fixedPeerCatalogConsumerShardHasHitsV1(frame.Response) {
		t.Fatalf("restored cached consumer shard: frame=%+v err=%v", frame, err)
	}
	fixedPeerAssertCatalogConsumerOwnerV1(t, ctx, control, owner)
}

func fixedPeerCatalogConsumerShardHasHitsV1(response *VectorPartitionShardSearchResponseV1) bool {
	if response != nil {
		for _, partial := range response.Partials {
			if len(partial.Neighbors) != 0 {
				return true
			}
		}
	}
	return false
}

func fixedPeerCatalogConsumerShardSearchV1(t testing.TB, ctx context.Context, configs []FixedPeerTCPConfigV1, request public.SearchRequestV1, readyDigest string) (vectorPartitionShardSearchTCPFrameV1, error) {
	t.Helper()
	owner := configs[1]
	v := owner.Vector
	transport, err := NewPeerTransportV1(configs[0])
	if err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	defer transport.Close()
	shard := vectorPartitionShardSearchRequestTestV1([]uint32{0})
	shard.Database, shard.Catalog, shard.Collection = v.Collection.Database, v.Collection.Catalog, v.Collection.Collection
	shard.IndexName, shard.IndexDefinitionDigest = v.Identity.Index.IndexName, v.Identity.Index.IndexDefinitionDigest
	shard.SourceGeneration, shard.SourceChecksum = v.Identity.Source.Generation, v.Identity.Source.Checksum
	shard.SourceSchemaHash, shard.SourceRowCount = v.Identity.Source.SchemaHash, v.Identity.Source.RowCount
	shard.PartitionGeneration, shard.RouterGeneration = v.Identity.Generation, v.Identity.Generation
	shard.TargetGroupID, shard.TargetNodeID = "group-b", "owner-b"
	shard.ReadySetDigest = readyDigest
	shard.Query = slices.Clone(request.Query)
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := transport.dialScope(callCtx, v.ShardAddresses["group-b"]["owner-b"], "owner-b", "shard:group-b")
	if err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	shard.DeadlineUnixNano = time.Now().Add(5 * time.Second).UnixNano()
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Request: &shard}, vectorPartitionShardSearchTCPMaxFrameBytesV1); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	return readVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPMaxFrameBytesV1)
}

func fixedPeerAssertColdCatalogConsumerOwnerV1(t testing.TB, ctx context.Context, configs []FixedPeerTCPConfigV1, control *FixedPeerTCPClientV1) {
	t.Helper()
	owner := configs[1]
	transport, err := NewPeerTransportV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	assertUnbound := func() {
		callCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		conn, err := transport.dialScope(callCtx, owner.Vector.ShardAddresses["group-b"]["owner-b"], "owner-b", "shard:group-b")
		if err == nil {
			conn.Close()
			t.Fatal("observation warmed a cold consumer shard listener")
		}
	}
	assertUnbound()
	peer, err := DialContext(ctx, "tcp", owner.Vector.PublicAddresses["owner-b"])
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if status, err := peer.VectorStatusV1(ctx); err != nil || status.Health.Ready || status.Health.Reason != "topology_unavailable" {
		t.Fatalf("cold consumer status must report unwarmed topology: %+v %v", status, err)
	}
	if report, err := control.ReadinessV1(ctx, "owner-b"); err == nil || report.Ready {
		t.Fatalf("cold consumer readiness advertised READY: %+v %v", report, err)
	}
	assertUnbound()
	fixedPeerAssertCatalogConsumerOwnerV1(t, ctx, control, owner)
}

func fixedPeerCatalogConsumerCachedInvalidationV1(t *testing.T, ctx context.Context, configs []FixedPeerTCPConfigV1, control *FixedPeerTCPClientV1, client *Client, request public.SearchRequestV1) {
	t.Helper()
	reply, err := control.call(ctx, "source-holder", "vector-catalog-read", fixedPeerRequestV1{VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorCatalogActiveV1}}, false)
	if err != nil || reply.VectorCatalog == nil {
		t.Fatalf("consumer invalidation initial decision: %+v %v", reply, err)
	}
	digest := reply.VectorCatalog.ReadySetDigest
	frame, err := fixedPeerCatalogConsumerShardSearchV1(t, ctx, configs, request, digest)
	if err != nil || frame.Error != nil || !fixedPeerCatalogConsumerShardHasHitsV1(frame.Response) {
		t.Fatalf("initial cached consumer search: %+v %v", frame, err)
	}
	fixedPeerAssertInFlightActiveInvalidationV1(t, ctx, os.Getenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL"), control, client, request)
	frame, err = fixedPeerCatalogConsumerShardSearchV1(t, ctx, configs, request, digest)
	if (err == nil && frame.Error == nil) || fixedPeerCatalogConsumerShardHasHitsV1(frame.Response) {
		t.Fatalf("invalidated cached consumer returned candidates: %+v %v", frame, err)
	}
	if report, err := control.ReadinessV1(ctx, "owner-b"); err == nil || report.Ready {
		t.Fatalf("invalidated consumer readiness: %+v %v", report, err)
	}
	peer, err := DialContext(ctx, "tcp", configs[1].Vector.PublicAddresses["owner-b"])
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if status, err := peer.VectorStatusV1(ctx); err == nil && status.Health.Ready {
		t.Fatalf("invalidated consumer public status: %+v", status)
	}
	fixedPeerAssertCatalogConsumerOwnerV1(t, ctx, control, configs[1])
}

func TestFixedPeerImmutableVectorCatalogDecisionBindsIdentityAndReadyV1(t *testing.T) {
	seed := newVectorPartitionLiveNativewireDocumentsForOwnersModeV1(t, []vectorPartitionLiveDocumentV1{
		{id: "a", vector: []float32{1, 0}, home: 0}, {id: "b", vector: []float32{.8, .2}, home: 1},
		{id: "c", vector: []float32{0, 1}, home: 2}, {id: "d", vector: []float32{.2, .8}, home: 2},
	}, nil, [2]string{"group-b", "group-c"}, true, true)
	defer seed.database.Close()
	configs := fixedPeerMultiOwnerSearchConfigsV1(t, seed.manifest, seed.collection.MetaView())
	t.Run("owner_config_boundaries", func(t *testing.T) {
		fixedPeerCatalogConsumerConfigBoundariesV1(t, configs)
	})
	owned, _, err := validateFixedPeerConfigV1(configs[1])
	if err != nil {
		t.Fatal(err)
	}
	r := &FixedPeerTCPRuntimeV1{config: owned, immutableVectorAssetDigests: fixedPeerImmutableVectorAssetDigestsV1(owned.Vector)}
	i := r.config.Vector.Identity
	status := raftplacement.CatalogMetaStatusV1{Epoch: i.Index.CatalogEpoch, Digest: i.Index.CatalogDigest, AppliedIndex: 9}
	owners := fixedPeerVectorOwnerGroupsV1(r.config.Vector.Placement)
	record := raftplacement.VectorPartitionLifecycleRecordV1{Identity: i, State: raftplacement.VectorPartitionLifecycleActiveV1, RequiredGroups: owners}
	for _, owner := range owners {
		record.ReadyGroups = append(record.ReadyGroups, raftplacement.VectorPartitionLifecycleGroupReadyV1{GroupID: owner, AppliedIndex: 7, AssetSetDigest: vectorPartitionM8GroupAssetSetDigestV1(string(owner), seed.manifest)})
	}
	record.ReadySetDigest, err = raftplacement.VectorPartitionLifecycleReadySetDigestV1(i, owners, record.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.validateImmutableVectorCatalogDecisionV1(status, record, fixedPeerVectorCatalogActiveV1); err != nil {
		t.Fatalf("valid ACTIVE decision: %v", err)
	}
	t.Run("caller_config_mutation_isolated", func(t *testing.T) {
		caller := configs[1].Vector
		caller.Manifest.Assets[0].Bytes++
		caller.Manifest.RouterAsset.Checksum = "caller-mutated"
		caller.Manifest.Placements[0].GroupID = "caller-mutated"
		caller.Placement.Partitions[0].GroupID = "caller-mutated"
		caller.Identity.Immutable.ManifestDigest = "caller-mutated"
		if r.config.Vector.Identity != i || r.config.Vector.Manifest.Assets[0].Bytes == caller.Manifest.Assets[0].Bytes ||
			r.config.Vector.Manifest.RouterAsset.Checksum == caller.Manifest.RouterAsset.Checksum ||
			r.config.Vector.Manifest.Placements[0].GroupID == caller.Manifest.Placements[0].GroupID ||
			r.config.Vector.Placement.Partitions[0].GroupID == caller.Placement.Partitions[0].GroupID {
			t.Fatal("runtime configuration aliases caller-owned immutable bindings")
		}
		if err := r.validateImmutableVectorCatalogDecisionV1(status, record, fixedPeerVectorCatalogActiveV1); err != nil {
			t.Fatalf("caller mutation changed valid ACTIVE decision: %v", err)
		}
	})
	t.Run("changed_assets_with_fresh_ready_digest_refused", func(t *testing.T) {
		changed := record
		changed.ReadyGroups = slices.Clone(record.ReadyGroups)
		changed.ReadyGroups[0].AssetSetDigest = strings.Repeat("f", 64)
		if changed.ReadyGroups[0].AssetSetDigest == record.ReadyGroups[0].AssetSetDigest {
			changed.ReadyGroups[0].AssetSetDigest = strings.Repeat("a", 64)
		}
		changed.ReadySetDigest, err = raftplacement.VectorPartitionLifecycleReadySetDigestV1(i, owners, changed.ReadyGroups)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.validateImmutableActiveVectorRecordV1(changed, owners); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
			t.Fatalf("changed assets admitted with a self-consistent READY digest: %v", err)
		}
	})
	t.Run("missing_precomputed_binding_fails_closed", func(t *testing.T) {
		uninitialized := &FixedPeerTCPRuntimeV1{config: owned}
		if err := uninitialized.validateImmutableActiveVectorRecordV1(record, owners); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
			t.Fatalf("uninitialized runtime admitted ACTIVE: %v", err)
		}
	})
	t.Run("fresh_ready_apply_is_not_cached", func(t *testing.T) {
		changed := record
		changed.ReadyGroups = slices.Clone(record.ReadyGroups)
		changed.ReadyGroups[0].AppliedIndex++
		// The old digest must fail even though the fixed asset expectations match.
		if err := r.validateImmutableActiveVectorRecordV1(changed, owners); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
			t.Fatalf("changed READY apply admitted with old digest: %v", err)
		}
		changed.ReadySetDigest, err = raftplacement.VectorPartitionLifecycleReadySetDigestV1(i, owners, changed.ReadyGroups)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.validateImmutableActiveVectorRecordV1(changed, owners); err != nil {
			t.Fatalf("fresh READY apply with its own digest refused: %v", err)
		}
	})
	building := record
	building.State = raftplacement.VectorPartitionLifecycleBuildingV1
	building.ReadyGroups, building.ReadySetDigest = nil, ""
	if err := r.validateImmutableVectorCatalogDecisionV1(status, building, fixedPeerVectorCatalogStageV1); err != nil {
		t.Fatalf("valid BUILD decision: %v", err)
	}
	if err := r.validateImmutableVectorCatalogDecisionV1(status, building, fixedPeerVectorCatalogActiveV1); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("BUILD served as ACTIVE: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*raftplacement.CatalogMetaStatusV1, *raftplacement.VectorPartitionLifecycleRecordV1)
	}{
		{"catalog epoch", func(s *raftplacement.CatalogMetaStatusV1, _ *raftplacement.VectorPartitionLifecycleRecordV1) {
			s.Epoch++
		}},
		{"catalog digest", func(s *raftplacement.CatalogMetaStatusV1, _ *raftplacement.VectorPartitionLifecycleRecordV1) {
			s.Digest = "wrong"
		}},
		{"no apply", func(s *raftplacement.CatalogMetaStatusV1, _ *raftplacement.VectorPartitionLifecycleRecordV1) {
			s.AppliedIndex = 0
		}},
		{"generation", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Identity.Generation++
		}},
		{"source", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Identity.Source.Checksum++
		}},
		{"incarnation", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Identity.Index.CollectionIncarnation++
		}},
		{"index epoch", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Identity.Index.IndexEpoch++
		}},
		{"manifest", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Identity.Immutable.ManifestDigest = "wrong"
		}},
		{"placement", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Identity.Immutable.PlacementDigest = "wrong"
		}},
		{"missing owner", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.RequiredGroups = r.RequiredGroups[1:]
		}},
		{"missing READY", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.ReadyGroups = r.ReadyGroups[1:]
		}},
		{"zero READY apply", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.ReadyGroups[0].AppliedIndex = 0
		}},
		{"wrong READY asset", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.ReadyGroups[0].AssetSetDigest = "wrong"
		}},
		{"wrong READY owner", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.ReadyGroups[0].GroupID = "group-a"
		}},
		{"READY digest", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.ReadySetDigest = "wrong"
		}},
		{"invalidation", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.InvalidationEpoch++
		}},
		{"abort", func(_ *raftplacement.CatalogMetaStatusV1, r *raftplacement.VectorPartitionLifecycleRecordV1) {
			r.Aborted = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, changed := status, record
			changed.RequiredGroups, changed.ReadyGroups = slices.Clone(record.RequiredGroups), slices.Clone(record.ReadyGroups)
			test.change(&s, &changed)
			for _, action := range []fixedPeerVectorLifecycleActionV1{fixedPeerVectorCatalogActiveV1, fixedPeerVectorCatalogStageV1} {
				if err := r.validateImmutableVectorCatalogDecisionV1(s, changed, action); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
					t.Fatalf("accepted %s for %s: %v", test.name, action, err)
				}
			}
		})
	}
	if peerControlScopeV1("vector-catalog-read") != "control-read" || peerControlRequestModeV1("vector-catalog-read", peerRequestIngressV1) != peerRequestInternalV1 {
		t.Fatal("consumer decision is not a bounded drain dependency read")
	}
}

func fixedPeerCatalogConsumerConfigBoundariesV1(t *testing.T, configs []FixedPeerTCPConfigV1) {
	t.Helper()
	ca := newPeerCAFixtureV1(t)
	configs = slices.Clone(configs)
	for i := range configs {
		configs[i].RaftListen = maps.Clone(configs[i].RaftListen)
		configs[i].ClusterID = "consumer-config-boundaries"
		configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	}
	fixedPeerCatalogConsumerOwnerConfigV1(t, configs)
	owner, _, err := validateFixedPeerConfigV1(configs[1])
	if err != nil {
		t.Fatalf("credentialed immutable owner preflight: %v", err)
	}
	client, err := NewFixedPeerTCPClientV1(owner)
	if err != nil {
		t.Fatalf("authenticated consumer client constructor: %v", err)
	}
	client.Close()
	transport, err := NewPeerTransportV1(owner)
	if err != nil {
		t.Fatalf("authenticated consumer transport constructor: %v", err)
	}
	_ = transport.Close()
	assertNoStorage := func(t *testing.T) {
		t.Helper()
		for _, root := range []string{owner.DataRoot, owner.RaftRoot} {
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("config/client/authentication preflight created storage %s: %v", root, err)
			}
		}
	}
	assertNoStorage(t)
	for _, index := range []int{0, 1, 3} {
		name := string(configs[index].NodeID)
		t.Run(name+"_requires_catalog_vote", func(t *testing.T) {
			config := configs[index]
			if index == 1 {
				config.Credentials = nil
			} else {
				config.Catalog.Peers = slices.DeleteFunc(slices.Clone(config.Catalog.Peers), func(peer raftcluster.Peer) bool { return peer.ID == config.NodeID })
				config.Catalog.BootstrapNode = "owner-c"
				config.RaftListen = maps.Clone(config.RaftListen)
				delete(config.RaftListen, config.Catalog.ID)
			}
			if _, _, err := validateFixedPeerConfigV1(config); !errors.Is(err, raftcluster.ErrInvalidConfig) || err.Error() != "raftcluster: invalid config: fixed-peer vector runtime requires local catalog authority" {
				t.Fatalf("nonowner or uncredentialed consumer admitted: %v", err)
			}
		})
	}
	t.Run("multiple_local_groups", func(t *testing.T) {
		if err := validateFixedPeerVectorConfigV1(owner, map[raftcluster.GroupID]bool{"group-b": true, "group-c": true}); err == nil || err.Error() != "fixed-peer vector runtime requires exactly one local data group" {
			t.Fatalf("consumer with multiple local groups admitted: %v", err)
		}
	})
	t.Run("mutable_requires_catalog_vote", TestFixedPeerVectorConfigRequiresLocalCatalogAuthorityV1)
	t.Run("immutable_manifest_binding", func(t *testing.T) {
		config := owner
		config.Vector = cloneFixedPeerVectorConfigV1(owner.Vector)
		config.Vector.Identity.Immutable.ManifestDigest = "wrong"
		if _, _, err := validateFixedPeerConfigV1(config); !errors.Is(err, raftcluster.ErrInvalidConfig) {
			t.Fatalf("consumer with a mismatched immutable manifest admitted: %v", err)
		}
	})
	for _, invalid := range []string{"wrong_node", "expired"} {
		t.Run(invalid, func(t *testing.T) {
			config := owner
			node, notAfter := string(owner.NodeID), time.Now().Add(time.Hour)
			if invalid == "wrong_node" {
				node = "ingress"
			} else {
				notAfter = time.Now().Add(-time.Minute)
			}
			config.Credentials = ca.issue(t, owner.ClusterID, node, time.Now().Add(-time.Hour), notAfter)
			if _, err := NewFixedPeerTCPClientV1(config); !errors.Is(err, errPeerAuthenticationV1) {
				t.Fatalf("invalid consumer client credentials admitted: %v", err)
			}
			if _, err := NewPeerTransportV1(config); !errors.Is(err, errPeerAuthenticationV1) {
				t.Fatalf("invalid consumer transport credentials admitted: %v", err)
			}
			if _, err := fixedPeerOpenTestRuntimeV1(t, config); !errors.Is(err, errPeerAuthenticationV1) {
				t.Fatalf("invalid consumer runtime credentials admitted: %v", err)
			}
			assertNoStorage(t)
		})
	}
}
