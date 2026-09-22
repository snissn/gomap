package nativewire

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

type liveLifecycleRouteV1 struct{ domains, packs []uint32 }

// This opt-in test mutates only a deliberately named copy, never an M3 asset.
// The enclosing admission script retains the source/copy hashes and resources.
type liveLifecycleRetainedInputV1 struct {
	DB, Queries, Truth, ManifestSHA256           string
	Collection, Index                            string
	Generation                                   uint64
	Probes                                       int
	Python, PythonSHA256, Adapter, AdapterSHA256 string
}

func liveLifecycleReadPinnedV1(t *testing.T, path, expected string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) != 64 || fmt.Sprintf("%x", sha256.Sum256(raw)) != expected {
		t.Fatalf("pin mismatch: %s", path)
	}
	return raw
}

func liveLifecycleRetainedInputForV1(t *testing.T) liveLifecycleRetainedInputV1 {
	t.Helper()
	raw, err := os.ReadFile(os.Getenv("GOMAP_SELECTED_LIVE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	var input liveLifecycleRetainedInputV1
	if err := json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(input.DB) || filepath.Base(filepath.Clean(input.DB)) != "mutable-lifecycle-copy" || input.Collection != "m3_partition_source" || input.Generation == 0 || input.Index == "" || input.Probes < 1 || input.Probes >= 16 {
		t.Fatal("invalid retained mutable-copy specification")
	}
	if os.Getenv("GOMAP_SELECTED_LIVE_RECEIPTS") == "" {
		t.Fatal("retained real lifecycle requires a receipt directory")
	}
	liveLifecycleReadPinnedV1(t, input.Python, input.PythonSHA256)
	liveLifecycleReadPinnedV1(t, input.Adapter, input.AdapterSHA256)
	t.Logf("retained_input_sha256=%x", sha256.Sum256(raw))
	return input
}

func liveLifecycleOpenRetainedV1(t *testing.T) (vectorPartitionLiveProductionFixtureV1, map[string][]float32, [][]float32, int) {
	t.Helper()
	in := liveLifecycleRetainedInputForV1(t)
	format, present, err := backenddb.LoadFormatConfig(in.DB)
	if err != nil || !present || !format.RequiresCommandWALV2() {
		t.Fatalf("copy lacks command-WAL eligibility: %v", err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: in.DB, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	// The driver takes ownership on success; failures still release the DB.
	ok := false
	defer func() {
		if !ok {
			_ = db.Close()
		}
	}()
	if !db.CommandWALEnabled() {
		t.Fatal("command WAL is disabled")
	}
	col, err := collections.NewCollectionManager(db).OpenCollection(in.Collection)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := col.PreparedVectorPartitionManifestWithContextV1(t.Context(), in.Index, in.Generation)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != in.ManifestSHA256 {
		t.Fatalf("manifest pin mismatch: %v", err)
	}
	if err := validateLiveLifecycleRetainedGeometryV1(manifest); err != nil {
		t.Fatal(err)
	}
	fixture := vectorPartitionLiveProductionFixtureV1{dir: in.DB, database: db, collection: col, manifest: manifest}
	for _, def := range col.Meta().VectorIndexes {
		if def.Name == in.Index {
			fixture.definition = def
		}
	}
	if fixture.definition.Dimensions != 768 || fixture.definition.Strategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatal("wrong retained definition")
	}
	source, rows, err := col.ReadVectorPartitionRouterSourceRowsV1(in.Index)
	if err != nil {
		t.Fatal(err)
	}
	if source.Generation != manifest.SourceGeneration || source.Checksum != manifest.SourceChecksum || source.SchemaHash != manifest.SourceSchemaHash || source.RowCount != manifest.SourceRowCount {
		t.Fatal("retained source identity drift")
	}
	vectors := make(map[string][]float32, len(rows))
	for _, row := range rows {
		if _, duplicate := vectors[string(row.DocumentID)]; duplicate {
			t.Fatal("duplicate stable ID")
		}
		vectors[string(row.DocumentID)] = row.Values
	}
	raw = liveLifecycleReadPinnedV1(t, in.Queries, "8a27d38fb9d79e3607ee393af989644874e965813d7f26a55258dc60a0b1b70e")
	if len(raw) != 512*768*4 {
		t.Fatal("wrong real query shape")
	}
	queries := make([][]float32, 512)
	for q := range queries {
		queries[q] = make([]float32, 768)
		for d := range queries[q] {
			queries[q][d] = math.Float32frombits(binary.LittleEndian.Uint32(raw[(q*768+d)*4:]))
		}
	}
	liveLifecyclePlacementV1(t, &fixture)
	ok = true
	return fixture, vectors, queries, in.Probes
}

func validateLiveLifecycleRetainedGeometryV1(manifest collections.VectorPartitionManifestV1) error {
	if manifest.SourceRowCount != 100000 || manifest.DomainCount != 16 {
		return fmt.Errorf("retained fixture must be real100K/D16")
	}
	input := vectorpartition.SelectedShardPlanRequestV1(100000, 768)
	input.LogicalDomains, input.TargetHotBytes = 16, 7696384
	plan, err := vectorpartition.PlanByteBoundedShardsV1(input)
	if err != nil {
		return err
	}
	policy, valid := collections.ParseVectorPartitionOverlapPolicyV1(manifest.BalancePolicy)
	// PackDomainMembershipsV1 replaces the logical-domain capacity with the
	// per-pack capacity before M3 persists the policy. Do not compare units.
	if !valid || policy.Budget != 20000 || policy.Realized == 0 || policy.Capacity != uint64(plan.OverlapCapacity) || manifest.PartitionCount != uint32(plan.Partitions) {
		return fmt.Errorf("retained fixture must be the selected 20%% overlap multi-pack asset: policy=%+v packs=%d, want pack_capacity=%d packs=%d", policy, manifest.PartitionCount, plan.OverlapCapacity, plan.Partitions)
	}
	return nil
}

func TestVectorPartitionLiveRetainedGeometryV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{SourceRowCount: 100000, DomainCount: 16, PartitionCount: 64}
	for _, tc := range []struct {
		name      string
		capacity  uint64
		budget    uint64
		realized  uint64
		packs     uint32
		wantError bool
	}{
		{"physical-pack-capacity", 1875, 20000, 20000, 64, false},
		{"logical-domain-capacity-is-not-pack-capacity", 7500, 20000, 20000, 64, true},
		{"no-overlap", 1875, 20000, 0, 64, true},
		{"disjoint-budget", 1875, 0, 0, 64, true},
		{"wrong-pack-count", 1875, 20000, 20000, 16, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := manifest
			m.PartitionCount = tc.packs
			var err error
			m.BalancePolicy, err = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: tc.capacity, Budget: tc.budget, Realized: tc.realized, Unspent: tc.budget - tc.realized})
			if err != nil {
				t.Fatal(err)
			}
			if err := validateLiveLifecycleRetainedGeometryV1(m); (err != nil) != tc.wantError {
				t.Fatalf("geometry error=%v, wantError=%v", err, tc.wantError)
			}
		})
	}
	manifest.BalancePolicy = "malformed"
	if err := validateLiveLifecycleRetainedGeometryV1(manifest); err == nil {
		t.Fatal("accepted malformed overlap policy")
	}
}

// Match M3's actual encoded-byte boundary, not only AccountShardPacksV1's
// modeled envelope, before publishing a replacement generation.
func validateLiveLifecyclePackBytesV1(assets []collections.VectorPartitionAssetV1, accounts []vectorpartition.ShardPackSummaryV1) error {
	if len(assets) != len(accounts) || len(accounts) == 0 {
		return fmt.Errorf("materialized %d shard packs for %d planned packs", len(assets), len(accounts))
	}
	seen := make([]bool, len(accounts))
	for _, asset := range assets {
		p := asset.PartitionID
		if p >= uint32(len(accounts)) || seen[p] || accounts[p].Partition != int(p) {
			return fmt.Errorf("materialized shard pack %d is duplicate or outside the canonical plan", p)
		}
		seen[p] = true
		if asset.Bytes == 0 || asset.Bytes > accounts[p].Bytes {
			return fmt.Errorf("materialized shard pack %d bytes=%d outside planned envelope (0,%d]", p, asset.Bytes, accounts[p].Bytes)
		}
	}
	return nil
}

func TestVectorPartitionLiveRetainedPackBytesV1(t *testing.T) {
	accounts := []vectorpartition.ShardPackSummaryV1{{Partition: 0, Bytes: 100}, {Partition: 1, Bytes: 200}}
	for _, tc := range []struct {
		name      string
		assets    []collections.VectorPartitionAssetV1
		wantError bool
	}{
		{"valid", []collections.VectorPartitionAssetV1{{PartitionID: 0, Bytes: 100}, {PartitionID: 1, Bytes: 199}}, false},
		{"missing", []collections.VectorPartitionAssetV1{{PartitionID: 0, Bytes: 100}}, true},
		{"duplicate", []collections.VectorPartitionAssetV1{{PartitionID: 0, Bytes: 100}, {PartitionID: 0, Bytes: 100}}, true},
		{"outside-plan", []collections.VectorPartitionAssetV1{{PartitionID: 0, Bytes: 100}, {PartitionID: 2, Bytes: 100}}, true},
		{"zero", []collections.VectorPartitionAssetV1{{PartitionID: 0, Bytes: 0}, {PartitionID: 1, Bytes: 100}}, true},
		{"oversized", []collections.VectorPartitionAssetV1{{PartitionID: 0, Bytes: 101}, {PartitionID: 1, Bytes: 100}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateLiveLifecyclePackBytesV1(tc.assets, accounts); (err != nil) != tc.wantError {
				t.Fatalf("pack byte error=%v, wantError=%v", err, tc.wantError)
			}
		})
	}
}

// The live writer scans all representatives, unlike approximate query routing.
// Check actual tiny live-domain contents instead of inferring ownership from
// an approximate router winner. This is untimed quiescent bookkeeping.
func liveLifecycleCheckOwnersV1(t *testing.T, f vectorPartitionLiveProductionFixtureV1, changed, deleted string, query []float32) {
	t.Helper()
	pin, err := f.collection.AcquireVectorPartitionLiveSearchPinV1(f.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	owners := make(map[string]uint32)
	for domain := range f.manifest.DomainCount {
		results, _, err := pin.SearchDomainV1(t.Context(), domain, query, collections.VectorPartitionSearchOptionsV1{TopK: 2, EfSearch: 96, MaxStableIDBytes: 128})
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			if _, duplicate := owners[result.ID]; duplicate {
				t.Fatal("live ID exists in multiple domains")
			}
			owners[result.ID] = domain
		}
	}
	a, hasChanged := owners[changed]
	b, hasInserted := owners["live"]
	if len(owners) != 2 || !hasChanged || !hasInserted || a == b || !pin.ExcludesBaseIDV1(deleted) || !pin.ExcludesBaseIDV1(changed) {
		t.Fatalf("cross-domain ownership or stale-base exclusion missing: %v", owners)
	}
	retainLiveLifecycleJSONV1(t, "live-owners", struct {
		Status  collections.VectorIndexPartitionLiveStatusV1
		Owners  map[string]uint32
		Deleted string
	}{pin.StatusV1(), owners, deleted})
}

func liveLifecyclePlacementV1(t *testing.T, f *vectorPartitionLiveProductionFixtureV1) {
	t.Helper()
	m := f.manifest
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: m.Collection}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: m.IndexName, IndexDefinitionDigest: m.IndexDefinitionDigest, SourceGeneration: m.SourceGeneration, SourceChecksum: m.SourceChecksum, SourceSchemaHash: m.SourceSchemaHash, SourceRowCount: m.SourceRowCount, PartitionGeneration: m.Generation, PartitionCount: m.PartitionCount}
	catalog := raftplacement.CatalogV1{Features: raftplacement.DefaultFeatureSet()}
	seen := map[string]bool{}
	for _, p := range m.Placements {
		group := raftcluster.GroupID(p.GroupID)
		if !seen[p.GroupID] {
			node := raftcluster.NodeID(p.GroupID + "-node")
			catalog.Groups = append(catalog.Groups, raftplacement.GroupV1{ID: group, Members: []raftcluster.NodeID{node}, LeaderHint: node})
			seen[p.GroupID] = true
		}
		placement.Partitions = append(placement.Partitions, raftplacement.VectorPartitionGroupV1{PartitionID: p.PartitionID, GroupID: group})
	}
	if len(catalog.Groups) == 0 {
		t.Fatal("empty placement")
	}
	catalog.Placements = []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: catalog.Groups[0].ID, Mode: raftplacement.PlacementModeCollectionV1}}
	resolved, err := raftplacement.Validate(catalog)
	if err != nil {
		t.Fatal(err)
	}
	f.catalog, f.placement = resolved, placement
}

func liveLifecycleRoutesV1(t *testing.T, f vectorPartitionLiveProductionFixtureV1, queries [][]float32, probes int) []liveLifecycleRouteV1 {
	t.Helper()
	router, _, err := f.collection.OpenVectorPartitionRouterV1(f.definition.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	routes := make([]liveLifecycleRouteV1, len(queries))
	for q, query := range queries {
		result, err := router.Search(query, collections.VectorPartitionRouterSearchOptionsV3{Mode: collections.VectorPartitionRouterModeApproxV1, ScoreBudget: 256, PartitionProbes: probes})
		if err != nil {
			t.Fatal(err)
		}
		for _, domain := range result.Partitions {
			routes[q].domains = append(routes[q].domains, domain.PartitionID)
			for _, pack := range f.manifest.DomainPacks {
				if pack.DomainID == domain.PartitionID {
					routes[q].packs = append(routes[q].packs, pack.PackID)
					break
				}
			}
		}
		if len(routes[q].domains) != probes || len(routes[q].packs) != probes {
			t.Fatal("incomplete expected route")
		}
	}
	return routes
}

func liveLifecycleValidateRetainedTruthV1(t *testing.T, truth []liveLifecycleTruthV1) {
	t.Helper()
	in := liveLifecycleRetainedInputForV1(t)
	raw := liveLifecycleReadPinnedV1(t, in.Truth, "347a618317d2fde97802c2c84593980d36563c27dd2a859ab46320ca674f5fc8")
	var baseline struct {
		Truth [][]VectorPartitionCoordinatorNeighborV1
	}
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	if len(baseline.Truth) != len(truth) {
		t.Fatal("baseline truth query mismatch")
	}
	for q, neighbors := range baseline.Truth {
		if len(neighbors) != len(truth[q].top) {
			t.Fatal("baseline top-k mismatch")
		}
		for _, n := range neighbors {
			if !truth[q].top[n.ID] || math.Float32bits(truth[q].scores[n.ID]) != math.Float32bits(n.Score) {
				t.Fatalf("baseline truth mismatch q=%d id=%s", q, n.ID)
			}
		}
	}
}

// Only a seed assignment for the bound external request; never admitted as the
// real-data partitioner. Same bounded round-robin request as the M3 command.
type liveLifecycleRequestPartitionerV1 struct{}

func (liveLifecycleRequestPartitionerV1) Name() string    { return "kahip_request_seed_v1" }
func (liveLifecycleRequestPartitionerV1) License() string { return "request-only" }
func (liveLifecycleRequestPartitionerV1) Partition(g vectorpartition.Graph, parts, capacity int) ([]int, error) {
	if parts < 1 || len(g.Neighbors) < parts || capacity < (len(g.Neighbors)+parts-1)/parts {
		return nil, fmt.Errorf("invalid request shape")
	}
	a := make([]int, len(g.Neighbors))
	for i := range a {
		a[i] = i % parts
	}
	return a, nil
}

// Quiesced, full immutable rebuild. No request-path rebuild, automatic folding,
// or new product orchestration is introduced by this component test.
func liveLifecycleRebuildV1(t *testing.T, f vectorPartitionLiveProductionFixtureV1, current map[string][]float32) collections.VectorPartitionManifestV1 {
	t.Helper()
	router, _, err := f.collection.OpenPreparedVectorPartitionRouterForLiveRecoveryWithContextV1(t.Context(), f.definition.Name, f.manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	routerConfig := router.Status().Config
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	status, err := f.collection.RebuildVectorIndex(f.definition.Name)
	if err != nil || !status.Loaded {
		t.Fatalf("source rebuild: %+v err=%v", status, err)
	}
	source, rows, err := f.collection.ReadVectorPartitionRouterSourceRowsV1(f.definition.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(current) || source.Generation == f.manifest.SourceGeneration && source.Checksum == f.manifest.SourceChecksum {
		t.Fatal("source rebuild did not absorb mutations")
	}
	input := make([]vectorpartition.Vector, len(rows))
	byID := make(map[string]collections.VectorPartitionRouterSourceRowV1, len(rows))
	for i, row := range rows {
		id := string(row.DocumentID)
		if row.VectorOrdinal != uint64(i) || !slices.Equal(row.Values, current[id]) {
			t.Fatalf("rebuilt source mismatch id=%s", id)
		}
		if _, duplicate := byID[id]; duplicate {
			t.Fatal("duplicate rebuilt ID")
		}
		byID[id] = row
		values := make([]float64, len(row.Values))
		for d, value := range row.Values {
			values[d] = float64(value)
		}
		input[i] = vectorpartition.Vector{ID: id, Values: values}
	}
	config := vectorpartition.DefaultConfig()
	config.Seed, config.Partitions, config.MaxDistanceWork = 4017, int(f.manifest.DomainCount), 120_000_000_000
	config.MaxPartitionWork = 250_000_000
	planInput := vectorpartition.SelectedShardPlanRequestV1(len(rows), f.definition.Dimensions)
	planInput.LogicalDomains, planInput.TargetHotBytes = config.Partitions, 1<<20
	backend := vectorpartition.Partitioner(vectorpartition.ReferencePartitioner{})
	real := f.manifest.Collection != "docs"
	if real {
		planInput.TargetHotBytes = 7696384
		backend = liveLifecycleRequestPartitionerV1{}
	}
	plan, err := vectorpartition.PlanByteBoundedShardsV1(planInput)
	if err != nil {
		t.Fatal(err)
	}
	if real && uint32(plan.Partitions) != f.manifest.PartitionCount {
		t.Fatal("replacement pack geometry drift")
	}
	artifact, err := vectorpartition.BuildWithPartitioner(input, config, vectorpartition.Source{SourceID: fmt.Sprintf("lifecycle-source:%d:%d", source.Generation, source.Checksum)}, backend)
	if err != nil {
		t.Fatal(err)
	}
	var homes []int
	var packing *vectorpartition.HomePackingReceiptV1
	if real {
		in := liveLifecycleRetainedInputForV1(t)
		liveLifecycleReadPinnedV1(t, in.Python, in.PythonSHA256)
		adapter := liveLifecycleReadPinnedV1(t, in.Adapter, in.AdapterSHA256)
		request := artifact
		raw, err := vectorpartition.CanonicalJSON(request)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		artifact, err = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{in.Python, "-c", string(adapter)}, raw, vectorpartition.ExternalJSONLimits{MaxInput: len(raw), MaxOutput: len(raw) + len(rows)*5 + 1024}, request)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Minute)
		var receipt vectorpartition.HomePackingReceiptV1
		homes, receipt, err = vectorpartition.RunExternalHomePackingV1(ctx, []string{in.Python, "-c", string(adapter)}, vectorpartition.ExternalJSONLimits{
			MaxInput: 256 << 20, MaxOutput: 8*len(rows) + 1024,
		}, plan, artifact)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		packing = &receipt
	}
	digest, err := vectorpartition.Digest(artifact)
	if err != nil {
		t.Fatal(err)
	}
	overlapConfig := vectorpartition.SelectedOverlapConfigV1(plan.DomainOverlapCapacity)
	overlap, err := vectorpartition.BuildOverlap(artifact, overlapConfig)
	if err != nil {
		t.Fatal(err)
	}
	if homes != nil {
		overlap, err = vectorpartition.PackDomainMembershipsWithHomesV1(plan, overlap, homes)
	} else {
		overlap, err = vectorpartition.PackDomainMembershipsV1(plan, overlap)
	}
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := vectorpartition.AccountShardPacksV1(plan, overlap.Memberships)
	if err != nil {
		t.Fatal(err)
	}
	build := struct {
		Artifact    string
		Config      vectorpartition.Config
		Router      vectorpartition.RouterConfigV1
		Plan        vectorpartition.ShardPlanV1
		Overlap     vectorpartition.OverlapConfig
		HomePacking *vectorpartition.HomePackingReceiptV1
	}{digest, config, routerConfig, plan, overlapConfig, packing}
	raw, err := json.Marshal(build)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(overlap.Capacity), Budget: uint64(overlap.Budget), Realized: uint64(overlap.Used), Unspent: uint64(overlap.Unspent), BuildIdentityDigest: fmt.Sprintf("%x", sha256.Sum256(raw))})
	if err != nil {
		t.Fatal(err)
	}
	m := collections.VectorPartitionManifestV1{State: "building", Collection: f.manifest.Collection, IndexName: f.definition.Name, IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(f.definition), SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount, Generation: f.manifest.Generation + 1, PartitionCount: uint32(plan.Partitions), DomainCount: uint32(plan.LogicalDomains), BalancePolicy: policy}
	if m.Generation <= f.manifest.Generation {
		t.Fatal("generation overflow")
	}
	parts := make([]vectorpartition.RouterPartitionV1, plan.Partitions)
	inputs := make([]collections.VectorPartitionSearchAssetV1, plan.Partitions)
	for p := range parts {
		m.DomainPacks = append(m.DomainPacks, collections.VectorPartitionDomainPackV1{DomainID: uint32(p / plan.PacksPerDomain), PackID: uint32(p)})
		m.Placements = append(m.Placements, collections.VectorPartitionPlacementV1{PartitionID: uint32(p), GroupID: fmt.Sprintf("lifecycle-group-%d", p%4)})
		parts[p].PartitionID = uint32(p)
		inputs[p] = collections.VectorPartitionSearchAssetV1{Source: source, Generation: m.Generation, PartitionID: uint32(p), Dimensions: f.definition.Dimensions}
	}
	if real {
		m.Placements = slices.Clone(f.manifest.Placements)
	}
	// Artifact IDs are sorted; source ordinals may not be. Resolve every
	// membership through its stable ID and share immutable source FP32 slices.
	for _, member := range overlap.Memberships {
		row, found := byID[artifact.IDs[member.VectorOrdinal]]
		if !found {
			t.Fatal("artifact ID absent from current source")
		}
		membership := collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: uint32(member.Partition)}
		kind := collections.VectorPartitionMembershipOverlapV1
		if member.Home {
			m.Memberships = append(m.Memberships, membership)
			kind = collections.VectorPartitionMembershipHomeV1
		} else {
			m.OverlapMemberships = append(m.OverlapMemberships, membership)
		}
		parts[member.Partition].Vectors = append(parts[member.Partition].Vectors, vectorpartition.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: row.Values, MembershipKind: string(kind)})
	}
	m.Canonicalize()
	// Test-reserved IDs, fail on any collision instead of appending into an
	// earlier generation's segment. Production allocator remains unchanged.
	for _, id := range []uint32{61001, 61002} {
		if err := filepath.WalkDir(f.database.ColumnAssetRootDir(), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Name() == fmt.Sprintf("segment-%06d.tca", id) {
				return fmt.Errorf("asset ID collision: %s", path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	assets, resources, err := f.collection.MaterializeVectorPartitionLocalSearchAssetsV1(f.definition.Name, m, 61001, inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if err := validateLiveLifecyclePackBytesV1(assets, accounts); err != nil {
		t.Fatal(err)
	}
	m.Assets = assets
	m.Canonicalize()
	if err := f.collection.PublishVectorPartitionManifestV1(m, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.collection.BuildAndPublishVectorPartitionRouterV1(t.Context(), m, parts, collections.VectorPartitionRouterBuildOptionsV1{Config: routerConfig, AssetFileID: 61002, AssetPartID: 1, M: 16, EfConstruction: 128, EfSearch: 128}); err != nil {
		t.Fatal(err)
	}
	ready, err := f.collection.PreparedVectorPartitionManifestWithContextV1(t.Context(), m.IndexName, m.Generation)
	if err != nil {
		t.Fatal(err)
	}
	retainLiveLifecycleJSONV1(t, "administrative-build", struct {
		Build    any
		Artifact vectorpartition.Artifact
		Accounts []vectorpartition.ShardPackSummaryV1
		Manifest collections.VectorPartitionManifestV1
	}{build, artifact, accounts, ready})
	return ready
}

func liveLifecycleAssetPathsV1(t *testing.T, root string, manifest collections.VectorPartitionManifestV1) []string {
	t.Helper()
	names := map[string]bool{}
	for _, asset := range append(slices.Clone(manifest.Assets), manifest.RouterAsset) {
		names[fmt.Sprintf("segment-%06d.tca", asset.Ref.FileID)] = true
	}
	var paths []string
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && names[d.Name()] {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(paths) != len(names) {
		t.Fatal("old asset paths missing or ambiguous")
	}
	return paths
}

// The unchanged top-k plus every touched ID is exact for every scheduled
// revision, including deletes: an unchanged row below kth cannot enter top-k.
// Retain O(queries*k + source vectors), not O(queries*source rows*revisions).
func liveLifecycleUnchangedTruthV1(t *testing.T, base map[string][]float32, queries [][]float32, touched []string, k int) []liveLifecycleTruthV1 {
	t.Helper()
	truth := make([]liveLifecycleTruthV1, len(queries))
	for q, query := range queries {
		scorer, err := collections.NewCanonicalVectorPartitionCosineScorerV1(query)
		if err != nil {
			t.Fatal(err)
		}
		top := make([]VectorPartitionCoordinatorNeighborV1, 0, k+1)
		for id, vector := range base {
			if slices.Contains(touched, id) {
				continue
			}
			score, err := scorer.ScoreV1(vector)
			if err != nil {
				t.Fatal(err)
			}
			n := VectorPartitionCoordinatorNeighborV1{ID: id, Score: score}
			if len(top) == k && liveLifecycleNeighborCompareV1(n, top[k-1]) >= 0 {
				continue
			}
			i, _ := slices.BinarySearchFunc(top, n, liveLifecycleNeighborCompareV1)
			top = slices.Insert(top, i, n)
			if len(top) > k {
				top = top[:k]
			}
		}
		if len(top) != k {
			t.Fatal("unchanged population smaller than top-k")
		}
		cell := liveLifecycleTruthV1{base: base, scorer: scorer, scores: make(map[string]float32, k), top: make(map[string]bool, k)}
		for _, n := range top {
			cell.scores[n.ID], cell.top[n.ID] = n.Score, true
		}
		truth[q] = cell
	}
	return truth
}

func liveLifecycleNeighborCompareV1(a, b VectorPartitionCoordinatorNeighborV1) int {
	if a.Score != b.Score {
		return cmp.Compare(b.Score, a.Score)
	}
	return cmp.Compare(a.ID, b.ID)
}

func liveLifecycleDeltaTruthV1(t *testing.T, unchanged []liveLifecycleTruthV1, delta map[string][]float32) []liveLifecycleTruthV1 {
	t.Helper()
	delta = maps.Clone(delta) // Caller replaces vectors; never edits their backing arrays.
	truth := make([]liveLifecycleTruthV1, len(unchanged))
	for q, cell := range unchanged {
		top := make([]VectorPartitionCoordinatorNeighborV1, 0, len(cell.top)+len(delta))
		for id, score := range cell.scores {
			top = append(top, VectorPartitionCoordinatorNeighborV1{ID: id, Score: score})
		}
		for id, vector := range delta {
			if _, excluded := cell.scores[id]; excluded {
				t.Fatal("touched ID was not excluded from unchanged truth")
			}
			if vector == nil {
				continue
			}
			score, err := cell.scorer.ScoreV1(vector)
			if err != nil {
				t.Fatal(err)
			}
			top = append(top, VectorPartitionCoordinatorNeighborV1{ID: id, Score: score})
		}
		slices.SortFunc(top, liveLifecycleNeighborCompareV1)
		out := liveLifecycleTruthV1{base: cell.base, delta: delta, scorer: cell.scorer, scores: make(map[string]float32, len(cell.top)), top: make(map[string]bool, len(cell.top))}
		for _, n := range top[:len(cell.top)] {
			out.scores[n.ID], out.top[n.ID] = n.Score, true
		}
		truth[q] = out
	}
	return truth
}

func TestVectorPartitionLiveBoundedTruthV1(t *testing.T) {
	base := map[string][]float32{"z": {1, 0}, "b": {1, 1}, "a": {1, 1}, "c": {0, 1}, "d": {-1, 0}}
	queries := [][]float32{{1, 0}, {0, 1}}
	unchanged := liveLifecycleUnchangedTruthV1(t, base, queries, []string{"z", "b", "new"}, 2)
	for _, delta := range []map[string][]float32{
		{"z": base["z"], "b": base["b"]},
		{"z": nil, "b": {0, -1}, "new": {0, 1}},
		{"z": {0, 1}, "b": nil, "new": {1, 0}},
	} {
		current := maps.Clone(base)
		for id, v := range delta {
			if v == nil {
				delete(current, id)
			} else {
				current[id] = v
			}
		}
		want := liveLifecycleTruthForV1(t, current, queries, 2)
		got := liveLifecycleDeltaTruthV1(t, unchanged, delta)
		for q := range want {
			if !maps.Equal(got[q].top, want[q].top) {
				t.Fatalf("q=%d top got=%v want=%v", q, got[q].top, want[q].top)
			}
			for id, score := range got[q].scores {
				if score != want[q].scores[id] {
					t.Fatalf("q=%d id=%s score mismatch", q, id)
				}
			}
		}
	}
}
