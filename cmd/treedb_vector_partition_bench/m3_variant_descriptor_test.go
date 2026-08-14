package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func testM3VariantDescriptorV1(dir string) m3VariantDescriptorV1 {
	hash := strings.Repeat("a", 64)
	partitionConfig := vectorpartition.DefaultConfig()
	partitionConfig.Partitions = 4
	partitionConfig.MaxDistanceWork = 20_000_000_000
	routerConfig := vectorpartition.DefaultRouterConfigV1()
	d := m3VariantDescriptorV1{
		SchemaVersion: 6, ResultKind: "m3_persistent_variant_descriptor_v6", VariantID: "graph-overlap-020-v1",
		AssignmentBasis: partitionAssignmentGraphV1, OverlapRatio: .2,
		BaseSHA: strings.Repeat("b", 40), HeadSHA: strings.Repeat("c", 40),
		FixtureChecksum: hash, ExecutableSHA256: hash, ArtifactSHA256: hash, GraphArtifactSHA256: hash, GraphBuildSHA256: hash, ArtifactBackend: "reference", Source: vectorpartition.Source{SourceID: "fixture", Checksum: hash, Vectors: 8, Dimensions: 2, Metric: "cosine"},
		DatabaseDirectory: dir, ManifestIntegrity: hash, ReadySetDigest: hash, RouterAssetChecksum: hash, RouterModelDigest: hash,
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRows: 8, SourceOrdinalDigest: hash, PartitionGeneration: 4, RouterGeneration: 4,
		Partitions: 4, IndexDefinitionDigest: hash, PartitionHNSWM: 16, PartitionConfig: partitionConfig, PartitionMaxDistanceWork: 20_000_000_000, RouterMaxScalarWork: 20_000_000_000, RouterConfig: routerConfig, M3MaxBenchmarkVisits: 400_000_000, RouterRepresentatives: 4, Capacity: 3, OverlapRequested: 1, OverlapRealized: 1, OverlapRejected: 0, OverlapUseful: 1, OverlapUnusedCapacity: 3, EdgeCutBefore: 2, EdgeCutAfter: 1, PartitionLoads: []int{3, 2, 2, 2}, OverlapMemberships: 1, PersistentAssetBytes: 1024,
	}
	d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
	d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: 3, Budget: 1, Realized: 1, BuildIdentityDigest: d.BuildIdentityDigest})
	return d
}

func TestM3VariantDescriptorRoundTripAndImmutableCreateV1(t *testing.T) {
	dir := t.TempDir()
	want := testM3VariantDescriptorV1(dir)
	if err := m3WriteVariantDescriptorV1(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := m3ReadVariantDescriptorV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.VariantID != want.VariantID || got.BaseSHA != want.BaseSHA || got.HeadSHA != want.HeadSHA || got.BuildDirty != want.BuildDirty || got.ExecutableSHA256 != want.ExecutableSHA256 || got.ReadySetDigest != want.ReadySetDigest || got.GraphBuildSHA256 != want.GraphBuildSHA256 || got.PartitionConfig != want.PartitionConfig || got.PartitionMaxDistanceWork != want.PartitionMaxDistanceWork || got.RouterMaxScalarWork != want.RouterMaxScalarWork || got.RouterConfig != want.RouterConfig || got.M3MaxBenchmarkVisits != want.M3MaxBenchmarkVisits || got.RouterRepresentatives != want.RouterRepresentatives || len(got.PartitionLoads) != 4 {
		t.Fatalf("descriptor=%+v", got)
	}
	if err := m3WriteVariantDescriptorV1(dir, want); err == nil {
		t.Fatal("overwrote immutable variant descriptor")
	}
	if err := os.WriteFile(filepath.Join(dir, m3VariantDescriptorFileV1), []byte("{} {}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m3ReadVariantDescriptorV1(dir); err == nil {
		t.Fatal("accepted trailing or malformed descriptor JSON")
	}
}

func TestM3VariantDescriptorRejectsMissingSourceOrdinalDigestV1(t *testing.T) {
	descriptor := testM3VariantDescriptorV1(t.TempDir())
	descriptor.SourceOrdinalDigest = ""
	if err := validateM3VariantDescriptorV1(descriptor); err == nil {
		t.Fatal("accepted missing source ordinal digest")
	}
}

func TestM3VariantDescriptorReadRejectsOversizedFileBeforeDecodeV1(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, m3VariantDescriptorFileV1)
	if err := os.WriteFile(path, []byte{'{'}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, m3VariantDescriptorMaxBytesV1+1); err != nil {
		t.Fatal(err)
	}
	if _, err := m3ReadVariantDescriptorV1(dir); err == nil || !strings.Contains(err.Error(), "invalid byte length") {
		t.Fatalf("oversized descriptor error=%v", err)
	}
}

func TestM3GraphBuildDigestExcludesAssignmentV1(t *testing.T) {
	hash := strings.Repeat("a", 64)
	artifact := vectorpartition.Artifact{
		Source: vectorpartition.Source{SourceID: "fixture", Checksum: hash, Vectors: 2, Dimensions: 1, Metric: "cosine"},
		Config: vectorpartition.Config{Metric: "cosine", Seed: 1, Partitions: 2},
		IDs:    []string{"a", "b"}, Graph: vectorpartition.Graph{Neighbors: [][]int{{1}, {0}}},
		Backend: "graph", Assignment: []int{0, 1}, Metrics: vectorpartition.Metrics{EdgeCut: 1},
	}
	want, err := m3GraphBuildSHA256V1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	stable := artifact
	stable.Backend, stable.Assignment, stable.Metrics = "stable_id_hash_baseline_v1", []int{1, 0}, vectorpartition.Metrics{}
	if got, err := m3GraphBuildSHA256V1(stable); err != nil || got != want {
		t.Fatalf("stable graph build digest=%q err=%v want %q", got, err, want)
	}
	stable.Graph = vectorpartition.Graph{Neighbors: [][]int{{}, {}}}
	if got, err := m3GraphBuildSHA256V1(stable); err != nil || got == want {
		t.Fatalf("changed graph digest=%q err=%v want distinct from %q", got, err, want)
	}
}

func TestM3VariantDescriptorWriteAllowsPostBuildRepresentativeCountV1(t *testing.T) {
	dir := t.TempDir()
	d := testM3VariantDescriptorV1(dir)
	// Representatives are produced after the prebuild graph identity is fixed.
	d.RouterRepresentatives = 7
	if err := m3WriteVariantDescriptorV1(dir, d); err != nil {
		t.Fatal(err)
	}
}

func TestM3VariantDescriptorRejectsMalformedOverlapPolicyAccountingV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	for name, policy := range map[string]string{
		"realized exceeds budget": strings.Replace(d.OverlapPolicy, "realized=1", "realized=2", 1),
		"unspent mismatch":        strings.Replace(d.OverlapPolicy, "unspent=0", "unspent=1", 1),
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			candidate.OverlapPolicy = policy
			if err := validateM3VariantDescriptorV1(candidate); err == nil {
				t.Fatalf("accepted malformed policy %q", policy)
			}
		})
	}
}

func TestM3VariantDescriptorAllowsUsefulOnlyShortfallV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	d.OverlapRealized = 0
	d.OverlapRejected = d.OverlapRequested
	d.OverlapUseful = 0
	d.OverlapFiller = 0
	d.OverlapMemberships = 0
	d.OverlapUnusedCapacity = d.Capacity*int(d.Partitions) - int(d.SourceRows)
	d.PartitionLoads = []int{2, 2, 2, 2}
	d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
	d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(d.Capacity), Budget: uint64(d.OverlapRequested), Realized: 0, Unspent: uint64(d.OverlapRejected), BuildIdentityDigest: d.BuildIdentityDigest})
	if err := validateM3VariantDescriptorV1(d); err != nil {
		t.Fatalf("useful-only shortfall rejected: %v", err)
	}
}

func TestM3VariantDescriptorRejectsFillerReplicasV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	d.OverlapUseful = 0
	d.OverlapFiller = d.OverlapRealized
	d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
	d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(d.Capacity), Budget: uint64(d.OverlapRequested), Realized: uint64(d.OverlapRealized), BuildIdentityDigest: d.BuildIdentityDigest})
	if err := validateM3VariantDescriptorV1(d); err == nil {
		t.Fatal("accepted filler replicas in useful-only descriptor")
	}
}

func TestM3VariantDescriptorRejectsMalformedPartitionLoadsV1(t *testing.T) {
	base := testM3VariantDescriptorV1(t.TempDir())
	for name, mutate := range map[string]func([]int){
		"negative":       func(loads []int) { loads[0] = -1 },
		"over capacity":  func(loads []int) { loads[0] = base.Capacity + 1 },
		"total mismatch": func(loads []int) { loads[0]-- },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.PartitionLoads = append([]int(nil), base.PartitionLoads...)
			mutate(candidate.PartitionLoads)
			if err := validateM3VariantDescriptorV1(candidate); err == nil {
				t.Fatalf("accepted malformed loads %v", candidate.PartitionLoads)
			}
		})
	}
}

func TestM3VariantBuildIdentityBindsOverlapInputsAndOutcomesV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	baseline, err := m3VariantBuildIdentityDigestV1(d)
	if err != nil {
		t.Fatal(err)
	}
	targetChanged := d
	targetChanged.OverlapRequested++
	targetDigest, err := m3VariantBuildIdentityDigestV1(targetChanged)
	if err != nil || targetDigest == baseline {
		t.Fatalf("target identity baseline=%s changed=%s err=%v", baseline, targetDigest, err)
	}
	capacityChanged := d
	capacityChanged.Capacity++
	capacityDigest, err := m3VariantBuildIdentityDigestV1(capacityChanged)
	if err != nil || capacityDigest == baseline {
		t.Fatalf("capacity identity baseline=%s changed=%s err=%v", baseline, capacityDigest, err)
	}
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"base revision":        func(candidate *m3VariantDescriptorV1) { candidate.BaseSHA = strings.Repeat("d", 40) },
		"head revision":        func(candidate *m3VariantDescriptorV1) { candidate.HeadSHA = strings.Repeat("e", 40) },
		"dirty build":          func(candidate *m3VariantDescriptorV1) { candidate.BuildDirty = true },
		"benchmark executable": func(candidate *m3VariantDescriptorV1) { candidate.ExecutableSHA256 = strings.Repeat("d", 64) },
		"KaHIP python":         func(candidate *m3VariantDescriptorV1) { candidate.KaHIPPythonSHA256 = strings.Repeat("d", 64) },
		"KaHIP adapter":        func(candidate *m3VariantDescriptorV1) { candidate.KaHIPAdapterSHA256 = strings.Repeat("e", 64) },
		"graph build":          func(candidate *m3VariantDescriptorV1) { candidate.GraphBuildSHA256 = strings.Repeat("d", 64) },
		"partition config":     func(candidate *m3VariantDescriptorV1) { candidate.PartitionConfig.Pivots++ },
		"partition work cap":   func(candidate *m3VariantDescriptorV1) { candidate.PartitionMaxDistanceWork++ },
		"router work cap":      func(candidate *m3VariantDescriptorV1) { candidate.RouterMaxScalarWork++ },
		"M3 visit cap":         func(candidate *m3VariantDescriptorV1) { candidate.M3MaxBenchmarkVisits++ },
		"useful":               func(candidate *m3VariantDescriptorV1) { candidate.OverlapUseful-- },
		"filler":               func(candidate *m3VariantDescriptorV1) { candidate.OverlapFiller++ },
		"cut before":           func(candidate *m3VariantDescriptorV1) { candidate.EdgeCutBefore++ },
		"cut after":            func(candidate *m3VariantDescriptorV1) { candidate.EdgeCutAfter++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			mutate(&candidate)
			changed, err := m3VariantBuildIdentityDigestV1(candidate)
			if err != nil || changed == baseline {
				t.Fatalf("outcome identity baseline=%s changed=%s err=%v", baseline, changed, err)
			}
		})
	}
}

// testM3ByteBoundedDescriptorV1 rebuilds the shared fixture at a shape the
// byte-bounded planner actually produces, so the persisted plan describes the
// packs the descriptor claims.
func testM3ByteBoundedDescriptorV1(t *testing.T, dir string) m3VariantDescriptorV1 {
	t.Helper()
	plan, err := vectorpartition.PlanByteBoundedShardsV1(vectorpartition.ShardPlanInputV1{
		Vectors: 8, Dimensions: 2, OverlapRatio: .2, Imbalance: vectorpartition.DefaultConfig().Imbalance,
		TargetHotBytes: uint64(3 * (alignedRowBytesForTest(2) + vectorpartition.GraphIdentityOverheadPerRowV1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Partitions != 3 || plan.OverlapCapacity != 3 {
		t.Fatalf("unexpected fixture plan=%+v", plan)
	}
	d := testM3VariantDescriptorV1(dir)
	d.ShardPlan = plan
	d.Partitions = uint32(plan.Partitions)
	d.PartitionConfig.Partitions = plan.Partitions
	d.Capacity = plan.OverlapCapacity
	d.PartitionLoads = []int{3, 3, 3}
	d.ShardGenerationDigest = strings.Repeat("d", 64)
	d.OverlapUnusedCapacity = plan.OverlapCapacity*plan.Partitions - int(d.SourceRows) - d.OverlapRealized
	refreshTestM3DescriptorIdentityV1(t, &d)
	if err := validateM3VariantDescriptorV1(d); err != nil {
		t.Fatalf("byte-bounded fixture rejected: %v", err)
	}
	return d
}

func refreshTestM3DescriptorIdentityV1(t *testing.T, d *m3VariantDescriptorV1) {
	t.Helper()
	var err error
	if d.BuildIdentityDigest, err = m3VariantBuildIdentityDigestV1(*d); err != nil {
		t.Fatal(err)
	}
	if d.OverlapPolicy, err = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
		Capacity: uint64(d.Capacity), Budget: uint64(d.OverlapRequested), Realized: uint64(d.OverlapRealized),
		Unspent: uint64(d.OverlapRejected), BuildIdentityDigest: d.BuildIdentityDigest,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestM3VariantBuildIdentityBindsShardPlanV1(t *testing.T) {
	d := testM3ByteBoundedDescriptorV1(t, t.TempDir())
	baseline, err := m3VariantBuildIdentityDigestV1(d)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"cleared plan":   func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan = vectorpartition.ShardPlanV1{} },
		"hot bytes":      func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.TargetHotBytes++ },
		"partitions":     func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.Partitions++ },
		"pack capacity":  func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.MaxMembershipsPerPack++ },
		"home capacity":  func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.HomeCapacity++ },
		"planned rows":   func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.PlannedMemberships++ },
		"planned ratio":  func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.OverlapRatio += .1 },
		"row byte width": func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.TraversalRowBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			mutate(&candidate)
			changed, err := m3VariantBuildIdentityDigestV1(candidate)
			if err != nil || changed == baseline {
				t.Fatalf("shard-plan identity baseline=%s changed=%s err=%v", baseline, changed, err)
			}
		})
	}
}

// TestM3VariantDescriptorValidatesPersistedShardPlanV1 rejects a stale, zeroed,
// or edited plan on reopen even after its build-identity digest is recomputed,
// so the recorded byte bound cannot drift away from the packs it describes.
func TestM3VariantDescriptorValidatesPersistedShardPlanV1(t *testing.T) {
	d := testM3ByteBoundedDescriptorV1(t, t.TempDir())
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"stale hot bytes":      func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.TargetHotBytes *= 2 },
		"partition mismatch":   func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.Partitions++ },
		"capacity mismatch":    func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.OverlapCapacity++ },
		"row mismatch":         func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.Vectors++ },
		"dimension mismatch":   func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.Dimensions++ },
		"imbalance mismatch":   func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.Imbalance += .01 },
		"row byte mismatch":    func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.TraversalRowBytes++ },
		"unprovisioned ratio":  func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.OverlapRatio = .1 },
		"pack budget mismatch": func(candidate *m3VariantDescriptorV1) { candidate.ShardPlan.MaxMembershipsPerPack = 2 },
		"partial plan": func(candidate *m3VariantDescriptorV1) {
			candidate.ShardPlan = vectorpartition.ShardPlanV1{Partitions: candidate.ShardPlan.Partitions}
		},
		"missing generation digest": func(candidate *m3VariantDescriptorV1) { candidate.ShardGenerationDigest = "" },
		"malformed generation digest": func(candidate *m3VariantDescriptorV1) {
			candidate.ShardGenerationDigest = "not-a-sha"
		},
		"generation digest without a plan": func(candidate *m3VariantDescriptorV1) {
			candidate.ShardPlan = vectorpartition.ShardPlanV1{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			mutate(&candidate)
			refreshTestM3DescriptorIdentityV1(t, &candidate)
			if err := validateM3VariantDescriptorV1(candidate); err == nil {
				t.Fatalf("accepted %s: %+v", name, candidate.ShardPlan)
			}
		})
	}
	// A build that never opted into the planner records no plan at all, and an
	// absent plan stays absent rather than being partially believed.
	unplanned := d
	unplanned.ShardPlan = vectorpartition.ShardPlanV1{}
	unplanned.ShardGenerationDigest = ""
	refreshTestM3DescriptorIdentityV1(t, &unplanned)
	if err := validateM3VariantDescriptorV1(unplanned); err != nil {
		t.Fatalf("unplanned descriptor rejected: %v", err)
	}
}

func TestM3VariantDescriptorBindsKaHIPExecutionIdentityV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	d.ArtifactBackend = "kahip_python_3.25_eco_symmetrized_v1_seed_1"
	d.PartitionConfig.Seed = 1
	d.KaHIPPythonSHA256 = strings.Repeat("a", 64)
	d.KaHIPAdapterSHA256 = strings.Repeat("b", 64)
	d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
	d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(d.Capacity), Budget: uint64(d.OverlapRequested), Realized: uint64(d.OverlapRealized), BuildIdentityDigest: d.BuildIdentityDigest})
	if err := validateM3VariantDescriptorV1(d); err != nil {
		t.Fatalf("valid KaHIP identity: %v", err)
	}
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"missing python":    func(d *m3VariantDescriptorV1) { d.KaHIPPythonSHA256 = "" },
		"malformed adapter": func(d *m3VariantDescriptorV1) { d.KaHIPAdapterSHA256 = "bad" },
		"stable identity": func(d *m3VariantDescriptorV1) {
			d.ArtifactBackend = "stable_id_hash_baseline_v1"
			d.AssignmentBasis = partitionAssignmentStableIDHashV1
			d.VariantID = "stable-id-hash-disjoint-v1"
			d.OverlapRatio = 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			mutate(&candidate)
			candidate.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(candidate)
			candidate.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(candidate.Capacity), Budget: 0, Realized: 0, BuildIdentityDigest: candidate.BuildIdentityDigest})
			if err := validateM3VariantDescriptorV1(candidate); err == nil {
				t.Fatal("accepted invalid KaHIP execution identity")
			}
		})
	}
}

func TestM3VariantDescriptorRejectsMissingBuildWorkCapsV1(t *testing.T) {
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"partition": func(d *m3VariantDescriptorV1) { d.PartitionMaxDistanceWork = 0 },
		"router":    func(d *m3VariantDescriptorV1) { d.RouterMaxScalarWork = 0 },
		"M3 visits": func(d *m3VariantDescriptorV1) { d.M3MaxBenchmarkVisits = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			d := testM3VariantDescriptorV1(t.TempDir())
			mutate(&d)
			var err error
			d.BuildIdentityDigest, err = m3VariantBuildIdentityDigestV1(d)
			if err != nil {
				t.Fatal(err)
			}
			d.OverlapPolicy, err = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
				Capacity: uint64(d.Capacity), Budget: uint64(d.OverlapRequested), Realized: uint64(d.OverlapRealized),
				Unspent: uint64(d.OverlapRejected), BuildIdentityDigest: d.BuildIdentityDigest,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := validateM3VariantDescriptorV1(d); err == nil {
				t.Fatal("accepted missing build work cap")
			}
		})
	}
}

func TestM3VariantDescriptorRequiresRevisionsAndRouterConfigV1(t *testing.T) {
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"missing base":            func(d *m3VariantDescriptorV1) { d.BaseSHA = "" },
		"malformed head":          func(d *m3VariantDescriptorV1) { d.HeadSHA = "not-a-sha" },
		"missing executable":      func(d *m3VariantDescriptorV1) { d.ExecutableSHA256 = "" },
		"malformed executable":    func(d *m3VariantDescriptorV1) { d.ExecutableSHA256 = "not-a-sha" },
		"partition work mismatch": func(d *m3VariantDescriptorV1) { d.PartitionConfig.MaxDistanceWork++ },
		"router scalar mismatch":  func(d *m3VariantDescriptorV1) { d.RouterConfig.MaxScalarWork++ },
	} {
		t.Run(name, func(t *testing.T) {
			d := testM3VariantDescriptorV1(t.TempDir())
			mutate(&d)
			d.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(d)
			d.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(d.Capacity), Budget: uint64(d.OverlapRequested), Realized: uint64(d.OverlapRealized), BuildIdentityDigest: d.BuildIdentityDigest})
			if err := validateM3VariantDescriptorV1(d); err == nil {
				t.Fatal("accepted malformed revision or router config")
			}
		})
	}
}

func TestM3OverlapCapacityUsesExactGlobalTargetV1(t *testing.T) {
	artifact := vectorpartition.Artifact{IDs: make([]string, 1_000_000), Config: vectorpartition.Config{Partitions: 16}, Metrics: vectorpartition.Metrics{Cap: 65_625}}
	capacity, err := m3OverlapCapacityV1(artifact, .2)
	if err != nil {
		t.Fatal(err)
	}
	if capacity != 75_000 {
		t.Fatalf("capacity=%d want 75000", capacity)
	}
	if capacity, err = m3OverlapCapacityV1(artifact, 0); err != nil || capacity != 65_625 {
		t.Fatalf("disjoint capacity=%d err=%v", capacity, err)
	}
}

func TestM3OverlapCapacityAvoidsCeilAdditionOverflowV1(t *testing.T) {
	capacity, err := m3OverlapCapacityForRequestedV1(math.MaxInt-1, 1, 2, 0)
	if err != nil || capacity != math.MaxInt/2+1 {
		t.Fatalf("capacity=%d err=%v", capacity, err)
	}
}

func TestM3VariantDescriptorBindsReadyManifestV1(t *testing.T) {
	d := testM3VariantDescriptorV1(t.TempDir())
	manifest := collections.VectorPartitionManifestV1{
		IntegrityDigest: d.ManifestIntegrity, ReadySetDigest: d.ReadySetDigest,
		IndexDefinitionDigest: d.IndexDefinitionDigest,
		RouterAsset:           collections.VectorPartitionAssetV1{Checksum: d.RouterAssetChecksum},
		SourceGeneration:      d.SourceGeneration, SourceChecksum: d.SourceChecksum, SourceSchemaHash: d.SourceSchemaHash, SourceRowCount: d.SourceRows,
		Generation: d.PartitionGeneration, RouterGeneration: d.RouterGeneration, PartitionCount: d.Partitions, BalancePolicy: d.OverlapPolicy,
		Memberships:        []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 0}, {VectorOrdinal: 2, PartitionID: 1}, {VectorOrdinal: 3, PartitionID: 1}, {VectorOrdinal: 4, PartitionID: 2}, {VectorOrdinal: 5, PartitionID: 2}, {VectorOrdinal: 6, PartitionID: 3}, {VectorOrdinal: 7, PartitionID: 3}},
		Representatives:    []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 2, PartitionID: 1}, {VectorOrdinal: 4, PartitionID: 2}, {VectorOrdinal: 6, PartitionID: 3}},
		OverlapMemberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 7, PartitionID: 0}},
		Assets:             []collections.VectorPartitionAssetV1{{Bytes: 200}, {Bytes: 200}, {Bytes: 200}, {Bytes: 200}},
	}
	manifest.RouterAsset.Bytes = 224
	fixture := fixtureManifest{Checksum: d.FixtureChecksum}
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest, d.RouterConfig); err != nil {
		t.Fatal(err)
	}
	manifest.ReadySetDigest = strings.Repeat("b", 64)
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest, d.RouterConfig); err == nil {
		t.Fatal("accepted descriptor for a different ready manifest")
	}
	manifest.ReadySetDigest = d.ReadySetDigest
	runtimeConfig := d.RouterConfig
	runtimeConfig.LeafSize++
	if err := m3DescriptorMatchesManifestV1(d, fixture, manifest, d.RouterModelDigest, runtimeConfig); err == nil {
		t.Fatal("accepted descriptor for a router reopened with a different build config")
	}
	for name, mutate := range map[string]func(*m3VariantDescriptorV1){
		"assignment relabel": func(candidate *m3VariantDescriptorV1) {
			candidate.AssignmentBasis = partitionAssignmentStableIDHashV1
			candidate.OverlapRatio = 0
			candidate.VariantID = "stable-id-hash-disjoint-v1"
		},
		"artifact":         func(candidate *m3VariantDescriptorV1) { candidate.ArtifactSHA256 = strings.Repeat("b", 64) },
		"index definition": func(candidate *m3VariantDescriptorV1) { candidate.IndexDefinitionDigest = strings.Repeat("b", 64) },
		"capacity":         func(candidate *m3VariantDescriptorV1) { candidate.Capacity++ },
		"loads":            func(candidate *m3VariantDescriptorV1) { candidate.PartitionLoads[0]-- },
		"asset bytes":      func(candidate *m3VariantDescriptorV1) { candidate.PersistentAssetBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := d
			candidate.PartitionLoads = append([]int(nil), d.PartitionLoads...)
			mutate(&candidate)
			if err := m3DescriptorMatchesManifestV1(candidate, fixture, manifest, candidate.RouterModelDigest, candidate.RouterConfig); err == nil {
				t.Fatal("accepted descriptor mutation not covered by retained state")
			}
		})
	}
	relocated := d
	relocated.DatabaseDirectory = filepath.Join(t.TempDir(), "relocated")
	if err := m3DescriptorMatchesManifestV1(relocated, fixture, manifest, relocated.RouterModelDigest, relocated.RouterConfig); err != nil {
		t.Fatalf("portable descriptor rejected after directory relocation: %v", err)
	}
}

func TestM3VariantIdentityV1(t *testing.T) {
	for _, tc := range []struct {
		assignment string
		ratio      float64
		want       string
	}{
		{partitionAssignmentGraphV1, 0, "graph-disjoint-v1"},
		{partitionAssignmentGraphV1, .2, "graph-overlap-020-v1"},
		{partitionAssignmentStableIDHashV1, 0, "stable-id-hash-disjoint-v1"},
	} {
		got, err := m3VariantIDV1(tc.assignment, tc.ratio)
		if err != nil || got != tc.want {
			t.Fatalf("variant (%q,%v)=%q err=%v want %q", tc.assignment, tc.ratio, got, err, tc.want)
		}
	}
	if _, err := m3VariantIDV1(partitionAssignmentStableIDHashV1, .2); err == nil {
		t.Fatal("accepted overlapping stable-ID hash baseline")
	}
}
