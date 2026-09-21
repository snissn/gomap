package vectorpartition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Two logical domains interleave in source order; each contains two disconnected
// two-vertex communities. Ordinal striping splits every affinity edge despite
// an exactly balanced, zero-cut physical layout being available.
func TestDomainHomePackingKeepsCommunitiesV1(t *testing.T) {
	plan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{
		Vectors: 8, Dimensions: 2, LogicalDomains: 2,
		TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 2*(alignedRowBytesForTest(2)+GraphIdentityOverheadPerRowV1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	logical := OverlapResult{Capacity: 4, Loads: []int{4, 4}}
	for ordinal := range 8 {
		logical.Memberships = append(logical.Memberships, Membership{VectorOrdinal: ordinal, Partition: ordinal % 2, Home: true})
	}
	// The solver's home map must survive packing rather than being replaced
	// by ordinal stripes. The pinned adapter is exercised separately.
	packed, err := PackDomainMembershipsWithHomesV1(plan, logical, []int{0, 2, 0, 2, 1, 3, 1, 3})
	if err != nil {
		t.Fatal(err)
	}
	homes := make([]int, 8)
	for _, member := range packed.Memberships {
		homes[member.VectorOrdinal] = member.Partition
	}
	cut := 0
	for _, edge := range [][2]int{{0, 2}, {1, 3}, {4, 6}, {5, 7}} {
		if homes[edge[0]] != homes[edge[1]] {
			cut++
		}
	}
	if cut != 0 {
		t.Fatalf("physical packing cuts %d of 4 within-domain community edges: homes=%v", cut, homes)
	}
}

func homePackingFixtureV1(t *testing.T) (Artifact, ShardPlanV1, []int) {
	t.Helper()
	vectors := append(fixture(), Vector{"f", []float64{1, 1}}, Vector{"g", []float64{1, .5}})
	a, err := Build(vectors, config())
	if err != nil {
		t.Fatal(err)
	}
	a.Assignment = []int{0, 1, 0, 1, 0, 1, 0, 1}
	a.Graph.Neighbors = [][]int{{1, 2}, {0, 3}, {0}, {1}, {6}, {7}, {4}, {5}}
	a.Metrics = metrics(a)
	if err := ValidateArtifact(a); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{
		Vectors: 8, Dimensions: 2, LogicalDomains: 2, OverlapRatio: .25, Imbalance: .05,
		TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 3*(alignedRowBytesForTest(2)+GraphIdentityOverheadPerRowV1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return a, plan, []int{0, 2, 0, 2, 1, 3, 1, 3}
}

func TestHomePackingBoundResponseAndRetainedReuseV1(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process fixture")
	}
	a, plan, homes := homePackingFixtureV1(t)
	input, _, err := homePackingRequestJSONV1(plan, a)
	if err != nil {
		t.Fatal(err)
	}
	response := homePackingResponseV1{fmt.Sprintf("%x", sha256.Sum256(input)), HomePackingPolicyV1, homes}
	command := []string{"sh", "-c", `printf '%s' "$TREE_DB_TEST_RESPONSE" > "$1"`}
	run := func(r homePackingResponseV1, suffix string, limits ExternalJSONLimits) ([]int, HomePackingReceiptV1, error) {
		t.Helper()
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("TREE_DB_TEST_RESPONSE", string(raw)+suffix)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		return RunExternalHomePackingV1(ctx, command, limits, plan, a)
	}
	limits := ExternalJSONLimits{MaxInput: len(input), MaxOutput: 2048}
	got, receipt, err := run(response, "", limits)
	if err != nil || !reflect.DeepEqual(got, homes) {
		t.Fatalf("homes=%v err=%v", got, err)
	}
	for name, mutate := range map[string]func(*homePackingResponseV1){
		"unbound request": func(r *homePackingResponseV1) { r.RequestSHA256 = strings.Repeat("a", 64) },
		"wrong policy":    func(r *homePackingResponseV1) { r.Policy = "ordinal_striping" },
		"cross domain":    func(r *homePackingResponseV1) { r.Homes[0] = 2 },
		"over exact cap":  func(r *homePackingResponseV1) { r.Homes[4] = 0 },
		"missing home":    func(r *homePackingResponseV1) { r.Homes = r.Homes[:7] },
		"empty pack":      func(r *homePackingResponseV1) { r.Homes[4], r.Homes[6] = 0, 0 },
	} {
		t.Run(name, func(t *testing.T) {
			r := response
			r.Homes = append([]int(nil), homes...)
			mutate(&r)
			if _, _, err := run(r, "", limits); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
	if _, _, err := run(response, "{}", limits); err == nil {
		t.Fatal("accepted trailing JSON")
	}
	if _, _, err := run(response, "", ExternalJSONLimits{MaxInput: len(input) - 1, MaxOutput: 2048}); err == nil {
		t.Fatal("accepted input over cap")
	}
	if _, _, err := run(response, "", ExternalJSONLimits{MaxInput: len(input), MaxOutput: 16}); err == nil {
		t.Fatal("accepted output over cap")
	}
	if err := ValidateHomePackingV1(plan, a, homes, receipt); err != nil {
		t.Fatal(err)
	}
	changed := a
	changed.Config.Seed++
	if err := ValidateHomePackingV1(plan, changed, homes, receipt); err == nil {
		t.Fatal("reused homes for changed parent")
	}
	request := plan.request()
	request.TargetHotBytes++
	changedPlan, err := PlanByteBoundedShardsV1(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateHomePackingV1(changedPlan, a, homes, receipt); err == nil {
		t.Fatal("reused homes for changed plan")
	}
	sparseVectors := make([]Vector, 16)
	for i := range sparseVectors {
		sparseVectors[i] = Vector{fmt.Sprintf("sparse-%02d", i), []float64{float64(i), 1}}
	}
	sparseConfig := config()
	sparseConfig.Partitions = 4
	tooFew, err := Build(sparseVectors, sparseConfig)
	if err != nil {
		t.Fatal(err)
	}
	for i := range tooFew.Assignment {
		tooFew.Assignment[i] = i / 5
	}
	tooFew.Metrics = metrics(tooFew)
	if err := ValidateArtifact(tooFew); err != nil {
		t.Fatal(err)
	}
	sparsePlan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{
		Vectors: 16, Dimensions: 2, LogicalDomains: 4, Imbalance: .05,
		TargetHotBytes: uint64(PackFixedOverheadBytesV1 + 2*(alignedRowBytesForTest(2)+GraphIdentityOverheadPerRowV1)),
	})
	if err != nil || sparsePlan.PacksPerDomain != 3 {
		t.Fatalf("sparse plan=%+v err=%v", sparsePlan, err)
	}
	if _, _, err := homePackingRequestJSONV1(sparsePlan, tooFew); err == nil || !strings.Contains(err.Error(), "nonempty packs") {
		t.Fatalf("too few homes must fail before solver: %v", err)
	}
	for _, ratio := range []float64{0, .25} {
		logical, err := BuildOverlap(a, OverlapConfig{Ratio: ratio, Capacity: plan.DomainOverlapCapacity, UsefulOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		packed, err := PackDomainMembershipsWithHomesV1(plan, logical, homes)
		if err != nil {
			t.Fatal(err)
		}
		d, err := NewShardGenerationDescriptorV1(plan, OverlapConfig{Ratio: ratio, Capacity: plan.OverlapCapacity, UsefulOnly: true}, packed)
		if err != nil {
			t.Fatal(err)
		}
		d.HomePacking = &receipt
		raw, err := CanonicalShardGenerationJSONV1(d)
		if err != nil {
			t.Fatal(err)
		}
		d, err = DecodeShardGenerationDescriptorV1(raw, len(raw))
		if err != nil {
			t.Fatal(err)
		}
		retained, err := d.HomePacksV1()
		if err != nil || !reflect.DeepEqual(retained, homes) {
			t.Fatalf("retained homes=%v err=%v", retained, err)
		}
		bad := *d.HomePacking
		bad.HomesSHA256 = strings.Repeat("a", 64)
		d.HomePacking = &bad
		if _, err := CanonicalShardGenerationJSONV1(d); err == nil {
			t.Fatal("accepted changed home digest")
		}
	}
}

// Opt-in native check uses the same pinned distribution as production. Ordinary
// CI exercises the strict protocol above without installing an offline solver.
func TestHomePackingPinnedKaHIPV1(t *testing.T) {
	python := os.Getenv("TREEDB_KAHIP_TEST_PYTHON")
	if python == "" {
		t.Skip("set TREEDB_KAHIP_TEST_PYTHON to the pinned KaHIP Python")
	}
	adapter, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "treedb_kahip_partition.py"))
	if err != nil {
		t.Fatal(err)
	}
	a, plan, _ := homePackingFixtureV1(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var first []int
	for range 2 {
		homes, receipt, err := RunExternalHomePackingV1(ctx, []string{python, "-c", string(adapter)}, ExternalJSONLimits{MaxInput: 1 << 20, MaxOutput: 4096}, plan, a)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateHomePackingV1(plan, a, homes, receipt); err != nil {
			t.Fatal(err)
		}
		for _, edge := range [][2]int{{0, 2}, {1, 3}, {4, 6}, {5, 7}} {
			if homes[edge[0]] != homes[edge[1]] {
				t.Fatalf("community split: %v", homes)
			}
		}
		if first != nil && !reflect.DeepEqual(first, homes) {
			t.Fatal("native packing is not deterministic")
		}
		first = homes
	}
	// No affinities is legitimate. The pinned solver must return admissible
	// homes without a repair, retry, or reference-backend substitution.
	a.Graph = emptyGraph(len(a.IDs))
	a.Metrics = metrics(a)
	for _, singlePack := range []bool{false, true} {
		if singlePack {
			request := plan.request()
			request.TargetHotBytes *= 2
			plan, err = PlanByteBoundedShardsV1(request)
			if err != nil || plan.PacksPerDomain != 1 {
				t.Fatalf("single-pack plan=%+v err=%v", plan, err)
			}
		}
		homes, receipt, err := RunExternalHomePackingV1(ctx, []string{python, "-c", string(adapter)}, ExternalJSONLimits{MaxInput: 1 << 20, MaxOutput: 4096}, plan, a)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateHomePackingV1(plan, a, homes, receipt); err != nil {
			t.Fatal(err)
		}
		if singlePack && !reflect.DeepEqual(homes, a.Assignment) {
			t.Fatal("single pack changed logical homes")
		}
	}
}

// Construction-only allocation guardrail: identical logical membership and
// byte geometry, with the separately measured solver outside this boundary.
func BenchmarkDomainHomePackingV1(b *testing.B) {
	plan, err := PlanByteBoundedShardsV1(ShardPlanInputV1{
		Vectors: 100_000, Dimensions: 768, LogicalDomains: 16,
		OverlapRatio: .2, Imbalance: .05, TargetHotBytes: 7_696_384,
	})
	if err != nil {
		b.Fatal(err)
	}
	logical := OverlapResult{Capacity: plan.DomainOverlapCapacity, Loads: make([]int, plan.LogicalDomains)}
	homes := make([]int, plan.Vectors)
	for ordinal := range plan.Vectors {
		domain := ordinal % plan.LogicalDomains
		logical.Memberships = append(logical.Memberships, Membership{VectorOrdinal: ordinal, Partition: domain, Home: true})
		logical.Loads[domain]++
		homes[ordinal] = domain*plan.PacksPerDomain + (ordinal/plan.LogicalDomains)*plan.PacksPerDomain/(plan.Vectors/plan.LogicalDomains)
	}
	for _, explicit := range []bool{false, true} {
		b.Run(fmt.Sprintf("explicit_homes=%t", explicit), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var packed OverlapResult
				var err error
				if explicit {
					packed, err = PackDomainMembershipsWithHomesV1(plan, logical, homes)
				} else {
					packed, err = PackDomainMembershipsV1(plan, logical)
				}
				if err != nil || len(packed.Memberships) != plan.Vectors {
					b.Fatalf("packing: %v", err)
				}
			}
		})
	}
}
