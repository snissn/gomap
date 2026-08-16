package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM0MaterializeVariantV1OnlyAcceptsProductionVariants(t *testing.T) {
	for _, want := range []struct {
		variant collections.VectorPartitionLocalGraphVariantV1
		m, ef   int
	}{
		{collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 16, 128},
		{collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 18, 256},
	} {
		variant, m, efConstruction, err := m0MaterializeVariantV1(string(want.variant))
		if err != nil || variant != want.variant || m != want.m || efConstruction != want.ef {
			t.Fatalf("variant %q = (%q,%d,%d,%v)", want.variant, variant, m, efConstruction, err)
		}
	}
	if _, _, _, err := m0MaterializeVariantV1(string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1)); err == nil {
		t.Fatal("unsupported M20 variant accepted")
	}
}

func TestM0MaterializeAcceptsHistoricalDescriptorGraphDigestV1(t *testing.T) {
	config := vectorpartition.DefaultConfig()
	config.Partitions, config.Degree = 2, 1
	graph, err := vectorpartition.BuildWithPartitioner([]vectorpartition.Vector{{ID: "a", Values: []float64{1}}, {ID: "b", Values: []float64{-1}}}, config, vectorpartition.Source{SourceID: "m0-historical-descriptor"}, m0StaticPartitionerV1{})
	if err != nil {
		t.Fatal(err)
	}
	assignment := graph
	assignment.Assignment = []int{1, 0}
	graphRaw, err := vectorpartition.CanonicalJSON(graph)
	if err != nil {
		t.Fatal(err)
	}
	assignmentRaw, err := vectorpartition.CanonicalJSON(assignment)
	if err != nil {
		t.Fatal(err)
	}
	account := m0MembershipAccountV1{GraphArtifactSHA256: m0SHA256V1(graphRaw), AssignmentArtifactSHA256: m0SHA256V1(assignmentRaw)}
	descriptor := m3VariantDescriptorV1{Source: assignment.Source, ArtifactSHA256: account.AssignmentArtifactSHA256, GraphArtifactSHA256: account.AssignmentArtifactSHA256}
	if err := m0MaterializeRetainedDescriptorBindingV1(descriptor, assignment, account); err != nil {
		t.Fatalf("historical descriptor rejected: %v", err)
	}
	if err := m0AssignmentBindsFrozenGraphV1(graph, assignment, graphRaw, account); err != nil {
		t.Fatalf("valid frozen graph binding rejected: %v", err)
	}
	bad := account
	bad.GraphArtifactSHA256 = account.AssignmentArtifactSHA256
	if err := m0AssignmentBindsFrozenGraphV1(graph, assignment, graphRaw, bad); err == nil {
		t.Fatal("unrelated graph/account accepted")
	}
}

func testM0MaterializeBuildIdentityV1(t *testing.T) {
	t.Helper()
	previous := m0MaterializeBuildIdentityProviderV1
	m0MaterializeBuildIdentityProviderV1 = func() (m0CleanBuildIdentityV1, error) {
		return m0CleanBuildIdentityV1{BinarySHA256: strings.Repeat("f", 64), SourceRevision: strings.Repeat("e", 40)}, nil
	}
	t.Cleanup(func() { m0MaterializeBuildIdentityProviderV1 = previous })
}

func TestM0MaterializeMembershipReopensDisposableClone(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	testM0MaterializeBuildIdentityV1(t)
	root := t.TempDir()
	fixture := fixtureManifest{SchemaVersion: 1, Fixture: "m0-clone", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: 32, Queries: 1, Dimensions: 4, Metric: "cosine", Seed: 17, Checksum: strings.Repeat("a", 64)}
	sourceDB := filepath.Join(root, "source")
	testM8QualificationRetainedDescriptorV1(t, sourceDB, strings.Repeat("b", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
	vectors := fixtureVectors(fixture)
	input := make([]vectorpartition.Vector, len(vectors))
	for i := range vectors {
		input[i] = vectorpartition.Vector{ID: fmt.Sprintf("doc-%06d", i), Values: vectors[i]}
	}
	config := vectorpartition.DefaultConfig()
	config.Partitions, config.Seed, config.MaxDistanceWork = 16, fixture.Seed, 20_000_000_000
	artifact, err := vectorpartition.BuildWithPartitioner(input, config, vectorpartition.Source{SourceID: "qualification-test:" + fixture.Checksum}, vectorpartition.ReferencePartitioner{})
	if err != nil {
		t.Fatal(err)
	}
	artifact.Backend = fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed)
	raw, err := vectorpartition.CanonicalJSON(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(root, "assignment.json")
	if err = os.WriteFile(artifactPath, raw, 0644); err != nil {
		t.Fatal(err)
	}
	graphPath := filepath.Join(root, "graph.json")
	if err = os.WriteFile(graphPath, raw, 0644); err != nil {
		t.Fatal(err)
	}
	digest, err := vectorpartition.Digest(artifact)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{})
	if err != nil {
		t.Fatal(err)
	}
	zeroSHA, err := m0MembershipDigestV1(zero.Memberships)
	if err != nil {
		t.Fatal(err)
	}
	account := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", GraphArtifactSHA256: m0SHA256V1(raw), AssignmentArtifactSHA256: m0SHA256V1(raw), RepartitionedArtifactSHA256: digest, Partitions: 16, EdgeCut: 0, Modes: []m0MembershipModeV1{{Name: "zero", Materialize: true, MembershipSHA256: zeroSHA}, {Name: "useful_only_20", EquivalentTo: "zero"}, {Name: "exact_20", Rejected: "exact-20 contains filler"}}}
	accountRaw, err := json.Marshal(account)
	if err != nil {
		t.Fatal(err)
	}
	accountPath := filepath.Join(root, "account.json")
	if err = os.WriteFile(accountPath, accountRaw, 0644); err != nil {
		t.Fatal(err)
	}
	bad := account
	bad.GraphArtifactSHA256 = strings.Repeat("c", 64)
	badRaw, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	badPath := filepath.Join(root, "bad-account.json")
	if err = os.WriteFile(badPath, badRaw, 0644); err != nil {
		t.Fatal(err)
	}
	if err = runM0MaterializeMembershipV1([]string{"-source-db", sourceDB, "-artifact", artifactPath, "-graph-artifact", graphPath, "-membership-report", badPath, "-root", filepath.Join(root, "bad-clones"), "-out", filepath.Join(root, "bad.json")}, bytes.NewBuffer(nil)); err == nil {
		t.Fatal("materialized assignment from unrelated graph lineage")
	}
	out := filepath.Join(root, "out.json")
	if err = runM0MaterializeMembershipV1([]string{"-source-db", sourceDB, "-artifact", artifactPath, "-graph-artifact", graphPath, "-membership-report", accountPath, "-root", filepath.Join(root, "clones"), "-out", out}, bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	}
	var report m0MaterializeReportV1
	reportRaw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(reportRaw, &report); err != nil {
		t.Fatal(err)
	}
	if report.PartitionCount != 16 || report.OverlapCount != 0 || report.PackBytes == 0 || report.CloneLogicalBytes == 0 || report.SourceOrdinalDigestBefore == "" || report.SourceOrdinalDigestBefore != report.SourceOrdinalDigestAfter {
		t.Fatalf("report=%+v", report)
	}
	descriptor, err := m3ReadVariantDescriptorV1(report.CloneDB)
	if err != nil {
		t.Fatal(err)
	}
	h, err := openM8ProductionExistingAssetSetModeV1(report.CloneDB, true)
	if err != nil {
		t.Fatalf("strict reopen: %v", err)
	}
	policy, ok := collections.ParseVectorPartitionOverlapPolicyV1(h.manifest.BalancePolicy)
	if !ok || descriptor.PartitionHNSWM != 18 || descriptor.PartitionHNSWEfC != 256 || descriptor.ArtifactSHA256 != account.AssignmentArtifactSHA256 || descriptor.GraphArtifactSHA256 != account.GraphArtifactSHA256 || policy.BuildIdentityDigest != descriptor.BuildIdentityDigest || descriptor.OverlapMemberships != descriptor.OverlapRealized {
		_ = h.Close()
		t.Fatalf("rewritten descriptor=%+v policy=%+v", descriptor, policy)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM0MaterializeUsefulMembershipReopensDisposableClone(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	testM0MaterializeBuildIdentityV1(t)
	root := t.TempDir()
	fixture := fixtureManifest{SchemaVersion: 1, Fixture: "m0-useful-clone", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: 32, Queries: 1, Dimensions: 4, Metric: "cosine", Seed: 17, Checksum: strings.Repeat("a", 64)}
	sourceDB := filepath.Join(root, "source")
	testM8QualificationRetainedDescriptorV1(t, sourceDB, strings.Repeat("b", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
	vectors := fixtureVectors(fixture)
	input := make([]vectorpartition.Vector, len(vectors))
	for i := range vectors {
		input[i] = vectorpartition.Vector{ID: fmt.Sprintf("doc-%06d", i), Values: vectors[i]}
	}
	config := vectorpartition.DefaultConfig()
	// Match the retained source descriptor exactly: M0 must reject a different
	// assignment artifact before cloning, even when it has the same source.
	config.Partitions, config.Seed, config.MaxDistanceWork = 16, fixture.Seed, 20_000_000_000
	artifact, err := vectorpartition.BuildWithPartitioner(input, config, vectorpartition.Source{SourceID: "qualification-test:" + fixture.Checksum}, vectorpartition.ReferencePartitioner{})
	if err != nil {
		t.Fatal(err)
	}
	artifact.Backend = fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed)
	raw, err := vectorpartition.CanonicalJSON(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(root, "assignment.json")
	if err = os.WriteFile(artifactPath, raw, 0644); err != nil {
		t.Fatal(err)
	}
	graphPath := filepath.Join(root, "graph.json")
	if err = os.WriteFile(graphPath, raw, 0644); err != nil {
		t.Fatal(err)
	}
	digest, err := vectorpartition.Digest(artifact)
	if err != nil {
		t.Fatal(err)
	}
	capacity, err := m3OverlapCapacityV1(artifact, .2)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{})
	if err != nil {
		t.Fatal(err)
	}
	useful, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: .2, Capacity: capacity})
	if err != nil || useful.Used == 0 || useful.Useful != useful.Used || useful.Filler != 0 {
		t.Fatalf("useful overlap=%+v err=%v", useful, err)
	}
	exact, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: .2, Capacity: capacity, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	modes, err := m0MembershipModesV1(artifact, zero, useful, exact)
	if err != nil {
		t.Fatal(err)
	}
	account := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", GraphArtifactSHA256: m0SHA256V1(raw), AssignmentArtifactSHA256: m0SHA256V1(raw), RepartitionedArtifactSHA256: digest, Partitions: config.Partitions, Modes: modes}
	if _, selected, err := m0SelectedMembershipV1(artifact, raw, account, "useful_only_20"); err != nil || selected.Used != useful.Used {
		t.Fatalf("select useful membership selected=%+v err=%v", selected, err)
	}
	for _, mutate := range []func(*m0MembershipModeV1){
		func(mode *m0MembershipModeV1) { mode.Filler++ },
		func(mode *m0MembershipModeV1) { mode.EquivalentTo = "zero" },
	} {
		bad := account
		bad.Modes = append([]m0MembershipModeV1(nil), account.Modes...)
		for i := range bad.Modes {
			if bad.Modes[i].Name == "useful_only_20" {
				mutate(&bad.Modes[i])
			}
		}
		if _, _, err := m0SelectedMembershipV1(artifact, raw, bad, "useful_only_20"); err == nil {
			t.Fatal("invalid useful disposition accepted")
		}
	}
	if _, _, err := m0SelectedMembershipV1(artifact, raw, account, "exact_20"); err == nil {
		t.Fatal("duplicate exact mode accepted")
	}
	accountRaw, err := json.Marshal(account)
	if err != nil {
		t.Fatal(err)
	}
	accountPath := filepath.Join(root, "account.json")
	if err = os.WriteFile(accountPath, accountRaw, 0644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "out.json")
	if err = runM0MaterializeMembershipV1([]string{"-source-db", sourceDB, "-artifact", artifactPath, "-graph-artifact", graphPath, "-membership-report", accountPath, "-mode", "useful_only_20", "-root", filepath.Join(root, "clones"), "-out", out}, bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	}
	var report m0MaterializeReportV1
	reportRaw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read materialization report: %v", err)
	}
	if err = json.Unmarshal(reportRaw, &report); err != nil {
		t.Fatalf("decode materialization report: %v", err)
	}
	usefulSHA, err := m0MembershipDigestV1(useful.Memberships)
	if err != nil {
		t.Fatal(err)
	}
	if report.Mode != "useful_only_20" || report.MembershipSHA256 != usefulSHA || report.OverlapCount != useful.Used || report.PartitionCount != uint32(config.Partitions) || report.SourceOrdinalDigestBefore == "" || report.SourceOrdinalDigestBefore != report.SourceOrdinalDigestAfter {
		t.Fatalf("report=%+v", report)
	}
	descriptor, err := m3ReadVariantDescriptorV1(report.CloneDB)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.OverlapMemberships != useful.Used || descriptor.OverlapRealized != useful.Used || descriptor.OverlapMemberships != descriptor.OverlapRealized {
		t.Fatalf("rewritten overlap accounting descriptor=%+v useful=%+v", descriptor, useful)
	}
	h, err := openM8ProductionExistingAssetSetModeV1(report.CloneDB, true)
	if err != nil {
		t.Fatalf("strict useful reopen: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}
