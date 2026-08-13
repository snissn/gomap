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

func TestM0MaterializeMembershipReopensDisposableClone(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	root := t.TempDir()
	fixture := fixtureManifest{SchemaVersion: 1, Fixture: "m0-clone", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: 32, Queries: 1, Dimensions: 4, Metric: "cosine", Seed: 17, Checksum: strings.Repeat("a", 64)}
	sourceDB := filepath.Join(root, "source")
	descriptor := testM8QualificationRetainedDescriptorV1(t, sourceDB, strings.Repeat("b", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
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
	account := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", GraphArtifactSHA256: descriptor.GraphArtifactSHA256, AssignmentArtifactSHA256: m0SHA256V1(raw), RepartitionedArtifactSHA256: digest, Partitions: 16, EdgeCut: 0, Modes: []m0MembershipModeV1{{Name: "zero", Materialize: true, MembershipSHA256: zeroSHA}, {Name: "useful_only_20", EquivalentTo: "zero"}, {Name: "exact_20", Rejected: "exact-20 contains filler"}}}
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
	if err = runM0MaterializeMembershipV1([]string{"-source-db", sourceDB, "-artifact", artifactPath, "-membership-report", badPath, "-root", filepath.Join(root, "bad-clones"), "-out", filepath.Join(root, "bad.json")}, bytes.NewBuffer(nil)); err == nil {
		t.Fatal("materialized assignment from unrelated graph lineage")
	}
	out := filepath.Join(root, "out.json")
	if err = runM0MaterializeMembershipV1([]string{"-source-db", sourceDB, "-artifact", artifactPath, "-membership-report", accountPath, "-root", filepath.Join(root, "clones"), "-out", out}, bytes.NewBuffer(nil)); err != nil {
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
}

func TestM0MaterializeUsefulMembershipReopensDisposableClone(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	root := t.TempDir()
	fixture := fixtureManifest{SchemaVersion: 1, Fixture: "m0-useful-clone", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: 32, Queries: 1, Dimensions: 4, Metric: "cosine", Seed: 17, Checksum: strings.Repeat("a", 64)}
	sourceDB := filepath.Join(root, "source")
	descriptor := testM8QualificationRetainedDescriptorV1(t, sourceDB, strings.Repeat("b", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
	vectors := fixtureVectors(fixture)
	input := make([]vectorpartition.Vector, len(vectors))
	for i := range vectors {
		input[i] = vectorpartition.Vector{ID: fmt.Sprintf("doc-%06d", i), Values: vectors[i]}
	}
	config := vectorpartition.DefaultConfig()
	config.Partitions, config.Seed, config.MaxDistanceWork = 4, fixture.Seed, 20_000_000_000
	artifact, err := vectorpartition.BuildWithPartitioner(input, config, vectorpartition.Source{SourceID: "qualification-test:" + fixture.Checksum}, m0ModuloPartitionerV1{})
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
	account := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", GraphArtifactSHA256: descriptor.GraphArtifactSHA256, AssignmentArtifactSHA256: m0SHA256V1(raw), RepartitionedArtifactSHA256: digest, Partitions: config.Partitions, Modes: modes}
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
	if err = runM0MaterializeMembershipV1([]string{"-source-db", sourceDB, "-artifact", artifactPath, "-membership-report", accountPath, "-mode", "useful_only_20", "-root", filepath.Join(root, "clones"), "-out", out}, bytes.NewBuffer(nil)); err != nil {
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
}

type m0ModuloPartitionerV1 struct{}

func (m0ModuloPartitionerV1) Name() string    { return "m0_modulo_test" }
func (m0ModuloPartitionerV1) License() string { return "test" }
func (m0ModuloPartitionerV1) Partition(graph vectorpartition.Graph, partitions, _ int) ([]int, error) {
	assignment := make([]int, len(graph.Neighbors))
	for i := range assignment {
		assignment[i] = i % partitions
	}
	return assignment, nil
}
