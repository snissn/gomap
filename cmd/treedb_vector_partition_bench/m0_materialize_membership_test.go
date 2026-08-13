package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM0MaterializeMembershipReopensDisposableClone(t *testing.T) {
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
	account := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", AssignmentArtifactSHA256: m0SHA256V1(raw), RepartitionedArtifactSHA256: digest, Partitions: 16, EdgeCut: 0, Modes: []m0MembershipModeV1{{Name: "zero", Materialize: true, MembershipSHA256: zeroSHA}, {Name: "useful_only_20", EquivalentTo: "zero"}, {Name: "exact_20", Rejected: "exact-20 contains filler"}}}
	accountRaw, err := json.Marshal(account)
	if err != nil {
		t.Fatal(err)
	}
	accountPath := filepath.Join(root, "account.json")
	if err = os.WriteFile(accountPath, accountRaw, 0644); err != nil {
		t.Fatal(err)
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
