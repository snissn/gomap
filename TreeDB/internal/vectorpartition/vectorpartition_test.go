package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture() []Vector {
	return []Vector{{"z", []float64{1, 0}}, {"a", []float64{1, .01}}, {"b", []float64{.99, .02}}, {"c", []float64{0, 1}}, {"d", []float64{.01, 1}}, {"e", []float64{.02, .99}}}
}
func config() Config {
	c := DefaultConfig()
	c.Partitions = 2
	c.Pivots = 2
	c.MaxLeafBucket = 2
	c.Degree = 2
	c.Repetitions = 2
	c.MaxVectors = 64
	c.MaxEdges = 128
	return c
}
func TestDenseBallGraphAndPartitionDeterministic(t *testing.T) {
	a, e := Build(fixture(), config())
	if e != nil {
		t.Fatal(e)
	}
	b, e := Build(fixture(), config())
	if e != nil {
		t.Fatal(e)
	}
	aj, _ := CanonicalJSON(a)
	bj, _ := CanonicalJSON(b)
	if !bytes.Equal(aj, bj) {
		t.Fatal("artifact bytes differ")
	}
	if a.IDs[0] != "a" || a.Metrics.MaxPartitionSize > a.Metrics.Cap {
		t.Fatalf("artifact=%+v", a)
	}
	if e := ValidateArtifact(a); e != nil {
		t.Fatal(e)
	}
}
func TestGraphDegreeAndCapOnDuplicateSkew(t *testing.T) {
	v := make([]Vector, 12)
	for i := range v {
		v[i] = Vector{ID: string(rune('a' + i)), Values: []float64{1, 0}}
	}
	c := config()
	c.Partitions = 3
	c.MaxLeafBucket = 2
	a, e := Build(v, c)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range a.Graph.Neighbors {
		if len(n) > c.Degree {
			t.Fatal("degree cap")
		}
	}
	if a.Metrics.MaxPartitionSize > a.Metrics.Cap {
		t.Fatal("partition cap")
	}
}
func TestSkewedPivotBucketsUseDeterministicChunkFallback(t *testing.T) {
	v := make([]Vector, 128)
	for i := range v {
		// Identical directions force a single pivot bucket; this formerly
		// exercised unbounded recursive depth on sufficiently skewed inputs.
		v[i] = Vector{ID: fmt.Sprintf("skew-%03d", i), Values: []float64{1, 0}}
	}
	c := config()
	c.MaxLeafBucket = 8
	c.Degree = 4
	c.MaxVectors = len(v)
	c.MaxEdges = 1024
	a, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateArtifact(a); err != nil {
		t.Fatal(err)
	}
	if _, err := nearest(v, []int{0, 1, 2}, 0, 2, &distanceBudget{remaining: 1}); err == nil {
		t.Fatal("leaf distance-work budget was not enforced")
	}
}
func TestMalformedFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		v    []Vector
		c    Config
	}{{"empty", nil, config()}, {"duplicate", []Vector{{"a", []float64{1}}, {"a", []float64{1}}}, func() Config { c := config(); c.Partitions = 2; return c }()}, {"nonfinite", []Vector{{"a", []float64{math.NaN()}}, {"b", []float64{1}}}, func() Config { c := config(); c.Partitions = 2; return c }()}}
	for _, x := range cases {
		if _, e := Build(x.v, x.c); e == nil {
			t.Fatalf("%s accepted", x.name)
		}
	}
}
func TestValidatorRejectsCorruptBackendOutput(t *testing.T) {
	a, e := Build(fixture(), config())
	if e != nil {
		t.Fatal(e)
	}
	a.Assignment[0] = a.Config.Partitions
	if e := ValidateArtifact(a); e == nil {
		t.Fatal("out of range accepted")
	}
	a, e = Build(fixture(), config())
	a.Graph.Neighbors[0] = append(a.Graph.Neighbors[0], 0)
	if e := ValidateArtifact(a); e == nil {
		t.Fatal("self edge accepted")
	}
}

func TestStrictDecoderRejectsNonCanonicalAndMetricForgery(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeArtifact(raw, len(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeArtifact(append(raw, '\n'), len(raw)+1); err == nil {
		t.Fatal("noncanonical whitespace accepted")
	}
	a.Metrics.EdgeCut++
	bad, _ := json.Marshal(a)
	if _, err := DecodeArtifact(bad, len(bad)); err == nil {
		t.Fatal("forged metrics accepted")
	}
}

func TestGraphPartitionBeatsStableIDHashOnClusteredFixture(t *testing.T) {
	v := make([]Vector, 24)
	for i := range v {
		cluster := i / 12
		x, y := 0.0, 0.0
		if cluster == 0 {
			x = 1
		} else {
			y = 1
		}
		v[i] = Vector{ID: string(rune('a' + i)), Values: []float64{x, y}}
	}
	c := config()
	c.Partitions = 2
	c.MaxLeafBucket = 32
	c.Degree = 4
	c.MaxEdges = 256
	a, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	if a.Metrics.EdgeCut >= a.Metrics.StableIDHashEdgeCut {
		t.Fatalf("graph cut=%d hash=%d", a.Metrics.EdgeCut, a.Metrics.StableIDHashEdgeCut)
	}
}

func TestExternalBackendCancellationCleansPrivateTemp(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	_, err := RunExternalJSONForSource(ctx, []string{"sh", "-c", "sleep 1"}, []byte("{}"), 1024, Source{SourceID: "expected", Checksum: strings.Repeat("0", 64), Vectors: 1, Dimensions: 1, Metric: "cosine"})
	if err == nil {
		t.Fatal("cancelled backend accepted")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() && filepath.Base(entry.Name()) != "" {
			t.Fatalf("backend temporary directory leaked: %s", entry.Name())
		}
	}
}
func TestExternalBackendRequiresDeadline(t *testing.T) {
	if _, err := RunExternalJSON(context.Background(), []string{"true"}, []byte("{}"), 1024); err == nil {
		t.Fatal("deadline-less backend accepted")
	}
}

func TestExternalBackendRejectsUnboundExpectedSourceBeforeExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, source := range []Source{
		{},
		{SourceID: "only-id"},
		{SourceID: "id", Checksum: strings.Repeat("0", 64), Vectors: 1, Dimensions: 1, Metric: "l2"},
	} {
		if _, err := RunExternalJSONForSource(ctx, []string{"definitely-not-an-executable"}, []byte("{}"), 1024, source); err == nil || !strings.Contains(err.Error(), "invalid expected source binding") {
			t.Fatalf("unbound expected source reached backend: source=%+v err=%v", source, err)
		}
	}
}

func TestDecodeArtifactForSourceRejectsForgedSource(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	expected := a.Source
	expected.Checksum = strings.Repeat("f", 64)
	if _, err := DecodeArtifactForSource(raw, len(raw), expected); err == nil {
		t.Fatal("forged expected source accepted")
	}
}

type badPartitioner struct{}

func (badPartitioner) Name() string    { return "bad" }
func (badPartitioner) License() string { return "test license" }
func (badPartitioner) Partition(g Graph, parts, cap int) ([]int, error) {
	return make([]int, len(g.Neighbors)), nil
}
func TestBuildWithPartitionerValidatesBackendOutputAndSource(t *testing.T) {
	_, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "fixture", Checksum: "00"}, badPartitioner{})
	if err == nil {
		t.Fatal("mismatched source accepted")
	}
	if _, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "fixture"}, badPartitioner{}); err == nil {
		t.Fatal("malicious assignment accepted")
	}
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	a.Graph.Neighbors[0] = []int{2, 1}
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("noncanonical neighbor ordering accepted")
	}
}
func TestBoundedUnionKeepsClosestNotLowestOrdinal(t *testing.T) {
	s := map[int]float64{}
	addCandidateBounded(s, 99, .1, 2)
	addCandidateBounded(s, 1, .9, 2)
	addCandidateBounded(s, 50, .2, 2)
	if _, ok := s[1]; ok {
		t.Fatal("ordinal-only truncation retained distant neighbor")
	}
	if _, ok := s[99]; !ok {
		t.Fatal("closest candidate lost")
	}
}
func TestArtifactRejectsOversizedIDsAndSourceDimensions(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	a.Source.Dimensions = 5000
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("oversized source dimensions accepted")
	}
	a, err = Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	a.IDs[0] = strings.Repeat("x", maxIDBytes+1)
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("oversized ID accepted")
	}
}

func TestMetricsHighPartitionCountUsesAssignmentHistogram(t *testing.T) {
	const n = 16_384
	ids := make([]string, n)
	assignment := make([]int, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%06d", i)
		assignment[i] = i
	}
	c := Config{Metric: "cosine", Seed: 1, Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 1, Partitions: n, Imbalance: .05, MaxVectors: n, MaxEdges: n}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: Source{SourceID: "test", Checksum: strings.Repeat("0", 64), Vectors: n, Dimensions: 1, Metric: "cosine"}, Config: c, IDs: ids, Graph: Graph{Neighbors: make([][]int, n)}, Assignment: assignment}
	a.Metrics = metrics(a)
	if err := ValidateArtifact(a); err != nil {
		t.Fatal(err)
	}
}
