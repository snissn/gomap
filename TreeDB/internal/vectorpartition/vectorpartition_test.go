package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
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
	if got, want := mustDigest(t, a), "9ec528eff2020ba0d6af5a73b069ce9ba2368850270fb7b561a27d45c889e80e"; got != want {
		t.Fatalf("tiny canonical graph/assignment bytes changed: got %s want %s", got, want)
	}
}
func mustDigest(t *testing.T, a Artifact) string {
	t.Helper()
	d, err := Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	return d
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
	}{{"empty", nil, config()}, {"duplicate", []Vector{{"a", []float64{1}}, {"a", []float64{1}}}, func() Config { c := config(); c.Partitions = 2; return c }()}, {"wrong-dimension", []Vector{{"a", []float64{1}}, {"b", []float64{1, 0}}}, func() Config { c := config(); c.Partitions = 2; return c }()}, {"nonfinite", []Vector{{"a", []float64{math.NaN()}}, {"b", []float64{1}}}, func() Config { c := config(); c.Partitions = 2; return c }()}}
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
	a, _ = Build(fixture(), config())
	a.Graph.Neighbors = a.Graph.Neighbors[:len(a.Graph.Neighbors)-1]
	if e := ValidateArtifact(a); e == nil {
		t.Fatal("missing graph node accepted")
	}
	a, _ = Build(fixture(), config())
	a.IDs[1] = a.IDs[0]
	if e := ValidateArtifact(a); e == nil {
		t.Fatal("duplicate ID accepted")
	}
	a, _ = Build(fixture(), config())
	a.Assignment = []int{0, 0, 0, 0, 0, 0}
	if e := ValidateArtifact(a); e == nil {
		t.Fatal("over-cap assignment accepted")
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
	if _, err := DecodeArtifact(raw, len(raw)-1); err == nil {
		t.Fatal("over-max decoder input accepted")
	}
	if _, err := DecodeArtifact(append(raw, '\n'), len(raw)+1); err == nil {
		t.Fatal("noncanonical whitespace accepted")
	}
	a.Metrics.EdgeCut++
	bad, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
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
func TestGraphPartitionBeatsStableIDHashOnDeterministic10kClusters(t *testing.T) {
	const n = 10_000
	v := make([]Vector, n)
	for i := range v {
		cluster := i % 16
		values := make([]float64, 16)
		values[cluster] = 1
		values[(cluster+1)%16] = float64(i%17) * 1e-6
		v[i] = Vector{ID: fmt.Sprintf("cluster-%02d-%04d", cluster, i/16), Values: values}
	}
	c := DefaultConfig()
	c.Partitions, c.Repetitions, c.Pivots, c.MaxLeafBucket, c.Degree = 16, 1, 4, 64, 4
	c.MaxVectors, c.MaxEdges = n, n*c.Degree
	a, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	if a.Metrics.EdgeCut >= a.Metrics.StableIDHashEdgeCut {
		t.Fatalf("10k clustered edge cut=%d hash=%d", a.Metrics.EdgeCut, a.Metrics.StableIDHashEdgeCut)
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
func TestExternalBackendStartedCancellationCleansPrivateTemp(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := RunExternalJSONForSource(ctx, []string{"sh", "-c", "printf '{' > \"$1\"; sleep 1"}, []byte("{}"), 1024, Source{SourceID: "expected", Checksum: strings.Repeat("0", 64), Vectors: 1, Dimensions: 1, Metric: "cosine"})
	if err == nil {
		t.Fatal("started backend cancellation accepted")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("backend temporary directory leaked after start: %v", entries)
	}
}
func TestExternalBackendDeadlineKillsPipeHoldingDescendant(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := RunExternalJSONForSource(ctx, []string{"sh", "-c", "printf '{' > \"$1\"; (sleep 5) & wait"}, []byte("{}"), 1024, Source{SourceID: "expected", Checksum: strings.Repeat("0", 64), Vectors: 1, Dimensions: 1, Metric: "cosine"})
	if err == nil {
		t.Fatal("pipe-holding descendant accepted")
	}
	if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
		t.Fatalf("deadline return too slow: %s", elapsed)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("descendant temp cleanup leaked: %v", entries)
	}
}
func TestExternalBackendRootExitStillCleansPipeHoldingDescendant(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	// The shell exits successfully immediately, leaving sleep with the command's
	// inherited stderr pipe. CommandContext does not invoke Cancel in this case;
	// post-Wait process-group cleanup must do so.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	_, err := RunExternalJSONForSource(ctx, []string{"sh", "-c", "printf '{' > \"$1\"; (sleep 5) & exit 0"}, []byte("{}"), 1024, Source{SourceID: "expected", Checksum: strings.Repeat("0", 64), Vectors: 1, Dimensions: 1, Metric: "cosine"})
	if err == nil {
		t.Fatal("root-exited pipe-holding descendant accepted")
	}
	if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
		t.Fatalf("root-exited descendant return too slow: %s", elapsed)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("root-exited descendant temp cleanup leaked: %v", entries)
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

type assignmentPartitioner struct{ assignment []int }

func (p assignmentPartitioner) Name() string    { return "assignment-test" }
func (p assignmentPartitioner) License() string { return "test" }
func (p assignmentPartitioner) Partition(Graph, int, int) ([]int, error) {
	return p.assignment, nil
}

type mutatingPartitioner struct {
	assignment []int
	err        error
}

func (mutatingPartitioner) Name() string    { return (ReferencePartitioner{}).Name() }
func (mutatingPartitioner) License() string { return (ReferencePartitioner{}).License() }
func (p mutatingPartitioner) Partition(g Graph, _ int, _ int) ([]int, error) {
	for i, neighbors := range g.Neighbors {
		if len(neighbors) > 1 {
			neighbors[0], neighbors[len(neighbors)-1] = neighbors[len(neighbors)-1], neighbors[0]
			g.Neighbors[i] = neighbors[:1]
		} else {
			g.Neighbors[i] = nil
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	return append([]int(nil), p.assignment...), nil
}

func TestBuildWithPartitionerPreservesCanonicalGraphAgainstMutation(t *testing.T) {
	canonical, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "inline_vectors_v1"}, mutatingPartitioner{assignment: canonical.Assignment})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Graph, canonical.Graph) {
		t.Fatalf("backend mutation changed canonical graph: got=%v want=%v", got.Graph, canonical.Graph)
	}
	if got.Metrics != canonical.Metrics {
		t.Fatalf("backend mutation changed canonical metrics: got=%+v want=%+v", got.Metrics, canonical.Metrics)
	}
	if gotDigest, wantDigest := mustDigest(t, got), mustDigest(t, canonical); gotDigest != wantDigest {
		t.Fatalf("backend mutation changed canonical artifact digest: got=%s want=%s", gotDigest, wantDigest)
	}
}

func TestBuildWithPartitionerFailsClosedAfterMutatingBackendFailure(t *testing.T) {
	_, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "inline_vectors_v1"}, mutatingPartitioner{err: fmt.Errorf("backend failure")})
	if err == nil || !strings.Contains(err.Error(), "backend failure") {
		t.Fatalf("mutating backend failure accepted: %v", err)
	}
	_, err = BuildWithPartitioner(fixture(), config(), Source{SourceID: "inline_vectors_v1"}, mutatingPartitioner{assignment: make([]int, len(fixture()))})
	if err == nil {
		t.Fatal("mutating backend malformed assignment accepted")
	}
}

func TestCloneGraphBoundedUsesIndependentExactBacking(t *testing.T) {
	original := Graph{Neighbors: [][]int{{1, 2}, {0}}}
	clone, err := cloneGraphBounded(original, 3)
	if err != nil {
		t.Fatal(err)
	}
	clone.Neighbors[0][0] = 99
	if original.Neighbors[0][0] != 1 {
		t.Fatalf("backend graph clone aliases canonical graph: %v", original)
	}
	if _, err := cloneGraphBounded(original, 2); err == nil {
		t.Fatal("oversized backend graph copy accepted")
	}
}
func TestBuildFailsClosedOnMalformedBackendAssignment(t *testing.T) {
	for _, assignment := range [][]int{nil, {0}, {0, 1, 0, 1, 0, 1, 0}, {-1, 0, 0, 1, 1, 0}, {2, 0, 0, 1, 1, 0}} {
		if _, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "fixture"}, assignmentPartitioner{assignment}); err == nil {
			t.Fatalf("malformed assignment accepted: %v", assignment)
		}
	}
}
func TestValidatorRejectsSymmetricPolicyEvenForReciprocalGraph(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	a.Graph.Neighbors = [][]int{{1}, {0}, {3}, {2}, {5}, {4}}
	loads, err := validateAssignment(a.Assignment, len(a.IDs), a.Config)
	if err != nil {
		t.Fatal(err)
	}
	a.Metrics = metricsWithLoads(a, loads)
	a.Config.Symmetric = true
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("reciprocal graph accepted as symmetric")
	}
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeArtifact(raw, len(raw)); err == nil {
		t.Fatal("decode accepted a reciprocal symmetric artifact")
	}
}
func TestReferencePartitionerCoversEveryPartition(t *testing.T) {
	g := Graph{Neighbors: make([][]int, 8)}
	a, err := (ReferencePartitioner{}).Partition(g, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]bool, 4)
	for _, p := range a {
		seen[p] = true
	}
	for p, ok := range seen {
		if !ok {
			t.Fatalf("partition %d left empty: %v", p, a)
		}
	}
}
func TestReferencePartitionerRejectsExcessiveWork(t *testing.T) {
	g := Graph{Neighbors: make([][]int, 16_384)}
	if _, err := (ReferencePartitioner{}).Partition(g, 16_384, 2); err == nil {
		t.Fatal("unbounded partition work accepted")
	}
}
func TestPartitionWorkCountsAllPassesAtEmptyGraphBoundary(t *testing.T) {
	// On an empty graph, the capacity boundary is determined entirely by the
	// full-node initialization, partition scan, and global slice setup. This
	// guards against silently accepting an allocation shape above the declared
	// work cap merely because it has degree zero.
	const n = 1_000_000
	if work, overflow := partitionWorkUnits(n, 237, 0); overflow || work != 249_000_716 || work > maxPartitionWork {
		t.Fatalf("accepted boundary miscounted: work=%d overflow=%v", work, overflow)
	}
	if work, overflow := partitionWorkUnits(n, 238, 0); overflow || work != 250_000_719 || work <= maxPartitionWork {
		t.Fatalf("rejected boundary miscounted: work=%d overflow=%v", work, overflow)
	}
	if !partitionWorkExceeded(n, 238, 0) {
		t.Fatal("empty-graph work above cap accepted")
	}
}
func TestReferencePartitionerRejectsVectorBoundBeforeInternalAllocation(t *testing.T) {
	g := Graph{Neighbors: make([][]int, maxVectors+1)}
	if _, err := (ReferencePartitioner{}).Partition(g, 1, maxVectors+1); err == nil {
		t.Fatal("partitioner accepted graph above direct vector bound")
	}
}
func TestLeafNearestTieBreaksByOrdinal(t *testing.T) {
	v := []Vector{{"a", []float64{1, 0}}, {"b", []float64{0, 1}}, {"c", []float64{0, -1}}, {"d", []float64{-1, 0}}}
	got, err := nearest(v, []int{0, 1, 2, 3}, 0, 2, &distanceBudget{remaining: 100})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].i != 1 || got[1].i != 2 {
		t.Fatalf("ties not ordinal-stable: %+v", got)
	}
}
func TestBuildWithPartitionerValidatesBackendOutputAndSource(t *testing.T) {
	_, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "fixture", Checksum: "00"}, badPartitioner{})
	if err == nil {
		t.Fatal("mismatched source accepted")
	}
	if _, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "fixture"}, badPartitioner{}); err == nil {
		t.Fatal("malicious assignment accepted")
	}
	if _, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: "fixture", Metric: "cosine"}, ReferencePartitioner{}); err == nil {
		t.Fatal("partial source identity accepted")
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
func TestArtifactRejectsAggregateIDByteCap(t *testing.T) {
	chunk := strings.Repeat("x", maxTotalIDBytes/2+1)
	c := Config{Metric: "cosine", Repetitions: 1, Pivots: 2, MaxLeafBucket: 2, Degree: 1, Partitions: 2, MaxVectors: 2, MaxEdges: 2}
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: Source{SourceID: "test", Checksum: strings.Repeat("0", 64), Vectors: 2, Dimensions: 1, Metric: "cosine"}, Config: c, IDs: []string{"a" + chunk, "b" + chunk}, Graph: Graph{Neighbors: make([][]int, 2)}, Assignment: []int{0, 1}}
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("aggregate ID cap accepted")
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
func TestCosineDistanceHandlesHugeFiniteAndRejectsZeroNorm(t *testing.T) {
	if d := distance([]float64{math.MaxFloat64, math.MaxFloat64}, []float64{math.MaxFloat64, math.MaxFloat64}); !finite(d) || math.Abs(d) > 1e-12 {
		t.Fatalf("unstable huge cosine distance: %v", d)
	}
	c := config()
	if _, err := Build([]Vector{{ID: "a", Values: []float64{0, 0}}, {ID: "b", Values: []float64{1, 0}}}, c); err == nil {
		t.Fatal("zero norm accepted")
	}
}
