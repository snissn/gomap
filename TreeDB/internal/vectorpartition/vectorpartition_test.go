package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func emptyGraph(nodes int) Graph {
	g := Graph{Neighbors: make([][]int, nodes)}
	for i := range g.Neighbors {
		g.Neighbors[i] = make([]int, 0)
	}
	return g
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
	// MaxDistanceWork is intentionally persisted in Config so an artifact
	// records the scalar-work safety envelope that constructed it.
	if got, want := mustDigest(t, a), "b8a79eb002035b5104793e86e0c993fb4514350f37f60e49c0d5d19e983ef7c7"; got != want {
		t.Fatalf("tiny canonical graph/assignment bytes changed: got %s want %s", got, want)
	}
}
func TestBuildStableIDHashBaselineOwnsAssignmentAndPreservesSource(t *testing.T) {
	source, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := BuildStableIDHashBaseline(source)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Backend != "stable_id_hash_baseline_v1" || baseline.Source != source.Source || baseline.Config != source.Config || !reflect.DeepEqual(baseline.Graph, source.Graph) {
		t.Fatalf("baseline identity mismatch: %+v", baseline)
	}
	for i, id := range baseline.IDs {
		if got, want := baseline.Assignment[i], stableIDPartition(id, baseline.Config.Partitions); got != want {
			t.Fatalf("assignment[%d]=%d want %d", i, got, want)
		}
	}
	if err := ValidateArtifact(baseline); err != nil {
		t.Fatal(err)
	}
	baseline.Assignment[0] = (baseline.Assignment[0] + 1) % baseline.Config.Partitions
	baseline.Graph.Neighbors[0] = append(baseline.Graph.Neighbors[0], 99)
	if reflect.DeepEqual(baseline.Assignment, source.Assignment) || reflect.DeepEqual(baseline.Graph, source.Graph) {
		t.Fatal("baseline retained mutable source slices")
	}
}

func TestBuildStableIDHashBaselinePreservesEmptyAdjacencyArray(t *testing.T) {
	cfg := config()
	cfg.Partitions = 1
	source, err := Build([]Vector{{ID: "only", Values: []float64{1, 0}}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if source.Graph.Neighbors[0] == nil || len(source.Graph.Neighbors[0]) != 0 {
		t.Fatalf("source empty adjacency=%v", source.Graph.Neighbors[0])
	}
	baseline, err := BuildStableIDHashBaseline(source)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Graph.Neighbors[0] == nil || len(baseline.Graph.Neighbors[0]) != 0 {
		t.Fatalf("baseline empty adjacency=%v", baseline.Graph.Neighbors[0])
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

func TestBuildRejectsOverCapBeforeVectorElementValidation(t *testing.T) {
	sharedValues := make([]float64, maxDimensions)
	sharedValues[0] = math.NaN()
	vectors := make([]Vector, maxVectors+1)
	for i := range vectors {
		// Every entry shares the same backing values; if Build scans even one
		// element, its empty ID/non-finite value would produce a different error.
		vectors[i] = Vector{Values: sharedValues}
	}
	_, err := Build(vectors, DefaultConfig())
	if err == nil || !strings.Contains(err.Error(), "vector count outside configured bounds") {
		t.Fatalf("Build error=%v; count cap must reject before element validation", err)
	}
}

func TestBuildPreflightsSelectedShapeBeforeFullVectorScan(t *testing.T) {
	shapeConfig := func() Config {
		c := DefaultConfig()
		c.Repetitions = 1
		c.Pivots = 2
		c.MaxLeafBucket = 2
		c.Degree = 1
		return c
	}
	edge := shapeConfig()
	edge.Partitions, edge.Degree, edge.MaxEdges = 1, 2, 1
	pivot := shapeConfig()
	pivot.Partitions, pivot.Pivots = 1, maxPivots
	overlap := shapeConfig()
	overlap.Partitions, overlap.MaxLeafBucket = 1, 65_536
	partition := shapeConfig()
	partition.Partitions = 238
	for _, tc := range []struct {
		name  string
		count int
		dims  int
		cfg   Config
		want  string
	}{
		{"edge bound", 2, 1, edge, "graph edge bound"},
		{"pivot scalar work", 310_000, 64, pivot, "scalar-work"},
		{"dual leaf scalar work", 300_000, 1, overlap, "scalar-work"},
		{"reference partition work", 1_000_000, 1, partition, "partition work"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shared := make([]float64, tc.dims)
			shared[0] = math.NaN()
			vectors := make([]Vector, tc.count)
			for i := range vectors {
				// An element scan would reject this empty ID/non-finite value
				// instead of the selected deterministic shape bound.
				vectors[i] = Vector{Values: shared}
			}
			_, err := Build(vectors, tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Build error=%v; want preflight %q", err, tc.want)
			}
		})
	}
}

func TestBuildAppliesReferenceWorkOnlyToReferenceBackend(t *testing.T) {
	c := DefaultConfig()
	c.Partitions, c.Repetitions, c.Pivots, c.MaxLeafBucket, c.Degree = 238, 1, 2, 2, 1
	vectors := make([]Vector, 1_000_000)
	shared := []float64{math.NaN()}
	for i := range vectors {
		vectors[i] = Vector{ID: "valid", Values: shared}
	}
	_, err := BuildWithPartitioner(vectors, c, Source{SourceID: "custom"}, assignmentPartitioner{})
	if err == nil || !strings.Contains(err.Error(), "non-finite vector value") {
		t.Fatalf("custom backend incorrectly received reference work gate: %v", err)
	}
}

func TestBuildChecksLaterDimensionBeforeValues(t *testing.T) {
	c := config()
	c.Partitions = 2
	vectors := []Vector{
		{ID: "first", Values: []float64{1}},
		{ID: "second", Values: []float64{math.NaN(), 1}},
	}
	_, err := Build(vectors, c)
	if err == nil || !strings.Contains(err.Error(), "wrong vector dimension") {
		t.Fatalf("heterogeneous dimension did not fail before scalar scan: %v", err)
	}
}

func TestBuildAndArtifactRejectInvalidUTF8IDs(t *testing.T) {
	vectors := fixture()
	vectors[0].ID = string([]byte{0xff})
	if _, err := Build(vectors, config()); err == nil {
		t.Fatal("Build accepted an invalid UTF-8 vector ID")
	}
	artifact, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	artifact.IDs[0] = string([]byte{0xff})
	if _, err := CanonicalJSON(artifact); err == nil {
		t.Fatal("CanonicalJSON accepted an artifact with an invalid UTF-8 ID")
	}
	validArtifact, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := CanonicalJSON(validArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeArtifact(canonical, len(canonical)); err != nil {
		t.Fatalf("strict decode rejected a valid canonical artifact: %v", err)
	}
}

func TestIdentityStringsRejectInvalidUTF8BeforeBackendEffects(t *testing.T) {
	invalid := string([]byte{0xff})
	for _, tc := range []struct {
		name    string
		source  Source
		backend identityPartitioner
	}{
		{name: "source ID", source: Source{SourceID: invalid}, backend: identityPartitioner{name: "backend", license: "license"}},
		{name: "backend name", source: Source{SourceID: "source"}, backend: identityPartitioner{name: invalid, license: "license"}},
		{name: "backend license", source: Source{SourceID: "source"}, backend: identityPartitioner{name: "backend", license: invalid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			tc.backend.called = &called
			_, err := BuildWithPartitioner(fixture(), config(), tc.source, tc.backend)
			if err == nil {
				t.Fatal("invalid UTF-8 identity accepted")
			}
			if called {
				t.Fatal("invalid UTF-8 identity reached backend")
			}
		})
	}
}

func TestArtifactIdentityStringsRejectInvalidUTF8AndDecodeStrictly(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	invalid := string([]byte{0xff})
	for _, tc := range []struct {
		name   string
		valid  string
		mutate func(*Artifact)
	}{
		{name: "source ID", valid: a.Source.SourceID, mutate: func(x *Artifact) { x.Source.SourceID = invalid }},
		{name: "backend name", valid: a.Backend, mutate: func(x *Artifact) { x.Backend = invalid }},
		{name: "backend license", valid: a.BackendLicense, mutate: func(x *Artifact) { x.BackendLicense = invalid }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forged := a
			tc.mutate(&forged)
			if err := ValidateArtifact(forged); err == nil {
				t.Fatal("ValidateArtifact accepted invalid UTF-8 identity")
			}
			if _, err := CanonicalJSON(forged); err == nil {
				t.Fatal("CanonicalJSON accepted invalid UTF-8 identity")
			}
			invalidRaw := bytes.Replace(raw, []byte("\""+tc.valid+"\""), []byte{'"', 0xff, '"'}, 1)
			if bytes.Equal(invalidRaw, raw) {
				t.Fatal("test did not forge identity bytes")
			}
			if _, err := DecodeArtifact(invalidRaw, len(invalidRaw)); err == nil {
				t.Fatal("DecodeArtifact accepted invalid UTF-8 identity bytes")
			}
		})
	}
	invalidExpected := a.Source
	invalidExpected.SourceID = invalid
	if _, err := DecodeArtifactForSource(raw, len(raw), invalidExpected); err == nil {
		t.Fatal("DecodeArtifactForSource accepted invalid expected source ID")
	}
}

func TestUnicodeIdentityStringsRoundTripCanonically(t *testing.T) {
	canonical, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	sourceID, backendName, backendLicense := "source-東京", "backend-雪", "license-λ"
	a, err := BuildWithPartitioner(fixture(), config(), Source{SourceID: sourceID}, identityPartitioner{name: backendName, license: backendLicense, assignment: canonical.Assignment})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeArtifact(raw, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	again, err := CanonicalJSON(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) || decoded.Source.SourceID != sourceID || decoded.Backend != backendName || decoded.BackendLicense != backendLicense {
		t.Fatalf("unicode identities did not round trip canonically: decoded=%+v", decoded)
	}
}

func TestInputShapePreflightCountsTopLevelPivotWork(t *testing.T) {
	c := DefaultConfig()
	c.Partitions = 1
	c.Repetitions = 1
	c.Pivots = maxPivots
	c.MaxLeafBucket = 2
	c.Degree = 1
	if err := ValidateInputShape(c, 300_000, 64); err != nil {
		t.Fatalf("pivot-work boundary below cap rejected: %v", err)
	}
	if err := ValidateInputShape(c, 310_000, 64); err == nil || !strings.Contains(err.Error(), "scalar-work") {
		t.Fatalf("pivot-work shape error=%v; expected scalar-work rejection", err)
	}
}

func TestInputShapePreflightCountsTopLevelOverlapLeafWork(t *testing.T) {
	c := DefaultConfig()
	c.Partitions = 1
	c.Repetitions = 1
	c.Pivots = 2
	c.MaxLeafBucket = 65_536
	c.Degree = 1
	if err := ValidateInputShape(c, 150_000, 1); err != nil {
		t.Fatalf("overlap-work shape below cap rejected: %v", err)
	}
	if err := ValidateInputShape(c, 300_000, 1); err == nil || !strings.Contains(err.Error(), "scalar-work") {
		t.Fatalf("overlap-work shape error=%v; expected scalar-work rejection", err)
	}
}

func TestReferenceInputShapeSharesReferencePartitionWorkGate(t *testing.T) {
	c := DefaultConfig()
	c.Partitions = 238
	c.Repetitions = 1
	c.Pivots = 2
	c.MaxLeafBucket = 2
	c.Degree = 1
	const vectors = 1_000_000
	if !partitionWorkExceeded(vectors, c.Partitions, c.Degree) {
		t.Fatal("test shape must exceed the reference partition work cap")
	}
	if err := ValidateInputShape(c, vectors, 1); err != nil {
		t.Fatalf("generic shape preflight unexpectedly rejected reference-only shape: %v", err)
	}
	if err := ValidateReferenceInputShape(c, vectors, 1); err == nil || !strings.Contains(err.Error(), "partition work") {
		t.Fatalf("reference shape error=%v; expected partition-work rejection", err)
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
	a.Graph.Neighbors[0] = nil
	if e := ValidateArtifact(a); e == nil {
		t.Fatal("null graph row accepted")
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

func TestZeroDegreeGraphRowsAreCanonicalEmptyArrays(t *testing.T) {
	c := config()
	c.Partitions, c.MaxVectors, c.MaxEdges = 1, 1, 4
	isolate, err := Build([]Vector{{ID: "only", Values: []float64{1, 0}}}, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(isolate.Graph.Neighbors) != 1 || isolate.Graph.Neighbors[0] == nil || len(isolate.Graph.Neighbors[0]) != 0 {
		t.Fatalf("isolated build emitted non-canonical graph: %#v", isolate.Graph.Neighbors)
	}

	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	for i := range a.Graph.Neighbors {
		a.Graph.Neighbors[i] = make([]int, 0)
	}
	a.Metrics = metrics(a)
	if err := ValidateArtifact(a); err != nil {
		t.Fatalf("all-empty graph rejected: %v", err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`null`)) {
		t.Fatalf("canonical all-empty graph contains null: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"neighbors":[[],`)) {
		t.Fatalf("canonical all-empty graph does not use arrays: %s", raw)
	}
	roundTrip, err := DecodeArtifact(raw, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip.Graph, a.Graph) {
		t.Fatalf("canonical graph round trip changed rows: %#v", roundTrip.Graph.Neighbors)
	}
	digest, err := Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	roundTripDigest, err := Digest(roundTrip)
	if err != nil || digest != roundTripDigest {
		t.Fatalf("canonical digest changed after round trip: %q %q %v", digest, roundTripDigest, err)
	}
	if _, err := DecodeArtifactForRequest(raw, len(raw), a); err != nil {
		t.Fatalf("exact request binding rejected canonical empty rows: %v", err)
	}
	forged := bytes.Replace(raw, []byte(`"neighbors":[[]`), []byte(`"neighbors":[null`), 1)
	if bytes.Equal(forged, raw) {
		t.Fatal("failed to forge null graph row")
	}
	if _, err := DecodeArtifact(forged, len(forged)); err == nil || !strings.Contains(err.Error(), "neighbor row must be an array") {
		t.Fatalf("null graph row was normalized or accepted: %v", err)
	}
	if _, err := DecodeArtifactForRequest(forged, len(forged), a); err == nil || !strings.Contains(err.Error(), "neighbor row must be an array") {
		t.Fatalf("request binding normalized null graph row: %v", err)
	}
}

func TestValidateInputReportsSpecificIDFailures(t *testing.T) {
	chunk := strings.Repeat("x", maxIDBytes)
	aggregate := make([]Vector, maxTotalIDBytes/maxIDBytes+1)
	for i := range aggregate {
		aggregate[i] = Vector{ID: chunk, Values: []float64{1}}
	}
	cases := []struct {
		name string
		v    []Vector
		want string
	}{
		{name: "empty", v: []Vector{{ID: "", Values: []float64{1}}}, want: "empty vector ID"},
		{name: "invalid UTF-8", v: []Vector{{ID: string([]byte{0xff}), Values: []float64{1}}}, want: "invalid UTF-8 vector ID"},
		{name: "per-ID cap", v: []Vector{{ID: strings.Repeat("x", maxIDBytes+1), Values: []float64{1}}}, want: "vector ID exceeds per-ID byte cap"},
		{name: "aggregate cap", v: aggregate, want: "vector ID aggregate bytes exceed cap"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateInput(tc.v, 1)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("validateInput error=%v want %q", err, tc.want)
			}
		})
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
func TestGraphConnectsExactDuplicateClassAcrossLeaves(t *testing.T) {
	v := make([]Vector, 16)
	for i := range v {
		values := []float64{0, 1}
		if i == 0 || i == 15 {
			values = []float64{1, 0}
		}
		v[i] = Vector{ID: fmt.Sprintf("v-%02d", i), Values: values}
	}
	c := config()
	c.Partitions, c.Repetitions, c.Pivots, c.MaxLeafBucket, c.Degree = 2, 1, 2, 2, 2
	c.MaxEdges = len(v) * c.Degree
	a, err := Build(v, c)
	if err != nil {
		t.Fatal(err)
	}
	contains := func(values []int, want int) bool {
		for _, value := range values {
			if value == want {
				return true
			}
		}
		return false
	}
	if !contains(a.Graph.Neighbors[0], 15) || !contains(a.Graph.Neighbors[15], 0) {
		t.Fatalf("duplicate endpoints not linked: first=%v last=%v", a.Graph.Neighbors[0], a.Graph.Neighbors[15])
	}
}

func TestGraphConnectsDuplicateClassLargerThanDegree(t *testing.T) {
	const duplicateCount = 20
	v := make([]Vector, duplicateCount*2)
	for i := 0; i < duplicateCount; i++ {
		v[i] = Vector{ID: fmt.Sprintf("a-duplicate-%02d", i), Values: []float64{1, 0, 0}}
		v[duplicateCount+i] = Vector{ID: fmt.Sprintf("z-distractor-%02d", i), Values: []float64{0, 1, float64(i + 1)}}
	}
	for _, degree := range []int{1, 4} {
		t.Run(fmt.Sprintf("degree-%d", degree), func(t *testing.T) {
			c := config()
			c.Partitions, c.Repetitions, c.Pivots, c.MaxLeafBucket, c.Degree = 4, 2, 3, 2, degree
			c.MaxVectors, c.MaxEdges = len(v), len(v)*c.Degree*c.Repetitions
			a, err := Build(v, c)
			if err != nil {
				t.Fatal(err)
			}
			seen := make([]bool, duplicateCount)
			queue := []int{0}
			seen[0] = true
			for len(queue) > 0 {
				ordinal := queue[0]
				queue = queue[1:]
				if len(a.Graph.Neighbors[ordinal]) > c.Degree {
					t.Fatalf("duplicate ordinal %d exceeds degree: neighbors=%v", ordinal, a.Graph.Neighbors[ordinal])
				}
				for _, neighbor := range a.Graph.Neighbors[ordinal] {
					if neighbor < duplicateCount && !seen[neighbor] {
						seen[neighbor] = true
						queue = append(queue, neighbor)
					}
				}
			}
			for ordinal, reachable := range seen {
				if !reachable {
					t.Fatalf("duplicate class larger than degree is disconnected at ordinal %d", ordinal)
				}
			}
		})
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
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("TMPDIR", root)
	t.Setenv("TREE_DB_START_MARKER", marker)
	request, input := externalBackendRequest(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	_, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", ": > \"$TREE_DB_START_MARKER\"; sleep 1"}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: 1024}, request)
	if err == nil {
		t.Fatal("cancelled backend accepted")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled context started backend: %v", err)
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
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("TMPDIR", root)
	t.Setenv("TREE_DB_START_MARKER", marker)
	request, input := externalBackendRequest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", ": > \"$TREE_DB_START_MARKER\"; printf '{' > \"$1\"; sleep 1"}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: 1024}, request)
		done <- err
	}()
	if err := waitForExternalBackendStartMarkerV1(ctx, marker, done); err != nil {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal(err)
	}
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("started backend did not return after cancellation")
	}
	if err == nil {
		t.Fatal("started backend cancellation accepted")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("cancellation test backend did not start: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("backend temporary directory leaked after start: %v", entries)
	}
}

func waitForExternalBackendStartMarkerV1(ctx context.Context, marker string, done <-chan error) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(marker); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat external backend start marker: %w", err)
		}
		select {
		case err := <-done:
			if err == nil {
				return errors.New("external backend exited successfully before start marker")
			}
			return fmt.Errorf("external backend returned before start marker: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("waiting for external backend start marker: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestExternalBackendDeadlineKillsPipeHoldingDescendant(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("TMPDIR", root)
	t.Setenv("TREE_DB_START_MARKER", marker)
	request, input := externalBackendRequest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", ": > \"$TREE_DB_START_MARKER\"; printf '{' > \"$1\"; (sleep 5) & wait"}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: 1024}, request)
	if err == nil {
		t.Fatal("pipe-holding descendant accepted")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("deadline test backend did not start: %v", err)
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
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("TMPDIR", root)
	t.Setenv("TREE_DB_START_MARKER", marker)
	// The shell exits successfully immediately, leaving sleep with the command's
	// inherited stderr pipe. CommandContext does not invoke Cancel in this case;
	// post-Wait process-group cleanup must do so.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, input := externalBackendRequest(t)
	started := time.Now()
	_, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", ": > \"$TREE_DB_START_MARKER\"; printf '{' > \"$1\"; (sleep 5) & exit 0"}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: 1024}, request)
	if err == nil {
		t.Fatal("root-exited pipe-holding descendant accepted")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("root-exit test backend did not start: %v", err)
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
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("TREE_DB_START_MARKER", marker)
	request, input := externalBackendRequest(t)
	if _, err := RunExternalJSONForRequestWithLimits(context.Background(), []string{"sh", "-c", ": > \"$TREE_DB_START_MARKER\""}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: 1024}, request); err == nil || !strings.Contains(err.Error(), "requires context deadline") {
		t.Fatal("deadline-less backend accepted")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deadline-less request started backend: %v", err)
	}
}

func externalBackendRequest(t *testing.T) (Artifact, []byte) {
	t.Helper()
	request, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	input, err := CanonicalJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	return request, input
}

func TestExternalBackendSeparatesInputAndOutputCaps(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	input := raw
	t.Setenv("TREE_DB_TEST_RESPONSE", string(raw))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", "printf '%s' \"$TREE_DB_TEST_RESPONSE\" > \"$1\""}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: len(raw)}, a)
	if err != nil {
		t.Fatalf("canonical requested artifact rejected: %v", err)
	}
	if !reflect.DeepEqual(got, a) {
		t.Fatal("external backend response changed")
	}
}

func TestExternalBackendRejectsInputCapBeforeExecution(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = RunExternalJSONForRequestWithLimits(ctx, []string{"definitely-not-an-executable"}, []byte("{}"), ExternalJSONLimits{MaxInput: 1, MaxOutput: 1024}, a)
	if err == nil || !strings.Contains(err.Error(), "input exceeds cap") {
		t.Fatalf("over-input cap reached backend or returned wrong error: %v", err)
	}
}

func TestExternalRequestBoundAccountsForEscapedIdentities(t *testing.T) {
	bound, ok := externalJSONByteBound(4, 0, 1)
	if !ok || bound < 6*4 {
		t.Fatalf("escaped identity bound=%d ok=%v", bound, ok)
	}
	if maxExternalJSONBytes < 6*maxTotalIDBytes {
		t.Fatalf("production request cap=%d undercharges escaped IDs", maxExternalJSONBytes)
	}
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	// Six control-byte IDs require six escaped bytes each in JSON. The bounded
	// request reaches the backend at its explicit cap rather than being rejected
	// by the global request ceiling.
	request := a
	request.IDs = append([]string(nil), a.IDs...)
	for i := range request.IDs {
		request.IDs[i] = string([]byte{0, byte('a' + i)})
	}
	request.Metrics = metrics(request)
	input, err := CanonicalJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(input) > bound {
		t.Fatalf("escaped request length=%d exceeds modeled bound=%d", len(input), bound)
	}
	t.Setenv("TREE_DB_TEST_RESPONSE", string(input))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", "printf '%s' \"$TREE_DB_TEST_RESPONSE\" > \"$1\""}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: len(input)}, request); err != nil {
		t.Fatalf("escaped bounded input rejected before backend: %v", err)
	}
	_, err = RunExternalJSONForRequestWithLimits(ctx, []string{"definitely-not-an-executable"}, input, ExternalJSONLimits{MaxInput: len(input) - 1, MaxOutput: len(input)}, request)
	if err == nil || !strings.Contains(err.Error(), "input exceeds cap") {
		t.Fatalf("one-over input cap reached backend or returned wrong error: %v", err)
	}
}

func TestExternalBackendStillRejectsOutputOverflow(t *testing.T) {
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TREE_DB_TEST_RESPONSE", string(raw))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = RunExternalJSONForRequestWithLimits(ctx, []string{"sh", "-c", "printf '%s' \"$TREE_DB_TEST_RESPONSE\" > \"$1\""}, raw, ExternalJSONLimits{MaxInput: len(raw), MaxOutput: len(raw) - 1}, a)
	if err == nil || !strings.Contains(err.Error(), "output exceeds cap") {
		t.Fatalf("output overflow accepted or wrong error: %v", err)
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
	request, input := externalBackendRequest(t)
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("TREE_DB_START_MARKER", marker)
	if _, err := RunExternalJSONForSourceWithLimits(ctx, []string{"sh", "-c", ": > \"$TREE_DB_START_MARKER\""}, input, ExternalJSONLimits{MaxInput: len(input), MaxOutput: 1024}, request.Source); err == nil || !strings.Contains(err.Error(), "requires requested artifact binding") {
		t.Fatalf("source-only backend did not fail closed: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source-only adapter started backend: %v", err)
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

func TestDecodeArtifactForRequestBindsGraphConfigAndIDs(t *testing.T) {
	request, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	assertRejected := func(t *testing.T, response Artifact) {
		t.Helper()
		response.Metrics = metrics(response)
		raw, err := CanonicalJSON(response)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeArtifactForRequest(raw, len(raw), request); err == nil || !strings.Contains(err.Error(), "requested graph/configuration") {
			t.Fatalf("forged requested binding accepted: %v", err)
		}
	}
	t.Run("IDs", func(t *testing.T) {
		response := request
		response.IDs = append([]string(nil), request.IDs...)
		response.IDs[0] = response.IDs[0] + "-changed"
		assertRejected(t, response)
	})
	t.Run("config", func(t *testing.T) {
		response := request
		response.Config.Seed++
		assertRejected(t, response)
	})
	t.Run("graph", func(t *testing.T) {
		response := request
		response.Graph = emptyGraph(len(request.Graph.Neighbors))
		assertRejected(t, response)
	})
	// Assignment is the intended backend result. A different valid assignment
	// remains supported as long as it is self-consistent and the request graph,
	// IDs, config, and corpus source remain exact.
	response := request
	response.Assignment = append([]int(nil), request.Assignment...)
	left, right := -1, -1
	for i, p := range response.Assignment {
		if p == 0 && left < 0 {
			left = i
		}
		if p == 1 && right < 0 {
			right = i
		}
	}
	if left < 0 || right < 0 {
		t.Fatal("fixture did not cover both partitions")
	}
	response.Assignment[left], response.Assignment[right] = response.Assignment[right], response.Assignment[left]
	response.Metrics = metrics(response)
	raw, err := CanonicalJSON(response)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeArtifactForRequest(raw, len(raw), request); err != nil || !reflect.DeepEqual(got.Assignment, response.Assignment) {
		t.Fatalf("valid custom backend assignment rejected: artifact=%+v err=%v", got, err)
	}
}

func TestExternalBackendRequestMustBeExactCanonicalArtifact(t *testing.T) {
	request, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := CanonicalJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = RunExternalJSONForRequestWithLimits(ctx, []string{"definitely-not-an-executable"}, append(raw, '\n'), ExternalJSONLimits{MaxInput: len(raw) + 1, MaxOutput: len(raw)}, request)
	if err == nil || !strings.Contains(err.Error(), "input does not match requested artifact") {
		t.Fatalf("non-canonical or mismatched request reached backend: %v", err)
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

type identityPartitioner struct {
	name, license string
	assignment    []int
	called        *bool
}

func (p identityPartitioner) Name() string    { return p.name }
func (p identityPartitioner) License() string { return p.license }
func (p identityPartitioner) Partition(Graph, int, int) ([]int, error) {
	if p.called != nil {
		*p.called = true
	}
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

func TestRepartitionArtifactReusesCanonicalGraph(t *testing.T) {
	in, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	out, err := RepartitionArtifact(in, 2, ReferencePartitioner{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.Graph, out.Graph) || !reflect.DeepEqual(in.IDs, out.IDs) || in.Source != out.Source || out.Config.Partitions != 2 {
		t.Fatalf("repartition changed frozen graph identity: in=%+v out=%+v", in, out)
	}
	if err := ValidateArtifact(out); err != nil {
		t.Fatal(err)
	}
}

func TestRepartitionArtifactFailsClosedAfterMutatingBackend(t *testing.T) {
	in, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	before := in
	_, err = RepartitionArtifact(in, 2, mutatingPartitioner{err: fmt.Errorf("backend failure")})
	if err == nil {
		t.Fatal("accepted mutating backend failure")
	}
	if !reflect.DeepEqual(in, before) {
		t.Fatal("mutating backend changed frozen input artifact")
	}
}

func TestCloneGraphBoundedUsesIndependentExactBacking(t *testing.T) {
	original := Graph{Neighbors: [][]int{{1, 2}, {}, {0}}}
	clone, err := cloneGraphBounded(original, 3)
	if err != nil {
		t.Fatal(err)
	}
	clone.Neighbors[0][0] = 99
	if original.Neighbors[0][0] != 1 {
		t.Fatalf("backend graph clone aliases canonical graph: %v", original)
	}
	if clone.Neighbors[1] == nil {
		t.Fatal("backend graph clone changed an empty row to null")
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

func TestSymmetricConfigFailsBeforeInputOrBackendWork(t *testing.T) {
	c := config()
	c.Symmetric = true
	if err := ValidateConfig(c); err == nil || err.Error() != "symmetric graph policy is not supported" {
		t.Fatalf("ValidateConfig error=%v", err)
	}
	called := false
	poison := []Vector{{ID: string([]byte{0xff}), Values: []float64{math.NaN()}}}
	_, err := BuildWithPartitioner(poison, c, Source{SourceID: "symmetric-preflight"}, identityPartitioner{name: "poison", license: "test", called: &called})
	if err == nil || err.Error() != "symmetric graph policy is not supported" {
		t.Fatalf("symmetric build did not fail in config preflight: %v", err)
	}
	if called {
		t.Fatal("symmetric config reached partition backend")
	}
	if _, err := Build(fixture(), config()); err != nil {
		t.Fatalf("directed build regressed: %v", err)
	}
}

func TestNegativeMaxPartitionWorkFailsConfigValidation(t *testing.T) {
	c := config()
	c.MaxPartitionWork = -1
	if err := ValidateConfig(c); err == nil {
		t.Fatal("negative MaxPartitionWork accepted")
	}
	c = config()
	c.MaxDistanceWork = -1
	if err := ValidateConfig(c); err == nil {
		t.Fatal("negative MaxDistanceWork accepted")
	}
	a, err := Build(fixture(), config())
	if err != nil {
		t.Fatal(err)
	}
	a.Config.MaxDistanceWork = -1
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("artifact with negative MaxDistanceWork accepted")
	}
	a.Config = config()
	a.Config.MaxPartitionWork = -1
	if err := ValidateArtifact(a); err == nil {
		t.Fatal("artifact with negative MaxPartitionWork accepted")
	}
}
func TestReferencePartitionerCoversEveryPartition(t *testing.T) {
	g := emptyGraph(8)
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
	g := emptyGraph(16_384)
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
	g := emptyGraph(maxVectors + 1)
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
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: Source{SourceID: "test", Checksum: strings.Repeat("0", 64), Vectors: 2, Dimensions: 1, Metric: "cosine"}, Config: c, IDs: []string{"a" + chunk, "b" + chunk}, Graph: emptyGraph(2), Assignment: []int{0, 1}}
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
	a := Artifact{SchemaVersion: SchemaVersion, Backend: "test", BackendLicense: "test", Source: Source{SourceID: "test", Checksum: strings.Repeat("0", 64), Vectors: n, Dimensions: 1, Metric: "cosine"}, Config: c, IDs: ids, Graph: emptyGraph(n), Assignment: assignment}
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
