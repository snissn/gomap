package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := RunExternalJSON(ctx, []string{"sh", "-c", "sleep 1"}, []byte("{}"), 1024)
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
