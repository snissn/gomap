package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

func canonicalLiveTestNodesV1(ids []string, vectors [][]float32) []vectorIndexNode {
	nodes := make([]vectorIndexNode, len(ids))
	for i, id := range ids {
		var norm float64
		for _, v := range vectors[i] {
			norm += float64(v) * float64(v)
		}
		nodes[i] = vectorIndexNode{documentID: []byte(id), vector: vectors[i], normSquared: norm, cachedInvNorm: 1 / math.Sqrt(norm), neighbors: make([][]vectorIndexNeighbor, 1)}
		for j := range ids {
			if i != j {
				nodes[i].neighbors[0] = append(nodes[i].neighbors[0], vectorIndexNeighbor{nodeID: uint32(j)})
			}
		}
	}
	return nodes
}

func canonicalLiveTestPinV1(base, delta []vectorIndexNode, dimensions int) *VectorIndexPartitionLiveSearchPinV1 {
	view := &vectorIndexSearchView{nodes: base, deltaNodes: delta, metric: VectorMetricCosine, dimensions: dimensions, m: 16, efSearch: 32, entry: 0, maxLevel: 0, deltaEntry: 0, deltaMaxLevel: 0, liveDocs: len(base) + len(delta), deltaLiveDocs: len(delta), sourceDocumentRootsValid: true}
	return &VectorIndexPartitionLiveSearchPinV1{domains: map[uint32]vectorIndexPartitionLiveDomainPinV1{0: {view: view, maxStableIDBytes: 16}}}
}

func TestVectorIndexPartitionLiveCanonicalQueryDimensionsV1(t *testing.T) {
	pin := canonicalLiveTestPinV1(canonicalLiveTestNodesV1([]string{"a"}, [][]float32{{1, 0}}), nil, 2)
	opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 16}
	_, scratch, err := pin.DomainSearchPreflightV1(0, opts)
	if err != nil || scratch != 64+2*4+16 {
		t.Fatalf("scratch=%d err=%v", scratch, err)
	}
	query := make([]float32, 1<<18)
	for _, value := range []float32{1, float32(math.NaN())} {
		query[0] = value
		results, metrics, err := pin.SearchDomainV1(t.Context(), 0, query, opts)
		// Even an invalid norm must be rejected for shape before normalization.
		if err == nil || err.Error() != "collections: vector query has dimension 262144, want 2" || results != nil || metrics != (VectorPartitionSearchMetricsV1{}) {
			t.Fatalf("results=%v metrics=%+v err=%v", results, metrics, err)
		}
	}
}

func TestVectorIndexPartitionLiveCanonicalScoresAndCutsV1(t *testing.T) {
	retained := make([]float32, 128)
	retained[0], retained[1] = -.43388373, .90096885
	a := canonicalLiveTestNodesV1([]string{"a"}, [][]float32{{1, .0002}})
	z := canonicalLiveTestNodesV1([]string{"z"}, [][]float32{{1, .0001}})
	ties := canonicalLiveTestNodesV1([]string{"z", "a"}, [][]float32{{1, .0001}, {1, .0002}})
	for _, tc := range []struct {
		name        string
		base, delta []vectorIndexNode
		query       []float32
		id          string
		bits        uint32
	}{
		{"retained", canonicalLiveTestNodesV1([]string{"doc-01"}, [][]float32{retained}), nil, retained, "doc-01", 0x3f7fffff},
		{"base_cut", ties, nil, []float32{1, 0}, "a", 0x3f800000},
		{"delta_cut", nil, ties, []float32{1, 0}, "a", 0x3f800000},
		{"base_delta_tie", z, a, []float32{1, 0}, "a", 0x3f800000},
		{"delta_base_tie", a, z, []float32{1, 0}, "a", 0x3f800000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pin := canonicalLiveTestPinV1(tc.base, tc.delta, len(tc.query))
			opts := VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8}
			results, metrics, err := pin.SearchDomainV1(t.Context(), 0, tc.query, opts)
			if err != nil || len(results) != 1 || results[0].ID != tc.id || math.Float32bits(results[0].Score) != tc.bits {
				t.Fatalf("results=%+v metrics=%+v err=%v want %s score bits=%08x", results, metrics, err, tc.id, tc.bits)
			}
			opts.MaxScoreCalls = int(metrics.ScoreCalls)
			bounded, boundedMetrics, err := pin.SearchDomainV1(t.Context(), 0, tc.query, opts)
			if err != nil || len(bounded) != 1 || bounded[0] != results[0] || boundedMetrics != metrics {
				t.Fatalf("exact budget results=%+v metrics=%+v err=%v", bounded, boundedMetrics, err)
			}
			opts.MaxScoreCalls--
			if out, _, err := pin.SearchDomainV1(t.Context(), 0, tc.query, opts); !errors.Is(err, ErrVectorPartitionSearchUnavailable) || out != nil {
				t.Fatalf("under budget results=%v err=%v", out, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if out, _, err := pin.SearchDomainV1(ctx, 0, tc.query, opts); !errors.Is(err, context.Canceled) || out != nil {
				t.Fatalf("cancelled results=%v err=%v", out, err)
			}
		})
	}
	// The opt-in partition scorer must not change legacy graph-only scoring.
	pin := canonicalLiveTestPinV1(ties, nil, 2)
	var buffer VectorIndexSearchBuffer
	legacy, err := pin.domains[0].view.searchGraphOnlyWithScoreBudget([]float32{1, 0}, 1, 8, 0, &buffer)
	if err != nil || len(legacy) != 1 || string(legacy[0].ID) != "z" {
		t.Fatalf("legacy results=%v err=%v", legacy, err)
	}
}

func TestVectorIndexPartitionLiveCanonicalPinnedRevisionV1(t *testing.T) {
	vector := []float32{-.43388373, .90096885}
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8})
	if err != nil {
		t.Fatal(err)
	}
	manifest := VectorPartitionManifestV1{IndexName: "embedding", IndexDefinitionDigest: "definition", SourceGeneration: 3, SourceChecksum: 4, SourceSchemaHash: 5, SourceRowCount: 1, Generation: 7, DomainCount: 1, PartitionCount: 1, DomainPacks: []VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}}
	if err := idx.bindVectorPartitionLiveV1(manifest, manifest.SourceGeneration, []vectorPartitionLiveRepresentativeV1{{domain: 0, vector: vector}}); err != nil {
		t.Fatal(err)
	}
	mutate := func(vector []float32) {
		t.Helper()
		idx.mu.Lock()
		err := idx.reconcileVectorPartitionMutationLocked([]byte("doc-01"), vector)
		idx.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	mutate(vector)
	oldPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer oldPin.Release()
	mutate([]float32{1, 0})
	newPin, err := idx.acquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer newPin.Release()
	for _, tc := range []struct {
		pin   *VectorIndexPartitionLiveSearchPinV1
		query []float32
		bits  uint32
	}{{oldPin, vector, 0x3f7fffff}, {newPin, []float32{1, 0}, 0x3f800000}} {
		results, _, err := tc.pin.SearchDomainV1(t.Context(), 0, tc.query, VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8})
		if err != nil || len(results) != 1 || math.Float32bits(results[0].Score) != tc.bits {
			t.Fatalf("results=%v err=%v want bits=%08x", results, err, tc.bits)
		}
	}
}

func TestVectorIndexPartitionLiveCanonicalCachedNormV1(t *testing.T) {
	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(i-63) / 64
	}
	scorer, err := NewCanonicalVectorPartitionCosineScorerV1(query)
	if err != nil {
		t.Fatal(err)
	}
	for _, scale := range []float32{1, 1e-30, 1e30} {
		vector := make([]float32, len(query))
		for i := range vector {
			vector[i] = scale * float32(64-i) / 64
		}
		node := vectorIndexNode{vector: vector}
		node.cacheVectorNorms()
		want, err := scorer.ScoreV1(vector)
		if err != nil {
			t.Fatal(err)
		}
		got, err := canonicalVectorPartitionScoreWithInvNormV1(scorer.normalizedQuery, node.vector, float32(node.cachedInvNorm))
		if err != nil || math.Float32bits(got) != math.Float32bits(want) {
			t.Fatalf("scale=%g cached score=%g want=%g err=%v", scale, got, want, err)
		}
	}
	for _, value := range []float32{0, math.SmallestNonzeroFloat32, float32(math.NaN()), float32(math.Inf(1))} {
		vector := make([]float32, len(query))
		vector[0] = value
		node := vectorIndexNode{vector: vector}
		node.cacheVectorNorms()
		if _, err := canonicalVectorPartitionScoreWithInvNormV1(scorer.normalizedQuery, node.vector, float32(node.cachedInvNorm)); !errors.Is(err, ErrVectorPartitionSearchUnavailable) {
			t.Fatalf("invalid norm value=%g err=%v", value, err)
		}
	}
}

func TestVectorIndexPartitionLiveCanonicalDeltaResumeV1(t *testing.T) {
	ids, vectors := make([]string, 50), make([][]float32, 50)
	for i := range ids {
		ids[i] = fmt.Sprintf("z-%02d", i)
		vectors[i] = []float32{1, .0001}
		if i >= 32 {
			ids[i] = fmt.Sprintf("a-%02d", i-32)
			vectors[i] = []float32{1, .0002}
		}
	}
	pin := canonicalLiveTestPinV1(canonicalLiveTestNodesV1(ids[:32], vectors[:32]), canonicalLiveTestNodesV1(ids[32:], vectors[32:]), 2)
	opts := VectorPartitionSearchOptionsV1{TopK: 17, EfSearch: 64}
	results, metrics, err := pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, opts)
	if err != nil || len(results) != 17 {
		t.Fatalf("results=%v metrics=%v err=%v", results, metrics, err)
	}
	for i, result := range results {
		if result.ID != fmt.Sprintf("a-%02d", i) || result.Score != 1 {
			t.Fatalf("rank %d=%v", i, result)
		}
	}
	if metrics.ScoreCalls <= 100 {
		t.Fatalf("score calls=%d must include resumed canonical rerank", metrics.ScoreCalls)
	}
	opts.MaxScoreCalls = int(metrics.ScoreCalls)
	if _, exact, err := pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, opts); err != nil || exact != metrics {
		t.Fatalf("exact metrics=%v err=%v", exact, err)
	}
	opts.MaxScoreCalls--
	if out, _, err := pin.SearchDomainV1(t.Context(), 0, []float32{1, 0}, opts); !errors.Is(err, ErrVectorPartitionSearchUnavailable) || out != nil {
		t.Fatalf("under budget results=%v err=%v", out, err)
	}
}

func BenchmarkVectorIndexPartitionLiveCanonicalSearchV1(b *testing.B) {
	for _, dimensions := range []int{2, 128} {
		b.Run(fmt.Sprintf("dimensions=%d", dimensions), func(b *testing.B) {
			ids, vectors := make([]string, 32), make([][]float32, 32)
			for i := range ids {
				ids[i] = fmt.Sprintf("doc-%02d", i)
				vectors[i] = make([]float32, dimensions)
				vectors[i][0], vectors[i][1] = -.43388373, .90096885+float32(i)*.001
			}
			pin := canonicalLiveTestPinV1(canonicalLiveTestNodesV1(ids[:16], vectors[:16]), canonicalLiveTestNodesV1(ids[16:], vectors[16:]), dimensions)
			query := vectors[0]
			opts := VectorPartitionSearchOptionsV1{TopK: 4, EfSearch: 16}
			var scoreCalls uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, metrics, err := pin.SearchDomainV1(b.Context(), 0, query, opts)
				if err != nil || len(results) != 4 {
					b.Fatalf("results=%v err=%v", results, err)
				}
				scoreCalls += metrics.ScoreCalls
			}
			b.StopTimer()
			b.ReportMetric(float64(scoreCalls)/float64(b.N), "scorecalls/op")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
		})
	}
}
