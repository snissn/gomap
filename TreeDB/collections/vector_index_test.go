package collections

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionVectorIndexSearchReranksCanonicalRows(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"tag":"keep"}`),
			[]byte(`{"embedding":[0,1],"tag":"keep"}`),
			[]byte(`{"embedding":[0.9,0.1],"tag":"keep"}`),
			[]byte(`{"embedding":[0,0.8],"tag":"keep"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      4,
	})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.Strategy != "ann_graph" || trace.CandidatesExamined == 0 || trace.RerankCount == 0 || trace.ReturnedCount != 2 {
		t.Fatalf("unexpected trace: %+v", trace)
	}
	stats := index.Stats()
	if stats.LiveDocs != 4 || stats.DeletedDocs != 0 || stats.Dimensions != 2 || stats.AvgDegree == 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestCollectionVectorIndexSearchDocumentsAreOwned(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts backenddb.Options
	}{
		{name: "inline"},
		{name: "value_log_pointer", opts: backenddb.Options{
			ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.Dir = t.TempDir()
			d, err := backenddb.Open(tc.opts)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()
			col := openVectorIndexTestCollection(t, d)
			docA := []byte(`{"embedding":[1,0],"tag":"alpha"}`)
			docB := []byte(`{"embedding":[0.9,0.1],"tag":"beta"}`)
			if _, err := col.InsertBatch(
				[][]byte{[]byte("a"), []byte("b")},
				[][]byte{docA, docB},
			); err != nil {
				t.Fatalf("insert: %v", err)
			}
			index, err := col.BuildVectorIndex(VectorIndexOptions{
				Name:   "embedding",
				Field:  "embedding",
				Metric: VectorMetricCosine,
				M:      4,
			})
			if err != nil {
				t.Fatalf("build vector index: %v", err)
			}

			results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
			if err != nil {
				t.Fatalf("search vector index: %v", err)
			}
			requireVectorResultIDs(t, results, "a", "b")
			if !bytes.Equal(results[0].Document, docA) || !bytes.Equal(results[1].Document, docB) {
				t.Fatalf("unexpected documents: %q %q", results[0].Document, results[1].Document)
			}
			for i, result := range results {
				if cap(result.DocumentID) != len(result.DocumentID) {
					t.Fatalf("result %d document id cap=%d len=%d, want exact cap", i, cap(result.DocumentID), len(result.DocumentID))
				}
				if cap(result.Document) != len(result.Document) {
					t.Fatalf("result %d document cap=%d len=%d, want exact cap", i, cap(result.Document), len(result.Document))
				}
			}

			results[0].Document[0] = '['
			stored, err := col.Get([]byte("a"))
			if err != nil {
				t.Fatalf("get document: %v", err)
			}
			if !bytes.Equal(stored, docA) {
				t.Fatalf("stored document changed: %q want %q", stored, docA)
			}
			if !bytes.Equal(results[1].Document, docB) {
				t.Fatalf("second result changed: %q want %q", results[1].Document, docB)
			}
		})
	}
}

func TestCollectionVectorIndexSearchKeepsExtraCandidatesThroughAttach(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        4,
		EfSearch: 8,
	})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	otherCol, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open second collection handle: %v", err)
	}
	if deleted, err := otherCol.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete through second handle deleted=%d err=%v", deleted, err)
	}
	if err := otherCol.Flush(); err != nil {
		t.Fatalf("flush second handle: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 2,
		DisableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("search stale vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")
	if trace.ReturnedCount != 2 {
		t.Fatalf("trace returned count=%d want 2: %+v", trace.ReturnedCount, trace)
	}
}

func TestVectorIndexPruneLayerNeighborsUsesDistanceThenDocumentIDForDiverseNeighbors(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("from"), vector: []float32{1, 0}},
		{documentID: []byte("b"), vector: unitVectorAtDegrees(10)},
		{documentID: []byte("a"), vector: unitVectorAtDegrees(-10)},
		{documentID: []byte("far"), vector: []float32{0, 1}},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	got := index.pruneLayerNeighborsLocked(0, []vectorIndexNeighbor{
		{nodeID: 3, distance: mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[3].vector)},
		{nodeID: 1, distance: mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[1].vector)},
		{nodeID: 2, distance: mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[2].vector)},
	}, 2)
	if len(got) != 2 || got[0].nodeID != 2 || got[1].nodeID != 1 {
		t.Fatalf("pruned neighbors=%v want [2 1]", got)
	}
}

func TestVectorIndexLevelForDocumentIDUsesMDependentHNSWDistribution(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}

	const docs = 65536
	var ge1, ge2, ge3 int
	for i := 0; i < docs; i++ {
		level := index.levelForDocumentID([]byte(fmt.Sprintf("doc-%06d", i)))
		if level >= 1 {
			ge1++
		}
		if level >= 2 {
			ge2++
		}
		if level >= 3 {
			ge3++
		}
	}
	if ge1 < docs/20 || ge1 > docs/12 {
		t.Fatalf("level>=1 count=%d, want roughly docs/M=%d", ge1, docs/16)
	}
	if ge2 < docs/400 || ge2 > docs/160 {
		t.Fatalf("level>=2 count=%d, want roughly docs/M^2=%d", ge2, docs/(16*16))
	}
	if ge3 < 1 || ge3 > docs/2000 {
		t.Fatalf("level>=3 count=%d, want roughly docs/M^3=%d", ge3, docs/(16*16*16))
	}
}

func TestVectorIndexSelectLayerNeighborsUsesHNSWDiversity(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      4,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	query := []float32{1, 0}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("query"), vector: query, level: 0},
		{documentID: []byte("a"), vector: unitVectorAtDegrees(5), level: 0},
		{documentID: []byte("redundant"), vector: unitVectorAtDegrees(6), level: 0},
		{documentID: []byte("diverse"), vector: unitVectorAtDegrees(-7), level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	candidates := []vectorIndexCandidate{
		{nodeID: 1, distance: mustExactVectorDistance(t, query, index.nodes[1].vector)},
		{nodeID: 2, distance: mustExactVectorDistance(t, query, index.nodes[2].vector)},
		{nodeID: 3, distance: mustExactVectorDistance(t, query, index.nodes[3].vector)},
	}

	got := index.selectLayerNeighborsLocked(query, vectorNormSquared(query), nil, candidates, 0, 2, 0)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("selected neighbors=%v want diverse [1 3]", got)
	}
}

func TestVectorIndexSelectLayerNeighborsSkipsDiversityForInnerProduct(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricInnerProduct,
		M:      4,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	query := []float32{1, 0}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("query"), vector: query, level: 0},
		{documentID: []byte("best"), vector: []float32{10, 0}, level: 0},
		{documentID: []byte("next"), vector: []float32{9, 0}, level: 0},
		{documentID: []byte("orthogonal"), vector: []float32{0, 1}, level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	candidates := []vectorIndexCandidate{
		{nodeID: 1, distance: -10},
		{nodeID: 2, distance: -9},
		{nodeID: 3, distance: 0},
	}

	got := index.selectLayerNeighborsLocked(query, vectorNormSquared(query), nil, candidates, 0, 2, 0)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("selected neighbors=%v want inner-product top candidates [1 2]", got)
	}
}

func TestVectorIndexPruneLayerNeighborsUsesHNSWDiversity(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      4,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("from"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("a"), vector: unitVectorAtDegrees(5), level: 0},
		{documentID: []byte("redundant"), vector: unitVectorAtDegrees(6), level: 0},
		{documentID: []byte("diverse"), vector: unitVectorAtDegrees(-7), level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	got := index.pruneLayerNeighborsLocked(0, []vectorIndexNeighbor{
		{nodeID: 1, distance: mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[1].vector)},
		{nodeID: 2, distance: mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[2].vector)},
		{nodeID: 3, distance: mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[3].vector)},
	}, 2)
	if len(got) != 2 || got[0].nodeID != 1 || got[1].nodeID != 3 {
		t.Fatalf("pruned neighbors=%v want diverse [1 3]", got)
	}
}

func TestVectorIndexSelectLayerNeighborsBackfillsPrunedCandidates(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      4,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	query := []float32{1, 0}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("query"), vector: query, level: 0},
		{documentID: []byte("a"), vector: unitVectorAtDegrees(5), level: 0},
		{documentID: []byte("b"), vector: unitVectorAtDegrees(6), level: 0},
		{documentID: []byte("c"), vector: unitVectorAtDegrees(7), level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	candidates := []vectorIndexCandidate{
		{nodeID: 1, distance: mustExactVectorDistance(t, query, index.nodes[1].vector)},
		{nodeID: 2, distance: mustExactVectorDistance(t, query, index.nodes[2].vector)},
		{nodeID: 3, distance: mustExactVectorDistance(t, query, index.nodes[3].vector)},
	}

	got := index.selectLayerNeighborsLocked(query, vectorNormSquared(query), nil, candidates, 0, 3, 0)
	if len(got) != 3 {
		t.Fatalf("selected %d neighbors=%v, want backfilled degree 3", len(got), got)
	}
}

func TestColumnVectorGraphConstructionTraceIsOptInAndDoesNotChangeGraphV1(t *testing.T) {
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 2, M: 2, EfConstruction: 4}
	rows := []columnVectorGraphAssetRow{{ID: []byte("a"), Vector: []float32{1, 0}}, {ID: []byte("b"), Vector: []float32{.99, .01}}, {ID: []byte("c"), Vector: []float32{0, 1}}, {ID: []byte("d"), Vector: []float32{-1, 0}}}
	plain := append([]columnVectorGraphAssetRow(nil), rows...)
	traced := append([]columnVectorGraphAssetRow(nil), rows...)
	if err := buildColumnVectorGraphAdjacency(plain, def); err != nil {
		t.Fatal(err)
	}
	trace := &vectorIndexConstructionTraceV1{detailed: true}
	if err := buildColumnVectorGraphAdjacencyWithConstructionTraceV1(traced, def, trace); err != nil {
		t.Fatal(err)
	}
	if len(trace.selections) == 0 {
		t.Fatal("missing opt-in construction selections")
	}
	if len(trace.events) == 0 {
		t.Fatal("missing opt-in construction edge events")
	}
	for _, event := range trace.events {
		if event.From < 0 || event.From >= len(traced) || event.To < 0 || event.To >= len(traced) {
			t.Fatalf("construction event has non-locality ordinal: %+v", event)
		}
	}
	for i := range plain {
		if string(plain[i].ID) != string(traced[i].ID) || !reflect.DeepEqual(plain[i].Adjacency, traced[i].Adjacency) {
			t.Fatalf("trace changed graph row=%d", i)
		}
	}
}

func TestVectorIndexConstructionTraceSamplesZeroCandidateSelectionV1(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      2,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{{documentID: []byte("sampled"), vector: []float32{1, 0}, level: 0}}
	index.nodes[0].cacheVectorNorms()
	candidates := []vectorIndexCandidate{{nodeID: 0, distance: 0}}

	plain := index.selectLayerNeighborsLocked(index.nodes[0].vector, vectorNormSquared(index.nodes[0].vector), nil, append([]vectorIndexCandidate(nil), candidates...), 0, 2, 0)
	trace := &vectorIndexConstructionTraceV1{detailed: true, sampleIDs: map[string]struct{}{"sampled": {}}}
	index.constructionTrace = trace
	traced := index.selectLayerNeighborsLocked(index.nodes[0].vector, vectorNormSquared(index.nodes[0].vector), nil, append([]vectorIndexCandidate(nil), candidates...), 0, 2, 0)
	if !reflect.DeepEqual(plain, traced) {
		t.Fatalf("trace changed zero-candidate selection: plain=%v traced=%v", plain, traced)
	}
	if len(trace.selections) != 1 {
		t.Fatalf("selections=%d want 1", len(trace.selections))
	}
	selection := trace.selections[0]
	if !selection.Sampled || selection.CandidateNodes == nil || len(selection.CandidateNodes) != 0 || selection.Candidates != 0 || selection.Selected != 0 {
		t.Fatalf("zero-candidate sampled selection=%+v", selection)
	}
}

func TestColumnVectorGraphConstructionEdgeTraceReconcilesLocalityGraphV1(t *testing.T) {
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 2, M: 1, EfConstruction: 10}
	rows := make([]columnVectorGraphAssetRow, 10)
	for i := range rows {
		angle := float64(i) * 2 * math.Pi / float64(len(rows))
		rows[i] = columnVectorGraphAssetRow{ID: []byte(fmt.Sprintf("node-%02d", i)), Vector: []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}}
	}
	plain := append([]columnVectorGraphAssetRow(nil), rows...)
	traced := append([]columnVectorGraphAssetRow(nil), rows...)
	trace := &vectorIndexConstructionTraceV1{detailed: true}
	if err := buildColumnVectorGraphAdjacency(plain, def); err != nil {
		t.Fatal(err)
	}
	if err := buildColumnVectorGraphAdjacencyWithConstructionTraceV1(traced, def, trace); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plain, traced) {
		t.Fatal("construction trace changed adjacency")
	}
	counts := make(map[string]int)
	origins := make(map[string]int)
	final := make(map[vectorIndexConstructionEdgeKeyV1]struct{})
	for _, event := range trace.events {
		if event.From < 0 || event.From >= len(traced) || event.To < 0 || event.To >= len(traced) || event.InsertionOrdinal < 0 || event.InsertionOrdinal >= len(traced) {
			t.Fatalf("invalid remapped construction event: %+v", event)
		}
		counts[event.Action]++
		origins[event.Origin]++
		if event.Action == "final_survivor" {
			final[vectorIndexConstructionEdgeKeyV1{From: event.From, To: event.To, Layer: event.Layer}] = struct{}{}
		}
	}
	for _, action := range []string{"initial_add", "reciprocal_add", "reciprocal_prune_drop", "final_survivor"} {
		if counts[action] == 0 {
			t.Fatalf("toy graph did not exercise %s: counts=%v", action, counts)
		}
	}
	if trace.pruneKeeps == 0 {
		t.Fatalf("toy graph did not exercise count-only prune keep: lifecycle=%+v", trace.compactLifecycle)
	}
	if origins["diversity_selected"] == 0 || origins["nearest_backfill"] == 0 {
		t.Fatalf("toy graph did not exercise selection origins: origins=%v", origins)
	}
	wantFinal := make(map[vectorIndexConstructionEdgeKeyV1]struct{})
	for from, row := range traced {
		maxLayer, err := columnVectorGraphAdjacencyMaxLayer(row.Adjacency)
		if err != nil {
			t.Fatal(err)
		}
		for layer := 0; layer <= maxLayer; layer++ {
			neighbors, err := columnVectorGraphAdjacencyLayer(row.Adjacency, layer)
			if err != nil {
				t.Fatal(err)
			}
			for _, to := range neighbors {
				wantFinal[vectorIndexConstructionEdgeKeyV1{From: from, To: int(to), Layer: layer}] = struct{}{}
			}
		}
	}
	if !reflect.DeepEqual(final, wantFinal) {
		t.Fatalf("final survivor trace does not exactly reconcile adjacency: got=%v want=%v", final, wantFinal)
	}
	again := append([]columnVectorGraphAssetRow(nil), rows...)
	againTrace := &vectorIndexConstructionTraceV1{detailed: true}
	if err := buildColumnVectorGraphAdjacencyWithConstructionTraceV1(again, def, againTrace); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(trace.selections, againTrace.selections) || !reflect.DeepEqual(trace.events, againTrace.events) {
		t.Fatal("construction trace is not deterministic")
	}
}

func TestVectorIndexSelectDiverseCandidatesKeepsBackfillDistanceSorted(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      2,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	query := []float32{1, 0}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("from"), vector: query, level: 0},
		{documentID: []byte("a"), vector: unitVectorAtDegrees(1), level: 0},
		{documentID: []byte("backfill"), vector: unitVectorAtDegrees(2), level: 0},
		{documentID: []byte("diverse"), vector: unitVectorAtDegrees(-20), level: 0},
		{documentID: []byte("extra"), vector: unitVectorAtDegrees(-21), level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	candidates := []vectorIndexCandidate{
		{nodeID: 1, distance: mustExactVectorDistance(t, query, index.nodes[1].vector)},
		{nodeID: 2, distance: mustExactVectorDistance(t, query, index.nodes[2].vector)},
		{nodeID: 3, distance: mustExactVectorDistance(t, query, index.nodes[3].vector)},
		{nodeID: 4, distance: mustExactVectorDistance(t, query, index.nodes[4].vector)},
	}

	got := index.selectDiverseCandidatesLocked(candidates, 3)
	if len(got) != 3 {
		t.Fatalf("selected %d candidates=%v, want 3", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if index.compareVectorIndexCandidatesByDistanceLocked(got[i-1], got[i]) > 0 {
			t.Fatalf("selected candidates not distance sorted: %v", got)
		}
	}
}

func TestColumnVectorGraphLayer0ConstructionPolicyReservesReciprocalCapacityV1(t *testing.T) {
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 2, M: 2, EfConstruction: 32}
	rows := make([]columnVectorGraphAssetRow, 64)
	for i := range rows {
		// Uneven angular spacing creates both redundant candidates and useful
		// distant alternatives while retaining a deterministic insertion order.
		angle := float64((i*i*17)%997) * 2 * math.Pi / 997
		rows[i] = columnVectorGraphAssetRow{ID: []byte(fmt.Sprintf("policy-%03d", i)), Vector: []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}}
	}
	invalidDef := def
	invalidDef.Metric = VectorMetricInnerProduct
	if err := buildColumnVectorGraphAdjacencyWithConstructionPolicyV1(append([]columnVectorGraphAssetRow(nil), rows...), invalidDef, nil, true, &vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 1}); err == nil {
		t.Fatal("offline layer-0 policy accepted a non-cosine builder")
	}
	if err := buildColumnVectorGraphAdjacencyWithConstructionPolicyV1(append([]columnVectorGraphAssetRow(nil), rows...), def, nil, true, &vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 3}); err == nil {
		t.Fatal("offline layer-0 policy accepted an unknown initial-selection factor")
	}
	build := func(policy *vectorIndexLayer0ConstructionPolicyV1) ([]columnVectorGraphAssetRow, *vectorIndexConstructionTraceV1) {
		t.Helper()
		got := append([]columnVectorGraphAssetRow(nil), rows...)
		trace := &vectorIndexConstructionTraceV1{}
		if err := buildColumnVectorGraphAdjacencyWithConstructionPolicyV1(got, def, trace, true, policy); err != nil {
			t.Fatal(err)
		}
		return got, trace
	}
	control, controlTrace := build(nil)
	explicitControl, explicitControlTrace := build(&vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 2, backfill: true})
	if !reflect.DeepEqual(control, explicitControl) || !reflect.DeepEqual(controlTrace.selections, explicitControlTrace.selections) || !reflect.DeepEqual(controlTrace.events, explicitControlTrace.events) {
		t.Fatal("explicit layer-0 2M/backfill-on policy changed the current construction")
	}

	for _, test := range []struct {
		name         string
		policy       vectorIndexLayer0ConstructionPolicyV1
		wantBackfill bool
		wantReserved bool
	}{
		{"m_off", vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 1, backfill: false}, false, true},
		{"m_on", vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 1, backfill: true}, true, true},
		{"2m_off", vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 2, backfill: false}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, trace := build(&test.policy)
			backfill := 0
			underfilled := false
			for _, selection := range trace.selections {
				if selection.Layer != 0 {
					continue
				}
				limit := def.M * test.policy.initialSelectionFactor
				if selection.Selected > limit {
					t.Fatalf("initial layer-0 selected=%d exceeds limit=%d: %+v", selection.Selected, limit, selection)
				}
				backfill += selection.BackfillSelected
				if selection.Candidates >= limit && selection.Selected < limit {
					underfilled = true
				}
			}
			if test.wantBackfill != (backfill > 0) {
				t.Fatalf("backfill=%d want enabled=%t", backfill, test.wantBackfill)
			}
			if !test.wantBackfill && !underfilled {
				t.Fatal("backfill-off policy did not retain any diversity-underfilled selection")
			}
			maxDegree := 0
			for _, row := range got {
				neighbors, err := columnVectorGraphAdjacencyLayer(row.Adjacency, 0)
				if err != nil {
					t.Fatal(err)
				}
				if len(neighbors) > 2*def.M {
					t.Fatalf("final degree=%d exceeds reciprocal capacity=%d", len(neighbors), 2*def.M)
				}
				maxDegree = max(maxDegree, len(neighbors))
			}
			if test.wantReserved && maxDegree <= def.M {
				t.Fatalf("reciprocal insertion never consumed reserved capacity: max degree=%d", maxDegree)
			}
		})
	}
}

func TestVectorIndexQualityPostfillIsFinalStageAndTraceIndependentV1(t *testing.T) {
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 2, M: 2, EfConstruction: 32}
	rows := make([]columnVectorGraphAssetRow, 64)
	for i := range rows {
		angle := float64((i*i*17)%997) * 2 * math.Pi / 997
		rows[i] = columnVectorGraphAssetRow{ID: []byte(fmt.Sprintf("postfill-%03d", i)), Vector: []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}}
	}
	build := func(trace *vectorIndexConstructionTraceV1) *VectorIndex {
		t.Helper()
		idx, err := newVectorIndex(nil, vectorIndexOptionsFromDefinition(def))
		if err != nil {
			t.Fatal(err)
		}
		idx.constructionTrace = trace
		idx.layer0ConstructionPolicy = &vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 2, qualityPostfill: true}
		idx.mu.Lock()
		defer idx.mu.Unlock()
		for _, row := range rows {
			if err := idx.insertVectorLocked(row.ID, row.Vector); err != nil {
				t.Fatal(err)
			}
		}
		if err := idx.applyQualityPostfillLocked(trace, 2*def.M); err != nil {
			t.Fatal(err)
		}
		return idx
	}
	traced := &vectorIndexConstructionTraceV1{detailed: true}
	withTrace, withoutTrace := build(traced), build(nil)
	if traced.postfillEdges == 0 || traced.compactLifecycle.QualityPostfillAdd[5] != traced.postfillEdges {
		t.Fatalf("quality postfill provenance=%d lifecycle=%+v", traced.postfillEdges, traced.compactLifecycle)
	}
	for node := range withTrace.nodes {
		left, right := withTrace.nodes[node].neighbors[0], withoutTrace.nodes[node].neighbors[0]
		if len(left) != len(right) {
			t.Fatalf("trace changed degree node=%d got=%d want=%d", node, len(left), len(right))
		}
		if len(left) > 2*def.M {
			t.Fatalf("postfill exceeded cap node=%d degree=%d", node, len(left))
		}
		if len(left) != 2*def.M {
			t.Fatalf("postfill left unused capacity node=%d degree=%d want=%d", node, len(left), 2*def.M)
		}
		for i := range left {
			if left[i].nodeID != right[i].nodeID {
				t.Fatalf("trace changed postfill node=%d edge=%d", node, i)
			}
		}
	}
}

func TestVectorIndexRobustPruneRetainsRejectedPoolWithoutTraceV1(t *testing.T) {
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 2, M: 2, EfConstruction: 32}
	build := func(trace *vectorIndexConstructionTraceV1) *VectorIndex {
		t.Helper()
		idx, err := newVectorIndex(nil, vectorIndexOptionsFromDefinition(def))
		if err != nil {
			t.Fatal(err)
		}
		idx.constructionTrace = trace
		idx.layer0ConstructionPolicy = &vectorIndexLayer0ConstructionPolicyV1{initialSelectionFactor: 2, robustPruneRefinement: true}
		idx.mu.Lock()
		defer idx.mu.Unlock()
		for i := 0; i < 32; i++ {
			a := float64((i*i*31)%509) * 2 * math.Pi / 509
			if err := idx.insertVectorLocked([]byte(fmt.Sprintf("robust-%03d", i)), []float32{float32(math.Cos(a)), float32(math.Sin(a))}); err != nil {
				t.Fatal(err)
			}
		}
		if len(idx.qualityPostfillCandidates) == 0 {
			t.Fatal("robust-prune did not retain rejected candidates")
		}
		if err := idx.applyRobustPruneRefinementLocked(trace, 2*def.M); err != nil {
			t.Fatal(err)
		}
		return idx
	}
	traced := &vectorIndexConstructionTraceV1{detailed: true}
	withTrace, withoutTrace := build(traced), build(nil)
	for node := range withTrace.nodes {
		if len(withTrace.nodes[node].neighbors[0]) != 2*def.M || len(withoutTrace.nodes[node].neighbors[0]) != 2*def.M {
			t.Fatalf("robust degree node=%d traced=%d untraced=%d", node, len(withTrace.nodes[node].neighbors[0]), len(withoutTrace.nodes[node].neighbors[0]))
		}
		for edge := range withTrace.nodes[node].neighbors[0] {
			if withTrace.nodes[node].neighbors[0][edge].nodeID != withoutTrace.nodes[node].neighbors[0][edge].nodeID {
				t.Fatalf("trace changed robust edge node=%d edge=%d", node, edge)
			}
		}
	}
	if traced.compactLifecycle.VariantAdd[6]+traced.compactLifecycle.VariantAdd[7] == 0 {
		t.Fatal("robust refinement emitted no provenance")
	}
}

func TestVectorIndexRobustPruneCosineThresholdUsesEuclideanAlphaV1(t *testing.T) {
	// For normalized vectors cosine distance is half squared Euclidean distance.
	// The DiskANN alpha=1.2 boundary is therefore 1.2^2=1.44 here.
	if vectorIndexRobustPruneOccludesV1(1.44, 1, 1.43) {
		t.Fatal("cosine robust-prune threshold used an overly weak alpha")
	}
	if !vectorIndexRobustPruneOccludesV1(1.44, 1, 1.44) {
		t.Fatal("cosine robust-prune threshold rejected Euclidean alpha boundary")
	}
}

func TestVectorIndexFloat32CosineCandidateDistanceFastPathMatchesExact(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("candidate"), vector: []float32{0.25, -0.5, 1.25, 2}, level: 0},
		{documentID: []byte("existing"), vector: []float32{1.5, -0.25, 0.75, 3}, level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	got := index.distanceBetweenFloat32CosineCandidateAndNodeLocked(&index.nodes[0], 1)
	want := mustExactVectorDistance(t, index.nodes[0].vector, index.nodes[1].vector)
	if math.Abs(float64(got-want)) > 1e-6 {
		t.Fatalf("fast candidate distance=%v want %v", got, want)
	}
}

func TestVectorIndexInt8InsertUnchangedVectorNoops(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
		Encoding:   VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.mu.Lock()
	if err := index.insertVectorLocked([]byte("a"), []float32{1, 0}); err != nil {
		index.mu.Unlock()
		t.Fatalf("insert vector: %v", err)
	}
	if err := index.insertVectorLocked([]byte("a"), []float32{1, 0}); err != nil {
		index.mu.Unlock()
		t.Fatalf("insert unchanged vector: %v", err)
	}
	index.mu.Unlock()

	stats := index.Stats()
	if stats.Nodes != 1 || stats.LiveDocs != 1 || stats.DeletedDocs != 0 {
		t.Fatalf("stats=%+v want one unchanged live node", stats)
	}
}

func TestVectorIndexCandidateDiversitySkipsInvalidPairDistance(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      4,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("candidate"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("invalid"), vector: []float32{1, 0, 0}, level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	if !index.vectorIndexCandidateIsDiverseLocked(vectorIndexCandidate{nodeID: 0, distance: 0.1}, []vectorIndexCandidate{{nodeID: 1}}) {
		t.Fatal("invalid pair distance rejected the candidate instead of preserving sentinel fallback behavior")
	}
}

func TestVectorIndexDistanceBetweenNodesFastMatchesStoredCosine(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("left"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("right"), vector: []float32{0.5, 0.5}, level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	fast, ok := index.distanceBetweenNodesFastLocked(0, 1)
	if !ok {
		t.Fatal("fast pair distance path unavailable for valid cosine nodes")
	}
	slow := index.distanceBetweenNodesLocked(0, 1)
	if math.Abs(float64(fast-slow)) > 1e-6 {
		t.Fatalf("fast distance=%v slow=%v", fast, slow)
	}
}

func TestVectorIndexNewNodeCapsEagerNeighborCapacity(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      10_000,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	node := index.newVectorIndexNode([]byte("a"), []float32{1, 0}, 2)
	for layer, neighbors := range node.neighbors {
		if cap(neighbors) > maxVectorIndexEagerNeighborCap {
			t.Fatalf("layer %d eager cap=%d exceeds cap %d", layer, cap(neighbors), maxVectorIndexEagerNeighborCap)
		}
	}
	if got := index.maxNeighborsForLayer(0); got <= maxVectorIndexEagerNeighborCap {
		t.Fatalf("test did not exercise high-M logical limit: %d", got)
	}
}

func TestVectorIndexSelectLayerNeighborsReusesCandidateDistances(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("query"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("slow"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("fast"), vector: []float32{0, 1}, level: 0},
		{documentID: []byte("middle"), vector: []float32{0.5, 0.5}, level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	got := index.selectLayerNeighborsLocked(
		[]float32{1, 0},
		1,
		nil,
		[]vectorIndexCandidate{
			{nodeID: 1, distance: 0.9},
			{nodeID: 2, distance: 0.1},
			{nodeID: 3, distance: 0.2},
		},
		0,
		2,
		0,
	)
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("selected neighbors=%v want [2 3]", got)
	}
}

func TestSortVectorIndexCandidatesByDistanceTailInsert(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("a")},
		{documentID: []byte("b")},
		{documentID: []byte("c")},
	}
	candidates := []vectorIndexCandidate{
		{nodeID: 0, distance: 0.1},
		{nodeID: 2, distance: 0.3},
		{nodeID: 1, distance: 0.2},
	}
	index.sortVectorIndexCandidatesByDistanceLocked(candidates)
	if candidates[0].nodeID != 0 || candidates[1].nodeID != 1 || candidates[2].nodeID != 2 {
		t.Fatalf("tail-insert sort candidates=%+v", candidates)
	}
}

func TestSortVectorIndexCandidatesByDistanceFallbackSort(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("a")},
		{documentID: []byte("b")},
		{documentID: []byte("c")},
	}
	candidates := []vectorIndexCandidate{
		{nodeID: 2, distance: 0.3},
		{nodeID: 1, distance: 0.2},
		{nodeID: 0, distance: 0.1},
	}
	index.sortVectorIndexCandidatesByDistanceLocked(candidates)
	if candidates[0].nodeID != 0 || candidates[1].nodeID != 1 || candidates[2].nodeID != 2 {
		t.Fatalf("fallback sort candidates=%+v", candidates)
	}
}

func TestVectorIndexCandidateDiversityAllowsEqualDistance(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("selected"), vector: []float32{1, 0}},
		{documentID: []byte("candidate"), vector: []float32{0, 1}},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	if !index.vectorIndexCandidateIsDiverseLocked(
		vectorIndexCandidate{nodeID: 1, distance: 1},
		[]vectorIndexCandidate{{nodeID: 0, distance: 0.1}},
	) {
		t.Fatal("equal candidate-to-selected distance was treated as occluded")
	}
}

func unitVectorAtDegrees(degrees float64) []float32 {
	radians := degrees * math.Pi / 180
	return []float32{float32(math.Cos(radians)), float32(math.Sin(radians))}
}

func mustExactVectorDistance(t *testing.T, left, right []float32) float32 {
	t.Helper()
	distance, err := exactVectorDistance(left, right, VectorMetricCosine)
	if err != nil {
		t.Fatalf("exact vector distance: %v", err)
	}
	return distance
}

func TestVectorIndexInsertCachesStoredNorm(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := index.insertVectorLocked([]byte("a"), []float32{3, 4}); err != nil {
		t.Fatalf("insert vector: %v", err)
	}
	if got, want := index.nodes[0].normSquared, float64(25); got != want {
		t.Fatalf("node cached norm=%v want %v", got, want)
	}
	if got, want := index.nodes[0].cachedInvNorm, float32(0.2); got != want {
		t.Fatalf("node cached inverse norm=%v want %v", got, want)
	}
}

func TestVectorIndexInsertCachesInt8StoredNorm(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:     "embedding",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := index.insertVectorLocked([]byte("a"), []float32{3, 4}); err != nil {
		t.Fatalf("insert vector: %v", err)
	}
	node := index.nodes[0]
	if node.normSquared == 0 {
		t.Fatal("node cached norm is zero")
	}
	if got, want := node.normSquared, node.storedNormSquared(); got != want {
		t.Fatalf("node cached norm=%v want dequantized norm %v", got, want)
	}
}

func TestVectorIndexFloat32CosineSpecializationMatchesExactDistance(t *testing.T) {
	query := []float32{0.25, -0.5, 1.25, 2}
	node := vectorIndexNode{documentID: []byte("right"), vector: []float32{1.5, -0.25, 0.75, 3}}
	node.cacheVectorNorms()
	queryNorm := vectorNormSquared(query)
	prepared, err := prepareFloat32CosineQuery(query, queryNorm)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}

	gotQuery, err := vectorDistanceToFloat32NodeCosine(query, queryNorm, &node)
	if err != nil {
		t.Fatalf("specialized query distance: %v", err)
	}
	wantQuery, err := exactVectorDistance(query, node.vector, VectorMetricCosine)
	if err != nil {
		t.Fatalf("exact query distance: %v", err)
	}
	if math.Abs(float64(gotQuery-wantQuery)) > 1e-6 {
		t.Fatalf("specialized query distance=%v want %v", gotQuery, wantQuery)
	}
	gotPrepared, err := vectorDistanceToFloat32NodeCosinePrepared(prepared, &node)
	if err != nil {
		t.Fatalf("prepared query distance: %v", err)
	}
	if math.Abs(float64(gotPrepared-wantQuery)) > 1e-6 {
		t.Fatalf("prepared query distance=%v want %v", gotPrepared, wantQuery)
	}
	gotUnchecked := vectorDistanceToFloat32NodeCosineUnchecked(prepared, &node)
	if math.Abs(float64(gotUnchecked-wantQuery)) > 1e-6 {
		t.Fatalf("unchecked query distance=%v want %v", gotUnchecked, wantQuery)
	}

	left := vectorIndexNode{documentID: []byte("left"), vector: query}
	left.cacheVectorNorms()
	gotBetween, err := vectorDistanceBetweenFloat32NodesCosine(&left, &node)
	if err != nil {
		t.Fatalf("specialized node distance: %v", err)
	}
	wantBetween, err := exactVectorDistance(left.vector, node.vector, VectorMetricCosine)
	if err != nil {
		t.Fatalf("exact node distance: %v", err)
	}
	if math.Abs(float64(gotBetween-wantBetween)) > 1e-6 {
		t.Fatalf("specialized node distance=%v want %v", gotBetween, wantBetween)
	}
}

func TestVectorDistanceBetweenFloat32NodesCosineRejectsMismatchedDimensions(t *testing.T) {
	left := vectorIndexNode{documentID: []byte("left"), vector: []float32{1, 0}}
	right := vectorIndexNode{documentID: []byte("right"), vector: []float32{1, 0, 0}}
	left.cacheVectorNorms()
	right.cacheVectorNorms()

	_, err := vectorDistanceBetweenFloat32NodesCosine(&left, &right)
	if err == nil {
		t.Fatal("distance succeeded, want dimension mismatch error")
	}
	if !strings.Contains(err.Error(), "vector dimensions differ") {
		t.Fatalf("error=%v, want dimension mismatch", err)
	}
}

func TestVectorDistanceFloat32CosineFallsBackForHugeVectors(t *testing.T) {
	huge := float32(1e20)
	query := []float32{huge, huge}
	node := vectorIndexNode{documentID: []byte("right"), vector: []float32{huge, huge}}
	node.cacheVectorNorms()
	queryNorm := vectorNormSquared(query)

	gotQuery, err := vectorDistanceToFloat32NodeCosine(query, queryNorm, &node)
	if err != nil {
		t.Fatalf("query distance: %v", err)
	}
	if math.IsInf(float64(gotQuery), 0) || math.IsNaN(float64(gotQuery)) || math.Abs(float64(gotQuery)) > 1e-3 {
		t.Fatalf("query distance=%v, want finite near zero", gotQuery)
	}

	left := vectorIndexNode{documentID: []byte("left"), vector: query}
	left.cacheVectorNorms()
	gotBetween, err := vectorDistanceBetweenFloat32NodesCosine(&left, &node)
	if err != nil {
		t.Fatalf("node distance: %v", err)
	}
	if math.IsInf(float64(gotBetween), 0) || math.IsNaN(float64(gotBetween)) || math.Abs(float64(gotBetween)) > 1e-3 {
		t.Fatalf("node distance=%v, want finite near zero", gotBetween)
	}
}

func TestPrepareFloat32CosineQueryCachesInverseNorm(t *testing.T) {
	query := []float32{3, 4}
	prepared, err := prepareFloat32CosineQuery(query, -1)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if &prepared.vector[0] != &query[0] {
		t.Fatal("prepared query copied vector")
	}
	if got, want := prepared.invNorm, float32(0.2); got != want {
		t.Fatalf("prepared inverse norm=%v want %v", got, want)
	}
	if _, err := prepareFloat32CosineQuery([]float32{0, 0}, -1); err == nil {
		t.Fatal("prepare zero cosine query succeeded")
	}
}

func TestVectorIndexSearchLayerScratchReusesBuffers(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("a"), vector: []float32{1, 0}, neighbors: [][]vectorIndexNeighbor{{{nodeID: 1}, {nodeID: 2}}}},
		{documentID: []byte("b"), vector: []float32{0.9, 0.1}, neighbors: [][]vectorIndexNeighbor{{{nodeID: 0}, {nodeID: 2}}}},
		{documentID: []byte("c"), vector: []float32{0, 1}, neighbors: [][]vectorIndexNeighbor{{{nodeID: 0}, {nodeID: 1}}}},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	var scratch vectorIndexSearchScratch
	query := []float32{1, 0}
	queryNorm := vectorNormSquared(query)
	prepared, err := prepareFloat32CosineQuery(query, queryNorm)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}

	first := index.searchLayerWithScratchLocked(query, queryNorm, &prepared, 0, 3, 0, &scratch)
	if len(first) != 3 || first[0].nodeID != 0 || first[1].nodeID != 1 || first[2].nodeID != 2 {
		t.Fatalf("first candidates=%v want node IDs [0 1 2]", first)
	}
	if len(scratch.visitedEpochs) != len(index.nodes) || cap(scratch.queue) == 0 || cap(scratch.best) == 0 || cap(scratch.out) == 0 {
		t.Fatalf("scratch was not populated: visited=%d queue_cap=%d best_cap=%d out_cap=%d", len(scratch.visitedEpochs), cap(scratch.queue), cap(scratch.best), cap(scratch.out))
	}
	visited := &scratch.visitedEpochs[0]
	queueCap := cap(scratch.queue)
	bestCap := cap(scratch.best)
	outCap := cap(scratch.out)

	second := index.searchLayerWithScratchLocked(query, queryNorm, &prepared, 0, 3, 0, &scratch)
	if len(second) != len(first) {
		t.Fatalf("second candidates=%v want len %d", second, len(first))
	}
	if &scratch.visitedEpochs[0] != visited || cap(scratch.queue) != queueCap || cap(scratch.best) != bestCap || cap(scratch.out) != outCap {
		t.Fatal("scratch buffers were not reused")
	}
}

func TestVectorIndexSearchScratchVisitedEpochsGrowGeometrically(t *testing.T) {
	var scratch vectorIndexSearchScratch

	scratch.nextVisitedEpoch(64)
	if cap(scratch.visitedEpochs) != 64 {
		t.Fatalf("initial visited cap=%d want 64", cap(scratch.visitedEpochs))
	}
	scratch.nextVisitedEpoch(65)
	if cap(scratch.visitedEpochs) != 128 {
		t.Fatalf("visited cap after one-node growth=%d want 128", cap(scratch.visitedEpochs))
	}
	visited := &scratch.visitedEpochs[0]
	for nodes := 66; nodes <= 128; nodes++ {
		scratch.nextVisitedEpoch(nodes)
		if &scratch.visitedEpochs[0] != visited {
			t.Fatalf("visited epochs reallocated at nodes=%d before capacity was exhausted", nodes)
		}
	}
}

func TestCollectionVectorIndexInt8EncodingReranksCanonicalRows(t *testing.T) {
	const (
		docs = 24
		dims = 96
		topK = 5
	)
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	ids := make([][]byte, 0, docs)
	documents := make([][]byte, 0, docs)
	for i := 0; i < docs; i++ {
		ids = append(ids, []byte(fmt.Sprintf("doc-%02d", i)))
		documents = append(documents, vectorBenchmarkDocument(i, dims))
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		t.Fatalf("insert: %v", err)
	}
	floatIndex, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding_f32",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      8,
	})
	if err != nil {
		t.Fatalf("build float32 vector index: %v", err)
	}
	int8Index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        8,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("build int8 vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(7, dims)
	exact, err := col.SearchVectorsExact(query, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   topK,
	})
	if err != nil {
		t.Fatalf("exact search: %v", err)
	}
	results, trace, err := int8Index.Search(query, VectorIndexSearchOptions{
		TopK:                 topK,
		EfSearch:             docs,
		FetchMultiplier:      docs,
		DisableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("int8 search: %v", err)
	}
	if trace.RerankCount < topK {
		t.Fatalf("int8 search did not rerank canonical rows: %+v", trace)
	}
	for i := range exact {
		if string(results[i].DocumentID) != string(exact[i].DocumentID) {
			t.Fatalf("int8 result[%d]=%q want exact %q", i, results[i].DocumentID, exact[i].DocumentID)
		}
	}
	floatStats := floatIndex.Stats()
	int8Stats := int8Index.Stats()
	if int8Stats.Encoding != VectorIndexEncodingInt8 || int8Stats.Dimensions != dims {
		t.Fatalf("unexpected int8 stats: %+v", int8Stats)
	}
	if int8Stats.BytesMemory >= floatStats.BytesMemory {
		t.Fatalf("int8 memory bytes=%d want less than float32 %d", int8Stats.BytesMemory, floatStats.BytesMemory)
	}
	int8Index.mu.RLock()
	defer int8Index.mu.RUnlock()
	if len(int8Index.nodes) == 0 || len(int8Index.nodes[0].vector) != 0 || len(int8Index.nodes[0].quantized) != dims || int8Index.nodes[0].quantScale <= 0 {
		t.Fatalf("unexpected int8 node storage: %+v", int8Index.nodes[0])
	}
}

func TestCollectionVectorIndexSelectiveRangeFilterUsesExactFilteredStrategy(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollectionWithIndexes(t, d, IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[1,0],"city":"sea"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"hnl"}`),
			[]byte(`{"embedding":[0,1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{
		TopK: 2,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.Strategy != "exact_filtered" || trace.CandidatesExamined != 3 || trace.RerankCount != 3 || trace.ReturnedCount != 2 {
		t.Fatalf("unexpected trace: %+v", trace)
	}
}

func TestCollectionVectorIndexBroadRangeFilterUsesANNPostFilter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollectionWithIndexes(t, d, IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[0.95,0.05],"city":"hnl"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"sea"}`),
			[]byte(`{"embedding":[0,1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 1,
		EfSearch:             8,
		FetchMultiplier:      8,
		ExactFilterMaxDocs:   1,
		DisableExactFallback: true,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	requireVectorResultIDs(t, results, "a")
	if trace.Strategy != "ann_postfilter" || trace.CandidatesAfterFilter == 0 || trace.RerankCount == 0 {
		t.Fatalf("unexpected trace: %+v", trace)
	}
}

func TestCollectionVectorIndexInsertAndTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert c: %v", err)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after insert: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")

	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tombstone: %v", err)
	}
	requireVectorResultIDs(t, results, "c", "b")
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 1 || !stats.RebuildNeeded {
		t.Fatalf("unexpected post-tombstone stats: %+v", stats)
	}
}

func TestCollectionVectorIndexInsertDocumentNoopsWhenVectorUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	before := index.Stats()
	if err := index.InsertDocument([]byte("a")); err != nil {
		t.Fatalf("insert unchanged document: %v", err)
	}
	after := index.Stats()
	if after.Nodes != before.Nodes || after.DeletedDocs != before.DeletedDocs || after.LiveDocs != before.LiveDocs {
		t.Fatalf("unchanged insert mutated graph before=%+v after=%+v", before, after)
	}
}

func TestCollectionVectorIndexTracksRegisteredMutations(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert tracked c: %v", err)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tracked insert: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")

	replaced, err := col.Replace([]byte("b"), []byte(`{"embedding":[0.95,0.05]}`))
	if err != nil || !replaced {
		t.Fatalf("replace b replaced=%v err=%v", replaced, err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tracked replace: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b", "c")

	deleted, err := col.DeleteBatch([][]byte{[]byte("a")})
	if err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tracked delete: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 2 {
		t.Fatalf("tracked mutation stats=%+v want live=2 deleted=2", stats)
	}
}

func TestCollectionVectorIndexRebuildRemovesTombstones(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	index.TombstoneDocumentID([]byte("a"))
	if stats := index.Stats(); stats.DeletedDocs != 1 || !stats.RebuildNeeded {
		t.Fatalf("pre-rebuild stats=%+v want tombstone and rebuild-needed", stats)
	}
	if err := index.Rebuild(); err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 0 || stats.RebuildNeeded {
		t.Fatalf("post-rebuild stats=%+v want live=2 deleted=0 rebuild=false", stats)
	}
	if stats.LastRebuildDuration <= 0 {
		t.Fatalf("post-rebuild stats missing rebuild duration: %+v", stats)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt index: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")

	if _, err := col.InsertBatch(
		[][]byte{[]byte("d")},
		[][]byte{[]byte(`{"embedding":[0.99,0.01]}`)},
	); err != nil {
		t.Fatalf("insert after rebuild: %v", err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after post-rebuild insert: %v", err)
	}
	requireVectorResultIDs(t, results, "d", "b")
}

func TestCollectionVectorIndexUpdateDocumentReplacesCurrentNode(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	replaced, err := col.Replace([]byte("b"), []byte(`{"embedding":[0.95,0.05]}`))
	if err != nil || !replaced {
		t.Fatalf("replace b replaced=%v err=%v", replaced, err)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after update: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 1 {
		t.Fatalf("unexpected stats after update: %+v", stats)
	}
}

func TestCollectionVectorIndexUnderfillFallsBackToExact(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 1, EfSearch: 1})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 4, EfSearch: 1, FetchMultiplier: 1})
	if err != nil {
		t.Fatalf("search with fallback: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b", "c")
	if trace.Strategy != "ann_graph_exact_fallback" || trace.ExactFallbackReason == "" {
		t.Fatalf("expected exact fallback trace, got %+v", trace)
	}
}

func TestCollectionVectorIndexCheckRecall(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	recall, err := index.CheckRecall([][]float32{{1, 0}, {0, 1}}, VectorIndexSearchOptions{TopK: 1})
	if err != nil {
		t.Fatalf("check recall: %v", err)
	}
	if recall.Queries != 2 || recall.ExactTotal != 2 || recall.Overlap != 2 || recall.Recall != 1 {
		t.Fatalf("unexpected recall: %+v", recall)
	}
}

func TestCollectionVectorIndexCheckRecallExactBatchMatchesPerQueryExact(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")},
		[][]byte{
			[]byte(`{"embedding":[1,0,0]}`),
			[]byte(`{"embedding":[0.9,0.1,0]}`),
			[]byte(`{"embedding":[0.1,0.9,0]}`),
			[]byte(`{"embedding":[0,1,0]}`),
			[]byte(`{"embedding":[0,0.1,0.9]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	queries := [][]float32{{1, 0.05, 0}, {0.05, 1, 0}}
	got, usedBatch, err := index.checkRecallExactBatch(queries, VectorIndexSearchOptions{TopK: 3})
	if err != nil {
		t.Fatalf("check recall exact batch: %v", err)
	}
	if !usedBatch {
		t.Fatal("exact recall did not use batch path")
	}
	for i, query := range queries {
		want, err := col.SearchVectorsExact(query, VectorSearchOptions{
			Field:  "embedding",
			Metric: VectorMetricCosine,
			TopK:   3,
		})
		if err != nil {
			t.Fatalf("exact search query %d: %v", i, err)
		}
		if len(got[i]) != len(want) {
			t.Fatalf("query %d batch len=%d want %d", i, len(got[i]), len(want))
		}
		for j := range want {
			if !bytes.Equal(got[i][j].DocumentID, want[j].DocumentID) || got[i][j].Distance != want[j].Distance {
				t.Fatalf("query %d result %d batch=%+v want=%+v", i, j, got[i][j], want[j])
			}
		}
	}
}

func TestCollectionVectorIndexCheckRecallUsesIndexRangeFilter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollectionWithIndexes(t, d, IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"hnl"}`),
			[]byte(`{"embedding":[0.8,0.2],"city":"sea"}`),
			[]byte(`{"embedding":[0,1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	recall, err := index.CheckRecall([][]float32{{1, 0}}, VectorIndexSearchOptions{
		TopK: 2,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("check filtered recall: %v", err)
	}
	if recall.ExactTotal != 2 || recall.ANNTotal != 2 || recall.Overlap != 2 || recall.Recall != 1 {
		t.Fatalf("filtered recall used wrong baseline: %+v", recall)
	}
	if len(recall.SearchTraces) != 1 || recall.SearchTraces[0].Strategy != "exact_filtered" {
		t.Fatalf("unexpected filtered recall traces: %+v", recall.SearchTraces)
	}
}

func TestCollectionVectorIndexCheckRecallFallsBackForUnsafeBatchNorms(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[100000000000000000000,100000000000000000000]}`),
			[]byte(`{"embedding":[100000000000000000000,-100000000000000000000]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	_, usedBatch, err := index.checkRecallExactBatch([][]float32{
		{1e20, 1e20},
		{1e20, -1e20},
	}, VectorIndexSearchOptions{TopK: 1})
	if err != nil {
		t.Fatalf("check recall exact batch: %v", err)
	}
	if usedBatch {
		t.Fatal("unsafe cosine norms used float32 batch path")
	}
}

func TestAngularDistancesFloat32BatchMatchesExactCosine(t *testing.T) {
	queries := [][]float32{
		{1e9, 1e9 + 128, 1e9 - 64},
		{1e9, -1e9, 3},
	}
	documents := [][]float32{
		{1e9, 1e9 + 256, 1e9 - 128},
		{1e9, 1e9 + 128, -1e9},
	}
	queryMatrix := make([]float32, 0, len(queries)*len(queries[0]))
	for _, query := range queries {
		queryMatrix = append(queryMatrix, query...)
	}
	documentMatrix := make([]float32, 0, len(documents)*len(documents[0]))
	for _, document := range documents {
		documentMatrix = append(documentMatrix, document...)
	}
	documentNorms := make([]float64, len(documents))
	for i, document := range documents {
		documentNorms[i] = vectorNormSquared(document)
	}
	distances := make([]float64, len(queries)*len(documents))
	angularDistancesFloat32Batch(queryMatrix, documentMatrix, documentNorms, len(queries), len(documents), len(queries[0]), distances)
	for queryIndex, query := range queries {
		for docIndex, document := range documents {
			want, err := exactVectorDistance(query, document, VectorMetricCosine)
			if err != nil {
				t.Fatalf("exact cosine distance: %v", err)
			}
			got := float32(distances[queryIndex*len(documents)+docIndex])
			if got != want {
				t.Fatalf("distance[%d][%d]=%.9g want exact %.9g", queryIndex, docIndex, got, want)
			}
		}
	}
}

func openVectorIndexTestCollection(tb testing.TB, d *backenddb.DB) *Collection {
	tb.Helper()
	return openVectorIndexTestCollectionWithIndexes(tb, d)
}

func openVectorIndexTestCollectionWithIndexes(tb testing.TB, d *backenddb.DB, indexes ...IndexDefinition) *Collection {
	tb.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: indexes}); err != nil {
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		tb.Fatalf("open collection: %v", err)
	}
	return col
}
