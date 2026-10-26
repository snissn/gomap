package collections

import (
	"bytes"
	"encoding/json"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Test-only packet against original 4d91; no candidate production helper is
// required. Each cold route has its own independently closed/reopened owner.
func TestNativeHNSWTemplateColdBinding(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, route := range []string{"load", "dense", "mixed"} {
			t.Run(string(profile)+"/"+route, func(t *testing.T) {
				f := newTemplateHNSWBindingFixture(t, profile, true)
				idx, err := f.c.BuildVectorIndex(VectorIndexOptions{Name: "embedding"})
				if err != nil {
					t.Fatal(err)
				}
				templateHNSWBindingSearch(t, idx, "tenant", "one", "a", "c")
				if _, err = idx.SaveSnapshot(); err != nil {
					t.Fatal(err)
				}
				if err = f.db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				f.close()
				f.open(false)
				if f.c.registeredVectorIndex("embedding") != nil {
					t.Fatal("reopen unexpectedly registered runtime")
				}
				// No Create/Build/Load/other query before the selected route.
				switch route {
				case "load":
					loaded, status, err := f.c.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding"})
					if err != nil || !status.Loaded {
						t.Fatalf("cold template Load: status=%+v err=%v", status, err)
					}
					templateHNSWBindingSearch(t, loaded, "tenant", "one", "a", "c")
				case "dense":
					var buffer VectorIndexSearchBuffer
					defer buffer.Reset()
					response, view, err := f.c.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
						IndexName: "embedding", Query: []float32{1, 0}, TopK: 2,
						StatsMode:            VectorIndexSearchStatsModeProduction,
						DeclaredScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "one"},
					}, &buffer)
					if view != nil {
						defer view.Close()
					}
					if err != nil {
						t.Fatalf("first cold template Dense: %v", err)
					}
					if response.Strategy != VectorIndexStrategyNativeRuntime || response.Stats.SearchRouteNativeRuntime != 1 || response.Stats.DocumentsFetched != 0 || !response.Diagnostics().NoDocumentGuardrailsOK || response.Diagnostics().LiveANN.ExactFallbacks != 0 {
						t.Fatalf("cold template Dense route: %+v diagnostics=%+v", response, response.Diagnostics())
					}
					if len(response.Results) != 2 || string(response.Results[0].ID) != "a" || string(response.Results[1].ID) != "c" {
						t.Fatalf("cold Dense IDs: %+v", response.Results)
					}
				case "mixed":
					response, err := f.c.SearchHybrid(HybridSearchOptions{
						TopK:         2,
						Text:         &HybridTextQuery{IndexName: "content", Query: "policy", CandidateLimit: 8},
						Vector:       &HybridVectorQuery{IndexName: "embedding", Query: []float32{1, 0}, CandidateLimit: 8},
						ScalarFilter: &HybridScalarFilter{IndexName: "tenant", Value: "one"},
						// Template results are IDs/scores, not JSON projection.
						Debug: HybridSearchDebugOptions{IncludeCandidates: true},
					})
					if err != nil {
						t.Fatalf("first cold template mixed Hybrid: %v", err)
					}
					if !response.Snapshot.Native || response.Snapshot.CollectionIncarnation == 0 || response.Stats.TextCandidatesReturned != 2 || response.Stats.VectorCandidatesReturned != 2 || response.Stats.FusionBoth != 2 || response.Stats.FullDocumentScanFallbacks != 0 || response.Stats.FailClosed != 0 || response.Stats.DocumentsFetched != 0 {
						t.Fatalf("cold mixed route: %+v", response)
					}
					if len(response.Results) != 2 || string(response.Results[0].ID) != "a" || string(response.Results[1].ID) != "c" {
						t.Fatalf("cold mixed IDs: %+v", response.Results)
					}
				}
			})
		}
	}
}

type templateHNSWBindingFixture struct {
	t       *testing.T
	dir     string
	profile treedb.Profile
	db      *treedb.DB
	manager *CollectionManager
	c       *Collection
	budget  nativeHNSWAdmissionBudget
}

func newTemplateHNSWBindingFixture(t *testing.T, profile treedb.Profile, scalar bool) *templateHNSWBindingFixture {
	t.Helper()
	f := &templateHNSWBindingFixture{t: t, dir: t.TempDir(), profile: profile}
	t.Cleanup(f.close)
	f.open(true)
	meta := &CollectionMeta{
		Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
		VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyNativeRuntime, M: 4}},
		TextIndexes:   []TextIndexDefinition{{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: true}},
	}
	if scalar {
		meta.Indexes = []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}
	}
	if _, err := f.manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	var err error
	f.c, err = f.manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{
		templateHNSWBindingDocument(t, "one", "policy policy", "shape_a", []float32{1, 0}),
		templateHNSWBindingDocument(t, "two", "policy other", "shape_b", []float32{0, 1}),
		templateHNSWBindingDocument(t, "one", "policy", "shape_c", []float32{0.8, 0.2}),
	}); err != nil {
		t.Fatal(err)
	}
	// The real preparer must persist numeric dictionaries/TD1D, not JSON input.
	stored, err := f.c.Get([]byte("a"))
	if err != nil || !bytes.HasPrefix(stored, []byte("TD1D")) {
		t.Fatalf("stored template: %x %v", stored, err)
	}
	m, err := f.c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	document, err := m.StoredDocumentJSON(stored)
	closeErr := m.Close()
	if err != nil || closeErr != nil || !bytes.Contains(document, []byte("policy policy")) {
		t.Fatalf("public template materializer: %s %v %v", document, err, closeErr)
	}
	return f
}

func (f *templateHNSWBindingFixture) open(initial bool) {
	f.t.Helper()
	opts := treedb.OptionsFor(f.profile, f.dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	var err error
	f.db, err = treedb.Open(opts)
	if err != nil {
		f.t.Fatal(err)
	}
	backend, err := f.db.NativeSemanticBackend()
	if err != nil {
		f.t.Fatal(err)
	}
	cut, err := backend.CaptureNativeSemanticState()
	if err != nil {
		f.t.Fatal(err)
	}
	f.budget = cut.NativeAllocator().(nativeHNSWAdmissionBudget)
	if err = cut.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.manager = NewCollectionManager(backend)
	if !initial {
		f.c, err = f.manager.OpenCollection("docs")
		if err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *templateHNSWBindingFixture) close() {
	f.t.Helper()
	if f.manager != nil {
		if err := f.manager.Close(); err != nil {
			f.t.Error(err)
		}
		f.manager = nil
	}
	if f.db != nil {
		if err := f.db.Close(); err != nil {
			f.t.Error(err)
		}
		f.db = nil
	}
	if f.budget != nil {
		if s := f.budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
			f.t.Errorf("full template owner Close: %+v", s)
		}
		f.budget = nil
	}
}

func templateHNSWBindingDocument(t *testing.T, tenant, content, shape string, vector []float32) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"tenant": tenant, "content": content, "embedding": vector,
		"nested":   map[string]any{shape: map[string]any{"label": tenant}},
		"repeated": []any{map[string]any{"label": tenant}, map[string]any{"label": "again"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeTemplateV1DocumentJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func templateHNSWBindingSearch(t *testing.T, idx *VectorIndex, name, value string, want ...string) {
	t.Helper()
	rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: len(want), DisableExactFallback: true,
		IndexRangeFilter: &VectorIndexRangeFilter{IndexName: name, Range: IndexRangeOptions{Lower: IndexRangeBound{Value: value, Inclusive: true}, Upper: IndexRangeBound{Value: value, Inclusive: true}}},
	})
	if err != nil || len(rows) != len(want) {
		t.Fatalf("template scalar search: rows=%+v err=%v", rows, err)
	}
	for i := range want {
		if string(rows[i].DocumentID) != want[i] {
			t.Fatalf("template scalar IDs: %+v want=%v", rows, want)
		}
	}
}
