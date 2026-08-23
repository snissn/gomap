package collections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/collections/chunking"
	"github.com/snissn/gomap/TreeDB/collections/embedding"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// openIngestTestCollection opens a collection wired for full RAG ingestion
// coverage: a scalar index over chunk kind, a lexical index over the chunked
// body field, and a declared cosine vector index over the "embedding" field.
func openIngestTestCollection(t testing.TB, dims int) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	// Close before TempDir cleanup: Windows cannot unlink files that are
	// still held by the database.
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{{
			Name:      "by_kind",
			Field:     chunking.MetaFieldKind,
			ValueType: IndexValueString,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: dims,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV1,
		Fields:         []TextIndexField{{Field: "body"}},
		StorePositions: true,
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	return dir, d, col
}

func ingestTestCfg(dims int) IngestSourcesConfig {
	return IngestSourcesConfig{
		Chunking:        fixedWindowCfg(64, 8),
		Embedding:       embedding.Config{Provider: embedding.ProviderHashing, Dimensions: dims},
		VectorIndexName: "embedding",
	}
}

// ingestTestSource builds a source whose body repeats a per-source token often
// enough that every derived chunk contains it, letting lexical searches target
// exactly one source's children.
func ingestTestSource(id string, ordinal int) SourceDocument {
	token := fmt.Sprintf("tok%d", ordinal)
	body := strings.Repeat(token+" alpha beta gamma delta ", 40)
	fields := map[string]any{"body": body, "title": fmt.Sprintf("source-%d", ordinal)}
	return SourceDocument{ID: []byte(id), Fields: fields, Meta: map[string]any{"origin": "ingest-test"}}
}

func ingestTestSources(n int) []SourceDocument {
	sources := make([]SourceDocument, n)
	for i := range sources {
		sources[i] = ingestTestSource(fmt.Sprintf("src-%d", i), i)
	}
	return sources
}

func mustIngest(t *testing.T, col *Collection, sources []SourceDocument, cfg IngestSourcesConfig) IngestResult {
	t.Helper()
	res, err := col.IngestSources(context.Background(), sources, cfg)
	if err != nil {
		t.Fatalf("IngestSources: %v", err)
	}
	return res
}

// liveChildIDSet returns the union of live chunk children across parents.
func liveChildIDSet(t *testing.T, col *Collection, parents []string) map[string]struct{} {
	t.Helper()
	set := make(map[string]struct{})
	for _, p := range parents {
		children, err := col.ChunkChildren([]byte(p))
		if err != nil {
			t.Fatalf("ChunkChildren(%q): %v", p, err)
		}
		for _, id := range children {
			set[string(id)] = struct{}{}
		}
	}
	return set
}

// assertIndexParity proves text, vector, and scalar indexes resolve exactly the
// live child set: no stale rows, no gaps, no ghosts.
func assertIndexParity(t *testing.T, col *Collection, parents []string) {
	t.Helper()
	live := liveChildIDSet(t, col, parents)

	// Scalar parity: the by_kind range enumerates exactly the live children.
	got, truncated, err := col.FindByIndexRange("by_kind", IndexRangeOptions{
		Lower: IndexRangeBound{Value: chunking.KindChunk, Inclusive: true},
		Upper: IndexRangeBound{Value: chunking.KindChunk, Inclusive: true},
		Limit: 1 << 20,
	})
	if err != nil {
		t.Fatalf("FindByIndexRange: %v", err)
	}
	if truncated {
		t.Fatal("FindByIndexRange truncated; test scale exceeded limit")
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, id := range got {
		gotSet[string(id)] = struct{}{}
	}
	if len(gotSet) != len(got) {
		t.Fatalf("scalar range returned duplicates: %d ids, %d unique", len(got), len(gotSet))
	}
	if !stringSetsEqual(gotSet, live) {
		t.Fatalf("scalar index mismatch: got %d children, want %d live children\ngot=%v\nwant=%v",
			len(gotSet), len(live), sortedKeys(gotSet), sortedKeys(live))
	}

	// Lexical parity: every source's token resolves exactly that source's
	// live children, and no query ever surfaces a dead row.
	for _, p := range parents {
		var token string
		if _, err := fmt.Sscanf(p, "src-%s", &token); err != nil {
			token = p
		}
		resp, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: token, TopK: len(live) + 16})
		if err != nil {
			t.Fatalf("SearchText(%q): %v", token, err)
		}
		wantParentPrefix := strings.Replace(token, "tok", "", 1)
		for _, r := range resp.Results {
			id := string(r.DocumentID)
			if _, ok := live[id]; !ok {
				t.Fatalf("text search surfaced non-live row %q", id)
			}
			if !strings.HasPrefix(strings.TrimSuffix(strings.TrimPrefix(id, "src"), ""), wantParentPrefix) {
				// Cross-check membership instead of parsing: the child must
				// belong to the queried parent.
				childBelongs := false
				children, _ := col.ChunkChildren([]byte(p))
				for _, cid := range children {
					if string(cid) == id {
						childBelongs = true
						break
					}
				}
				if !childBelongs {
					t.Fatalf("text query for parent %q returned foreign child %q", p, id)
				}
			}
		}
	}

	// Vector parity: querying with any live child's stored embedding must
	// return that child as the exact zero-distance top hit, and every result
	// must be a live child.
	for id := range firstN(live, 3) {
		raw, err := col.Get([]byte(id))
		if err != nil || len(raw) == 0 {
			t.Fatalf("Get(%q)=%d,%v", id, len(raw), err)
		}
		var doc struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("unmarshal %q: %v", id, err)
		}
		if len(doc.Embedding) == 0 {
			t.Fatalf("live child %q has no embedding field", id)
		}
		results, err := col.SearchVectorsExact(doc.Embedding, VectorSearchOptions{
			Field:  "embedding",
			Metric: VectorMetricCosine,
			TopK:   len(live),
		})
		if err != nil {
			t.Fatalf("SearchVectorsExact: %v", err)
		}
		if len(results) == 0 {
			t.Fatalf("vector self-query for %q returned no results", id)
		}
		foundSelf := false
		for _, r := range results {
			if string(r.DocumentID) == id && math.Abs(float64(r.Distance)) <= 1e-6 {
				foundSelf = true
			}
			if _, ok := live[string(r.DocumentID)]; !ok {
				t.Fatalf("vector search surfaced non-live row %q", r.DocumentID)
			}
		}
		if !foundSelf {
			t.Fatalf("vector self-query for %q did not return a zero-distance self hit", id)
		}
	}
}

func stringSetsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func firstN(set map[string]struct{}, n int) map[string]struct{} {
	out := make(map[string]struct{}, n)
	for k := range set {
		if len(out) == n {
			break
		}
		out[k] = struct{}{}
	}
	return out
}

// TestIngestSourcesHappyPathAndIndexParity ingests multiple sources through
// the composed pipeline and proves text/scalar/vector indexes resolve exactly
// the live child set.
func TestIngestSourcesHappyPathAndIndexParity(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	sources := ingestTestSources(4)
	res := mustIngest(t, col, sources, ingestTestCfg(256))
	if len(res.Ingested) != len(sources) {
		t.Fatalf("res.Ingested=%d want %d", len(res.Ingested), len(sources))
	}
	parents := make([]string, len(sources))
	for i, s := range sources {
		parents[i] = string(s.ID)
		if res.Ingested[i].ID == nil || string(res.Ingested[i].ID) != parents[i] {
			t.Fatalf("outcome %d ID=%q want %q", i, res.Ingested[i].ID, parents[i])
		}
		if len(res.Ingested[i].ChildIDs) == 0 {
			t.Fatalf("source %q produced no children", parents[i])
		}
	}
	assertIndexParity(t, col, parents)
}

// failingEmbedder fails every batch until disarmed; safe for concurrent workers.
type failingEmbedder struct {
	dims int

	mu     sync.Mutex
	failed bool
	err    error
}

func (f *failingEmbedder) Dimensions() int { return f.dims }

func (f *failingEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed {
		return nil, f.err
	}
	return makeVectors(len(texts), f.dims), nil
}

func makeVectors(n, dims int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32(i+1) / float32(j+2)
		}
		out[i] = vec
	}
	return out
}

const ingestFailingProvider = "ingest-test-failing"

func registerFailingEmbedder(t *testing.T, dims int) *failingEmbedder {
	t.Helper()
	emb := &failingEmbedder{dims: dims, err: errors.New("boom: embed service unavailable")}
	if err := embedding.DefaultRegistry().Register(ingestFailingProvider, func(cfg embedding.Config) (embedding.Embedder, error) {
		return emb, nil
	}); err != nil {
		t.Fatalf("register failing embedder: %v", err)
	}
	return emb
}

// TestIngestSourcesEmbedFailureTaxonomy proves an embed failure on source N of
// M leaves sources 0..N-1 fully intact, source N fully absent, later sources
// untouched, and the typed error naming the failed source ID and stage.
func TestIngestSourcesEmbedFailureTaxonomy(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	const m = 4
	const n = 2
	emb := registerFailingEmbedder(t, 256)
	cfg := ingestTestCfg(256)
	cfg.Concurrency = 1
	cfg.Embedding.Provider = ingestFailingProvider
	sources := ingestTestSources(m)
	cfg.hooks = &ingestFaultHooks{
		beforeSource: func(i int) error {
			emb.mu.Lock()
			emb.failed = i == n
			emb.mu.Unlock()
			return nil
		},
	}

	res, err := col.IngestSources(context.Background(), sources, cfg)
	if err == nil {
		t.Fatal("IngestSources succeeded with failing embedder")
	}
	var ingestErr *IngestError
	if !errors.As(err, &ingestErr) {
		t.Fatalf("err=%T (%v) want *IngestError", err, err)
	}
	if ingestErr.Stage != IngestStageEmbed {
		t.Fatalf("stage=%v want IngestStageEmbed", ingestErr.Stage)
	}
	if string(ingestErr.SourceID) != string(sources[n].ID) || ingestErr.SourceIndex != n {
		t.Fatalf("error names source %q (%d), want %q (%d)", ingestErr.SourceID, ingestErr.SourceIndex, sources[n].ID, n)
	}
	if !strings.Contains(err.Error(), string(sources[n].ID)) {
		t.Fatalf("error text %q does not name the failed source ID", err.Error())
	}

	// Sources before N are fully intact.
	for i := 0; i < n; i++ {
		children, err := col.ChunkChildren(sources[i].ID)
		if err != nil || len(children) == 0 {
			t.Fatalf("source %d children=%d err=%v; expected fully intact prior source", i, len(children), err)
		}
		raw, err := col.Get(sources[i].ID)
		if err != nil || len(raw) == 0 {
			t.Fatalf("prior source %d parent missing: len=%d err=%v", i, len(raw), err)
		}
	}
	// Source N is fully absent: no children, no parent row.
	children, err := col.ChunkChildren(sources[n].ID)
	if err != nil || len(children) != 0 {
		t.Fatalf("failed source N has children=%d err=%v; want fully absent", len(children), err)
	}
	raw, err := col.Get(sources[n].ID)
	if err != nil || len(raw) != 0 {
		t.Fatalf("failed source N parent present: len=%d err=%v", len(raw), err)
	}
	// Later sources are untouched.
	for i := n + 1; i < m; i++ {
		c, err := col.ChunkChildren(sources[i].ID)
		if err != nil || len(c) != 0 {
			t.Fatalf("later source %d touched: children=%d", i, len(c))
		}
	}
	// Result reports only fully committed sources.
	if len(res.Ingested) != n {
		t.Fatalf("res.Ingested=%d want %d (only pre-failure sources)", len(res.Ingested), n)
	}
}

type serialGuardEmbedder struct {
	dims       int
	active     atomic.Int32
	concurrent atomic.Bool
}

func (e *serialGuardEmbedder) Dimensions() int { return e.dims }

func (e *serialGuardEmbedder) EmbedBatch(_ context.Context, texts [][]byte) ([][]float32, error) {
	if e.active.Add(1) != 1 {
		e.concurrent.Store(true)
	}
	defer e.active.Add(-1)
	time.Sleep(time.Millisecond)
	return makeVectors(len(texts), e.dims), nil
}

const ingestSerialGuardProvider = "ingest-test-serial-guard"

func TestIngestSourcesSerializesSharedEmbedder(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 16)
	emb := &serialGuardEmbedder{dims: 16}
	if err := embedding.DefaultRegistry().Register(ingestSerialGuardProvider, func(embedding.Config) (embedding.Embedder, error) {
		return emb, nil
	}); err != nil {
		t.Fatalf("register serial guard embedder: %v", err)
	}
	cfg := ingestTestCfg(16)
	cfg.Embedding.Provider = ingestSerialGuardProvider
	cfg.Concurrency = 8
	if _, err := col.IngestSources(context.Background(), ingestTestSources(16), cfg); err != nil {
		t.Fatalf("IngestSources: %v", err)
	}
	if emb.concurrent.Load() {
		t.Fatal("shared embedder observed concurrent EmbedBatch calls")
	}
}

// TestIngestSourcesChunkFailureFailClosedBeforeMutation proves plan validation
// runs up front across the whole batch: one invalid source leaves the entire
// collection untouched, with the typed chunk-stage error naming that source.
func TestIngestSourcesChunkFailureFailClosedBeforeMutation(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	sources := ingestTestSources(3)
	sources[1].Fields["body"] = "this source carries the invalid global config"
	cfg.Chunking = fixedWindowCfg(64, 64) // overlap == size fails validation

	res, err := col.IngestSources(context.Background(), sources, cfg)
	if err == nil {
		t.Fatal("invalid chunk config accepted")
	}
	var ingestErr *IngestError
	if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageChunk {
		t.Fatalf("err=%v want chunk-stage IngestError", err)
	}
	if ingestErr.SourceIndex != 0 {
		t.Fatalf("error source index=%d want 0 for batch config failure", ingestErr.SourceIndex)
	}
	if len(res.Ingested) != 0 {
		t.Fatalf("batch-level validation returned ingested outcomes: %+v", res.Ingested)
	}
	for _, s := range sources {
		if raw, _ := col.Get(s.ID); len(raw) != 0 {
			t.Fatalf("source %q has parent despite batch-level failure", s.ID)
		}
		children, err := col.ChunkChildren(s.ID)
		if err != nil {
			t.Fatalf("ChunkChildren(%q): %v", s.ID, err)
		}
		if len(children) != 0 {
			t.Fatalf("source %q has children despite batch-level failure", s.ID)
		}
	}
}

// TestIngestSourcesMissingTextFieldTypedChunkError proves a source without the
// configured text field fails closed with a chunk-stage typed error and lands
// no mutation.
func TestIngestSourcesMissingTextFieldTypedChunkError(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	cfg.TextField = "body"
	sources := []SourceDocument{
		{ID: []byte("ok-src"), Fields: map[string]any{"body": "hello world content"}},
		{ID: []byte("bad-src"), Fields: map[string]any{"title": "no body here"}},
	}
	_, err := col.IngestSources(context.Background(), sources, cfg)
	var ingestErr *IngestError
	if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageChunk {
		t.Fatalf("err=%v want chunk-stage IngestError", err)
	}
	if string(ingestErr.SourceID) != "bad-src" {
		t.Fatalf("error names %q want bad-src", ingestErr.SourceID)
	}
	if raw, _ := col.Get([]byte("ok-src")); len(raw) != 0 {
		t.Fatal("valid sibling source mutated by batch-level fail-closed validation")
	}
}

// TestIngestSourcesIdempotentDoubleIngest proves re-ingesting unchanged
// sources is a clean replace: identical live child set and identical index
// contents, never duplicate children.
func TestIngestSourcesIdempotentDoubleIngest(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	sources := ingestTestSources(3)
	parents := []string{"src-0", "src-1", "src-2"}

	first := mustIngest(t, col, sources, cfg)
	assertIndexParity(t, col, parents)
	firstChildren := liveChildIDSet(t, col, parents)

	second := mustIngest(t, col, sources, cfg)
	if len(second.Ingested) != len(parents) {
		t.Fatalf("second ingest res.Ingested=%d", len(second.Ingested))
	}
	for i, o := range second.Ingested {
		if o.Replaced != len(first.Ingested[i].ChildIDs) {
			t.Fatalf("source %q Replaced=%d want %d (prior child count)", o.ID, o.Replaced, len(first.Ingested[i].ChildIDs))
		}
	}
	secondChildren := liveChildIDSet(t, col, parents)
	if !stringSetsEqual(firstChildren, secondChildren) {
		t.Fatalf("double ingest changed the live child set: %d -> %d\n%v\n%v",
			len(firstChildren), len(secondChildren), sortedKeys(firstChildren), sortedKeys(secondChildren))
	}
	// Derived IDs converge: <parent>#<ordinal>, count stable.
	for _, p := range parents {
		children, err := col.ChunkChildren([]byte(p))
		if err != nil {
			t.Fatalf("ChunkChildren(%q): %v", p, err)
		}
		if len(children) != len(firstChildren)/len(parents) {
			t.Fatalf("parent %q has %d children, want stable count", p, len(children))
		}
	}
	assertIndexParity(t, col, parents)
}

// TestIngestSourcesChangedSourceCleanReplace proves re-ingesting a source with
// new content replaces its children cleanly: old rows vanish from every index,
// new rows resolve, other sources keep their original children.
func TestIngestSourcesChangedSourceCleanReplace(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	sources := ingestTestSources(2)
	parents := []string{"src-0", "src-1"}
	mustIngest(t, col, sources, cfg)

	changed := ingestTestSource("src-0", 7) // new token + content, same ID
	reIngest := mustIngest(t, col, []SourceDocument{changed, sources[1]}, cfg)
	if len(reIngest.Ingested) != 2 {
		t.Fatalf("re-ingest incomplete: %+v", reIngest.Ingested)
	}
	assertIndexParity(t, col, parents)
	// The replaced source's children carry the new token exclusively.
	resp, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "tok7", TopK: 100})
	if err != nil {
		t.Fatalf("SearchText(tok7): %v", err)
	}
	if len(resp.Results) == 0 {
		t.Fatal("re-chunked source not searchable under its new token")
	}
	childHits := 0
	for _, r := range resp.Results {
		id := string(r.DocumentID)
		if id == "src-0" {
			continue // the parent is also indexed in the lexical index
		}
		if !strings.HasPrefix(id, "src-0#") {
			t.Fatalf("new token hit foreign document %q", id)
		}
		childHits++
	}
	if childHits == 0 {
		t.Fatal("new token did not resolve any replaced child")
	}
	respOld, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "tok0", TopK: 100})
	if err != nil {
		t.Fatalf("SearchText(tok0): %v", err)
	}
	if len(respOld.Results) != 0 {
		t.Fatalf("stale lexical rows survived replace: %d hits for tok0", len(respOld.Results))
	}
}

var errIngestInjected = errors.New("injected fault at batch boundary")

// TestIngestSourcesFaultBoundaryDurabilityReopen injects a fault before source
// N touches the collection, then proves that after close and reopen only
// fully ingested sources exist: zero orphaned children, zero stale vector rows,
// and index parity across text/scalar/vector.
func TestIngestSourcesFaultBoundaryDurabilityReopen(t *testing.T) {
	dir, d, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	const n = 2
	cfg.Concurrency = 1
	sources := ingestTestSources(4)
	cfg.hooks = &ingestFaultHooks{
		beforeSource: func(i int) error {
			if i == n {
				return errIngestInjected
			}
			return nil
		},
	}

	_, err := col.IngestSources(context.Background(), sources, cfg)
	if !errors.Is(err, errIngestInjected) {
		t.Fatalf("err=%v want injected sentinel", err)
	}
	var ingestErr *IngestError
	if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageStorage {
		t.Fatalf("err=%v want storage-stage IngestError", err)
	}
	if string(ingestErr.SourceID) != string(sources[n].ID) {
		t.Fatalf("error names %q want %q", ingestErr.SourceID, sources[n].ID)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and prove durability semantics.
	d2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = d2.Close() })
	col2, err := NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}

	completeParents := make([]string, 0, n)
	for i := 0; i < n; i++ {
		completeParents = append(completeParents, string(sources[i].ID))
	}
	// Fully ingested sources survive with their children.
	for _, p := range completeParents {
		children, err := col2.ChunkChildren([]byte(p))
		if err != nil || len(children) == 0 {
			t.Fatalf("committed source %q lost on reopen: children=%d err=%v", p, len(children), err)
		}
	}
	// The interrupted source and everything after it are fully absent.
	for i := n; i < len(sources); i++ {
		p := string(sources[i].ID)
		if raw, err := col2.Get(sources[i].ID); err != nil || len(raw) != 0 {
			t.Fatalf("interrupted source %q parent present on reopen", p)
		}
		if children, _ := col2.ChunkChildren(sources[i].ID); len(children) != 0 {
			t.Fatalf("interrupted source %q has orphaned children on reopen", p)
		}
	}
	assertIndexParity(t, col2, completeParents)
}

// TestIngestSourcesRetryConvergesAfterMidBatchFault injects a fault before
// either mutation boundary during a changed re-ingest. The storage error is
// surfaced with its source identity; a fault-free retry converges to exactly
// the new child set with full index parity and no stale rows.
func TestIngestSourcesRetryConvergesAfterMidBatchFault(t *testing.T) {
	_, d, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	sources := []SourceDocument{ingestTestSource("src-0", 0), ingestTestSource("src-1", 1)}
	parents := []string{"src-0", "src-1"}
	mustIngest(t, col, sources, cfg)

	changed := ingestTestSource("src-0", 7)
	cfg.hooks = &ingestFaultHooks{
		beforeInsert: func(i int) error {
			if i == 0 {
				return errIngestInjected
			}
			return nil
		},
	}
	_, err := col.IngestSources(context.Background(), []SourceDocument{changed, sources[1]}, cfg)
	if !errors.Is(err, errIngestInjected) {
		t.Fatalf("mid-batch fault err=%v want injected sentinel", err)
	}
	cfg.hooks = nil

	// Fault-free retry converges.
	mustIngest(t, col, []SourceDocument{changed, sources[1]}, cfg)
	assertIndexParity(t, col, parents)
	resp, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "tok7", TopK: 100})
	if err != nil {
		t.Fatalf("SearchText(tok7): %v", err)
	}
	if len(resp.Results) == 0 {
		t.Fatal("retry did not land the new children")
	}
	_ = d
}

func TestIngestSourcesCommitAmbiguityBoundariesReopenAndRetry(t *testing.T) {
	tests := []struct {
		name          string
		setHook       func(*ingestFaultHooks)
		wantParentNew bool
		wantChildren  bool
	}{
		{
			name: "after delete",
			setHook: func(h *ingestFaultHooks) {
				h.afterDelete = func(int) error { return errIngestInjected }
			},
		},
		{
			name: "after insert",
			setHook: func(h *ingestFaultHooks) {
				h.afterInsert = func(int) error { return errIngestInjected }
			},
			wantChildren: true,
		},
		{
			name: "after parent",
			setHook: func(h *ingestFaultHooks) {
				h.afterParent = func(int) error { return errIngestInjected }
			},
			wantParentNew: true,
			wantChildren:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, d, col := openIngestTestCollection(t, 256)
			oldSource := ingestTestSource("src-0", 0)
			newSource := ingestTestSource("src-0", 7)
			cfg := ingestTestCfg(256)
			mustIngest(t, col, []SourceDocument{oldSource}, cfg)

			hooks := &ingestFaultHooks{}
			tc.setHook(hooks)
			cfg.hooks = hooks
			_, err := col.IngestSources(context.Background(), []SourceDocument{newSource}, cfg)
			var ingestErr *IngestError
			if !errors.Is(err, errIngestInjected) || !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageStorage {
				t.Fatalf("boundary error=%v typed=%+v", err, ingestErr)
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint ambiguous state: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("Close ambiguous state: %v", err)
			}
			d2, err := backenddb.Open(backenddb.Options{Dir: dir})
			if err != nil {
				t.Fatalf("reopen ambiguous state: %v", err)
			}
			defer func() { _ = d2.Close() }()
			col2, err := NewCollectionManager(d2).OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection after ambiguous state: %v", err)
			}

			parentRaw, err := col2.Get([]byte("src-0"))
			if err != nil {
				t.Fatalf("Get parent: %v", err)
			}
			var parent map[string]any
			if err := json.Unmarshal(parentRaw, &parent); err != nil {
				t.Fatalf("decode parent: %v", err)
			}
			parentBody := parent["body"].(string)
			if gotNew := strings.Contains(parentBody, "tok7"); gotNew != tc.wantParentNew {
				t.Fatalf("parent new=%t want %t body=%q", gotNew, tc.wantParentNew, parentBody[:min(80, len(parentBody))])
			}
			children, err := col2.ChunkChildren([]byte("src-0"))
			if err != nil {
				t.Fatalf("ChunkChildren ambiguous state: %v", err)
			}
			if got := len(children) > 0; got != tc.wantChildren {
				t.Fatalf("children present=%t count=%d want %t", got, len(children), tc.wantChildren)
			}

			cfg.hooks = nil
			mustIngest(t, col2, []SourceDocument{newSource}, cfg)
			assertIndexParity(t, col2, []string{"src-0"})
			resp, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Query: "tok7", TopK: 100})
			if err != nil || len(resp.Results) == 0 {
				t.Fatalf("retry new text results=%d err=%v", len(resp.Results), err)
			}
		})
	}
}

func TestIngestSourcesIndependentParentsDoNotShareLifecycleLock(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	baseCfg := ingestTestCfg(256)
	mustIngest(t, col, []SourceDocument{ingestTestSource("src-a", 0), ingestTestSource("src-b", 1)}, baseCfg)

	firstAfterDelete := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondBeforeMutation := make(chan struct{})
	cfgA := baseCfg
	cfgA.hooks = &ingestFaultHooks{afterDelete: func(int) error {
		close(firstAfterDelete)
		<-releaseFirst
		return nil
	}}
	cfgB := baseCfg
	cfgB.hooks = &ingestFaultHooks{beforeInsert: func(int) error {
		close(secondBeforeMutation)
		return nil
	}}

	errs := make(chan error, 2)
	go func() {
		_, err := col.IngestSources(context.Background(), []SourceDocument{ingestTestSource("src-a", 7)}, cfgA)
		errs <- err
	}()
	<-firstAfterDelete
	go func() {
		_, err := col.IngestSources(context.Background(), []SourceDocument{ingestTestSource("src-b", 8)}, cfgB)
		errs <- err
	}()
	select {
	case <-secondBeforeMutation:
	case <-time.After(time.Second):
		t.Fatal("independent parent blocked before its mutation boundary")
	}
	close(releaseFirst)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("independent ingest: %v", err)
		}
	}
	assertIndexParity(t, col, []string{"src-a", "src-b"})
}

func TestIngestSourcesLifecycleLockHonorsCancellation(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	baseCfg := ingestTestCfg(256)
	mustIngest(t, col, []SourceDocument{ingestTestSource("src-cancel-lock", 0)}, baseCfg)

	entered := make(chan struct{})
	release := make(chan struct{})
	blockingCfg := baseCfg
	blockingCfg.hooks = &ingestFaultHooks{afterDelete: func(int) error {
		close(entered)
		<-release
		return nil
	}}
	firstDone := make(chan error, 1)
	go func() {
		_, err := col.IngestSources(context.Background(), []SourceDocument{ingestTestSource("src-cancel-lock", 1)}, blockingCfg)
		firstDone <- err
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := col.IngestSources(ctx, []SourceDocument{ingestTestSource("src-cancel-lock", 2)}, baseCfg)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting lifecycle lock err=%v want context deadline", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("canceled lifecycle acquisition took %s", elapsed)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("blocking ingest: %v", err)
	}
}

func TestIngestSourcesProgressCallbackCanReadCommittedParent(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	callbackErr := make(chan error, 1)
	cfg.Progress = func(progress IngestSourcesProgress) {
		children, err := col.ChunkChildren(progress.SourceID)
		if err == nil && len(children) == 0 {
			err = errors.New("progress callback observed no committed children")
		}
		callbackErr <- err
	}
	ingestDone := make(chan error, 1)
	go func() {
		_, err := col.IngestSources(context.Background(), []SourceDocument{ingestTestSource("src-progress-read", 0)}, cfg)
		ingestDone <- err
	}()
	select {
	case err := <-ingestDone:
		if err != nil {
			t.Fatalf("ingest: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("progress callback deadlocked on the committed parent's lifecycle lock")
	}
	if err := <-callbackErr; err != nil {
		t.Fatalf("progress callback: %v", err)
	}
}

// TestIngestSourcesCancelBetweenSources proves ctx cancellation stops the
// pipeline between sources: completed sources stay intact, unstarted sources
// remain untouched, and the cancellation is surfaced.
func TestIngestSourcesCancelBetweenSources(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	cfg.Concurrency = 1
	sources := ingestTestSources(6)
	parents := make([]string, len(sources))
	for i, s := range sources {
		parents[i] = string(s.ID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	stopped := false
	cfg.Progress = func(p IngestSourcesProgress) {
		mu.Lock()
		defer mu.Unlock()
		if p.SourcesCompleted >= 2 && !stopped {
			stopped = true
			cancel()
		}
	}

	res, err := col.IngestSources(ctx, sources, cfg)
	if err == nil {
		t.Fatal("cancellation not surfaced")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if len(res.Ingested) > 2 {
		t.Fatalf("res.Ingested=%d exceeds sources completed before cancellation", len(res.Ingested))
	}
	completed := make([]string, 0, len(res.Ingested))
	for _, o := range res.Ingested {
		completed = append(completed, string(o.ID))
	}
	for _, p := range completed {
		children, err := col.ChunkChildren([]byte(p))
		if err != nil || len(children) == 0 {
			t.Fatalf("completed source %q lost after cancellation", p)
		}
	}
	for _, p := range parents[len(res.Ingested):] {
		if raw, _ := col.Get([]byte(p)); len(raw) != 0 {
			t.Fatalf("unstarted source %q was written", p)
		}
	}
}

// TestIngestSourcesFlushCheckpointReopenMatrix walks the flush/checkpoint/
// reopen matrix over an ingested corpus and asserts index parity at every step.
func TestIngestSourcesFlushCheckpointReopenMatrix(t *testing.T) {
	dir, d, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	sources := ingestTestSources(3)
	parents := []string{"src-0", "src-1", "src-2"}
	mustIngest(t, col, sources, cfg)

	t.Run("flush", func(t *testing.T) {
		if err := col.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		assertIndexParity(t, col, parents)
	})
	t.Run("checkpoint", func(t *testing.T) {
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		assertIndexParity(t, col, parents)
	})

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	t.Run("reopen", func(t *testing.T) {
		d2, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() { _ = d2.Close() }()
		col2, err := NewCollectionManager(d2).OpenCollection("docs")
		if err != nil {
			t.Fatalf("reopen collection: %v", err)
		}
		assertIndexParity(t, col2, parents)
	})
}

// TestIngestSourcesBatchValidationEdges covers empty batches, duplicate
// source IDs, and empty-ID sources: all fail closed without mutation.
func TestIngestSourcesBatchValidationEdges(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)

	res, err := col.IngestSources(context.Background(), nil, cfg)
	if err != nil || len(res.Ingested) != 0 {
		t.Fatalf("empty batch: res=%+v err=%v", res, err)
	}

	dupes := []SourceDocument{
		{ID: []byte("same"), Fields: map[string]any{"body": "one"}},
		{ID: []byte("same"), Fields: map[string]any{"body": "two"}},
	}
	if _, err := col.IngestSources(context.Background(), dupes, cfg); err == nil {
		t.Fatal("duplicate source IDs accepted")
	} else if raw, _ := col.Get([]byte("same")); len(raw) != 0 {
		t.Fatal("duplicate-ID batch mutated the collection")
	}

	if _, err := col.IngestSources(context.Background(), []SourceDocument{{Fields: map[string]any{"body": "x"}}}, cfg); err == nil {
		t.Fatal("empty source ID accepted")
	}
}

// TestIngestSourcesConcurrentBoundedPool proves bounded concurrency ingests
// all sources correctly with multiple workers.
func TestIngestSourcesConcurrentBoundedPool(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 256)
	cfg := ingestTestCfg(256)
	cfg.Concurrency = 3
	sources := ingestTestSources(9)
	parents := make([]string, len(sources))
	for i, s := range sources {
		parents[i] = string(s.ID)
	}
	res := mustIngest(t, col, sources, cfg)
	if len(res.Ingested) != len(sources) {
		t.Fatalf("res.Ingested=%d want %d", len(res.Ingested), len(sources))
	}
	assertIndexParity(t, col, parents)
}

func TestAttachVectorsToChildrenNestedJSONPath(t *testing.T) {
	children := []chunkChild{{
		id:       []byte("parent#0"),
		document: []byte(`{"body":"chunk","payload":{"source":"raw"}}`),
	}}
	docs, err := attachVectorsToChildren(children, [][]float32{{1, 2, 3}}, "payload.embedding")
	if err != nil {
		t.Fatalf("attach vectors: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(docs[0], &doc); err != nil {
		t.Fatalf("unmarshal attached child: %v", err)
	}
	if _, ok := doc["payload.embedding"]; ok {
		t.Fatal("nested vector was written as a literal dotted key")
	}
	payload, ok := doc["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload=%T %v want object", doc["payload"], doc["payload"])
	}
	got, ok := payload["embedding"].([]any)
	if !ok || len(got) != 3 || got[0] != float64(1) || got[1] != float64(2) || got[2] != float64(3) {
		t.Fatalf("payload.embedding=%v want [1 2 3]", payload["embedding"])
	}
}

func TestIngestSourcesRejectsTextFieldVectorFieldCollision(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 8)
	cfg := ingestTestCfg(8)
	cfg.TextField = "embedding"
	res, err := col.IngestSources(context.Background(), []SourceDocument{{
		ID:     []byte("collision"),
		Fields: map[string]any{"embedding": "text that must not be overwritten"},
	}}, cfg)
	if err == nil {
		t.Fatal("TextField/vector field collision accepted")
	}
	var ingestErr *IngestError
	if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageChunk {
		t.Fatalf("err=%v want chunk-stage IngestError", err)
	}
	if len(res.Ingested) != 0 {
		t.Fatalf("collision rejection returned ingested outcomes: %+v", res.Ingested)
	}
	if raw, _ := col.Get([]byte("collision")); len(raw) != 0 {
		t.Fatal("collision rejection mutated the collection")
	}
}

func TestIngestSourcesRejectsAncestorTextVectorPaths(t *testing.T) {
	tests := []struct {
		name       string
		textField  string
		vectorPath string
	}{
		{name: "text parent", textField: "payload", vectorPath: "payload.embedding"},
		{name: "vector parent", textField: "payload.embedding", vectorPath: "payload"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, col := openIngestTestCollection(t, 8)
			col.meta.VectorIndexes[0].Field = tc.vectorPath
			cfg := ingestTestCfg(8)
			cfg.TextField = tc.textField
			res, err := col.IngestSources(context.Background(), []SourceDocument{{
				ID:     []byte("ancestor-collision"),
				Fields: map[string]any{"body": "must not be indexed"},
			}}, cfg)
			if err == nil {
				t.Fatal("ancestor TextField/vector field overlap accepted")
			}
			var ingestErr *IngestError
			if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageChunk {
				t.Fatalf("err=%v want chunk-stage IngestError", err)
			}
			if len(res.Ingested) != 0 {
				t.Fatalf("overlap rejection returned ingested outcomes: %+v", res.Ingested)
			}
			if raw, _ := col.Get([]byte("ancestor-collision")); len(raw) != 0 {
				t.Fatal("overlap rejection mutated the collection")
			}
		})
	}
}

func TestIngestSourcesRejectsVectorChunkMetadataOverlap(t *testing.T) {
	for _, field := range []string{
		chunking.MetaFieldParent,
		chunking.MetaFieldParent + ".embedding",
		chunking.MetaFieldOrdinal,
		chunking.MetaFieldOrdinal + ".embedding",
		chunking.MetaFieldKind,
		chunking.MetaFieldKind + ".embedding",
	} {
		t.Run(field, func(t *testing.T) {
			_, _, col := openIngestTestCollection(t, 8)
			col.meta.VectorIndexes[0].Field = field
			res, err := col.IngestSources(context.Background(), []SourceDocument{{
				ID:     []byte("reserved-vector-field"),
				Fields: map[string]any{"body": "must not be indexed"},
			}}, ingestTestCfg(8))
			if err == nil {
				t.Fatal("vector field overlaps chunk metadata but was accepted")
			}
			var ingestErr *IngestError
			if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageChunk {
				t.Fatalf("err=%v want chunk-stage IngestError", err)
			}
			if len(res.Ingested) != 0 {
				t.Fatalf("reserved vector field rejection returned ingested outcomes: %+v", res.Ingested)
			}
			if raw, _ := col.Get([]byte("reserved-vector-field")); len(raw) != 0 {
				t.Fatal("reserved vector field rejection mutated the collection")
			}
		})
	}
}

func TestIngestSourcesRejectsParentChildNamespaceCollision(t *testing.T) {
	tests := []struct {
		name        string
		parentIDs   []string
		collidingID string
	}{
		{name: "planned ordinal", parentIDs: []string{"doc", "doc#0"}, collidingID: "doc#0"},
		{name: "unplanned ordinal", parentIDs: []string{"doc", "doc#999"}, collidingID: "doc#999"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, col := openIngestTestCollection(t, 8)
			cfg := ingestTestCfg(8)
			sources := []SourceDocument{
				ingestTestSource(tc.parentIDs[0], 0),
				ingestTestSource(tc.parentIDs[1], 1),
			}
			res, err := col.IngestSources(context.Background(), sources, cfg)
			if err == nil {
				t.Fatal("parent/child namespace collision accepted")
			}
			var ingestErr *IngestError
			if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageChunk {
				t.Fatalf("err=%v want chunk-stage IngestError", err)
			}
			if !strings.Contains(err.Error(), tc.collidingID) {
				t.Fatalf("err=%v missing colliding ID", err)
			}
			if len(res.Ingested) != 0 {
				t.Fatalf("collision rejection returned ingested outcomes: %+v", res.Ingested)
			}
			for _, id := range tc.parentIDs {
				if raw, _ := col.Get([]byte(id)); len(raw) != 0 {
					t.Fatalf("collision rejection mutated %q", id)
				}
			}
		})
	}
}

func TestAttachVectorsToChildrenPropagatesNestedPathError(t *testing.T) {
	children := []chunkChild{{
		id:       []byte("parent#0"),
		document: []byte(`{"body":"chunk","payload":"raw"}`),
	}}
	docs, err := attachVectorsToChildren(children, [][]float32{{1, 2, 3}}, "payload.embedding")
	if err == nil || !strings.Contains(err.Error(), "non-object ancestor") {
		t.Fatalf("err=%v want nested path attachment error", err)
	}
	if docs != nil {
		t.Fatalf("docs=%v want nil on attachment error", docs)
	}
}

func TestIngestSourcesRejectsInvalidReplacementVectorsBeforeDelete(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 8)
	cfg := ingestTestCfg(8)
	original := ingestTestSource("replace-zero", 1)
	mustIngest(t, col, []SourceDocument{original}, cfg)
	oldChildren, err := col.ChunkChildren(original.ID)
	if err != nil || len(oldChildren) == 0 {
		t.Fatalf("old children=%d err=%v", len(oldChildren), err)
	}
	oldChild, err := col.Get(oldChildren[0])
	if err != nil {
		t.Fatalf("get old child: %v", err)
	}
	oldParent, err := col.Get(original.ID)
	if err != nil {
		t.Fatalf("get old parent: %v", err)
	}

	replacement := SourceDocument{ID: append([]byte(nil), original.ID...), Fields: map[string]any{
		"body": strings.Repeat(" ", 16),
	}}
	res, err := col.IngestSources(context.Background(), []SourceDocument{replacement}, cfg)
	if err == nil || !strings.Contains(err.Error(), "zero magnitude") {
		t.Fatalf("replacement err=%v want cosine zero-magnitude embed failure", err)
	}
	var ingestErr *IngestError
	if !errors.As(err, &ingestErr) || ingestErr.Stage != IngestStageEmbed {
		t.Fatalf("err=%v want embed-stage IngestError", err)
	}
	children, err := col.ChunkChildren(original.ID)
	if err != nil {
		t.Fatalf("children after rejected replacement: %v", err)
	}
	if len(children) != len(oldChildren) || string(children[0]) != string(oldChildren[0]) {
		t.Fatalf("stale children changed after invalid embedding: got=%q want=%q", children, oldChildren)
	}
	newChild, err := col.Get(oldChildren[0])
	if err != nil {
		t.Fatalf("get child after rejected replacement: %v", err)
	}
	if string(newChild) != string(oldChild) {
		t.Fatal("stale child document changed after invalid embedding")
	}
	newParent, err := col.Get(original.ID)
	if err != nil {
		t.Fatalf("get parent after rejected replacement: %v", err)
	}
	if string(newParent) != string(oldParent) {
		t.Fatal("parent changed after invalid embedding")
	}
	if len(res.Ingested) != 0 {
		t.Fatalf("invalid replacement returned ingested outcomes: %+v", res.Ingested)
	}
}

func TestIngestSourcesProgressCompletionNumbersMonotonic(t *testing.T) {
	_, _, col := openIngestTestCollection(t, 16)
	cfg := ingestTestCfg(16)
	cfg.Concurrency = 8
	sources := ingestTestSources(16)
	var mu sync.Mutex
	progress := make([]IngestSourcesProgress, 0, len(sources))
	cfg.Progress = func(p IngestSourcesProgress) {
		mu.Lock()
		progress = append(progress, p)
		mu.Unlock()
	}
	if _, err := col.IngestSources(context.Background(), sources, cfg); err != nil {
		t.Fatalf("IngestSources: %v", err)
	}
	if len(progress) != len(sources) {
		t.Fatalf("progress callbacks=%d want %d", len(progress), len(sources))
	}
	for i, p := range progress {
		if p.SourcesCompleted != i+1 || p.SourcesTotal != len(sources) {
			t.Fatalf("progress[%d]=%+v want completion=%d total=%d", i, p, i+1, len(sources))
		}
	}
}

// BenchmarkIngestSources10K measures the one-call chunk/embed/index path at
// the C8 before-state scale: 10,000 documents and the hashing embedder at
// 256 dimensions. Stage shares are reported from the pipeline's wall counters.
func BenchmarkIngestSources10K(b *testing.B) {
	_, _, col := openIngestTestCollection(b, 256)
	sources := make([]SourceDocument, 10_000)
	for i := range sources {
		sources[i] = SourceDocument{
			ID:     []byte(fmt.Sprintf("bench-%d", i)),
			Fields: map[string]any{"body": "benchmark ingestion document with enough text for one chunk"},
		}
	}
	cfg := ingestTestCfg(256)
	cfg.Concurrency = 4
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		start := time.Now()
		res, err := col.IngestSources(context.Background(), sources, cfg)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(sources))/elapsed.Seconds(), "docs/sec")
		stageTotal := res.ChunkNanos + res.EmbedNanos + res.IndexNanos
		if stageTotal > 0 {
			b.ReportMetric(float64(res.ChunkNanos)/float64(stageTotal)*100, "chunk-%")
			b.ReportMetric(float64(res.EmbedNanos)/float64(stageTotal)*100, "embed-%")
			b.ReportMetric(float64(res.IndexNanos)/float64(stageTotal)*100, "index-%")
		}
	}
}
