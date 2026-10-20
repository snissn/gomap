package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Use the real native factory and default finite owner, never a fabricated
// graph/certificate. These controls observe source authority through captures.
func openNativeDocumentContract(t *testing.T, formats ...DocumentFormat) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		pins := handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
			t.Errorf("live source resources after Close: %+v", s)
		}
	})
	format := DocumentFormatJSON
	if len(formats) != 0 {
		format = formats[0]
	}
	meta := nativeDocumentMetadataFixture(t, format)
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	return database, handle, manager, col
}

func nativeDocumentContractGeneration(t *testing.T, manager *CollectionManager, name string) (uint64, uint64) {
	t.Helper()
	read, err := manager.nativeJSON.Capture(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	base, _ := read.capture.State().(*nativeEngineVersion)
	if base == nil {
		base = read.privateEngine
	}
	generation, err := nativeDocumentGenerationAtRead(read, base)
	if err != nil {
		t.Fatal(err)
	}
	return read.collection.incarnation, generation
}

func TestNativeDocumentVectorCanonicalFrameReduction(t *testing.T) {
	database, _, manager, col := openNativeDocumentContract(t)
	a := []byte(`{"embedding":[1,0],"note":"a"}`)
	b := []byte(`{"embedding":[0,1],"note":"b"}`)
	if _, err := col.Insert([]byte("one"), a); err != nil {
		t.Fatal(err)
	}
	incarnation, generation := nativeDocumentContractGeneration(t, manager, col.name)
	if generation != 1 {
		t.Fatalf("first inserted document generation=%d", generation)
	}
	// Two updates in ONE original canonical composite restore its opening row.
	tx, err := manager.nativeJSON.Begin(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	for _, document := range [][]byte{b, a} {
		if _, err := tx.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("one"), Update: func([]byte) ([]byte, bool, error) { return bytes.Clone(document), true, nil }}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, got := nativeDocumentContractGeneration(t, manager, col.name); got != 1 {
		t.Fatalf("one frame A->B->A advanced source to %d", got)
	}
	// The SAME changes in two separately acknowledged frames contribute twice.
	for _, document := range [][]byte{b, a} {
		if _, changed, err := col.Update([]byte("one"), func([]byte) ([]byte, bool, error) { return bytes.Clone(document), true, nil }); err != nil || !changed {
			t.Fatalf("separate update: changed=%v err=%v", changed, err)
		}
	}
	if _, got := nativeDocumentContractGeneration(t, manager, col.name); got != 3 {
		t.Fatalf("two frames advanced source to %d want3", got)
	}
	// A changed ordinary field still changes source authority despite identical vector.
	note := []byte(`{"embedding":[1,0],"note":"different"}`)
	if _, _, err := col.Update([]byte("one"), func([]byte) ([]byte, bool, error) { return note, true, nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.Update([]byte("one"), func(raw []byte) ([]byte, bool, error) { return raw, true, nil }); err != nil {
		t.Fatal(err)
	}
	if deleted, err := col.DeleteDocument([]byte("absent")); err != nil || deleted {
		t.Fatalf("missing delete=%v: %v", deleted, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if gotInc, got := nativeDocumentContractGeneration(t, manager, col.name); gotInc != incarnation || got != 4 {
		t.Fatalf("noops/checkpoint changed authority: incarnation=%d generation=%d", gotInc, got)
	}
	if got, err := col.Get([]byte("one")); err != nil || !bytes.Equal(got, note) {
		t.Fatalf("final stored row=%s err=%v", got, err)
	}
}

func TestNativeDocumentVectorConditionalCallbacks(t *testing.T) {
	_, handle, manager, col := openNativeDocumentContract(t)
	seed := []byte(`{"embedding":[1,0],"note":"original"}`)
	if _, err := col.Insert([]byte("one"), seed); err != nil {
		t.Fatal(err)
	}
	check := func(next uint64) {
		t.Helper()
		if handle.CommandWALNextLSN() != next {
			t.Fatalf("decline/error assigned a frame: %d want%d", handle.CommandWALNextLSN(), next)
		}
		if _, got := nativeDocumentContractGeneration(t, manager, col.name); got != 1 {
			t.Fatalf("decline/error changed source=%d", got)
		}
		if got, err := col.Get([]byte("one")); err != nil || !bytes.Equal(got, seed) {
			t.Fatalf("callback changed row=%s err=%v", got, err)
		}
	}
	sentinel := errors.New("callback refusal")
	for _, variant := range []string{"false", "error", "panic"} {
		t.Run(variant, func(t *testing.T) {
			next := handle.CommandWALNextLSN()
			called := false
			var panicked any
			var deleted bool
			var err error
			func() {
				defer func() { panicked = recover() }()
				deleted, err = col.DeleteDocumentIf([]byte("one"), func(raw []byte) (bool, error) {
					called = true
					if !bytes.Equal(raw, seed) {
						t.Fatalf("wrong opening row=%s", raw)
					}
					raw[0] = '!'
					switch variant {
					case "error":
						return true, sentinel
					case "panic":
						panic(sentinel)
					}
					return false, nil
				})
			}()
			if !called || deleted {
				t.Fatalf("callback called=%v deleted=%v", called, deleted)
			}
			if variant == "error" && !errors.Is(err, sentinel) {
				t.Fatalf("lost original callback cause: %v", err)
			}
			if variant == "false" && err != nil {
				t.Fatal(err)
			}
			if variant == "panic" && panicked != sentinel {
				t.Fatalf("predicate panic changed: %v", panicked)
			}
			check(next)
		})
	}
	// Update recovers callback panic through its original batch wrapper; it must
	// preserve the cause and execute original transaction Close before returning.
	next := handle.CommandWALNextLSN()
	if _, _, err := col.Update([]byte("one"), func([]byte) ([]byte, bool, error) { panic(sentinel) }); err == nil {
		t.Fatal("Update swallowed panic")
	}
	check(next)
	if deleted, err := col.DeleteDocumentIf([]byte("one"), func(raw []byte) (bool, error) { return bytes.Equal(raw, seed), nil }); err != nil || !deleted {
		t.Fatalf("accepted delete=%v err=%v", deleted, err)
	}
	if _, got := nativeDocumentContractGeneration(t, manager, col.name); got != 2 {
		t.Fatalf("accepted delete source=%d", got)
	}
}

func TestNativeDocumentVectorColdCertificateAndSearcherClose(t *testing.T) {
	database, _, manager, col := openNativeDocumentContract(t)
	if _, err := col.Insert([]byte("one"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	read, err := manager.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	domain, ok := read.collection.domains.Get("vector-index/embedding")
	if !ok || domain.baseRoot == 0 {
		t.Fatal("fixture lacks physical graph")
	}
	// A generic descriptor is intentionally NOT certified even for the same root.
	// Use the original constructor/admission and release; never mutate published state.
	fresh, err := newNativeDomainState(domain.name, domain.baseRoot, domain.interpretation, domain.storagePolicy, nil, func(n uint64) (cowbtree.OwnedMapLease, error) {
		return read.capture.NativeAllocator().AcquireRetention(n)
	})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.documentGraph.version != 0 {
		t.Fatal("generic descriptor inherited graph authority")
	}
	record, err := (nativeDomainLifetime{}).Release(fresh)
	if err != nil {
		t.Fatal(err)
	}
	fresh = nil
	record.Complete()
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := searcher.Search(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1}); err != nil {
		t.Fatal(err)
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	if err := searcher.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := searcher.Search(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1}); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed searcher returned %v", err)
	}
	if _, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestNativeDocumentVectorRejectsLossyNodeIdentity(t *testing.T) {
	for _, raw := range []string{`{"document_id":"legacy","vector":[1,0]}`, `{"document_id":"conflict","document_id_bytes":"/w==","vector":[1,0]}`, `{"document_id":"","document_id_bytes":"","vector":[1,0]}`} {
		if _, err := decodeNativeDocumentGraphNode([]byte(raw), json.Unmarshal); err == nil {
			t.Fatalf("ambiguous/lossy v5 node accepted: %s", raw)
		}
	}
}

// A closer tombstone remains a routing waypoint, but cannot evict the only
// live answer from a bounded public result heap. The retained cut keeps A.
func TestNativeDocumentVectorDeletedRoutingRetainsLiveResults(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			_, _, _, col := openNativeDocumentContract(t, format)
			a, b := []byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)
			if format == DocumentFormatBSON {
				var err error
				a, err = bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0}}})
				if err != nil {
					t.Fatal(err)
				}
				b, err = bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{0.0, 1.0}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{a, b}); err != nil {
				t.Fatal(err)
			}
			held, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding"})
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if deleted, err := col.DeleteDocument([]byte("a")); err != nil || !deleted {
				t.Fatalf("delete A=%v: %v", deleted, err)
			}
			opts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1, EfSearch: 1}
			assertStats := func(response VectorIndexSearchResponse, rows, opens uint64) {
				t.Helper()
				if response.Stats.CandidateRows != rows || response.Stats.SearchRouteNativeRuntime != 1 || response.Stats.OpenSearcherCalls != opens || response.Stats.OpenSetupInTimedLoop != opens {
					t.Fatalf("captured domain/setup stats=%+v, want%d live rows/%d opens", response.Stats, rows, opens)
				}
			}
			old, err := held.Search(opts)
			if err != nil || len(old.Results) != 1 || string(old.Results[0].ID) != "a" {
				t.Fatalf("held answer=%+v: %v", old, err)
			}
			assertStats(old, 2, 0)
			var buffer VectorIndexSearchBuffer
			current, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: "embedding", Query: opts.Query, TopK: opts.TopK, EfSearch: opts.EfSearch}, &buffer)
			if err != nil || len(current.Results) != 1 || string(current.Results[0].ID) != "b" {
				t.Fatalf("current answer=%+v: %v", current, err)
			}
			assertStats(current, 1, 1)
			old, err = held.Search(VectorIndexSearcherSearchOptions{Query: opts.Query, TopK: 0})
			if err != nil || len(old.Results) != 0 {
				t.Fatalf("held zero-result search=%+v: %v", old, err)
			}
			assertStats(old, 2, 0)
			current, err = col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding", Query: opts.Query, TopK: 0})
			if err != nil || len(current.Results) != 0 {
				t.Fatalf("current zero-result search=%+v: %v", current, err)
			}
			assertStats(current, 1, 1)
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
