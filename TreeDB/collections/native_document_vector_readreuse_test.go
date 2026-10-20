package collections

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeDocumentVectorReopenedCutRepresentationReuse(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	var database *treedb.DB
	var backend *backenddb.DB
	var manager *CollectionManager
	var held *VectorIndexSearcher
	closeDB := func() {
		t.Helper()
		if held != nil {
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
		}
		if database != nil {
			pins := backend.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("final resource identities=%+v", s)
			}
		}
	}
	t.Cleanup(closeDB)
	open := func() {
		t.Helper()
		var err error
		database, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(backend)
		if manager.nativeJSON == nil {
			t.Fatal("not the actual native factory")
		}
	}
	open()
	meta := nativeDocumentMetadataFixture(t, DocumentFormatJSON)
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	closeDB()
	open()
	// This original capture holds the physical-only predecessor throughout the
	// representation replacement; it must not gain a semantic edge in place.
	original, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	if original.State() != nil {
		t.Fatal("fixture did not reopen a physical-only cut")
	}
	physical, ok := original.Snapshot().StateToken()
	if !ok {
		t.Fatal("missing physical state")
	}
	sequence, next := original.LogicalSequence(), backend.CommandWALNextLSN()
	allocator := original.NativeAllocator().(*memtable.COWBudget)
	cold, err := manager.nativeJSON.Capture(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer cold.Close()
	engine, ok := cold.capture.State().(*nativeEngineVersion)
	if !ok || cold.privateEngine != nil {
		t.Fatal("real complete import was not retained by its exact cut")
	}
	if !engine.NativeSemanticImportEvidence().Matches(engine, cold.capture.NativeAllocator(), backenddb.NativeSemanticImportBasis(physical), sequence) {
		t.Fatal("complete physical import lost its actual basis proof")
	}
	domain, ok := cold.collection.domains.Get("vector-index/embedding")
	if !ok || domain.documentGraph.verified.Load() {
		t.Fatal("import bypassed mandatory cold certification")
	}
	col, err = manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	search := func(want string) {
		t.Helper()
		response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
		if err != nil || len(response.Results) != 1 || !bytes.Equal(response.Results[0].ID, []byte(want)) {
			t.Fatalf("current search want=%s response=%+v err=%v", want, response, err)
		}
	}
	search("a")
	if !domain.documentGraph.verified.Load() {
		t.Fatal("public search did not certify its actual captured graph")
	}
	current, err := manager.nativeJSON.Capture(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	currentDomain, _ := current.collection.domains.Get("vector-index/embedding")
	if current.privateEngine != nil || current.capture.State() != engine || currentDomain != domain {
		t.Fatal("independent read re-imported the unchanged physical graph")
	}
	if state, _ := current.capture.Snapshot().StateToken(); state != physical || current.capture.LogicalSequence() != sequence || backend.CommandWALNextLSN() != next || original.State() != nil {
		t.Fatal("representation reuse changed original cut or publication frontiers")
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	held, err = col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return []byte(`{"embedding":[-1,0]}`), true, nil }); err != nil {
		t.Fatal(err)
	}
	search("b")
	response, err := held.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
	if err != nil || len(response.Results) != 1 || !bytes.Equal(response.Results[0].ID, []byte("a")) {
		t.Fatalf("held search changed after successor publication: %+v %v", response, err)
	}
	successor, err := manager.nativeJSON.Capture(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	nextEngine, ok := successor.capture.State().(*nativeEngineVersion)
	successorToken, _ := successor.capture.Snapshot().StateToken()
	if !ok || nextEngine == engine || nextEngine.NativeSemanticImportEvidence().Matches(nextEngine, successor.capture.NativeAllocator(), backenddb.NativeSemanticImportBasis(successorToken), successor.capture.LogicalSequence()) {
		t.Fatal("logical successor inherited complete physical-import authority")
	}
	if err := successor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	search("b")
	if err := cold.Close(); err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	closeDB()
	if stats := allocator.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("final imported owner accounting=%+v", stats)
	}
	open()
	col, err = manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	search("b")
}
