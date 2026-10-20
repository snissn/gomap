package collections

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeDocumentVectorBinaryIDLifecycle(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
			var database *treedb.DB
			var manager *CollectionManager
			var col *Collection
			closeDB := func() {
				t.Helper()
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Fatal(err)
					}
					manager = nil
				}
				if database != nil {
					if err := database.Close(); err != nil {
						t.Fatal(err)
					}
					database = nil
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
				handle, err := database.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager = NewCollectionManager(handle)
			}
			open()
			meta := nativeDocumentMetadataFixture(t, format)
			if _, err := manager.CreateCollection(&meta); err != nil {
				t.Fatal(err)
			}
			var err error
			col, err = manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			document := func(x, y float64) []byte {
				if format == DocumentFormatJSON {
					if x == 1 {
						return []byte(`{"embedding":[1,0]}`)
					}
					return []byte(`{"embedding":[0,1]}`)
				}
				raw, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{x, y}}})
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			seed, updated := document(1, 0), document(0, 1)
			// Include invalid UTF-8, embedded NUL, and a valid U+FFFD byte sequence:
			// replacement of invalid UTF-8 would alias these otherwise distinct IDs.
			id := []byte{0xff, 0, 0x80, 'x'}
			other := []byte("\ufffd\x00\ufffdx")
			if _, err := col.Insert(id, seed); err != nil {
				t.Fatal(err)
			}
			if _, err := col.Insert(other, updated); err != nil {
				t.Fatal(err)
			}
			held, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding"})
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			check := func(searcher *VectorIndexSearcher, want []byte, doc []byte, liveRows uint64) {
				t.Helper()
				response, err := searcher.Search(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1, IncludeDocuments: true})
				if err != nil || len(response.Results) != 1 || !bytes.Equal(response.Results[0].ID, want) || !bytes.Equal(response.Results[0].Document, doc) {
					t.Fatalf("lossless held/current result=%+v err=%v wantID=%x", response, err, want)
				}
				if response.Stats.CandidateRows != liveRows || response.Stats.OpenSearcherCalls != 0 || response.Stats.OpenSetupInTimedLoop != 0 {
					t.Fatalf("held/reopened domain/setup stats=%+v, want%d live rows/no per-search open", response.Stats, liveRows)
				}
			}
			check(held, id, seed, 2)
			if _, _, err := col.Update(id, func(raw []byte) ([]byte, bool, error) {
				if !bytes.Equal(raw, seed) {
					t.Fatalf("binary callback read %x", raw)
				}
				return updated, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			check(held, id, seed, 2)
			if deleted, err := col.DeleteDocument(other); err != nil || !deleted {
				t.Fatalf("binary neighbor delete=%v err=%v", deleted, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check(held, id, seed, 2)
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			closeDB()
			open()
			col, err = manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			// Reopening imports a bound but unverified exact immutable graph. The
			// public searcher must validate its bytes before memoizing that certificate.
			cold, err := manager.nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			domain, ok := cold.collection.domains.Get("vector-index/embedding")
			if !ok || domain.documentGraph.verified.Load() {
				t.Fatal("reopened graph skipped cold validation")
			}
			current, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding"})
			if err != nil {
				t.Fatal(err)
			}
			check(current, id, updated, 1)
			// A capture of an empty native runtime imports a PRIVATE engine. A
			// second capture may therefore own a distinct descriptor for the same
			// physical graph. Check the descriptor actually validated by this
			// searcher, without granting authority to the earlier cold capture.
			actual := current.nativeDocument
			actualDomain, present := actual.collection.domains.Get("vector-index/embedding")
			if !present {
				t.Fatal("searcher lost its captured graph descriptor")
			}
			base, _ := actual.capture.State().(*nativeEngineVersion)
			if base == nil {
				base = actual.privateEngine
			}
			generation, err := nativeDocumentGenerationAtRead(actual, base)
			if err != nil {
				t.Fatal(err)
			}
			def, present := findVectorIndex(actual.meta.VectorIndexes, "embedding")
			if !present {
				t.Fatal("searcher lost its captured index definition")
			}
			schema, err := nativeDocumentGraphSchema(def)
			if err != nil {
				t.Fatal(err)
			}
			if !actualDomain.documentGraph.valid(actual.collection.incarnation, generation, schema) {
				t.Fatal("validated searcher's exact graph tuple was not certified")
			}
			if actualDomain != domain && domain.documentGraph.verified.Load() {
				t.Fatal("validation leaked into an independently owned cold descriptor")
			}
			if err := cold.Close(); err != nil {
				t.Fatal(err)
			}
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := col.Get(other); err != nil || got != nil {
				t.Fatalf("deleted replacement-ID returned %x err=%v", got, err)
			}
			if deleted, err := col.DeleteDocument(id); err != nil || !deleted {
				t.Fatalf("exact binary delete=%v err=%v", deleted, err)
			}
			if got, err := col.Get(id); err != nil || got != nil {
				t.Fatalf("deleted byte-ID=%x err=%v", got, err)
			}
		})
	}
}

// Original durable frames exercise both singleton and composite startup reducers
// with byte IDs. No native engine or caller-provided replay token is fabricated.
func TestNativeDocumentVectorBinaryIDOriginalReplay(t *testing.T) {
	dir := t.TempDir()
	if err := saveCollectionCommandWALFormat(dir); err != nil {
		t.Fatal(err)
	}
	meta := nativeDocumentMetadataFixture(t, DocumentFormatJSON)
	meta.nativeDocumentCoverageVersion = nativeDocumentVectorCoverageVersion
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, meta))
	seeded := openCollectionCommandWALDB(t, dir)
	opening := seeded.AcquireSnapshot()
	identity, present, err := getSystemValue(opening, nativeCatalogIncarnationPrefix+meta.Name)
	if err != nil || !present {
		t.Fatalf("actual replay incarnation absent: %v", err)
	}
	incarnation, err := nativeCatalogIdentityValue(identity, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := opening.Close(); err != nil {
		t.Fatal(err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatal(err)
	}
	id := []byte{0xff, 0, 0x80}
	a, b := []byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)
	insert, err := commitlog.EncodeCollectionInsertBatchByIDPayload(meta.Name, []commitlog.CollectionDocument{{ID: id, Document: a}})
	if err != nil {
		t.Fatal(err)
	}
	update := func(doc []byte) []byte {
		raw, err := commitlog.EncodeCollectionUpdateBatchByIDPayload(meta.Name, []commitlog.CollectionDocument{{ID: id, Document: doc}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// Bind the composite to the identity actually installed by CatalogCreate.
	composite, err := commitlog.EncodeCompositeMutationPayloadTo(nil, []commitlog.CompositeMutationMember{
		{Kind: commitlog.CommandKindCollectionUpdateBatchByID, PayloadFormat: commitlog.PayloadFormatCollectionUpdateBatchByIDV1, Incarnation: incarnation, Payload: update(b)},
		{Kind: commitlog.CommandKindCollectionUpdateBatchByID, PayloadFormat: commitlog.PayloadFormatCollectionUpdateBatchByIDV1, Incarnation: incarnation, Payload: update(a)},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Use one original writer so the sequence is physically contiguous.
	walDir := backenddb.WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	writer, err := commitlog.NewWriter(filepath.Join(walDir, commitlog.CommandSegmentName(0, 2)))
	if err != nil {
		t.Fatal(err)
	}
	for i, frame := range []commitlog.CommandEnvelope{
		{Kind: commitlog.CommandKindCollectionInsertBatchByID, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionInsertBatchByIDV1, Payload: insert},
		{Kind: commitlog.CommandKindCompositeMutation, Scope: commitlog.CommandScopeSystem, PayloadFormat: commitlog.PayloadFormatCompositeMutationV1, Payload: composite},
		{Kind: commitlog.CommandKindCollectionUpdateBatchByID, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionUpdateBatchByIDV1, Payload: update(b)},
	} {
		frame.Version = commitlog.CommandFrameVersionV2
		frame.LSN = uint64(i + 2)
		frame.DurabilityClass = commitlog.CommandDurabilityDurable
		if err := writer.AppendCommand(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer d.Close()
	if d.State().AppliedCommandLSN != 4 {
		t.Fatalf("actual startup applied=%d", d.State().AppliedCommandLSN)
	}
	snapshot := d.AcquireSnapshot()
	defer snapshot.Close()
	key, err := nativeDocumentGenerationKey(incarnation)
	if err != nil {
		t.Fatal(err)
	}
	raw, present, err := getSystemValue(snapshot, key)
	if err != nil || !present || !bytes.Equal(raw, encodeRootID(2)) {
		t.Fatalf("singleton+composite replay counter=%x present=%v err=%v", raw, present, err)
	}
	manager := NewCollectionManager(d)
	defer manager.Close()
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get(id); err != nil || !bytes.Equal(got, b) {
		t.Fatalf("original replay byte-ID value=%x err=%v", got, err)
	}
}
