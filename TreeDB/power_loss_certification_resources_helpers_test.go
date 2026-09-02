package treedb_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	authoritativeDocumentCollection = "certification-docs"
	authoritativePhysicalCollection = "certification-physical"
	authoritativeVectorCollection   = "certification-vectors"
	authoritativeTemplateCollection = "certification-templates"
	authoritativeDocumentCount      = 32
	authoritativePrimaryDocument    = 7
)

type authoritativeResourceWitness struct {
	primaryID      []byte
	primaryJSON    []byte
	templateID     []byte
	templateJSON   []byte
	dictionaryKey  []byte
	dictionaryData []byte
}

func prepareAuthoritativeResourceWitness(t *testing.T, database *treedb.DB, dir string, backgroundErrors <-chan error) authoritativeResourceWitness {
	t.Helper()
	backend := treedb.PowerLossCertificationBackendForTest(database)
	if backend == nil {
		t.Fatal("public TreeDB handle has no collection backend")
	}
	manager := collections.NewCollectionManager(backend)
	meta := collections.CollectionMeta{
		Name: authoritativeDocumentCollection,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
		Indexes: []collections.IndexDefinition{{
			Name: "by_title", Field: "title", ValueType: collections.IndexValueString,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name: "lexical", Version: collections.TextIndexVersionV2,
			Fields: []collections.TextIndexField{{Field: "title", Weight: 2}, {Field: "body"}}, StorePositions: true,
		}},
	}
	physicalMeta := collections.CollectionMeta{
		Name: authoritativePhysicalCollection,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:        true,
				ProfileSupport: collections.ColumnStoreProfileBenchmarkRelaxed,
				Columns: []collections.ColumnStoreColumn{
					{Name: "score", Path: "score", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
					{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset, Dictionary: true},
				},
			},
		},
	}
	vectorMeta := collections.CollectionMeta{
		Name: authoritativeVectorCollection,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:        true,
				ProfileSupport: collections.ColumnStoreProfileBenchmarkRelaxed,
				Columns: []collections.ColumnStoreColumn{
					{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, Owner: collections.TypedStorageOwnerColumnPart, VectorDims: 3},
				},
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine,
			Dimensions: 3, M: 2, Strategy: collections.VectorIndexStrategyColumnGraph,
		}},
	}
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateCollection(&physicalMeta); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateCollection(&vectorMeta); err != nil {
		t.Fatal(err)
	}
	templateMeta := collections.CollectionMeta{
		Name:    authoritativeTemplateCollection,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatTemplateV1},
		Indexes: []collections.IndexDefinition{{Name: "by_title", Field: "title", ValueType: collections.IndexValueString}},
	}
	if _, err := manager.CreateCollection(&templateMeta); err != nil {
		t.Fatal(err)
	}

	ids := make([][]byte, authoritativeDocumentCount)
	docs := make([][]byte, authoritativeDocumentCount)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%02d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"durability beacon%02d","body":"stable resource closure","kind":"kind_%s","score":%d,"embedding":[1,%d,%d]}`, i, []string{"even", "odd"}[i%2], i, i, i*i))
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	physicalCollection, err := manager.OpenCollection(physicalMeta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := physicalCollection.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := physicalCollection.Flush(); err != nil {
		t.Fatal(err)
	}
	vectorCollection, err := manager.OpenCollection(vectorMeta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vectorCollection.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := vectorCollection.Flush(); err != nil {
		t.Fatal(err)
	}
	templateCollection, err := manager.OpenCollection(templateMeta.Name)
	if err != nil {
		t.Fatal(err)
	}
	templateDocs := make([][]byte, len(docs))
	for i := range templateDocs {
		templateDocs[i], err = collections.EncodeTemplateV1DocumentJSON(docs[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := templateCollection.InsertBatch(ids, templateDocs); err != nil {
		t.Fatal(err)
	}
	if err := templateCollection.Flush(); err != nil {
		t.Fatal(err)
	}

	// This separately exercises maindb value-log dictionary compression and its
	// dictdb dependency. Collection TemplateV1 above witnesses collection template
	// roots only: public treedb.Open deliberately forces live templatedb value-log
	// compression off, so this test makes no claim that it is active.
	// Exactly one minimum training cohort keeps dictionary publication and its
	// durability-event count deterministic while still crossing TrainBytes.
	writeDictionaryTrainingBatch(t, database, "certification/dict-training/a", 8)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	waitForPublishedValueLogDictionary(t, database, backgroundErrors)
	dictionaryKey, dictionaryData := writeDictionaryTrainingBatch(t, database, "certification/dict-frame/b", 1)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	requireDictionaryEncodedValueLogFrame(t, database, filepath.Join(dir, "maindb", "value_vlog"), dictionaryKey, database.Stats())
	status, err := vectorCollection.RebuildVectorIndex("embedding_graph")
	if err != nil {
		t.Fatal(err)
	}
	if status.State != collections.VectorIndexStateColumnGraphLoaded || !status.Loaded || status.RebuildNeeded {
		t.Fatalf("RebuildVectorIndex status=%+v want loaded column_graph state", status)
	}

	return authoritativeResourceWitness{
		primaryID:      append([]byte(nil), ids[authoritativePrimaryDocument]...),
		primaryJSON:    append([]byte(nil), docs[authoritativePrimaryDocument]...),
		templateID:     append([]byte(nil), ids[authoritativePrimaryDocument]...),
		templateJSON:   append([]byte(nil), docs[authoritativePrimaryDocument]...),
		dictionaryKey:  dictionaryKey,
		dictionaryData: dictionaryData,
	}
}

func assertAuthoritativeResourceWitness(t *testing.T, reopened *treedb.DB, witness authoritativeResourceWitness) {
	t.Helper()
	backend := treedb.PowerLossCertificationBackendForTest(reopened)
	if backend == nil {
		t.Fatal("reopened public TreeDB handle has no collection backend")
	}
	manager := collections.NewCollectionManager(backend)
	collection, err := manager.OpenCollection(authoritativeDocumentCollection)
	if err != nil {
		t.Fatal(err)
	}

	primary, err := collection.Get(witness.primaryID)
	if err != nil {
		t.Fatalf("Collection.Get(%q): %v", witness.primaryID, err)
	}
	assertJSONDocumentEqual(t, primary, witness.primaryJSON, "primary Collection.Get")

	secondaryIDs, err := collection.FindByIndexValue("by_title", "durability beacon07")
	if err != nil {
		t.Fatalf("FindByIndexValue: %v", err)
	}
	if len(secondaryIDs) != 1 || !bytes.Equal(secondaryIDs[0], witness.primaryID) {
		t.Fatalf("FindByIndexValue ids=%q want [%q]", secondaryIDs, witness.primaryID)
	}

	text, err := collection.SearchText(collections.TextSearchOptions{
		IndexName: "lexical", Query: "beacon07", TopK: 8,
		ResultMode: collections.TextSearchResultModeScoreOnly,
	})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if !textSearchContainsDocument(text, witness.primaryID) {
		t.Fatalf("SearchText results=%+v want document %q", text.Results, witness.primaryID)
	}

	physicalCollection, err := manager.OpenCollection(authoritativePhysicalCollection)
	if err != nil {
		t.Fatal(err)
	}
	column, err := physicalCollection.RunColumnPhysicalQuery(collections.ColumnPhysicalQueryRequest{
		Kind: collections.ColumnPhysicalQueryGroupCount, GroupColumn: "kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	wantGroups := map[string]int{"kind_even": authoritativeDocumentCount / 2, "kind_odd": authoritativeDocumentCount / 2}
	if got := columnGroupCounts(column.Groups); !reflect.DeepEqual(got, wantGroups) {
		t.Fatalf("RunColumnPhysicalQuery groups=%v want=%v diagnostics=%+v", got, wantGroups, column.Diagnostics)
	}
	diagnostics := column.Diagnostics
	if diagnostics.ManifestRoot == 0 || diagnostics.AssetRefs == 0 || diagnostics.DictionaryCodeHits == 0 ||
		diagnostics.StorageSource == collections.ColumnPhysicalQueryStorageSourceRowScan ||
		diagnostics.StorageSource == collections.ColumnPhysicalQueryStorageSourceFallback ||
		diagnostics.FallbackReason != collections.ColumnPhysicalQueryFallbackNone ||
		diagnostics.RowMaterializations != 0 || diagnostics.DocumentMaterializations != 0 ||
		diagnostics.ReduceRows != authoritativeDocumentCount {
		t.Fatalf("RunColumnPhysicalQuery diagnostics=%+v want physical dictionary authority without row/document fallback", diagnostics)
	}

	vectorCollection, err := manager.OpenCollection(authoritativeVectorCollection)
	if err != nil {
		t.Fatal(err)
	}
	vector, err := vectorCollection.SearchVectorIndex(collections.VectorIndexSearchOptions{
		IndexName: "embedding_graph", Query: []float32{1, 7, 49}, TopK: 1, EfSearch: authoritativeDocumentCount,
		StatsMode: collections.VectorIndexSearchStatsModeBenchmarkDebug,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if vector.Status.State != collections.VectorIndexStateColumnGraphLoaded || !vector.Status.Loaded || vector.Status.RebuildNeeded ||
		len(vector.Results) != 1 || !bytes.Equal(vector.Results[0].ID, witness.primaryID) {
		t.Fatalf("SearchVectorIndex response=%+v want reopened loaded top-1 document %q", vector, witness.primaryID)
	}

	templateCollection, err := manager.OpenCollection(authoritativeTemplateCollection)
	if err != nil {
		t.Fatal(err)
	}
	storedTemplate, err := templateCollection.Get(witness.templateID)
	if err != nil {
		t.Fatalf("template Collection.Get(%q): %v", witness.templateID, err)
	}
	templateJSON, err := templateCollection.StoredDocumentJSON(storedTemplate)
	if err != nil {
		t.Fatalf("StoredDocumentJSON: %v", err)
	}
	assertJSONDocumentEqual(t, templateJSON, witness.templateJSON, "TemplateV1 StoredDocumentJSON")

	dictionaryValue, err := reopened.Get(witness.dictionaryKey)
	if err != nil {
		t.Fatalf("dictionary-frame Get(%q): %v", witness.dictionaryKey, err)
	}
	if !bytes.Equal(dictionaryValue, witness.dictionaryData) {
		t.Fatalf("dictionary-frame Get bytes=%d want=%d", len(dictionaryValue), len(witness.dictionaryData))
	}
}

func writeDictionaryTrainingBatch(t *testing.T, database *treedb.DB, prefix string, count int) ([]byte, []byte) {
	t.Helper()
	const valueSize = 16 << 10
	base := bytes.Repeat([]byte("certification-compressible-resource-"), valueSize/len("certification-compressible-resource-")+1)[:valueSize]
	var firstKey, firstValue []byte
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("%s/%06d", prefix, i))
		value := append([]byte(nil), base...)
		binary.LittleEndian.PutUint32(value[valueSize-4:], uint32(i))
		if err := database.Set(key, value); err != nil {
			t.Fatalf("Set dictionary training value: %v", err)
		}
		if i == 0 {
			firstKey = append([]byte(nil), key...)
			firstValue = append([]byte(nil), value...)
		}
	}
	return firstKey, firstValue
}

func waitForPublishedValueLogDictionary(t *testing.T, database *treedb.DB, backgroundErrors <-chan error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-backgroundErrors:
			t.Fatalf("value-log dictionary background error: %v", err)
		default:
		}
		stats := database.Stats()
		if id, err := strconv.ParseUint(stats["treedb.cache.vlog_dict.last_applied_dict_id"], 10, 64); err == nil && id > 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	stats := database.Stats()
	t.Fatalf("dictionary publication timed out: last_applied_dict_id=%q frames_attempted=%q", stats["treedb.cache.vlog_dict.last_applied_dict_id"], stats["treedb.cache.vlog_dict.frames_attempted"])
}

func requireDictionaryEncodedValueLogFrame(t *testing.T, database *treedb.DB, valueLogDir string, key []byte, stats map[string]string) {
	t.Helper()
	backend := treedb.PowerLossCertificationBackendForTest(database)
	if backend == nil {
		t.Fatal("public TreeDB handle has no collection backend")
	}
	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("dictionary frame proof acquired no backend snapshot")
	}
	defer func() { _ = snapshot.Close() }()
	state, ok := snapshot.StateToken()
	if !ok || state.RootPageID == 0 {
		t.Fatalf("dictionary frame proof snapshot state=%+v available=%t", state, ok)
	}
	entry, err := snapshot.GetEntryAtRoot(state.RootPageID, key)
	if err != nil {
		t.Fatalf("dictionary frame proof GetEntryAtRoot(%q): %v", key, err)
	}
	ptr := entry.ValuePtr
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) || !page.ValuePtrIsGrouped(ptr) || ptr.Offset < 4 {
		t.Fatalf("dictionary frame proof key=%q flags=%#x ptr=%+v want grouped value-log pointer", key, entry.Flags, ptr)
	}
	path := valuelog.SegmentPath(valueLogDir, ptr.FileID)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("dictionary frame proof read %s: %v", path, err)
	}
	recordStart := int(ptr.Offset - 4)
	if recordStart < 0 || recordStart+valuelog.HeaderSize > len(raw) {
		t.Fatalf("dictionary frame proof key=%q record start=%d file_bytes=%d", key, recordStart, len(raw))
	}
	header := raw[recordStart : recordStart+valuelog.HeaderSize]
	if header[4] != valuelog.Version || header[5]&byte(1) == 0 {
		t.Fatalf("dictionary frame proof key=%q record version=%d flags=%#x", key, header[4], header[5])
	}
	payloadBytes := int(binary.LittleEndian.Uint32(header[16:20]))
	payloadStart := recordStart + valuelog.HeaderSize
	payloadEnd := payloadStart + payloadBytes
	if payloadBytes <= 0 || payloadEnd < payloadStart || payloadEnd > len(raw) {
		t.Fatalf("dictionary frame proof key=%q invalid payload bytes=%d file_bytes=%d", key, payloadBytes, len(raw))
	}
	frame, _, offsets, _, err := valuelog.DecodeFrame(raw[payloadStart:payloadEnd])
	if err != nil {
		t.Fatalf("dictionary frame proof key=%q decode %s at %d: %v", key, path, recordStart, err)
	}
	subIndex := page.ValuePtrSubIndex(ptr)
	dictID, _ := strconv.ParseUint(stats["treedb.cache.vlog_dict.last_applied_dict_id"], 10, 64)
	if frame.DictID == 0 || frame.Flags&valuelog.FrameFlagCompressed == 0 || int(subIndex) >= int(frame.K) || len(offsets) != int(frame.K)+1 || dictID == 0 {
		t.Fatalf("dictionary frame proof key=%q ptr=%+v frame=%+v offsets=%d published_dict_id=%q", key, ptr, frame, len(offsets), stats["treedb.cache.vlog_dict.last_applied_dict_id"])
	}
}

func assertJSONDocumentEqual(t *testing.T, got, want []byte, label string) {
	t.Helper()
	var gotDocument, wantDocument any
	if err := json.Unmarshal(got, &gotDocument); err != nil {
		t.Fatalf("%s got JSON: %v", label, err)
	}
	if err := json.Unmarshal(want, &wantDocument); err != nil {
		t.Fatalf("%s want JSON: %v", label, err)
	}
	if !reflect.DeepEqual(gotDocument, wantDocument) {
		t.Fatalf("%s=%s want=%s", label, got, want)
	}
}

func textSearchContainsDocument(response collections.TextSearchResponse, documentID []byte) bool {
	for _, result := range response.Results {
		if bytes.Equal(result.DocumentID, documentID) {
			return true
		}
	}
	return false
}

func columnGroupCounts(groups []collections.ColumnPhysicalQueryGroup) map[string]int {
	out := make(map[string]int, len(groups))
	for _, group := range groups {
		out[group.Key] = group.Count
	}
	return out
}
