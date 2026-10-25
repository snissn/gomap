package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/buger/jsonparser"
	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeTextPrivateTableRepresentation(t *testing.T) {
	// Guard the dependency layouts used by the pre-construction structural bound.
	field, _ := reflect.TypeOf(memtable.BTree{}).FieldByName("tree")
	mapType := field.Type.Elem()
	root, _ := mapType.FieldByName("root")
	nodeType := root.Type.Elem()
	items, _ := nodeType.FieldByName("items")
	pairType := items.Type.Elem()
	entry, _ := pairType.FieldByName("value")
	if entry.Type.Size() != unsafe.Sizeof(nativeTextBTreeEntry{}) || pairType.Size() != unsafe.Sizeof(nativeTextBTreePair{}) || nodeType.Size() != unsafe.Sizeof(nativeTextBTreeNode{}) {
		t.Fatal("native text BTree admission layout changed")
	}
	def := TextIndexDefinition{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple}
	legacy, err := buildCreateTextV2IndexBackfillPlan(nil, &collectionCatalog{meta: CollectionMeta{Name: "legacy"}}, def, collectionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range legacy.tables {
		if _, ok := table.(*memtable.AppendOnly); !ok {
			t.Fatalf("legacy representation=%T", table)
		}
	}
	resetCollectionTables(legacy.tables)
	beforeEntries, beforeValues := memtable.AppendOnlyEntryPoolStatsSnapshot(), memtable.AppendOnlyValueArenaPoolStatsSnapshot()
	options := collectionOptions{textTableFactory: nativeTextTable}
	table := options.newTextTable(0)
	if _, ok := table.(*memtable.BTree); !ok {
		t.Fatalf("native representation=%T", table)
	}
	actualMap := reflect.ValueOf(table).Elem().FieldByName("tree").Elem()
	if actualMap.FieldByName("min").Int() != 31 || actualMap.FieldByName("max").Int() != 63 {
		t.Fatal("native text BTree degree changed")
	}
	// Growth/splits, overwrite, tombstone: no copying setters or package pools.
	for i := 0; i < 2048; i++ {
		key := []byte(fmt.Sprintf("key-%04d", (i*37)%2048))
		table.SetSteal(key, []byte("first"))
		table.SetSteal(key, []byte("last"))
		if i%2 == 0 {
			table.DeleteSteal(key)
		}
	}
	arena, _ := reflect.TypeOf(memtable.BTree{}).FieldByName("arena")
	if arena.Type.Elem().Size() != unsafe.Sizeof(nativeTextBTreeArena{}) {
		t.Fatal("native text arena layout changed")
	}
	it := table.NewIterator(nil, nil)
	if reflect.TypeOf(it).Elem().Size() != unsafe.Sizeof(nativeTextBTreeIterator{}) {
		t.Fatal("native text iterator layout changed")
	}
	if !reflect.ValueOf(table).Elem().FieldByName("arena").Elem().FieldByName("chunks").IsNil() {
		t.Fatal("steal setters allocated copying arena")
	}
	var live, dead int
	for ; it.Valid(); it.Next() {
		if it.IsDeleted() {
			dead++
		} else {
			live++
			if string(it.UnsafeValue()) != "last" {
				t.Fatal("overwrite lost")
			}
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if live != 1024 || dead != 1024 {
		t.Fatalf("temporary rows live=%d dead=%d", live, dead)
	}
	resetCollectionRunTable(table)
	table = nil
	afterEntries, afterValues := memtable.AppendOnlyEntryPoolStatsSnapshot(), memtable.AppendOnlyValueArenaPoolStatsSnapshot()
	if afterEntries.PutsTotal != beforeEntries.PutsTotal || afterValues.PutsTotal != beforeValues.PutsTotal {
		t.Fatal("native temporary backing escaped to global AppendOnly pools")
	}
}

// This text-only class isolates text admission from scalar JSON preparation.
// The existing mixed scalar/ANN/text controls exercise their shared publication.
func TestNativeTextPublicSelectedSourceSizing(t *testing.T) {
	for _, field := range []string{"content", "article.content"} {
		t.Run(field, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			cut, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			budget := cut.NativeAllocator().(nativeHNSWAdmissionBudget)
			cut.Close()
			manager := NewCollectionManager(backend)
			defer func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
				if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
					t.Errorf("text full Close receipts: %+v", stats)
				}
			}()
			_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: field}}, Analyzer: TextAnalyzerSimple, StorePositions: true}}})
			if err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			payload := strings.Repeat("x", 1<<20)
			doc := []byte(fmt.Sprintf(`{"content":"ignored","cont\u0065nt":"refund policy","payload":%q}`, payload))
			if field != "content" {
				// Nested paths retain full JSON decoding, separately charged from text.
				doc = []byte(fmt.Sprintf(`{"article":{"content":"ignored"},"article":{"cont\u0065nt":["","refund",null,7,"policy",""]},"payload":%q}`, payload[:4096]))
			}
			if _, err = c.Insert([]byte("a"), doc); err != nil {
				t.Fatalf("insert: %v", err)
			}
			requireNativeTextIDs(t, c, "refund", "a")
			got, err := c.Get([]byte("a"))
			if err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("unrelated payload changed: %v", err)
			}
			next := bytes.Replace(doc, []byte("refund"), []byte("returns"), 1)
			if _, err = c.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("a"), Update: func([]byte) ([]byte, bool, error) { return next, true, nil }}}); err != nil {
				t.Fatalf("update: %v", err)
			}
			requireNativeTextIDs(t, c, "refund")
			requireNativeTextIDs(t, c, "returns", "a")
			if err = owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			requireNativeTextIDs(t, c, "returns", "a")
			t.Logf("payload=%d budget=%+v", len(doc), budget.Stats())
		})
	}
}

func TestNativeTextBorrowedFieldSelection(t *testing.T) {
	def := TextIndexDefinition{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: true}
	for _, raw := range []string{
		`{"content":"ignored","content":"last wins"}`,
		`{"cont\u0065nt":"escaped key", "other":"unrelated"}`,
		`{"content":["array one",null,7,"two\\escaped"]}`,
	} {
		values := make([]jsonParserIndexValue, 1)
		handled, err := selectTextIndexDocumentJSONRootValues(def, []byte(raw), values)
		if err != nil || !handled {
			t.Fatalf("select %s: handled=%v err=%v", raw, handled, err)
		}
		analysis, err := analyzeTextIndexDocument(def, []byte(raw))
		if err != nil || len(analysis.Fields) != 1 {
			t.Fatalf("analysis %s: %+v %v", raw, analysis, err)
		}
		text, ok, err := textIndexFieldTextFromJSONParser(values[0], new([]byte))
		if err != nil || !ok {
			t.Fatalf("selected text: %v", err)
		}
		if strings.Contains(raw, "ignored") && text != "last wins" {
			t.Fatalf("duplicate selection=%q", text)
		}
	}
}

// Exercise the actual native bootstrap/mutation adapter and its maintained
// reducer error cleanup under a real owner lease, rather than only the factory.
func TestNativeTextBuilderTemporaryOwnership(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) { testNativeTextBuilderTemporaryOwnership(t, version) })
	}
}

func testNativeTextBuilderTemporaryOwnership(t *testing.T, version TextIndexVersion) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true, version)
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	var charged uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	beforeE, beforeV := memtable.AppendOnlyEntryPoolStatsSnapshot(), memtable.AppendOnlyValueArenaPoolStatsSnapshot()
	image, err := nativeTextInitialDomains(read.meta, admit)
	wantRoots, wantCreated := 7, 7
	if version == TextIndexVersionV1 {
		wantRoots, wantCreated = 3, 2
	}
	if err != nil || len(image) != wantRoots {
		t.Fatalf("native bootstrap domains=%d err=%v", len(image), err)
	}
	image = nil
	catalog, err := newNativeTextCatalog(read, admit)
	if err != nil {
		t.Fatal(err)
	}
	rows := []nativeJSONRow{{id: []byte("a"), current: []byte(`{"content":"refund policy"}`)}}
	changes, err := nativeTextMutationDomains(read.capture.Snapshot(), catalog, rows, nil, admit)
	if err != nil || len(changes) == 0 {
		t.Fatalf("native mutation domains=%d err=%v", len(changes), err)
	}
	changes = nil
	opts, err := collectionPlannerOptions(read.meta)
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	opts.textTableFactory = func(n int) memtable.Table { created++; return nativeTextTable(n) }
	var names []string
	var policies []backenddb.OrderedRootStoragePolicy
	var tables []memtable.Table
	err = appendSingleTextIndexMutationDeltas(read.capture.Snapshot(), catalog, opts, read.meta.TextIndexes[0], []textDocumentMutation{{documentID: nil}}, &names, make(map[string]uint64), &policies, &tables)
	if err == nil || !strings.Contains(err.Error(), "document id cannot be empty") || created != wantCreated || len(tables) != 0 {
		t.Fatalf("post-construction cleanup tables=%d created=%d err=%v", len(tables), created, err)
	}
	afterE, afterV := memtable.AppendOnlyEntryPoolStatsSnapshot(), memtable.AppendOnlyValueArenaPoolStatsSnapshot()
	if afterE.PutsTotal != beforeE.PutsTotal || afterV.PutsTotal != beforeV.PutsTotal {
		t.Fatalf("native builder/error cleanup transferred backing to AppendOnly pools: entries%+v->%+v values%+v->%+v", beforeE, afterE, beforeV, afterV)
	}
}

// These ordinary mutations refuse at two different admission boundaries on the
// original transaction lease. Pin every setup cut so teardown cannot offset an
// accidental positive reservation; no test allocator or production hook.
func TestNativeTextPublicNestedAdmissionStages(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	var pins []backenddb.NativeSemanticCapture
	var manager *CollectionManager
	var budget nativeHNSWAdmissionBudget
	defer func() {
		for i := len(pins) - 1; i >= 0; i-- {
			if err := pins[i].Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		if budget != nil {
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("nested full Close receipts: %+v", stats)
			}
		}
	}()
	pin := func() {
		t.Helper()
		cut, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, cut)
	}
	pin()
	budget = pins[0].NativeAllocator().(nativeHNSWAdmissionBudget)
	manager = NewCollectionManager(backend)
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "article.content"}}, Analyzer: TextAnalyzerSimple, StorePositions: true}}}); err != nil {
		t.Fatal(err)
	}
	pin()
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	small := []byte(`{"article":{"content":"refund policy"}}`)
	if _, err = c.Insert([]byte("a"), small); err != nil {
		t.Fatal(err)
	}
	pin()
	requireNativeTextIDs(t, c, "refund", "a")
	if _, err = c.Get([]byte("a")); err != nil {
		t.Fatal(err)
	}
	opening := pins[len(pins)-1]
	beforeState, beforeSequence := opening.State(), opening.LogicalSequence()
	for _, test := range []struct {
		name, label string
		document    []byte
	}{
		{"decoder", "native text JSON decode workspace", []byte(fmt.Sprintf(`{"article":{"content":"refund policy"},"payload":%q}`, strings.Repeat("x", 1<<20)))},
		{"tokens", "native text analysis workspace", []byte(fmt.Sprintf(`{"article":{"content":%q}}`, strings.Repeat("word ", 1<<13)))},
	} {
		t.Run(test.name, func(t *testing.T) {
			for attempt := 0; attempt < 3; attempt++ {
				before := budget.Stats()
				beforeLSN := backend.CommandWALNextLSN()
				_, err := c.Insert([]byte("refused"), test.document)
				if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), test.label) {
					t.Fatalf("attempt%d expected %s refusal: %v", attempt, test.label, err)
				}
				if backend.CommandWALNextLSN() != beforeLSN {
					t.Fatal("refused mutation assigned WAL identity")
				}
				afterCut, err := backend.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				if afterCut.State() != beforeState || afterCut.LogicalSequence() != beforeSequence {
					t.Error("refused mutation changed native state")
				}
				if err = afterCut.Close(); err != nil {
					t.Fatal(err)
				}
				after := budget.Stats()
				if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
					t.Fatalf("attempt%d refused mutation changed receipts: before=%+v after=%+v", attempt, before, after)
				}
				if got, err := c.Get([]byte("refused")); err != nil || got != nil {
					t.Fatalf("refused row=%s err=%v", got, err)
				}
				requireNativeTextIDs(t, c, "refund", "a")
			}
		})
	}
	if _, err = c.Insert([]byte("refused"), small); err != nil {
		t.Fatal(err)
	}
	pin()
	requireNativeTextIDs(t, c, "refund", "a", "refused")
}

// Compare the structural census with the maintained analyzer, including token
// positions removed by stopwords. It bounds runs, not distinct terms.
func TestNativeTextSimpleRootTokenBound(t *testing.T) {
	def := TextIndexDefinition{Analyzer: TextAnalyzerSimple}
	for _, test := range []struct {
		name, text string
		stop       []string
	}{
		{"empty", "", nil}, {"ascii", "refund policy " + strings.Repeat("payload ", 32), nil},
		{"repeats", "one one one", nil}, {"unicode", "École 東京 १२३ İ HTTP_500 _", nil},
		{"delimiters", " .,-!\t one___two : three\n", nil},
		{"invalid_utf8", string([]byte{'a', 0xff, 'b', 0xc0, 0xaf, '_', 0xfe}), nil},
		{"stopwords", "the refund the policy", []string{"the", "policy"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := []byte(test.text)
			value := jsonParserIndexValue{raw: raw, valueType: jsonparser.String}
			bound := nativeTextRootValueTokenBound(def, value)
			unfiltered, err := AnalyzeText(TextAnalyzerSimple, test.text)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := AnalyzeTextWithOptions(TextAnalyzerSimple, &TextAnalyzerOptions{StopWords: test.stop}, test.text)
			if err != nil {
				t.Fatal(err)
			}
			if bound != uint64(len(unfiltered)) || bound < uint64(len(actual)) {
				t.Fatalf("bound=%d unfiltered=%d actual=%d", bound, len(unfiltered), len(actual))
			}
			var got uint64
			if allocs := testing.AllocsPerRun(100, func() { got = nativeTextRootValueTokenBound(def, value) }); allocs != 0 || got != bound {
				t.Fatalf("census allocations=%g got=%d want=%d", allocs, got, bound)
			}
		})
	}
	for _, test := range []struct {
		name     string
		analyzer TextAnalyzer
		value    jsonParserIndexValue
	}{
		{"array", TextAnalyzerSimple, jsonParserIndexValue{raw: []byte(`["refund","policy"]`), valueType: jsonparser.Array}},
		{"escaped", TextAnalyzerSimple, jsonParserIndexValue{raw: []byte(`refund\u0020policy`), valueType: jsonparser.String}},
		{"other_analyzer", TextAnalyzer("other"), jsonParserIndexValue{raw: []byte("longsingleword"), valueType: jsonparser.String}},
		{"omitted_analyzer", TextAnalyzer(""), jsonParserIndexValue{raw: []byte("longsingleword"), valueType: jsonparser.String}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, want := nativeTextRootValueTokenBound(TextIndexDefinition{Analyzer: test.analyzer}, test.value), (uint64(len(test.value.raw))+1)/2; got != want {
				t.Fatalf("fallback=%d want%d", got, want)
			}
		})
	}
	// Nested selection must keep the existing decoder/byte-derived path.
	nested := TextIndexDefinition{Analyzer: TextAnalyzerSimple, Fields: []TextIndexField{{Field: "article.content"}}}
	var values [1]jsonParserIndexValue
	if handled, err := selectTextIndexDocumentJSONRootValues(nested, []byte(`{"article":{"content":"refund policy"}}`), values[:]); err != nil || handled {
		t.Fatalf("nested selected as top-level: %v %v", handled, err)
	}
}

func TestNativeTextRootTermCensusAdmission(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		for _, text := range []string{"", "refund policy " + strings.Repeat("payload ", 32), "A a É é 東京 १२३ _", "a b c d e f g h i j k l m n o p q r s t u v w x y z", strings.Repeat("LongWord", 4096)} {
			def := TextIndexDefinition{Version: version, Analyzer: TextAnalyzerSimple, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"policy"}}, StorePositions: true, StoreOffsets: true}
			value := jsonParserIndexValue{raw: []byte(text), valueType: jsonparser.String}
			denied := errors.New("census denied")
			if _, err := nativeTextRootValueTermBound(def, value, func(uint64) error { return denied }); !errors.Is(err, denied) {
				t.Fatalf("census admission: %v", err)
			}
			var charged uint64
			got, err := nativeTextRootValueTermBound(def, value, func(n uint64) error { charged += n; return nil })
			if err != nil || charged == 0 {
				t.Fatalf("census charge=%d err=%v", charged, err)
			}
			field, _, err := analyzeTextIndexField(def, "content", text)
			if err != nil || got != uint64(len(field.Terms)) {
				t.Fatalf("version=%s census=%d terms=%d err=%v", version, got, len(field.Terms), err)
			}
		}
	}
	for _, value := range []jsonParserIndexValue{{raw: []byte(`["one","two"]`), valueType: jsonparser.Array}, {raw: []byte(`one\u0020two`), valueType: jsonparser.String}} {
		got, err := nativeTextRootValueTermBound(TextIndexDefinition{Analyzer: TextAnalyzerSimple}, value, func(uint64) error { t.Fatal("fallback constructed census"); return nil })
		if err != nil || got != (uint64(len(value.raw))+1)/2 {
			t.Fatalf("fallback=%d err=%v", got, err)
		}
	}
}

// Original command geometry's first request, through the ordinary native
// WithStats authority with its unchanged core64MiB budget and mixed indexes.
func TestNativeTextPublicRepeatedContentBatch128(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	opening, err := backend.CaptureNativeSemanticState()
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	budget := opening.NativeAllocator().(nativeHNSWAdmissionBudget)
	if err = opening.Close(); err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("fullClose receipts: %+v", stats)
		}
	}()
	meta := nativeTextPublicMeta()
	meta.Indexes[0].Field = "meta.tenant"
	meta.Indexes = append(meta.Indexes, IndexDefinition{Name: "rank", Field: "meta.rank", ValueType: IndexValueInt64})
	meta.VectorIndexes[0].Dimensions = 8
	meta.VectorIndexes[0].M = 16
	meta.VectorIndexes[0].EfConstruction = 128
	if _, err = manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	const count = 128
	content := "refund policy " + strings.Repeat("payload ", 32)
	if got := nativeTextRootValueTokenBound(TextIndexDefinition{Analyzer: TextAnalyzerSimple}, jsonParserIndexValue{raw: []byte(content), valueType: jsonparser.String}); len(content) != 270 || got != 34 {
		t.Fatalf("command content geometry: bytes=%d tokens=%d", len(content), got)
	}
	ids, documents := make([][]byte, count), make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("d%06d", i))
		var embedding [8]float32
		embedding[0] = 1
		for d := 1; d < len(embedding); d++ {
			embedding[d] = float32((i+d)%17) / 17
		}
		documents[i], err = json.Marshal(struct {
			Content   string     `json:"content"`
			Embedding [8]float32 `json:"embedding"`
			Meta      struct {
				Tenant string `json:"tenant"`
				Rank   int64  `json:"rank"`
			} `json:"meta"`
		}{Content: content, Embedding: embedding, Meta: struct {
			Tenant string `json:"tenant"`
			Rank   int64  `json:"rank"`
		}{Tenant: fmt.Sprintf("t%02d", i%16), Rank: int64(i)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	inserted, stats, err := c.InsertBatchWithStats(ids, documents)
	if err != nil || len(inserted) != count || stats.Documents != count {
		t.Fatalf("128-row insert: ids=%d stats=%+v err=%v", len(inserted), stats, err)
	}
	response, err := c.SearchText(TextSearchOptions{IndexName: "content", Query: "refund", TopK: count})
	if err != nil || len(response.Results) != count {
		t.Fatalf("indexed rows=%d err=%v", len(response.Results), err)
	}
	for _, i := range []int{0, count - 1} {
		raw, err := c.Get(ids[i])
		if err != nil || !bytes.Equal(raw, documents[i]) {
			t.Fatalf("row%d err=%v", i, err)
		}
	}
	held, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	sequence := held.LogicalSequence()
	// A genuinely large token workload must still refuse before assignment;
	// do not turn this tighter structural bound into unlimited admission.
	refused := []byte(fmt.Sprintf(`{"content":%q,"embedding":[1,1,1,1,1,1,1,1],"meta":{"tenant":"tenant-00"}}`, strings.Repeat("word ", 1<<16)))
	for attempt := 0; attempt < 2; attempt++ {
		before := budget.Stats()
		beforeLSN := backend.CommandWALNextLSN()
		if _, err = c.Insert([]byte("refused"), refused); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("expected capacity refusal: %v", err)
		}
		if backend.CommandWALNextLSN() != beforeLSN {
			t.Fatal("refusal assigned WAL identity")
		}
		after, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		if after.LogicalSequence() != sequence {
			t.Error("refusal changed logical sequence")
		}
		if err = after.Close(); err != nil {
			t.Fatal(err)
		}
		current := budget.Stats()
		if current.ReservedBytes > before.ReservedBytes || current.ExternalLeases > before.ExternalLeases {
			t.Fatalf("refusal added ownership: before=%+v after=%+v", before, current)
		}
		if got, err := c.Get([]byte("refused")); err != nil || got != nil {
			t.Fatalf("refused row=%q err=%v", got, err)
		}
	}
	t.Logf("contentBytes=%d unfilteredRuns=34 priorByteBound=%d budget=%+v", len(content), (len(content)+1)/2, budget.Stats())
}

// Count actual maintained analyzer/state output, including repeated positions
// and offsets, beside the pre-effect envelope. This is an allocation bound
// witness, not an alternative analyzer or a performance benchmark.
func TestNativeTextSimpleFieldAllocationBound(t *testing.T) {
	inputs := []string{
		"", "refund policy " + strings.Repeat("payload ", 32),
		strings.Repeat("REPEAT_1 ", 1024),
		"ÄPFEL İSTANBUL ǰ 你好 Привет κόσμος _1\t--refund--",
		"one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen",
	}
	for _, flags := range []struct{ positions, offsets bool }{{}, {true, false}, {true, true}} {
		def := TextIndexDefinition{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: flags.positions, StoreOffsets: flags.offsets, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"policy"}}}
		for index, input := range inputs {
			t.Run(fmt.Sprintf("positions=%v/offsets=%v/input=%d", flags.positions, flags.offsets, index), func(t *testing.T) {
				value := jsonParserIndexValue{raw: []byte(input), valueType: jsonparser.String}
				runs := nativeTextRootValueTokenBound(def, value)
				terms, err := nativeTextRootValueTermBound(def, value, func(uint64) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				field, ok, err := analyzeTextIndexField(def, "content", input)
				if err != nil || !ok || uint64(len(field.Terms)) != terms || uint64(field.Length) > runs {
					t.Fatalf("maintained analysis: runs=%d terms=%d field=%+v err=%v", runs, terms, field, err)
				}
				analysis := textAnalyzedDocument{Fields: []textAnalyzedField{field}}
				state := textDocumentStateValueFromAnalysis(analysis)
				var retained uint64
				retained += memtable.COWAllocationCharge(uint64(cap(state.Fields[0].Terms)) * uint64(unsafe.Sizeof(textDocumentTermState{})))
				for _, term := range field.Terms {
					if flags.positions && uint64(len(term.Positions)) != uint64(term.Frequency) || flags.offsets && uint64(len(term.Offsets)) != uint64(term.Frequency) {
						t.Fatal("repeated occurrence information lost")
					}
					retained += memtable.COWAllocationCharge(uint64(len(term.Term)))
					retained += memtable.COWAllocationCharge(uint64(cap(term.Positions)) * uint64(unsafe.Sizeof(uint32(0))))
					retained += memtable.COWAllocationCharge(uint64(cap(term.Offsets)) * uint64(unsafe.Sizeof(textTokenOffset{})))
				}
				bound, err := nativeTextSimpleFieldCharge(def, uint64(len(input)), runs, terms, 7, uint64(len("content")))
				if err != nil || bound < retained {
					t.Fatalf("retained output exceeds pre-effect bound: bound=%d retained=%d err=%v", bound, retained, err)
				}
			})
		}
	}
	def := TextIndexDefinition{Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "content"}}, StorePositions: true, StoreOffsets: true}
	short, err := nativeTextSimpleFieldCharge(def, 4096, 2, 1, 7, 7)
	if err != nil {
		t.Fatal(err)
	}
	many, err := nativeTextSimpleFieldCharge(def, 4096, 256, 1, 7, 7)
	if err != nil || many <= short {
		t.Fatalf("one distinct term must still pay every occurrence: short=%d many=%d err=%v", short, many, err)
	}
	for _, test := range []struct{ source, tokens, terms, id, field uint64 }{
		{1, 2, 1, 1, 1}, {2, 1, 2, 1, 1},
		{^uint64(0), 1, 1, 1, 1}, {1, 1, 1, ^uint64(0), 1},
	} {
		if _, err := nativeTextSimpleFieldCharge(def, test.source, test.tokens, test.terms, test.id, test.field); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("unchecked count/overflow %+v: %v", test, err)
		}
	}
}

func TestNativeTextSimpleFieldSourceEnvelopeSelection(t *testing.T) {
	def := TextIndexDefinition{Name: "content", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple, StorePositions: true, StoreOffsets: true}
	meta := CollectionMeta{Name: "docs", TextIndexes: []TextIndexDefinition{def}}
	// Keep exact legacy byte weighting for unhandled representations, including
	// its aggregate rounding. Neither escaped strings nor arrays use T/U bounds.
	unit := 2 * (uint64(unsafe.Sizeof(TextToken{})) + uint64(unsafe.Sizeof(textAnalyzedTerm{})) + uint64(unsafe.Sizeof(textDocumentTermState{})) + uint64(unsafe.Sizeof(textV2PostingBlockEntry{})) + uint64(unsafe.Sizeof(textV2PositionFieldValue{})) + 8*uint64(unsafe.Sizeof(string(""))))
	for _, raw := range []string{`{"content":"refund\u0020policy"}`, `{"content":["refund","policy",7]}`} {
		row := nativeJSONRow{id: []byte("id"), current: []byte(raw)}
		var values [1]jsonParserIndexValue
		if ok, err := selectTextIndexDocumentJSONRootValues(def, row.current, values[:]); !ok || err != nil {
			t.Fatalf("select fallback: %v", err)
		}
		n := uint64(len(values[0].raw))
		definition, err := nativeTextDefinitionCharge(def, 3)
		if err != nil {
			t.Fatal(err)
		}
		tables, err := nativeTextTableCharge(9+1+3+3*((n+1)/2), 7)
		if err != nil {
			t.Fatal(err)
		}
		want := definition + tables + memtable.COWAllocationCharge((n+uint64(len(row.id))+2)*unit)
		got, err := nativeTextSourceCharge(meta, []nativeJSONRow{row}, collectionOptions{}, func(uint64) error { return nil })
		if err != nil || got != want {
			t.Fatalf("fallback %s: charge=%d want original=%d err=%v", raw, got, want, err)
		}
	}
	content := "refund policy " + strings.Repeat("payload ", 32)
	document, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		t.Fatal(err)
	}
	got, err := nativeTextSourceCharge(meta, []nativeJSONRow{{id: []byte("d000000"), current: document}}, collectionOptions{}, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	// Compare identical definition/table accounting: a small batch still pays
	// seven minimum BTree roots, so its total need not fit the old byte lane
	// alone. This discriminator concerns only the changed source envelope.
	definition, err := nativeTextDefinitionCharge(def, 3)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := nativeTextTableCharge(9+1+3+3*3, 7)
	if err != nil {
		t.Fatal(err)
	}
	oldByteLane := memtable.COWAllocationCharge(uint64(len(content)+7+2) * unit)
	oldTotal := definition + tables + oldByteLane
	if got >= oldTotal {
		t.Fatalf("eligible repeated field retained byte-as-descriptor envelope: got=%d old total=%d", got, oldTotal)
	}
	t.Logf("bytes=%d runs=34 distinct=3 analysis+tables=%d prior analysis+tables=%d", len(content), got, oldTotal)
}
