package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

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
