package collections

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Preparation uses the maintained analyzer/state constructor, with its actual
// flags. The retained state must own term/occurrence backing after the selected
// document bytes are overwritten; metadata field names belong to the capture.
func TestNativeTextPreparedStateOwnership(t *testing.T) {
	for _, flags := range []struct{ positions, offsets bool }{{false, false}, {true, false}, {true, true}} {
		def := TextIndexDefinition{Name: "content", Version: TextIndexVersionV2,
			Fields:   []TextIndexField{{Field: "content"}, {Field: "title"}, {Field: "missing"}},
			Analyzer: TextAnalyzerSimple, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"the"}},
			StorePositions: flags.positions, StoreOffsets: flags.offsets}
		doc := []byte(`{"content":"ignored","content":"The École école 東京 alpha beta gamma delta epsilon zeta eta theta iota kappa alpha","title":"refund refund policy"}`)
		analysis, err := analyzeTextIndexDocument(def, doc)
		if err != nil {
			t.Fatal(err)
		}
		want := encodeTextDocumentStateValue(textDocumentStateValueFromAnalysis(analysis))
		values := make([]jsonParserIndexValue, len(def.Fields))
		if handled, err := selectTextIndexDocumentJSONRootValues(def, doc, values); !handled || err != nil || !nativeTextCanPrepare(def, collectionOptions{}, values) {
			t.Fatalf("eligible selection handled=%v err=%v", handled, err)
		}
		var charged uint64
		state, err := nativeTextPrepareCurrent(def, values, func(n uint64) error { charged += n; return nil })
		if err != nil || charged == 0 {
			t.Fatalf("prepare charge=%d err=%v", charged, err)
		}
		clear(doc)
		if got := encodeTextDocumentStateValue(state); !bytes.Equal(got, want) {
			t.Fatalf("prepared state differs from maintained state or borrowed overwritten input: positions=%v offsets=%v", flags.positions, flags.offsets)
		}
	}
	def := TextIndexDefinition{Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "content"}}, Analyzer: TextAnalyzerSimple}
	for _, raw := range []string{`{"content":"refund\\policy"}`, `{"content":["refund","policy"]}`} {
		var selected [1]jsonParserIndexValue
		if handled, err := selectTextIndexDocumentJSONRootValues(def, []byte(raw), selected[:]); !handled || err != nil {
			t.Fatal(err)
		}
		if nativeTextCanPrepare(def, collectionOptions{}, selected[:]) {
			t.Fatalf("fallback representation prepared: %s", raw)
		}
	}
	var selected [1]jsonParserIndexValue
	if _, err := selectTextIndexDocumentJSONRootValues(def, []byte(`{"content":"refund policy"}`), selected[:]); err != nil {
		t.Fatal(err)
	}
	for _, format := range []DocumentFormat{DocumentFormatBSON, DocumentFormatTemplateV1} {
		if nativeTextCanPrepare(def, collectionOptions{documentFormat: format}, selected[:]) {
			t.Fatalf("stored format %s prepared as JSON", format)
		}
	}
	def.Fields[0].Field = "article.content"
	if nativeTextCanPrepare(def, collectionOptions{}, nil) {
		t.Fatal("nested field prepared without decoder cohorts")
	}
	def.Fields[0].Field = "content"
	def.Version = TextIndexVersionV1
	if nativeTextCanPrepare(def, collectionOptions{}, selected[:]) {
		t.Fatal("V1 prepared without its reconstruction cohorts")
	}
}

// An actual captured native catalog and original admitted lease feed the same
// reducer twice. Poisoning only the newDocument of the prepared route proves it
// consumes preparedNew rather than parsing/analyzing the document again. The
// complete named-domain bytes must equal the maintained unprepared reducer.
func TestNativeTextPreparedReducerReuseAndRefusal(t *testing.T) {
	var budget nativeHNSWAdmissionBudget
	// Registered first, so this runs after the fixture manager/owner cleanups.
	t.Cleanup(func() {
		if budget != nil {
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("prepared full Close receipts: %+v", stats)
			}
		}
	})
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	budget = read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
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
	catalog, err := newNativeTextCatalog(read, admit)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := collectionPlannerOptions(read.meta)
	if err != nil {
		t.Fatal(err)
	}
	opts.textTableFactory = nativeTextTable
	opts.textMaterializationAdmit = admit
	rows := []nativeJSONRow{
		{id: []byte("a"), current: []byte(`{"content":"refund refund policy alpha beta gamma delta epsilon zeta eta theta iota kappa"}`)},
		{id: []byte("b"), current: []byte(`{"content":"shipping policy École 東京"}`)},
	}
	// Test-owned staging descriptors follow the same pre-effect receipt rule.
	if err := admit(memtable.COWAllocationCharge(uint64(len(rows)) * (uint64(unsafe.Sizeof(textDocumentStateValue{})) + uint64(unsafe.Sizeof(textDocumentMutation{}))))); err != nil {
		t.Fatal(err)
	}
	mutations := make([]textDocumentMutation, len(rows))
	for i := range rows {
		mutations[i] = textDocumentMutation{documentID: rows[i].id, newDocument: rows[i].current, setNew: true}
	}
	preparation := nativeTextPreparation{states: make([]textDocumentStateValue, len(rows)), mutations: mutations}
	remaining, err := nativeTextSourceChargePrepared(read.meta, rows, opts, admit, &preparation)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(remaining); err != nil {
		t.Fatal(err)
	}
	for i := range mutations {
		if mutations[i].preparedNew != &preparation.states[i] {
			t.Fatalf("row%d not bound to its own prepared state", i)
		}
	}
	build := func(in []textDocumentMutation) []nativeJSONDomainChange {
		t.Helper()
		var names []string
		var policies []backenddb.OrderedRootStoragePolicy
		var tables []memtable.Table
		defer func() { resetCollectionTables(tables) }()
		if err := appendSingleTextIndexMutationDeltas(read.capture.Snapshot(), catalog, opts, read.meta.TextIndexes[0], in, &names, make(map[string]uint64), &policies, &tables); err != nil {
			t.Fatal(err)
		}
		out, err := nativeTextTables(names, policies, tables, admit)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	// Each complete reducer owns its own separately admitted construction work.
	baselineCharge, err := nativeTextSourceCharge(read.meta, rows, opts, admit)
	if err != nil {
		t.Fatal(err)
	}
	if err := admit(baselineCharge); err != nil {
		t.Fatal(err)
	}
	baseline := make([]textDocumentMutation, len(rows))
	copy(baseline, mutations)
	for i := range baseline {
		baseline[i].preparedNew = nil
	}
	want := build(baseline)
	for i := range mutations {
		mutations[i].newDocument = []byte(`{"content":`)
	}
	got := build(mutations)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("prepared reducer changed domain metadata, postings, positions or corpus statistics")
	}
	// Admission errors before analyzer/state allocation must not attach a
	// partial prepared row. Both successful preliminary cohorts remain charged
	// until the original lease closes; there is no early refund on error.
	for _, refusal := range []struct {
		call  int
		label string
	}{{4, "prepared analysis"}, {5, "prepared state"}} {
		preparation := nativeTextPreparation{states: make([]textDocumentStateValue, 1), mutations: make([]textDocumentMutation, 1)}
		before := charged
		calls := 0
		_, err := nativeTextSourceChargePrepared(read.meta, rows[:1], opts, func(n uint64) error {
			calls++
			if calls == refusal.call {
				return memtable.ErrCOWCapacity
			}
			return admit(n)
		}, &preparation)
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), refusal.label) || preparation.mutations[0].preparedNew != nil || len(preparation.states[0].Fields) != 0 || charged < before {
			t.Fatalf("stage %s refusal: calls=%d charge=%d->%d state=%+v err=%v", refusal.label, calls, before, charged, preparation.states, err)
		}
	}
	// None of this planning can publish the staged rows or advance logical V.
	after, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if after.capture.LogicalSequence() != read.capture.LogicalSequence() {
		t.Error("planning/refusal advanced native logical state")
	}
	if err := after.Close(); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Get([]byte("a")); err != nil || value != nil {
		t.Fatalf("planning published a row: %s %v", value, err)
	}
}
