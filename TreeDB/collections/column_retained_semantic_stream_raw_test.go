package collections

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"
	"unsafe"

	"github.com/buger/jsonparser"
)

func TestColumnRetainedSemanticStreamV1JSONParserRawValueUsesQuotedSource3202(t *testing.T) {
	document := []byte(`{"plain":"value","escaped":"a\"b\\c","empty":"","num":17}`)
	got := make(map[string][]byte)
	if err := jsonparser.ObjectEach(document, func(key, value []byte, dataType jsonparser.ValueType, valueEndOffset int) error {
		raw, err := columnRetainedSemanticStreamV1JSONParserRawValue(document, value, dataType, valueEndOffset)
		if err != nil {
			return err
		}
		got[string(key)] = raw
		return nil
	}); err != nil {
		t.Fatalf("ObjectEach: %v", err)
	}

	for key, want := range map[string]string{
		"plain":   `"value"`,
		"escaped": `"a\"b\\c"`,
		"empty":   `""`,
		"num":     `17`,
	} {
		raw := got[key]
		if string(raw) != want {
			t.Fatalf("%s raw=%q want %q", key, raw, want)
		}
		start := bytes.Index(document, []byte(want))
		if start < 0 {
			t.Fatalf("%s token %q not found in document", key, want)
		}
		if len(raw) > 0 && &raw[0] != &document[start] {
			t.Fatalf("%s raw value does not alias source token", key)
		}
	}
}

func TestColumnRetainedSemanticStreamV1JSONParserRawValueUsesNestedQuotedSource3202(t *testing.T) {
	document := []byte(`{"outer":{"nested":"value"}}`)
	object, dataType, _, err := jsonparser.Get(document, "outer")
	if err != nil {
		t.Fatalf("Get outer: %v", err)
	}
	if dataType != jsonparser.Object {
		t.Fatalf("outer type=%s want object", dataType)
	}

	var raw []byte
	if err := jsonparser.ObjectEach(object, func(key, value []byte, dataType jsonparser.ValueType, valueEndOffset int) error {
		if string(key) != "nested" {
			return nil
		}
		var err error
		raw, err = columnRetainedSemanticStreamV1JSONParserRawValue(object, value, dataType, valueEndOffset)
		return err
	}); err != nil {
		t.Fatalf("ObjectEach nested: %v", err)
	}
	if string(raw) != `"value"` {
		t.Fatalf("nested raw=%q want quoted value", raw)
	}
	start := bytes.Index(object, []byte(`"value"`))
	if start < 0 {
		t.Fatalf("nested token not found in object")
	}
	if len(raw) > 0 && &raw[0] != &object[start] {
		t.Fatalf("nested raw value does not alias object token")
	}
}

func TestColumnRetainedSemanticStreamV1JSONParserRawValueFallsBackOnInvalidStringOffset3202(t *testing.T) {
	raw, err := columnRetainedSemanticStreamV1JSONParserRawValue([]byte(`{"s":"value"}`), []byte("value"), jsonparser.String, 0)
	if err != nil {
		t.Fatalf("raw value fallback: %v", err)
	}
	if string(raw) != `"value"` {
		t.Fatalf("fallback raw=%q want quoted value", raw)
	}
}

func TestColumnRetainedSemanticStreamV1LookupStringDoesNotEscapeRetainedPaths3204(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "declared", Path: "declared", ValueType: ColumnStoreValueInt64},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids := [][]byte{[]byte("doc-0")}
	plan, ok := columnRetainedSemanticStreamV1RootFastPathPlanForConfig(cfg, ids, 1)
	if !ok {
		t.Fatal("root fast path plan not available")
	}
	document := []byte(`{"declared":7,"retained":2,"":3}`)
	streams := newColumnRetainedSemanticStreamStreams()
	pathInterner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	values, err := collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg, plan, document, 0, 1, streams, pathInterner, nil, nil)
	if err != nil {
		t.Fatalf("collect root fast path document: %v", err)
	}
	if len(values) != 1 || values[0].Int64 != 7 {
		t.Fatalf("declared values=%+v want declared int64 7", values)
	}
	if stream := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"declared"})]; stream != nil {
		t.Fatalf("declared key leaked into retained streams: %+v", stream.segments)
	}
	retainedStream := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"retained"})]
	if retainedStream == nil {
		t.Fatal("retained key missing from retained streams")
	}
	emptyKeyStream := streams.byKey[columnRetainedSemanticStreamPathKey([]string{""})]
	if emptyKeyStream == nil {
		t.Fatal("empty JSON key missing from retained streams")
	}
	if got := retainedStream.segments; len(got) != 1 || got[0] != "retained" {
		t.Fatalf("retained stream segments=%q want [retained]", got)
	}
	if got := emptyKeyStream.segments; len(got) != 1 || got[0] != "" {
		t.Fatalf("empty-key stream segments=%q want empty segment", got)
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(retainedStream.segments[0], document) {
		t.Fatal("retained path segment aliases mutable JSON source")
	}

	start := bytes.Index(document, []byte(`"retained"`))
	if start < 0 {
		t.Fatal("retained key not found in source document")
	}
	copy(document[start+1:], []byte("mutained"))
	if got := retainedStream.segments[0]; got != "retained" {
		t.Fatalf("retained path changed after source mutation: %q", got)
	}
}

func TestColumnRetainedSemanticStreamV1LocatorArenaMatchesStandalone3208(t *testing.T) {
	blockKey := make([]byte, sha256.Size)
	for i := range blockKey {
		blockKey[i] = byte(i)
	}
	rows := 300
	arena := make([]byte, 0, columnRetainedSemanticStreamV1LocatorBlockArenaCapacity(rows))
	locators := make([][]byte, rows)
	for row := 0; row < rows; row++ {
		start := len(arena)
		arena = appendColumnRetainedSemanticStreamV1Locator(arena, blockKey, uint64(row))
		locators[row] = arena[start:len(arena):len(arena)]

		standalone := encodeColumnRetainedSemanticStreamV1Locator(blockKey, uint64(row))
		if !bytes.Equal(locators[row], standalone) {
			t.Fatalf("row %d packed locator=%x want standalone=%x", row, locators[row], standalone)
		}
		gotBlockKey, gotRow, ok, err := parseColumnRetainedSemanticStreamV1Locator(locators[row])
		if err != nil {
			t.Fatalf("row %d parse packed locator: %v", row, err)
		}
		if !ok {
			t.Fatalf("row %d packed locator not recognized", row)
		}
		if gotRow != uint64(row) {
			t.Fatalf("row %d parsed row=%d", row, gotRow)
		}
		if !bytes.Equal(gotBlockKey, blockKey) {
			t.Fatalf("row %d parsed block key=%x want %x", row, gotBlockKey, blockKey)
		}
		if cap(locators[row]) != len(locators[row]) {
			t.Fatalf("row %d locator cap=%d len=%d; appends must not reach the shared arena", row, cap(locators[row]), len(locators[row]))
		}
	}
	if len(arena) != cap(arena) {
		t.Fatalf("locator arena len=%d cap=%d; capacity helper should be exact", len(arena), cap(arena))
	}

	nextBefore := append([]byte(nil), locators[1]...)
	appended := append(locators[0], 0xff)
	if unsafe.SliceData(appended) == unsafe.SliceData(locators[0]) {
		t.Fatal("append to a packed locator reused the shared arena backing")
	}
	if !bytes.Equal(locators[1], nextBefore) {
		t.Fatalf("append to first locator corrupted second locator: got %x want %x", locators[1], nextBefore)
	}
}

func TestColumnRetainedSemanticStreamV1DeclaredRowArenaOwnsAndCapsRows3210(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "declared", Path: "declared", ValueType: ColumnStoreValueInt64},
			{Name: "label", Path: "label", ValueType: ColumnStoreValueString},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids := [][]byte{[]byte("doc-0"), []byte("doc-1")}
	documents := [][]byte{
		[]byte(`{"declared":7,"label":"alpha","retained":1}`),
		[]byte(`{"declared":8,"label":"bravo","retained":2}`),
	}
	prepared, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, ids, documents)
	if err != nil {
		t.Fatalf("prepare semantic-stream-v1 documents: %v", err)
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)

	if !prepared.declaredRowsReady {
		t.Fatal("declared rows were not prepared")
	}
	if len(prepared.declaredRows) != len(documents) {
		t.Fatalf("declared rows=%d want %d", len(prepared.declaredRows), len(documents))
	}
	first := prepared.declaredRows[0]
	second := prepared.declaredRows[1]
	if got := string(first.ID); got != "doc-0" {
		t.Fatalf("first row ID=%q want doc-0", got)
	}
	if len(first.Values) != 2 || first.Values[0].Int64 != 7 || first.Values[1].String != "alpha" {
		t.Fatalf("first row values=%+v want declared=7 label=alpha", first.Values)
	}
	if len(second.Values) != 2 || second.Values[0].Int64 != 8 || second.Values[1].String != "bravo" {
		t.Fatalf("second row values=%+v want declared=8 label=bravo", second.Values)
	}
	if columnRetainedSemanticStreamV1TestBytesAliasBytes(first.ID, ids[0]) {
		t.Fatal("declared row ID aliases mutable input ID")
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(first.Values[1].String, documents[0]) {
		t.Fatal("declared string aliases mutable input document")
	}
	if cap(first.ID) != len(first.ID) {
		t.Fatalf("first row ID cap=%d len=%d; appends must not reach the shared arena", cap(first.ID), len(first.ID))
	}
	if cap(first.Values) != len(first.Values) {
		t.Fatalf("first row values cap=%d len=%d; appends must not reach the shared arena", cap(first.Values), len(first.Values))
	}

	secondIDBefore := append([]byte(nil), second.ID...)
	secondValuesBefore := append([]columnDeclaredValue(nil), second.Values...)
	appendedID := append(first.ID, 'x')
	if unsafe.SliceData(appendedID) == unsafe.SliceData(first.ID) {
		t.Fatal("append to declared row ID reused the shared arena backing")
	}
	appendedValues := append(first.Values, columnDeclaredValue{Int64: 99})
	if unsafe.SliceData(appendedValues) == unsafe.SliceData(first.Values) {
		t.Fatal("append to declared row values reused the shared arena backing")
	}
	if !bytes.Equal(second.ID, secondIDBefore) {
		t.Fatalf("append to first row ID corrupted second ID: got %q want %q", second.ID, secondIDBefore)
	}
	if len(second.Values) != len(secondValuesBefore) || second.Values[0].Int64 != secondValuesBefore[0].Int64 || second.Values[1].String != secondValuesBefore[1].String {
		t.Fatalf("append to first row values corrupted second values: got %+v want %+v", second.Values, secondValuesBefore)
	}

	ids[0][0] = 'X'
	labelStart := bytes.Index(documents[0], []byte("alpha"))
	if labelStart < 0 {
		t.Fatal("label token not found in source document")
	}
	copy(documents[0][labelStart:], []byte("omega"))
	if got := string(first.ID); got != "doc-0" {
		t.Fatalf("declared row ID changed after source ID mutation: %q", got)
	}
	if got := first.Values[1].String; got != "alpha" {
		t.Fatalf("declared string changed after source document mutation: %q", got)
	}
}

func TestColumnRetainedSemanticStreamV1DictionaryStringInternerReusesDeclaredValues3213(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids := [][]byte{[]byte("doc-0"), []byte("doc-1"), []byte("doc-2")}
	documents := [][]byte{
		[]byte(`{"kind":"shared","retained":1}`),
		[]byte(`{"kind":"shared","retained":2}`),
		[]byte(`{"kind":"other","retained":3}`),
	}
	prepared, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, ids, documents)
	if err != nil {
		t.Fatalf("prepare semantic-stream-v1 documents: %v", err)
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)

	if !prepared.declaredRowsReady {
		t.Fatal("declared rows were not prepared")
	}
	if len(prepared.declaredRows) != len(documents) {
		t.Fatalf("declared rows=%d want %d", len(prepared.declaredRows), len(documents))
	}
	first := prepared.declaredRows[0].Values[0].String
	second := prepared.declaredRows[1].Values[0].String
	third := prepared.declaredRows[2].Values[0].String
	if first != "shared" || second != "shared" || third != "other" {
		t.Fatalf("declared strings=(%q, %q, %q), want shared/shared/other", first, second, third)
	}
	if !columnRetainedSemanticStreamV1TestStringsShareBacking(first, second) {
		t.Fatal("repeated dictionary declared strings do not reuse string backing")
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(first, documents[0]) {
		t.Fatal("first dictionary declared string aliases mutable input document")
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(second, documents[1]) {
		t.Fatal("second dictionary declared string aliases mutable input document")
	}

	for _, document := range documents[:2] {
		start := bytes.Index(document, []byte("shared"))
		if start < 0 {
			t.Fatal("shared token not found in source document")
		}
		copy(document[start:], []byte("mutant"))
	}
	if first != "shared" || second != "shared" {
		t.Fatalf("dictionary strings changed after source mutation: first=%q second=%q", first, second)
	}
}

func TestColumnRetainedSemanticStreamV1PathSegmentInternerOwnsAndReusesSegments3206(t *testing.T) {
	interner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	source := []byte("shared")
	first := interner.intern(source)
	copy(source, []byte("mutant"))
	second := interner.intern([]byte("shared"))

	if first != "shared" || second != "shared" {
		t.Fatalf("interned values=(%q, %q) want shared", first, second)
	}
	if !columnRetainedSemanticStreamV1TestStringsShareBacking(first, second) {
		t.Fatal("repeated interned segment does not reuse string backing")
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(first, source) {
		t.Fatal("interned path segment aliases mutable source bytes")
	}
}

func TestColumnRetainedSemanticStreamV1PathSegmentInternerTransitionsToMap3067(t *testing.T) {
	interner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	sources := make([][]byte, columnRetainedSemanticStreamV1PathSegmentInternerLinearLimit+1)
	values := make([]string, len(sources))
	for i := range sources {
		source := []byte{byte('a' + i)}
		sources[i] = source
		values[i] = interner.intern(source)
		source[0] = byte('A' + i)
	}
	if interner.segments == nil {
		t.Fatal("path segment interner did not transition to map after linear limit")
	}
	if len(interner.values) != 0 {
		t.Fatalf("path segment interner retained %d linear values after map transition", len(interner.values))
	}
	if len(interner.segments) != len(sources) {
		t.Fatalf("path segment interner map entries=%d want %d", len(interner.segments), len(sources))
	}
	for i, value := range values {
		want := string([]byte{byte('a' + i)})
		if value != want {
			t.Fatalf("value %d=%q want %q", i, value, want)
		}
		got := interner.intern([]byte{byte('a' + i)})
		if got != want {
			t.Fatalf("reused value %d=%q want %q", i, got, want)
		}
		if !columnRetainedSemanticStreamV1TestStringsShareBacking(value, got) {
			t.Fatalf("reused value %d does not share backing after map transition", i)
		}
		if columnRetainedSemanticStreamV1TestStringAliasesBytes(got, sources[i]) {
			t.Fatalf("reused value %d aliases mutable source bytes after map transition", i)
		}
	}

	postTransition := []byte("post")
	first := interner.intern(postTransition)
	copy(postTransition, []byte("mut!"))
	second := interner.intern([]byte("post"))
	if first != "post" || second != "post" {
		t.Fatalf("post-transition values=(%q, %q) want post", first, second)
	}
	if !columnRetainedSemanticStreamV1TestStringsShareBacking(first, second) {
		t.Fatal("post-transition repeated segment does not reuse string backing")
	}
}

func TestColumnRetainedSemanticStreamV1PathSegmentInternerReusesRetainedTraversalSegments3206(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "declared", Path: "declared", ValueType: ColumnStoreValueInt64},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids := [][]byte{[]byte("doc-0"), []byte("doc-1")}
	plan, ok := columnRetainedSemanticStreamV1RootFastPathPlanForConfig(cfg, ids, len(ids))
	if !ok {
		t.Fatal("root fast path plan not available")
	}
	documents := [][]byte{
		[]byte(`{"declared":1,"payload":{"shared":1,"shared":10},"meta":{"shared":2}}`),
		[]byte(`{"declared":2,"payload":{"shared":3},"meta":{"shared":4}}`),
	}
	streams := newColumnRetainedSemanticStreamStreams()
	pathInterner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	for row, document := range documents {
		values, err := collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg, plan, document, uint64(row), len(documents), streams, pathInterner, nil, nil)
		if err != nil {
			t.Fatalf("collect row %d: %v", row, err)
		}
		if len(values) != 1 || values[0].Int64 != int64(row+1) {
			t.Fatalf("row %d declared values=%+v", row, values)
		}
	}

	payloadShared := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"payload", "shared"})]
	if payloadShared == nil {
		t.Fatal("payload.shared stream missing")
	}
	metaShared := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"meta", "shared"})]
	if metaShared == nil {
		t.Fatal("meta.shared stream missing")
	}
	if got := payloadShared.segments; len(got) != 2 || got[0] != "payload" || got[1] != "shared" {
		t.Fatalf("payload.shared segments=%q", got)
	}
	if got := metaShared.segments; len(got) != 2 || got[0] != "meta" || got[1] != "shared" {
		t.Fatalf("meta.shared segments=%q", got)
	}
	if !columnRetainedSemanticStreamV1TestStringsShareBacking(payloadShared.segments[1], metaShared.segments[1]) {
		t.Fatal("shared child path segment was not interned across retained streams")
	}
	for _, document := range documents {
		for _, segment := range append(append([]string(nil), payloadShared.segments...), metaShared.segments...) {
			if columnRetainedSemanticStreamV1TestStringAliasesBytes(segment, document) {
				t.Fatalf("path segment %q aliases mutable source document", segment)
			}
		}
	}
	if payloadShared.entryCount() != 2 {
		t.Fatalf("payload.shared entries=%d want 2", payloadShared.entryCount())
	}
	if got := string(payloadShared.rawValues[0]); got != "10" {
		t.Fatalf("duplicate key retained raw=%q want last value 10", got)
	}
	if got := string(payloadShared.rawValues[1]); got != "3" {
		t.Fatalf("second row retained raw=%q want 3", got)
	}
}

func TestColumnRetainedSemanticStreamV1DenseRowsStayImplicit3218(t *testing.T) {
	stream := &columnRetainedSemanticStreamPath{
		segments:  []string{"payload", "value"},
		rawValues: make([][]byte, 0, 3),
	}
	stream.appendValue(0, []byte("a"))
	stream.appendValue(1, []byte("b"))
	if stream.rows != nil {
		t.Fatalf("dense stream allocated explicit rows: %v", stream.rows)
	}
	if got := stream.rowAt(1); got != 1 {
		t.Fatalf("dense rowAt(1)=%d want 1", got)
	}

	stream.appendValue(3, []byte("d"))
	if got, want := stream.rows, []uint64{0, 1, 3}; !slices.Equal(got, want) {
		t.Fatalf("sparse rows=%v want %v", got, want)
	}
	if got := stream.rowAt(2); got != 3 {
		t.Fatalf("sparse rowAt(2)=%d want 3", got)
	}
}

func TestColumnRetainedSemanticStreamV1PathSegmentInternerReusesSkipTraversalSegments3206(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "declared", Path: "declared.nested", ValueType: ColumnStoreValueInt64},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	document := []byte(`{"declared":{"nested":1},"payload":{"shared":10},"meta":{"shared":11}}`)
	streams := newColumnRetainedSemanticStreamStreams()
	pathInterner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	if _, err := collectColumnRetainedSemanticStreamV1RetainedJSONParserDocument(cfg, columnRetainedSemanticStreamV1RetainedSkipTrieForConfig(cfg), document, 0, 1, streams, pathInterner, nil, nil, nil); err != nil {
		t.Fatalf("collect retained JSON parser document: %v", err)
	}
	if stream := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"declared", "nested"})]; stream != nil {
		t.Fatalf("declared nested path leaked into retained streams: %+v", stream.segments)
	}
	payloadShared := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"payload", "shared"})]
	if payloadShared == nil {
		t.Fatal("payload.shared stream missing")
	}
	metaShared := streams.byKey[columnRetainedSemanticStreamPathKey([]string{"meta", "shared"})]
	if metaShared == nil {
		t.Fatal("meta.shared stream missing")
	}
	if !columnRetainedSemanticStreamV1TestStringsShareBacking(payloadShared.segments[1], metaShared.segments[1]) {
		t.Fatal("shared child path segment was not interned through skip traversal")
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(payloadShared.segments[1], document) {
		t.Fatal("skip traversal path segment aliases mutable source document")
	}
}

func columnRetainedSemanticStreamV1TestStringsShareBacking(a, b string) bool {
	if len(a) == 0 || len(b) == 0 {
		return a == b
	}
	return unsafe.StringData(a) == unsafe.StringData(b)
}

func columnRetainedSemanticStreamV1TestStringAliasesBytes(value string, source []byte) bool {
	if len(value) == 0 || len(source) == 0 {
		return false
	}
	valuePtr := uintptr(unsafe.Pointer(unsafe.StringData(value)))
	sourcePtr := uintptr(unsafe.Pointer(unsafe.SliceData(source)))
	return valuePtr >= sourcePtr && valuePtr < sourcePtr+uintptr(len(source))
}

func columnRetainedSemanticStreamV1TestBytesAliasBytes(value, source []byte) bool {
	if len(value) == 0 || len(source) == 0 {
		return false
	}
	valuePtr := uintptr(unsafe.Pointer(unsafe.SliceData(value)))
	sourcePtr := uintptr(unsafe.Pointer(unsafe.SliceData(source)))
	return valuePtr >= sourcePtr && valuePtr < sourcePtr+uintptr(len(source))
}
