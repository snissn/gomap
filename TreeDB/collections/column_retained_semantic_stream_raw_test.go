package collections

import (
	"bytes"
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
	values, err := collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg, plan, document, 0, 1, streams, pathInterner)
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
		values, err := collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg, plan, document, uint64(row), len(documents), streams, pathInterner)
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
	if len(payloadShared.entries) != 2 {
		t.Fatalf("payload.shared entries=%d want 2", len(payloadShared.entries))
	}
	if got := string(payloadShared.entries[0].raw); got != "10" {
		t.Fatalf("duplicate key retained raw=%q want last value 10", got)
	}
	if got := string(payloadShared.entries[1].raw); got != "3" {
		t.Fatalf("second row retained raw=%q want 3", got)
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
	if err := collectColumnRetainedSemanticStreamV1RetainedJSONParserDocument(cfg, columnRetainedSemanticStreamV1RetainedSkipTrieForConfig(cfg), document, 0, 1, streams, pathInterner); err != nil {
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
