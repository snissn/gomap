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
	values, err := collectColumnRetainedSemanticStreamV1RootFastPathDocument(cfg, plan, document, 0, 1, streams)
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

func columnRetainedSemanticStreamV1TestStringAliasesBytes(value string, source []byte) bool {
	if len(value) == 0 || len(source) == 0 {
		return false
	}
	valuePtr := uintptr(unsafe.Pointer(unsafe.StringData(value)))
	sourcePtr := uintptr(unsafe.Pointer(unsafe.SliceData(source)))
	return valuePtr >= sourcePtr && valuePtr < sourcePtr+uintptr(len(source))
}
