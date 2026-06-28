package collections

import (
	"bytes"
	"testing"

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
