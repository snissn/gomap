package collections

import (
	"bytes"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var bsonIndexKeyCompareResultV2 int
var bsonIndexEntryKeyResultV2 []byte

func BenchmarkBSONIndexKeyCodecV2Encode(b *testing.B) {
	decimal, err := bson.ParseDecimal128("123456789.0123")
	if err != nil {
		b.Fatal(err)
	}
	cases := []struct {
		name      string
		value     bson.RawValue
		valueType IndexValueType
		legacy    any
	}{
		{name: "string", value: mustBSONIndexRawValueV2(b, "prefix\x00unicode-界"), valueType: IndexValueString, legacy: "prefix\x00unicode-界"},
		{name: "int64", value: mustBSONIndexRawValueV2(b, int64(-123456789)), valueType: IndexValueInt64, legacy: int64(-123456789)},
		{name: "double", value: mustBSONIndexRawValueV2(b, -12345.625), valueType: IndexValueDouble, legacy: -12345.625},
		{name: "decimal128", value: mustBSONIndexRawValueV2(b, decimal)},
	}
	for _, tc := range cases {
		b.Run(tc.name+"/v2", func(b *testing.B) {
			b.ReportAllocs()
			var encoded []byte
			for b.Loop() {
				encoded, _, err = appendBSONIndexKeyComponentV2(encoded[:0], tc.value)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encoded)), "bytes/key")
		})
		if tc.valueType != "" {
			b.Run(tc.name+"/typed-v1", func(b *testing.B) {
				b.ReportAllocs()
				var encoded []byte
				for b.Loop() {
					encoded, _, err = appendIndexScalar(encoded[:0], tc.valueType, tc.legacy)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(encoded)), "bytes/key")
			})
		}
	}
}

func BenchmarkBSONIndexKeyCodecV2EntryKey(b *testing.B) {
	component, err := encodeBSONIndexKeyComponentV2(mustBSONIndexRawValueV2(b, strings.Repeat("prefix-界-", 256)))
	if err != nil {
		b.Fatal(err)
	}
	documentID := []byte("document-7")
	b.ReportAllocs()
	for b.Loop() {
		entryKey, err := bsonIndexEntryKeyV2(component, documentID)
		if err != nil {
			b.Fatal(err)
		}
		bsonIndexEntryKeyResultV2 = entryKey
	}
}

func BenchmarkBSONIndexKeyCodecV2Compare(b *testing.B) {
	type benchmarkCase struct {
		name  string
		left  []byte
		right []byte
	}
	encodeV2 := func(value any) []byte {
		out, err := encodeBSONIndexKeyComponentV2(mustBSONIndexRawValueV2(b, value))
		if err != nil {
			b.Fatal(err)
		}
		return out
	}
	encodeV1 := func(valueType IndexValueType, value any) []byte {
		out, _, err := appendIndexScalar(nil, valueType, value)
		if err != nil {
			b.Fatal(err)
		}
		return out
	}
	cases := []benchmarkCase{
		{name: "string/v2", left: encodeV2("prefix\x00unicode-界"), right: encodeV2("prefix\x00unicode-集")},
		{name: "string/typed-v1", left: encodeV1(IndexValueString, "prefix\x00unicode-界"), right: encodeV1(IndexValueString, "prefix\x00unicode-集")},
		{name: "int64/v2", left: encodeV2(int64(-123456789)), right: encodeV2(int64(-123456788))},
		{name: "int64/typed-v1", left: encodeV1(IndexValueInt64, int64(-123456789)), right: encodeV1(IndexValueInt64, int64(-123456788))},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				bsonIndexKeyCompareResultV2 = bytes.Compare(tc.left, tc.right)
			}
			b.ReportMetric(float64(len(tc.left)+len(tc.right))/2, "bytes/key")
		})
	}
}
