package collections

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
