package collections

import (
	"bytes"
	"encoding/hex"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestBSONIndexKeyCodecV2Golden(t *testing.T) {
	decimalOne := mustBSONIndexDecimal128V2(t, "1.00")
	objectID, err := bson.ObjectIDFromHex("00112233445566778899aabb")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		value bson.RawValue
		hex   string
		kind  bsonIndexKeyKindV2
		canon string
	}{
		{name: "missing", value: bson.RawValue{}, hex: "b208", kind: bsonIndexKeyKindMissingV2, canon: "missing"},
		{name: "null", value: bson.RawValue{Type: bson.TypeNull}, hex: "b210", kind: bsonIndexKeyKindNullV2, canon: "null"},
		{name: "int32 one", value: mustBSONIndexRawValueV2(t, int32(1)), hex: "b22040800020", kind: bsonIndexKeyKindNumberV2, canon: "1e0"},
		{name: "decimal one", value: mustBSONIndexRawValueV2(t, decimalOne), hex: "b22040800020", kind: bsonIndexKeyKindNumberV2, canon: "1e0"},
		{name: "negative one", value: mustBSONIndexRawValueV2(t, int64(-1)), hex: "b220207fffdf", kind: bsonIndexKeyKindNumberV2, canon: "-1e0"},
		{name: "zero", value: mustBSONIndexRawValueV2(t, math.Copysign(0, -1)), hex: "b22030", kind: bsonIndexKeyKindNumberV2, canon: "0"},
		{name: "one point five", value: mustBSONIndexRawValueV2(t, 1.5), hex: "b2204080002600", kind: bsonIndexKeyKindNumberV2, canon: "15e-1"},
		{name: "negative infinity", value: mustBSONIndexRawValueV2(t, math.Inf(-1)), hex: "b22010", kind: bsonIndexKeyKindNumberV2, canon: "-Infinity"},
		{name: "positive infinity", value: mustBSONIndexRawValueV2(t, math.Inf(1)), hex: "b22050", kind: bsonIndexKeyKindNumberV2, canon: "Infinity"},
		{name: "nan", value: mustBSONIndexRawValueV2(t, math.NaN()), hex: "b22008", kind: bsonIndexKeyKindNumberV2, canon: "NaN"},
		{name: "empty string", value: mustBSONIndexRawValueV2(t, ""), hex: "b2300000", kind: bsonIndexKeyKindStringV2, canon: ""},
		{name: "nul string", value: mustBSONIndexRawValueV2(t, "a\x00b"), hex: "b2306100ff620000", kind: bsonIndexKeyKindStringV2, canon: "a\x00b"},
		{name: "object id", value: mustBSONIndexRawValueV2(t, objectID), hex: "b27000112233445566778899aabb", kind: bsonIndexKeyKindObjectIDV2, canon: objectID.Hex()},
		{name: "false", value: mustBSONIndexRawValueV2(t, false), hex: "b28000", kind: bsonIndexKeyKindBoolV2, canon: "false"},
		{name: "true", value: mustBSONIndexRawValueV2(t, true), hex: "b28001", kind: bsonIndexKeyKindBoolV2, canon: "true"},
		{name: "date zero", value: bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 0)}, hex: "b2908000000000000000", kind: bsonIndexKeyKindDateTimeV2, canon: "0"},
		{name: "timestamp", value: bson.RawValue{Type: bson.TypeTimestamp, Value: bsoncore.AppendTimestamp(nil, 1, 2)}, hex: "b2a00000000100000002", kind: bsonIndexKeyKindTimestampV2, canon: "1:2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeBSONIndexKeyComponentV2(tc.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if gotHex := hex.EncodeToString(got); gotHex != tc.hex {
				t.Fatalf("encoding=%s want %s", gotHex, tc.hex)
			}
			decoded, n, err := decodeBSONIndexKeyComponentV2(got)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if n != len(got) || decoded.Descending || decoded.Kind != tc.kind || decoded.Canonical != tc.canon {
				t.Fatalf("decoded=%+v n=%d want kind=%v canon=%q n=%d", decoded, n, tc.kind, tc.canon, len(got))
			}
			length, err := bsonIndexKeyComponentV2Length(got)
			if err != nil || length != len(got) {
				t.Fatalf("component length=%d err=%v want %d", length, err, len(got))
			}
		})
	}
}

func TestBSONIndexKeyCodecV2NumericEqualityAndOrder(t *testing.T) {
	decimalOne := mustBSONIndexDecimal128V2(t, "1.000")
	decimalZero := mustBSONIndexDecimal128V2(t, "-0.000")
	decimalTenth := mustBSONIndexDecimal128V2(t, "0.1")
	decimalPosInf := mustBSONIndexDecimal128V2(t, "Infinity")
	decimalNegInf := mustBSONIndexDecimal128V2(t, "-Infinity")
	decimalNaN := mustBSONIndexDecimal128V2(t, "NaN")

	equalityGroups := [][]bson.RawValue{
		{mustBSONIndexRawValueV2(t, int32(1)), mustBSONIndexRawValueV2(t, int64(1)), mustBSONIndexRawValueV2(t, float64(1)), mustBSONIndexRawValueV2(t, decimalOne)},
		{mustBSONIndexRawValueV2(t, int32(0)), mustBSONIndexRawValueV2(t, int64(0)), mustBSONIndexRawValueV2(t, float64(0)), mustBSONIndexRawValueV2(t, math.Copysign(0, -1)), mustBSONIndexRawValueV2(t, decimalZero)},
		{mustBSONIndexRawValueV2(t, math.Inf(-1)), mustBSONIndexRawValueV2(t, decimalNegInf)},
		{mustBSONIndexRawValueV2(t, math.Inf(1)), mustBSONIndexRawValueV2(t, decimalPosInf)},
		{mustBSONIndexRawValueV2(t, math.NaN()), mustBSONIndexRawValueV2(t, decimalNaN)},
	}
	for groupIndex, group := range equalityGroups {
		first := mustEncodeBSONIndexV2(t, group[0])
		for i := 1; i < len(group); i++ {
			if got := mustEncodeBSONIndexV2(t, group[i]); !bytes.Equal(first, got) {
				t.Fatalf("equality group %d differs: %x vs %x", groupIndex, first, got)
			}
		}
	}

	ordered := []bson.RawValue{
		mustBSONIndexRawValueV2(t, math.NaN()),
		mustBSONIndexRawValueV2(t, math.Inf(-1)),
		mustBSONIndexRawValueV2(t, int64(math.MinInt64)),
		mustBSONIndexRawValueV2(t, int64(-10)),
		mustBSONIndexRawValueV2(t, -0.1),
		mustBSONIndexRawValueV2(t, int32(0)),
		mustBSONIndexRawValueV2(t, decimalTenth),
		mustBSONIndexRawValueV2(t, 0.1), // exact binary float is greater than exact decimal 0.1
		mustBSONIndexRawValueV2(t, int64(9_007_199_254_740_992)),
		mustBSONIndexRawValueV2(t, int64(9_007_199_254_740_993)),
		mustBSONIndexRawValueV2(t, math.Inf(1)),
	}
	assertBSONIndexV2StrictOrder(t, ordered)
}

func TestBSONIndexKeyCodecV2MatchesReferenceBSONOrder(t *testing.T) {
	objectLow, _ := bson.ObjectIDFromHex("00112233445566778899aabb")
	objectHigh, _ := bson.ObjectIDFromHex("10112233445566778899aabb")
	values := []bson.RawValue{
		{},
		{Type: bson.TypeNull},
		mustBSONIndexRawValueV2(t, math.NaN()),
		mustBSONIndexRawValueV2(t, math.Inf(-1)),
		mustBSONIndexRawValueV2(t, int64(-2)),
		mustBSONIndexRawValueV2(t, int32(0)),
		mustBSONIndexRawValueV2(t, 1.25),
		mustBSONIndexRawValueV2(t, math.Inf(1)),
		mustBSONIndexRawValueV2(t, ""),
		mustBSONIndexRawValueV2(t, "a"),
		mustBSONIndexRawValueV2(t, objectLow),
		mustBSONIndexRawValueV2(t, objectHigh),
		mustBSONIndexRawValueV2(t, false),
		mustBSONIndexRawValueV2(t, true),
		{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, -1)},
		{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 1)},
		{Type: bson.TypeTimestamp, Value: bsoncore.AppendTimestamp(nil, 1, 9)},
		{Type: bson.TypeTimestamp, Value: bsoncore.AppendTimestamp(nil, 2, 0)},
	}

	for i := range values {
		for j := range values {
			want := signBSONIndexV2(referenceCompareBSONIndexV2(t, values[i], values[j]))
			left := mustEncodeBSONIndexV2(t, values[i])
			right := mustEncodeBSONIndexV2(t, values[j])
			got := signBSONIndexV2(bytes.Compare(left, right))
			if got != want {
				t.Fatalf("pair (%d,%d) byte compare=%d reference=%d\nleft=%s %x\nright=%s %x", i, j, got, want, values[i].DebugString(), left, values[j].DebugString(), right)
			}
		}
	}
}

func TestBSONIndexKeyCodecV2StringsDescendingAndBoundaries(t *testing.T) {
	stringsInOrder := []string{"", "a", "a\x00", "a\x00a", "aa", "b", "é", "界"}
	values := make([]bson.RawValue, len(stringsInOrder))
	for i, value := range stringsInOrder {
		values[i] = mustBSONIndexRawValueV2(t, value)
	}
	assertBSONIndexV2StrictOrder(t, values)

	for i := 0; i+1 < len(values); i++ {
		left := mustEncodeBSONIndexV2(t, values[i])
		right := mustEncodeBSONIndexV2(t, values[i+1])
		leftDesc, err := descendingBSONIndexKeyComponentV2(left)
		if err != nil {
			t.Fatal(err)
		}
		rightDesc, err := descendingBSONIndexKeyComponentV2(right)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(leftDesc, rightDesc) <= 0 {
			t.Fatalf("descending order not reversed: %x <= %x", leftDesc, rightDesc)
		}
		leftDescLen, err := bsonIndexKeyComponentV2Length(leftDesc)
		if err != nil || leftDescLen != len(leftDesc) {
			t.Fatalf("descending component length=%d err=%v want %d", leftDescLen, err, len(leftDesc))
		}
		roundTrip, err := ascendingBSONIndexKeyComponentV2(leftDesc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(roundTrip, left) {
			t.Fatalf("descending round trip=%x want %x", roundTrip, left)
		}
	}

	first := mustEncodeBSONIndexV2(t, mustBSONIndexRawValueV2(t, "a\x00b"))
	second := mustEncodeBSONIndexV2(t, mustBSONIndexRawValueV2(t, int64(42)))
	compound := append(append([]byte(nil), first...), second...)
	firstLen, err := bsonIndexKeyComponentV2Length(compound)
	if err != nil || firstLen != len(first) {
		t.Fatalf("first component length=%d err=%v want %d", firstLen, err, len(first))
	}
	secondLen, err := bsonIndexKeyComponentV2Length(compound[firstLen:])
	if err != nil || secondLen != len(second) {
		t.Fatalf("second component length=%d err=%v want %d", secondLen, err, len(second))
	}

	key, err := bsonIndexEntryKeyV2(first, []byte("document-7"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := bsonIndexKeyDocumentIDV2(key)
	if err != nil || string(id) != "document-7" {
		t.Fatalf("document ID=%q err=%v", id, err)
	}
	key, err = bsonIndexEntryKeyV2(first, []byte("document\x00seven"))
	if err != nil {
		t.Fatal(err)
	}
	id, err = bsonIndexKeyDocumentIDV2(key)
	if err != nil || string(id) != "document\x00seven" {
		t.Fatalf("escaped document ID=%q err=%v", id, err)
	}
	if _, err := bsonIndexEntryKeyV2(first, bytes.Repeat([]byte{0}, bsonIndexKeyComponentV2MaxBytes/2+1)); err != errBSONIndexKeyV2TooLarge {
		t.Fatalf("over-budget escaped document ID error=%v", err)
	}
	oversizedEntry := append(append([]byte(nil), first...), bsonIndexKeyDocumentIDSuffixMarkerV2)
	oversizedEntry = append(oversizedEntry, bytes.Repeat([]byte{'x'}, bsonIndexKeyComponentV2MaxBytes)...)
	oversizedEntry = append(oversizedEntry, 0, 0)
	if _, err := bsonIndexKeyDocumentIDV2(oversizedEntry); err != errBSONIndexKeyV2TooLarge {
		t.Fatalf("decoded over-budget document ID error=%v", err)
	}
}

func TestBSONIndexKeyCodecV2RejectsUnsupportedMalformedAndOverBudget(t *testing.T) {
	unsupported := []bson.RawValue{
		mustBSONIndexRawValueV2(t, bson.D{{Key: "n", Value: 1}}),
		mustBSONIndexRawValueV2(t, bson.A{1, 2}),
		mustBSONIndexRawValueV2(t, []byte{1, 2}),
		{Type: bson.TypeMinKey},
		{Type: bson.TypeMaxKey},
	}
	for _, value := range unsupported {
		if _, err := encodeBSONIndexKeyComponentV2(value); err == nil || !strings.Contains(err.Error(), "unsupported") {
			t.Fatalf("unsupported %v error=%v", value.Type, err)
		}
	}
	if _, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: []byte{0xff}}); err == nil {
		t.Fatal("malformed raw string accepted")
	}
	fixedWidth := []bson.RawValue{
		mustBSONIndexRawValueV2(t, int32(1)),
		mustBSONIndexRawValueV2(t, int64(1)),
		mustBSONIndexRawValueV2(t, float64(1)),
		mustBSONIndexRawValueV2(t, mustBSONIndexDecimal128V2(t, "1")),
		mustBSONIndexRawValueV2(t, bson.NewObjectID()),
		mustBSONIndexRawValueV2(t, true),
		{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 1)},
		{Type: bson.TypeTimestamp, Value: bsoncore.AppendTimestamp(nil, 1, 2)},
	}
	for _, value := range fixedWidth {
		trailing := value
		trailing.Value = append(append([]byte(nil), value.Value...), 0)
		if _, err := encodeBSONIndexKeyComponentV2(trailing); err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("trailing %s raw bytes accepted: %x err=%v", value.Type, trailing.Value, err)
		}
		truncated := value
		truncated.Value = append([]byte(nil), value.Value[:len(value.Value)-1]...)
		if _, err := encodeBSONIndexKeyComponentV2(truncated); err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("truncated %s raw bytes accepted: %x err=%v", value.Type, truncated.Value, err)
		}
	}
	for _, value := range []bson.RawValue{
		{Type: bson.TypeBoolean, Value: []byte{2}},
		{Type: bson.TypeNull, Value: []byte{0}},
		{Value: []byte{0}},
	} {
		if _, err := encodeBSONIndexKeyComponentV2(value); err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("invalid %s raw bytes accepted: %x err=%v", value.Type, value.Value, err)
		}
	}
	tooLong := strings.Repeat("x", bsonIndexKeyComponentV2MaxBytes)
	if _, err := encodeBSONIndexKeyComponentV2(mustBSONIndexRawValueV2(t, tooLong)); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("over-budget string error=%v", err)
	}
	escapedTooLong := strings.Repeat("\x00", bsonIndexKeyComponentV2MaxBytes/2+1)
	if _, err := encodeBSONIndexKeyComponentV2(mustBSONIndexRawValueV2(t, escapedTooLong)); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("over-budget escaped string error=%v", err)
	}
	overBudgetEncodedString := append(
		[]byte{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagStringV2},
		bytes.Repeat([]byte{'x'}, bsonIndexKeyComponentV2MaxBytes-3)...,
	)
	overBudgetEncodedString = append(overBudgetEncodedString, 0, 0)
	if _, _, err := decodeBSONIndexKeyComponentV2(overBudgetEncodedString); err != errBSONIndexKeyV2TooLarge {
		t.Fatalf("decoded over-budget string error=%v", err)
	}

	validComponent := mustEncodeBSONIndexV2(t, mustBSONIndexRawValueV2(t, "suffix-bound"))
	for _, suffix := range [][]byte{
		{bsonIndexKeyDocumentIDSuffixMarkerV2},
		{bsonIndexKeyDocumentIDSuffixMarkerV2, 'x'},
	} {
		entry := append(append([]byte(nil), validComponent...), suffix...)
		if _, err := bsonIndexKeyDocumentIDV2(entry); err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("truncated document ID suffix %x error=%v", suffix, err)
		}
	}

	corrupt := [][]byte{
		nil,
		{0x00, 0x08},
		{bsonIndexKeyComponentV2AscendingMarker},
		{bsonIndexKeyComponentV2AscendingMarker, 0x11},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagStringV2, 'a'},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagStringV2, 0x00, 0x01},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagBoolV2, 0x02},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagObjectIDV2, 1, 2},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagDateTimeV2, 1},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagTimestampV2, 1},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2, 0x77},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2, bsonIndexNumberPositiveFiniteV2, 0x80, 0x00},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2, bsonIndexNumberPositiveFiniteV2, 0x80, 0x00, 0xb0},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2, bsonIndexNumberPositiveFiniteV2, 0x80, 0x00, 0x10, 0x00}, // leading zero digit
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2, bsonIndexNumberPositiveFiniteV2, 0x80, 0x00, 0x21, 0x00}, // trailing zero digit
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagNumberV2, bsonIndexNumberPositiveFiniteV2, 0x00, 0x00, 0x20},       // decoded exponent below the work bound
	}
	for i, encoded := range corrupt {
		if _, _, err := decodeBSONIndexKeyComponentV2(encoded); err == nil {
			t.Fatalf("corrupt case %d accepted: %x", i, encoded)
		}
		if _, err := bsonIndexKeyComponentV2Length(encoded); err == nil {
			t.Fatalf("length scan accepted corrupt case %d: %x", i, encoded)
		}
	}
	if _, err := bsonIndexKeyDocumentIDV2(mustEncodeBSONIndexV2(t, mustBSONIndexRawValueV2(t, "only-component"))); err == nil {
		t.Fatal("missing document ID accepted")
	}
}

func TestBSONIndexKeyCodecV2RandomReferenceOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(4061))
	values := make([]bson.RawValue, 0, 512)
	for i := 0; i < 128; i++ {
		values = append(values,
			mustBSONIndexRawValueV2(t, rng.Int63()),
			mustBSONIndexRawValueV2(t, math.Float64frombits(rng.Uint64()&^(uint64(1)<<62))),
			mustBSONIndexRawValueV2(t, randomBSONIndexStringV2(rng, 24)),
			bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, rng.Int63())},
		)
	}
	encodings := make([][]byte, len(values))
	for i := range values {
		encodings[i] = mustEncodeBSONIndexV2(t, values[i])
	}
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			want := signBSONIndexV2(referenceCompareBSONIndexV2(t, values[i], values[j]))
			got := signBSONIndexV2(bytes.Compare(encodings[i], encodings[j]))
			if got != want {
				t.Fatalf("random pair (%d,%d) got=%d want=%d left=%v right=%v", i, j, got, want, values[i], values[j])
			}
		}
	}
}

func TestBSONIndexKeyCodecV2ReopenPreservesBytes(t *testing.T) {
	dir := t.TempDir()
	value := mustEncodeBSONIndexV2(t, mustBSONIndexRawValueV2(t, mustBSONIndexDecimal128V2(t, "123456789.012300")))
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("codec-v2"), value); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Get([]byte("codec-v2"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("reopened bytes=%x want %x", got, value)
	}
	if _, n, err := decodeBSONIndexKeyComponentV2(got); err != nil || n != len(got) {
		t.Fatalf("decode reopened n=%d err=%v", n, err)
	}
}

func FuzzBSONIndexKeyCodecV2Decode(f *testing.F) {
	for _, seed := range [][]byte{
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagMissingV2},
		{bsonIndexKeyComponentV2AscendingMarker, bsonIndexKeyTagStringV2, 'a', 0, 0},
		{bsonIndexKeyComponentV2DescendingMarker, ^byte(bsonIndexKeyTagBoolV2), ^byte(1)},
		{0xff, 0xff, 0xff},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, encoded []byte) {
		decoded, n, err := decodeBSONIndexKeyComponentV2(encoded)
		scannedLength, scanErr := bsonIndexKeyComponentV2Length(encoded)
		if err != nil {
			if scanErr == nil {
				t.Fatalf("length scan accepted decoder-rejected input %x at length %d", encoded, scannedLength)
			}
			return
		}
		if scanErr != nil || scannedLength != n {
			t.Fatalf("length scan=%d err=%v decode=%d for %x", scannedLength, scanErr, n, encoded)
		}
		if n <= 0 || n > len(encoded) {
			t.Fatalf("valid decode returned invalid length %d for %x", n, encoded)
		}
		component := encoded[:n]
		if decoded.Descending {
			ascending, err := ascendingBSONIndexKeyComponentV2(component)
			if err != nil {
				t.Fatalf("ascending transform: %v", err)
			}
			descending, err := descendingBSONIndexKeyComponentV2(ascending)
			if err != nil || !bytes.Equal(descending, component) {
				t.Fatalf("descending round trip=%x err=%v want %x", descending, err, component)
			}
		} else {
			descending, err := descendingBSONIndexKeyComponentV2(component)
			if err != nil {
				t.Fatalf("descending transform: %v", err)
			}
			ascending, err := ascendingBSONIndexKeyComponentV2(descending)
			if err != nil || !bytes.Equal(ascending, component) {
				t.Fatalf("ascending round trip=%x err=%v want %x", ascending, err, component)
			}
		}
	})
}

func mustBSONIndexRawValueV2(t testing.TB, value any) bson.RawValue {
	t.Helper()
	typeValue, raw, err := bson.MarshalValue(value)
	if err != nil {
		t.Fatalf("marshal BSON value: %v", err)
	}
	return bson.RawValue{Type: typeValue, Value: raw}
}

func mustBSONIndexDecimal128V2(t testing.TB, value string) bson.Decimal128 {
	t.Helper()
	out, err := bson.ParseDecimal128(value)
	if err != nil {
		t.Fatalf("parse Decimal128 %q: %v", value, err)
	}
	return out
}

func mustEncodeBSONIndexV2(t testing.TB, value bson.RawValue) []byte {
	t.Helper()
	out, err := encodeBSONIndexKeyComponentV2(value)
	if err != nil {
		t.Fatalf("encode %s: %v", value.DebugString(), err)
	}
	return out
}

func assertBSONIndexV2StrictOrder(t testing.TB, values []bson.RawValue) {
	t.Helper()
	for i := 0; i+1 < len(values); i++ {
		left := mustEncodeBSONIndexV2(t, values[i])
		right := mustEncodeBSONIndexV2(t, values[i+1])
		if bytes.Compare(left, right) >= 0 {
			t.Fatalf("value %d does not precede %d: %s %x >= %s %x", i, i+1, values[i].DebugString(), left, values[i+1].DebugString(), right)
		}
	}
}

func referenceCompareBSONIndexV2(t testing.TB, left, right bson.RawValue) int {
	t.Helper()
	if left.IsZero() || right.IsZero() {
		switch {
		case left.IsZero() && right.IsZero():
			return 0
		case left.IsZero():
			return -1
		default:
			return 1
		}
	}
	if left.IsNumber() && right.IsNumber() {
		return referenceCompareBSONNumbersV2(t, left, right)
	}
	leftRank := referenceBSONIndexTypeRankV2(left.Type)
	rightRank := referenceBSONIndexTypeRankV2(right.Type)
	if leftRank != rightRank {
		return leftRank - rightRank
	}
	switch left.Type {
	case bson.TypeNull:
		return 0
	case bson.TypeString:
		leftValue, leftOK := left.StringValueOK()
		rightValue, rightOK := right.StringValueOK()
		if !leftOK || !rightOK {
			t.Fatal("invalid reference string")
		}
		return strings.Compare(leftValue, rightValue)
	case bson.TypeObjectID:
		leftValue, leftOK := left.ObjectIDOK()
		rightValue, rightOK := right.ObjectIDOK()
		if !leftOK || !rightOK {
			t.Fatal("invalid reference ObjectID")
		}
		return bytes.Compare(leftValue[:], rightValue[:])
	case bson.TypeBoolean:
		leftValue, leftOK := left.BooleanOK()
		rightValue, rightOK := right.BooleanOK()
		if !leftOK || !rightOK {
			t.Fatal("invalid reference bool")
		}
		if leftValue == rightValue {
			return 0
		}
		if !leftValue {
			return -1
		}
		return 1
	case bson.TypeDateTime:
		leftValue, leftOK := left.DateTimeOK()
		rightValue, rightOK := right.DateTimeOK()
		if !leftOK || !rightOK {
			t.Fatal("invalid reference date")
		}
		return signBSONIndexV2(int64CompareBSONIndexV2(leftValue, rightValue))
	case bson.TypeTimestamp:
		leftT, leftI, leftOK := left.TimestampOK()
		rightT, rightI, rightOK := right.TimestampOK()
		if !leftOK || !rightOK {
			t.Fatal("invalid reference timestamp")
		}
		if leftT != rightT {
			return signBSONIndexV2(int64(leftT) - int64(rightT))
		}
		return signBSONIndexV2(int64(leftI) - int64(rightI))
	default:
		t.Fatalf("unsupported reference type %s", left.Type)
		return 0
	}
}

func referenceCompareBSONNumbersV2(t testing.TB, left, right bson.RawValue) int {
	t.Helper()
	leftRat, leftClass := referenceBSONNumberV2(t, left)
	rightRat, rightClass := referenceBSONNumberV2(t, right)
	if leftClass != rightClass {
		return leftClass - rightClass
	}
	if leftClass != 2 {
		return 0
	}
	return leftRat.Cmp(rightRat)
}

func referenceBSONNumberV2(t testing.TB, value bson.RawValue) (*big.Rat, int) {
	t.Helper()
	switch value.Type {
	case bson.TypeInt32:
		v, ok := value.Int32OK()
		if !ok {
			t.Fatal("invalid int32")
		}
		return big.NewRat(int64(v), 1), 2
	case bson.TypeInt64:
		v, ok := value.Int64OK()
		if !ok {
			t.Fatal("invalid int64")
		}
		return big.NewRat(v, 1), 2
	case bson.TypeDouble:
		v, ok := value.DoubleOK()
		if !ok {
			t.Fatal("invalid double")
		}
		switch {
		case math.IsInf(v, -1):
			return nil, 1
		case math.IsInf(v, 1):
			return nil, 3
		case math.IsNaN(v):
			return nil, 0
		default:
			rat := new(big.Rat)
			if rat.SetFloat64(v) == nil {
				t.Fatal("finite float has no rational")
			}
			return rat, 2
		}
	case bson.TypeDecimal128:
		v, ok := value.Decimal128OK()
		if !ok {
			t.Fatal("invalid decimal")
		}
		if v.IsNaN() {
			return nil, 0
		}
		switch v.IsInf() {
		case -1:
			return nil, 1
		case 1:
			return nil, 3
		}
		coefficient, exponent, err := v.BigInt()
		if err != nil {
			t.Fatal(err)
		}
		rat := new(big.Rat).SetInt(coefficient)
		if exponent != 0 {
			scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absBSONIndexV2(exponent))), nil)
			if exponent > 0 {
				rat.Mul(rat, new(big.Rat).SetInt(scale))
			} else {
				rat.Quo(rat, new(big.Rat).SetInt(scale))
			}
		}
		return rat, 2
	default:
		t.Fatalf("not numeric: %s", value.Type)
		return nil, 0
	}
}

func referenceBSONIndexTypeRankV2(valueType bson.Type) int {
	switch valueType {
	case bson.TypeNull:
		return 1
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return 2
	case bson.TypeString:
		return 3
	case bson.TypeObjectID:
		return 7
	case bson.TypeBoolean:
		return 8
	case bson.TypeDateTime:
		return 9
	case bson.TypeTimestamp:
		return 10
	default:
		return 50 + int(valueType)
	}
}

func int64CompareBSONIndexV2(left, right int64) int64 {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func signBSONIndexV2[T ~int | ~int64](value T) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}

func randomBSONIndexStringV2(rng *rand.Rand, max int) string {
	length := rng.Intn(max + 1)
	out := make([]byte, length)
	for i := range out {
		switch rng.Intn(10) {
		case 0:
			out[i] = 0
		default:
			out[i] = byte(rng.Intn(0x7f))
		}
	}
	return string(out)
}

func absBSONIndexV2(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
