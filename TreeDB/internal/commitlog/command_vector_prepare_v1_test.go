package commitlog

import (
	"bytes"
	"strings"
	"testing"
)

func vectorPrepareFixtureV1() VectorPrepareV1 {
	return VectorPrepareV1{Version: 1, Operation: "prepare", Collection: "docs", Index: "embedding", Group: "data-a", IndexDefinitionDigest: strings.Repeat("a", 64), Generation: 7, MaxSourceRows: 512, SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4}
}
func TestVectorPreparePayloadV1CanonicalAndBounded(t *testing.T) {
	v := vectorPrepareFixtureV1()
	raw, err := EncodeVectorPreparePayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeVectorPreparePayloadV1(raw)
	if err != nil || got != v {
		t.Fatalf("roundtrip=%+v err=%v", got, err)
	}
	for _, bad := range [][]byte{append(append([]byte(nil), raw...), byte(' ')), append(append([]byte(nil), raw...), []byte("{}")...), bytes.Replace(raw, []byte("\"Version\":1"), []byte("\"Version\":1,\"Unknown\":1"), 1)} {
		if _, err := DecodeVectorPreparePayloadV1(bad); err == nil {
			t.Fatal("accepted noncanonical or unknown payload")
		}
	}
	cases := []VectorPrepareV1{v, v, v, v, v, v, v}
	cases[0].MaxSourceRows = VectorPrepareMaxSourceRowsV1 + 1
	cases[1].SourceRowCount = 513
	cases[2].Group = ""
	cases[3].Term = 1
	cases[4].IndexDefinitionDigest = strings.Repeat("A", 64)
	cases[5].Operation = "activate"
	cases[6].Generation = 0
	for i, bad := range cases {
		if _, err := EncodeVectorPreparePayloadV1(bad); err == nil {
			t.Fatalf("accepted malformed case %d", i)
		}
	}
	v.Term, v.IndexPosition, v.CommandDigest = 2, 8, strings.Repeat("b", 64)
	if _, err := EncodeVectorPreparePayloadV1(v); err != nil {
		t.Fatal(err)
	}
	v.CommandDigest = ""
	if _, err := EncodeVectorPreparePayloadV1(v); err == nil {
		t.Fatal("accepted position without command digest")
	}
}
func TestVectorPrepareRebuildV1HasSeparateSourceAuthority(t *testing.T) {
	v := vectorPrepareFixtureV1()
	v.Operation = "rebuild"
	if _, err := EncodeVectorPreparePayloadV1(v); err == nil {
		t.Fatal("rebuild accepted caller source")
	}
	v.SourceGeneration, v.SourceChecksum, v.SourceSchemaHash, v.SourceRowCount = 0, 0, 0, 0
	if _, err := EncodeVectorPreparePayloadV1(v); err != nil {
		t.Fatal(err)
	}
}
