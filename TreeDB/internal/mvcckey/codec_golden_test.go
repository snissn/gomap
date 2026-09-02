package mvcckey

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"testing"
)

type codecGoldenCase struct {
	Name        string `json:"name"`
	LogicalHex  string `json:"logical_hex"`
	Timestamp   uint64 `json:"timestamp"`
	PhysicalHex string `json:"physical_hex"`
}

type codecGoldenFixture struct {
	Schema        string            `json:"schema"`
	Cases         []codecGoldenCase `json:"cases"`
	PhysicalOrder []string          `json:"physical_order"`
}

func TestCodecV1GoldenFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/codec_v1_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture codecGoldenFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Schema != "treedb-mvcc-key-codec-v1" {
		t.Fatalf("schema=%q", fixture.Schema)
	}
	type encodedCase struct {
		name     string
		physical []byte
	}
	encoded := make([]encodedCase, 0, len(fixture.Cases))
	for _, testCase := range fixture.Cases {
		logical, err := hex.DecodeString(testCase.LogicalHex)
		if err != nil {
			t.Fatalf("%s logical: %v", testCase.Name, err)
		}
		wantPhysical, err := hex.DecodeString(testCase.PhysicalHex)
		if err != nil {
			t.Fatalf("%s physical: %v", testCase.Name, err)
		}
		gotPhysical, err := Encode(logical, testCase.Timestamp)
		if err != nil {
			t.Fatalf("%s Encode: %v", testCase.Name, err)
		}
		if !bytes.Equal(gotPhysical, wantPhysical) {
			t.Fatalf("%s physical=%x want=%x", testCase.Name, gotPhysical, wantPhysical)
		}
		gotLogical, gotTimestamp, err := Decode(wantPhysical)
		if err != nil || !bytes.Equal(gotLogical, logical) || gotTimestamp != testCase.Timestamp {
			t.Fatalf("%s Decode=(%x,%d,%v) want=(%x,%d,nil)", testCase.Name, gotLogical, gotTimestamp, err, logical, testCase.Timestamp)
		}
		encoded = append(encoded, encodedCase{name: testCase.Name, physical: gotPhysical})
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i].physical, encoded[j].physical) < 0 })
	gotOrder := make([]string, len(encoded))
	for i := range encoded {
		gotOrder[i] = encoded[i].name
	}
	if !equalStrings(gotOrder, fixture.PhysicalOrder) {
		t.Fatalf("physical order=%v want=%v", gotOrder, fixture.PhysicalOrder)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
