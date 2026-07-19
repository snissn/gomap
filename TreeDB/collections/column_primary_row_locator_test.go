package collections

import (
	"strings"
	"testing"
)

func TestColumnPrimaryRowLocatorRoundTripAndCorruptionP3890(t *testing.T) {
	ref := DocumentRowRef{DocumentID: []byte("doc-7"), Generation: 3, PartID: 1, RowIndex: 17, AppliedCommandLSN: 29}
	encoded := encodeColumnPrimaryRowLocator(ref)
	got, err := decodeColumnPrimaryRowLocator(ref.DocumentID, encoded)
	if err != nil {
		t.Fatalf("decode locator: %v", err)
	}
	if got.Generation != ref.Generation || got.PartID != ref.PartID || got.RowIndex != ref.RowIndex || got.AppliedCommandLSN != ref.AppliedCommandLSN || string(got.DocumentID) != string(ref.DocumentID) {
		t.Fatalf("decoded locator=%+v want %+v", got, ref)
	}
	encoded[0] ^= 0xff
	if _, err := decodeColumnPrimaryRowLocator(ref.DocumentID, encoded); err == nil || !strings.Contains(err.Error(), "invalid primary row locator") {
		t.Fatalf("corrupt locator err=%v want fail-closed invalid locator", err)
	}
	if _, err := decodeColumnPrimaryRowLocator(ref.DocumentID, nil); err == nil || !strings.Contains(err.Error(), "invalid primary row locator") {
		t.Fatalf("empty live locator err=%v want fail-closed invalid locator", err)
	}
}

func TestColumnPrimaryRowLocatorDeleteUsesTombstoneP3890(t *testing.T) {
	table, err := buildColumnPrimaryRowLocatorTable(
		ColumnPublishPlan{Operation: ColumnPublishOperationDelete, Rows: 1},
		[]columnWriteDocument{{ID: []byte("doc-7")}},
	)
	if err != nil {
		t.Fatalf("build delete locator table: %v", err)
	}
	it := table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	if !it.Valid() || string(it.UnsafeKey()) != "doc-7" {
		t.Fatalf("delete locator iterator valid=%t key=%q", it.Valid(), it.UnsafeKey())
	}
	if !it.IsDeleted() {
		t.Fatal("delete locator entry is not a tombstone")
	}
}
