package commitlog

import (
	"bytes"
	"testing"
)

func TestEncodeCollectionInsertBatchByIDPayloadToParity(t *testing.T) {
	docs := []CollectionDocument{
		{ID: []byte("doc-002"), Document: []byte(`{"k":2,"v":"beta"}`)},
		{ID: []byte("doc-001"), Document: []byte(`{"k":1,"v":"alpha"}`)},
		{ID: []byte("doc-003"), Document: []byte(`{"k":3,"v":"gamma"}`)},
	}
	want, err := EncodeCollectionInsertBatchByIDPayload("users", docs)
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}

	gotNil, err := EncodeCollectionInsertBatchByIDPayloadTo(nil, "users", docs)
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayloadTo(nil): %v", err)
	}
	if !bytes.Equal(gotNil, want) {
		t.Fatalf("nil dst mismatch: got %x want %x", gotNil, want)
	}

	// Pre-populate dst with non-zero bytes and sufficient capacity to verify reuse
	// overwrites every byte cleanly without allocating a new backing array.
	buf := bytes.Repeat([]byte{0xaa}, len(want)+64)
	gotReused, err := EncodeCollectionInsertBatchByIDPayloadTo(buf[:0], "users", docs)
	if err != nil {
		t.Fatalf("EncodeCollectionInsertBatchByIDPayloadTo(reused): %v", err)
	}
	if !bytes.Equal(gotReused, want) {
		t.Fatalf("reused dst mismatch: got %x want %x", gotReused, want)
	}
	if &gotReused[0] != &buf[0] {
		t.Fatalf("reused dst unexpectedly reallocated backing array")
	}
}
