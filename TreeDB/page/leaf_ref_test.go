package page

import "testing"

func TestLeafRef_EncodeDecode_RoundTrip(t *testing.T) {
	in := ValuePtr{
		FileID: ValueLogFileID(123),
		Offset: 456,
		Length: 999, // ignored by EncodeLeafRef
	}
	id, err := EncodeLeafRef(in)
	if err != nil {
		t.Fatalf("EncodeLeafRef: %v", err)
	}
	got, ok := DecodeLeafRef(id)
	if !ok {
		t.Fatalf("DecodeLeafRef ok=false")
	}
	if got.FileID != in.FileID {
		t.Fatalf("FileID=%d want %d", got.FileID, in.FileID)
	}
	if got.Offset != in.Offset {
		t.Fatalf("Offset=%d want %d", got.Offset, in.Offset)
	}
	if !ValuePtrIsGrouped(got) {
		t.Fatalf("expected grouped ValuePtr for LeafRef")
	}
	if sub := ValuePtrSubIndex(got); sub != 0 {
		t.Fatalf("subIndex=%d want 0", sub)
	}
	if hint := ValuePtrRecordLength(got); hint != 0 {
		t.Fatalf("recordLengthHint=%d want 0", hint)
	}
}

func TestLeafRef_EncodeRejectsNonValueLogFileID(t *testing.T) {
	_, err := EncodeLeafRef(ValuePtr{FileID: 123, Offset: 0})
	if err == nil {
		t.Fatalf("expected EncodeLeafRef to reject non-value-log file id")
	}
}

func TestLeafRef_EncodeRejectsOffsetOverflow(t *testing.T) {
	_, err := EncodeLeafRef(ValuePtr{FileID: ValueLogFileID(1), Offset: uint64(^uint32(0)) + 1})
	if err == nil {
		t.Fatalf("expected EncodeLeafRef to reject u32 overflow offset")
	}
}

func TestLeafRef_DecodeRejectsNonValueLogFileID(t *testing.T) {
	id := (uint64(123) << 32) | 1 // FileID without value-log marker.
	if _, ok := DecodeLeafRef(id); ok {
		t.Fatalf("expected DecodeLeafRef ok=false for non-value-log file id")
	}
}
