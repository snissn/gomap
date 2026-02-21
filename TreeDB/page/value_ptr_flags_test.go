package page

import "testing"

func TestValuePtrRecordLength_GroupedKeepsBit23(t *testing.T) {
	const recordLen = uint32(0x00800080)
	ptr := ValuePtr{Length: ValuePtrMarkGrouped(recordLen, 3)}
	if !ValuePtrIsGrouped(ptr) {
		t.Fatalf("expected grouped pointer")
	}
	if got := ValuePtrRecordLength(ptr); got != recordLen {
		t.Fatalf("record length mismatch: got=%#x want=%#x", got, recordLen)
	}
}

func TestValuePtrRecordLengthHintMatches_GroupedLegacyFenceMarkerCompat(t *testing.T) {
	const expected = uint32(0x00000080)
	ptr := ValuePtr{Length: ValuePtrMarkGrouped(expected, 1)}
	if !ValuePtrRecordLengthHintMatches(ptr, expected) {
		t.Fatalf("expected direct grouped hint to match")
	}

	// Legacy grouped fence markers reused bit23 and collided with grouped hints.
	legacy := ptr
	legacy.Length |= valuePtrFenceOuterGroupedMask
	if !ValuePtrRecordLengthHintMatches(legacy, expected) {
		t.Fatalf("expected legacy grouped fence marker compatibility match")
	}
	if ValuePtrRecordLengthHintMatches(legacy, expected+1) {
		t.Fatalf("unexpected match for wrong expected length")
	}
}

func TestValuePtrRecordLengthHintMatches_GroupedLegacyMarkerOnlyAsOmitted(t *testing.T) {
	ptr := ValuePtr{Length: ValuePtrMarkGrouped(0, 1)}
	legacy := ptr
	legacy.Length |= valuePtrFenceOuterGroupedMask
	if !ValuePtrRecordLengthHintMatches(legacy, 12345) {
		t.Fatalf("expected marker-only grouped hint to be treated as omitted")
	}
}

func TestValuePtrMarkFenceOuter_GroupedNoOp(t *testing.T) {
	ptr := ValuePtr{Length: ValuePtrMarkGrouped(1234, 2)}
	marked := ValuePtrMarkFenceOuter(ptr)
	if marked != ptr {
		t.Fatalf("grouped fence mark should be no-op: got=%#x want=%#x", marked.Length, ptr.Length)
	}
}

func TestValuePtrMarkFenceOuter_NonGrouped(t *testing.T) {
	ptr := ValuePtr{Length: 1234}
	marked := ValuePtrMarkFenceOuter(ptr)
	if !ValuePtrIsFenceOuter(marked) {
		t.Fatalf("expected non-grouped pointer to be fence-marked")
	}
	if got, want := ValuePtrRecordLength(marked), uint32(1234); got != want {
		t.Fatalf("non-grouped record length mismatch: got=%d want=%d", got, want)
	}
}
