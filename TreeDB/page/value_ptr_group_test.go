package page

import "testing"

func TestValuePtrGroupedFlags(t *testing.T) {
	baseLen := uint32(1234)
	ptr := ValuePtr{
		Length: ValuePtrMarkGrouped(ValuePtrMarkCompressed(baseLen), 3),
	}
	if !ValuePtrIsGrouped(ptr) {
		t.Fatalf("expected grouped flag set")
	}
	if got := ValuePtrSubIndex(ptr); got != 3 {
		t.Fatalf("subIndex mismatch got=%d want=3", got)
	}
	if got := ValuePtrRecordLength(ptr); got != baseLen {
		t.Fatalf("record length stripped mismatch got=%d want=%d", got, baseLen)
	}
}
