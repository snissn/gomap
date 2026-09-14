package quantizedasset

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestPreparedOneColumnRetainedMetadataBoundForPointerBytes(t *testing.T) {
	for _, tc := range []struct {
		pointerBytes uintptr
		want         uintptr
		ok           bool
	}{
		{pointerBytes: 8, want: 648, ok: true},
		{pointerBytes: 4, want: 2 << 10, ok: true},
		{pointerBytes: 16, want: 0, ok: false},
	} {
		got, ok := preparedOneColumnRetainedMetadataBoundForPointerBytes(tc.pointerBytes)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("pointer bytes=%d got=(%d,%t) want=(%d,%t)", tc.pointerBytes, got, ok, tc.want, tc.ok)
		}
	}
}

func TestPreparedOneColumnRetainedMetadataBound64BitShape(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("64-bit shape assertion")
	}
	if got, want := reflect.TypeFor[Prepared]().Size(), uintptr(80); got != want {
		t.Fatalf("Prepared size=%d want=%d; update one-column retained bound", got, want)
	}
	if got, want := reflect.TypeFor[preparedColumn]().Size(), uintptr(144); got != want {
		t.Fatalf("preparedColumn size=%d want=%d; update one-column retained bound", got, want)
	}
	if got, want := reflect.TypeFor[ColumnFootprint]().Size(), uintptr(152); got != want {
		t.Fatalf("ColumnFootprint size=%d want=%d; update one-column retained bound", got, want)
	}
	prepared := &Prepared{columns: make(map[Role]preparedColumn, 1)}
	prepared.columns[RoleCodes] = preparedColumn{}
	prepared.footprint.Columns = append(prepared.footprint.Columns, ColumnFootprint{})
	if got, want := len(prepared.columns), 1; got != want {
		t.Fatalf("columns=%d want=%d", got, want)
	}
	if got, want := cap(prepared.footprint.Columns), 1; got != want {
		t.Fatalf("footprint capacity=%d want=%d; update one-column retained bound", got, want)
	}
	if got, ok := PreparedOneColumnRetainedMetadataBound(); !ok || got != 648 {
		t.Fatalf("bound=(%d,%t) want=(648,true)", got, ok)
	}
}
