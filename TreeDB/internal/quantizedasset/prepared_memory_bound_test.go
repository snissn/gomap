package quantizedasset

import (
	"reflect"
	"strconv"
	"testing"
)

func TestPreparedOneColumnRetainedMetadataBound(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Skip("the fixed bound is intentionally conservative on non-64-bit targets")
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
	if got, want := PreparedOneColumnRetainedMetadataBound(), uintptr(648); got != want {
		t.Fatalf("bound=%d want=%d", got, want)
	}
}
