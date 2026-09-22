package collections

import (
	"reflect"
	"testing"
)

func TestTypedGraphContiguousProjectionWorkspace(t *testing.T) {
	columns := []ColumnStoreColumn{{Name: "vector"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	rows := make([]columnDeclaredRow, 128)
	for i := range rows {
		rows[i] = columnDeclaredRow{ID: []byte("id"), Values: []columnDeclaredValue{{}, {String: "content"}, {String: "user"}, {String: "path"}}}
	}
	before := cloneColumnDeclaredValues(rows[0].Values)
	got, err := projectColumnDeclaredRowsForColumns(columns, columns[1:3], rows)
	if err != nil || len(got) != len(rows) {
		t.Fatalf("rows=%d err=%v", len(got), err)
	}
	if cap(got[0].Values) != 2 || &got[0].Values[0] != &rows[0].Values[1] || &got[0].ID[0] != &rows[0].ID[0] {
		t.Fatal("internal read-only contiguous projection copied row IDs/union headers or retained excess capacity")
	}
	if !reflect.DeepEqual(rows[0].Values, before) {
		t.Fatal("projection mutated input")
	}
	if !collectionsRaceEnabled {
		allocs := testing.AllocsPerRun(5, func() {
			if _, err := projectColumnDeclaredRowsForColumns(columns, columns[1:3], rows); err != nil {
				panic(err)
			}
		})
		if allocs > 3 {
			t.Fatalf("projection allocs=%g want fixed <=3, independent of rows", allocs)
		}
	}
	for _, selected := range [][]ColumnStoreColumn{{columns[2], columns[0]}, {columns[0], columns[2]}} {
		projected, err := projectColumnDeclaredRowsForColumns(columns, selected, rows)
		if err != nil || len(projected[0].Values) != 2 {
			t.Fatal(err)
		}
		if &projected[0].Values[0] == &rows[0].Values[0] {
			t.Fatal("reordered/noncontiguous fallback aliased union headers")
		}
	}
	if _, err := projectColumnDeclaredRowsForColumns(columns, columns[1:3], []columnDeclaredRow{{ID: []byte("bad")}}); err == nil {
		t.Fatal("accepted invalid live row width")
	}
	if _, err := projectColumnDeclaredRowsForColumns(columns, []ColumnStoreColumn{{Name: "missing"}}, rows); err == nil {
		t.Fatal("accepted missing column")
	}
	deleted, err := projectColumnDeclaredRowsForColumns(columns, columns[1:3], []columnDeclaredRow{{ID: []byte("deleted"), Deleted: true}})
	if err != nil || !deleted[0].Deleted || len(deleted[0].Values) != 0 {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}
}
