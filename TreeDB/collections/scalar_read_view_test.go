package collections

import (
	"context"
	"errors"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionReadViewScalarEquality(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		name := "immediate"
		if buffered {
			name = "buffered"
		}
		t.Run(name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mgr := NewCollectionManager(db)
			if _, err := mgr.CreateCollection(&CollectionMeta{Name: "rows", Options: CollectionOptions{BufferedIndexedWrites: buffered, DisableIndexedWriteMemtables: !buffered}, Indexes: []IndexDefinition{{Name: "user", Field: "user", ValueType: IndexValueString}, {Name: "path", Field: "path", ValueType: IndexValueString}}}); err != nil {
				t.Fatal(err)
			}
			col, err := mgr.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := col.Insert([]byte("a"), []byte(`{"user":"u","path":"old"}`)); err != nil {
				t.Fatal(err)
			}
			view, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			var users []string
			if err := view.VisitIndexValueIDs("user", "u", func(id []byte) error { users = append(users, string(id)); return nil }); err != nil {
				t.Fatal(err)
			}
			writer, err := NewCollectionManager(db).OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := writer.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return []byte(`{"user":"v","path":"new"}`), true, nil }); err != nil {
				t.Fatal(err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
			var paths []string
			if err := view.VisitIndexValueIDs("path", "old", func(id []byte) error { paths = append(paths, string(id)); return nil }); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(users, []string{"a"}) || !reflect.DeepEqual(paths, users) {
				t.Fatalf("captured conjunction: users=%v paths=%v", users, paths)
			}
			if err := view.VisitIndexValueIDs("path", "new", func([]byte) error { t.Fatal("held view saw later writer"); return nil }); err != nil {
				t.Fatal(err)
			}
			for _, probe := range []struct {
				index string
				value any
			}{{"missing", "u"}, {"user", 12}} {
				if err := view.VisitIndexValueIDs(probe.index, probe.value, func([]byte) error { return nil }); !errors.Is(err, ErrHybridSearchIndexUnavailable) {
					t.Fatalf("invalid probe=%+v err=%v", probe, err)
				}
			}
			calls := 0
			if err := view.VisitIndexValueIDs("user", "u", func([]byte) error { calls++; return context.Canceled }); !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("callback calls=%d err=%v", calls, err)
			}
			if err := view.VisitIndexValueIDs("user", "u", nil); err == nil {
				t.Fatal("nil callback accepted")
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			if err := view.VisitIndexValueIDs("user", "u", func([]byte) error { return nil }); err == nil {
				t.Fatal("closed view accepted")
			}
			next, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			var latest []string
			if err := next.VisitIndexValueIDs("path", "new", func(id []byte) error { latest = append(latest, string(id)); return nil }); err != nil || !reflect.DeepEqual(latest, users) {
				t.Fatalf("latest=%v err=%v", latest, err)
			}
		})
	}
}
