package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCreateAndListCompoundDescendingBSONIndex(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	assertOK(t, serveCommand(t, server, 40631, bson.D{
		{Key: "createIndexes", Value: "events"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "createdAt", Value: int32(-1)}, {Key: "_id", Value: int32(1)}}}, {Key: "name", Value: "tenant_created_id"}, {Key: "unique", Value: true}}}},
		{Key: "$db", Value: "app"},
	}))
	indexes := cursorFirstBatch(t, serveCommand(t, server, 40632, bson.D{{Key: "listIndexes", Value: "events"}, {Key: "$db", Value: "app"}}))
	if len(indexes) != 2 {
		t.Fatalf("listIndexes count=%d want 2", len(indexes))
	}
	key := indexes[1].Lookup("key").Document()
	elements, err := key.Elements()
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		field string
		dir   int32
	}{{"tenant", 1}, {"createdAt", -1}, {"_id", 1}}
	if len(elements) != len(want) {
		t.Fatalf("component count=%d want %d", len(elements), len(want))
	}
	for i, element := range elements {
		field, _ := element.KeyErr()
		got, _ := element.Value().Int32OK()
		if field != want[i].field || got != want[i].dir {
			t.Fatalf("component[%d]=%s:%d want %s:%d", i, field, got, want[i].field, want[i].dir)
		}
	}
	unique, ok := indexes[1].Lookup("unique").BooleanOK()
	if !ok || !unique {
		t.Fatalf("listIndexes unique=%v ok=%v want true", unique, ok)
	}
}
