package mongogateway

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustMarshalFindDoc(t *testing.T, v any) wire.Document {
	t.Helper()
	raw, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return wire.Document(raw)
}

func TestParseFastPrimaryEqualityFind(t *testing.T) {
	valid := []bson.D{
		{
			{Key: "find", Value: "docs"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: "doc-1"}}},
			{Key: "limit", Value: int32(1)},
			{Key: "singleBatch", Value: true},
			{Key: "$db", Value: "app"},
		},
		{
			{Key: "find", Value: "docs"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: bson.NewObjectID()}}},
			{Key: "batchSize", Value: int32(10)},
			{Key: "$db", Value: "app"},
		},
		{
			{Key: "find", Value: "docs"},
			{Key: "filter", Value: bson.D{{Key: "_id", Value: true}}},
			{Key: "$db", Value: "app"},
		},
	}
	for i, doc := range valid {
		req, ok := parseFastPrimaryEqualityFind(mustMarshalFindDoc(t, doc))
		if !ok {
			t.Fatalf("case %d: expected fast-path parse ok=true", i)
		}
		if req.collection != "docs" || req.database != "app" {
			t.Fatalf("case %d: got col=%q db=%q want docs/app", i, req.collection, req.database)
		}
	}

	ineligible := []struct {
		name string
		doc  bson.D
	}{
		{"projection", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "projection", Value: bson.D{{Key: "_id", Value: 0}}}, {Key: "$db", Value: "app"}}},
		{"sort", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "$db", Value: "app"}}},
		{"hint", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "hint", Value: "_id_"}, {Key: "$db", Value: "app"}}},
		{"skip", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "skip", Value: int32(1)}, {Key: "$db", Value: "app"}}},
		{"negative_limit", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "limit", Value: int32(-1)}, {Key: "$db", Value: "app"}}},
		{"zero_batch_size", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "batchSize", Value: int32(0)}, {Key: "$db", Value: "app"}}},
		{"non_id_filter", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "email", Value: "a@b.c"}}}, {Key: "$db", Value: "app"}}},
		{"multi_filter", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}, {Key: "x", Value: int32(1)}}}, {Key: "$db", Value: "app"}}},
		{"operator_filter", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: "a"}}}}}, {Key: "$db", Value: "app"}}},
		{"numeric_id", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: int32(42)}}}, {Key: "$db", Value: "app"}}},
		{"null_id", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: nil}}}, {Key: "$db", Value: "app"}}},
		{"read_concern", bson.D{{Key: "find", Value: "docs"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}}, {Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}}, {Key: "$db", Value: "app"}}},
	}
	for _, tc := range ineligible {
		if _, ok := parseFastPrimaryEqualityFind(mustMarshalFindDoc(t, tc.doc)); ok {
			t.Fatalf("%s: expected fast-path parse ok=false", tc.name)
		}
	}
}

func TestFastPrimaryEqualityFindParityBSONAndJSON(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()

			srv := NewServer()
			srv.Collections = collections.NewCollectionManager(db)
			srv.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: format}

			insertCmd := mustMarshalFindDoc(t, bson.D{
				{Key: "insert", Value: "items"},
				{Key: "documents", Value: bson.A{
					bson.D{{Key: "_id", Value: "k1"}, {Key: "name", Value: "alpha"}},
					bson.D{{Key: "_id", Value: "k2"}, {Key: "name", Value: "beta"}},
				}},
				{Key: "$db", Value: "testdb"},
			})
			if _, err := srv.commandResponse(context.Background(), "insert", insertCmd, nil, 1); err != nil {
				t.Fatalf("insert: %v", err)
			}

			for _, tc := range []struct {
				name      string
				col       string
				id        string
				wantCount int
				wantName  string
			}{
				{"hit", "items", "k2", 1, "beta"},
				{"miss_doc", "items", "missing", 0, ""},
				{"miss_col", "no_such_col", "k1", 0, ""},
			} {
				cmd := mustMarshalFindDoc(t, bson.D{
					{Key: "find", Value: tc.col},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: tc.id}}},
					{Key: "limit", Value: int32(1)},
					{Key: "singleBatch", Value: true},
					{Key: "$db", Value: "testdb"},
				})
				rawMsg, err := srv.findMsgResponseInto(context.Background(), nil, cmd, 7, 3, 1)
				if err != nil {
					t.Fatalf("%s: findMsgResponseInto: %v", tc.name, err)
				}
				h, err := wire.ParseHeader(rawMsg)
				if err != nil {
					t.Fatalf("%s: ParseHeader: %v", tc.name, err)
				}
				if int(h.MessageLength) != len(rawMsg) || h.OpCode != wire.OpMsg {
					t.Fatalf("%s: bad header %+v len=%d", tc.name, h, len(rawMsg))
				}
				msg, err := wire.ParseMsg(rawMsg[wire.HeaderLen:])
				if err != nil {
					t.Fatalf("%s: ParseMsg: %v", tc.name, err)
				}
				body := bson.Raw(msg.Body)
				if okVal, ok := body.Lookup("ok").DoubleOK(); !ok || okVal != 1.0 {
					t.Fatalf("%s: ok=%v (%t)", tc.name, okVal, ok)
				}
				cursor, ok := body.Lookup("cursor").DocumentOK()
				if !ok {
					t.Fatalf("%s: missing cursor", tc.name)
				}
				if ns, _ := cursor.Lookup("ns").StringValueOK(); ns != "testdb."+tc.col {
					t.Fatalf("%s: ns=%q want %q", tc.name, ns, "testdb."+tc.col)
				}
				if cid, ok := cursor.Lookup("id").Int64OK(); !ok || cid != 0 {
					t.Fatalf("%s: cursor.id=%d (%t) want 0", tc.name, cid, ok)
				}
				arr, ok := cursor.Lookup("firstBatch").ArrayOK()
				if !ok {
					t.Fatalf("%s: missing firstBatch", tc.name)
				}
				vals, err := arr.Values()
				if err != nil {
					t.Fatalf("%s: firstBatch values: %v", tc.name, err)
				}
				if len(vals) != tc.wantCount {
					t.Fatalf("%s: count=%d want %d", tc.name, len(vals), tc.wantCount)
				}
				if tc.wantCount == 1 {
					doc, ok := vals[0].DocumentOK()
					if !ok {
						t.Fatalf("%s: batch element not doc", tc.name)
					}
					if got, _ := doc.Lookup("name").StringValueOK(); got != tc.wantName {
						t.Fatalf("%s: name=%q want %q", tc.name, got, tc.wantName)
					}
				}
			}
		})
	}
}
