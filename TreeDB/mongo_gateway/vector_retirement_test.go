package mongogateway

import (
	"context"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestStandaloneServerOfficialDriverRejectsUnsupportedVectorDDL(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, journal := range []bool{false, true} {
			name := map[bool]string{false: "absent", true: "existing"}[existing] + "/" + map[bool]string{false: "w1", true: "journaled"}[journal]
			t.Run(name, func(t *testing.T) {
				standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
				if err != nil {
					t.Fatal(err)
				}
				client, cancel, ln, serveErr := startStandaloneMongoClientForTest(t, standalone)
				defer stopStandaloneMongoClientForTest(t, client, cancel, ln, serveErr, standalone)
				db := client.Database("app")
				if existing {
					if _, err := db.Collection("items").InsertOne(context.Background(), bson.D{{Key: "_id", Value: "one"}, {Key: "value", Value: "kept"}}); err != nil {
						t.Fatal(err)
					}
				}
				var before collections.CollectionMeta
				if existing {
					col, err := standalone.Collections.OpenCollection("app.items")
					if err != nil {
						t.Fatal(err)
					}
					before = col.Meta()
				}
				scalar := bson.D{{Key: "key", Value: bson.D{{Key: "value", Value: int32(1)}}}, {Key: "name", Value: "value_1"}, {Key: "treedbValueType", Value: "string"}}
				vector := bson.D{{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}}, {Key: "name", Value: "embedding_vector"}, {Key: "treedbIndexType", Value: "vector"}, {Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(2)}}}}
				cmd := bson.D{{Key: "createIndexes", Value: "items"}, {Key: "indexes", Value: bson.A{scalar, vector}}, {Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: journal}}}}
				err = db.RunCommand(context.Background(), cmd).Err()
				var commandErr mongo.CommandError
				if !errors.As(err, &commandErr) || commandErr.Code != 2 {
					t.Fatalf("expected BadValue code 2, got %v", err)
				}
				col, err := standalone.Collections.OpenCollection("app.items")
				if !existing {
					if !errors.Is(err, collections.ErrCollectionNotFound) {
						t.Fatalf("request created collection: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if after := col.Meta(); !reflect.DeepEqual(before, after) {
					t.Fatalf("request changed schema: before=%+v after=%+v", before, after)
				}
				var doc bson.Raw
				if err := db.Collection("items").FindOne(context.Background(), bson.D{{Key: "_id", Value: "one"}}).Decode(&doc); err != nil || doc.Lookup("value").StringValue() != "kept" {
					t.Fatalf("existing document changed: %s, %v", doc, err)
				}
			})
		}
	}
}

func TestMongoUnsupportedVectorCreateIndexesHasNoEffects(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, vectorFirst := range []bool{false, true} {
			t.Run(map[bool]string{false: "absent", true: "existing"}[existing]+"/"+map[bool]string{false: "scalar-first", true: "vector-first"}[vectorFirst], func(t *testing.T) {
				db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				s := NewServer()
				s.Collections = collections.NewCollectionManager(db)
				s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
				if existing {
					assertOK(t, serveCommand(t, s, 1, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "one"}, {Key: "value", Value: "kept"}}}}, {Key: "$db", Value: "app"}}))
				}
				var before collections.CollectionMeta
				if existing {
					col, err := s.Collections.OpenCollection("app.items")
					if err != nil {
						t.Fatal(err)
					}
					before = col.Meta()
				}
				scalar := bson.D{{Key: "key", Value: bson.D{{Key: "value", Value: int32(1)}}}, {Key: "name", Value: "value_1"}, {Key: "treedbValueType", Value: "string"}}
				vector := bson.D{{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}}, {Key: "name", Value: "embedding_vector"}, {Key: "treedbIndexType", Value: "vector"}, {Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(2)}}}}
				indexes := bson.A{scalar, vector}
				if vectorFirst {
					indexes = bson.A{vector, scalar}
				}
				assertCommandError(t, serveCommand(t, s, 2, bson.D{{Key: "createIndexes", Value: "items"}, {Key: "indexes", Value: indexes}, {Key: "$db", Value: "app"}}), "BadValue")
				col, err := s.Collections.OpenCollection("app.items")
				if !existing {
					if !errors.Is(err, collections.ErrCollectionNotFound) {
						t.Fatalf("rejected request created collection: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if after := col.Meta(); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected request changed schema: before=%+v after=%+v", before, after)
				}
				found := serveCommand(t, s, 3, bson.D{{Key: "find", Value: "items"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "one"}}}, {Key: "$db", Value: "app"}})
				batch := cursorFirstBatch(t, found)
				if len(batch) != 1 || batch[0].Lookup("value").StringValue() != "kept" {
					t.Fatalf("existing document changed: %v", batch)
				}
			})
		}
	}
}

func TestMongoVectorOptionsRejectUnknownAndDuplicate(t *testing.T) {
	for _, extra := range []bson.E{{Key: "strategy", Value: "native_runtime"}, {Key: "unknown", Value: true}, {Key: "dimensions", Value: int32(3)}} {
		t.Run(extra.Key, func(t *testing.T) {
			index := bson.D{{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}}, {Key: "treedbIndexType", Value: "vector"}, {Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(2)}, extra}}}
			_, err := parseCreateIndexDefinition(mustDocument(t, index))
			if err == nil {
				t.Fatalf("option %q must be rejected", extra.Key)
			}
		})
	}
}

func TestMongoUnsupportedMissingJSONVectorDoesNotInvalidateCache(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	s := NewServer()
	s.Collections = collections.NewCollectionManager(db)
	s.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}
	sentinel := &collections.Collection{}
	s.collectionCache = map[string]*collections.Collection{"app.items": sentinel}
	vector := bson.D{{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}}, {Key: "name", Value: "embedding_vector"}, {Key: "treedbIndexType", Value: "vector"}, {Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(2)}}}}
	assertCommandError(t, serveCommand(t, s, 1, bson.D{{Key: "createIndexes", Value: "items"}, {Key: "indexes", Value: bson.A{vector}}, {Key: "$db", Value: "app"}}), "BadValue")
	if s.collectionCache["app.items"] != sentinel {
		t.Fatal("unsupported vector declaration invalidated collection cache")
	}
	if _, err := s.Collections.OpenCollection("app.items"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("unsupported vector declaration created collection: %v", err)
	}
}

func TestMongoPreconfiguredJSONColumnGraphAutoCreateAndIdenticalDDL(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer()
	s.Collections = collections.NewCollectionManager(backend)
	defer func() {
		if err := s.Collections.Close(); err != nil {
			t.Error(err)
		}
	}()
	s.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
		ColumnStore: &collections.ColumnStoreConfig{Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
			Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}},
	}
	vector := bson.D{{Key: "key", Value: bson.D{{Key: "embedding", Value: "vector"}}}, {Key: "name", Value: "embedding_vector"}, {Key: "treedbIndexType", Value: "vector"}, {Key: "treedbVector", Value: bson.D{{Key: "dimensions", Value: int32(2)}}}}
	cmd := bson.D{{Key: "createIndexes", Value: "items"}, {Key: "indexes", Value: bson.A{vector}}, {Key: "$db", Value: "app"}}
	assertOK(t, serveCommand(t, s, 1, cmd))
	col, err := s.Collections.OpenCollection("app.items")
	if err != nil {
		t.Fatal(err)
	}
	before := col.Meta()
	if len(before.VectorIndexes) != 1 || before.VectorIndexes[0].Strategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("unexpected vector metadata: %+v", before.VectorIndexes)
	}
	assertOK(t, serveCommand(t, s, 2, cmd))
	if after := col.Meta(); !reflect.DeepEqual(before, after) {
		t.Fatalf("identical DDL changed metadata: before=%+v after=%+v", before, after)
	}
}
