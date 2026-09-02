package mongogateway

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkServerIndexMetadataScalarCycle(b *testing.B) {
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(b, serveCommand(b, server, 1, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	}))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("score_%d", i)
		name := field + "_1"
		assertOK(b, serveCommand(b, server, int32(10+i*3), bson.D{
			{Key: "createIndexes", Value: "users"},
			{Key: "indexes", Value: bson.A{bson.D{
				{Key: "key", Value: bson.D{{Key: field, Value: int32(1)}}},
				{Key: "name", Value: name},
				{Key: "treedbValueType", Value: "double"},
			}}},
			{Key: "$db", Value: "app"},
		}))
		assertOK(b, serveCommand(b, server, int32(11+i*3), bson.D{
			{Key: "listIndexes", Value: "users"},
			{Key: "$db", Value: "app"},
		}))
		assertOK(b, serveCommand(b, server, int32(12+i*3), bson.D{
			{Key: "dropIndexes", Value: "users"},
			{Key: "index", Value: name},
			{Key: "$db", Value: "app"},
		}))
	}
}

func BenchmarkServerIndexMetadataVectorCycle(b *testing.B) {
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	assertOK(b, serveCommand(b, server, 1, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	}))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("embedding_%d", i)
		name := field + "_vector"
		assertOK(b, serveCommand(b, server, int32(10+i*3), bson.D{
			{Key: "createIndexes", Value: "users"},
			{Key: "indexes", Value: bson.A{bson.D{
				{Key: "key", Value: bson.D{{Key: field, Value: "vector"}}},
				{Key: "name", Value: name},
				{Key: "treedbIndexType", Value: "vector"},
				{Key: "treedbVector", Value: bson.D{
					{Key: "dimensions", Value: int32(64)},
					{Key: "metric", Value: "cosine"},
					{Key: "m", Value: int32(16)},
					{Key: "efConstruction", Value: int32(128)},
					{Key: "efSearch", Value: int32(64)},
					{Key: "encoding", Value: "float32"},
				}},
			}}},
			{Key: "$db", Value: "app"},
		}))
		assertOK(b, serveCommand(b, server, int32(11+i*3), bson.D{
			{Key: "listIndexes", Value: "users"},
			{Key: "$db", Value: "app"},
		}))
		assertOK(b, serveCommand(b, server, int32(12+i*3), bson.D{
			{Key: "dropIndexes", Value: "users"},
			{Key: "index", Value: name},
			{Key: "$db", Value: "app"},
		}))
	}
}
