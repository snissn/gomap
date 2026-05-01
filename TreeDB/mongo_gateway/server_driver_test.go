package mongogateway

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestServerOfficialGoDriverBasicCRUD(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	if err := client.Ping(opCtx, nil); err != nil {
		t.Fatalf("driver ping: %v", err)
	}

	coll := client.Database("app").Collection("users")
	id := bson.NewObjectID()
	if _, err := coll.InsertOne(opCtx, bson.D{
		{Key: "_id", Value: id},
		{Key: "name", Value: "ada"},
		{Key: "age", Value: int64(37)},
	}); err != nil {
		t.Fatalf("driver insert one: %v", err)
	}

	var got bson.M
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); err != nil {
		t.Fatalf("driver find one: %v", err)
	}
	if got["_id"] != id {
		t.Fatalf("decoded _id=%v want %v", got["_id"], id)
	}
	if got["name"] != "ada" {
		t.Fatalf("decoded name=%v want ada", got["name"])
	}
	if got["age"] != int64(37) {
		t.Fatalf("decoded age=%v want 37", got["age"])
	}

	updateResult, err := coll.UpdateOne(opCtx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "age", Value: int64(38)},
			{Key: "city", Value: "London"},
		}}},
	)
	if err != nil {
		t.Fatalf("driver update one: %v", err)
	}
	if updateResult.MatchedCount != 1 || updateResult.ModifiedCount != 1 {
		t.Fatalf("update result matched=%d modified=%d want 1/1", updateResult.MatchedCount, updateResult.ModifiedCount)
	}

	got = nil
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); err != nil {
		t.Fatalf("driver find one after update: %v", err)
	}
	if got["age"] != int64(38) {
		t.Fatalf("decoded updated age=%v want 38", got["age"])
	}
	if got["city"] != "London" {
		t.Fatalf("decoded city=%v want London", got["city"])
	}

	deleteResult, err := coll.DeleteOne(opCtx, bson.D{{Key: "_id", Value: id}})
	if err != nil {
		t.Fatalf("driver delete one: %v", err)
	}
	if deleteResult.DeletedCount != 1 {
		t.Fatalf("delete result deleted=%d want 1", deleteResult.DeletedCount)
	}
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: id}}).Decode(&got); !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("driver find one after delete err=%v want ErrNoDocuments", err)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}

func TestServerOfficialGoDriverUnacknowledgedInsertMany(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	if err := client.Ping(opCtx, nil); err != nil {
		t.Fatalf("driver ping: %v", err)
	}

	ackColl := client.Database("app").Collection("users")
	unackColl := client.Database("app").Collection("users", options.Collection().SetWriteConcern(writeconcern.Unacknowledged()))
	result, err := unackColl.InsertMany(opCtx, []any{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}},
	})
	if err != nil {
		t.Fatalf("unacknowledged insert many: %v", err)
	}
	if result.Acknowledged {
		t.Fatal("unacknowledged insert reported Acknowledged=true")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		var got bson.M
		err = ackColl.FindOne(opCtx, bson.D{{Key: "_id", Value: "u2"}}).Decode(&got)
		if err == nil {
			if got["name"] != "grace" {
				t.Fatalf("decoded name=%v want grace", got["name"])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("find after unacknowledged insert: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerOfficialGoDriverIndexMetadata(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	mongoDB := client.Database("app")
	coll := mongoDB.Collection("users")
	if err := mongoDB.RunCommand(opCtx, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
			{Key: "name", Value: "email_1"},
			{Key: "treedbValueType", Value: "string"},
			{Key: "unique", Value: true},
		}}},
	}).Err(); err != nil {
		t.Fatalf("driver create index: %v", err)
	}

	names, err := mongoDB.ListCollectionNames(opCtx, bson.D{})
	if err != nil {
		t.Fatalf("driver list collection names: %v", err)
	}
	if len(names) != 1 || names[0] != "users" {
		t.Fatalf("collection names=%q want [users]", names)
	}

	specs, err := coll.Indexes().ListSpecifications(opCtx)
	if err != nil {
		t.Fatalf("driver list index specs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("index spec len=%d want 2: %+v", len(specs), specs)
	}
	if specs[0].Name != "_id_" || specs[1].Name != "email_1" {
		t.Fatalf("index spec names=%q,%q want _id_,email_1", specs[0].Name, specs[1].Name)
	}
	if specs[1].Unique == nil || !*specs[1].Unique {
		t.Fatalf("email_1 unique=%v want true", specs[1].Unique)
	}

	if err := coll.Indexes().DropOne(opCtx, "email_1"); err != nil {
		t.Fatalf("driver drop index: %v", err)
	}
	specs, err = coll.Indexes().ListSpecifications(opCtx)
	if err != nil {
		t.Fatalf("driver list after drop: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "_id_" {
		t.Fatalf("index specs after drop=%+v want only _id_", specs)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}

func TestServerOfficialGoDriverFindPlanner(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	serveErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					serveErr <- nil
					return
				}
				serveErr <- err
				return
			}
			go func() {
				_ = server.ServeConn(ctx, conn)
			}()
		}
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	opCtx, opCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer opCancel()
	mongoDB := client.Database("app")
	coll := mongoDB.Collection("users")
	if err := mongoDB.RunCommand(opCtx, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}},
			{Key: "name", Value: "city_1"},
			{Key: "treedbValueType", Value: "string"},
		}}},
	}).Err(); err != nil {
		t.Fatalf("driver create city index: %v", err)
	}
	docs := []any{
		bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(37)}},
		bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(42)}},
		bson.D{{Key: "_id", Value: "u3"}, {Key: "name", Value: "katherine"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(36)}},
	}
	if _, err := coll.InsertMany(opCtx, docs); err != nil {
		t.Fatalf("driver insert many: %v", err)
	}

	cursor, err := coll.Find(opCtx,
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "city", Value: "hnl"}},
			bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(40)}}}},
		}}},
		options.Find().
			SetProjection(bson.D{{Key: "name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}))
	if err != nil {
		t.Fatalf("driver indexed find: %v", err)
	}
	var results []bson.M
	if err := cursor.All(opCtx, &results); err != nil {
		t.Fatalf("driver indexed find all: %v", err)
	}
	if len(results) != 1 || results[0]["name"] != "grace" {
		t.Fatalf("indexed find results=%v want grace", results)
	}
	if _, ok := results[0]["_id"]; ok {
		t.Fatalf("indexed find projection included _id: %v", results[0])
	}

	cursor, err = coll.Find(opCtx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{"u3", "u1"}}}}},
		options.Find().SetSort(bson.D{{Key: "name", Value: int32(1)}}).SetSkip(1).SetLimit(1))
	if err != nil {
		t.Fatalf("driver _id in find: %v", err)
	}
	results = nil
	if err := cursor.All(opCtx, &results); err != nil {
		t.Fatalf("driver _id in find all: %v", err)
	}
	if len(results) != 1 || results[0]["name"] != "katherine" {
		t.Fatalf("_id in find results=%v want katherine", results)
	}

	cursor, err = coll.Find(opCtx,
		bson.D{},
		options.Find().SetSort(bson.D{{Key: "name", Value: int32(1)}}).SetBatchSize(1))
	if err != nil {
		t.Fatalf("driver batched find: %v", err)
	}
	results = nil
	if err := cursor.All(opCtx, &results); err != nil {
		t.Fatalf("driver batched find all: %v", err)
	}
	if len(results) != 3 || results[0]["name"] != "ada" || results[2]["name"] != "katherine" {
		t.Fatalf("batched find results=%v want ada..katherine", results)
	}

	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve loop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve loop did not stop")
	}
}
