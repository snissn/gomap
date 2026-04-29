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
