package fastclient

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestClientInsertManyRawBSON(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := mongogateway.NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}

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
			go func() { _ = server.ServeConn(ctx, conn) }()
		}
	}()

	client, err := Connect(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("connect fast client: %v", err)
	}
	defer func() { _ = client.Close() }()

	badRaw := append(append(bson.Raw(nil), mustBSON(t, bson.D{{Key: "_id", Value: "bad-trailing-bytes"}})...), 0)
	if _, err := client.InsertManyRawBSON(ctx, "app", "users", []bson.Raw{badRaw}); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("InsertManyRawBSON malformed err=%v want ErrMalformed", err)
	}

	rawDocs := []bson.Raw{
		mustBSON(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}}),
		mustBSON(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "name", Value: "grace"}}),
	}
	insertCtx, insertCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer insertCancel()
	n, err := client.InsertManyRawBSON(insertCtx, "app", "users", rawDocs)
	if err != nil {
		t.Fatalf("InsertManyRawBSON: %v", err)
	}
	if n != len(rawDocs) {
		t.Fatalf("inserted=%d want %d", n, len(rawDocs))
	}

	driverClient, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = driverClient.Disconnect(context.Background()) }()
	var got bson.M
	if err := driverClient.Database("app").Collection("users").FindOne(insertCtx, bson.D{{Key: "_id", Value: "u2"}}).Decode(&got); err != nil {
		t.Fatalf("driver find inserted document: %v", err)
	}
	if got["name"] != "grace" {
		t.Fatalf("decoded name=%v want grace", got["name"])
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

func mustBSON(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal BSON: %v", err)
	}
	return raw
}
