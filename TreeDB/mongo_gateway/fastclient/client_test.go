package fastclient

import (
	"context"
	"errors"
	"net"
	"strings"
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
	findCommand := mustBSON(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "name", Value: "grace"}}},
		{Key: "limit", Value: 1},
		{Key: "$db", Value: "app"},
	})
	found, err := client.FindRawBSON(insertCtx, findCommand)
	if err != nil {
		t.Fatalf("FindRawBSON: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("FindRawBSON docs=%d want 1", len(found))
	}
	if got, ok := found[0].Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("FindRawBSON name=%q ok=%t want grace", got, ok)
	}
	adaCommand := mustBSON(t, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "name", Value: "ada"}}},
		{Key: "limit", Value: 1},
		{Key: "$db", Value: "app"},
	})
	adaFound, err := client.FindRawBSON(insertCtx, adaCommand)
	if err != nil {
		t.Fatalf("FindRawBSON ada: %v", err)
	}
	if len(adaFound) != 1 {
		t.Fatalf("FindRawBSON ada docs=%d want 1", len(adaFound))
	}
	if got, ok := adaFound[0].Lookup("name").StringValueOK(); !ok || got != "ada" {
		t.Fatalf("FindRawBSON ada name=%q ok=%t want ada", got, ok)
	}
	if got, ok := found[0].Lookup("name").StringValueOK(); !ok || got != "grace" {
		t.Fatalf("first FindRawBSON result changed after second query: name=%q ok=%t want grace", got, ok)
	}
	var borrowedCount int
	if err := client.FindRawBSONBorrowed(insertCtx, findCommand, func(docs []bson.Raw) error {
		borrowedCount = len(docs)
		return nil
	}); err != nil {
		t.Fatalf("FindRawBSONBorrowed: %v", err)
	}
	if borrowedCount != 1 {
		t.Fatalf("FindRawBSONBorrowed docs=%d want 1", borrowedCount)
	}
	if err := client.FindRawBSONBorrowed(insertCtx, mustBSON(t, bson.D{{Key: "ping", Value: 1}, {Key: "$db", Value: "admin"}}), func([]bson.Raw) error {
		t.Fatal("borrowed callback should not run for non-find command")
		return nil
	}); err == nil || !strings.Contains(err.Error(), "requires a find command document") {
		t.Fatalf("FindRawBSONBorrowed non-find err=%v want find-command error", err)
	}
	if err := client.FindRawBSONBorrowed(insertCtx, findCommand, func(docs []bson.Raw) error {
		if client.mu.TryLock() {
			client.mu.Unlock()
			t.Fatal("FindRawBSONBorrowed callback ran without holding the client lock")
		}
		if len(docs) != 1 {
			t.Fatalf("borrowed concurrent docs=%d want 1", len(docs))
		}
		if got, ok := docs[0].Lookup("name").StringValueOK(); !ok || got != "grace" {
			t.Fatalf("borrowed doc changed while callback active: name=%q ok=%t want grace", got, ok)
		}
		return nil
	}); err != nil {
		t.Fatalf("FindRawBSONBorrowed concurrent: %v", err)
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

func TestClientInsertManyRawBSONCancellationWithoutDeadlineInterruptsRead(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	client := New(clientConn)
	defer func() { _ = client.Close() }()

	rawDocs := []bson.Raw{mustBSON(t, bson.D{{Key: "_id", Value: "u1"}})}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := client.InsertManyRawBSON(ctx, "app", "users", rawDocs)
		errCh <- err
	}()

	readDone := make(chan error, 1)
	go func() {
		_, _, err := wire.ReadMessage(serverConn, 0)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("server read request: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive request")
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("InsertManyRawBSON err=%v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("InsertManyRawBSON did not return after cancellation")
	}
}

func TestClientInsertManyRawBSONCanceledDeadlineContextInterruptsRead(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	client := New(clientConn)
	defer func() { _ = client.Close() }()

	rawDocs := []bson.Raw{mustBSON(t, bson.D{{Key: "_id", Value: "u1"}})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.InsertManyRawBSON(ctx, "app", "users", rawDocs)
		errCh <- err
	}()

	readDone := make(chan error, 1)
	go func() {
		_, _, err := wire.ReadMessage(serverConn, 0)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("server read request: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive request")
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("InsertManyRawBSON err=%v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("InsertManyRawBSON did not return after cancellation")
	}
}

func TestClientInsertManyRawBSONDeadlineInterruptsRead(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	client := New(clientConn)
	defer func() { _ = client.Close() }()

	rawDocs := []bson.Raw{mustBSON(t, bson.D{{Key: "_id", Value: "u1"}})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.InsertManyRawBSON(ctx, "app", "users", rawDocs)
		errCh <- err
	}()

	readDone := make(chan error, 1)
	go func() {
		_, _, err := wire.ReadMessage(serverConn, 0)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("server read request: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive request")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("InsertManyRawBSON err=%v want context.DeadlineExceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("InsertManyRawBSON did not return after deadline")
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
