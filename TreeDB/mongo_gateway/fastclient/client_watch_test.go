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
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestClientPersistentContextWatchReuseAndCancel(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	server := mongogateway.NewServer()
	defer func() { _ = server.Close() }()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}

	srvCtx, srvCancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = server.ServeConn(srvCtx, conn) }()
		}
	}()
	defer func() {
		srvCancel()
		_ = ln.Close()
		<-acceptDone
	}()

	client, err := Connect(srvCtx, ln.Addr().String())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	phaseCtx, phaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer phaseCancel()

	docs := []bson.Raw{mustBSON(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "v", Value: "ok"}})}
	if _, err := client.InsertManyRawBSON(phaseCtx, "app", "items", docs); err != nil {
		t.Fatalf("InsertManyRawBSON: %v", err)
	}

	cmd := mustBSON(t, bson.D{
		{Key: "find", Value: "items"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "k1"}}},
		{Key: "limit", Value: int32(1)},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: "app"},
	})
	for i := 0; i < 20; i++ {
		if err := client.FindRawBSONBorrowed(phaseCtx, cmd, func(batch []bson.Raw) error {
			if len(batch) != 1 {
				t.Fatalf("iter %d: len=%d want 1", i, len(batch))
			}
			return nil
		}); err != nil {
			t.Fatalf("iter %d: FindRawBSONBorrowed: %v", i, err)
		}
	}

	// Verify cancelling a fresh request context unblocks an in-flight read cleanly.
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	stalledClient := New(clientConn)
	defer func() { _ = stalledClient.Close() }()

	// Drain client's write so the client blocks on Read waiting for a reply.
	go func() {
		buf := make([]byte, 4096)
		_, _ = serverConn.Read(buf)
	}()

	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()
	err = stalledClient.FindRawBSONBorrowed(cancelCtx, cmd, func([]bson.Raw) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled FindRawBSONBorrowed err=%v want context.Canceled", err)
	}
}
