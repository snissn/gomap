package fastclient

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
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

type deadlineRecordingConn struct {
	net.Conn
	mu        sync.Mutex
	deadlines []time.Time
}

func (c *deadlineRecordingConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

func (c *deadlineRecordingConn) nonZeroDeadlines() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, d := range c.deadlines {
		if !d.IsZero() {
			n++
		}
	}
	return n
}

func (c *deadlineRecordingConn) lastDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.deadlines) == 0 {
		return time.Time{}, false
	}
	return c.deadlines[len(c.deadlines)-1], true
}

func TestClientRetiredWatchCallbackCannotPoisonNextWatch(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	recConn := &deadlineRecordingConn{Conn: clientConn}
	c := New(recConn)
	defer func() { _ = c.Close() }()

	// Repeatedly reproduce the exact race window:
	// 1. Complete a request under ctx1 (watch remains registered, watchInFlight=false).
	// 2. Cancel ctx1 so its AfterFunc callback is queued/running.
	// 3. Immediately begin a fresh watch under an uncanceled ctx2.
	// 4. Yield/wait for ctx1's callback to execute and verify watchFired stays
	//    false and the connection deadline is never expired.
	for i := 0; i < 64; i++ {
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())

		c.mu.Lock()
		if err := c.beginContextWatchLocked(ctx1); err != nil {
			c.mu.Unlock()
			cancel1()
			cancel2()
			t.Fatalf("iter %d begin ctx1: %v", i, err)
		}
		c.endContextWatchLocked()

		cancel1()
		if err := c.beginContextWatchLocked(ctx2); err != nil {
			c.mu.Unlock()
			cancel2()
			t.Fatalf("iter %d begin ctx2: %v", i, err)
		}
		// Yield while ctx2 is in flight so any queued ctx1 callback runs while
		// watchInFlight is true.
		for y := 0; y < 8; y++ {
			runtime.Gosched()
		}
		time.Sleep(500 * time.Microsecond)

		if c.watchFired.Load() {
			c.endContextWatchLocked()
			c.mu.Unlock()
			cancel2()
			t.Fatalf("iter %d: stale ctx1 callback poisoned watchFired for uncanceled ctx2", i)
		}
		if got := recConn.nonZeroDeadlines(); got != 0 {
			c.endContextWatchLocked()
			c.mu.Unlock()
			cancel2()
			t.Fatalf("iter %d: stale ctx1 callback expired conn deadline (%d non-zero SetDeadline calls)", i, got)
		}

		c.endContextWatchLocked()
		c.mu.Unlock()
		cancel2()
	}

	// Verify that cancelling an active watch expires and then clears the deadline
	// cleanly when the request ends, leaving the connection usable for ctx3.
	ctxActive, cancelActive := context.WithCancel(context.Background())
	c.mu.Lock()
	if err := c.beginContextWatchLocked(ctxActive); err != nil {
		c.mu.Unlock()
		cancelActive()
		t.Fatalf("begin ctxActive: %v", err)
	}
	cancelActive()
	deadline := time.Now().Add(2 * time.Second)
	for !c.watchFired.Load() {
		if time.Now().After(deadline) {
			c.endContextWatchLocked()
			c.mu.Unlock()
			t.Fatal("timed out waiting for active context watch to fire")
		}
		runtime.Gosched()
	}
	c.endContextWatchLocked()
	c.mu.Unlock()

	if last, ok := recConn.lastDeadline(); !ok || !last.IsZero() {
		t.Fatalf("after endContextWatchLocked, lastDeadline=(%v, %v), want zero deadline", last, ok)
	}
}

func TestClientFindRawBSONBorrowedClearsRetainedDocsBuf(t *testing.T) {
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
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_ = server.ServeConn(srvCtx, c)
			}(conn)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	docs := []bson.Raw{
		mustBSON(t, bson.D{{Key: "_id", Value: "a"}, {Key: "v", Value: 1}}),
		mustBSON(t, bson.D{{Key: "_id", Value: "b"}, {Key: "v", Value: 2}}),
		mustBSON(t, bson.D{{Key: "_id", Value: "c"}, {Key: "v", Value: 3}}),
		mustBSON(t, bson.D{{Key: "_id", Value: "d"}, {Key: "v", Value: 4}}),
	}
	if _, err := client.InsertManyRawBSON(ctx, "bench", "items", docs); err != nil {
		t.Fatalf("InsertManyRawBSON: %v", err)
	}

	// Fetch 4 docs, then 1 doc, and verify every entry in the retained docsBuf
	// backing array is cleared to nil after each callback returns.
	findAll := mustBSON(t, bson.D{
		{Key: "find", Value: "items"},
		{Key: "filter", Value: bson.D{}},
		{Key: "batchSize", Value: 10},
		{Key: "$db", Value: "bench"},
	})
	gotCount := 0
	if err := client.FindRawBSONBorrowed(ctx, findAll, func(docs []bson.Raw) error {
		gotCount = len(docs)
		return nil
	}); err != nil {
		t.Fatalf("findAll: %v", err)
	}
	if gotCount != 4 {
		t.Fatalf("findAll count=%d want 4", gotCount)
	}
	assertCleared := func(stage string) {
		t.Helper()
		client.mu.Lock()
		full := client.docsBuf[:cap(client.docsBuf)]
		client.mu.Unlock()
		for i, doc := range full {
			if doc != nil {
				t.Fatalf("%s: retained docsBuf[%d] is non-nil (len=%d)", stage, i, len(doc))
			}
		}
	}
	assertCleared("after_4_docs")

	findOne := mustBSON(t, bson.D{
		{Key: "find", Value: "items"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "a"}}},
		{Key: "limit", Value: 1},
		{Key: "$db", Value: "bench"},
	})
	if err := client.FindRawBSONBorrowed(ctx, findOne, func(docs []bson.Raw) error {
		gotCount = len(docs)
		return nil
	}); err != nil {
		t.Fatalf("findOne: %v", err)
	}
	if gotCount != 1 {
		t.Fatalf("findOne count=%d want 1", gotCount)
	}
	assertCleared("after_1_doc")
}
