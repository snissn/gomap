package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// BenchmarkStandalonePublicIndexedUpdate deliberately uses the product opener,
// its profile defaults, and the official driver. It measures request-to-ACK work
// in one process (client and server allocations), without per-operation tracing.
// Each writer owns disjoint documents; every update changes an indexed value.
func BenchmarkStandalonePublicIndexedUpdate(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		for _, writers := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/writers=%d", profile, writers), func(b *testing.B) {
				benchmarkStandalonePublicIndexedUpdate(b, profile, writers)
			})
		}
	}
}

func benchmarkStandalonePublicIndexedUpdate(b *testing.B, profile treedb.Profile, writers int) {
	b.StopTimer()
	server, collection, ctx, _, _ := openStandalonePublicFixture(b, profile, writers, 256)
	const rowsPerWriter = 64
	before := server.Backend.Stats()
	var wg sync.WaitGroup
	errors := make(chan error, writers)
	start := make(chan struct{})
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-start
			for operation := writer; operation < b.N; operation += writers {
				local := operation / writers
				id := int64(writer*rowsPerWriter + local%rowsPerWriter)
				result, err := collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
					bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(local/rowsPerWriter + 1)}}}})
				if err != nil {
					errors <- err
					return
				}
				if result.MatchedCount != 1 || result.ModifiedCount != 1 {
					errors <- fmt.Errorf("unexpected update result: %+v", result)
					return
				}
			}
		}(writer)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	close(errors)
	for err := range errors {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ACK/s")
	after := server.Backend.Stats()
	for _, key := range []string{"treedb.command_wal.file_sync.calls_total", "treedb.command_wal.sync.count_total"} {
		first, firstErr := strconv.ParseUint(before[key], 10, 64)
		last, lastErr := strconv.ParseUint(after[key], 10, 64)
		if firstErr != nil || lastErr != nil || last < first {
			b.Fatalf("invalid counter %s: before=%q after=%q", key, before[key], after[key])
		}
		b.Logf("timed %s=%d", key, last-first)
	}
}

// openStandalonePublicFixture is shared only by test/benchmark harnesses. The
// production opener and official-driver request path remain identical.
func openStandalonePublicFixture(tb testing.TB, profile treedb.Profile, writers, payloadBytes int) (*mongogateway.StandaloneServer, *mongo.Collection, context.Context, string, func()) {
	tb.Helper()
	dir := tb.TempDir()
	server, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: dir, Profile: profile})
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		_ = server.Close()
		tb.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + listener.Addr().String()).SetDirect(true).
		SetRetryWrites(false).SetMaxPoolSize(uint64(writers)).SetServerSelectionTimeout(5 * time.Second).
		SetWriteConcern(&writeconcern.WriteConcern{W: 1}))
	if err != nil {
		cancel()
		_ = server.Close()
		tb.Fatal(err)
	}
	var closeOnce sync.Once
	finish := func() {
		closeOnce.Do(func() {
			closeStart := time.Now()
			_ = client.Disconnect(context.Background())
			cancel()
			if err := server.Close(); err != nil {
				tb.Errorf("close: %v", err)
			}
			if err := <-served; err != nil {
				tb.Errorf("serve: %v", err)
			}
			tb.Logf("outside timed ACK interval: disconnect/close=%s", time.Since(closeStart))
		})
	}
	tb.Cleanup(finish)
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer setupCancel()
	if err := client.Ping(setupCtx, nil); err != nil {
		tb.Fatal(err)
	}
	collection := client.Database("publicbench").Collection("rows")
	// Create the indexed collection before seeding; command-WAL v2 permits
	// initial indexed schemas while existing-collection index DDL is separate.
	if _, err := collection.Indexes().CreateMany(setupCtx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "n", Value: 1}}}, {Keys: bson.D{{Key: "email", Value: 1}}},
	}); err != nil {
		tb.Fatal(err)
	}
	const rowsPerWriter = 64
	for writer := 0; writer < writers; writer++ {
		rows := make([]any, 0, rowsPerWriter)
		for row := 0; row < rowsPerWriter; row++ {
			id := int64(writer*rowsPerWriter + row)
			rows = append(rows, bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(0)},
				{Key: "email", Value: strconv.FormatInt(id, 10) + "@example.test"}, {Key: "payload", Value: strings.Repeat("x", payloadBytes)}})
		}
		if _, err := collection.InsertMany(setupCtx, rows); err != nil {
			tb.Fatal(err)
		}
	}
	tb.Logf("route: product OpenStandaloneServer profile=%s native=%t format=%s ACK=w:1,j:unspecified fixture=%d rows payload=%dB two secondary indexes", profile,
		server.Backend.HasNativeSemanticBridge(), server.Options.DefaultCollectionOptions.DocumentFormat, writers*rowsPerWriter, payloadBytes)
	return server, collection, ctx, dir, finish
}
