package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Deliberately uses the retained generic projection contract as the oracle.
func genericProjectionForTest(doc, projection wire.Document) (wire.Document, error) {
	if projection == nil {
		return doc, nil
	}
	mode, fields, includeID, idSpecified, err := parseProjection(projection)
	if err != nil {
		return nil, err
	}
	if _, err = projectionTree(fields); err != nil {
		return nil, err
	}
	return projectDocumentWithProjection(doc, compiledProjection{present: true, mode: mode, fields: fields, includeID: includeID, idSpecified: idSpecified})
}

func marshalProjectionFixture(t testing.TB, v any) wire.Document {
	t.Helper()
	out, err := bson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Document(out)
}

func TestMongoFlatProjectionDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20261030))
	projections := []bson.D{
		nil,
		{}, {{Key: "field0", Value: 1}}, {{Key: "field0", Value: 0}},
		{{Key: "_id", Value: 0}}, {{Key: "_id", Value: 1}},
		{{Key: "field0", Value: 1}, {Key: "missing", Value: 1}, {Key: "_id", Value: 0}},
		{{Key: "field0", Value: 0}, {Key: "_id", Value: 0}},
		{{Key: "field0", Value: true}, {Key: "field1", Value: int64(1)}},
		{{Key: "nested.a", Value: 1}}, {{Key: "nested.a", Value: 0}},
		{{Key: "array.a", Value: 1}}, {{Key: "array", Value: 1}},
		{{Key: "field0", Value: 1}, {Key: "field1", Value: 0}},
		{{Key: "_id.a", Value: 1}}, {{Key: "$bad", Value: 1}},
		{{Key: ".bad", Value: 1}}, {{Key: "bad..path", Value: 1}},
		{{Key: "bad.$path", Value: 1}}, {{Key: "", Value: 1}},
		{{Key: "field0", Value: "bad"}}, {{Key: "field0", Value: 1}, {Key: "field0", Value: 1}},
		{{Key: "_id", Value: 0}, {Key: "_id", Value: 1}},
		{{Key: strings.Repeat("a.", mongoMutationMaxPathDepth) + "a", Value: 1}}, {{Key: "field0", Value: 1}, {Key: "field0", Value: 0}},
	}
	checked := 0
	for sample := 0; sample < 80; sample++ {
		doc := bson.D{{Key: "_id", Value: fmt.Sprintf("id-%d", sample)}}
		for i := 0; i < 1+sample%32; i++ {
			var value any
			switch i % 6 {
			case 0:
				value = bson.Binary{Subtype: 0, Data: bytes.Repeat([]byte{byte(sample), byte(i)}, 50)}
			case 1:
				value = int64(rng.Int63())
			case 2:
				value = fmt.Sprintf("s-%d", rng.Int63())
			case 3:
				value = bson.A{int32(1), "x", bson.D{{Key: "b", Value: true}}}
			case 4:
				value = nil
			case 5:
				value = float64(sample) + 0.5
			}
			doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", i), Value: value})
		}
		doc = append(doc, bson.E{Key: "nested", Value: bson.D{{Key: "a", Value: sample}, {Key: "b", Value: 2}}}, bson.E{Key: "array", Value: bson.A{bson.D{{Key: "a", Value: 1}}}})
		raw := marshalProjectionFixture(t, doc)
		for pi, p := range projections {
			var projection wire.Document
			if p != nil {
				projection = marshalProjectionFixture(t, p)
			}
			want, werr := genericProjectionForTest(raw, projection)
			got, gerr := projectDocument(raw, projection)
			if (werr == nil) != (gerr == nil) || (werr != nil && werr.Error() != gerr.Error()) || !bytes.Equal(want, got) {
				t.Fatalf("sample=%d projection=%d want=%x err=%v got=%x err=%v", sample, pi, want, werr, got, gerr)
			}
			checked++
		}
	}
	// The optimized iterator must fall back for malformed or trailing bytes and
	// preserve the original generic decoder's outcome, including permissive cases.
	raw := marshalProjectionFixture(t, bson.D{{Key: "_id", Value: "a"}, {Key: "field0", Value: "b"}})
	projection := marshalProjectionFixture(t, bson.D{{Key: "field0", Value: 1}})
	malformed := [][]byte{nil, {1, 0, 0, 0}, append(bytes.Clone(raw), 1, 2, 3), bytes.Clone(raw[:len(raw)-1])}
	// Include every truncation and a corrupt type, length, key terminator and
	// document terminator. Some decoder outcomes are deliberately permissive.
	for end := 0; end < len(raw); end++ {
		malformed = append(malformed, bytes.Clone(raw[:end]))
	}
	for _, offset := range []int{0, 4, 8, len(raw) - 1} {
		corrupt := bytes.Clone(raw)
		corrupt[offset] = 0xff
		malformed = append(malformed, corrupt)
	}
	for _, d := range malformed {
		want, werr := genericProjectionForTest(d, projection)
		got, gerr := projectDocument(d, projection)
		if (werr == nil) != (gerr == nil) || (werr != nil && werr.Error() != gerr.Error()) || !bytes.Equal(want, got) {
			t.Fatalf("malformed parity want=%x %v got=%x %v", want, werr, got, gerr)
		}
		checked++
	}
	t.Logf("differential cases: %d", checked)
}

func TestMongoFlatProjectionOwnsOutput(t *testing.T) {
	doc := marshalProjectionFixture(t, bson.D{{Key: "_id", Value: "a"}, {Key: "field0", Value: "value"}, {Key: "unused", Value: 1}})
	saved := bytes.Clone(doc)
	out, err := projectDocument(doc, marshalProjectionFixture(t, bson.D{{Key: "field0", Value: 1}}))
	if err != nil {
		t.Fatal(err)
	}
	for i := range out {
		out[i] = 0
	}
	if !bytes.Equal(doc, saved) {
		t.Fatal("projected output aliases input")
	}
}

func BenchmarkProjectionCompileAndApply(b *testing.B) {
	doc := bson.D{{Key: "_id", Value: "user123"}}
	all := bson.D{}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("field%d", i)
		doc = append(doc, bson.E{Key: key, Value: bson.Binary{Subtype: 0, Data: bytes.Repeat([]byte{byte(i)}, 100)}})
		all = append(all, bson.E{Key: key, Value: 1})
	}
	raw := marshalProjectionFixture(b, doc)
	cases := []struct {
		name       string
		projection bson.D
	}{
		{"all-ten-fields", all},
		{"one-field", bson.D{{Key: "field0", Value: 1}, {Key: "_id", Value: 0}}},
		{"exclude-one", bson.D{{Key: "field0", Value: 0}}},
		{"nested-fallback", bson.D{{Key: "missing.part", Value: 1}}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			p := marshalProjectionFixture(b, tc.projection)
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := projectDocument(raw, p)
				if err != nil || len(out) < 5 {
					b.Fatal(err)
				}
			}
		})
	}
}

// Projection output escapes the request and storage owners. It must survive
// reuse of both the stored BSON and the command used by a continuing cursor.
func TestMongoFlatProjectionBorrowedInputAndCursorPlan(t *testing.T) {
	command := marshalProjectionFixture(t, bson.D{{Key: "field0", Value: 1}, {Key: "_id", Value: 0}})
	compiled, err := compileProjection(command)
	if err != nil {
		t.Fatal(err)
	}
	plan := cloneFindPlanForCursor(findPlan{projection: compiled})
	charged := findPlanCursorRetainedBytes(plan)
	doc := marshalProjectionFixture(t, bson.D{{Key: "_id", Value: "a"}, {Key: "field0", Value: "value"}, {Key: "unused", Value: 1}})
	want := marshalProjectionFixture(t, bson.D{{Key: "field0", Value: "value"}})
	out, err := projectDocumentWithProjection(doc, plan.projection)
	if err != nil {
		t.Fatal(err)
	}
	clear(command)
	clear(doc)
	if !bytes.Equal(out, want) {
		t.Fatal("projected output changed after input reuse")
	}
	fresh := marshalProjectionFixture(t, bson.D{{Key: "field0", Value: "value"}})
	got, err := projectDocumentWithProjection(fresh, plan.projection)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("cursor projection after command reuse=%x err=%v", got, err)
	}
	if got := findPlanCursorRetainedBytes(plan); got != charged {
		t.Fatalf("retained charge changed: %d -> %d", charged, got)
	}
}

func TestStandaloneFlatProjectionDriverPointAndCursor(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			client, cancel, ln, served := startStandaloneMongoClientForTest(t, s)
			defer stopStandaloneMongoClientForTest(t, client, cancel, ln, served, s)
			ctx, ctxCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer ctxCancel()
			c := client.Database("projection").Collection("rows")
			all := bson.D{{Key: "_id", Value: 0}}
			rows := make([]any, 3)
			expected := make([]wire.Document, len(rows))
			for i := range rows {
				row := bson.D{{Key: "_id", Value: int32(i)}}
				projected := bson.D{}
				for f := 0; f < 10; f++ {
					key := fmt.Sprintf("field%d", f)
					field := bson.E{Key: key, Value: bson.Binary{Subtype: 0, Data: bytes.Repeat([]byte{byte(i), byte(f)}, 50)}}
					row = append(row, field)
					projected = append(projected, field)
					if i == 0 {
						all = append(all, bson.E{Key: key, Value: 1})
					}
				}
				rows[i] = row
				expected[i] = marshalProjectionFixture(t, projected)
			}
			if _, err := c.InsertMany(ctx, rows); err != nil {
				t.Fatal(err)
			}
			for i, row := range rows {
				filter := bson.D{{Key: "_id", Value: int32(i)}}
				full, err := c.FindOne(ctx, filter).Raw()
				if err != nil || !bytes.Equal(full, marshalProjectionFixture(t, row)) {
					t.Fatalf("unprojected row %d=%x err=%v", i, full, err)
				}
				got, err := c.FindOne(ctx, filter, options.FindOne().SetProjection(all)).Raw()
				if err != nil || !bytes.Equal(got, expected[i]) {
					t.Fatalf("projected point %d=%x err=%v", i, got, err)
				}
			}
			cursor, err := c.Find(ctx, bson.D{}, options.Find().SetProjection(all).SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(1))
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for cursor.Next(ctx) {
				if count >= len(expected) || !bytes.Equal(cursor.Current, expected[count]) {
					t.Fatalf("projected cursor row %d=%x", count, cursor.Current)
				}
				count++
			}
			if err := cursor.Err(); err != nil {
				t.Fatal(err)
			}
			if err := cursor.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if count != len(expected) {
				t.Fatalf("cursor rows=%d want %d", count, len(expected))
			}
		})
	}
}

// BenchmarkStandalonePointProjection is a developmental public-driver read
// comparison over a warmed 16-row fixture, not the mixed YCSB workload. Each
// worker owns one pooled official client and repeatedly reads its own fixed ID.
// The projected cases request all ten binary 100-byte fields and exclude _id.
// Setup, connection warmup, full BSON checks and checked shutdown are untimed.
// ReportAllocs includes the driver, gateway and concurrent Go background work in
// this process; it does not isolate server allocations or measure retained heap.
func BenchmarkStandalonePointProjection(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, clients := range []int{1, 16} {
			for _, projected := range []bool{true, false} {
				shape := "all-ten-fields"
				if !projected {
					shape = "unprojected"
				}
				b.Run(fmt.Sprintf("%s/clients=%d/%s", profile, clients, shape), func(b *testing.B) {
					benchmarkStandalonePointProjection(b, profile, clients, projected)
				})
			}
		}
	}
}

func benchmarkStandalonePointProjection(b *testing.B, profile treedb.Profile, clientCount int, projected bool) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: b.TempDir(), Profile: profile})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("standalone close: %v", err)
		}
	})
	if !s.Backend.HasNativeSemanticBridge() {
		b.Fatal("shared native owner inactive")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()
	b.Cleanup(func() {
		cancel()
		_ = ln.Close()
		select {
		case err := <-done:
			if err != nil {
				b.Errorf("serve: %v", err)
			}
		case <-time.After(standaloneShutdownTimeout):
			b.Error("serve did not stop")
		}
	})
	clients := make([]*mongo.Client, clientCount)
	for i := range clients {
		client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).
			SetDirect(true).SetMaxPoolSize(1).SetRetryReads(false).SetRetryWrites(false).
			SetServerSelectionTimeout(3 * time.Second).SetTimeout(5 * time.Second))
		if err != nil {
			b.Fatal(err)
		}
		clients[i] = client
		b.Cleanup(func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), standaloneShutdownTimeout)
			defer closeCancel()
			if err := client.Disconnect(closeCtx); err != nil {
				b.Errorf("driver disconnect: %v", err)
			}
		})
	}
	const fixtureRows = 16
	rows := make([]any, fixtureRows)
	want := make([]wire.Document, fixtureRows)
	projection := bson.D{{Key: "_id", Value: 0}}
	for i := range rows {
		row := bson.D{{Key: "_id", Value: int32(i)}}
		fields := bson.D{}
		for field := 0; field < 10; field++ {
			key := fmt.Sprintf("field%d", field)
			element := bson.E{Key: key, Value: bson.Binary{Subtype: 0, Data: bytes.Repeat([]byte{byte(i), byte(field)}, 50)}}
			row = append(row, element)
			fields = append(fields, element)
			if i == 0 {
				projection = append(projection, bson.E{Key: key, Value: 1})
			}
		}
		rows[i] = row
		if projected {
			want[i] = marshalProjectionFixture(b, fields)
		} else {
			want[i] = marshalProjectionFixture(b, row)
		}
	}
	inserted, err := clients[0].Database("bench").Collection("items").InsertMany(ctx, rows)
	if err != nil {
		b.Fatal(err)
	}
	if len(inserted.InsertedIDs) != fixtureRows {
		b.Fatalf("inserted rows=%d want %d", len(inserted.InsertedIDs), fixtureRows)
	}
	findOptions := options.FindOne()
	if projected {
		findOptions.SetProjection(projection)
	}
	collections := make([]*mongo.Collection, clientCount)
	filters := make([]bson.D, clientCount)
	for i, client := range clients {
		collections[i] = client.Database("bench").Collection("items")
		filters[i] = bson.D{{Key: "_id", Value: int32(i)}}
		got, err := collections[i].FindOne(ctx, filters[i], findOptions).Raw()
		if err != nil || !bytes.Equal(got, want[i]) {
			b.Fatalf("client %d warmup: result=%x err=%v", i, got, err)
		}
	}
	type result struct {
		last      bson.Raw
		completed int
		err       error
	}
	results := make([]result, clientCount)
	start := make(chan struct{})
	var ready, finished sync.WaitGroup
	ready.Add(clientCount)
	finished.Add(clientCount)
	for worker := range clients {
		go func(i int) {
			var read result
			defer func() {
				results[i] = read
				finished.Done()
			}()
			ready.Done()
			<-start
			for operation := i; operation < b.N; operation += clientCount {
				got, err := collections[i].FindOne(ctx, filters[i], findOptions).Raw()
				if err != nil {
					read.err = err
					return
				}
				if len(got) != len(want[i]) {
					read.err = fmt.Errorf("document bytes=%d want %d", len(got), len(want[i]))
					return
				}
				read.last = got
				read.completed++
			}
		}(worker)
	}
	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	finished.Wait()
	b.StopTimer()
	completed := 0
	for i, result := range results {
		if result.err != nil {
			b.Fatalf("client %d read: %v", i, result.err)
		}
		if result.completed != 0 && !bytes.Equal(result.last, want[i]) {
			b.Fatalf("client %d final result=%x", i, result.last)
		}
		completed += result.completed
	}
	if completed != b.N {
		b.Fatalf("completed reads=%d want %d", completed, b.N)
	}
	b.ReportMetric(float64(completed)/b.Elapsed().Seconds(), "reads/s")
}
