package mongogateway

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestStandaloneExplicitCOWBudgetRefusesBeforeAssignment(t *testing.T) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed, COWMemtableLimits: treedb.DefaultCOWMemtableLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	_, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "app.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := s.Collections.OpenCollection("app.rows")
	if err != nil {
		t.Fatal(err)
	}
	document, err := bson.Marshal(bson.D{{Key: "payload", Value: strings.Repeat("x", defaultMaxBSONObjectSize-1024)}})
	if err != nil {
		t.Fatal(err)
	}
	before := s.Backend.CommandWALNextLSN()
	_, err = col.InsertBatchValidatedBSON([][]byte{[]byte("large")}, [][]byte{document})
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("explicit finite budget: %v", err)
	}
	if s.Backend.CommandWALNextLSN() != before {
		t.Fatal("refusal assigned a WAL command")
	}
	if got, err := col.Get([]byte("large")); len(got) != 0 || (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) {
		t.Fatalf("refusal published document: length=%d err=%v", len(got), err)
	}
}

func TestMongoInsertCoalescerBoundsBytes(t *testing.T) {
	// Each request fits this explicit small budget. Combining both requests
	// would exceed it; the byte boundary must split independent requests.
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed, MaxMessageLength: 8 << 20, COWMemtableLimits: treedb.DefaultCOWMemtableLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	_, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "app.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := s.Collections.OpenCollection("app.rows")
	if err != nil {
		t.Fatal(err)
	}
	document, err := bson.Marshal(bson.D{{Key: "payload", Value: strings.Repeat("x", 7<<20)}})
	if err != nil {
		t.Fatal(err)
	}
	batch := []mongoInsertCoalescerRequest{
		{col: col, id: []byte("a"), stored: document, done: make(chan mongoInsertCoalescerResult, 1)},
		{col: col, id: []byte("b"), stored: document, done: make(chan mongoInsertCoalescerResult, 1)},
	}
	coalescer := mongoInsertCoalescer{server: s.Server}
	coalescer.runBatch(batch)
	for i, request := range batch {
		if result := <-request.done; result.err != nil {
			t.Fatalf("request%d: %v", i, result.err)
		}
	}
	for _, id := range [][]byte{[]byte("a"), []byte("b")} {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("read %q: %v", id, err)
		}
	}
}

func TestMongoInsertCoalescerRechecksDeadlineBetweenByteGroups(t *testing.T) {
	large, err := bson.Marshal(bson.D{{Key: "payload", Value: strings.Repeat("x", 1024)}})
	if err != nil {
		t.Fatal(err)
	}
	small, err := bson.Marshal(bson.D{{Key: "payload", Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	// The first request fills one byte group. The two remaining requests fit
	// together, exercising the batch path rather than deadline-aware singles.
	limit := len(large) + 1 + 8
	if 2*(len(small)+1+8) > limit {
		t.Fatal("fixture must retain a two-request second byte group")
	}
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, MaxMessageLength: int32(limit)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "app.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	col, err := s.Collections.OpenCollection("app.rows")
	if err != nil {
		t.Fatal(err)
	}
	batch := []mongoInsertCoalescerRequest{
		{col: col, id: []byte("a"), stored: large, done: make(chan mongoInsertCoalescerResult, 2)},
		{col: col, id: []byte("b"), stored: small, deadline: time.Now().Add(time.Hour), done: make(chan mongoInsertCoalescerResult, 2)},
		{col: col, id: []byte("c"), stored: small, deadline: time.Now().Add(time.Hour), done: make(chan mongoInsertCoalescerResult, 2)},
	}
	var once sync.Once
	var atFirstSyncLSN uint64
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyFileSync {
			once.Do(func() {
				atFirstSyncLSN = s.Backend.CommandWALNextLSN()
				// Deterministically model expiry while the first group waits for
				// real durable completion, without a wall-clock scheduling race.
				for i := 1; i < len(batch); i++ {
					batch[i].deadline = time.Now().Add(-time.Second)
				}
			})
		}
		return nil
	})
	defer restore()
	(&mongoInsertCoalescer{server: s.Server}).runBatch(batch)
	if atFirstSyncLSN == 0 {
		t.Fatal("fixture did not reach a real durable boundary")
	}
	for i, request := range batch {
		select {
		case result := <-request.done:
			if i == 0 && result.err != nil {
				t.Fatalf("first request: %v", result.err)
			}
			if i > 0 && (result.err == nil || !strings.Contains(result.err.Error(), "execution-time budget")) {
				t.Errorf("expired request%d result=%v", i, result.err)
			}
		default:
			t.Fatalf("request%d has no completion", i)
		}
		if len(request.done) != 0 {
			t.Errorf("request%d completed more than once", i)
		}
		got, err := col.Get(request.id)
		if i == 0 && (err != nil || len(got) == 0) {
			t.Errorf("first document unavailable: %v", err)
		}
		if i > 0 && (len(got) != 0 || (err != nil && !errors.Is(err, tree.ErrKeyNotFound))) {
			t.Errorf("expired request%d published: len=%d err=%v", i, len(got), err)
		}
	}
	if after := s.Backend.CommandWALNextLSN(); after != atFirstSyncLSN {
		t.Errorf("expired group assigned WAL work: before=%d after=%d", atFirstSyncLSN, after)
	}
}

// The public constructor advertises a 16 MiB BSON limit. A large unrelated
// payload must not make tiny scalar indexes reserve whole-document decoders.
func TestStandaloneLargeIndexedBSON(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- s.Serve(ctx, ln) }()
			client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetRetryWrites(false))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Disconnect(context.Background())
			c := client.Database("app").Collection("rows")
			_, err = c.Indexes().CreateMany(ctx, []mongo.IndexModel{
				{Keys: bson.D{{Key: "n", Value: 1}}},
				{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)},
			})
			if err != nil {
				t.Fatal(err)
			}
			payload := strings.Repeat("x", defaultMaxBSONObjectSize-1024)
			_, err = c.InsertOne(ctx, bson.D{{Key: "_id", Value: "large"}, {Key: "n", Value: 1}, {Key: "email", Value: "a@example.test"}, {Key: "payload", Value: payload}})
			if err != nil {
				t.Fatalf("insert near advertised maximum: %v", err)
			}
			_, err = c.UpdateOne(ctx, bson.D{{Key: "_id", Value: "large"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: 2}}}})
			if err != nil {
				t.Fatalf("update large indexed document: %v", err)
			}
			var got struct {
				Payload string `bson:"payload"`
			}
			if err = c.FindOne(ctx, bson.D{{Key: "n", Value: 2}}).Decode(&got); err != nil || got.Payload != payload {
				t.Fatalf("indexed read: length=%d err=%v", len(got.Payload), err)
			}
			if err = client.Disconnect(ctx); err != nil {
				t.Fatal(err)
			}
			cancel()
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s = nil
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			s, err = OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			col, err := s.Collections.OpenCollection("app.rows")
			if err != nil {
				t.Fatal(err)
			}
			ids, err := col.FindByIndexValue("n_1", bson.RawValue{Type: bson.TypeInt32, Value: []byte{2, 0, 0, 0}})
			if err != nil || len(ids) != 1 {
				t.Fatalf("reopen index: %q %v", ids, err)
			}
			stored, err := col.Get(ids[0])
			if err != nil {
				t.Fatal(err)
			}
			if text, ok := bson.Raw(stored).Lookup("payload").StringValueOK(); !ok || text != payload {
				t.Fatalf("reopen payload length=%d ok=%v", len(text), ok)
			}
		})
	}
}
