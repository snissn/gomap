package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func insertBufferFrame(tb testing.TB, query bool, requestID int32, command bson.D) []byte {
	tb.Helper()
	doc := mustDocument(tb, command)
	var frame []byte
	var err error
	if query {
		frame, err = wire.AppendQueryMessage(nil, requestID, 0, 0, "app.$cmd", 0, -1, doc, nil)
	} else {
		frame, err = wire.AppendMsgMessage(nil, requestID, 0, 0, doc)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return frame
}

func insertBufferExchange(tb testing.TB, s *Server, frame, readBuf []byte) ([]byte, wire.Document) {
	tb.Helper()
	rw := &readWriter{r: bytes.NewReader(frame)}
	next, _, err := s.serveOneWithOwner(context.Background(), rw, 1, readBuf, nil)
	if err != nil {
		tb.Fatal(err)
	}
	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), wire.DefaultMaxMessageLength)
	if err != nil {
		tb.Fatal(err)
	}
	if h.OpCode == wire.OpReply {
		reply, err := wire.ParseReply(body)
		if err != nil || len(reply.Documents) != 1 {
			tb.Fatalf("reply: %+v %v", reply, err)
		}
		return next, reply.Documents[0]
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		tb.Fatal(err)
	}
	return next, msg.Body
}

func insertBufferCommand(id string, payload string) bson.D {
	return bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: payload}, {Key: "n", Value: int32(7)}}}}, {Key: "$db", Value: "app"}}
}

// Poison the server's actual ReadMessageInto destination, not the serialized
// client frame. Baseline drops its returned buffer but the retained alias lets
// this ownership characterization run unchanged before the reuse optimization.
func TestStandaloneInsertRequestBodyOwnership(t *testing.T) {
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatBSON, collections.DocumentFormatJSON} {
		for _, query := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/query=%v", format, query), func(t *testing.T) {
				dir := t.TempDir()
				opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: format}}
				s, err := OpenStandaloneServer(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				})
				if !s.Backend.HasNativeSemanticBridge() {
					t.Fatal("native owner inactive")
				}
				expected := make([]wire.Document, 4)
				var alias []byte
				for i := range expected {
					id := fmt.Sprintf("id-%02d", i)
					payload := strings.Repeat(string(rune('a'+i)), 1024)
					expected[i] = mustDocument(t, bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: payload}, {Key: "n", Value: int32(7)}})
					frame := insertBufferFrame(t, query, int32(i+1), insertBufferCommand(id, payload))
					if alias == nil {
						alias = make([]byte, len(frame)-wire.HeaderLen)
					}
					if len(alias) != len(frame)-wire.HeaderLen {
						t.Fatal("fixture must reuse same-size body")
					}
					_, response := insertBufferExchange(t, s.Server, frame, alias[:0])
					assertOK(t, response)
					if n, ok := bson.Raw(response).Lookup("n").Int32OK(); !ok || n != 1 {
						t.Fatalf("insert count: %s", bson.Raw(response))
					}
					if !bytes.Equal(alias, frame[wire.HeaderLen:]) {
						t.Fatal("poison target did not receive actual server body")
					}
					for j := range alias {
						alias[j] = 0xa5
					}
				}
				check := func() {
					t.Helper()
					for i, want := range expected {
						frame := insertBufferFrame(t, false, 100+int32(i), bson.D{{Key: "find", Value: "rows"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("id-%02d", i)}}}, {Key: "$db", Value: "app"}})
						_, response := insertBufferExchange(t, s.Server, frame, nil)
						assertOK(t, response)
						got := cursorFirstBatch(t, response)
						if len(got) != 1 || !bytes.Equal(got[0], want) {
							t.Fatalf("row%d got=%v want=%s", i, got, bson.Raw(want))
						}
					}
				}
				check()
				if err := s.Collections.CheckpointStorage(); err != nil {
					t.Fatal(err)
				}
				check()
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = OpenStandaloneServer(opts)
				if err != nil {
					t.Fatal(err)
				}
				check()
			})
		}
	}
}

func TestStandaloneInsertReadBufferReuse(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(fmt.Sprintf("query=%v", query), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			frame := insertBufferFrame(t, query, 1, insertBufferCommand("one", strings.Repeat("x", 1024)))
			backing := make([]byte, len(frame)-wire.HeaderLen)
			next, response := insertBufferExchange(t, s.Server, frame, backing[:0])
			assertOK(t, response)
			if len(next) == 0 {
				t.Fatal("native standalone insert did not retain read buffer")
			}
			if &next[0] != &backing[0] {
				t.Fatal("insert replaced preallocated body")
			}
			frame = insertBufferFrame(t, query, 2, insertBufferCommand("two", strings.Repeat("y", 1024)))
			again, response := insertBufferExchange(t, s.Server, frame, next)
			assertOK(t, response)
			if len(again) == 0 || &again[0] != &backing[0] {
				t.Fatal("second insert did not reuse same body")
			}
			// A duplicate is an acknowledged indexed write error, never another success.
			_, response = insertBufferExchange(t, s.Server, frame, again)
			assertOK(t, response)
			var result struct {
				N      int32 `bson:"n"`
				Errors []struct {
					Index int32 `bson:"index"`
					Code  int32 `bson:"code"`
				} `bson:"writeErrors"`
			}
			if err := bson.Unmarshal(response, &result); err != nil {
				t.Fatal(err)
			}
			if result.N != 0 || len(result.Errors) != 1 || result.Errors[0].Index != 0 || result.Errors[0].Code != 11000 {
				t.Fatalf("duplicate disposition: %s", bson.Raw(response))
			}
		})
	}
}

func TestStandaloneInsertReadBufferFence(t *testing.T) {
	for _, query := range []bool{false, true} {
		for _, mode := range []string{"unmarked", "replacement", "cluster", "over-cap"} {
			t.Run(fmt.Sprintf("query=%v/%s", query, mode), func(t *testing.T) {
				s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				})
				server := s.Server
				payload := strings.Repeat("x", 1024)
				switch mode {
				case "unmarked":
					server = NewServer()
					server.Collections = s.Collections
					t.Cleanup(func() {
						if err := server.Close(); err != nil {
							t.Error(err)
						}
					})
				case "replacement":
					other, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := other.Close(); err != nil {
							t.Error(err)
						}
					})
					server.Collections = other.Collections
				case "cluster":
					server.ClusterSubmitter = &mongoClusterFakeSubmitter{}
				case "over-cap":
					payload = strings.Repeat("x", maxRetainedWireReadBuffer+1)
				}
				frame := insertBufferFrame(t, query, 1, insertBufferCommand("one", payload))
				next, response := insertBufferExchange(t, server, frame, make([]byte, 0, len(frame)-wire.HeaderLen))
				if mode != "cluster" {
					assertOK(t, response)
				}
				if next != nil {
					t.Fatal("excluded insert retained body")
				}
			})
		}
	}
}
