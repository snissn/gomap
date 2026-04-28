package mongogateway

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type readWriter struct {
	r io.Reader
	w bytes.Buffer
}

func (rw *readWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *readWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func TestServerHandlesQueryHello(t *testing.T) {
	queryDoc := mustDocument(t, bson.D{
		{Key: "isMaster", Value: int32(1)},
		{Key: "helloOk", Value: true},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendQueryMessage(nil, 100, 0, 0, "admin.$cmd", 0, -1, queryDoc, nil)
	if err != nil {
		t.Fatalf("AppendQueryMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpReply || h.ResponseTo != 100 {
		t.Fatalf("response header=%+v", h)
	}
	reply, err := wire.ParseReply(body)
	if err != nil {
		t.Fatalf("ParseReply: %v", err)
	}
	if len(reply.Documents) != 1 {
		t.Fatalf("reply document count=%d want 1", len(reply.Documents))
	}
	assertOK(t, reply.Documents[0])
	assertBool(t, reply.Documents[0], "helloOk", true)
}

func TestServerHandlesMsgPing(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	req, err := wire.AppendMsgMessage(nil, 200, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	if err := NewServer().ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}

	h, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadMessage response: %v", err)
	}
	if h.OpCode != wire.OpMsg || h.ResponseTo != 200 {
		t.Fatalf("response header=%+v", h)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	assertOK(t, msg.Body)
}

func TestServerRejectsCompressedMessages(t *testing.T) {
	req, err := wire.AppendMessage(nil, 300, 0, wire.OpCompressed, nil)
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}

	err = NewServer().ServeOne(rw)
	if !errors.Is(err, wire.ErrUnsupported) {
		t.Fatalf("ServeOne err=%v want ErrUnsupported", err)
	}
	if rw.w.Len() != 0 {
		t.Fatalf("unexpected response bytes=%d", rw.w.Len())
	}
}

func mustDocument(tb testing.TB, doc bson.D) wire.Document {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal BSON document: %v", err)
	}
	return wire.Document(raw)
}

func assertOK(tb testing.TB, doc wire.Document) {
	tb.Helper()
	raw := bson.Raw(doc)
	value := raw.Lookup("ok")
	ok, okType := value.DoubleOK()
	if !okType || ok != 1.0 {
		tb.Fatalf("ok=%v typeOK=%v want 1.0", ok, okType)
	}
}

func assertBool(tb testing.TB, doc wire.Document, key string, want bool) {
	tb.Helper()
	raw := bson.Raw(doc)
	value := raw.Lookup(key)
	got, ok := value.BooleanOK()
	if !ok || got != want {
		tb.Fatalf("%s=%v typeOK=%v want %v", key, got, ok, want)
	}
}
