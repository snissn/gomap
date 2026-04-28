package wire

import (
	"bytes"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustDocument(tb testing.TB, doc bson.D) Document {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal BSON document: %v", err)
	}
	return Document(raw)
}

func TestReadMessageRoundTrip(t *testing.T) {
	body := []byte{1, 2, 3, 4}
	wireBytes := AppendMessage(nil, 42, 7, OpMsg, body)

	h, gotBody, err := ReadMessage(bytes.NewReader(wireBytes), 0)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if h.MessageLength != int32(HeaderLen+len(body)) {
		t.Fatalf("message length=%d want %d", h.MessageLength, HeaderLen+len(body))
	}
	if h.RequestID != 42 || h.ResponseTo != 7 || h.OpCode != OpMsg {
		t.Fatalf("header=%+v", h)
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("body=%v want %v", gotBody, body)
	}
}

func TestReadMessageRejectsOversized(t *testing.T) {
	wireBytes := AppendMessage(nil, 1, 0, OpMsg, []byte{1, 2, 3, 4})
	_, _, err := ReadMessage(bytes.NewReader(wireBytes), HeaderLen+2)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("ReadMessage err=%v want ErrMessageTooLarge", err)
	}
}

func TestQueryHandshakeReplyRoundTrip(t *testing.T) {
	queryDoc := mustDocument(t, bson.D{
		{Key: "isMaster", Value: int32(1)},
		{Key: "helloOk", Value: true},
		{Key: "$db", Value: "admin"},
	})
	queryBytes, err := AppendQueryMessage(nil, 101, 0, 0, "admin.$cmd", 0, -1, queryDoc, nil)
	if err != nil {
		t.Fatalf("AppendQueryMessage: %v", err)
	}

	h, body, err := ReadMessage(bytes.NewReader(queryBytes), 0)
	if err != nil {
		t.Fatalf("ReadMessage query: %v", err)
	}
	if h.OpCode != OpQuery {
		t.Fatalf("opcode=%d want %d", h.OpCode, OpQuery)
	}
	q, err := ParseQuery(body)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	if q.FullCollectionName != "admin.$cmd" || q.NumberToReturn != -1 {
		t.Fatalf("query=%+v", q)
	}
	name, err := CommandName(q.Query)
	if err != nil {
		t.Fatalf("CommandName: %v", err)
	}
	if name != "isMaster" {
		t.Fatalf("command=%q want isMaster", name)
	}

	replyDoc := mustDocument(t, bson.D{
		{Key: "ok", Value: 1.0},
		{Key: "helloOk", Value: true},
		{Key: "isWritablePrimary", Value: true},
		{Key: "maxWireVersion", Value: int32(21)},
	})
	replyBytes, err := AppendReplyMessage(nil, 202, h.RequestID, 0, 0, 0, replyDoc)
	if err != nil {
		t.Fatalf("AppendReplyMessage: %v", err)
	}
	replyHeader, replyBody, err := ReadMessage(bytes.NewReader(replyBytes), 0)
	if err != nil {
		t.Fatalf("ReadMessage reply: %v", err)
	}
	if replyHeader.OpCode != OpReply || replyHeader.ResponseTo != h.RequestID {
		t.Fatalf("reply header=%+v", replyHeader)
	}
	reply, err := ParseReply(replyBody)
	if err != nil {
		t.Fatalf("ParseReply: %v", err)
	}
	if len(reply.Documents) != 1 || !bytes.Equal(reply.Documents[0], replyDoc) {
		t.Fatalf("reply documents=%d", len(reply.Documents))
	}
}

func TestMsgBodyRoundTrip(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{
		{Key: "ping", Value: int32(1)},
		{Key: "$db", Value: "admin"},
	})
	wireBytes, err := AppendMsgMessage(nil, 77, 0, 0, commandDoc)
	if err != nil {
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	h, body, err := ReadMessage(bytes.NewReader(wireBytes), 0)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if h.OpCode != OpMsg {
		t.Fatalf("opcode=%d want %d", h.OpCode, OpMsg)
	}
	msg, err := ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	name, err := CommandName(msg.Body)
	if err != nil {
		t.Fatalf("CommandName: %v", err)
	}
	if name != "ping" {
		t.Fatalf("command=%q want ping", name)
	}
}

func TestParseMsgAllowsOptionalFlags(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}})
	body := appendInt32(nil, int32(MsgFlagExhaustAllowed))
	body = append(body, MsgSectionBody)
	body = append(body, commandDoc...)

	msg, err := ParseMsg(body)
	if err != nil {
		t.Fatalf("ParseMsg: %v", err)
	}
	if msg.Flags != MsgFlagExhaustAllowed {
		t.Fatalf("flags=%#x want %#x", msg.Flags, MsgFlagExhaustAllowed)
	}
}

func TestParseMsgRejectsDocumentSequenceUntilNeeded(t *testing.T) {
	body := appendInt32(nil, 0)
	body = append(body, 1) // kind 1 document sequence
	body = appendInt32(body, 5)
	body = append(body, 0)

	_, err := ParseMsg(body)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ParseMsg err=%v want ErrUnsupported", err)
	}
}

func TestParseMsgRejectsChecksumPresent(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}})
	body := appendInt32(nil, int32(MsgFlagChecksumPresent))
	body = append(body, MsgSectionBody)
	body = append(body, commandDoc...)
	body = appendInt32(body, 0)

	_, err := ParseMsg(body)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ParseMsg err=%v want ErrUnsupported", err)
	}
}

func TestParseMsgRejectsUnknownRequiredFlag(t *testing.T) {
	commandDoc := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}})
	body := appendInt32(nil, 1<<2)
	body = append(body, MsgSectionBody)
	body = append(body, commandDoc...)

	_, err := ParseMsg(body)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("ParseMsg err=%v want ErrMalformed", err)
	}
}
