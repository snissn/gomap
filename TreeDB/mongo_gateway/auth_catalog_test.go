package mongogateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/crypto/pbkdf2"
)

func TestAuthCatalogDurableVerifierRotationAndDisable(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	raw, err := db.Get(authCatalogKey("admin", "alice"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("correct horse battery staple")) {
		t.Fatal("catalog stored plaintext password")
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("wrong")); err == nil {
		t.Fatal("wrong password accepted")
	}
	if err := catalog.UpsertPassword("admin", "alice", []byte("rotated password")); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("correct horse battery staple")); err == nil {
		t.Fatal("old password survived rotation")
	}
	if err := catalog.SetEnabled("admin", "alice", false); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("rotated password")); err == nil {
		t.Fatal("disabled user authenticated")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("rotated password")); err == nil {
		t.Fatal("disabled user authenticated after reopen")
	}
}

func TestAuthCatalogRejectsCorruptOrOversizedRecords(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	if err := db.SetSync(authCatalogKey("admin", "bad"), []byte(`{"version":1,"username":"bad"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "bad", []byte("password")); err == nil {
		t.Fatal("corrupt record authenticated")
	}
	if err := catalog.UpsertPassword("admin", "huge", make([]byte, maxAuthPasswordBytes+1)); err == nil {
		t.Fatal("oversized password accepted")
	}
}

func TestSCRAMSHA256EstablishesConnectionIdentityAndGatesCommands(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	password := []byte("correct horse battery staple")
	if err := catalog.UpsertPassword("admin", "alice", password); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	owner := int64(99)
	denied, err := server.commandResponse(context.Background(), "find", mustDocument(t, bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}}), nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if bson.Raw(denied).Lookup("code").Int32() != 13 {
		t.Fatalf("unauthenticated find=%s", bson.Raw(denied))
	}
	clientFirstBare := "n=alice,r=clientnonce"
	startRaw, _ := marshalDocument(bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,," + clientFirstBare)}}, {Key: "$db", Value: "admin"}})
	start, err := server.commandResponse(context.Background(), "saslStart", startRaw, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	startDoc := bson.Raw(start)
	if startDoc.Lookup("ok").Double() != 1 {
		t.Fatalf("start=%s", startDoc)
	}
	id := startDoc.Lookup("conversationId").Int32()
	_, serverFirstBytes := startDoc.Lookup("payload").Binary()
	serverFirst := string(serverFirstBytes)
	parts, ok := scramFields(serverFirst)
	if !ok {
		t.Fatal(serverFirst)
	}
	salt, err := base64.StdEncoding.DecodeString(parts["s"])
	if err != nil {
		t.Fatal(err)
	}
	var count int
	_, err = fmt.Sscanf(parts["i"], "%d", &count)
	if err != nil {
		t.Fatal(err)
	}
	salted := pbkdf2.Key(password, salt, count, sha256.Size, sha256.New)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + parts["r"]
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	signature := hmacSHA256(stored[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ signature[i]
	}
	continueRaw, _ := marshalDocument(bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: id}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof))}}, {Key: "$db", Value: "admin"}})
	continued, err := server.commandResponse(context.Background(), "saslContinue", continueRaw, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(continued); got.Lookup("done").Boolean() != true || got.Lookup("ok").Double() != 1 {
		t.Fatalf("continue=%s", got)
	}
	status, err := server.commandResponse(context.Background(), "connectionStatus", mustDocument(t, bson.D{{Key: "connectionStatus", Value: 1}, {Key: "$db", Value: "admin"}}), nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bson.Raw(status).String(), "alice") {
		t.Fatalf("connection status missed identity: %s", bson.Raw(status))
	}
}

// Exercise the OP_MSG fast find path, cursor commands, write admission, and
// legacy OP_QUERY through the wire server rather than commandResponse alone.
func TestAuthenticationAdmissionCoversWireAndCursorCommandPaths(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	for requestID, doc := range map[int32]bson.D{
		1: {{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}},
		2: {{Key: "getMore", Value: int64(42)}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}},
		3: {{Key: "killCursors", Value: "items"}, {Key: "cursors", Value: bson.A{int64(42)}}, {Key: "$db", Value: "app"}},
		4: {{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "x"}}}}, {Key: "$db", Value: "app"}},
	} {
		assertCommandError(t, serveCommand(t, server, requestID, doc), "Unauthorized")
	}
	query, err := wire.AppendQueryMessage(nil, 9, 0, 0, "admin.$cmd", 0, -1, mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}), nil)
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: bytes.NewReader(query)}
	if err := server.ServeOneWithOwner(rw, 71); err != nil {
		t.Fatal(err)
	}
	_, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := wire.ParseReply(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Documents) != 1 {
		t.Fatalf("reply documents=%d", len(reply.Documents))
	}
	assertCommandError(t, reply.Documents[0], "Unauthorized")
	findRaw := mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}})
	fastResponse, err := server.findMsgResponse(context.Background(), findRaw, 11, 10, 72)
	if err != nil {
		t.Fatal(err)
	}
	fastDoc, err := readMsgResponseResult(fastResponse, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, fastDoc, "Unauthorized")
}

func TestSCRAMUnknownUserKeepsChallengeShapeThenFailsGenerically(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	if err := catalog.UpsertPassword("admin", "alice", []byte("password")); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	var unknownConversationID int32
	for owner, username := range map[int64]string{1: "alice", 2: "unknown"} {
		raw, _ := marshalDocument(bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=" + username + ",r=nonce")}}, {Key: "$db", Value: "admin"}})
		response, err := server.commandResponse(context.Background(), "saslStart", raw, nil, owner)
		if err != nil {
			t.Fatal(err)
		}
		if got := bson.Raw(response); got.Lookup("ok").Double() != 1 || got.Lookup("done").Boolean() {
			t.Fatalf("%s saslStart shape=%s", username, got)
		}
		if username == "unknown" {
			unknownConversationID = bson.Raw(response).Lookup("conversationId").Int32()
		}
	}
	invalid, _ := marshalDocument(bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: unknownConversationID}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("c=biws,r=nonce,p=AAAA")}}, {Key: "$db", Value: "admin"}})
	response, err := server.commandResponse(context.Background(), "saslContinue", invalid, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, response, "AuthenticationFailed")
}
