package mongogateway

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const maxSCRAMPayloadBytes = 16 << 10

type scramConversation struct {
	id                           int32
	user                         AuthUser
	record                       AuthUserRecord
	clientFirstBare, serverFirst string
	valid                        bool
}
type authConnectionState struct {
	user          *AuthUser
	conversations map[int32]scramConversation
}

func (s *Server) authState(owner int64) *authConnectionState {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.authConnections == nil {
		s.authConnections = make(map[int64]*authConnectionState)
	}
	state := s.authConnections[owner]
	if state == nil {
		state = &authConnectionState{conversations: make(map[int32]scramConversation)}
		s.authConnections[owner] = state
	}
	return state
}
func (s *Server) clearAuthState(owner int64) {
	s.authMu.Lock()
	delete(s.authConnections, owner)
	s.authMu.Unlock()
}
func (s *Server) authenticated(owner int64) bool {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	return s.authConnections[owner] != nil && s.authConnections[owner].user != nil
}
func (s *Server) authUser(owner int64) *AuthUser {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if u := s.authConnections[owner]; u != nil && u.user != nil {
		copy := *u.user
		return &copy
	}
	return nil
}
func (s *Server) authenticationRequired() bool {
	return s != nil && s.AuthenticationEnabled && s.AuthCatalog != nil
}

func authUnauthenticatedCommand(name string) bool {
	switch name {
	case "hello", "isMaster", "ismaster", "saslStart", "saslContinue", "connectionStatus", "buildInfo", "ping":
		return true
	}
	return false
}

func authFailure() (wire.Document, error) {
	return commandError(18, "AuthenticationFailed", "Authentication failed")
}

func (s *Server) saslStartResponse(command wire.Document, owner int64) (wire.Document, error) {
	if !s.authenticationRequired() {
		return commandError(59, "CommandNotFound", "authentication is not enabled")
	}
	raw := bson.Raw(command)
	mechanism, ok := raw.Lookup("mechanism").StringValueOK()
	if !ok || mechanism != "SCRAM-SHA-256" {
		return authFailure()
	}
	_, payload := raw.Lookup("payload").Binary()
	if len(payload) == 0 || len(payload) > maxSCRAMPayloadBytes {
		return authFailure()
	}
	message := string(payload)
	if !strings.HasPrefix(message, "n,,") {
		return authFailure()
	}
	bare := strings.TrimPrefix(message, "n,,")
	fields, ok := scramFields(bare)
	if !ok || fields["n"] == "" || fields["r"] == "" || len(fields["r"]) > maxAuthNameBytes {
		return authFailure()
	}
	username, ok := scramUnescape(fields["n"])
	if !ok {
		return authFailure()
	}
	authDB, ok := raw.Lookup("$db").StringValueOK()
	if !ok {
		return authFailure()
	}
	record, err := s.AuthCatalog.record(authDB, username)
	valid := err == nil
	if !valid {
		// Preserve the saslStart shape for unknown/disabled/corrupt records so
		// username existence is not exposed before proof verification.
		record = newSCRAMRecord(authDB, username, []byte("mongo-gateway-invalid-user-verifier"), []byte("mongo-gateway-invalid-salt-32-byte"), defaultSCRAMIterations)
	}
	random := make([]byte, 18)
	if _, err := rand.Read(random); err != nil {
		return authFailure()
	}
	nonce := fields["r"] + base64.RawStdEncoding.EncodeToString(random)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", nonce, base64.StdEncoding.EncodeToString(record.Salt), record.Iterations)
	state := s.authState(owner)
	s.authMu.Lock()
	id := s.nextSASLConversation.Add(1)
	if id == 0 {
		id = s.nextSASLConversation.Add(1)
	}
	state.conversations[id] = scramConversation{id: id, user: AuthUser{Username: username, AuthDB: authDB}, record: record, clientFirstBare: bare, serverFirst: serverFirst, valid: valid}
	s.authMu.Unlock()
	return marshalDocument(bson.D{{Key: "conversationId", Value: id}, {Key: "done", Value: false}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(serverFirst)}}, {Key: "ok", Value: 1.0}})
}

func (s *Server) saslContinueResponse(command wire.Document, owner int64) (wire.Document, error) {
	raw := bson.Raw(command)
	id, ok := raw.Lookup("conversationId").Int32OK()
	if !ok {
		return authFailure()
	}
	_, payload := raw.Lookup("payload").Binary()
	if len(payload) == 0 || len(payload) > maxSCRAMPayloadBytes {
		return authFailure()
	}
	s.authMu.Lock()
	state := s.authConnections[owner]
	var conversation scramConversation
	if state != nil {
		conversation, ok = state.conversations[id]
		delete(state.conversations, id)
	}
	s.authMu.Unlock()
	if !ok {
		return authFailure()
	}
	message := string(payload)
	fields, parsed := scramFields(message)
	proofB64 := fields["p"]
	if !parsed || proofB64 == "" || fields["c"] != "biws" {
		s.authFailures.Add(1)
		return authFailure()
	}
	withoutProof := strings.TrimSuffix(message, ",p="+proofB64)
	if withoutProof == message || fields["r"] == "" || !strings.HasPrefix(fields["r"], strings.Split(conversation.serverFirst, ",")[0][2:]) {
		s.authFailures.Add(1)
		return authFailure()
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil || len(proof) != 32 {
		s.authFailures.Add(1)
		return authFailure()
	}
	authMessage := conversation.clientFirstBare + "," + conversation.serverFirst + "," + withoutProof
	clientSignature := hmacSHA256(conversation.record.StoredKey, []byte(authMessage))
	clientKey := make([]byte, 32)
	for i := range clientKey {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}
	stored := sha256.Sum256(clientKey)
	if !conversation.valid || subtle.ConstantTimeCompare(stored[:], conversation.record.StoredKey) != 1 {
		s.authFailures.Add(1)
		return authFailure()
	}
	serverSignature := hmacSHA256(conversation.record.ServerKey, []byte(authMessage))
	user := conversation.user
	s.authMu.Lock()
	if state := s.authConnections[owner]; state != nil {
		state.user = &user
	}
	s.authMu.Unlock()
	return marshalDocument(bson.D{{Key: "conversationId", Value: id}, {Key: "done", Value: true}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("v=" + base64.StdEncoding.EncodeToString(serverSignature))}}, {Key: "ok", Value: 1.0}})
}

func scramFields(input string) (map[string]string, bool) {
	fields := make(map[string]string)
	for _, field := range strings.Split(input, ",") {
		pair := strings.SplitN(field, "=", 2)
		if len(pair) != 2 || len(pair[0]) != 1 || pair[1] == "" {
			return nil, false
		}
		if _, duplicate := fields[pair[0]]; duplicate {
			return nil, false
		}
		fields[pair[0]] = pair[1]
	}
	return fields, true
}
func scramUnescape(v string) (string, bool) {
	v = strings.ReplaceAll(v, "=2C", ",")
	v = strings.ReplaceAll(v, "=3D", "=")
	return v, validAuthField(v)
}
