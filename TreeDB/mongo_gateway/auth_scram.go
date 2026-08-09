package mongogateway

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	maxSCRAMPayloadBytes               = 16 << 10
	maxSCRAMConversationsPerConnection = 8
	maxSCRAMConversationAge            = 30 * time.Second
)

type scramConversation struct {
	id                           int32
	user                         AuthUser
	record                       AuthUserRecord
	clientFirstBare, serverFirst string
	serverNonce                  string
	issuedAt                     time.Time
	valid                        bool
}
type authConnectionState struct {
	user          atomic.Pointer[AuthUser]
	mu            sync.Mutex
	conversations map[int32]scramConversation
}

func (s *Server) authState(owner int64) *authConnectionState {
	state := &authConnectionState{conversations: make(map[int32]scramConversation)}
	actual, _ := s.authConnections.LoadOrStore(owner, state)
	return actual.(*authConnectionState)
}
func (s *Server) clearAuthState(owner int64) {
	s.authConnections.Delete(owner)
}
func (s *Server) authenticated(owner int64) bool {
	return s.authUserSnapshot(owner) != nil
}
func (s *Server) authUser(owner int64) *AuthUser {
	u := s.authUserSnapshot(owner)
	if u == nil {
		return nil
	}
	copy := *u
	return &copy
}
func (s *Server) authUserSnapshot(owner int64) *AuthUser {
	state, ok := s.authConnections.Load(owner)
	if !ok {
		return nil
	}
	connection := state.(*authConnectionState)
	user := connection.user.Load()
	if user == nil {
		return nil
	}
	if s.AuthCatalog == nil || !s.AuthCatalog.identityActive(*user) {
		connection.user.CompareAndSwap(user, nil)
		return nil
	}
	return user
}
func (s *Server) authenticationRequired() bool {
	return s != nil && s.AuthenticationEnabled
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

func (s *Server) authFailureCounted() (wire.Document, error) {
	if s != nil {
		s.authFailures.Add(1)
	}
	return authFailure()
}

func (s *Server) saslStartResponse(command wire.Document, owner int64) (wire.Document, error) {
	if !s.authenticationRequired() {
		return commandError(59, "CommandNotFound", "authentication is not enabled")
	}
	if s.AuthCatalog == nil {
		return s.authFailureCounted()
	}
	raw := bson.Raw(command)
	mechanism, ok := raw.Lookup("mechanism").StringValueOK()
	if !ok || mechanism != "SCRAM-SHA-256" {
		return s.authFailureCounted()
	}
	_, payload, ok := raw.Lookup("payload").BinaryOK()
	if !ok || len(payload) == 0 || len(payload) > maxSCRAMPayloadBytes {
		return s.authFailureCounted()
	}
	message := string(payload)
	if !strings.HasPrefix(message, "n,,") {
		return s.authFailureCounted()
	}
	bare := strings.TrimPrefix(message, "n,,")
	fields, ok := scramFields(bare)
	if !ok || fields["m"] != "" || fields["n"] == "" || fields["r"] == "" || len(fields["r"]) > maxAuthNameBytes {
		return s.authFailureCounted()
	}
	username, ok := scramUnescape(fields["n"])
	if !ok {
		return s.authFailureCounted()
	}
	authDB, ok := raw.Lookup("$db").StringValueOK()
	if !ok {
		return s.authFailureCounted()
	}
	record, err := s.AuthCatalog.record(authDB, username)
	valid := err == nil
	if !valid {
		// Preserve the saslStart shape for unknown/disabled/corrupt records so
		// username existence is not exposed before proof verification.
		record, err = s.AuthCatalog.syntheticSCRAMRecord(authDB, username)
		if err != nil {
			return s.authFailureCounted()
		}
	}
	random := make([]byte, 18)
	if _, err := rand.Read(random); err != nil {
		return s.authFailureCounted()
	}
	nonce := fields["r"] + base64.RawStdEncoding.EncodeToString(random)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", nonce, base64.StdEncoding.EncodeToString(record.Salt), record.Iterations)
	state := s.authState(owner)
	state.mu.Lock()
	s.expireSCRAMConversationsLocked(state, time.Now())
	if len(state.conversations) >= maxSCRAMConversationsPerConnection {
		state.mu.Unlock()
		return s.authFailureCounted()
	}
	id := s.nextSASLConversation.Add(1)
	if id == 0 {
		id = s.nextSASLConversation.Add(1)
	}
	state.conversations[id] = scramConversation{id: id, user: AuthUser{Username: username, AuthDB: authDB, Incarnation: record.Incarnation}, record: record, clientFirstBare: bare, serverFirst: serverFirst, serverNonce: nonce, issuedAt: time.Now(), valid: valid}
	state.mu.Unlock()
	return marshalDocument(bson.D{{Key: "conversationId", Value: id}, {Key: "done", Value: false}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(serverFirst)}}, {Key: "ok", Value: 1.0}})
}

func (s *Server) saslContinueResponse(command wire.Document, owner int64) (wire.Document, error) {
	raw := bson.Raw(command)
	id, ok := raw.Lookup("conversationId").Int32OK()
	if !ok {
		return s.authFailureCounted()
	}
	_, payload, ok := raw.Lookup("payload").BinaryOK()
	if !ok || len(payload) == 0 || len(payload) > maxSCRAMPayloadBytes {
		return s.authFailureCounted()
	}
	value, exists := s.authConnections.Load(owner)
	var state *authConnectionState
	if exists {
		state = value.(*authConnectionState)
		state.mu.Lock()
	}
	var conversation scramConversation
	if state != nil {
		s.expireSCRAMConversationsLocked(state, time.Now())
		conversation, ok = state.conversations[id]
		delete(state.conversations, id)
	}
	if state != nil {
		state.mu.Unlock()
	}
	if !ok {
		return s.authFailureCounted()
	}
	message := string(payload)
	fields, parsed := scramFields(message)
	proofB64 := fields["p"]
	if !parsed || proofB64 == "" || fields["c"] != "biws" {
		return s.authFailureCounted()
	}
	withoutProof := strings.TrimSuffix(message, ",p="+proofB64)
	if withoutProof == message || fields["r"] == "" || fields["r"] != conversation.serverNonce {
		return s.authFailureCounted()
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil || len(proof) != 32 {
		return s.authFailureCounted()
	}
	authMessage := conversation.clientFirstBare + "," + conversation.serverFirst + "," + withoutProof
	clientSignature := hmacSHA256(conversation.record.StoredKey, []byte(authMessage))
	clientKey := make([]byte, 32)
	for i := range clientKey {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}
	stored := sha256.Sum256(clientKey)
	proofValid := subtle.ConstantTimeCompare(stored[:], conversation.record.StoredKey)
	if subtle.ConstantTimeSelect(boolToInt(conversation.valid), proofValid, 0) != 1 {
		return s.authFailureCounted()
	}
	serverSignature := hmacSHA256(conversation.record.ServerKey, []byte(authMessage))
	user := conversation.user
	if s.beforeSCRAMIdentityStore != nil {
		s.beforeSCRAMIdentityStore()
	}
	// Complete against the exact state which owned the consumed conversation.
	// ReleaseOwner may have removed it and a reused owner ID may now point at a
	// different state; never reload by owner here.
	state.user.Store(&user)
	return marshalDocument(bson.D{{Key: "conversationId", Value: id}, {Key: "done", Value: true}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("v=" + base64.StdEncoding.EncodeToString(serverSignature))}}, {Key: "ok", Value: 1.0}})
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Server) expireSCRAMConversationsLocked(state *authConnectionState, now time.Time) {
	for id, conversation := range state.conversations {
		if now.Sub(conversation.issuedAt) > maxSCRAMConversationAge {
			delete(state.conversations, id)
		}
	}
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
