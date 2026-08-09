package mongogateway

// This file contains the durable verifier catalog used by the gateway's
// SCRAM-SHA-256 exchange.  It is intentionally separate from command
// authorization: a successful verifier check establishes an identity only.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/xdg-go/stringprep"
	"golang.org/x/crypto/pbkdf2"
)

const (
	authCatalogVersion     = 1
	defaultSCRAMIterations = 15000
	minSCRAMIterations     = 4096
	maxSCRAMIterations     = 200000
	maxAuthNameBytes       = 256
	maxAuthPasswordBytes   = 1024
	maxSCRAMSaltBytes      = 64
)

var errAuthenticationFailed = errors.New("authentication failed")

// AuthUser is a safe identity projection. It deliberately contains no
// verifier material and may be retained on a connection after authentication.
type AuthUser struct{ Username, AuthDB string }

// AuthUserRecord is persisted as a versioned, verifier-only record. Never add
// a password (or a reversible equivalent) to this type.
type AuthUserRecord struct {
	Version    int    `json:"version"`
	Username   string `json:"username"`
	AuthDB     string `json:"auth_db"`
	Salt       []byte `json:"salt"`
	Iterations int    `json:"iterations"`
	StoredKey  []byte `json:"stored_key"`
	ServerKey  []byte `json:"server_key"`
	Enabled    bool   `json:"enabled"`
}

// AuthCatalog stores records in TreeDB's durable raw KV space. The prefix is
// private to the gateway and all writes use SetSync so rotation and bootstrap
// acknowledgement never claim persistence before the selected profile's
// durable boundary.
type authCatalogStore interface {
	Get([]byte) ([]byte, error)
	SetSync([]byte, []byte) error
}
type AuthCatalog struct {
	db authCatalogStore
	mu sync.Mutex
	// beforeSetEnabledWrite is a test-only interleaving hook. It is called
	// while mu is held so a rotation cannot race a stale record write.
	beforeSetEnabledWrite func()
}

func NewAuthCatalog(db authCatalogStore) (*AuthCatalog, error) {
	if db == nil {
		return nil, errors.New("mongo gateway auth: nil database")
	}
	return &AuthCatalog{db: db}, nil
}

func authCatalogKey(authDB, username string) []byte {
	return []byte("\x00mongo-gateway/auth/v1/" + base64.RawURLEncoding.EncodeToString([]byte(authDB)) + "/" + base64.RawURLEncoding.EncodeToString([]byte(username)))
}

func validAuthField(value string) bool {
	return value != "" && len(value) <= maxAuthNameBytes && !strings.ContainsRune(value, 0)
}

func validateAuthRecord(r AuthUserRecord) error {
	if r.Version != authCatalogVersion || !validAuthField(r.Username) || !validAuthField(r.AuthDB) || r.Iterations < minSCRAMIterations || r.Iterations > maxSCRAMIterations || len(r.Salt) < 16 || len(r.Salt) > maxSCRAMSaltBytes || len(r.StoredKey) != sha256.Size || len(r.ServerKey) != sha256.Size {
		return errAuthenticationFailed
	}
	return nil
}

// UpsertPassword creates or rotates a verifier. The password bytes are used
// only for derivation and are not kept by this API or its persisted record.
func (c *AuthCatalog) UpsertPassword(authDB, username string, password []byte) error {
	prepared, err := saslprepPassword(password)
	if c == nil || !validAuthField(authDB) || !validAuthField(username) || err != nil {
		return errAuthenticationFailed
	}
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("mongo gateway auth: random salt: %w", err)
	}
	r := newSCRAMRecord(authDB, username, prepared, salt, defaultSCRAMIterations)
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.SetSync(authCatalogKey(authDB, username), raw)
}

func newSCRAMRecord(authDB, username string, password, salt []byte, iterations int) AuthUserRecord {
	salted := pbkdf2.Key(password, salt, iterations, sha256.Size, sha256.New)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	return AuthUserRecord{Version: authCatalogVersion, Username: username, AuthDB: authDB, Salt: append([]byte(nil), salt...), Iterations: iterations, StoredKey: stored[:], ServerKey: hmacSHA256(salted, []byte("Server Key")), Enabled: true}
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func (c *AuthCatalog) record(authDB, username string) (AuthUserRecord, error) {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	c.mu.Lock()
	raw, err := c.db.Get(authCatalogKey(authDB, username))
	c.mu.Unlock()
	if err != nil {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	var r AuthUserRecord
	if json.Unmarshal(raw, &r) != nil || validateAuthRecord(r) != nil || r.Username != username || r.AuthDB != authDB || !r.Enabled {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	return r, nil
}

func (c *AuthCatalog) storedRecordLocked(authDB, username string) (AuthUserRecord, error) {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	raw, err := c.db.Get(authCatalogKey(authDB, username))
	var r AuthUserRecord
	if err != nil || json.Unmarshal(raw, &r) != nil || validateAuthRecord(r) != nil || r.Username != username || r.AuthDB != authDB {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	return r, nil
}

// SetEnabled atomically replaces the stored record with a changed enabled bit.
func (c *AuthCatalog) SetEnabled(authDB, username string, enabled bool) error {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return errAuthenticationFailed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.storedRecordLocked(authDB, username)
	if err != nil {
		return err
	}
	r.Enabled = enabled
	if c.beforeSetEnabledWrite != nil {
		c.beforeSetEnabledWrite()
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.db.SetSync(authCatalogKey(authDB, username), raw)
}

// VerifyPassword is used by tests and bootstrap tooling. Wire authentication
// uses the same stored verifier via the SCRAM proof path, never this helper in
// steady-state CRUD.
func (c *AuthCatalog) VerifyPassword(authDB, username string, password []byte) (AuthUser, error) {
	r, err := c.record(authDB, username)
	prepared, prepErr := saslprepPassword(password)
	if err != nil || prepErr != nil {
		return AuthUser{}, errAuthenticationFailed
	}
	trial := newSCRAMRecord(authDB, username, prepared, r.Salt, r.Iterations)
	if subtle.ConstantTimeCompare(trial.StoredKey, r.StoredKey) != 1 {
		return AuthUser{}, errAuthenticationFailed
	}
	return AuthUser{Username: username, AuthDB: authDB}, nil
}

func saslprepPassword(password []byte) ([]byte, error) {
	if len(password) == 0 || len(password) > maxAuthPasswordBytes || !utf8.Valid(password) {
		return nil, errAuthenticationFailed
	}
	prepared, err := stringprep.SASLprep.Prepare(string(password))
	if err != nil || len(prepared) == 0 || len(prepared) > maxAuthPasswordBytes {
		return nil, errAuthenticationFailed
	}
	return []byte(prepared), nil
}
