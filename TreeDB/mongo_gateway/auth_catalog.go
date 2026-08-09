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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	treedb "github.com/snissn/gomap/TreeDB"
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/xdg-go/stringprep"
	"golang.org/x/crypto/pbkdf2"
)

const (
	authCatalogVersion     = 2
	defaultSCRAMIterations = 15000
	minSCRAMIterations     = 4096
	maxSCRAMIterations     = 200000
	maxAuthNameBytes       = 256
	maxAuthPasswordBytes   = 1024
	maxSCRAMSaltBytes      = 64
)

var errAuthenticationFailed = errors.New("authentication failed")

var (
	errAuthUserExists                       = errors.New("mongo gateway auth: user already exists")
	errAuthUserNotFound                     = errors.New("mongo gateway auth: user not found")
	errCannotDisableLastServerAdministrator = errors.New("mongo gateway authorization: cannot disable the last enabled server administrator")
	errCannotDropLastServerAdministrator    = errors.New("mongo gateway authorization: cannot drop the last enabled server administrator")
	errCannotDemoteLastServerAdministrator  = errors.New("mongo gateway authorization: cannot demote the last enabled server administrator")
	errUserManagementUnauthorized           = errors.New("mongo gateway authorization: user management not authorized")
	errNoUsableServerAdministrator          = errors.New("mongo gateway authorization: no usable server administrator; offline repair required")
)

// AuthUser is a safe identity projection. It deliberately contains no
// verifier material and may be retained on a connection after authentication.
type AuthUser struct {
	Username    string
	AuthDB      string
	Incarnation uint64
}

// AuthUserRecord is persisted as a versioned, verifier-only record. Never add
// a password (or a reversible equivalent) to this type.
type AuthUserRecord struct {
	Version     int    `json:"version"`
	Username    string `json:"username"`
	AuthDB      string `json:"auth_db"`
	Salt        []byte `json:"salt"`
	Iterations  int    `json:"iterations"`
	StoredKey   []byte `json:"stored_key"`
	ServerKey   []byte `json:"server_key"`
	Enabled     bool   `json:"enabled"`
	Incarnation uint64 `json:"incarnation"`
}

// AuthCatalog stores records in TreeDB's durable raw KV space. The prefix is
// private to the gateway and all writes use SetSync so rotation and bootstrap
// acknowledgement never claim persistence before the selected profile's
// durable boundary.
type authCatalogStore interface {
	Get([]byte) ([]byte, error)
	SetSync([]byte, []byte) error
}
type authCatalogDeleteStore interface {
	DeleteSync([]byte) error
}
type authCatalogGetAppendStore interface {
	GetAppend([]byte, []byte) ([]byte, error)
}
type authCatalogLargeValueStore interface {
	authCatalogStore
	AppendValueLogValues([][]byte) ([]page.ValuePtr, error)
	ReleaseValueLogValues([]page.ValuePtr)
	NewBatch() batchpkg.Interface
}
type authCatalogPointerBatch interface {
	batchpkg.Interface
	SetPointer([]byte, page.ValuePtr) error
}

// authCatalogBackendLocks serializes catalog initialization and mutations for
// one in-process backend. TreeDB excludes simultaneous multi-process opens for
// this mode; callers must construct catalogs against the same process-local DB
// handle.
var authCatalogBackendLocks struct {
	sync.Mutex
	byBackend map[uintptr]*authCatalogBackendState
}

type authCatalogBackendState struct {
	mu                   sync.Mutex
	authorizationVersion atomic.Uint64
}

type AuthCatalog struct {
	db              authCatalogStore
	backend         *authCatalogBackendState
	mu              sync.Mutex
	syntheticSecret [sha256.Size]byte
	authorization   atomic.Pointer[authAuthorizationSnapshot]
	// beforeSetEnabledWrite is a test-only interleaving hook. It is called
	// while mu is held so a rotation cannot race a stale record write.
	beforeSetEnabledWrite func()
}

func NewAuthCatalog(db authCatalogStore) (*AuthCatalog, error) {
	if db == nil {
		return nil, errors.New("mongo gateway auth: nil database")
	}
	backend := authCatalogBackendLock(db)
	secret, err := func() ([sha256.Size]byte, error) {
		backend.mu.Lock()
		defer backend.mu.Unlock()
		secret, err := loadOrCreateSyntheticSecret(db)
		if err != nil {
			return secret, err
		}
		raw, err := getAuthCatalogValue(db, authAuthorizationCatalogKey())
		if errors.Is(err, treedb.ErrKeyNotFound) {
			return secret, nil
		}
		if err == nil {
			_, err = decodeAuthorizationRecord(raw)
		}
		return secret, err
	}()
	if err != nil {
		return nil, fmt.Errorf("mongo gateway auth: catalog initialization: %w", err)
	}
	catalog := &AuthCatalog{db: db, backend: backend, syntheticSecret: secret}
	if _, snapshotErr := catalog.authorizationSnapshot(); snapshotErr != nil {
		return nil, fmt.Errorf("mongo gateway auth: authorization catalog: %w", snapshotErr)
	}
	return catalog, nil
}

func authCatalogBackendLock(db authCatalogStore) *authCatalogBackendState {
	v := reflect.ValueOf(db)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return &authCatalogBackendLocksFallback
	}
	key := v.Pointer()
	authCatalogBackendLocks.Lock()
	defer authCatalogBackendLocks.Unlock()
	if authCatalogBackendLocks.byBackend == nil {
		authCatalogBackendLocks.byBackend = make(map[uintptr]*authCatalogBackendState)
	}
	if lock := authCatalogBackendLocks.byBackend[key]; lock != nil {
		return lock
	}
	lock := &authCatalogBackendState{}
	authCatalogBackendLocks.byBackend[key] = lock
	return lock
}

var authCatalogBackendLocksFallback authCatalogBackendState

func authCatalogKey(authDB, username string) []byte {
	return []byte("\x00mongo-gateway/auth/v1/" + base64.RawURLEncoding.EncodeToString([]byte(authDB)) + "/" + base64.RawURLEncoding.EncodeToString([]byte(username)))
}

func authCatalogSyntheticSecretKey() []byte {
	return []byte("\x00mongo-gateway/auth/v1/synthetic-secret")
}

type authCatalogSyntheticSecretRecord struct {
	Version int    `json:"version"`
	Secret  []byte `json:"secret"`
}

func loadOrCreateSyntheticSecret(db authCatalogStore) ([sha256.Size]byte, error) {
	var secret [sha256.Size]byte
	raw, err := getAuthCatalogValue(db, authCatalogSyntheticSecretKey())
	if err == nil {
		return decodeSyntheticSecret(raw)
	}
	if err != nil && !errors.Is(err, treedb.ErrKeyNotFound) {
		return secret, errAuthenticationFailed
	}
	if _, err := rand.Read(secret[:]); err != nil {
		return secret, err
	}
	raw, err = json.Marshal(authCatalogSyntheticSecretRecord{Version: authCatalogVersion, Secret: secret[:]})
	if err != nil {
		return secret, err
	}
	if err := setAuthCatalogValueSync(db, authCatalogSyntheticSecretKey(), raw); err != nil {
		return secret, err
	}
	// Read back the durable value: a catalog never operates with a merely
	// in-memory fallback secret, and a later reopen observes this exact value.
	raw, err = getAuthCatalogValue(db, authCatalogSyntheticSecretKey())
	if err != nil {
		return secret, errAuthenticationFailed
	}
	return decodeSyntheticSecret(raw)
}

func getAuthCatalogValue(db authCatalogStore, key []byte) ([]byte, error) {
	if appendStore, ok := db.(authCatalogGetAppendStore); ok {
		return appendStore.GetAppend(key, nil)
	}
	return db.Get(key)
}

// setAuthCatalogValueSync keeps growing catalog values in TreeDB's persistent
// value log once they exceed the index's inline threshold. The durable index
// publication owns dependency ordering and releases the appender's pending GC
// pin after the pointer becomes reachable.
func setAuthCatalogValueSync(db authCatalogStore, key, value []byte) error {
	err := db.SetSync(key, value)
	if !errors.Is(err, batchpkg.ErrValueTooLarge) {
		return err
	}
	largeStore, ok := db.(authCatalogLargeValueStore)
	if !ok {
		return err
	}
	ptrs, appendErr := largeStore.AppendValueLogValues([][]byte{value})
	if appendErr != nil {
		return appendErr
	}
	// The DB publication path consumes this pin after it owns the pointer. A
	// guarded release is then a no-op; on every early/error path it abandons the
	// pointer so ValueLog GC is not blocked indefinitely.
	defer largeStore.ReleaseValueLogValues(ptrs)
	if len(ptrs) != 1 {
		return errors.New("mongo gateway auth: value-log append returned an invalid pointer count")
	}
	rawBatch := largeStore.NewBatch()
	batch, ok := rawBatch.(authCatalogPointerBatch)
	if !ok {
		_ = rawBatch.Close()
		return errors.New("mongo gateway auth: backend does not support persistent value-log pointers")
	}
	if err := batch.SetPointer(key, ptrs[0]); err != nil {
		_ = batch.Close()
		return err
	}
	writeErr := batch.WriteSync()
	closeErr := batch.Close()
	return errors.Join(writeErr, closeErr)
}

func decodeSyntheticSecret(raw []byte) ([sha256.Size]byte, error) {
	var secret [sha256.Size]byte
	var record authCatalogSyntheticSecretRecord
	if json.Unmarshal(raw, &record) != nil || record.Version != authCatalogVersion || len(record.Secret) != len(secret) {
		return secret, errAuthenticationFailed
	}
	copy(secret[:], record.Secret)
	return secret, nil
}

func validAuthField(value string) bool {
	return value != "" && len(value) <= maxAuthNameBytes && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validateAuthRecord(r AuthUserRecord) error {
	if r.Version != authCatalogVersion || !validAuthField(r.Username) || !validAuthField(r.AuthDB) || !validAuthIncarnation(r.Incarnation) || r.Iterations < minSCRAMIterations || r.Iterations > maxSCRAMIterations || len(r.Salt) < 16 || len(r.Salt) > maxSCRAMSaltBytes || len(r.StoredKey) != sha256.Size || len(r.ServerKey) != sha256.Size {
		return errAuthenticationFailed
	}
	return nil
}

func validAuthIncarnation(incarnation uint64) bool {
	return incarnation != 0
}

func newAuthIncarnation() (uint64, error) {
	var raw [8]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, fmt.Errorf("mongo gateway auth: random incarnation: %w", err)
		}
		if incarnation := binary.LittleEndian.Uint64(raw[:]); incarnation != 0 {
			return incarnation, nil
		}
	}
}

// UpsertPassword creates or rotates a verifier. The password bytes are used
// only for derivation and are not kept by this API or its persisted record.
func (c *AuthCatalog) UpsertPassword(authDB, username string, password []byte) error {
	raw, err := prepareSCRAMRecord(authDB, username, password)
	if c == nil || err != nil {
		return errAuthenticationFailed
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	var prepared AuthUserRecord
	if json.Unmarshal(raw, &prepared) != nil || validateAuthRecord(prepared) != nil {
		return errAuthenticationFailed
	}
	if stored, storedErr := c.storedRecordLocked(authDB, username); storedErr == nil {
		raw, err = bindPreparedSCRAMRecord(raw, stored.Incarnation)
		if err != nil {
			return err
		}
		prepared.Incarnation = stored.Incarnation
	} else {
		_, lookupErr := getAuthCatalogValue(c.db, authCatalogKey(authDB, username))
		if !errors.Is(lookupErr, treedb.ErrKeyNotFound) {
			return errAuthenticationFailed
		}
	}
	authorization, err := c.loadAuthorizationRecordLocked()
	if err != nil {
		return errAuthenticationFailed
	}
	added, err := c.ensureBootstrapAdminLocked(&authorization, authDB, username, prepared.Incarnation)
	if err != nil {
		return err
	}
	if err := setAuthCatalogValueSync(c.db, authCatalogKey(authDB, username), raw); err != nil {
		return err
	}
	if added {
		return c.publishAuthorizationRecordLocked(authorization)
	}
	return nil
}

func prepareSCRAMRecord(authDB, username string, password []byte) ([]byte, error) {
	prepared, err := saslprepPassword(password)
	if !validAuthField(authDB) || !validAuthField(username) || err != nil {
		return nil, errAuthenticationFailed
	}
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("mongo gateway auth: random salt: %w", err)
	}
	incarnation, err := newAuthIncarnation()
	if err != nil {
		return nil, err
	}
	r := newSCRAMRecord(authDB, username, prepared, salt, defaultSCRAMIterations, incarnation)
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func newSCRAMRecord(authDB, username string, password, salt []byte, iterations int, incarnation uint64) AuthUserRecord {
	salted := pbkdf2.Key(password, salt, iterations, sha256.Size, sha256.New)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	return AuthUserRecord{Version: authCatalogVersion, Username: username, AuthDB: authDB, Salt: append([]byte(nil), salt...), Iterations: iterations, StoredKey: stored[:], ServerKey: hmacSHA256(salted, []byte("Server Key")), Enabled: true, Incarnation: incarnation}
}

func bindPreparedSCRAMRecord(raw []byte, incarnation uint64) ([]byte, error) {
	var record AuthUserRecord
	if json.Unmarshal(raw, &record) != nil || !validAuthIncarnation(incarnation) {
		return nil, errAuthenticationFailed
	}
	record.Incarnation = incarnation
	return json.Marshal(record)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func (c *AuthCatalog) syntheticSCRAMRecord(authDB, username string) (AuthUserRecord, error) {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	context := []byte(authDB + "\x00" + username)
	salt := hmacSHA256(c.syntheticSecret[:], append([]byte("scram-synthetic-salt\x00"), context...))
	stored := hmacSHA256(c.syntheticSecret[:], append([]byte("scram-synthetic-stored\x00"), context...))
	server := hmacSHA256(c.syntheticSecret[:], append([]byte("scram-synthetic-server\x00"), context...))
	// Keep this equal to the real-user default in UpsertPassword: a divergent
	// server-first iteration count would expose invalid identities.
	incarnation := binary.LittleEndian.Uint64(hmacSHA256(c.syntheticSecret[:], append([]byte("scram-synthetic-incarnation\x00"), context...))[:8])
	if incarnation == 0 {
		incarnation = 1
	}
	return AuthUserRecord{Version: authCatalogVersion, Username: username, AuthDB: authDB, Salt: salt, Iterations: defaultSCRAMIterations, StoredKey: stored, ServerKey: server, Enabled: false, Incarnation: incarnation}, nil
}

func (c *AuthCatalog) record(authDB, username string) (AuthUserRecord, error) {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return AuthUserRecord{}, errAuthenticationFailed
	}
	c.backend.mu.Lock()
	raw, err := c.db.Get(authCatalogKey(authDB, username))
	c.backend.mu.Unlock()
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

func (c *AuthCatalog) userExists(authDB, username string) bool {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return false
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	_, err := c.storedRecordLocked(authDB, username)
	return err == nil
}

// SetEnabled atomically replaces the stored record with a changed enabled bit.
func (c *AuthCatalog) SetEnabled(authDB, username string, enabled bool) error {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return errAuthenticationFailed
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.storedRecordLocked(authDB, username)
	if err != nil {
		return err
	}
	if !enabled && r.Enabled {
		authorization, authErr := c.loadAuthorizationRecordLocked()
		if authErr != nil {
			return errAuthenticationFailed
		}
		for _, assignment := range authorization.Users {
			if assignment.AuthDB == authDB && assignment.Username == username && hasServerAdmin(assignment.Roles) && c.usableServerAdminsLocked(authorization) == 1 {
				return errCannotDisableLastServerAdministrator
			}
		}
	}
	r.Enabled = enabled
	if c.beforeSetEnabledWrite != nil {
		c.beforeSetEnabledWrite()
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return setAuthCatalogValueSync(c.db, authCatalogKey(authDB, username), raw)
}

// DropUser durably removes one verifier and its role assignment. It refuses
// to remove the last enabled server administrator.
func (c *AuthCatalog) DropUser(authDB, username string) error {
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return errAuthenticationFailed
	}
	deleteStore, ok := c.db.(authCatalogDeleteStore)
	if !ok {
		return errors.New("mongo gateway auth: backend does not support durable user deletion")
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	stored, err := c.storedRecordLocked(authDB, username)
	if err != nil {
		return err
	}
	record, err := c.loadAuthorizationRecordLocked()
	if err != nil {
		return errAuthenticationFailed
	}
	index := -1
	for i := range record.Users {
		if record.Users[i].AuthDB == authDB && record.Users[i].Username == username {
			index = i
			break
		}
	}
	if index >= 0 && stored.Enabled && hasServerAdmin(record.Users[index].Roles) && c.usableServerAdminsLocked(record) == 1 {
		return errCannotDropLastServerAdministrator
	}
	if index >= 0 {
		record.Users = append(record.Users[:index], record.Users[index+1:]...)
		if err := c.publishAuthorizationRecordLocked(record); err != nil {
			return err
		}
	}
	return deleteStore.DeleteSync(authCatalogKey(authDB, username))
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
	trial := newSCRAMRecord(authDB, username, prepared, r.Salt, r.Iterations, r.Incarnation)
	if subtle.ConstantTimeCompare(trial.StoredKey, r.StoredKey) != 1 {
		return AuthUser{}, errAuthenticationFailed
	}
	return AuthUser{Username: username, AuthDB: authDB, Incarnation: r.Incarnation}, nil
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
