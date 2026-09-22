package mongogateway

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	authAuthorizationVersion = 2
	maxAuthorizationUsers    = 4096
	maxRolesPerUser          = 64
)

// AuthRole is one of the gateway's deliberately small built-in roles.
type AuthRole string

const (
	AuthRoleRead        AuthRole = "read"
	AuthRoleReadWrite   AuthRole = "readWrite"
	AuthRoleDBAdmin     AuthRole = "dbAdmin"
	AuthRoleUserAdmin   AuthRole = "userAdmin"
	AuthRoleServerAdmin AuthRole = "serverAdmin"
)

// AuthRoleGrant binds a built-in role to a server, database, or collection.
// Empty Database and Collection mean server scope; a non-empty Collection
// requires a non-empty Database.
type AuthRoleGrant struct {
	Role       AuthRole `json:"role"`
	Database   string   `json:"database,omitempty"`
	Collection string   `json:"collection,omitempty"`
}

type authRoleAssignment struct {
	Username    string          `json:"username"`
	AuthDB      string          `json:"auth_db"`
	Incarnation uint64          `json:"id"`
	Roles       []AuthRoleGrant `json:"roles"`
}

type authAuthorizationRecord struct {
	Version int                  `json:"version"`
	Users   []authRoleAssignment `json:"users"`
}

type authAuthorizationSnapshot struct {
	version uint64
	users   map[string]map[string]authAuthorizationIdentity
}

type authAuthorizationIdentity struct {
	incarnation uint64
	roles       []AuthRoleGrant
}

type AuthorizationMetrics struct {
	Allowed uint64
	Denied  uint64
}

func authAuthorizationCatalogKey() []byte {
	return []byte("\x00mongo-gateway/authorization/v1/catalog")
}

func authIdentityKey(authDB, username string) string { return authDB + "\x00" + username }

func validAuthRole(role AuthRole) bool {
	switch role {
	case AuthRoleRead, AuthRoleReadWrite, AuthRoleDBAdmin, AuthRoleUserAdmin, AuthRoleServerAdmin:
		return true
	default:
		return false
	}
}

func validateAuthRoleGrant(grant AuthRoleGrant) error {
	if !validAuthRole(grant.Role) {
		return fmt.Errorf("unknown role %q", grant.Role)
	}
	if grant.Database != "" && !validAuthField(grant.Database) {
		return errors.New("invalid role database")
	}
	if grant.Collection != "" && (grant.Database == "" || !validAuthField(grant.Collection)) {
		return errors.New("invalid role collection")
	}
	if grant.Role == AuthRoleServerAdmin && (grant.Database != "" || grant.Collection != "") {
		return errors.New("serverAdmin must use server scope")
	}
	return nil
}

func canonicalAuthRoles(roles []AuthRoleGrant) ([]AuthRoleGrant, error) {
	if len(roles) > maxRolesPerUser {
		return nil, errors.New("too many role grants")
	}
	result := append([]AuthRoleGrant(nil), roles...)
	for _, role := range result {
		if err := validateAuthRoleGrant(role); err != nil {
			return nil, err
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Role != result[j].Role {
			return result[i].Role < result[j].Role
		}
		if result[i].Database != result[j].Database {
			return result[i].Database < result[j].Database
		}
		return result[i].Collection < result[j].Collection
	})
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, errors.New("duplicate role grant")
		}
	}
	return result, nil
}

func decodeAuthorizationRecord(raw []byte) (authAuthorizationRecord, error) {
	var record authAuthorizationRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil || decoder.Decode(&struct{}{}) != io.EOF || record.Version != authAuthorizationVersion || len(record.Users) == 0 || len(record.Users) > maxAuthorizationUsers {
		return authAuthorizationRecord{}, errAuthenticationFailed
	}
	seen := make(map[string]struct{}, len(record.Users))
	for i := range record.Users {
		user := &record.Users[i]
		if !validAuthField(user.Username) || !validAuthField(user.AuthDB) || !validAuthIncarnation(user.Incarnation) {
			return authAuthorizationRecord{}, errAuthenticationFailed
		}
		key := authIdentityKey(user.AuthDB, user.Username)
		if _, duplicate := seen[key]; duplicate {
			return authAuthorizationRecord{}, errAuthenticationFailed
		}
		seen[key] = struct{}{}
		roles, err := canonicalAuthRoles(user.Roles)
		if err != nil {
			return authAuthorizationRecord{}, errAuthenticationFailed
		}
		user.Roles = roles
	}
	return record, nil
}

func (c *AuthCatalog) loadAuthorizationRecordLocked() (authAuthorizationRecord, error) {
	raw, err := getAuthCatalogValue(c.db, authAuthorizationCatalogKey())
	if errors.Is(err, treedb.ErrKeyNotFound) {
		return authAuthorizationRecord{Version: authAuthorizationVersion}, nil
	}
	if err != nil {
		return authAuthorizationRecord{}, err
	}
	return decodeAuthorizationRecord(raw)
}

func authorizationSnapshot(record authAuthorizationRecord, version uint64) *authAuthorizationSnapshot {
	users := make(map[string]map[string]authAuthorizationIdentity)
	for _, user := range record.Users {
		byName := users[user.AuthDB]
		if byName == nil {
			byName = make(map[string]authAuthorizationIdentity)
			users[user.AuthDB] = byName
		}
		byName[user.Username] = authAuthorizationIdentity{incarnation: user.Incarnation, roles: append([]AuthRoleGrant(nil), user.Roles...)}
	}
	return &authAuthorizationSnapshot{version: version, users: users}
}

func (c *AuthCatalog) publishAuthorizationRecordLocked(record authAuthorizationRecord) error {
	sort.Slice(record.Users, func(i, j int) bool {
		if record.Users[i].AuthDB != record.Users[j].AuthDB {
			return record.Users[i].AuthDB < record.Users[j].AuthDB
		}
		return record.Users[i].Username < record.Users[j].Username
	})
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := setAuthCatalogValueSync(c.db, authAuthorizationCatalogKey(), raw); err != nil {
		// A storage error may be an uncertain publication outcome. Invalidate the
		// immutable hot-path snapshot so no previously broader grant survives an
		// ambiguous durable revoke; the next command must reload or deny closed.
		c.backend.authorizationVersion.Add(1)
		c.authorization.Store(nil)
		return err
	}
	version := c.backend.authorizationVersion.Add(1)
	c.authorization.Store(authorizationSnapshot(record, version))
	return nil
}

func (c *AuthCatalog) authorizationSnapshot() (*authAuthorizationSnapshot, error) {
	if c == nil {
		return nil, errAuthenticationFailed
	}
	version := c.backend.authorizationVersion.Load()
	if snapshot := c.authorization.Load(); snapshot != nil && snapshot.version == version {
		return snapshot, nil
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	version = c.backend.authorizationVersion.Load()
	if snapshot := c.authorization.Load(); snapshot != nil && snapshot.version == version {
		return snapshot, nil
	}
	record, err := c.loadAuthorizationRecordLocked()
	if err != nil {
		return nil, errAuthenticationFailed
	}
	snapshot := authorizationSnapshot(record, version)
	c.authorization.Store(snapshot)
	return snapshot, nil
}

// UserRoles returns an owned copy of the durable role grants for one identity.
func (c *AuthCatalog) UserRoles(authDB, username string) ([]AuthRoleGrant, error) {
	roles, err := c.effectiveRoles(authDB, username)
	if err != nil {
		return nil, err
	}
	return append([]AuthRoleGrant(nil), roles...), nil
}

// effectiveRoles returns immutable snapshot storage for in-package hot paths.
// Callers must never retain it beyond a command or mutate it.
func (c *AuthCatalog) effectiveRoles(authDB, username string) ([]AuthRoleGrant, error) {
	if !validAuthField(authDB) || !validAuthField(username) {
		return nil, errAuthenticationFailed
	}
	snapshot, err := c.authorizationSnapshot()
	if err != nil {
		return nil, err
	}
	identity, ok := snapshot.users[authDB][username]
	if !ok {
		return nil, nil
	}
	return identity.roles, nil
}

func (c *AuthCatalog) effectiveRolesForUser(user AuthUser) ([]AuthRoleGrant, error) {
	if !validAuthField(user.AuthDB) || !validAuthField(user.Username) || !validAuthIncarnation(user.Incarnation) {
		return nil, errAuthenticationFailed
	}
	snapshot, err := c.authorizationSnapshot()
	if err != nil {
		return nil, err
	}
	identity, ok := snapshot.users[user.AuthDB][user.Username]
	if !ok || identity.incarnation != user.Incarnation {
		return nil, errAuthenticationFailed
	}
	return identity.roles, nil
}

func (c *AuthCatalog) identityActive(user AuthUser) bool {
	_, err := c.effectiveRolesForUser(user)
	return err == nil
}

// SetUserRoles durably replaces one user's grants. It refuses to demote the
// last enabled server administrator.
func (c *AuthCatalog) SetUserRoles(authDB, username string, roles []AuthRoleGrant) error {
	roles, err := canonicalAuthRoles(roles)
	if c == nil || !validAuthField(authDB) || !validAuthField(username) || err != nil {
		return errAuthenticationFailed
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
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
	if index >= 0 && stored.Enabled && hasServerAdmin(record.Users[index].Roles) && !hasServerAdmin(roles) && c.usableServerAdminsLocked(record) == 1 {
		return errCannotDemoteLastServerAdministrator
	}
	if index < 0 {
		if len(record.Users) >= maxAuthorizationUsers {
			return errors.New("mongo gateway authorization: user limit exceeded")
		}
		record.Users = append(record.Users, authRoleAssignment{Username: username, AuthDB: authDB, Incarnation: stored.Incarnation, Roles: roles})
	} else {
		if record.Users[index].Incarnation != stored.Incarnation {
			return errAuthenticationFailed
		}
		record.Users[index].Roles = roles
	}
	return c.publishAuthorizationRecordLocked(record)
}

func authIdentityInRecord(record authAuthorizationRecord, authDB, username string) (authRoleAssignment, int) {
	for i := range record.Users {
		if record.Users[i].AuthDB == authDB && record.Users[i].Username == username {
			return record.Users[i], i
		}
	}
	return authRoleAssignment{}, -1
}

// canManageUserGrants is the complete built-in userAdmin scope policy. The
// account's auth database must be covered independently of its grants. An
// empty grant set has no collection anchor and therefore requires database- or
// server-scoped userAdmin. serverAdmin grants remain reserved to serverAdmin.
func canManageUserGrants(actorRoles []AuthRoleGrant, commandDB string, grants []AuthRoleGrant) bool {
	for _, role := range actorRoles {
		if role.Role == AuthRoleServerAdmin {
			return true
		}
	}
	commandDBCovered := false
	emptyGrantSetCovered := false
	for _, role := range actorRoles {
		if role.Role != AuthRoleUserAdmin || (role.Database != "" && role.Database != commandDB) {
			continue
		}
		commandDBCovered = true
		if role.Collection == "" {
			emptyGrantSetCovered = true
		}
	}
	if !commandDBCovered {
		return false
	}
	if len(grants) == 0 {
		return emptyGrantSetCovered
	}
	for _, grant := range grants {
		if grant.Role == AuthRoleServerAdmin {
			return false
		}
		allowed := false
		for _, role := range actorRoles {
			if role.Role != AuthRoleUserAdmin {
				continue
			}
			if grant.Database == "" {
				allowed = role.Database == ""
			} else {
				allowed = roleScopeMatches(role, grant.Database, grant.Collection)
			}
			if allowed {
				break
			}
		}
		if !allowed {
			return false
		}
	}
	return true
}

// createUser applies a wire createUser request under the catalog's backend
// lock. It intentionally bypasses UpsertPassword's trusted-tooling bootstrap:
// an explicit network role set must never transiently acquire serverAdmin.
func (c *AuthCatalog) createUser(actor AuthUser, authDB, username string, password []byte, roles []AuthRoleGrant) error {
	prepared, err := prepareSCRAMRecord(authDB, username, password)
	if err != nil {
		return err
	}
	roles, err = canonicalAuthRoles(roles)
	if c == nil || err != nil {
		return errAuthenticationFailed
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	record, err := c.loadAuthorizationRecordLocked()
	if err != nil {
		return errAuthenticationFailed
	}
	var preparedRecord AuthUserRecord
	if json.Unmarshal(prepared, &preparedRecord) != nil || validateAuthRecord(preparedRecord) != nil {
		return errAuthenticationFailed
	}
	actorAssignment, actorIndex := authIdentityInRecord(record, actor.AuthDB, actor.Username)
	if actorIndex < 0 || actorAssignment.Incarnation != actor.Incarnation || !canManageUserGrants(actorAssignment.Roles, authDB, roles) {
		return errUserManagementUnauthorized
	}
	targetAssignment, targetIndex := authIdentityInRecord(record, authDB, username)
	if targetIndex >= 0 {
		if !canManageUserGrants(actorAssignment.Roles, authDB, targetAssignment.Roles) {
			return errUserManagementUnauthorized
		}
		return errAuthUserExists
	}
	if len(record.Users) >= maxAuthorizationUsers {
		return errors.New("mongo gateway authorization: user limit exceeded")
	}
	raw, lookupErr := getAuthCatalogValue(c.db, authCatalogKey(authDB, username))
	if lookupErr == nil && len(raw) != 0 {
		// An orphan verifier has no trustworthy grant anchor. Only serverAdmin,
		// which can manage every valid current grant, may observe the duplicate.
		if !hasServerAdmin(actorAssignment.Roles) {
			return errUserManagementUnauthorized
		}
		return errAuthUserExists
	} else if lookupErr != nil && !errors.Is(lookupErr, treedb.ErrKeyNotFound) {
		return errAuthenticationFailed
	}
	if err := setAuthCatalogValueSync(c.db, authCatalogKey(authDB, username), prepared); err != nil {
		return err
	}
	record.Users = append(record.Users, authRoleAssignment{Username: username, AuthDB: authDB, Incarnation: preparedRecord.Incarnation, Roles: roles})
	return c.publishAuthorizationRecordLocked(record)
}

// updateUser preflights current and requested grants under the same backend
// lock as its durable writes. For a mixed update the verifier is written first;
// if the role write then fails, the old credential is invalid and the new
// credential retains the old grants.
func (c *AuthCatalog) updateUser(actor AuthUser, authDB, username string, password []byte, updatePassword bool, roles []AuthRoleGrant, updateRoles bool) error {
	var prepared []byte
	var err error
	if updatePassword {
		prepared, err = prepareSCRAMRecord(authDB, username, password)
		if err != nil {
			return err
		}
	}
	if updateRoles {
		roles, err = canonicalAuthRoles(roles)
		if err != nil {
			return errAuthenticationFailed
		}
	}
	if c == nil || !validAuthField(authDB) || !validAuthField(username) {
		return errAuthenticationFailed
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	record, err := c.loadAuthorizationRecordLocked()
	if err != nil {
		return errAuthenticationFailed
	}
	actorAssignment, actorIndex := authIdentityInRecord(record, actor.AuthDB, actor.Username)
	targetAssignment, targetIndex := authIdentityInRecord(record, authDB, username)
	currentRoles := targetAssignment.Roles
	if actorIndex < 0 || actorAssignment.Incarnation != actor.Incarnation {
		return errUserManagementUnauthorized
	}
	if targetIndex < 0 {
		// A missing assignment is indistinguishable from an existing identity
		// whose current grants this actor cannot manage. Only serverAdmin covers
		// every possible valid current grant and may receive UserNotFound.
		if !hasServerAdmin(actorAssignment.Roles) {
			return errUserManagementUnauthorized
		}
		return errAuthUserNotFound
	}
	if !canManageUserGrants(actorAssignment.Roles, authDB, currentRoles) || (updateRoles && !canManageUserGrants(actorAssignment.Roles, authDB, roles)) {
		return errUserManagementUnauthorized
	}
	stored, err := c.storedRecordLocked(authDB, username)
	if err != nil || targetAssignment.Incarnation != stored.Incarnation {
		return errAuthenticationFailed
	}
	if updateRoles && targetIndex >= 0 && stored.Enabled && hasServerAdmin(currentRoles) && !hasServerAdmin(roles) && c.usableServerAdminsLocked(record) == 1 {
		return errCannotDemoteLastServerAdministrator
	}
	if updatePassword {
		prepared, err = bindPreparedSCRAMRecord(prepared, stored.Incarnation)
		if err != nil {
			return err
		}
		if err := setAuthCatalogValueSync(c.db, authCatalogKey(authDB, username), prepared); err != nil {
			return err
		}
	}
	if !updateRoles {
		return nil
	}
	record.Users[targetIndex].Roles = roles
	return c.publishAuthorizationRecordLocked(record)
}

func (c *AuthCatalog) dropUser(actor AuthUser, authDB, username string) error {
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
	record, err := c.loadAuthorizationRecordLocked()
	if err != nil {
		return errAuthenticationFailed
	}
	actorAssignment, actorIndex := authIdentityInRecord(record, actor.AuthDB, actor.Username)
	targetAssignment, targetIndex := authIdentityInRecord(record, authDB, username)
	currentRoles := targetAssignment.Roles
	if actorIndex < 0 || actorAssignment.Incarnation != actor.Incarnation {
		return errUserManagementUnauthorized
	}
	if targetIndex < 0 {
		if !hasServerAdmin(actorAssignment.Roles) {
			return errUserManagementUnauthorized
		}
		return errAuthUserNotFound
	}
	if !canManageUserGrants(actorAssignment.Roles, authDB, currentRoles) {
		return errUserManagementUnauthorized
	}
	stored, err := c.storedRecordLocked(authDB, username)
	if err != nil || targetAssignment.Incarnation != stored.Incarnation {
		return errAuthenticationFailed
	}
	if targetIndex >= 0 && stored.Enabled && hasServerAdmin(currentRoles) && c.usableServerAdminsLocked(record) == 1 {
		return errCannotDropLastServerAdministrator
	}
	if targetIndex >= 0 {
		record.Users = append(record.Users[:targetIndex], record.Users[targetIndex+1:]...)
		if err := c.publishAuthorizationRecordLocked(record); err != nil {
			return err
		}
	}
	return deleteStore.DeleteSync(authCatalogKey(authDB, username))
}

func hasServerAdmin(roles []AuthRoleGrant) bool {
	for _, role := range roles {
		if role.Role == AuthRoleServerAdmin && role.Database == "" && role.Collection == "" {
			return true
		}
	}
	return false
}

func (c *AuthCatalog) usableServerAdminsLocked(record authAuthorizationRecord) int {
	count := 0
	for _, assignment := range record.Users {
		if !hasServerAdmin(assignment.Roles) {
			continue
		}
		stored, err := c.storedRecordLocked(assignment.AuthDB, assignment.Username)
		if err == nil && stored.Enabled && stored.Incarnation == assignment.Incarnation {
			count++
		}
	}
	return count
}

func (c *AuthCatalog) ensureBootstrapAdminLocked(record *authAuthorizationRecord, authDB, username string, incarnation uint64) (bool, error) {
	target, targetIndex := authIdentityInRecord(*record, authDB, username)
	if !validAuthIncarnation(incarnation) || (targetIndex >= 0 && target.Incarnation != incarnation) {
		return false, errAuthenticationFailed
	}
	if c.usableServerAdminsLocked(*record) > 0 {
		if targetIndex >= 0 {
			return false, nil
		}
		if len(record.Users) >= maxAuthorizationUsers {
			return false, errors.New("mongo gateway authorization: user limit exceeded")
		}
		record.Users = append(record.Users, authRoleAssignment{Username: username, AuthDB: authDB, Incarnation: incarnation, Roles: []AuthRoleGrant{}})
		return true, nil
	}
	// Trusted bootstrap may create the first usable administrator, but it does
	// not recover a non-empty catalog whose administrator records became
	// unusable; that requires offline repair rather than privilege escalation.
	if len(record.Users) != 0 {
		return false, errNoUsableServerAdministrator
	}
	if len(record.Users) >= maxAuthorizationUsers {
		return false, errors.New("mongo gateway authorization: user limit exceeded")
	}
	assignment := authRoleAssignment{Username: username, AuthDB: authDB, Incarnation: incarnation, Roles: []AuthRoleGrant{{Role: AuthRoleServerAdmin}}}
	record.Users = append(record.Users, assignment)
	return true, nil
}

func (c *AuthCatalog) authorizationUsers() ([]authRoleAssignment, error) {
	snapshot, err := c.authorizationSnapshot()
	if err != nil {
		return nil, err
	}
	users := make([]authRoleAssignment, 0)
	for authDB, byName := range snapshot.users {
		for username, identity := range byName {
			users = append(users, authRoleAssignment{
				AuthDB:      authDB,
				Username:    username,
				Incarnation: identity.incarnation,
				Roles:       append([]AuthRoleGrant(nil), identity.roles...),
			})
		}
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].AuthDB != users[j].AuthDB {
			return users[i].AuthDB < users[j].AuthDB
		}
		return users[i].Username < users[j].Username
	})
	return users, nil
}

type authorizationPrivilege uint8

const (
	authorizationUnsupported authorizationPrivilege = iota
	authorizationPublic
	authorizationRead
	authorizationMetadataRead
	authorizationWrite
	authorizationDBAdmin
	authorizationUserAdmin
	authorizationServerAdmin
	authorizationListCollections
	authorizationListDatabases
)

type authorizationTarget struct {
	privilege     authorizationPrivilege
	database      string
	collection    string
	databaseRaw   []byte
	collectionRaw []byte
}

// commandStringBytes returns a view into the immutable command document. It
// avoids materializing strings on the admission hot path; handlers decode
// their fields normally after authorization succeeds.
func commandStringBytes(doc wire.Document, key string) ([]byte, error) {
	value := bson.Raw(doc).Lookup(key)
	if value.IsZero() {
		return nil, fmt.Errorf("Mongo command missing %q", key)
	}
	if value.Type != bson.TypeString || len(value.Value) < 5 {
		return nil, fmt.Errorf("Mongo command field %q must be a string", key)
	}
	length := uint64(binary.LittleEndian.Uint32(value.Value[:4]))
	if length < 1 || length > uint64(len(value.Value)-4) || value.Value[4+length-1] != 0 {
		return nil, fmt.Errorf("Mongo command field %q must be a string", key)
	}
	if length == 1 {
		return nil, fmt.Errorf("Mongo command field %q cannot be empty", key)
	}
	return value.Value[4 : 4+length-1], nil
}

func commandAuthorizationTarget(name string, command wire.Document) (authorizationTarget, error) {
	target := authorizationTarget{}
	if name == "explain" {
		inner, err := requiredDocumentField(command, "explain")
		if err != nil {
			return target, err
		}
		innerName, err := mongoCommandName(inner)
		if err != nil {
			return target, err
		}
		if !supportedExplainReadCommand(innerName) {
			return target, errors.New("Mongo gateway explain supports bounded standalone read commands only")
		}
		if bson.Raw(inner).Lookup("$db").IsZero() {
			db, err := commandStringBytes(command, "$db")
			if err != nil {
				return target, err
			}
			collection, err := commandStringBytes(inner, innerName)
			if err != nil {
				return target, err
			}
			return authorizationTarget{privilege: authorizationRead, databaseRaw: db, collectionRaw: collection}, nil
		}
		return commandAuthorizationTarget(innerName, inner)
	}
	switch name {
	case "hello", "isMaster", "ismaster", "saslStart", "saslContinue", "connectionStatus", "buildInfo", "ping", "endSessions":
		target.privilege = authorizationPublic
		return target, nil
	case "hostInfo":
		target.privilege = authorizationServerAdmin
		return target, nil
	case "serverStatus", "top":
		target.privilege = authorizationServerAdmin
		return target, nil
	case "listDatabases":
		target.privilege = authorizationListDatabases
		return target, nil
	case "listCollections":
		target.privilege = authorizationListCollections
	case "find", "aggregate", "count", "distinct", "getMore", "killCursors":
		target.privilege = authorizationRead
	case "listIndexes":
		target.privilege = authorizationMetadataRead
	case "dbStats":
		target.privilege = authorizationMetadataRead
		db, err := commandStringBytes(command, "$db")
		if err != nil {
			return target, err
		}
		target.databaseRaw = db
		return target, nil
	case "collStats":
		target.privilege = authorizationMetadataRead
	case "insert", "update", "delete", "findAndModify":
		target.privilege = authorizationWrite
	case "create", "createIndexes", "dropIndexes":
		target.privilege = authorizationDBAdmin
	case "createUser", "updateUser", "dropUser", "usersInfo":
		target.privilege = authorizationUserAdmin
	default:
		return target, nil
	}
	db, err := commandStringBytes(command, "$db")
	if err != nil {
		return target, err
	}
	target.databaseRaw = db
	if target.privilege == authorizationListCollections || target.privilege == authorizationUserAdmin {
		return target, nil
	}
	field := name
	if name == "getMore" {
		field = "collection"
	}
	collection, err := commandStringBytes(command, field)
	if err != nil {
		return target, err
	}
	target.collectionRaw = collection
	return target, nil
}

func roleScopeMatches(grant AuthRoleGrant, database, collection string) bool {
	if grant.Database == "" {
		return true
	}
	if grant.Database != database {
		return false
	}
	return grant.Collection == "" || grant.Collection == collection
}

func resourceNameMatches(name string, raw []byte, decoded string) bool {
	if raw == nil {
		return name == decoded
	}
	if len(name) != len(raw) {
		return false
	}
	for i := range raw {
		if name[i] != raw[i] {
			return false
		}
	}
	return true
}

func roleScopeMatchesTarget(grant AuthRoleGrant, target authorizationTarget) bool {
	if grant.Database == "" {
		return true
	}
	if !resourceNameMatches(grant.Database, target.databaseRaw, target.database) {
		return false
	}
	return grant.Collection == "" || resourceNameMatches(grant.Collection, target.collectionRaw, target.collection)
}

func roleAllows(grant AuthRoleGrant, target authorizationTarget) bool {
	if target.privilege == authorizationUnsupported {
		return false
	}
	if grant.Role == AuthRoleServerAdmin {
		return true
	}
	if target.privilege == authorizationServerAdmin {
		return false
	}
	if target.privilege == authorizationUserAdmin {
		return grant.Role == AuthRoleUserAdmin && (grant.Database == "" || resourceNameMatches(grant.Database, target.databaseRaw, target.database))
	}
	if target.privilege == authorizationListCollections || target.privilege == authorizationListDatabases {
		targetHasDatabase := target.databaseRaw != nil || target.database != ""
		if grant.Database != "" && targetHasDatabase && !resourceNameMatches(grant.Database, target.databaseRaw, target.database) {
			return false
		}
	} else if !roleScopeMatchesTarget(grant, target) {
		return false
	}
	switch target.privilege {
	case authorizationRead:
		return grant.Role == AuthRoleRead || grant.Role == AuthRoleReadWrite
	case authorizationMetadataRead, authorizationListCollections:
		return grant.Role == AuthRoleRead || grant.Role == AuthRoleReadWrite || grant.Role == AuthRoleDBAdmin
	case authorizationListDatabases:
		return grant.Role == AuthRoleRead || grant.Role == AuthRoleReadWrite || grant.Role == AuthRoleDBAdmin || grant.Role == AuthRoleUserAdmin
	case authorizationWrite:
		return grant.Role == AuthRoleReadWrite
	case authorizationDBAdmin:
		return grant.Role == AuthRoleDBAdmin
	case authorizationUserAdmin:
		return grant.Role == AuthRoleUserAdmin
	}
	return false
}

func (s *Server) authorizeCommand(name string, command wire.Document, owner int64) (authorizationTarget, wire.Document, error, bool) {
	target, err := commandAuthorizationTarget(name, command)
	if err != nil {
		doc, commandErr := commandError(commandCodeFailedToParse, "FailedToParse", err.Error())
		return target, doc, commandErr, false
	}
	if target.privilege == authorizationPublic {
		return target, nil, nil, true
	}
	if s.AuthCatalog == nil || s.clusterSubmitterConfigured() {
		s.authorizationDenied.Add(1)
		doc, commandErr := commandError(13, "Unauthorized", "not authorized for command "+name)
		return target, doc, commandErr, false
	}
	user := s.authUserSnapshot(owner)
	if user == nil {
		s.authorizationDenied.Add(1)
		doc, commandErr := commandError(13, "Unauthorized", "Authentication required")
		return target, doc, commandErr, false
	}
	roles, err := s.AuthCatalog.effectiveRolesForUser(*user)
	if err == nil {
		for _, role := range roles {
			if roleAllows(role, target) {
				s.authorizationAllowed.Add(1)
				return target, nil, nil, true
			}
		}
	}
	s.authorizationDenied.Add(1)
	doc, commandErr := commandError(13, "Unauthorized", "not authorized for command "+name)
	return target, doc, commandErr, false
}

func (s *Server) authorizedResource(owner int64, database, collection string, privilege authorizationPrivilege) bool {
	if !s.authenticationRequired() {
		return true
	}
	if s.AuthCatalog == nil {
		return false
	}
	user := s.authUserSnapshot(owner)
	if user == nil {
		return false
	}
	roles, err := s.AuthCatalog.effectiveRolesForUser(*user)
	if err != nil {
		return false
	}
	target := authorizationTarget{privilege: privilege, database: database, collection: collection}
	for _, role := range roles {
		if roleAllows(role, target) {
			return true
		}
	}
	return false
}

func (s *Server) AuthorizationMetrics() AuthorizationMetrics {
	if s == nil {
		return AuthorizationMetrics{}
	}
	return AuthorizationMetrics{Allowed: s.authorizationAllowed.Load(), Denied: s.authorizationDenied.Load()}
}

func authRoleDocuments(roles []AuthRoleGrant) bson.A {
	result := make(bson.A, 0, len(roles))
	for _, role := range roles {
		scope := "server"
		if role.Database != "" {
			scope = "database"
		}
		if role.Collection != "" {
			scope = "collection"
		}
		result = append(result, bson.D{{Key: "role", Value: string(role.Role)}, {Key: "db", Value: role.Database}, {Key: "collection", Value: role.Collection}, {Key: "scope", Value: scope}})
	}
	return result
}

func authPrivilegeDocuments(roles []AuthRoleGrant) bson.A {
	result := make(bson.A, 0, len(roles))
	for _, role := range roles {
		resource := bson.D{{Key: "db", Value: role.Database}, {Key: "collection", Value: role.Collection}}
		result = append(result, bson.D{{Key: "resource", Value: resource}, {Key: "actions", Value: bson.A{string(role.Role)}}})
	}
	return result
}

func parseAuthRoleGrants(command wire.Document, commandDB string) ([]AuthRoleGrant, error) {
	array, ok := bson.Raw(command).Lookup("roles").ArrayOK()
	if !ok {
		return nil, errors.New("roles must be an array")
	}
	values, err := array.Values()
	if err != nil {
		return nil, errors.New("roles must be an array")
	}
	roles := make([]AuthRoleGrant, 0, len(values))
	for _, value := range values {
		if role, ok := value.StringValueOK(); ok {
			grant := AuthRoleGrant{Role: AuthRole(role), Database: commandDB}
			if grant.Role == AuthRoleServerAdmin {
				grant.Database = ""
			}
			roles = append(roles, grant)
			continue
		}
		raw, ok := value.DocumentOK()
		if !ok {
			return nil, errors.New("roles entries must be strings or documents")
		}
		role, ok := raw.Lookup("role").StringValueOK()
		if !ok {
			return nil, errors.New("role name must be a string")
		}
		db := commandDB
		if value, exists := raw.LookupErr("db"); exists == nil {
			var ok bool
			db, ok = value.StringValueOK()
			if !ok {
				return nil, errors.New("role db must be a string")
			}
		}
		collection := ""
		if value, exists := raw.LookupErr("collection"); exists == nil {
			var ok bool
			collection, ok = value.StringValueOK()
			if !ok {
				return nil, errors.New("role collection must be a string")
			}
		}
		roles = append(roles, AuthRoleGrant{Role: AuthRole(role), Database: db, Collection: collection})
	}
	return canonicalAuthRoles(roles)
}

func (s *Server) principalCanManageGrants(owner int64, commandDB string, grants []AuthRoleGrant) bool {
	user := s.authUserSnapshot(owner)
	if user == nil || s.AuthCatalog == nil {
		return false
	}
	roles, err := s.AuthCatalog.effectiveRolesForUser(*user)
	if err != nil {
		return false
	}
	return canManageUserGrants(roles, commandDB, grants)
}

func (s *Server) principalCanManageTarget(owner int64, authDB, username string) bool {
	targetRoles, err := s.AuthCatalog.effectiveRoles(authDB, username)
	return err == nil && s.principalCanManageGrants(owner, authDB, targetRoles)
}

func (s *Server) userManagementResponse(name string, command wire.Document, owner int64) (wire.Document, error) {
	if s.AuthCatalog == nil {
		return commandError(13, "Unauthorized", "authorization catalog unavailable")
	}
	actor := s.authUserSnapshot(owner)
	if actor == nil {
		return commandError(13, "Unauthorized", "Authentication required")
	}
	database, err := commandString(command, "$db")
	if err != nil {
		return commandError(9, "FailedToParse", err.Error())
	}
	switch name {
	case "createUser":
		username, err := commandString(command, name)
		if err != nil {
			return commandError(9, "FailedToParse", err.Error())
		}
		password, ok := bson.Raw(command).Lookup("pwd").StringValueOK()
		if !ok {
			return commandError(9, "FailedToParse", "createUser requires a string pwd")
		}
		roles, err := parseAuthRoleGrants(command, database)
		if err != nil {
			return commandError(2, "BadValue", err.Error())
		}
		if !s.principalCanManageGrants(owner, database, roles) {
			return commandError(13, "Unauthorized", "not authorized to grant requested roles")
		}
		if err := s.AuthCatalog.createUser(*actor, database, username, []byte(password), roles); err != nil {
			if errors.Is(err, errAuthUserExists) {
				return commandError(51003, "DuplicateKey", "user already exists")
			}
			if errors.Is(err, errUserManagementUnauthorized) {
				return commandError(13, "Unauthorized", err.Error())
			}
			return commandError(1, "InternalError", err.Error())
		}
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})

	case "updateUser":
		username, err := commandString(command, name)
		if err != nil {
			return commandError(9, "FailedToParse", err.Error())
		}
		raw := bson.Raw(command)
		passwordValue := raw.Lookup("pwd")
		rolesValue := raw.Lookup("roles")
		if passwordValue.IsZero() && rolesValue.IsZero() {
			return commandError(9, "FailedToParse", "updateUser requires pwd or roles")
		}
		var roles []AuthRoleGrant
		if !rolesValue.IsZero() {
			roles, err = parseAuthRoleGrants(command, database)
			if err != nil {
				return commandError(2, "BadValue", err.Error())
			}
			if !s.principalCanManageGrants(owner, database, roles) {
				return commandError(13, "Unauthorized", "not authorized to grant requested roles")
			}
		}
		var password string
		if !passwordValue.IsZero() {
			var ok bool
			password, ok = passwordValue.StringValueOK()
			if !ok {
				return commandError(9, "FailedToParse", "updateUser pwd must be a string")
			}
		}
		if err := s.AuthCatalog.updateUser(*actor, database, username, []byte(password), !passwordValue.IsZero(), roles, !rolesValue.IsZero()); err != nil {
			switch {
			case errors.Is(err, errAuthUserNotFound):
				return commandError(11, "UserNotFound", "user not found")
			case errors.Is(err, errUserManagementUnauthorized), errors.Is(err, errCannotDemoteLastServerAdministrator):
				return commandError(13, "Unauthorized", err.Error())
			default:
				return commandError(1, "InternalError", err.Error())
			}
		}
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})

	case "dropUser":
		username, err := commandString(command, name)
		if err != nil {
			return commandError(9, "FailedToParse", err.Error())
		}
		if err := s.AuthCatalog.dropUser(*actor, database, username); err != nil {
			switch {
			case errors.Is(err, errAuthUserNotFound):
				return commandError(11, "UserNotFound", "user not found")
			case errors.Is(err, errUserManagementUnauthorized), errors.Is(err, errCannotDropLastServerAdministrator):
				return commandError(13, "Unauthorized", err.Error())
			default:
				return commandError(1, "InternalError", err.Error())
			}
		}
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})

	case "usersInfo":
		value := bson.Raw(command).Lookup(name)
		requested, requestedByName := value.StringValueOK()
		if !requestedByName {
			if all, ok := value.AsInt64OK(); !ok || all != 1 {
				return commandError(9, "FailedToParse", "usersInfo must be a username or 1")
			}
		}
		users, err := s.AuthCatalog.authorizationUsers()
		if err != nil {
			return commandError(13, "Unauthorized", "authorization catalog unavailable")
		}
		result := make(bson.A, 0, len(users))
		for _, user := range users {
			if user.AuthDB != database || (requestedByName && user.Username != requested) {
				continue
			}
			if !s.principalCanManageGrants(owner, database, user.Roles) {
				continue
			}
			result = append(result, bson.D{
				{Key: "_id", Value: user.AuthDB + "." + user.Username},
				{Key: "userId", Value: strconv.FormatUint(user.Incarnation, 16)},
				{Key: "user", Value: user.Username},
				{Key: "db", Value: user.AuthDB},
				{Key: "roles", Value: authRoleDocuments(user.Roles)},
				{Key: "mechanisms", Value: bson.A{"SCRAM-SHA-256"}},
			})
		}
		return marshalDocument(bson.D{{Key: "users", Value: result}, {Key: "ok", Value: 1.0}})
	default:
		return commandError(59, "CommandNotFound", "unsupported user management command")
	}
}
