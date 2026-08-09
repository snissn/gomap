package mongogateway

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	authAuthorizationVersion = 1
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
	Username string          `json:"username"`
	AuthDB   string          `json:"auth_db"`
	Roles    []AuthRoleGrant `json:"roles"`
}

type authAuthorizationRecord struct {
	Version int                  `json:"version"`
	Users   []authRoleAssignment `json:"users"`
}

type authAuthorizationSnapshot struct {
	version uint64
	users   map[string]map[string][]AuthRoleGrant
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
	if decoder.Decode(&record) != nil || decoder.Decode(&struct{}{}) != io.EOF || record.Version != authAuthorizationVersion || len(record.Users) > maxAuthorizationUsers {
		return authAuthorizationRecord{}, errAuthenticationFailed
	}
	seen := make(map[string]struct{}, len(record.Users))
	for i := range record.Users {
		user := &record.Users[i]
		if !validAuthField(user.Username) || !validAuthField(user.AuthDB) {
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
	users := make(map[string]map[string][]AuthRoleGrant)
	for _, user := range record.Users {
		byName := users[user.AuthDB]
		if byName == nil {
			byName = make(map[string][]AuthRoleGrant)
			users[user.AuthDB] = byName
		}
		byName[user.Username] = append([]AuthRoleGrant(nil), user.Roles...)
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
	if err := c.db.SetSync(authAuthorizationCatalogKey(), raw); err != nil {
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
	roles, ok := snapshot.users[authDB][username]
	if !ok {
		return nil, nil
	}
	return roles, nil
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
		return errors.New("mongo gateway authorization: cannot demote the last enabled server administrator")
	}
	if index < 0 {
		if len(record.Users) >= maxAuthorizationUsers {
			return errors.New("mongo gateway authorization: user limit exceeded")
		}
		record.Users = append(record.Users, authRoleAssignment{Username: username, AuthDB: authDB, Roles: roles})
	} else {
		record.Users[index].Roles = roles
	}
	return c.publishAuthorizationRecordLocked(record)
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
		if err == nil && stored.Enabled {
			count++
		}
	}
	return count
}

func (c *AuthCatalog) ensureBootstrapAdminLocked(record *authAuthorizationRecord, authDB, username string) (bool, error) {
	hasAdmin := false
	for _, assignment := range record.Users {
		if hasServerAdmin(assignment.Roles) {
			hasAdmin = true
		}
		if assignment.AuthDB == authDB && assignment.Username == username {
			return false, nil
		}
	}
	if len(record.Users) >= maxAuthorizationUsers {
		return false, errors.New("mongo gateway authorization: user limit exceeded")
	}
	assignment := authRoleAssignment{Username: username, AuthDB: authDB, Roles: []AuthRoleGrant{}}
	if !hasAdmin {
		assignment.Roles = []AuthRoleGrant{{Role: AuthRoleServerAdmin}}
	}
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
		for username, roles := range byName {
			users = append(users, authRoleAssignment{
				AuthDB:   authDB,
				Username: username,
				Roles:    append([]AuthRoleGrant(nil), roles...),
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
	authorizationPublic authorizationPrivilege = iota
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
	switch name {
	case "hello", "isMaster", "ismaster", "saslStart", "saslContinue", "connectionStatus", "buildInfo", "ping", "endSessions":
		return target, nil
	case "hostInfo":
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
	if grant.Role == AuthRoleServerAdmin {
		return true
	}
	if target.privilege == authorizationServerAdmin {
		return false
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
	roles, err := s.AuthCatalog.effectiveRoles(user.AuthDB, user.Username)
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
	roles, err := s.AuthCatalog.effectiveRoles(user.AuthDB, user.Username)
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
	values, err := bson.Raw(command).Lookup("roles").Array().Values()
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

func (s *Server) principalCanManageGrants(owner int64, grants []AuthRoleGrant) bool {
	user := s.authUserSnapshot(owner)
	if user == nil || s.AuthCatalog == nil {
		return false
	}
	roles, err := s.AuthCatalog.effectiveRoles(user.AuthDB, user.Username)
	if err != nil {
		return false
	}
	for _, role := range roles {
		if role.Role == AuthRoleServerAdmin {
			return true
		}
	}
	for _, grant := range grants {
		allowed := false
		if grant.Role == AuthRoleServerAdmin || grant.Database == "" {
			return false
		}
		for _, role := range roles {
			if role.Role == AuthRoleUserAdmin && roleScopeMatches(role, grant.Database, grant.Collection) {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	return true
}

func (s *Server) principalCanManageTarget(owner int64, authDB, username string) bool {
	targetRoles, err := s.AuthCatalog.effectiveRoles(authDB, username)
	if err != nil || !hasServerAdmin(targetRoles) {
		return true
	}
	user := s.authUserSnapshot(owner)
	if user == nil {
		return false
	}
	roles, err := s.AuthCatalog.effectiveRoles(user.AuthDB, user.Username)
	return err == nil && hasServerAdmin(roles)
}

func (s *Server) userManagementResponse(name string, command wire.Document, owner int64) (wire.Document, error) {
	if s.AuthCatalog == nil {
		return commandError(13, "Unauthorized", "authorization catalog unavailable")
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
		if !s.principalCanManageGrants(owner, roles) {
			return commandError(13, "Unauthorized", "not authorized to grant requested roles")
		}
		if s.AuthCatalog.userExists(database, username) {
			return commandError(51003, "DuplicateKey", "user already exists")
		}
		if err := s.AuthCatalog.UpsertPassword(database, username, []byte(password)); err != nil {
			return commandError(1, "InternalError", err.Error())
		}
		if err := s.AuthCatalog.SetUserRoles(database, username, roles); err != nil {
			return commandError(1, "InternalError", err.Error())
		}
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})

	case "updateUser":
		username, err := commandString(command, name)
		if err != nil {
			return commandError(9, "FailedToParse", err.Error())
		}
		if !s.AuthCatalog.userExists(database, username) {
			return commandError(11, "UserNotFound", "user not found")
		}
		if !s.principalCanManageTarget(owner, database, username) {
			return commandError(13, "Unauthorized", "not authorized to update server administrator")
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
			if !s.principalCanManageGrants(owner, roles) {
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
		// Apply the role policy boundary before verifier mutation so a denied
		// last-admin demotion cannot partially rotate the password.
		if !rolesValue.IsZero() {
			if err := s.AuthCatalog.SetUserRoles(database, username, roles); err != nil {
				return commandError(13, "Unauthorized", err.Error())
			}
		}
		if !passwordValue.IsZero() {
			if err := s.AuthCatalog.UpsertPassword(database, username, []byte(password)); err != nil {
				return commandError(1, "InternalError", err.Error())
			}
		}
		return marshalDocument(bson.D{{Key: "ok", Value: 1.0}})

	case "dropUser":
		username, err := commandString(command, name)
		if err != nil {
			return commandError(9, "FailedToParse", err.Error())
		}
		if !s.principalCanManageTarget(owner, database, username) {
			return commandError(13, "Unauthorized", "not authorized to drop server administrator")
		}
		if err := s.AuthCatalog.DropUser(database, username); err != nil {
			return commandError(13, "Unauthorized", err.Error())
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
			result = append(result, bson.D{
				{Key: "_id", Value: user.AuthDB + "." + user.Username},
				{Key: "userId", Value: user.AuthDB + "\x00" + user.Username},
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
