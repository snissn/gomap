package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type failNextAuthCatalogSetStore struct {
	authCatalogStore
	mu      sync.Mutex
	failKey string
}

func (s *failNextAuthCatalogSetStore) SetSync(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if string(key) == s.failKey {
		s.failKey = ""
		return fmt.Errorf("injected durable write failure for %q", key)
	}
	return s.authCatalogStore.SetSync(key, value)
}

func newAuthorizationTestServer(t *testing.T) (*Server, *AuthCatalog) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled = true
	server.AuthCatalog = catalog
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions.DocumentFormat = collections.DocumentFormatBSON
	return server, catalog
}

func newAuthorizationPersistentValueLogTestServer(t *testing.T) (*Server, *AuthCatalog) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled = true
	server.AuthCatalog = catalog
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions.DocumentFormat = collections.DocumentFormatBSON
	return server, catalog
}

func setAuthorizationTestUser(server *Server, owner int64, authDB, username string) {
	record, err := server.AuthCatalog.record(authDB, username)
	if err != nil {
		panic(err)
	}
	user := AuthUser{AuthDB: authDB, Username: username, Incarnation: record.Incarnation}
	server.authState(owner).user.Store(&user)
}

func serveAuthorizationCommand(t *testing.T, server *Server, owner int64, requestID int32, doc bson.D) wire.Document {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	req, err := wire.AppendMsgMessage(nil, requestID, 0, 0, wire.Document(raw))
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: bytes.NewReader(req)}
	if err := server.ServeOneWithOwner(rw, owner); err != nil {
		t.Fatalf("ServeOneWithOwner: %v", err)
	}
	response, err := readMsgResponseResult(rw.w.Bytes(), requestID)
	if err != nil {
		t.Fatal(fmt.Errorf("read response: %w", err))
	}
	return response
}

func assertAuthorizationErrorsIndistinguishable(t *testing.T, operation string, missing, protected wire.Document) {
	t.Helper()
	missingRaw, protectedRaw := bson.Raw(missing), bson.Raw(protected)
	missingName, _ := missingRaw.Lookup("codeName").StringValueOK()
	protectedName, _ := protectedRaw.Lookup("codeName").StringValueOK()
	missingMessage, _ := missingRaw.Lookup("errmsg").StringValueOK()
	protectedMessage, _ := protectedRaw.Lookup("errmsg").StringValueOK()
	if missingName != "Unauthorized" || protectedName != missingName || protectedMessage != missingMessage {
		t.Fatalf("%s leaked target existence: missing=%s protected=%s", operation, missingRaw, protectedRaw)
	}
}

func TestAuthorizationRoleMatrixAndPreMutationDenial(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "reader", []byte("reader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	setAuthorizationTestUser(server, 2, "admin", "reader")

	assertOK(t, serveAuthorizationCommand(t, server, 1, 1, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "root"}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveAuthorizationCommand(t, server, 2, 2, bson.D{{Key: "find", Value: "items"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 3, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "denied"}}}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 4, bson.D{{Key: "find", Value: "other"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	got := serveAuthorizationCommand(t, server, 1, 5, bson.D{{Key: "count", Value: "items"}, {Key: "$db", Value: "app"}})
	assertOK(t, got)
	if n := bson.Raw(got).Lookup("n").Int64(); n != 1 {
		t.Fatalf("count after denied insert=%d want 1", n)
	}
}

func TestAuthorizationDropRecreateRevokesStaleConnectionAndCursor(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "reader", []byte("old reader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	authenticateUser(t, server, 1, "root", []byte("root password"))
	authenticateUser(t, server, 2, "reader", []byte("old reader password"))
	assertOK(t, serveAuthorizationCommand(t, server, 1, 60, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "first"}},
		bson.D{{Key: "_id", Value: "second"}},
	}}, {Key: "$db", Value: "app"}}))
	find := serveAuthorizationCommand(t, server, 2, 61, bson.D{{Key: "find", Value: "items"}, {Key: "filter", Value: bson.D{}}, {Key: "batchSize", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, find)
	cursorID := bson.Raw(find).Lookup("cursor").Document().Lookup("id").Int64()
	if cursorID == 0 {
		t.Fatal("find did not retain a cursor")
	}

	assertOK(t, serveAuthorizationCommand(t, server, 1, 62, bson.D{{Key: "dropUser", Value: "reader"}, {Key: "$db", Value: "admin"}}))
	assertOK(t, serveAuthorizationCommand(t, server, 1, 63, bson.D{{Key: "createUser", Value: "reader"}, {Key: "pwd", Value: "new reader password"}, {Key: "roles", Value: bson.A{
		bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: "app"}, {Key: "collection", Value: "items"}},
	}}, {Key: "$db", Value: "admin"}}))

	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 64, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "stale"}}}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	if server.authenticated(2) {
		t.Fatal("stale connection remained authenticated after account recreation")
	}
	authenticateUser(t, server, 2, "reader", []byte("new reader password"))
	assertOK(t, serveAuthorizationCommand(t, server, 2, 65, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "fresh"}}}}, {Key: "$db", Value: "app"}}))
	getMore := serveAuthorizationCommand(t, server, 2, 66, bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}})
	assertCommandError(t, getMore, "CursorNotFound")

	assertOK(t, serveAuthorizationCommand(t, server, 1, 67, bson.D{{Key: "updateUser", Value: "reader"}, {Key: "pwd", Value: "rotated reader password"}, {Key: "$db", Value: "admin"}}))
	assertOK(t, serveAuthorizationCommand(t, server, 2, 68, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "after-rotation"}}}}, {Key: "$db", Value: "app"}}))
}

func TestAuthorizationDropRecreateRaceNeverElevatesStalePrincipal(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "worker", []byte("old worker password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "worker", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	root, err := catalog.VerifyPassword("admin", "root", []byte("root password"))
	if err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 2, "admin", "worker")
	command := mustDocument(t, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "stale"}}}}, {Key: "$db", Value: "app"}})

	var stop atomic.Bool
	var staleAllowed atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			_, _, _, allowed := server.authorizeCommand("insert", command, 2)
			if allowed {
				staleAllowed.Store(true)
				return
			}
		}
	}()
	if err := catalog.dropUser(root, "admin", "worker"); err != nil {
		t.Fatal(err)
	}
	if err := catalog.createUser(root, "admin", "worker", []byte("new worker password"), []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000 && !staleAllowed.Load(); i++ {
		_, _, _, allowed := server.authorizeCommand("insert", command, 2)
		if allowed {
			staleAllowed.Store(true)
		}
	}
	stop.Store(true)
	<-done
	if staleAllowed.Load() {
		t.Fatal("stale principal inherited recreated account's elevated grant")
	}
	fresh, err := catalog.VerifyPassword("admin", "worker", []byte("new worker password"))
	if err != nil {
		t.Fatal(err)
	}
	server.authState(3).user.Store(&fresh)
	if _, _, _, allowed := server.authorizeCommand("insert", command, 3); !allowed {
		t.Fatal("fresh recreated principal did not receive requested grant")
	}
}

func TestAuthorizationMultiWriteAdmissionPrecedesMutation(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	for _, username := range []string{"root", "reader", "writer"} {
		if err := catalog.UpsertPassword("admin", username, []byte(username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "writer", []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	setAuthorizationTestUser(server, 2, "admin", "reader")
	setAuthorizationTestUser(server, 3, "admin", "writer")

	assertOK(t, serveAuthorizationCommand(t, server, 1, 10, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{
		bson.D{{Key: "_id", Value: "one"}, {Key: "active", Value: true}},
		bson.D{{Key: "_id", Value: "two"}, {Key: "active", Value: true}},
	}}, {Key: "$db", Value: "app"}}))
	update := bson.D{{Key: "update", Value: "items"}, {Key: "updates", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "active", Value: false}}}}},
		{Key: "multi", Value: true},
	}}}, {Key: "$db", Value: "app"}}
	deniedDeleteMany := bson.D{{Key: "delete", Value: "items"}, {Key: "deletes", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "limit", Value: int32(0)},
	}}}, {Key: "$db", Value: "app"}}
	allowedDeleteMany := bson.D{{Key: "delete", Value: "items"}, {Key: "deletes", Value: bson.A{bson.D{
		{Key: "q", Value: bson.D{{Key: "active", Value: false}}},
		{Key: "limit", Value: int32(0)},
	}}}, {Key: "$db", Value: "app"}}

	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 11, update), "Unauthorized")
	active := serveAuthorizationCommand(t, server, 1, 12, bson.D{{Key: "count", Value: "items"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}})
	assertOK(t, active)
	if n := bson.Raw(active).Lookup("n").Int64(); n != 2 {
		t.Fatalf("active count after denied multi update=%d want 2", n)
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 13, deniedDeleteMany), "Unauthorized")
	active = serveAuthorizationCommand(t, server, 1, 14, bson.D{{Key: "count", Value: "items"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "$db", Value: "app"}})
	assertOK(t, active)
	if n := bson.Raw(active).Lookup("n").Int64(); n != 2 {
		t.Fatalf("active count after denied multi delete=%d want 2", n)
	}

	allowedUpdate := serveAuthorizationCommand(t, server, 3, 15, update)
	assertOK(t, allowedUpdate)
	assertInt32(t, allowedUpdate, "n", 2)
	assertInt32(t, allowedUpdate, "nModified", 2)
	allowedDelete := serveAuthorizationCommand(t, server, 3, 16, allowedDeleteMany)
	assertOK(t, allowedDelete)
	assertInt32(t, allowedDelete, "n", 2)
	remaining := serveAuthorizationCommand(t, server, 1, 17, bson.D{{Key: "count", Value: "items"}, {Key: "$db", Value: "app"}})
	assertOK(t, remaining)
	if n := bson.Raw(remaining).Lookup("n").Int64(); n != 0 {
		t.Fatalf("count after allowed multi delete=%d want 0", n)
	}
}

func TestAuthorizationEverySupportedCommandHasExplicitPrivilege(t *testing.T) {
	cases := map[string]authorizationPrivilege{
		"hello":            authorizationPublic,
		"isMaster":         authorizationPublic,
		"ismaster":         authorizationPublic,
		"buildInfo":        authorizationPublic,
		"connectionStatus": authorizationPublic,
		"saslStart":        authorizationPublic,
		"saslContinue":     authorizationPublic,
		"endSessions":      authorizationPublic,
		"ping":             authorizationPublic,
		"hostInfo":         authorizationServerAdmin,
		"find":             authorizationRead,
		"aggregate":        authorizationRead,
		"count":            authorizationRead,
		"distinct":         authorizationRead,
		"explain":          authorizationRead,
		"getMore":          authorizationRead,
		"killCursors":      authorizationRead,
		"listIndexes":      authorizationMetadataRead,
		"insert":           authorizationWrite,
		"update":           authorizationWrite,
		"delete":           authorizationWrite,
		"findAndModify":    authorizationWrite,
		"create":           authorizationDBAdmin,
		"createIndexes":    authorizationDBAdmin,
		"dropIndexes":      authorizationDBAdmin,
		"listCollections":  authorizationListCollections,
		"listDatabases":    authorizationListDatabases,
		"createUser":       authorizationUserAdmin,
		"updateUser":       authorizationUserAdmin,
		"dropUser":         authorizationUserAdmin,
		"usersInfo":        authorizationUserAdmin,
		"serverStatus":     authorizationServerAdmin,
		"top":              authorizationServerAdmin,
		"dbStats":          authorizationMetadataRead,
		"collStats":        authorizationMetadataRead,
	}
	if len(cases) != len(mongoGatewaySupportedCommands) {
		t.Fatalf("authorization cases=%d dispatched commands=%d", len(cases), len(mongoGatewaySupportedCommands))
	}
	for name := range mongoGatewaySupportedCommands {
		want, ok := cases[name]
		if !ok {
			t.Fatalf("dispatched command %q has no explicit authorization expectation", name)
		}
		value := any("items")
		if name == "getMore" {
			value = int64(1)
		}
		doc := bson.D{{Key: name, Value: value}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}}
		if name == "explain" {
			doc = bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}}, {Key: "$db", Value: "app"}}
		}
		target, err := commandAuthorizationTarget(name, mustDocument(t, doc))
		if err != nil || target.privilege != want {
			t.Fatalf("%s target=%+v want=%v err=%v", name, target, want, err)
		}
	}
	target, err := commandAuthorizationTarget("futureMutation", mustDocument(t, bson.D{{Key: "futureMutation", Value: 1}, {Key: "$db", Value: "app"}}))
	if err != nil || target.privilege != authorizationUnsupported {
		t.Fatalf("unknown command target=%+v err=%v", target, err)
	}
}

func TestAuthorizationExplainInheritsOuterDatabaseAndRejectsInnerWrites(t *testing.T) {
	inherited := mustDocument(t, bson.D{
		{Key: "explain", Value: bson.D{{Key: "find", Value: "items"}}},
		{Key: "$db", Value: "app"},
	})
	target, err := commandAuthorizationTarget("explain", inherited)
	if err != nil || target.privilege != authorizationRead || string(target.databaseRaw) != "app" || string(target.collectionRaw) != "items" {
		t.Fatalf("inherited explain target=%+v err=%v", target, err)
	}
	write := mustDocument(t, bson.D{
		{Key: "explain", Value: bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{}}}},
		{Key: "$db", Value: "app"},
	})
	if _, err := commandAuthorizationTarget("explain", write); err == nil {
		t.Fatal("inner write was accepted for explain authorization")
	}
}

func TestAuthorizationBuiltInRoleSeparation(t *testing.T) {
	targets := []authorizationTarget{
		{privilege: authorizationRead, database: "app", collection: "items"},
		{privilege: authorizationWrite, database: "app", collection: "items"},
		{privilege: authorizationDBAdmin, database: "app", collection: "items"},
		{privilege: authorizationUserAdmin, database: "app"},
		{privilege: authorizationServerAdmin},
		{privilege: authorizationMetadataRead, database: "app", collection: "items"},
		{privilege: authorizationListCollections, database: "app"},
		{privilege: authorizationListDatabases},
	}
	cases := []struct {
		role AuthRole
		want []bool
	}{
		{role: AuthRoleRead, want: []bool{true, false, false, false, false, true, true, true}},
		{role: AuthRoleReadWrite, want: []bool{true, true, false, false, false, true, true, true}},
		{role: AuthRoleDBAdmin, want: []bool{false, false, true, false, false, true, true, true}},
		{role: AuthRoleUserAdmin, want: []bool{false, false, false, true, false, false, false, true}},
		{role: AuthRoleServerAdmin, want: []bool{true, true, true, true, true, true, true, true}},
	}
	for _, tc := range cases {
		grant := AuthRoleGrant{Role: tc.role, Database: "app"}
		if tc.role == AuthRoleServerAdmin {
			grant.Database = ""
		}
		for i, target := range targets {
			if got := roleAllows(grant, target); got != tc.want[i] {
				t.Errorf("role=%s privilege=%v got=%v want=%v", tc.role, target.privilege, got, tc.want[i])
			}
		}
	}
	reader := AuthRoleGrant{Role: AuthRoleRead, Database: "app", Collection: "items"}
	if roleAllows(reader, authorizationTarget{privilege: authorizationRead, database: "app", collection: "other"}) {
		t.Fatal("collection-scoped read crossed collections")
	}
	if roleAllows(reader, authorizationTarget{privilege: authorizationRead, database: "other", collection: "items"}) {
		t.Fatal("collection-scoped read crossed databases")
	}
}

func TestAuthorizationDeniedDDLIndexDiagnosticsAndSafeStatus(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "reader", []byte("reader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	setAuthorizationTestUser(server, 2, "admin", "reader")
	assertOK(t, serveAuthorizationCommand(t, server, 1, 6, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "fixture"}, {Key: "n", Value: 1}}}}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveAuthorizationCommand(t, server, 1, 7, bson.D{{Key: "createIndexes", Value: "items"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "n", Value: 1}}}, {Key: "name", Value: "n_1"}}}}, {Key: "$db", Value: "app"}}))

	before := server.AuthorizationMetrics()
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 8, bson.D{{Key: "create", Value: "blocked"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 9, bson.D{{Key: "createIndexes", Value: "items"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "blocked", Value: 1}}}, {Key: "name", Value: "blocked_1"}}}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 10, bson.D{{Key: "dropIndexes", Value: "items"}, {Key: "index", Value: "n_1"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 11, bson.D{{Key: "hostInfo", Value: 1}, {Key: "$db", Value: "admin"}}), "Unauthorized")
	after := server.AuthorizationMetrics()
	if after.Allowed != before.Allowed || after.Denied-before.Denied != 4 {
		t.Fatalf("authorization metrics before=%+v after=%+v", before, after)
	}

	status := serveAuthorizationCommand(t, server, 2, 12, bson.D{{Key: "connectionStatus", Value: 1}, {Key: "$db", Value: "admin"}})
	authInfo := bson.Raw(status).Lookup("authInfo").Document()
	roles, err := authInfo.Lookup("authenticatedUserRoles").Array().Values()
	if err != nil || len(roles) != 1 || roles[0].Document().Lookup("role").StringValue() != "read" {
		t.Fatalf("connectionStatus roles=%v err=%v", roles, err)
	}
	privileges, err := authInfo.Lookup("authenticatedUserPrivileges").Array().Values()
	if err != nil || len(privileges) != 1 {
		t.Fatalf("connectionStatus privileges=%v err=%v", privileges, err)
	}

	collectionsResponse := serveAuthorizationCommand(t, server, 1, 13, bson.D{{Key: "listCollections", Value: 1}, {Key: "nameOnly", Value: true}, {Key: "$db", Value: "app"}})
	collectionsBatch := cursorFirstBatch(t, collectionsResponse)
	for _, collection := range collectionsBatch {
		if collection.Lookup("name").StringValue() == "blocked" {
			t.Fatal("denied create mutated the catalog")
		}
	}
	indexesResponse := serveAuthorizationCommand(t, server, 1, 14, bson.D{{Key: "listIndexes", Value: "items"}, {Key: "$db", Value: "app"}})
	indexesBatch := cursorFirstBatch(t, indexesResponse)
	foundN, foundBlocked := false, false
	for _, index := range indexesBatch {
		switch index.Lookup("name").StringValue() {
		case "n_1":
			foundN = true
		case "blocked_1":
			foundBlocked = true
		}
	}
	if !foundN || foundBlocked {
		t.Fatalf("indexes after denied changes n_1=%v blocked_1=%v", foundN, foundBlocked)
	}
}

func TestAuthorizationListFilteringRevocationAndLastAdmin(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "reader", []byte("reader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "visible"}}); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	for _, collection := range []string{"visible", "hidden"} {
		assertOK(t, serveAuthorizationCommand(t, server, 1, 10, bson.D{{Key: "insert", Value: collection}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: collection}}}}, {Key: "$db", Value: "app"}}))
	}
	assertOK(t, serveAuthorizationCommand(t, server, 1, 13, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "secret"}}}}, {Key: "$db", Value: "secret"}}))
	setAuthorizationTestUser(server, 2, "admin", "reader")
	list := serveAuthorizationCommand(t, server, 2, 11, bson.D{{Key: "listCollections", Value: 1}, {Key: "nameOnly", Value: true}, {Key: "$db", Value: "app"}})
	batch := cursorFirstBatch(t, list)
	if len(batch) != 1 || batch[0].Lookup("name").StringValue() != "visible" {
		t.Fatalf("filtered collections=%v", batch)
	}
	databases := serveAuthorizationCommand(t, server, 2, 14, bson.D{{Key: "listDatabases", Value: 1}})
	values, err := bson.Raw(databases).Lookup("databases").Array().Values()
	if err != nil || len(values) != 1 || values[0].Document().Lookup("name").StringValue() != "app" {
		t.Fatalf("filtered databases=%v err=%v", values, err)
	}
	if err := catalog.SetUserRoles("admin", "reader", nil); err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 12, bson.D{{Key: "find", Value: "visible"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	if err := catalog.SetEnabled("admin", "root", false); !errors.Is(err, errCannotDisableLastServerAdministrator) {
		t.Fatalf("last server administrator disable error=%v", err)
	}
	if _, err := catalog.VerifyPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal("denied last-admin disable changed verifier state")
	}
	if err := catalog.SetUserRoles("admin", "root", nil); !errors.Is(err, errCannotDemoteLastServerAdministrator) {
		t.Fatalf("last server administrator demotion error=%v", err)
	}
}

func TestAuthorizationUserManagementAndRoleBoundary(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	assertOK(t, serveAuthorizationCommand(t, server, 1, 20, bson.D{
		{Key: "createUser", Value: "alice"},
		{Key: "pwd", Value: "alice password"},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: "app"}}}},
		{Key: "$db", Value: "app"},
	}))
	setAuthorizationTestUser(server, 2, "app", "alice")
	assertOK(t, serveAuthorizationCommand(t, server, 2, 21, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "alice"}}}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 22, bson.D{{Key: "updateUser", Value: "alice"}, {Key: "roles", Value: bson.A{}}, {Key: "$db", Value: "app"}}), "Unauthorized")

	info := serveAuthorizationCommand(t, server, 1, 23, bson.D{{Key: "usersInfo", Value: "alice"}, {Key: "$db", Value: "app"}})
	assertOK(t, info)
	users, err := bson.Raw(info).Lookup("users").Array().Values()
	if err != nil || len(users) != 1 || users[0].Document().Lookup("user").StringValue() != "alice" {
		t.Fatalf("usersInfo=%v err=%v", users, err)
	}
	assertOK(t, serveAuthorizationCommand(t, server, 1, 24, bson.D{{Key: "updateUser", Value: "alice"}, {Key: "roles", Value: bson.A{}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 25, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertOK(t, serveAuthorizationCommand(t, server, 1, 26, bson.D{{Key: "dropUser", Value: "alice"}, {Key: "$db", Value: "app"}}))
	if catalog.userExists("app", "alice") {
		t.Fatal("dropped user verifier remains")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 27, bson.D{{Key: "updateUser", Value: "root"}, {Key: "pwd", Value: "stolen password"}, {Key: "roles", Value: bson.A{}}, {Key: "$db", Value: "admin"}}), "Unauthorized")
	if _, err := catalog.VerifyPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal("denied last-admin demotion changed password")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 28, bson.D{{Key: "dropUser", Value: "root"}, {Key: "$db", Value: "admin"}}), "Unauthorized")
}

func TestAuthorizationMixedUserUpdateRoleWriteFailureCannotElevateOldCredential(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("app", "alice", []byte("old password")); err != nil {
		t.Fatal(err)
	}
	oldRoles := []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}
	if err := catalog.SetUserRoles("app", "alice", oldRoles); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	catalog.db = &failNextAuthCatalogSetStore{authCatalogStore: catalog.db, failKey: string(authAuthorizationCatalogKey())}

	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 29, bson.D{
		{Key: "updateUser", Value: "alice"},
		{Key: "pwd", Value: "new password"},
		{Key: "roles", Value: bson.A{"readWrite"}},
		{Key: "$db", Value: "app"},
	}), "InternalError")
	if _, err := catalog.VerifyPassword("app", "alice", []byte("old password")); err == nil {
		t.Fatal("old credential survived failed mixed update")
	}
	if _, err := catalog.VerifyPassword("app", "alice", []byte("new password")); err != nil {
		t.Fatalf("new credential unavailable after verifier-first partial update: %v", err)
	}
	roles, err := catalog.UserRoles("app", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roles, oldRoles) {
		t.Fatalf("failed mixed update changed roles: got %#v want %#v", roles, oldRoles)
	}
	setAuthorizationTestUser(server, 2, "app", "alice")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 30, bson.D{
		{Key: "insert", Value: "items"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "forbidden"}}}},
		{Key: "$db", Value: "app"},
	}), "Unauthorized")
}

func TestAuthorizationUserManagementAdmissionPrecedesDurableMutation(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("app", "alice", []byte("old password")); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")

	transaction := bson.D{{Key: "txnNumber", Value: int64(1)}, {Key: "startTransaction", Value: true}, {Key: "autocommit", Value: false}}
	create := append(bson.D{{Key: "createUser", Value: "transactional"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{"read"}}}, transaction...)
	create = append(create, bson.E{Key: "$db", Value: "app"})
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 31, create), "BadValue")
	if catalog.userExists("app", "transactional") {
		t.Fatal("transaction-marked createUser persisted a verifier")
	}

	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 32, bson.D{
		{Key: "updateUser", Value: "alice"}, {Key: "pwd", Value: "new password"},
		{Key: "txnNumber", Value: int64(2)}, {Key: "$db", Value: "app"},
	}), "BadValue")
	if _, err := catalog.VerifyPassword("app", "alice", []byte("old password")); err != nil {
		t.Fatal("transaction-marked updateUser changed the verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 33, bson.D{
		{Key: "dropUser", Value: "alice"}, {Key: "txnNumber", Value: int64(3)}, {Key: "$db", Value: "app"},
	}), "BadValue")
	if !catalog.userExists("app", "alice") {
		t.Fatal("transaction-marked dropUser removed the verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 34, bson.D{
		{Key: "usersInfo", Value: int32(1)}, {Key: "txnNumber", Value: int64(4)}, {Key: "$db", Value: "app"},
	}), "BadValue")

	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 35, bson.D{
		{Key: "createUser", Value: "unacknowledged"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{"read"}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(0)}}}, {Key: "$db", Value: "app"},
	}), "WriteConcernFailed")
	if catalog.userExists("app", "unacknowledged") {
		t.Fatal("w:0 createUser persisted a verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 36, bson.D{
		{Key: "updateUser", Value: "alice"}, {Key: "pwd", Value: "new password"},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(0)}}}, {Key: "$db", Value: "app"},
	}), "WriteConcernFailed")
	if _, err := catalog.VerifyPassword("app", "alice", []byte("old password")); err != nil {
		t.Fatal("w:0 updateUser changed the verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 37, bson.D{
		{Key: "dropUser", Value: "alice"}, {Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(0)}}}, {Key: "$db", Value: "app"},
	}), "WriteConcernFailed")
	if !catalog.userExists("app", "alice") {
		t.Fatal("w:0 dropUser removed the verifier")
	}

	moreToCome := mustDocument(t, bson.D{{Key: "createUser", Value: "more-to-come"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{"read"}}, {Key: "$db", Value: "app"}})
	request, err := wire.AppendMsgMessage(nil, 38, 0, wire.MsgFlagMoreToCome, moreToCome)
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: bytes.NewReader(request)}
	if err := server.ServeOneWithOwner(rw, 1); err != nil {
		t.Fatal(err)
	}
	if rw.w.Len() != 0 || catalog.userExists("app", "more-to-come") {
		t.Fatalf("moreToCome response=%d userExists=%v", rw.w.Len(), catalog.userExists("app", "more-to-come"))
	}
}

func TestAuthorizationUserManagementScopesMalformedRolesAndUnknownCommands(t *testing.T) {
	server, catalog := newAuthorizationPersistentValueLogTestServer(t)
	for _, username := range []string{"root", "collection-admin", "database-admin", "server-user-admin"} {
		if err := catalog.UpsertPassword("admin", username, []byte(username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	for _, username := range []string{"items-user", "other-user"} {
		if err := catalog.UpsertPassword("app", username, []byte(username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.SetUserRoles("admin", "collection-admin", []AuthRoleGrant{{Role: AuthRoleUserAdmin, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "database-admin", []AuthRoleGrant{{Role: AuthRoleUserAdmin, Database: "app"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "server-user-admin", []AuthRoleGrant{{Role: AuthRoleUserAdmin}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("app", "items-user", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("app", "other-user", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "other"}}); err != nil {
		t.Fatal(err)
	}

	setAuthorizationTestUser(server, 2, "admin", "collection-admin")
	assertOK(t, serveAuthorizationCommand(t, server, 2, 40, bson.D{{Key: "createUser", Value: "collection-reader"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "app"}, {Key: "collection", Value: "items"}}}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 41, bson.D{{Key: "createUser", Value: "other-reader"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "app"}, {Key: "collection", Value: "other"}}}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 42, bson.D{{Key: "createUser", Value: "empty"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 43, bson.D{{Key: "createUser", Value: "cross-db"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{}}, {Key: "$db", Value: "payroll"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 44, bson.D{{Key: "createUser", Value: "malformed"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: "read"}, {Key: "$db", Value: "app"}}), "BadValue")

	info := serveAuthorizationCommand(t, server, 2, 45, bson.D{{Key: "usersInfo", Value: int32(1)}, {Key: "$db", Value: "app"}})
	assertOK(t, info)
	values, err := bson.Raw(info).Lookup("users").Array().Values()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if value.Document().Lookup("user").StringValue() == "other-user" {
			t.Fatal("collection-scoped usersInfo exposed an out-of-scope identity")
		}
	}
	named := serveAuthorizationCommand(t, server, 2, 46, bson.D{{Key: "usersInfo", Value: "other-user"}, {Key: "$db", Value: "app"}})
	assertOK(t, named)
	values, err = bson.Raw(named).Lookup("users").Array().Values()
	if err != nil || len(values) != 0 {
		t.Fatalf("named out-of-scope usersInfo=%v err=%v", values, err)
	}

	setAuthorizationTestUser(server, 3, "admin", "database-admin")
	assertOK(t, serveAuthorizationCommand(t, server, 3, 47, bson.D{{Key: "createUser", Value: "empty-db-user"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 3, 471, bson.D{{Key: "updateUser", Value: "missing"}, {Key: "pwd", Value: "password"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 3, 472, bson.D{{Key: "dropUser", Value: "missing"}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 3, 473, bson.D{{Key: "createUser", Value: "other-user"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{"read"}}, {Key: "$db", Value: "app"}}), "DuplicateKey")
	setAuthorizationTestUser(server, 4, "admin", "server-user-admin")
	assertOK(t, serveAuthorizationCommand(t, server, 4, 48, bson.D{{Key: "createUser", Value: "server-reader"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: ""}}}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 4, 49, bson.D{{Key: "createUser", Value: "forbidden-admin"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{"serverAdmin"}}, {Key: "$db", Value: "app"}}), "Unauthorized")

	setAuthorizationTestUser(server, 1, "admin", "root")
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 50, bson.D{{Key: "futureMutation", Value: 1}, {Key: "$db", Value: "app"}}), "Unauthorized")
}

func TestAuthorizationCollectionUserAdminCannotObserveOutOfScopeUserExistence(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	for _, user := range []struct {
		authDB   string
		username string
		password string
	}{
		{authDB: "admin", username: "root", password: "root password"},
		{authDB: "app", username: "collection-admin", password: "manager password"},
		{authDB: "app", username: "other-user", password: "other password"},
	} {
		if err := catalog.UpsertPassword(user.authDB, user.username, []byte(user.password)); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.SetUserRoles("app", "collection-admin", []AuthRoleGrant{{Role: AuthRoleUserAdmin, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	otherRoles := []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "other"}}
	if err := catalog.SetUserRoles("app", "other-user", otherRoles); err != nil {
		t.Fatal(err)
	}
	orphan, err := prepareSCRAMRecord("app", "orphan", []byte("orphan password"))
	if err != nil {
		t.Fatal(err)
	}
	if err := setAuthCatalogValueSync(catalog.db, authCatalogKey("app", "orphan"), orphan); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 2, "app", "collection-admin")

	assertAuthorizationErrorsIndistinguishable(t, "updateUser",
		serveAuthorizationCommand(t, server, 2, 51, bson.D{{Key: "updateUser", Value: "missing"}, {Key: "pwd", Value: "new password"}, {Key: "$db", Value: "app"}}),
		serveAuthorizationCommand(t, server, 2, 52, bson.D{{Key: "updateUser", Value: "other-user"}, {Key: "pwd", Value: "new password"}, {Key: "$db", Value: "app"}}),
	)
	assertAuthorizationErrorsIndistinguishable(t, "dropUser",
		serveAuthorizationCommand(t, server, 2, 53, bson.D{{Key: "dropUser", Value: "missing"}, {Key: "$db", Value: "app"}}),
		serveAuthorizationCommand(t, server, 2, 54, bson.D{{Key: "dropUser", Value: "other-user"}, {Key: "$db", Value: "app"}}),
	)
	manageableRoles := bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "app"}, {Key: "collection", Value: "items"}}}
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 55, bson.D{{Key: "createUser", Value: "other-user"}, {Key: "pwd", Value: "replacement password"}, {Key: "roles", Value: manageableRoles}, {Key: "$db", Value: "app"}}), "Unauthorized")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 56, bson.D{{Key: "createUser", Value: "orphan"}, {Key: "pwd", Value: "replacement password"}, {Key: "roles", Value: manageableRoles}, {Key: "$db", Value: "app"}}), "Unauthorized")

	if _, err := catalog.VerifyPassword("app", "other-user", []byte("other password")); err != nil {
		t.Fatal("denied operations changed the existing password")
	}
	if _, err := catalog.VerifyPassword("app", "other-user", []byte("new password")); err == nil {
		t.Fatal("denied update installed the requested password")
	}
	if roles, err := catalog.UserRoles("app", "other-user"); err != nil || !reflect.DeepEqual(roles, otherRoles) {
		t.Fatalf("denied operations changed roles=%v err=%v", roles, err)
	}
	if !catalog.userExists("app", "orphan") || catalog.userExists("app", "missing") {
		t.Fatalf("denied operations changed orphan=%v missing=%v", catalog.userExists("app", "orphan"), catalog.userExists("app", "missing"))
	}
}

func TestAuthorizationNarrowUserAdminsCannotObserveProtectedUserExistence(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	for _, user := range []struct {
		authDB   string
		username string
	}{
		{authDB: "admin", username: "root"},
		{authDB: "admin", username: "database-admin"},
		{authDB: "admin", username: "server-user-admin"},
		{authDB: "app", username: "cross-database-user"},
	} {
		if err := catalog.UpsertPassword(user.authDB, user.username, []byte(user.username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.SetUserRoles("admin", "database-admin", []AuthRoleGrant{{Role: AuthRoleUserAdmin, Database: "app"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "server-user-admin", []AuthRoleGrant{{Role: AuthRoleUserAdmin}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("app", "cross-database-user", []AuthRoleGrant{{Role: AuthRoleRead, Database: "payroll"}}); err != nil {
		t.Fatal(err)
	}
	orphan, err := prepareSCRAMRecord("app", "orphan", []byte("orphan password"))
	if err != nil {
		t.Fatal(err)
	}
	if err := setAuthCatalogValueSync(catalog.db, authCatalogKey("app", "orphan"), orphan); err != nil {
		t.Fatal(err)
	}

	setAuthorizationTestUser(server, 2, "admin", "database-admin")
	assertAuthorizationErrorsIndistinguishable(t, "database updateUser",
		serveAuthorizationCommand(t, server, 2, 61, bson.D{{Key: "updateUser", Value: "missing"}, {Key: "pwd", Value: "password"}, {Key: "$db", Value: "app"}}),
		serveAuthorizationCommand(t, server, 2, 62, bson.D{{Key: "updateUser", Value: "cross-database-user"}, {Key: "pwd", Value: "password"}, {Key: "$db", Value: "app"}}),
	)
	assertAuthorizationErrorsIndistinguishable(t, "database dropUser",
		serveAuthorizationCommand(t, server, 2, 63, bson.D{{Key: "dropUser", Value: "missing"}, {Key: "$db", Value: "app"}}),
		serveAuthorizationCommand(t, server, 2, 64, bson.D{{Key: "dropUser", Value: "cross-database-user"}, {Key: "$db", Value: "app"}}),
	)
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 65, bson.D{{Key: "createUser", Value: "orphan"}, {Key: "pwd", Value: "replacement"}, {Key: "roles", Value: bson.A{"read"}}, {Key: "$db", Value: "app"}}), "Unauthorized")

	setAuthorizationTestUser(server, 3, "admin", "server-user-admin")
	assertAuthorizationErrorsIndistinguishable(t, "server userAdmin updateUser",
		serveAuthorizationCommand(t, server, 3, 66, bson.D{{Key: "updateUser", Value: "missing"}, {Key: "pwd", Value: "password"}, {Key: "$db", Value: "admin"}}),
		serveAuthorizationCommand(t, server, 3, 67, bson.D{{Key: "updateUser", Value: "root"}, {Key: "pwd", Value: "password"}, {Key: "$db", Value: "admin"}}),
	)
	assertAuthorizationErrorsIndistinguishable(t, "server userAdmin dropUser",
		serveAuthorizationCommand(t, server, 3, 68, bson.D{{Key: "dropUser", Value: "missing"}, {Key: "$db", Value: "admin"}}),
		serveAuthorizationCommand(t, server, 3, 69, bson.D{{Key: "dropUser", Value: "root"}, {Key: "$db", Value: "admin"}}),
	)

	setAuthorizationTestUser(server, 1, "admin", "root")
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 70, bson.D{{Key: "updateUser", Value: "missing"}, {Key: "pwd", Value: "password"}, {Key: "$db", Value: "app"}}), "UserNotFound")
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 71, bson.D{{Key: "dropUser", Value: "missing"}, {Key: "$db", Value: "app"}}), "UserNotFound")
	assertCommandError(t, serveAuthorizationCommand(t, server, 1, 72, bson.D{{Key: "createUser", Value: "orphan"}, {Key: "pwd", Value: "replacement"}, {Key: "roles", Value: bson.A{"read"}}, {Key: "$db", Value: "app"}}), "DuplicateKey")
	if _, err := catalog.VerifyPassword("app", "cross-database-user", []byte("cross-database-user password")); err != nil {
		t.Fatal("denied operations changed the protected cross-database user")
	}
	if _, err := catalog.VerifyPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal("denied operations changed the protected server administrator")
	}
}

func TestAuthorizationCreateUserRoleWriteFailureLeavesNoPrivileges(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	catalog.db = &failNextAuthCatalogSetStore{authCatalogStore: catalog.db, failKey: string(authAuthorizationCatalogKey())}
	if catalog.userExists("app", "partial") {
		t.Fatal("partial user exists before createUser")
	}
	response := serveAuthorizationCommand(t, server, 1, 51, bson.D{{Key: "createUser", Value: "partial"}, {Key: "pwd", Value: "password"}, {Key: "roles", Value: bson.A{"readWrite"}}, {Key: "$db", Value: "app"}})
	assertCommandError(t, response, "InternalError")
	if _, err := catalog.VerifyPassword("app", "partial", []byte("password")); err != nil {
		t.Fatalf("partial create verifier unavailable: %v", err)
	}
	roles, err := catalog.UserRoles("app", "partial")
	if err != nil || len(roles) != 0 {
		t.Fatalf("partial create roles=%v err=%v", roles, err)
	}
}

func TestAuthorizationConcurrentDuplicateCreateCannotMixPasswordAndRoles(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	type createAttempt struct {
		password string
		roles    []AuthRoleGrant
		command  wire.Document
	}
	attempts := []createAttempt{
		{
			password: "first password",
			roles:    []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}},
			command:  mustDocument(t, bson.D{{Key: "createUser", Value: "racer"}, {Key: "pwd", Value: "first password"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "app"}, {Key: "collection", Value: "items"}}}}, {Key: "$db", Value: "app"}}),
		},
		{
			password: "second password",
			roles:    []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "app", Collection: "items"}},
			command:  mustDocument(t, bson.D{{Key: "createUser", Value: "racer"}, {Key: "pwd", Value: "second password"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: "app"}, {Key: "collection", Value: "items"}}}}, {Key: "$db", Value: "app"}}),
		},
	}
	type createResult struct {
		index    int
		response wire.Document
		err      error
	}
	start := make(chan struct{})
	results := make(chan createResult, len(attempts))
	for i := range attempts {
		go func(index int) {
			<-start
			response, err := server.commandResponse(context.Background(), "createUser", attempts[index].command, nil, 1)
			results <- createResult{index: index, response: response, err: err}
		}(i)
	}
	close(start)
	winner := -1
	for range attempts {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if mongoCommandResponseOK(result.response) {
			if winner >= 0 {
				t.Fatal("both concurrent createUser requests succeeded")
			}
			winner = result.index
			continue
		}
		if got, _ := bson.Raw(result.response).Lookup("codeName").StringValueOK(); got != "DuplicateKey" {
			t.Fatalf("losing createUser response=%s", bson.Raw(result.response))
		}
	}
	if winner < 0 {
		t.Fatal("neither concurrent createUser request succeeded")
	}
	for i := range attempts {
		_, err := catalog.VerifyPassword("app", "racer", []byte(attempts[i].password))
		if (err == nil) != (i == winner) {
			t.Fatalf("password attempt %d accepted=%v winner=%d", i, err == nil, winner)
		}
	}
	roles, err := catalog.UserRoles("app", "racer")
	if err != nil || !reflect.DeepEqual(roles, attempts[winner].roles) {
		t.Fatalf("concurrent create roles=%v err=%v want %v", roles, err, attempts[winner].roles)
	}
}

func TestAuthorizationUserAdminCannotEscalateOrTakeOverCrossScopeUser(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	for _, username := range []string{"root", "manager", "cross-scope"} {
		if err := catalog.UpsertPassword("admin", username, []byte(username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.SetUserRoles("admin", "manager", []AuthRoleGrant{{Role: AuthRoleUserAdmin, Database: "admin"}, {Role: AuthRoleUserAdmin, Database: "app"}}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "cross-scope", []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "other"}}); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 2, "admin", "manager")
	assertOK(t, serveAuthorizationCommand(t, server, 2, 28, bson.D{{Key: "createUser", Value: "reader"}, {Key: "pwd", Value: "reader password"}, {Key: "roles", Value: bson.A{"read"}}, {Key: "$db", Value: "app"}}))
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 29, bson.D{{Key: "createUser", Value: "escalated"}, {Key: "pwd", Value: "escalated password"}, {Key: "roles", Value: bson.A{"serverAdmin"}}, {Key: "$db", Value: "app"}}), "Unauthorized")
	if catalog.userExists("app", "escalated") {
		t.Fatal("denied privilege escalation created a verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 30, bson.D{{Key: "updateUser", Value: "root"}, {Key: "pwd", Value: "stolen password"}, {Key: "$db", Value: "admin"}}), "Unauthorized")
	if _, err := catalog.VerifyPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal("denied server administrator update changed verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 31, bson.D{{Key: "updateUser", Value: "cross-scope"}, {Key: "pwd", Value: "stolen password"}, {Key: "$db", Value: "admin"}}), "Unauthorized")
	if _, err := catalog.VerifyPassword("admin", "cross-scope", []byte("cross-scope password")); err != nil {
		t.Fatal("denied cross-scope update changed verifier")
	}
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 32, bson.D{{Key: "dropUser", Value: "cross-scope"}, {Key: "$db", Value: "admin"}}), "Unauthorized")
	if !catalog.userExists("admin", "cross-scope") {
		t.Fatal("denied cross-scope drop removed verifier")
	}
}

func TestAuthorizationCursorCannotTransferAcrossUsers(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	for _, username := range []string{"root", "reader", "other"} {
		if err := catalog.UpsertPassword("admin", username, []byte(username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	for _, username := range []string{"reader", "other"} {
		if err := catalog.SetUserRoles("admin", username, []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
			t.Fatal(err)
		}
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	assertOK(t, serveAuthorizationCommand(t, server, 1, 30, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "a"}}, bson.D{{Key: "_id", Value: "b"}}}}, {Key: "$db", Value: "app"}}))
	setAuthorizationTestUser(server, 2, "admin", "reader")
	find := serveAuthorizationCommand(t, server, 2, 31, bson.D{{Key: "find", Value: "items"}, {Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "batchSize", Value: 1}, {Key: "$db", Value: "app"}})
	cursorID := cursorIDFromResponse(t, find)
	if cursorID == 0 {
		t.Fatal("find did not retain cursor")
	}
	setAuthorizationTestUser(server, 2, "admin", "other")
	assertCommandError(t, serveAuthorizationCommand(t, server, 2, 32, bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}}), "CursorNotFound")
	setAuthorizationTestUser(server, 2, "admin", "reader")
	assertOK(t, serveAuthorizationCommand(t, server, 2, 33, bson.D{{Key: "getMore", Value: cursorID}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}}))
}

func TestAuthorizationConcurrentGrantCacheCoherence(t *testing.T) {
	server, first := newAuthorizationTestServer(t)
	second, err := NewAuthCatalog(first.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := first.UpsertPassword("admin", "worker", []byte("worker password")); err != nil {
		t.Fatal(err)
	}
	read := []AuthRoleGrant{{Role: AuthRoleRead, Database: "app"}}
	write := []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "app"}}
	if err := first.SetUserRoles("admin", "worker", read); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 2, "admin", "worker")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			roles := read
			if i%2 != 0 {
				roles = write
			}
			if err := first.SetUserRoles("admin", "worker", roles); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			roles, err := second.UserRoles("admin", "worker")
			if err != nil || len(roles) != 1 || (roles[0].Role != AuthRoleRead && roles[0].Role != AuthRoleReadWrite) {
				errs <- fmt.Errorf("roles=%v err=%v", roles, err)
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	roles, err := second.UserRoles("admin", "worker")
	if err != nil || len(roles) != 1 || roles[0].Role != AuthRoleReadWrite {
		t.Fatalf("second catalog did not observe final write roles=%v err=%v", roles, err)
	}
}

func TestAuthorizationGrantsSurviveReopenAndCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "writer", []byte("writer password")); err != nil {
		t.Fatal(err)
	}
	want := []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "app"}}
	if err := catalog.SetUserRoles("admin", "writer", want); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	got, err := catalog.UserRoles("admin", "writer")
	if err != nil || len(got) != 1 || got[0] != want[0] {
		t.Fatalf("reopened roles=%v err=%v", got, err)
	}
	if err := db.SetSync(authAuthorizationCatalogKey(), []byte(`{"version":1,"users":[`)); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAuthCatalog(db); err == nil {
		t.Fatal("corrupt authorization catalog accepted")
	}
}

func TestAuthorizationClusterProtectedCommandFailsClosed(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	setAuthorizationTestUser(server, 1, "admin", "root")
	server.ClusterSubmitter = &mongoClusterFakeSubmitter{}
	response, err := server.commandResponse(context.Background(), "find", mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, response, "Unauthorized")
}
