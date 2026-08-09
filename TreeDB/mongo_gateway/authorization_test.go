package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

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

func setAuthorizationTestUser(server *Server, owner int64, authDB, username string) {
	user := AuthUser{AuthDB: authDB, Username: username}
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

func TestAuthorizationEverySupportedCommandHasExplicitPrivilege(t *testing.T) {
	public := []string{"hello", "isMaster", "ismaster", "buildInfo", "connectionStatus", "saslStart", "saslContinue", "endSessions", "ping"}
	for _, name := range public {
		target, err := commandAuthorizationTarget(name, mustDocument(t, bson.D{{Key: name, Value: 1}}))
		if err != nil || target.privilege != authorizationPublic {
			t.Fatalf("%s target=%+v err=%v", name, target, err)
		}
	}
	cases := map[string]authorizationPrivilege{
		"hostInfo":        authorizationServerAdmin,
		"find":            authorizationRead,
		"aggregate":       authorizationRead,
		"count":           authorizationRead,
		"distinct":        authorizationRead,
		"getMore":         authorizationRead,
		"killCursors":     authorizationRead,
		"listIndexes":     authorizationMetadataRead,
		"insert":          authorizationWrite,
		"update":          authorizationWrite,
		"delete":          authorizationWrite,
		"findAndModify":   authorizationWrite,
		"create":          authorizationDBAdmin,
		"createIndexes":   authorizationDBAdmin,
		"dropIndexes":     authorizationDBAdmin,
		"listCollections": authorizationListCollections,
		"listDatabases":   authorizationListDatabases,
		"createUser":      authorizationUserAdmin,
		"updateUser":      authorizationUserAdmin,
		"dropUser":        authorizationUserAdmin,
		"usersInfo":       authorizationUserAdmin,
	}
	for name, want := range cases {
		value := any("items")
		if name == "getMore" {
			value = int64(1)
		}
		doc := bson.D{{Key: name, Value: value}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}}
		target, err := commandAuthorizationTarget(name, mustDocument(t, doc))
		if err != nil || target.privilege != want {
			t.Fatalf("%s target=%+v want=%v err=%v", name, target, want, err)
		}
	}
}

func TestAuthorizationBuiltInRoleSeparation(t *testing.T) {
	targets := []authorizationTarget{
		{privilege: authorizationRead, database: "app", collection: "items"},
		{privilege: authorizationWrite, database: "app", collection: "items"},
		{privilege: authorizationDBAdmin, database: "app", collection: "items"},
		{privilege: authorizationUserAdmin, database: "app"},
		{privilege: authorizationServerAdmin},
	}
	cases := []struct {
		role AuthRole
		want []bool
	}{
		{role: AuthRoleRead, want: []bool{true, false, false, false, false}},
		{role: AuthRoleReadWrite, want: []bool{true, true, false, false, false}},
		{role: AuthRoleDBAdmin, want: []bool{false, false, true, false, false}},
		{role: AuthRoleUserAdmin, want: []bool{false, false, false, true, false}},
		{role: AuthRoleServerAdmin, want: []bool{true, true, true, true, true}},
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
	if err := catalog.SetUserRoles("admin", "root", nil); err == nil {
		t.Fatal("last server administrator demotion succeeded")
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

func TestAuthorizationUserAdminCannotEscalatePrivileges(t *testing.T) {
	server, catalog := newAuthorizationTestServer(t)
	for _, username := range []string{"root", "manager"} {
		if err := catalog.UpsertPassword("admin", username, []byte(username+" password")); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.SetUserRoles("admin", "manager", []AuthRoleGrant{{Role: AuthRoleUserAdmin, Database: "admin"}, {Role: AuthRoleUserAdmin, Database: "app"}}); err != nil {
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
