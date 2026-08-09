package mongogateway

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var authorizationBenchmarkAllowed bool

func authorizationAdmissionForBenchmark(server *Server, name string, command wire.Document, owner int64) bool {
	if !server.authenticationRequired() {
		return true
	}
	_, _, _, allowed := server.authorizeCommand(name, command, owner)
	return allowed
}

func newAuthorizationBenchmarkServers(tb testing.TB) (*Server, *Server, *Server) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		tb.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		tb.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "allowed", []byte("allowed password")); err != nil {
		tb.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "denied", []byte("denied password")); err != nil {
		tb.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "allowed", []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "app", Collection: "items"}}); err != nil {
		tb.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "denied", []AuthRoleGrant{{Role: AuthRoleReadWrite, Database: "other", Collection: "items"}}); err != nil {
		tb.Fatal(err)
	}
	plain := NewServer()
	allowed := NewServer()
	allowed.AuthenticationEnabled, allowed.AuthCatalog = true, catalog
	denied := NewServer()
	denied.AuthenticationEnabled, denied.AuthCatalog = true, catalog
	setAuthorizationTestUser(allowed, 1, "admin", "allowed")
	setAuthorizationTestUser(denied, 2, "admin", "denied")
	return plain, allowed, denied
}

func BenchmarkAuthorizationAdmission(b *testing.B) {
	plain, allowed, denied := newAuthorizationBenchmarkServers(b)
	commands := []struct {
		name    string
		command wire.Document
	}{
		{name: "point-read", command: mustDocument(b, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}})},
		{name: "write", command: mustDocument(b, bson.D{{Key: "insert", Value: "items"}, {Key: "$db", Value: "app"}})},
		{name: "cursor", command: mustDocument(b, bson.D{{Key: "getMore", Value: int64(1)}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}})},
		{name: "metadata", command: mustDocument(b, bson.D{{Key: "listCollections", Value: 1}, {Key: "$db", Value: "app"}})},
	}
	for _, tc := range commands {
		commandName, ok := bsonDocumentCommandName(tc.command)
		if !ok {
			b.Fatalf("%s command name unavailable", tc.name)
		}
		for _, mode := range []struct {
			name   string
			server *Server
			owner  int64
			want   bool
		}{
			{name: "plain", server: plain, want: true},
			{name: "allowed", server: allowed, owner: 1, want: true},
			{name: "denied", server: denied, owner: 2, want: false},
		} {
			b.Run(tc.name+"/"+mode.name, func(b *testing.B) {
				if got := authorizationAdmissionForBenchmark(mode.server, commandName, tc.command, mode.owner); got != mode.want {
					b.Fatalf("admission=%v want %v", got, mode.want)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					authorizationBenchmarkAllowed = authorizationAdmissionForBenchmark(mode.server, commandName, tc.command, mode.owner)
				}
			})
		}
	}
}

func TestAuthorizationAllowedCheckIsAllocationFree(t *testing.T) {
	_, allowed, _ := newAuthorizationBenchmarkServers(t)
	command := mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}})
	if !authorizationAdmissionForBenchmark(allowed, "find", command, 1) {
		t.Fatal("allocation check fixture denied the allowed command")
	}
	if got := testing.AllocsPerRun(1000, func() {
		authorizationBenchmarkAllowed = authorizationAdmissionForBenchmark(allowed, "find", command, 1)
	}); got != 0 {
		t.Fatalf("steady-state allowed authorization allocations=%v want 0", got)
	}
}
