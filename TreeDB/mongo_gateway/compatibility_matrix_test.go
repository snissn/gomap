package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoCompatibilityProbe func(*testing.T, *Server)

type mongoCompatibilityMatrixProbe struct {
	capabilityID   string
	expectedStatus MongoCapabilityStatus
	probe          mongoCompatibilityProbe
}

func TestMongoCompatibilityMatrix(t *testing.T) {
	manifest := MongoGatewayCapabilities()
	probes := mongoCompatibilityMatrixProbes()
	if err := validateMongoCompatibilityProbes(manifest, probes); err != nil {
		t.Fatalf("validate compatibility probes: %v", err)
	}

	probeByID := make(map[string]mongoCompatibilityProbe, len(probes))
	for _, row := range probes {
		probeByID[row.capabilityID] = row.probe
	}
	seenNames := make(map[string]int, len(manifest.Capabilities))
	for _, capability := range manifest.Capabilities {
		capability := capability
		baseName := compatibilityTestSlug(capability.Category) + "_" + compatibilityTestSlug(capability.Feature)
		seenNames[baseName]++
		name := baseName
		if seenNames[baseName] > 1 {
			name = fmt.Sprintf("%s_%d", baseName, seenNames[baseName])
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			probeByID[capability.ID](t, newMongoCompatibilityMatrixServer(t))
		})
	}
}

func validateMongoCompatibilityProbes(manifest MongoGatewayCapabilityManifest, probes []mongoCompatibilityMatrixProbe) error {
	if err := ValidateMongoGatewayCapabilityManifest(manifest); err != nil {
		return err
	}
	capabilities := mongoGatewayCapabilityIndex(manifest)
	seen := make(map[string]struct{}, len(probes))
	for _, row := range probes {
		if row.capabilityID == "" {
			return fmt.Errorf("compatibility probe has empty capability id")
		}
		if row.probe == nil {
			return fmt.Errorf("capability %q has nil executable probe", row.capabilityID)
		}
		capability, ok := capabilities[row.capabilityID]
		if !ok {
			return fmt.Errorf("executable probe references unknown capability %q", row.capabilityID)
		}
		if row.expectedStatus == "" {
			return fmt.Errorf("capability %q executable probe has empty expected status", row.capabilityID)
		}
		if capability.Status != row.expectedStatus {
			return fmt.Errorf("capability %q manifest status %q does not match executable probe status %q", row.capabilityID, capability.Status, row.expectedStatus)
		}
		if _, ok := seen[row.capabilityID]; ok {
			return fmt.Errorf("duplicate executable probe for capability %q", row.capabilityID)
		}
		seen[row.capabilityID] = struct{}{}
	}
	for _, capability := range manifest.Capabilities {
		if _, ok := seen[capability.ID]; !ok {
			return fmt.Errorf("capability %q is missing executable probe", capability.ID)
		}
	}
	return nil
}

func mongoCompatibilityMatrixProbes() []mongoCompatibilityMatrixProbe {
	return []mongoCompatibilityMatrixProbe{
		{
			capabilityID:   "security.transport-tls-and-safe-remote-listen",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, _ *Server) {
				if !isLoopbackListener(&net.TCPAddr{IP: net.ParseIP("127.0.0.1")}) {
					t.Fatal("loopback listener classification failed")
				}
				if isLoopbackListener(&net.TCPAddr{IP: net.IPv4zero}) {
					t.Fatal("wildcard listener classified as loopback")
				}
			},
		},
		{
			capabilityID:   "wire.hello-command",
			expectedStatus: MongoCapabilitySupported,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 1, bson.D{{Key: "hello", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				assertBool(t, resp, "helloOk", true)
				assertInt32(t, resp, "logicalSessionTimeoutMinutes", mongoGatewayCapabilityManifest.Advertised.LogicalSessionTimeoutMinutes)
			},
		},
		{
			capabilityID:   "wire.ping-command",
			expectedStatus: MongoCapabilitySupported,
			probe: func(t *testing.T, server *Server) {
				assertOK(t, serveCommand(t, server, 2, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}}))
			},
		},
		{
			capabilityID:   "wire.connectionstatus-command",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 24, bson.D{{Key: "connectionStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				authInfo, ok := resp.Lookup("authInfo").DocumentOK()
				if !ok {
					t.Fatalf("authInfo missing or non-document in %s", resp)
				}
				if _, ok := authInfo.Lookup("authenticatedUsers").ArrayOK(); !ok {
					t.Fatalf("authenticatedUsers missing or non-array in %s", authInfo)
				}
				if _, ok := authInfo.Lookup("authenticatedUserRoles").ArrayOK(); !ok {
					t.Fatalf("authenticatedUserRoles missing or non-array in %s", authInfo)
				}
				if _, ok := authInfo.Lookup("authenticatedUserPrivileges").ArrayOK(); !ok {
					t.Fatalf("authenticatedUserPrivileges missing or non-array in %s", authInfo)
				}
			},
		},
		{
			capabilityID:   "wire.hostinfo-command",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 25, bson.D{{Key: "hostInfo", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				system, ok := resp.Lookup("system").DocumentOK()
				if !ok {
					t.Fatalf("system missing or non-document in %s", resp)
				}
				if _, ok := system.Lookup("hostname").StringValueOK(); !ok {
					t.Fatalf("hostname missing or non-string in %s", system)
				}
				osInfo, ok := resp.Lookup("os").DocumentOK()
				if !ok {
					t.Fatalf("os missing or non-document in %s", resp)
				}
				if _, ok := osInfo.Lookup("type").StringValueOK(); !ok {
					t.Fatalf("os.type missing or non-string in %s", osInfo)
				}
			},
		},
		{
			capabilityID:   "wire.buildinfo-command",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 26, bson.D{{Key: "buildInfo", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				if version, ok := resp.Lookup("version").StringValueOK(); !ok || version != mongoGatewayCapabilityManifest.Advertised.MongoVersion {
					t.Fatalf("version=%q ok=%v want %q", version, ok, mongoGatewayCapabilityManifest.Advertised.MongoVersion)
				}
				if _, ok := resp.Lookup("versionArray").ArrayOK(); !ok {
					t.Fatalf("versionArray missing or non-array in %s", resp)
				}
				if bits, ok := resp.Lookup("bits").Int32OK(); !ok || bits != runtimePointerSizeBits() {
					t.Fatalf("bits=%d ok=%v want %d", bits, ok, runtimePointerSizeBits())
				}
			},
		},
		{
			capabilityID:   "crud.insert-explicit-id",
			expectedStatus: MongoCapabilitySupported,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 3, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u4"}, {Key: "city", Value: "lax"}, {Key: "age", Value: int64(29)}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
			},
		},
		{
			capabilityID:   "crud.find-by-id-equality",
			expectedStatus: MongoCapabilitySupported,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 4, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u1"})
			},
		},
		{
			capabilityID:   "query.indexed-equality-and-range-predicates",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 5, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "$and", Value: bson.A{
						bson.D{{Key: "city", Value: "hnl"}},
						bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int64(40)}}}},
					}}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u2"})
			},
		},
		{
			capabilityID:   "query.in-on-indexed-scalar-fields",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 6, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "city", Value: bson.D{{Key: "$in", Value: bson.A{"sfo", "missing"}}}}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u3"})
			},
		},
		{
			capabilityID:   "query.compound-descending-bson-v2-planner",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				assertOK(t, serveCommand(t, server, 218, bson.D{{Key: "createIndexes", Value: "planner_events"}, {Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "tenant", Value: int32(1)}, {Key: "score", Value: int32(-1)}}}, {Key: "name", Value: "tenant_score"}}}}, {Key: "$db", Value: "app"}}))
				assertOK(t, serveCommand(t, server, 219, bson.D{{Key: "insert", Value: "planner_events"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "low"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(1)}}, bson.D{{Key: "_id", Value: "high"}, {Key: "tenant", Value: "acme"}, {Key: "score", Value: int32(2)}}}}, {Key: "$db", Value: "app"}}))
				resp := serveCommand(t, server, 220, bson.D{{Key: "find", Value: "planner_events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(-1)}}}, {Key: "$db", Value: "app"}})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"high", "low"})
				explain := serveCommand(t, server, 221, bson.D{{Key: "explain", Value: bson.D{{Key: "find", Value: "planner_events"}, {Key: "filter", Value: bson.D{{Key: "tenant", Value: "acme"}}}, {Key: "sort", Value: bson.D{{Key: "score", Value: int32(-1)}}}}}, {Key: "$db", Value: "app"}})
				assertOK(t, explain)
				if stage := bson.Raw(explain).Lookup("queryPlanner").Document().Lookup("winningPlan").Document().Lookup("stage").StringValue(); stage != "compound_index_scan" {
					t.Fatalf("compound planner capability stage=%q want compound_index_scan: %s", stage, explain)
				}
			},
		},
		{
			capabilityID:   "query.top-level-or-expressions",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 61, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "$or", Value: bson.A{
						bson.D{{Key: "city", Value: "hnl"}},
						bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int64(40)}}}},
					}}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u1", "u2"})
			},
		},
		{
			capabilityID:   "query.projection-sort-skip-and-limit",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 7, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{}},
					{Key: "sort", Value: bson.D{{Key: "age", Value: int32(1)}}},
					{Key: "skip", Value: int32(1)},
					{Key: "limit", Value: int32(1)},
					{Key: "projection", Value: bson.D{{Key: "_id", Value: int32(1)}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, resp), []string{"u1"})
			},
		},
		{
			capabilityID:   "cursor.getmore-and-killcursors",
			expectedStatus: MongoCapabilitySupported,
			probe: func(t *testing.T, server *Server) {
				find := serveCommand(t, server, 8, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{}},
					{Key: "sort", Value: bson.D{{Key: "_id", Value: int32(1)}}},
					{Key: "batchSize", Value: int32(1)},
					{Key: "$db", Value: "app"},
				})
				if first := cursorFirstBatch(t, find); len(first) != 1 {
					t.Fatalf("first batch len=%d want 1", len(first))
				}
				cursorID := cursorIDFromResponse(t, find)
				if cursorID == 0 {
					t.Fatal("cursor id=0 want open cursor")
				}
				next := serveCommand(t, server, 9, bson.D{
					{Key: "getMore", Value: cursorID},
					{Key: "collection", Value: "users"},
					{Key: "batchSize", Value: int32(1)},
					{Key: "$db", Value: "app"},
				})
				if batch := cursorNextBatch(t, next); len(batch) != 1 {
					t.Fatalf("next batch len=%d want 1", len(batch))
				}
				kill := serveCommand(t, server, 10, bson.D{
					{Key: "killCursors", Value: "users"},
					{Key: "cursors", Value: bson.A{cursorID}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, kill)
			},
		},
		{
			capabilityID:   "read-concern.local-available-readconcern-maps-to-local-stale",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				find := serveCommand(t, server, 901, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
					{Key: "readConcern", Value: bson.D{{Key: "level", Value: "local"}}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, find), []string{"u1"})
				list := serveCommand(t, server, 902, bson.D{
					{Key: "listCollections", Value: int32(1)},
					{Key: "nameOnly", Value: true},
					{Key: "readConcern", Value: bson.D{{Key: "level", Value: "available"}}},
					{Key: "$db", Value: "app"},
				})
				if batch := cursorFirstBatch(t, list); len(batch) != 1 {
					t.Fatalf("listCollections batch len=%d want 1", len(batch))
				}
			},
		},
		{
			capabilityID:   "read-concern-gap.majority-linearizable-and-snapshot-readconcern",
			expectedStatus: MongoCapabilityRejected,
			probe: func(t *testing.T, server *Server) {
				for i, level := range []string{"majority", "linearizable", "snapshot"} {
					resp := serveCommand(t, server, int32(910+i), bson.D{
						{Key: "find", Value: "users"},
						{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "readConcern", Value: bson.D{{Key: "level", Value: level}}},
						{Key: "$db", Value: "app"},
					})
					assertCommandError(t, resp, "BadValue")
				}
			},
		},
		{
			capabilityID:   "write-concern.standalone-w1-and-journal",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				syncs := 0
				server.standaloneWriteConcernSync = func() (bool, error) {
					syncs++
					return true, nil
				}
				assertOK(t, serveCommand(t, server, 920, bson.D{
					{Key: "insert", Value: "write-concern"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "w1"}}}},
					{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}}},
					{Key: "$db", Value: "app"},
				}))
				assertOK(t, serveCommand(t, server, 921, bson.D{
					{Key: "insert", Value: "write-concern"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "journal"}}}},
					{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
					{Key: "$db", Value: "app"},
				}))
				if syncs != 1 {
					t.Fatalf("journal sync calls=%d want 1", syncs)
				}
			},
		},
		{
			capabilityID:   "write-concern-gap.unacknowledged-replica-and-timeout",
			expectedStatus: MongoCapabilityRejected,
			probe: func(t *testing.T, server *Server) {
				for i, concern := range []bson.D{
					{{Key: "w", Value: int32(0)}},
					{{Key: "w", Value: "majority"}},
					{{Key: "wtimeout", Value: int32(1)}},
				} {
					response := serveCommand(t, server, int32(930+i), bson.D{
						{Key: "insert", Value: "write-concern-rejected"},
						{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: i}}}},
						{Key: "writeConcern", Value: concern},
						{Key: "$db", Value: "app"},
					})
					assertCommandError(t, response, "WriteConcernFailed")
				}
				if _, err := server.Collections.OpenCollection("app.write-concern-rejected"); !errors.Is(err, collections.ErrCollectionNotFound) {
					t.Fatalf("rejected write concern collection err=%v, want not found", err)
				}
			},
		},
		{
			capabilityID:   "crud.updateone-set-by-id",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 11, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 1)
			},
		},
		{
			capabilityID:   "crud.delete-by-id",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 12, bson.D{
					{Key: "delete", Value: "users"},
					{Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "limit", Value: int32(1)}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
			},
		},
		{
			capabilityID:   "metadata.listcollections",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 13, bson.D{
					{Key: "listCollections", Value: int32(1)},
					{Key: "nameOnly", Value: true},
					{Key: "$db", Value: "app"},
				})
				batch := cursorFirstBatch(t, resp)
				if len(batch) != 1 {
					t.Fatalf("collection batch len=%d want 1", len(batch))
				}
			},
		},
		{
			capabilityID:   "metadata.listdatabases",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 130, bson.D{
					{Key: "listDatabases", Value: int32(1)},
					{Key: "nameOnly", Value: true},
					{Key: "filter", Value: bson.D{{Key: "name", Value: "app"}}},
					{Key: "$db", Value: "admin"},
				})
				assertOK(t, resp)
				databases, ok := resp.Lookup("databases").ArrayOK()
				if !ok {
					t.Fatalf("databases missing or non-array in %s", resp)
				}
				values, err := databases.Values()
				if err != nil {
					t.Fatalf("databases values: %v", err)
				}
				if len(values) != 1 {
					t.Fatalf("databases len=%d want 1", len(values))
				}
				doc, ok := values[0].DocumentOK()
				if !ok {
					t.Fatalf("databases[0] is not a document")
				}
				if got, ok := doc.Lookup("name").StringValueOK(); !ok || got != "app" {
					t.Fatalf("database name=%q ok=%v want app", got, ok)
				}
			},
		},
		{
			capabilityID:   "metadata.create-collection",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				create := serveCommand(t, server, 131, bson.D{
					{Key: "create", Value: "matrix_created"},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, create)
				list := serveCommand(t, server, 132, bson.D{
					{Key: "listCollections", Value: int32(1)},
					{Key: "filter", Value: bson.D{{Key: "name", Value: "matrix_created"}}},
					{Key: "nameOnly", Value: true},
					{Key: "$db", Value: "app"},
				})
				batch := cursorFirstBatch(t, list)
				if len(batch) != 1 {
					t.Fatalf("created collection batch len=%d want 1", len(batch))
				}
				if got, ok := batch[0].Lookup("name").StringValueOK(); !ok || got != "matrix_created" {
					t.Fatalf("created collection name=%q ok=%v want matrix_created", got, ok)
				}
			},
		},
		{
			capabilityID:   "session.logical-session-handshake-and-endsessions",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				hello := serveCommand(t, server, 133, bson.D{{Key: "hello", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, hello)
				assertInt32(t, hello, "logicalSessionTimeoutMinutes", mongoGatewayCapabilityManifest.Advertised.LogicalSessionTimeoutMinutes)
				end := serveCommand(t, server, 134, bson.D{
					{Key: "endSessions", Value: bson.A{bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}}},
					{Key: "$db", Value: "admin"},
				})
				assertOK(t, end)
			},
		},
		{
			capabilityID:   "metadata.createindexes-listindexes-and-dropindexes",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				list := serveCommand(t, server, 14, bson.D{{Key: "listIndexes", Value: "users"}, {Key: "$db", Value: "app"}})
				assertIndexNameSet(t, cursorFirstBatch(t, list), []string{"_id_", "city_1", "age_1", "score_1"})
				create := serveCommand(t, server, 141, bson.D{
					{Key: "createIndexes", Value: "users"},
					{Key: "indexes", Value: bson.A{bson.D{
						{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}, {Key: "age", Value: int32(-1)}}},
						{Key: "name", Value: "city_1_age_-1"},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, create)
				list = serveCommand(t, server, 142, bson.D{{Key: "listIndexes", Value: "users"}, {Key: "$db", Value: "app"}})
				assertIndexNameSet(t, cursorFirstBatch(t, list), []string{"_id_", "city_1", "age_1", "score_1", "city_1_age_-1"})
				drop := serveCommand(t, server, 15, bson.D{{Key: "dropIndexes", Value: "users"}, {Key: "index", Value: "score_1"}, {Key: "$db", Value: "app"}})
				assertOK(t, drop)
				drop = serveCommand(t, server, 151, bson.D{{Key: "dropIndexes", Value: "users"}, {Key: "index", Value: "city_1_age_-1"}, {Key: "$db", Value: "app"}})
				assertOK(t, drop)
			},
		},
		{
			capabilityID:   "document.native-bson-storage-mode",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 16, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "binary"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				find := serveCommand(t, server, 161, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "binary"}}},
					{Key: "$db", Value: "app"},
				})
				batch := cursorFirstBatch(t, find)
				if len(batch) != 1 {
					t.Fatalf("native BSON batch len=%d want 1", len(batch))
				}
				subtype, payload := batch[0].Lookup("payload").Binary()
				if subtype != 0 || !bytes.Equal(payload, []byte{1, 2, 3}) {
					t.Fatalf("payload subtype/data=%#x/%v want 0/[1 2 3]", subtype, payload)
				}
			},
		},
		{
			capabilityID:   "query-gap.dotted-projection",
			expectedStatus: MongoCapabilityRejected,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 18, bson.D{
					{Key: "find", Value: "users"},
					{Key: "projection", Value: bson.D{{Key: "profile.name", Value: int32(1)}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
			},
		},
		{
			capabilityID:   "update-subset.natural-order-arbitrary-filter-update-delete-and-findandmodify",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				assertOK(t, serveCommand(t, server, 209, bson.D{{Key: "insert", Value: "filter_writes"}, {Key: "documents", Value: bson.A{
					bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int32(20)}, {Key: "active", Value: true}},
					bson.D{{Key: "_id", Value: "u2"}, {Key: "age", Value: int32(30)}, {Key: "active", Value: true}},
				}}, {Key: "$db", Value: "app"}}))
				update := serveCommand(t, server, 210, bson.D{{Key: "update", Value: "filter_writes"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(20)}}}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "picked", Value: true}}}}}}}}, {Key: "$db", Value: "app"}})
				assertOK(t, update)
				assertInt32(t, update, "n", 1)
				fam := serveCommand(t, server, 211, bson.D{{Key: "findAndModify", Value: "filter_writes"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "modified", Value: true}}}}}, {Key: "$db", Value: "app"}})
				assertOK(t, fam)
				if id, _ := bson.Raw(fam).Lookup("value").Document().Lookup("_id").StringValueOK(); id != "u1" {
					t.Fatalf("findAndModify id=%q want u1", id)
				}
				deleted := serveCommand(t, server, 212, bson.D{{Key: "delete", Value: "filter_writes"}, {Key: "deletes", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "age", Value: int32(20)}}}, {Key: "limit", Value: int32(1)}}}}, {Key: "$db", Value: "app"}})
				assertOK(t, deleted)
				assertInt32(t, deleted, "n", 1)
			},
		},
		{
			capabilityID:   "update.exact-id-upsert",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 19, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "upsert-matrix"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "city", Value: "sea"}}}}},
						{Key: "upsert", Value: true},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 0)
				upserted, ok := bson.Raw(resp).Lookup("upserted").ArrayOK()
				if !ok {
					t.Fatalf("missing upserted: %v", resp)
				}
				values, err := upserted.Values()
				if err != nil || len(values) != 1 {
					t.Fatalf("upserted=%v err=%v", values, err)
				}
				if id, ok := values[0].Document().Lookup("_id").StringValueOK(); !ok || id != "upsert-matrix" {
					t.Fatalf("upserted _id=%q ok=%v", id, ok)
				}
			},
		},
		{
			capabilityID:   "crud.bounded-multi-write-and-batch-ordering",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				// Prove the advertised multi-match subset rather than just accepting
				// multi:true on a one-document exact-id query.
				resp := serveCommand(t, server, 20, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "city", Value: "hnl"}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "multiProbe", Value: true}}}}},
						{Key: "multi", Value: true},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 2)

				ordered := serveCommand(t, server, 201, bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
					bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "city", Value: int32(1)}}}}}},
					bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "orderedLater", Value: true}}}}}},
				}}, {Key: "$db", Value: "app"}})
				assertIndexedWriteError(t, ordered, 0)
				assertInt32(t, ordered, "n", 0)

				unordered := serveCommand(t, server, 202, bson.D{{Key: "update", Value: "users"}, {Key: "ordered", Value: false}, {Key: "updates", Value: bson.A{
					bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "city", Value: int32(1)}}}}}},
					bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "unorderedLater", Value: true}}}}}},
				}}, {Key: "$db", Value: "app"}})
				assertIndexedWriteError(t, unordered, 0)
				assertInt32(t, unordered, "n", 1)
			},
		},
		{
			capabilityID:   "update.inc",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 21, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "age", Value: int32(1)}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 1)
			},
		},
		{
			capabilityID:   "update.unset",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 211, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "$unset", Value: bson.D{{Key: "city", Value: true}}}}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 1)
			},
		},
		{
			capabilityID:   "update.nested-set-unset-inc-and-bounded-array-modifiers-no-numeric-array-index-paths",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 213, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{
							{Key: "$set", Value: bson.D{{Key: "profile.name", Value: "ada"}}},
							{Key: "$unset", Value: bson.D{{Key: "profile.old", Value: true}}},
							{Key: "$inc", Value: bson.D{{Key: "profile.logins", Value: int32(1)}}},
							{Key: "$push", Value: bson.D{{Key: "events", Value: "login"}}},
							{Key: "$addToSet", Value: bson.D{{Key: "labels", Value: bson.D{{Key: "$each", Value: bson.A{"staff", "staff"}}}}}},
						}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 1)
			},
		},
		{
			capabilityID:   "update.replaceone-by-exact-id",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 212, bson.D{
					{Key: "update", Value: "users"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
						{Key: "u", Value: bson.D{{Key: "name", Value: "replacement"}}},
					}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				assertInt32(t, resp, "n", 1)
				assertInt32(t, resp, "nModified", 1)
			},
		},
		{
			capabilityID:   "index.bson-ordered-v2-without-treedbvaluetype",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 23, bson.D{
					{Key: "createIndexes", Value: "users"},
					{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
			},
		},
		{
			capabilityID:   "read-command.aggregate-match-project-sort-skip-limit-count",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				staged := serveCommand(t, server, 25, bson.D{
					{Key: "aggregate", Value: "users"},
					{Key: "pipeline", Value: bson.A{
						bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
						bson.D{{Key: "$sort", Value: bson.D{{Key: "age", Value: int32(-1)}}}},
						bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: int32(1)}}}},
						bson.D{{Key: "$skip", Value: int32(1)}},
						bson.D{{Key: "$limit", Value: int32(1)}},
					}},
					{Key: "cursor", Value: bson.D{}},
					{Key: "$db", Value: "app"},
				})
				assertBatchIDs(t, cursorFirstBatch(t, staged), []string{"u3"})

				counted := serveCommand(t, server, 251, bson.D{
					{Key: "aggregate", Value: "users"},
					{Key: "pipeline", Value: bson.A{
						bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
						bson.D{{Key: "$count", Value: "n"}},
					}},
					{Key: "cursor", Value: bson.D{}},
					{Key: "$db", Value: "app"},
				})
				batch := cursorFirstBatch(t, counted)
				if len(batch) != 1 {
					t.Fatalf("aggregate batch len=%d want 1", len(batch))
				}
				if n, ok := batch[0].Lookup("n").Int64OK(); !ok || n != 2 {
					t.Fatalf("aggregate count n=%d ok=%v want 2", n, ok)
				}
			},
		},
		{
			capabilityID:   "diagnostics.serverstatus",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 252, bson.D{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				if _, ok := bson.Raw(resp).Lookup("connections").DocumentOK(); !ok {
					t.Fatalf("serverStatus missing connections: %s", resp)
				}
			},
		},
		{
			capabilityID:   "diagnostics.top",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 253, bson.D{{Key: "top", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, resp)
				if _, ok := bson.Raw(resp).Lookup("totals").DocumentOK(); !ok {
					t.Fatalf("top missing totals: %s", resp)
				}
			},
		},
		{
			capabilityID:   "diagnostics.dbstats-and-collstats",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				assertOK(t, serveCommand(t, server, 254, bson.D{{Key: "dbStats", Value: int32(1)}, {Key: "$db", Value: "app"}}))
				assertOK(t, serveCommand(t, server, 255, bson.D{{Key: "collStats", Value: "users"}, {Key: "$db", Value: "app"}}))
			},
		},
		{
			capabilityID:   "read-command.count-filter-skip-limit",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				for requestID, pagination := range []bson.E{{Key: "skip", Value: int32(1)}, {Key: "limit", Value: int32(1)}} {
					command := bson.D{{Key: "count", Value: "users"}, {Key: "query", Value: bson.D{{Key: "active", Value: true}}}, pagination, {Key: "$db", Value: "app"}}
					resp := serveCommand(t, server, int32(26+requestID), command)
					assertOK(t, resp)
					if n, ok := bson.Raw(resp).Lookup("n").Int64OK(); !ok || n != 1 {
						t.Fatalf("count %s n=%d ok=%v want 1", pagination.Key, n, ok)
					}
				}
			},
		},
		{
			capabilityID:   "read-command.distinct-top-level-field-with-filter",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 261, bson.D{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "query", Value: bson.D{{Key: "active", Value: false}}}, {Key: "$db", Value: "app"}})
				assertOK(t, resp)
				values, err := bson.Raw(resp).Lookup("values").Array().Values()
				if err != nil || len(values) != 1 || values[0].StringValue() != "hnl" {
					t.Fatalf("distinct values=%v err=%v", values, err)
				}
			},
		},
		{
			capabilityID:   "read-command.explain-bounded-read-plans",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 922, bson.D{
					{Key: "explain", Value: bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}}},
					{Key: "verbosity", Value: "executionStats"}, {Key: "$db", Value: "app"},
				})
				assertOK(t, resp)
				planner, ok := resp.Lookup("queryPlanner").DocumentOK()
				if !ok {
					t.Fatalf("explain queryPlanner missing: %s", resp)
				}
				winning, ok := planner.Lookup("winningPlan").DocumentOK()
				if stage, ok := winning.Lookup("stage").StringValueOK(); !ok || stage != "primary_lookup" {
					t.Fatalf("explain stage=%q ok=%v want primary_lookup", stage, ok)
				}
				if _, ok := resp.Lookup("executionStats").DocumentOK(); !ok {
					t.Fatalf("explain executionStats missing: %s", resp)
				}
			},
		},
		{
			capabilityID:   "read-command-gap.maxtimems-on-aggregate-count-distinct",
			expectedStatus: MongoCapabilityRejected,
			probe: func(t *testing.T, server *Server) {
				commands := []bson.D{
					{{Key: "aggregate", Value: "users"}, {Key: "pipeline", Value: bson.A{}}, {Key: "cursor", Value: bson.D{}}, {Key: "maxTimeMS", Value: int64(1)}, {Key: "$db", Value: "app"}},
					{{Key: "count", Value: "users"}, {Key: "maxTimeMS", Value: int64(1)}, {Key: "$db", Value: "app"}},
					{{Key: "distinct", Value: "users"}, {Key: "key", Value: "city"}, {Key: "maxTimeMS", Value: int64(1)}, {Key: "$db", Value: "app"}},
				}
				for i, command := range commands {
					assertCommandError(t, serveCommand(t, server, int32(262+i), command), "BadValue")
				}
			},
		},
		{
			capabilityID:   "update-subset.findandmodify-exact-id-no-match",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 26, bson.D{{Key: "findAndModify", Value: "users"}, {Key: "query", Value: bson.D{{Key: "_id", Value: "none"}}}, {Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "none"}}}}}, {Key: "$db", Value: "app"}})
				assertOK(t, resp)
				if bson.Raw(resp).Lookup("value").Type != bson.TypeNull {
					t.Fatalf("value type=%v want null", bson.Raw(resp).Lookup("value").Type)
				}
			},
		},
		{
			capabilityID:   "transaction-gap.transactions-and-retryable-writes",
			expectedStatus: MongoCapabilityNotImplemented,
			probe: func(t *testing.T, server *Server) {
				resp := serveCommand(t, server, 27, bson.D{
					{Key: "insert", Value: "tx_users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "tx1"}}}},
					{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
					{Key: "txnNumber", Value: int64(1)},
					{Key: "startTransaction", Value: true},
					{Key: "autocommit", Value: false},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
				resp = serveCommand(t, server, 29, bson.D{
					{Key: "insert", Value: "retry_users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "retry1"}}}},
					{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
					{Key: "txnNumber", Value: int64(2)},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
				resp = serveCommand(t, server, 30, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
					{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: make([]byte, 16)}}}},
					{Key: "txnNumber", Value: int64(3)},
					{Key: "startTransaction", Value: true},
					{Key: "autocommit", Value: false},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, resp, "BadValue")
				resp = serveCommand(t, server, 28, bson.D{{Key: "commitTransaction", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertCommandError(t, resp, "CommandNotFound")
			},
		},
		{
			capabilityID:   "security.authentication-scram-sha-256",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				server.AuthenticationEnabled = true
				if err := server.AuthCatalog.UpsertPassword("admin", "matrix", []byte("correct horse battery staple")); err != nil {
					t.Fatal(err)
				}
				assertCommandError(t, serveCommand(t, server, 31, bson.D{
					{Key: "saslStart", Value: int32(1)},
					{Key: "mechanism", Value: "SCRAM-SHA-256"},
					{Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,")}},
					{Key: "$db", Value: "admin"},
				}), "AuthenticationFailed")
				find := serveCommand(t, server, 32, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
					{Key: "$db", Value: "app"},
				})
				assertCommandError(t, find, "Unauthorized")
				authenticateUser(t, server, 1, "matrix", []byte("correct horse battery staple"))
				find = serveCommand(t, server, 32, bson.D{
					{Key: "find", Value: "users"},
					{Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, find)
			},
		},
		{
			capabilityID:   "security.authorization-built-in-roles",
			expectedStatus: MongoCapabilitySupportedSubset,
			probe: func(t *testing.T, server *Server) {
				if err := server.AuthCatalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
					t.Fatal(err)
				}
				if err := server.AuthCatalog.UpsertPassword("admin", "reader", []byte("reader password")); err != nil {
					t.Fatal(err)
				}
				if err := server.AuthCatalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "users"}}); err != nil {
					t.Fatal(err)
				}
				server.AuthenticationEnabled = true
				authenticateUser(t, server, 1, "reader", []byte("reader password"))
				assertOK(t, serveCommand(t, server, 34, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{}}, {Key: "$db", Value: "app"}}))
				assertCommandError(t, serveCommand(t, server, 35, bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "denied"}}}}, {Key: "$db", Value: "app"}}), "Unauthorized")
			},
		},
		{
			capabilityID:   "cluster-gap.replica-set-and-sharding-advertisement",
			expectedStatus: MongoCapabilityNotImplemented,
			probe: func(t *testing.T, server *Server) {
				hello := serveCommand(t, server, 33, bson.D{{Key: "hello", Value: int32(1)}, {Key: "$db", Value: "admin"}})
				assertOK(t, hello)
				for _, field := range []string{"setName", "hosts", "passives", "arbiters", "serviceId", "msg"} {
					if value := hello.Lookup(field); value.Type != 0 {
						t.Fatalf("standalone hello unexpectedly advertises %s=%v", field, value)
					}
				}
			},
		},
	}
}

func newMongoCompatibilityMatrixServer(tb testing.TB) *Server {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })

	server := NewServer()
	server.AuthCatalog, err = NewAuthCatalog(db)
	if err != nil {
		tb.Fatalf("new auth catalog: %v", err)
	}
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatBSON,
	}
	assertOK(tb, serveCommand(tb, server, 1001, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "score", Value: int32(1)}}}, {Key: "name", Value: "score_1"}, {Key: "treedbValueType", Value: "double"}},
		}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(tb, serveCommand(tb, server, 1002, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(37)}, {Key: "score", Value: 1.25}, {Key: "active", Value: true}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}, {Key: "age", Value: int64(42)}, {Key: "score", Value: 2.5}, {Key: "active", Value: false}},
			bson.D{{Key: "_id", Value: "u3"}, {Key: "city", Value: "sfo"}, {Key: "age", Value: int64(36)}, {Key: "score", Value: 3.75}, {Key: "active", Value: true}},
		}},
		{Key: "$db", Value: "app"},
	}))
	return server
}

func expectCommandNotFound(command bson.D) mongoCompatibilityProbe {
	return func(t *testing.T, server *Server) {
		resp := serveCommand(t, server, 2000, command)
		assertCommandError(t, resp, "CommandNotFound")
	}
}

func assertIndexNameSet(tb testing.TB, batch []bson.Raw, want []string) {
	tb.Helper()
	if len(batch) != len(want) {
		tb.Fatalf("index batch len=%d want %d", len(batch), len(want))
	}
	got := make(map[string]struct{}, len(batch))
	for i, doc := range batch {
		name, ok := doc.Lookup("name").StringValueOK()
		if !ok {
			tb.Fatalf("index batch[%d] missing string name: %s", i, doc)
		}
		got[name] = struct{}{}
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			tb.Fatalf("index batch missing %q; got %v", name, got)
		}
	}
}

func compatibilityTestSlug(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
