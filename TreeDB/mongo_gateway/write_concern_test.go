package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneWriteConcernAcceptedShapes(t *testing.T) {
	tests := []struct {
		name        string
		concern     any
		wantSync    int
		wantVisible uint64
		wantJournal uint64
	}{
		{name: "absent", wantVisible: 1},
		{name: "empty", concern: bson.D{}, wantVisible: 1},
		{name: "w one", concern: bson.D{{Key: "w", Value: int32(1)}}, wantVisible: 1},
		{name: "journal false", concern: bson.D{{Key: "w", Value: int64(1)}, {Key: "j", Value: false}}, wantVisible: 1},
		{name: "journal true", concern: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: true}}, wantSync: 1, wantJournal: 1},
		{name: "journal default w", concern: bson.D{{Key: "j", Value: true}}, wantSync: 1, wantJournal: 1},
		{name: "zero timeout", concern: bson.D{{Key: "wtimeout", Value: int64(0)}}, wantVisible: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newWriteConcernTestServer(t)
			syncs := 0
			server.standaloneWriteConcernSync = func() (bool, error) {
				syncs++
				return true, nil
			}
			command := bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: tc.name}}}},
			}
			if tc.concern != nil {
				command = append(command, bson.E{Key: "writeConcern", Value: tc.concern})
			}
			command = append(command, bson.E{Key: "$db", Value: "app"})
			assertOK(t, serveCommand(t, server, 1, command))
			if syncs != tc.wantSync {
				t.Fatalf("sync calls=%d want %d", syncs, tc.wantSync)
			}
			stats := server.StandaloneWriteConcernStats()
			if stats.VisibleAcknowledgements != tc.wantVisible || stats.JournalAcknowledgements != tc.wantJournal {
				t.Fatalf("stats=%+v want visible=%d journal=%d", stats, tc.wantVisible, tc.wantJournal)
			}
		})
	}
}

func TestParseStandaloneWriteConcernDefaultsAreAllocationFree(t *testing.T) {
	for _, tc := range []struct {
		name    string
		concern any
	}{
		{name: "absent"},
		{name: "empty", concern: bson.D{}},
		{name: "w one", concern: bson.D{{Key: "w", Value: int32(1)}}},
		{name: "journal false", concern: bson.D{{Key: "j", Value: false}}},
		{name: "zero timeout", concern: bson.D{{Key: "wtimeout", Value: int32(0)}}},
		{name: "combined", concern: bson.D{{Key: "w", Value: int64(1)}, {Key: "j", Value: false}, {Key: "wtimeout", Value: int64(0)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commandDocument := bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
			}
			if tc.concern != nil {
				commandDocument = append(commandDocument, bson.E{Key: "writeConcern", Value: tc.concern})
			}
			commandDocument = append(commandDocument, bson.E{Key: "$db", Value: "app"})
			command := mustDocument(t, commandDocument)
			var parseErr error
			allocs := testing.AllocsPerRun(1000, func() {
				_, parseErr = parseStandaloneWriteConcern(command)
			})
			if parseErr != nil {
				t.Fatalf("parse default writeConcern: %v", parseErr)
			}
			if allocs != 0 {
				t.Fatalf("parse default writeConcern allocations=%v want 0", allocs)
			}
		})
	}
}

func TestStandaloneWriteConcernSharedFinalizationCoversEveryWriteCommand(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *Server)
		command bson.D
	}{
		{name: "create", command: bson.D{{Key: "create", Value: "users"}}},
		{name: "insert", command: bson.D{{Key: "insert", Value: "users"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "journaled"}}}}}},
		{name: "update", prepare: prepareWriteConcernDocument, command: bson.D{
			{Key: "update", Value: "users"},
			{Key: "updates", Value: bson.A{bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "updated"}}}}},
			}}},
		}},
		{name: "delete", prepare: prepareWriteConcernDocument, command: bson.D{
			{Key: "delete", Value: "users"},
			{Key: "deletes", Value: bson.A{bson.D{
				{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}},
				{Key: "limit", Value: int32(1)},
			}}},
		}},
		{name: "findAndModify", prepare: prepareWriteConcernDocument, command: bson.D{
			{Key: "findAndModify", Value: "users"},
			{Key: "query", Value: bson.D{{Key: "_id", Value: "u1"}}},
			{Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "updated"}}}}},
		}},
		{name: "createIndexes", prepare: prepareWriteConcernDocument, command: bson.D{
			{Key: "createIndexes", Value: "users"},
			{Key: "indexes", Value: bson.A{bson.D{
				{Key: "key", Value: bson.D{{Key: "age", Value: int64(1)}}},
				{Key: "name", Value: "age_1"},
				{Key: "treedbValueType", Value: "int64"},
			}}},
		}},
		{name: "dropIndexes", prepare: prepareWriteConcernIndex, command: bson.D{{Key: "dropIndexes", Value: "users"}, {Key: "index", Value: "age_1"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newWriteConcernTestServer(t)
			if tc.prepare != nil {
				tc.prepare(t, server)
			}
			syncs := 0
			server.standaloneWriteConcernSync = func() (bool, error) {
				syncs++
				return true, nil
			}
			command := append(append(bson.D(nil), tc.command...), bson.E{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}}, bson.E{Key: "$db", Value: "app"})
			assertOK(t, serveCommand(t, server, 10, command))
			stats := server.StandaloneWriteConcernStats()
			if syncs != 1 || stats.JournalRequests != 1 || stats.JournalAcknowledgements != 1 || stats.SyncAttempts != 1 {
				t.Fatalf("shared finalization syncs=%d stats=%+v", syncs, stats)
			}
		})
	}
}

func TestStandaloneJournalWriteConcernFinalizesPartialCreateIndexesError(t *testing.T) {
	server, _ := newWriteConcernTestServer(t)
	seedWriteConcernDuplicateEmails(t, server, false)
	before := server.StandaloneWriteConcernStats()
	syncs := 0
	server.standaloneWriteConcernSync = func() (bool, error) {
		syncs++
		return true, nil
	}
	response := serveCommand(t, server, 2, writeConcernPartialCreateIndexesCommand())
	assertCommandError(t, response, "DuplicateKey")
	stats := server.StandaloneWriteConcernStats()
	if syncs != 1 || stats.JournalAcknowledgements != before.JournalAcknowledgements+1 ||
		stats.PhysicalSyncBoundaries != before.PhysicalSyncBoundaries+1 || stats.LogicalWrites != before.LogicalWrites {
		t.Fatalf("partial createIndexes stats=%+v", stats)
	}
	collection, err := server.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findIndexDefinition(collection.MetaView().Indexes, "age_1"); !ok {
		t.Fatal("partial createIndexes did not retain first index")
	}
	if _, ok := findIndexDefinition(collection.MetaView().Indexes, "email_1"); ok {
		t.Fatal("duplicate-key createIndexes unexpectedly retained failing index")
	}
	if !server.standaloneWriteBoundaryMu.TryLock() {
		t.Fatal("journal command error left the standalone write boundary locked")
	}
	server.standaloneWriteBoundaryMu.Unlock()
}

func TestStandaloneWriteConcernBoundaryConcurrencyPolicy(t *testing.T) {
	server, _ := newWriteConcernTestServer(t)
	visible := mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "visible"}}}},
		{Key: "$db", Value: "app"},
	})

	// A second reader can enter while this reader is held, so ordinary visible
	// acknowledgements retain their pre-writeConcern concurrency.
	server.standaloneWriteBoundaryMu.RLock()
	visibleDone := make(chan error, 1)
	go func() {
		response, err := server.commandResponse(context.Background(), "insert", visible, nil, 0)
		if err == nil && !mongoCommandResponseOK(response) {
			err = fmt.Errorf("visible response: %s", response)
		}
		visibleDone <- err
	}()
	select {
	case err := <-visibleDone:
		if err != nil {
			server.standaloneWriteBoundaryMu.RUnlock()
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		server.standaloneWriteBoundaryMu.RUnlock()
		t.Fatal("visible acknowledgement did not overlap an existing standalone reader")
	}
	server.standaloneWriteBoundaryMu.RUnlock()

	// The journal sync hook runs while the writer lock is held. No ordinary
	// standalone write can enter until the mutation and boundary finish.
	enteredSync := make(chan struct{})
	releaseSync := make(chan struct{})
	server.standaloneWriteConcernSync = func() (bool, error) {
		close(enteredSync)
		<-releaseSync
		return true, nil
	}
	journal := mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "journal"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	})
	journalDone := make(chan error, 1)
	go func() {
		response, err := server.commandResponse(context.Background(), "insert", journal, nil, 0)
		if err == nil && !mongoCommandResponseOK(response) {
			err = fmt.Errorf("journal response: %s", response)
		}
		journalDone <- err
	}()
	select {
	case <-enteredSync:
	case <-time.After(time.Second):
		t.Fatal("journal command did not reach its sync boundary")
	}
	if server.standaloneWriteBoundaryMu.TryRLock() {
		server.standaloneWriteBoundaryMu.RUnlock()
		t.Fatal("ordinary reader entered while journal mutation boundary was active")
	}
	close(releaseSync)
	if err := <-journalDone; err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneWriteConcernBoundaryDoesNotSerializeClusterWrites(t *testing.T) {
	submitter := &mongoClusterFakeSubmitter{}
	server := NewServer()
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	setMongoClusterTestSubmitter(server, submitter, 47)
	command := mustDocument(t, bson.D{
		{Key: "create", Value: "users"},
		{Key: "$db", Value: "app"},
	})

	server.standaloneWriteBoundaryMu.Lock()
	done := make(chan error, 1)
	go func() {
		response, err := server.commandResponse(context.Background(), "create", command, nil, 0)
		if err == nil && !mongoCommandResponseOK(response) {
			err = fmt.Errorf("cluster response: %s", response)
		}
		done <- err
	}()
	select {
	case err := <-done:
		server.standaloneWriteBoundaryMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		server.standaloneWriteBoundaryMu.Unlock()
		t.Fatal("cluster write waited on the standalone-only mutation boundary")
	}
	if calls := submitter.snapshotCalls(); len(calls) != 1 {
		t.Fatalf("cluster submit calls=%d want 1", len(calls))
	}
	if stats := server.StandaloneWriteConcernStats(); stats.Requests != 0 {
		t.Fatalf("cluster write changed standalone stats: %+v", stats)
	}
}

func TestStandaloneWriteConcernBoundaryUnlocksAfterSyncPanic(t *testing.T) {
	server, _ := newWriteConcernTestServer(t)
	server.standaloneWriteConcernSync = func() (bool, error) {
		panic("injected sync panic")
	}
	command := mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	})

	panicked := false
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				panicked = true
			}
		}()
		_, _ = server.commandResponse(context.Background(), "insert", command, nil, 0)
	}()
	if !panicked {
		t.Fatal("injected sync panic did not propagate")
	}
	if !server.standaloneWriteBoundaryMu.TryLock() {
		t.Fatal("sync panic left the standalone write boundary locked")
	}
	server.standaloneWriteBoundaryMu.Unlock()
}

func TestStandaloneJournalWriteConcernPartialErrorCrashReopen(t *testing.T) {
	if os.Getenv("TREEDB_MONGO_WC_PARTIAL_CRASH_HELPER") == "1" {
		standalone, err := OpenStandaloneServer(StandaloneOptions{
			Dir:     os.Getenv("TREEDB_MONGO_WC_PARTIAL_DIR"),
			Profile: treedb.Profile(os.Getenv("TREEDB_MONGO_WC_PARTIAL_PROFILE")),
			DefaultCollectionOptions: collections.CollectionOptions{
				DocumentFormat: collections.DocumentFormatBSON,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		seedWriteConcernDuplicateEmails(t, standalone.Server, true)
		response := serveCommand(t, standalone.Server, 2, writeConcernPartialUpdateCommand())
		if ok, okOK := bson.Raw(response).Lookup("ok").DoubleOK(); !okOK || ok != 1 {
			t.Fatalf("partial response=%s", response)
		}
		if !bson.Raw(response).Lookup("writeConcernError").IsZero() {
			t.Fatalf("successful partial-error sync returned writeConcernError: %s", response)
		}
		os.Exit(0)
	}

	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestStandaloneJournalWriteConcernPartialErrorCrashReopen$")
			cmd.Env = append(os.Environ(),
				"TREEDB_MONGO_WC_PARTIAL_CRASH_HELPER=1",
				"TREEDB_MONGO_WC_PARTIAL_DIR="+dir,
				"TREEDB_MONGO_WC_PARTIAL_PROFILE="+string(profile),
			)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("partial-error crash helper: %v\n%s", err, output)
			}
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     dir,
				Profile: profile,
				DefaultCollectionOptions: collections.CollectionOptions{
					DocumentFormat: collections.DocumentFormatBSON,
				},
			})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = standalone.Close() }()
			if !writeConcernMarker(t, standalone.Server, "u1") {
				t.Fatal("reopen lost marker after first partial update")
			}
			if writeConcernMarker(t, standalone.Server, "u2") {
				t.Fatal("reopen applied marker after failing update item")
			}
		})
	}
}

func TestStandaloneJournalWriteConcernPartialErrorSyncFailureIsUncertain(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     t.TempDir(),
				Profile: profile,
				DefaultCollectionOptions: collections.CollectionOptions{
					DocumentFormat: collections.DocumentFormatBSON,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = standalone.Close() }()
			seedWriteConcernDuplicateEmails(t, standalone.Server, false)
			standalone.Server.standaloneWriteConcernSync = func() (bool, error) {
				return true, errors.New("injected post-sync publication failure")
			}
			response := serveCommand(t, standalone.Server, 2, writeConcernPartialUpdateCommand())
			if ok, okOK := bson.Raw(response).Lookup("ok").DoubleOK(); !okOK || ok != 1 {
				t.Fatalf("partial response=%s", response)
			}
			wcError, ok := bson.Raw(response).Lookup("writeConcernError").DocumentOK()
			if !ok || wcError.Lookup("codeName").StringValue() != "WriteConcernFailed" {
				t.Fatalf("partial-error response missing writeConcernError: %s", response)
			}
			stats := standalone.Server.StandaloneWriteConcernStats()
			if stats.SyncFailures != 1 || stats.UncertainOutcomes != 1 || stats.PhysicalSyncBoundaries != 1 || stats.JournalAcknowledgements != 0 {
				t.Fatalf("partial-error uncertainty stats=%+v", stats)
			}
			if !writeConcernMarker(t, standalone.Server, "u1") {
				t.Fatal("partial-error uncertainty lost first update marker")
			}
		})
	}
}

func seedWriteConcernDuplicateEmails(t *testing.T, server *Server, journal bool) {
	t.Helper()
	command := bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "u1@example.com"}},
			bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "u1@example.com"}, {Key: "count", Value: "not-a-number"}},
		}},
	}
	if journal {
		command = append(command, bson.E{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}})
	}
	command = append(command, bson.E{Key: "$db", Value: "app"})
	if response := serveCommand(t, server, 1, command); bson.Raw(response).Lookup("ok").Double() != 1 {
		t.Fatalf("seed insert=%s", response)
	}
}

func writeConcernPartialCreateIndexesCommand() bson.D {
	return bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
		}},
		{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	}
}

func writeConcernPartialUpdateCommand() bson.D {
	return bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "marker", Value: true}}}}}},
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "u2"}}}, {Key: "u", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int32(1)}}}}}},
		}},
		{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	}
}

func writeConcernFindDocumentCount(t *testing.T, server *Server, id string) int {
	t.Helper()
	response := serveCommand(t, server, 100, bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "$db", Value: "app"},
	})
	return len(cursorFirstBatch(t, response))
}

func writeConcernMarker(t *testing.T, server *Server, id string) bool {
	t.Helper()
	response := serveCommand(t, server, 100, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "app"}})
	docs := cursorFirstBatch(t, response)
	if len(docs) != 1 {
		return false
	}
	marker, ok := docs[0].Lookup("marker").BooleanOK()
	return ok && marker
}

func prepareWriteConcernDocument(t *testing.T, server *Server) {
	t.Helper()
	assertOK(t, serveCommand(t, server, 1, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "age", Value: int64(42)}}}},
		{Key: "$db", Value: "app"},
	}))
}

func prepareWriteConcernIndex(t *testing.T, server *Server) {
	t.Helper()
	prepareWriteConcernDocument(t, server)
	assertOK(t, serveCommand(t, server, 2, bson.D{
		{Key: "createIndexes", Value: "users"},
		{Key: "indexes", Value: bson.A{bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int64(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"}}}},
		{Key: "$db", Value: "app"},
	}))
}

func TestStandaloneWriteConcernRejectsBeforeCollectionCreation(t *testing.T) {
	tests := []struct {
		name     string
		concern  any
		codeName string
	}{
		{name: "not document", concern: "majority", codeName: "FailedToParse"},
		{name: "empty array", concern: bson.A{}, codeName: "FailedToParse"},
		{name: "nonempty array", concern: bson.A{bson.D{{Key: "w", Value: int32(1)}}}, codeName: "FailedToParse"},
		{name: "w zero", concern: bson.D{{Key: "w", Value: int32(0)}}, codeName: "WriteConcernFailed"},
		{name: "majority", concern: bson.D{{Key: "w", Value: "majority"}}, codeName: "WriteConcernFailed"},
		{name: "numeric replica count", concern: bson.D{{Key: "w", Value: int32(2)}}, codeName: "WriteConcernFailed"},
		{name: "tag", concern: bson.D{{Key: "w", Value: "rack-a"}}, codeName: "WriteConcernFailed"},
		{name: "wrong w type", concern: bson.D{{Key: "w", Value: true}}, codeName: "FailedToParse"},
		{name: "wrong j type", concern: bson.D{{Key: "j", Value: int32(1)}}, codeName: "FailedToParse"},
		{name: "positive timeout", concern: bson.D{{Key: "wtimeout", Value: int32(1)}}, codeName: "WriteConcernFailed"},
		{name: "deprecated timeout alias", concern: bson.D{{Key: "wtimeoutMS", Value: int32(0)}}, codeName: "WriteConcernFailed"},
		{name: "unknown", concern: bson.D{{Key: "fsync", Value: true}}, codeName: "WriteConcernFailed"},
		{name: "duplicate w", concern: bson.D{{Key: "w", Value: int32(1)}, {Key: "w", Value: int32(1)}}, codeName: "FailedToParse"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newWriteConcernTestServer(t)
			response := serveCommand(t, server, 1, bson.D{
				{Key: "insert", Value: "not-created"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
				{Key: "writeConcern", Value: tc.concern},
				{Key: "$db", Value: "app"},
			})
			assertCommandError(t, response, tc.codeName)
			if _, err := server.Collections.OpenCollection("app.not-created"); !errors.Is(err, collections.ErrCollectionNotFound) {
				t.Fatalf("rejected concern collection lookup err=%v, want not found", err)
			}
			stats := server.StandaloneWriteConcernStats()
			if stats.PreMutationRejections != 1 {
				t.Fatalf("stats=%+v want one pre-mutation rejection", stats)
			}
			if tc.name == "positive timeout" && (stats.TimeoutRejections != 1 || stats.UnsupportedRejections != 0) {
				t.Fatalf("timeout stats=%+v want exclusive timeout classification", stats)
			}
		})
	}
}

func TestStandaloneWriteConcernRejectsDuplicateCommandFieldBeforeMutation(t *testing.T) {
	server, _ := newWriteConcernTestServer(t)
	response := serveCommand(t, server, 1, bson.D{
		{Key: "insert", Value: "not-created"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}}},
		{Key: "$db", Value: "app"},
	})
	assertCommandError(t, response, "FailedToParse")
	if _, err := server.Collections.OpenCollection("app.not-created"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("duplicate writeConcern collection lookup err=%v, want not found", err)
	}
}

func TestStandaloneWriteConcernSyncFailureReportsUncertainVisibleMutation(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = standalone.Close() }()
			server := standalone.Server
			server.standaloneWriteConcernSync = func() (bool, error) { return true, errors.New("injected post-sync publication failure") }
			response := serveCommand(t, server, 1, bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
				{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, response)
			wcError, ok := bson.Raw(response).Lookup("writeConcernError").DocumentOK()
			if !ok {
				t.Fatalf("response missing writeConcernError: %s", response)
			}
			if code, ok := wcError.Lookup("code").Int32OK(); !ok || code != commandCodeWriteConcernFailed {
				t.Fatalf("writeConcernError code=%d ok=%v", code, ok)
			}
			info, ok := wcError.Lookup("errInfo").DocumentOK()
			if !ok || !info.Lookup("treedbMutationMayHaveOccurred").Boolean() || info.Lookup("treedbFailureReason").StringValue() != "sync_boundary_failed" {
				t.Fatalf("writeConcernError errInfo=%s ok=%v", info, ok)
			}
			find := serveCommand(t, server, 2, bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: "u1"}}}, {Key: "$db", Value: "app"}})
			if got := cursorFirstBatch(t, find); len(got) != 1 {
				t.Fatalf("visible documents=%d want 1", len(got))
			}
			stats := server.StandaloneWriteConcernStats()
			if stats.SyncFailures != 1 || stats.SyncBoundaryFailures != 1 || stats.UncertainOutcomes != 1 || stats.PhysicalSyncBoundaries != 1 || stats.JournalAcknowledgements != 0 {
				t.Fatalf("stats=%+v", stats)
			}
		})
	}
}

func TestStandaloneWriteConcernDurabilityUnavailableReportsStableReason(t *testing.T) {
	server, _ := newWriteConcernTestServer(t)
	server.standaloneWriteConcernSync = func() (bool, error) { return false, backenddb.ErrClosed }
	response := serveCommand(t, server, 1, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, response)
	wcError, ok := bson.Raw(response).Lookup("writeConcernError").DocumentOK()
	if !ok {
		t.Fatalf("response missing writeConcernError: %s", response)
	}
	info, ok := wcError.Lookup("errInfo").DocumentOK()
	if !ok || info.Lookup("treedbFailureReason").StringValue() != "durability_unavailable" {
		t.Fatalf("writeConcernError errInfo=%s ok=%v", info, ok)
	}
	stats := server.StandaloneWriteConcernStats()
	if stats.SyncFailures != 1 || stats.DurabilityUnavailableFailures != 1 || stats.SyncBoundaryFailures != 0 || stats.UncertainOutcomes != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestStandaloneWriteConcernRejectsMoreToComeWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		concern any
	}{
		{name: "absent"},
		{name: "w one", concern: bson.D{{Key: "w", Value: int32(1)}}},
		{name: "w zero", concern: bson.D{{Key: "w", Value: int32(0)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newWriteConcernTestServer(t)
			commandDocument := bson.D{
				{Key: "insert", Value: "users"},
				{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
			}
			if tc.concern != nil {
				commandDocument = append(commandDocument, bson.E{Key: "writeConcern", Value: tc.concern})
			}
			commandDocument = append(commandDocument, bson.E{Key: "$db", Value: "app"})
			command := mustDocument(t, commandDocument)
			request, err := wire.AppendMsgMessage(nil, 41, 0, wire.MsgFlagMoreToCome, command)
			if err != nil {
				t.Fatal(err)
			}
			ping := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}})
			request, err = wire.AppendMsgMessage(request, 42, 0, 0, ping)
			if err != nil {
				t.Fatal(err)
			}
			rw := &readWriter{r: strings.NewReader(string(request))}
			if err := server.ServeOne(rw); err != nil {
				t.Fatalf("ServeOne moreToCome: %v", err)
			}
			if rw.w.Len() != 0 {
				t.Fatalf("moreToCome response bytes=%d want 0", rw.w.Len())
			}
			if _, err := server.Collections.OpenCollection("app.users"); !errors.Is(err, collections.ErrCollectionNotFound) {
				t.Fatalf("moreToCome write collection err=%v, want not found", err)
			}
			stats := server.StandaloneWriteConcernStats()
			if stats.Requests != 1 || stats.PreMutationRejections != 1 || stats.UnsupportedRejections != 1 || stats.LogicalWrites != 0 {
				t.Fatalf("moreToCome rejection stats=%+v", stats)
			}
			if err := server.ServeOne(rw); err != nil {
				t.Fatalf("ServeOne ping after rejection: %v", err)
			}
			if rw.w.Len() == 0 {
				t.Fatal("connection did not return ping response after moreToCome rejection")
			}
		})
	}
}

func TestStandaloneJournalWriteConcernRepeatedAndConcurrentCommandWAL(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = standalone.Close() }()
			server := standalone.Server
			for i := 0; i < 5; i++ {
				assertOK(t, serveCommand(t, server, int32(i+1), bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: fmt.Sprintf("serial-%d", i)}}}},
					{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
					{Key: "$db", Value: "app"},
				}))
			}

			const workers = 8
			start := make(chan struct{})
			errs := make(chan error, workers)
			commands := make([]wire.Document, workers)
			for i := range commands {
				commands[i] = mustDocument(t, bson.D{
					{Key: "insert", Value: "users"},
					{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: fmt.Sprintf("concurrent-%d", i)}}}},
					{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
					{Key: "$db", Value: "app"},
				})
			}
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					response, err := server.commandResponse(context.Background(), "insert", commands[i], nil, 0)
					if err != nil {
						errs <- err
						return
					}
					if !mongoCommandResponseOK(response) {
						errs <- fmt.Errorf("response: %s", response)
					}
				}(i)
			}
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			stats := server.StandaloneWriteConcernStats()
			if stats.JournalAcknowledgements != 5+workers || stats.SyncFailures != 0 {
				t.Fatalf("repeated/concurrent journal stats=%+v", stats)
			}
		})
	}
}

func TestStandaloneJournalWriteConcernForcedPointerCrashReopen(t *testing.T) {
	if os.Getenv("TREEDB_MONGO_WC_CRASH_HELPER") == "1" {
		dir := os.Getenv("TREEDB_MONGO_WC_DIR")
		profile := treedb.Profile(os.Getenv("TREEDB_MONGO_WC_PROFILE"))
		opts := treedb.OptionsFor(profile, dir)
		opts.ValueLog.PointerThreshold = 1
		// This test forces document values through the persistent value log. Keep
		// outer index leaves in the index so command-WAL recovery does not also
		// depend on the separate cached leaf-page-log initialization contract.
		if profile != treedb.ProfileNoWALFast {
			opts.IndexOuterLeavesInValueLog = false
		}
		backend, _, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			t.Fatal(err)
		}
		server := NewServer()
		server.Collections = collections.NewCollectionManager(backend)
		server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
		response := serveCommand(t, server, 1, bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}, {Key: "payload", Value: strings.Repeat("p", 4096)}}}},
			{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
			{Key: "$db", Value: "app"},
		})
		if !mongoCommandResponseOK(response) {
			t.Fatalf("journal response: %s", response)
		}
		assertOK(t, response)
		os.Exit(0)
	}

	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestStandaloneJournalWriteConcernForcedPointerCrashReopen$")
			cmd.Env = append(os.Environ(), "TREEDB_MONGO_WC_CRASH_HELPER=1", "TREEDB_MONGO_WC_DIR="+dir, "TREEDB_MONGO_WC_PROFILE="+string(profile))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			opts := treedb.OptionsFor(profile, dir)
			opts.ValueLog.PointerThreshold = 1
			if profile != treedb.ProfileNoWALFast {
				opts.IndexOuterLeavesInValueLog = false
			}
			backend, closeBackend, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = closeBackend() }()
			collection, err := collections.NewCollectionManager(backend).OpenCollection("app.users")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), collections.DocumentFormatBSON)
			if err != nil {
				t.Fatal(err)
			}
			document, err := collection.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			if payload, ok := bson.Raw(document).Lookup("payload").StringValueOK(); !ok || len(payload) != 4096 {
				t.Fatalf("reopened pointer payload len=%d ok=%v", len(payload), ok)
			}
		})
	}
}

func newWriteConcernTestServer(tb testing.TB) (*Server, *backenddb.DB) {
	tb.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	server := NewServer()
	server.Collections = collections.NewCollectionManager(db)
	server.DefaultCollectionOptions = collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}
	return server, db
}
