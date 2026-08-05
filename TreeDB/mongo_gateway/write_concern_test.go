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
				{Key: "$db", Value: "app"},
			}
			if tc.concern != nil {
				command = append(command[:2], append(bson.D{{Key: "writeConcern", Value: tc.concern}}, command[2:]...)...)
			}
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

func TestStandaloneWriteConcernRejectsUnacknowledgedMoreToComeWithoutMutation(t *testing.T) {
	server, _ := newWriteConcernTestServer(t)
	command := mustDocument(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "u1"}}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(0)}}},
		{Key: "$db", Value: "app"},
	})
	request, err := wire.AppendMsgMessage(nil, 41, 0, wire.MsgFlagMoreToCome, command)
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: strings.NewReader(string(request))}
	if err := server.ServeOne(rw); err != nil {
		t.Fatalf("ServeOne: %v", err)
	}
	if rw.w.Len() != 0 {
		t.Fatalf("moreToCome response bytes=%d want 0", rw.w.Len())
	}
	if _, err := server.Collections.OpenCollection("app.users"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("unacknowledged write collection err=%v, want not found", err)
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
