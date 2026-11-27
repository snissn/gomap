package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// TestStandaloneServerOfficialDriverJournalFenceWithoutCheckpoint first checks
// the acknowledged native route and complete checked lifecycle. Checkpoints are
// the selected baseline side effect: that assertion must precede candidate-only
// forced-group activation checks so a missing new route cannot obscure the RED.
func TestStandaloneServerOfficialDriverJournalFenceWithoutCheckpoint(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			result := characterizeStandaloneNativeJournalDriver(t, profile)
			if result.checkpoints != 0 {
				t.Fatalf("explicit journal ACK constructed cache-owner checkpoints: delta=%d, want 0", result.checkpoints)
			}
			if result.forcedGroups != 0 {
				t.Fatalf("covered multi-doc BSON InsertMany forced groups=%d want 0 (completed via own operation receipt)", result.forcedGroups)
			}
			if result.uncoveredForcedGroups != result.uncoveredAcknowledged {
				t.Fatalf("uncovered forced groups=%d want one per explicit journal ACK=%d", result.uncoveredForcedGroups, result.uncoveredAcknowledged)
			}
		})
	}
}

type standaloneNativeJournalCharacterization struct {
	acknowledged, checkpoints, forcedGroups      uint64
	uncoveredAcknowledged, uncoveredForcedGroups uint64
}

func requireStandaloneNativeJournalOwner(tb testing.TB, standalone *StandaloneServer) {
	tb.Helper()
	if standalone.Backend == nil || standalone.Collections == nil || standalone.ownerStats == nil || standalone.cleanup == nil ||
		!standalone.Backend.CommandWALEnabled() || !standalone.Collections.UsesNativeCommandWALApply() {
		tb.Fatal("standalone must retain its original public CommandWAL/native owner and cleanup")
	}
	stats := standalone.Stats()
	if mode := stats["treedb.cache.memtable_mode"]; mode != "cow_btree" {
		tb.Fatalf("original cache-owner mode=%q want cow_btree", mode)
	}
	// These are original public completion-group and cache-owner counters, not
	// backend aliases or the public wrapper's Checkpoint API call count.
	for _, key := range []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.group_commit.forced_total", "treedb.command_wal.sync.count_total", "treedb.command_wal.file_sync.calls_total"} {
		writeConcernBenchmarkStat(tb, stats, key)
	}
}

func standaloneNativeJournalDocument(id string, generation int) bson.D {
	doc := bson.D{{Key: "_id", Value: id}}
	for field := 0; field < 10; field++ {
		payload := make([]byte, 100)
		for i := range payload {
			payload[i] = byte(generation*31 + field*7 + i)
		}
		doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", field), Value: bson.Binary{Subtype: 0, Data: payload}})
	}
	return doc
}

func requireStandaloneNativeJournalDocument(tb testing.TB, raw bson.Raw, id string, generation int) {
	tb.Helper()
	if got, ok := raw.Lookup("_id").StringValueOK(); !ok || got != id {
		tb.Fatalf("document _id=%q string=%t want %q", got, ok, id)
	}
	fields, err := raw.Elements()
	if err != nil || len(fields) != 11 {
		tb.Fatalf("document fields=%d err=%v want _id plus ten binary fields", len(fields), err)
	}
	for _, field := range standaloneNativeJournalDocument(id, generation)[1:] {
		want := field.Value.(bson.Binary)
		subtype, payload, ok := raw.Lookup(field.Key).BinaryOK()
		if !ok || subtype != want.Subtype || !bytes.Equal(payload, want.Data) {
			tb.Fatalf("document %s field %s: binary=%t subtype=%d bytes=%v want subtype=%d bytes=%v", id, field.Key, ok, subtype, payload, want.Subtype, want.Data)
		}
	}
}

func characterizeStandaloneNativeJournalDriver(t *testing.T, profile treedb.Profile) standaloneNativeJournalCharacterization {
	t.Helper()
	const count = 8 // Fresh ~10 KiB fixture; default product maintenance stays enabled.
	dir := t.TempDir()
	requireStandaloneExt4(t, dir)
	opts := StandaloneOptions{Dir: dir, Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	open := func() *StandaloneServer {
		t.Helper()
		s, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatalf("open standalone: %v", err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("checked standalone cleanup: %v", err)
			}
		})
		requireStandaloneNativeJournalOwner(t, s)
		checkStandaloneWALStream(t, s, false)
		return s
	}
	standalone := open()
	client, cancel, listener, served := startStandaloneMongoClientForTest(t, standalone)
	active := true
	defer func() {
		if active {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
		}
	}()
	ctx, cancelContext := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelContext()
	// Precreate before the observed request interval. Multi-document BSON wire
	// batches complete through their own operation receipt; an uncovered delete
	// separately exercises the standalone prefix fence and its real driver ACK.
	if err := client.Database("app").CreateCollection(ctx, "journal_fence"); err != nil {
		t.Fatalf("precreate BSON collection: %v", err)
	}
	const uncoveredSentinelID = "journal-uncovered-delete-target"
	if _, err := client.Database("app").Collection("journal_fence").InsertOne(ctx, standaloneNativeJournalDocument(uncoveredSentinelID, 999)); err != nil {
		t.Fatalf("seed uncovered delete target: %v", err)
	}
	journal := true
	collection := client.Database("app").Collection("journal_fence", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
	before := standalone.Stats()
	beforeACK := standalone.Server.StandaloneWriteConcernStats()
	var acknowledged uint64
	for i := 0; i < count; i += 2 {
		first, second := fmt.Sprintf("journal-%02d", i), fmt.Sprintf("journal-%02d", i+1)
		result, err := collection.InsertMany(ctx, []any{standaloneNativeJournalDocument(first, i), standaloneNativeJournalDocument(second, i+1)})
		if err != nil || result == nil || !result.Acknowledged || len(result.InsertedIDs) != 2 ||
			result.InsertedIDs[0] != first || result.InsertedIDs[1] != second {
			t.Fatalf("journal insert %s/%s result=%+v err=%v", first, second, result, err)
		}
		acknowledged++
	}
	after := standalone.Stats()
	afterACK := standalone.Server.StandaloneWriteConcernStats()
	t.Logf("native original owner profile=%s mode=%s acknowledged=%d checkpoints=%s->%s forced_groups=%s->%s WAL_sync_operations=%s->%s WAL_file_sync_calls=%s->%s before_ack=%+v after_ack=%+v", profile, after["treedb.cache.memtable_mode"], acknowledged,
		before["treedb.cache.checkpoint.runs"], after["treedb.cache.checkpoint.runs"],
		before["treedb.command_wal.group_commit.forced_total"], after["treedb.command_wal.group_commit.forced_total"],
		before["treedb.command_wal.sync.count_total"], after["treedb.command_wal.sync.count_total"],
		before["treedb.command_wal.file_sync.calls_total"], after["treedb.command_wal.file_sync.calls_total"], beforeACK, afterACK)
	const commands = count / 2
	if acknowledged != commands || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != commands ||
		afterACK.SyncAttempts-beforeACK.SyncAttempts != commands || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != commands ||
		afterACK.SyncFailures != beforeACK.SyncFailures || afterACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
		t.Fatalf("journal ACK/boundary mismatch acknowledged=%d before=%+v after=%+v", acknowledged, beforeACK, afterACK)
	}
	deltaBetween := func(firstStats, lastStats map[string]string, key string) uint64 {
		t.Helper()
		first, last := writeConcernBenchmarkStat(t, firstStats, key), writeConcernBenchmarkStat(t, lastStats, key)
		if last < first {
			t.Fatalf("counter %s decreased %d -> %d", key, first, last)
		}
		return last - first
	}
	if operations, files := deltaBetween(before, after, "treedb.command_wal.sync.count_total"), deltaBetween(before, after, "treedb.command_wal.file_sync.calls_total"); operations < acknowledged || files < acknowledged {
		t.Fatalf("ACKs=%d command-WAL sync operations=%d physical file-sync calls=%d; want complete physical boundaries", acknowledged, operations, files)
	}
	delResult, err := collection.DeleteOne(ctx, bson.D{{Key: "_id", Value: uncoveredSentinelID}})
	if err != nil || delResult == nil || !delResult.Acknowledged || delResult.DeletedCount != 1 {
		t.Fatalf("uncovered journal delete result=%+v err=%v", delResult, err)
	}
	afterUncovered := standalone.Stats()
	afterUncoveredACK := standalone.Server.StandaloneWriteConcernStats()
	const uncoveredCommands = uint64(1)
	if afterUncoveredACK.JournalAcknowledgements-afterACK.JournalAcknowledgements != uncoveredCommands ||
		afterUncoveredACK.SyncAttempts-afterACK.SyncAttempts != uncoveredCommands ||
		afterUncoveredACK.PhysicalSyncBoundaries-afterACK.PhysicalSyncBoundaries != uncoveredCommands ||
		afterUncoveredACK.SyncFailures != afterACK.SyncFailures || afterUncoveredACK.UncertainOutcomes != afterACK.UncertainOutcomes {
		t.Fatalf("uncovered journal ACK/boundary mismatch before=%+v after=%+v", afterACK, afterUncoveredACK)
	}
	if operations, files := deltaBetween(after, afterUncovered, "treedb.command_wal.sync.count_total"), deltaBetween(after, afterUncovered, "treedb.command_wal.file_sync.calls_total"); operations < uncoveredCommands || files < uncoveredCommands {
		t.Fatalf("uncovered ACKs=%d command-WAL sync operations=%d physical file-sync calls=%d; want complete physical boundaries", uncoveredCommands, operations, files)
	}
	check := func(c *mongo.Client) {
		t.Helper()
		coll := c.Database("app").Collection("journal_fence")
		cursor, err := coll.Find(ctx, bson.D{})
		if err != nil {
			t.Fatalf("readback find: %v", err)
		}
		var documents []bson.Raw
		if err := cursor.All(ctx, &documents); err != nil || len(documents) != count {
			t.Fatalf("readback documents=%d want %d err=%v", len(documents), count, err)
		}
		seen := make(map[string]bool, count)
		for _, document := range documents {
			id, ok := document.Lookup("_id").StringValueOK()
			if !ok || seen[id] {
				t.Fatalf("invalid/duplicate _id=%q string=%t", id, ok)
			}
			seen[id] = true
		}
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("journal-%02d", i)
			var document bson.Raw
			if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&document); err != nil {
				t.Fatalf("readback %s: %v", id, err)
			}
			requireStandaloneNativeJournalDocument(t, document, id, i)
		}
	}
	check(client)
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
	t.Log("original driver disconnect, Serve shutdown, standalone manager/DB close: PASS")
	standalone = open()
	client, cancel, listener, served = startStandaloneMongoClientForTest(t, standalone)
	active = true
	check(client)
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
	t.Log("fresh native owner typed reopen census and checked close: PASS (graceful reopen, not crash proof)")
	return standaloneNativeJournalCharacterization{
		acknowledged:          acknowledged,
		checkpoints:           deltaBetween(before, afterUncovered, "treedb.cache.checkpoint.runs"),
		forcedGroups:          deltaBetween(before, after, "treedb.command_wal.group_commit.forced_total"),
		uncoveredAcknowledged: uncoveredCommands,
		uncoveredForcedGroups: deltaBetween(after, afterUncovered, "treedb.command_wal.group_commit.forced_total"),
	}
}
