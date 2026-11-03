package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func nativeUpdateReceiptRequest(t *testing.T, col *collections.Collection, id string, n int32, strict bool) mongoUpdateCoalescerRequest {
	t.Helper()
	item, err := parseMongoUpdateItem(0, mustDocument(t, bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: n}}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	item.budget = newMongoWriteBudget(100)
	if strict {
		item.budget.journal = &standaloneOperationJournalOutcome{}
	}
	return mongoUpdateCoalescerRequest{col: col, item: item, done: make(chan mongoUpdateCoalescerResult, 1)}
}

func TestStandaloneNativeUpdateJournalReceiptMembership(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := s.Collections.CreateCollection(&collections.CollectionMeta{Name: "app.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			col, err := s.Collections.OpenCollection("app.rows")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				doc := mustDocument(t, bson.D{{Key: "_id", Value: fmt.Sprint(i)}, {Key: "n", Value: int32(1)}})
				key, stored, err := prepareInsertDocument(doc, collections.DocumentFormatBSON)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := col.InsertBatchValidatedBSON([][]byte{key}, [][]byte{stored}); err != nil {
					t.Fatal(err)
				}
			}
			batch := []mongoUpdateCoalescerRequest{
				nativeUpdateReceiptRequest(t, col, "0", 2, true),
				nativeUpdateReceiptRequest(t, col, "1", 2, false),
				nativeUpdateReceiptRequest(t, col, "2", 1, true),
				nativeUpdateReceiptRequest(t, col, "missing", 2, true),
				nativeUpdateReceiptRequest(t, col, "3", 2, true),
			}
			before := s.Stats()
			worker := mongoUpdateCoalescer{}
			worker.runBatch(batch)
			var lsn uint64
			for i, req := range batch {
				result := <-req.done
				if result.err != nil {
					t.Fatalf("member%d: %v", i, result.err)
				}
				changed := i == 0 || i == 1 || i == 4
				if result.modified != changed || result.matched != (i != 3) {
					t.Fatalf("member%d counts=%+v", i, result)
				}
				if req.item.budget.journal == nil {
					continue
				}
				completion := req.item.budget.journal.completion
				if completion.Selected() != changed || completion.Completed() != changed {
					t.Fatalf("member%d borrowed/lost receipt: %+v", i, completion)
				}
				if changed {
					if !completion.Attempted() || completion.AssignedLSN() == 0 {
						t.Fatalf("member%d no physical attempt", i)
					}
					if lsn != 0 && lsn != completion.AssignedLSN() {
						t.Fatal("changed members lost same publication")
					}
					lsn = completion.AssignedLSN()
				}
			}
			if writeConcernBenchmarkStat(t, s.Stats(), "treedb.command_wal.group_commit.forced_total") != writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total") {
				t.Fatal("receipt batch issued fresh force")
			}
			// Duplicate keys deliberately take the sequential route. The second
			// no-op cannot inherit the preceding changed member's receipt.
			duplicates := []mongoUpdateCoalescerRequest{nativeUpdateReceiptRequest(t, col, "0", 3, true), nativeUpdateReceiptRequest(t, col, "0", 3, true)}
			worker.runBatch(duplicates)
			for i, req := range duplicates {
				r := <-req.done
				if r.err != nil || r.modified != (i == 0) || req.item.budget.journal.completion.Selected() != (i == 0) {
					t.Fatalf("sequential member%d: %+v/%+v", i, r, req.item.budget.journal)
				}
			}
			want := mustDocument(t, bson.D{{Key: "_id", Value: "0"}, {Key: "n", Value: int32(3)}})
			if got, err := col.Get(batch[0].item.key); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("exact BSON=%x/%v", got, err)
			}
		})
	}
}

func TestStandaloneNativeUpdateJournalReceiptFailureNoReplay(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("selected update original physical sync failure")
			t.Cleanup(func() {
				if err := s.Close(); err != nil && !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Error(err)
				}
			})
			assertOK(t, serveCommand(t, s.Server, 1, bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "0"}, {Key: "n", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
			before, ack := s.Stats(), s.Server.StandaloneWriteConcernStats()
			var calls atomic.Int32
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
					calls.Add(1)
					return failure
				}
				return nil
			})
			var once sync.Once
			restore := func() { once.Do(undo) }
			t.Cleanup(restore)
			command := mustDocument(t, bson.D{{Key: "update", Value: "rows"}, {Key: "updates", Value: bson.A{bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "0"}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(2)}}}}}}}}, {Key: "writeConcern", Value: bson.D{{Key: "w", Value: 1}, {Key: "j", Value: true}}}, {Key: "$db", Value: "app"}})
			response, err := s.Server.commandResponse(context.Background(), "update", command, nil, 0)
			restore()
			after := s.Server.StandaloneWriteConcernStats()
			if err != nil || bson.Raw(response).Lookup("writeConcernError").Type != bson.TypeEmbeddedDocument || calls.Load() != 1 || after.SyncFailures-ack.SyncFailures != 1 || after.SyncAttempts-ack.SyncAttempts != 1 || after.UncertainOutcomes-ack.UncertainOutcomes != 1 || after.JournalAcknowledgements != ack.JournalAcknowledgements {
				t.Fatalf("failure replay/masked response=%s err=%v calls=%d before=%+v after=%+v", response, err, calls.Load(), ack, after)
			}
			if writeConcernBenchmarkStat(t, s.Stats(), "treedb.command_wal.group_commit.forced_total") != writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total") {
				t.Fatal("selected failure issued masking force")
			}
		})
	}
}
