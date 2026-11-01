package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneNativeJournalReceiptCoalescerMembership(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		for _, split := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/split=%t", profile, split), func(t *testing.T) {
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
				batch := make([]mongoInsertCoalescerRequest, 3)
				for i := range batch {
					stored, err := bson.Marshal(standaloneNativeJournalDocument(fmt.Sprint(i), i+1))
					if err != nil {
						t.Fatal(err)
					}
					batch[i] = mongoInsertCoalescerRequest{col: col, id: []byte(fmt.Sprint(i)), stored: stored, done: make(chan mongoInsertCoalescerResult, 1)}
					if i != 1 {
						batch[i].journal = &standaloneInsertJournalOutcome{}
					}
				}
				if split {
					s.Server.MaxMessageLength = int32(len(batch[0].stored) + len(batch[0].id) + 8)
				}
				before := s.Stats()
				worker := mongoInsertCoalescer{server: s.Server}
				worker.runBatch(batch)
				var lsns [3]uint64
				for i, req := range batch {
					result := <-req.done
					if result.err != nil {
						t.Fatalf("member%d err=%v", i, result.err)
					}
					if req.journal != nil {
						completion := req.journal.completion
						if !completion.Selected() || !completion.Completed() || !completion.Attempted() || req.journal.err != nil {
							t.Fatalf("member%d receipt=%+v err=%v", i, completion, req.journal.err)
						}
						lsns[i] = completion.AssignedLSN()
					}
					if got, err := col.Get(req.id); err != nil || !bytes.Equal(got, req.stored) {
						t.Fatalf("member%d exact BSON=%x/%v", i, got, err)
					}
				}
				if (!split && lsns[0] != lsns[2]) || (split && lsns[0] >= lsns[2]) {
					t.Fatalf("wrong per-sub-batch publication membership: split=%t lsns=%v", split, lsns)
				}
				if got := writeConcernBenchmarkStat(t, s.Stats(), "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total"); got != 0 {
					t.Fatalf("operation receipts issued force=%d", got)
				}
				// Duplicate fallback reuses only the independently successful member's
				// own publication; the rejected member never inherits that receipt.
				duplicate := batch[0]
				duplicate.done, duplicate.journal = make(chan mongoInsertCoalescerResult, 1), &standaloneInsertJournalOutcome{}
				fresh := batch[2]
				fresh.id, fresh.done, fresh.journal = []byte("new"), make(chan mongoInsertCoalescerResult, 1), &standaloneInsertJournalOutcome{}
				worker.runInsertBatch([]mongoInsertCoalescerRequest{duplicate, fresh})
				if r := <-duplicate.done; r.err == nil || duplicate.journal.completion.Selected() {
					t.Fatalf("duplicate acquired receipt: %+v/%v", duplicate.journal.completion, r.err)
				}
				if r := <-fresh.done; r.err != nil || !fresh.journal.completion.Completed() {
					t.Fatalf("fresh sequential member lost receipt: %+v/%v", fresh.journal.completion, r.err)
				}
			})
		}
	}
}

func TestStandaloneNativeJournalReceiptFallbackAndFailure(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("original insert receipt real physical sync failed")
			t.Cleanup(func() {
				if err := s.Close(); err != nil && !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Error(err)
				}
			})
			before := s.Stats()
			// Catalog auto-create and multi-document commands remain uncovered.
			for i, docs := range []bson.A{{standaloneNativeJournalDocument("seed", 1)}, {standaloneNativeJournalDocument("multi1", 2), standaloneNativeJournalDocument("multi2", 3)}} {
				command := bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: docs}, {Key: "writeConcern", Value: bson.D{{Key: "w", Value: 1}, {Key: "j", Value: true}}}, {Key: "$db", Value: "app"}}
				assertOK(t, serveCommand(t, s.Server, int32(i+1), command))
			}
			if got := writeConcernBenchmarkStat(t, s.Stats(), "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total"); got != 2 {
				t.Fatalf("uncovered DDL/multi command force=%d want2", got)
			}
			before, beforeACK := s.Stats(), s.Server.StandaloneWriteConcernStats()
			var calls atomic.Int32
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
					calls.Add(1)
					return failure
				}
				return nil
			})
			// Register exact once restoration before entering actual mutation.
			var once sync.Once
			restore := func() { once.Do(undo) }
			t.Cleanup(restore)
			command := mustDocument(t, bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: bson.A{standaloneNativeJournalDocument("failed", 4)}}, {Key: "writeConcern", Value: bson.D{{Key: "w", Value: 1}, {Key: "j", Value: true}}}, {Key: "$db", Value: "app"}})
			response, dispatchErr := s.Server.commandResponse(context.Background(), "insert", command, nil, 0)
			restore()
			raw := bson.Raw(response)
			if dispatchErr != nil || raw.Lookup("writeConcernError").Type != bson.TypeEmbeddedDocument || raw.Lookup("errorLabels").Type != bson.TypeArray {
				t.Fatalf("selected physical failure lost public uncertainty: %s/%v", response, dispatchErr)
			}
			afterACK := s.Server.StandaloneWriteConcernStats()
			if calls.Load() != 1 || afterACK.SyncAttempts-beforeACK.SyncAttempts != 1 || afterACK.SyncFailures-beforeACK.SyncFailures != 1 || afterACK.UncertainOutcomes-beforeACK.UncertainOutcomes != 1 || afterACK.JournalAcknowledgements != beforeACK.JournalAcknowledgements || afterACK.PhysicalSyncBoundaries != beforeACK.PhysicalSyncBoundaries {
				t.Fatalf("failure counted/retried/masked: calls=%d before=%+v after=%+v", calls.Load(), beforeACK, afterACK)
			}
			if writeConcernBenchmarkStat(t, s.Stats(), "treedb.command_wal.group_commit.forced_total") != writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total") {
				t.Fatal("selected failure issued a masking second force")
			}
		})
	}
}

// The default same-collection worker owns the native wait. Its successor queues
// until release; independent-native-entry tests deliberately use another fixture.
func TestStandaloneNativeJournalDefaultHeldReceiptQueuesSuccessor(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			f := newNativeSharedSafetyFixture(t, dir, profile, true)
			if err := f.clients[0].Database("app").CreateCollection(f.ctx, "shared_journal"); err != nil {
				t.Fatal(err)
			}
			if f.s.Server.InsertCoalescingMaxBatch < 2 {
				t.Fatal("default coalescer inactive")
			}
			before, beforeACK := f.s.Stats(), f.s.Server.StandaloneWriteConcernStats()
			entered, release := make(chan struct{}, 1), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			results := [2]chan error{make(chan error, 1), make(chan error, 1)}
			var started, joined [2]bool
			restore := func() {}
			t.Cleanup(func() {
				unblock()
				for i := range results {
					if started[i] && !joined[i] {
						select {
						case err := <-results[i]:
							joined[i] = true
							t.Logf("default held cleanup member%d: %v", i, err)
						case <-time.After(5 * time.Second):
							t.Errorf("default held member%d has no terminal", i)
						}
					}
				}
				restore()
				f.finish()
				for i := range results {
					if started[i] && !joined[i] {
						select {
						case err := <-results[i]:
							joined[i] = true
							t.Logf("default held member%d terminal after transport Close: %v", i, err)
						case <-time.After(5 * time.Second):
							t.Errorf("default held member%d still lacks terminal", i)
						}
					}
				}
			})
			var held, after atomic.Bool
			type syncIdentity struct {
				lsn        uint64
				root, path string
			}
			var heldIdentity atomic.Pointer[syncIdentity]
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceCommandWAL || e.LSN == 0 {
					return nil
				}
				if target := heldIdentity.Load(); target != nil && e.Point == durabilitycut.AfterDependencyFileSync && e.LSN == target.lsn && e.Root == target.root && e.Path == target.path {
					if e.StablePath == nil {
						return errors.New("default successful receipt lost original file")
					}
					after.Store(true)
				}
				if e.Point == durabilitycut.BeforeDependencyFileSync && held.CompareAndSwap(false, true) {
					if e.StablePath == nil {
						return errors.New("default held receipt lost original file")
					}
					heldIdentity.Store(&syncIdentity{lsn: e.LSN, root: e.Root, path: e.Path})
					entered <- struct{}{}
					<-release
				}
				return nil
			})
			var restoreOnce sync.Once
			restore = func() { restoreOnce.Do(undo) }
			ids := [2]string{"predecessor", "successor"}
			start := func(i int) {
				started[i] = true
				go func() {
					out, err := nativeSharedSafetyWriter(f, i).InsertOne(f.ctx, standaloneNativeJournalDocument(ids[i], i+1))
					if err == nil && (out == nil || out.InsertedID != ids[i] || (i == 0 && !after.Load())) {
						err = fmt.Errorf("member%d ACK before successful own physical After or wrong ID: %v", i, out)
					}
					results[i] <- err
				}()
			}
			start(0)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("default strict receipt did not enter real ordinary sync")
			}
			start(1)
			tick, deadline := time.NewTicker(10*time.Millisecond), time.NewTimer(5*time.Second)
			defer tick.Stop()
			defer deadline.Stop()
			for {
				f.s.Server.insertMu.Lock()
				worker := f.s.Server.insertCoalescers["app.shared_journal"]
				queued := worker != nil && len(worker.requests) == 1
				f.s.Server.insertMu.Unlock()
				if queued && f.s.Server.writeConcernStats.journalRequests.Load() == beforeACK.JournalRequests+2 {
					break
				}
				select {
				case <-tick.C:
				case <-deadline.C:
					t.Fatal("actual second strict RPC did not queue behind held default worker")
				}
			}
			for i := range results {
				select {
				case err := <-results[i]:
					joined[i] = true
					t.Fatalf("member%d ACK before held physical release: %v", i, err)
				default:
				}
			}
			unblock()
			for i := range results {
				select {
				case err := <-results[i]:
					joined[i] = true
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("queued member%d did not terminate after release", i)
				}
			}
			restore()
			afterACK := f.s.Server.StandaloneWriteConcernStats()
			if afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 || afterACK.SyncAttempts-beforeACK.SyncAttempts != 2 || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != 2 || writeConcernBenchmarkStat(t, f.s.Stats(), "treedb.command_wal.group_commit.forced_total") != writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total") {
				t.Fatalf("default receipt ACK/physical/force accounting before=%+v after=%+v", beforeACK, afterACK)
			}
			requireNativeSharedSafetyCensus(t, f)
			f.finish()
			fresh := newNativeSharedSafetyFixture(t, dir, profile, true)
			requireNativeSharedSafetyCensus(t, fresh)
			fresh.finish()
		})
	}
}
