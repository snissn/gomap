//go:build darwin || linux || freebsd || netbsd || openbsd

package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable} {
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
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			s.Server.EnableStandaloneJournalRouteStats()
			failure := errors.New("selected update original physical sync failure")
			t.Cleanup(func() {
				if err := s.Close(); err != nil && !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Error(err)
				}
			})
			assertOK(t, serveCommand(t, s.Server, 1, bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "0"}, {Key: "n", Value: int32(1)}}}}, {Key: "$db", Value: "app"}}))
			assertStandaloneJournalRouteStats(t, s.Server, nil) // Visible seed is excluded.
			for i, tc := range []struct {
				id     string
				upsert bool
				count  int32
			}{{id: "0", count: 1}, {id: "missing"}, {id: "upsert", upsert: true, count: 1}} {
				reply := serveCommand(t, s.Server, int32(i+2), bson.D{
					{Key: "update", Value: "rows"},
					{Key: "updates", Value: bson.A{bson.D{
						{Key: "q", Value: bson.D{{Key: "_id", Value: tc.id}}},
						{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int32(1)}}}}},
						{Key: "upsert", Value: tc.upsert},
					}}},
					{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
					{Key: "$db", Value: "app"},
				})
				assertOK(t, reply)
				raw := bson.Raw(reply)
				if raw.Lookup("n").Int32() != tc.count || raw.Lookup("nModified").Int32() != 0 ||
					(raw.Lookup("upserted").Type == bson.TypeArray) != tc.upsert || raw.Lookup("writeConcernError").Type != 0 {
					t.Fatalf("update control %q response: %s", tc.id, raw)
				}
			}
			assertStandaloneJournalRouteStats(t, s.Server, map[string]uint64{
				"UpdateUnmodified.SharedFreshFence": 2, "UpdateOther.SharedFreshFence": 1,
			})
			before, ack := s.Stats(), s.Server.StandaloneWriteConcernStats()
			startLSN := s.Backend.CommandWALNextLSN()
			var calls atomic.Int32
			var assigned, failedPrefix atomic.Uint64
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				switch e.Point {
				case durabilitycut.AfterDependencyAppend:
					assigned.Store(e.LSN)
				case durabilitycut.BeforeDependencyFileSync:
					calls.Add(1)
					failedPrefix.Store(e.LSN)
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
			raw := bson.Raw(response)
			message, _ := raw.Lookup("writeConcernError", "errmsg").StringValueOK()
			if err != nil || !mongoCommandResponseOK(response) || raw.Lookup("n").Int32() != 1 ||
				raw.Lookup("nModified").Int32() != 1 || raw.Lookup("writeErrors").Type != 0 ||
				!strings.Contains(message, failure.Error()) || calls.Load() != 1 || after.SyncFailures-ack.SyncFailures != 1 || after.SyncAttempts-ack.SyncAttempts != 1 || after.UncertainOutcomes-ack.UncertainOutcomes != 1 || after.JournalAcknowledgements != ack.JournalAcknowledgements {
				t.Fatalf("failure replay/masked response=%s err=%v calls=%d before=%+v after=%+v", response, err, calls.Load(), ack, after)
			}
			nextLSN := s.Backend.CommandWALNextLSN()
			if assigned.Load() != startLSN || failedPrefix.Load() != startLSN || nextLSN != startLSN+1 {
				t.Fatalf("failure did not retain original append/physical identity: next=%d..%d assigned=%d failed-prefix=%d",
					startLSN, nextLSN, assigned.Load(), failedPrefix.Load())
			}
			if s.Collections.CanShareStandaloneJournalBoundary() || s.Backend.CheckCommandWALPublishReady() == nil {
				t.Fatal("physical failure did not disable new receipt and publication")
			}
			if writeConcernBenchmarkStat(t, s.Stats(), "treedb.command_wal.group_commit.forced_total") != writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total") {
				t.Fatal("selected failure issued masking force")
			}
			assertStandaloneJournalRouteStats(t, s.Server, map[string]uint64{
				"UpdateUnmodified.SharedFreshFence": 2, "UpdateOther.SharedFreshFence": 1,
				"UpdateModified.ReceiptFailed": 1, "UpdateModified.ReceiptWaitAttempted": 1, "UpdateModified.ReceiptInstalled": 1,
			})
		})
	}
}

func TestStandaloneNativeJournalFailedBSONSetBatchRetainsInstalledMembership(t *testing.T) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("installed batch's shared physical sync failed")
	t.Cleanup(func() {
		if err := s.Close(); err != nil && !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
			t.Error(err)
		}
	})
	assertOK(t, serveCommand(t, s.Server, 1, bson.D{
		{Key: "insert", Value: "rows"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "_id", Value: "strict"}, {Key: "n", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "plain"}, {Key: "n", Value: int32(1)}},
			bson.D{{Key: "_id", Value: "unchanged"}, {Key: "n", Value: int32(1)}},
		}},
		{Key: "$db", Value: "app"},
	}))
	col, err := s.Collections.OpenCollection("app.rows")
	if err != nil {
		t.Fatal(err)
	}
	batch := []mongoUpdateCoalescerRequest{
		nativeUpdateReceiptRequest(t, col, "strict", 2, true),
		nativeUpdateReceiptRequest(t, col, "plain", 2, false),
		nativeUpdateReceiptRequest(t, col, "unchanged", 1, true),
		nativeUpdateReceiptRequest(t, col, "missing", 2, true),
	}
	before := s.Stats()
	assigned := s.Backend.CommandWALNextLSN()
	var failedPrefix atomic.Uint64
	undo := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			failedPrefix.Store(e.LSN)
			return failure
		}
		return nil
	})
	var once sync.Once
	restore := func() { once.Do(undo) }
	t.Cleanup(restore)
	worker := mongoUpdateCoalescer{}
	worker.runBatch(batch)
	restore()
	if failedPrefix.Load() != assigned || s.Backend.CommandWALNextLSN() != assigned+1 {
		t.Fatalf("expected only assigned LSN %d to enter the captured failure, got %d, next %d",
			assigned, failedPrefix.Load(), s.Backend.CommandWALNextLSN())
	}
	for i, req := range batch {
		result := <-req.done
		if result.err != nil || result.matched != (i != 3) || result.modified != (i < 2) {
			t.Fatalf("member %d logical outcome=%+v", i, result)
		}
		journal := req.item.budget.journal
		switch i {
		case 0:
			r := journal.completion
			if !r.Selected() || !r.Installed() || !r.Attempted() || r.Completed() || r.AssignedLSN() != assigned ||
				!errors.Is(journal.err, failure) {
				t.Fatalf("changed strict member receipt=%+v err=%v", r, journal.err)
			}
		case 1:
			if journal != nil {
				t.Fatal("plain member acquired a strict journal destination")
			}
		default:
			if journal.completion.Selected() || journal.err != nil {
				t.Fatalf("unchanged member %d inherited publication: %+v", i, journal)
			}
		}
	}
	const force = "treedb.command_wal.group_commit.forced_total"
	if writeConcernBenchmarkStat(t, s.Stats(), force) != writeConcernBenchmarkStat(t, before, force) {
		t.Fatal("failed operation batch issued a masking force")
	}
}

func TestStandaloneNativeJournalSingletonBSONSetRetriesCleanSameRowConflict(t *testing.T) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	requireStandaloneNativeJournalOwner(t, s)
	assertOK(t, serveCommand(t, s.Server, 1, bson.D{
		{Key: "insert", Value: "rows"},
		{Key: "documents", Value: bson.A{bson.D{
			{Key: "_id", Value: "same"}, {Key: "a", Value: int32(0)}, {Key: "b", Value: int32(0)},
		}}},
		{Key: "$db", Value: "app"},
	}))
	command := bson.D{
		{Key: "update", Value: "rows"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "same"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: int32(1)}}}}},
		}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: 1}, {Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	}
	requireChanged := func(response []byte, err error) {
		t.Helper()
		raw := bson.Raw(response)
		ok, _ := raw.Lookup("ok").DoubleOK()
		if err != nil || ok != 1 || raw.Lookup("n").Int32() != 1 ||
			raw.Lookup("nModified").Int32() != 1 || raw.Lookup("writeErrors").Type != 0 || raw.Lookup("writeConcernError").Type != 0 {
			t.Fatalf("update response=%s transport=%v", raw, err)
		}
	}
	col, err := s.Collections.OpenCollection("app.rows")
	if err != nil {
		t.Fatal(err)
	}
	advanceKey, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "same"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatal(err)
	}
	two := mustDocument(t, bson.D{{Key: "b", Value: int32(1)}})
	advanceValues := []collections.BSONSetField{{Key: "b", Value: bson.Raw(two).Lookup("b")}}
	before := s.Stats()
	start := s.Backend.CommandWALNextLSN()
	ack := s.Server.StandaloneWriteConcernStats()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var admissions, appends atomic.Int32
	var stable atomic.Uint64
	var capturedPath atomic.Pointer[string]
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission && admissions.Add(1) == 1 {
			close(entered)
			<-release
		}
		if e.Resource == durabilitycut.ResourceCommandWAL {
			switch e.Point {
			case durabilitycut.AfterDependencyAppend:
				appends.Add(1)
			case durabilitycut.AfterDependencyFileSync:
				stable.Store(e.LSN)
				path := e.Path
				capturedPath.Store(&path)
			}
		}
		return nil
	})
	defer restore()
	defer unblock()
	type result struct {
		response []byte
		err      error
	}
	singleton := make(chan result, 1)
	go func() {
		response, err := serveCommandResult(s.Server, 2, command)
		singleton <- result{response: response, err: err}
	}()
	limit, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-limit.Done():
		t.Fatal("strict singleton did not stage the original row")
	}
	// The maintained wire worker serializes same-collection queued mutations.
	// An original collection call advances the row while wire admission is held.
	advanced := make(chan error, 1)
	go func() {
		matched, modified, err := col.UpdateBSONSet(advanceKey, advanceValues)
		if err == nil && !(matched && modified) {
			err = fmt.Errorf("competitor matched=%t modified=%t", matched, modified)
		}
		advanced <- err
	}()
	select {
	case err := <-advanced:
		if err != nil {
			t.Fatal(err)
		}
	case <-limit.Done():
		t.Fatal("same-row competitor could not publish outside the held admission")
	}
	unblock()
	select {
	case got := <-singleton:
		requireChanged(got.response, got.err)
	case <-limit.Done():
		t.Fatal("singleton did not finish its bounded retry")
	}
	after := s.Server.StandaloneWriteConcernStats()
	if admissions.Load() != 3 || appends.Load() != 3 || stable.Load() != start+2 || s.Backend.CommandWALNextLSN() != start+3 ||
		after.JournalAcknowledgements-ack.JournalAcknowledgements != 1 || after.PhysicalSyncBoundaries-ack.PhysicalSyncBoundaries != 1 {
		t.Fatalf("admissions=%d appends=%d stable=%d next=%d start=%d ACK before=%+v after=%+v",
			admissions.Load(), appends.Load(), stable.Load(), s.Backend.CommandWALNextLSN(), start, ack, after)
	}
	const force = "treedb.command_wal.group_commit.forced_total"
	if writeConcernBenchmarkStat(t, s.Stats(), force) != writeConcernBenchmarkStat(t, before, force) {
		t.Fatal("retried singleton issued fresh force")
	}
	plain := mustDocument(t, bson.D{{Key: "_id", Value: "same"}, {Key: "a", Value: int32(0)}, {Key: "b", Value: int32(1)}})
	want := mustDocument(t, bson.D{{Key: "_id", Value: "same"}, {Key: "a", Value: int32(1)}, {Key: "b", Value: int32(1)}})
	if got, err := col.Get(advanceKey); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("exact retry image=%x want=%x err=%v", got, want, err)
	}
	// The ordinary competitor writes one relaxed mutation. The maintained
	// promotion marker covers it before the retried operation's durable frame.
	path := capturedPath.Load()
	if path == nil || *path == "" {
		t.Fatal("missing actual captured WAL path")
	}
	reader, err := commitlog.NewReaderWithOptions(*path, commitlog.Options{MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	observed := 0
	for scanned := 0; scanned < 8; scanned++ {
		frame, err := reader.ReadCommandFrameV2()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if frame.LSN < start {
			continue
		}
		if observed >= 3 || frame.LSN != start+uint64(observed) {
			t.Fatalf("extra or reordered WAL frame LSN=%d index=%d", frame.LSN, observed)
		}
		observed++
		if observed == 2 {
			if frame.Kind != commitlog.CommandKindDurablePrefixBarrier || frame.DurabilityClass != commitlog.CommandDurabilityDurable || len(frame.Payload) != 0 {
				t.Fatalf("expected promotion of ordinary competitor at LSN %d, got kind %d", frame.LSN, frame.Kind)
			}
			continue
		}
		image, class := want, commitlog.CommandDurabilityDurable
		if observed == 1 {
			image, class = plain, commitlog.CommandDurabilityRelaxed
		}
		documents, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
		if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID || frame.DurabilityClass != class || err != nil ||
			documents.Collection != "app.rows" || len(documents.Documents) != 1 ||
			!bytes.Equal(documents.Documents[0].ID, advanceKey) || !bytes.Equal(documents.Documents[0].Document, image) {
			t.Fatalf("update WAL action LSN=%d kind=%d class=%d payload=%+v err=%v", frame.LSN, frame.Kind, frame.DurabilityClass, documents, err)
		}
	}
	if observed != 3 {
		t.Fatalf("observed %d retained WAL frames, want two mutations and one promotion", observed)
	}
	t.Logf("same-row retry admitted three attempts and persisted two updates plus predecessor promotion; strict LSN=%d and zero fresh forces", stable.Load())
}

func TestStandaloneNativeJournalSingletonUniqueIndexChangeUsesFreshFence(t *testing.T) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	requireStandaloneNativeJournalOwner(t, s)
	s.Server.EnableStandaloneJournalRouteStats()
	assertOK(t, serveCommand(t, s.Server, 1, bson.D{
		{Key: "insert", Value: "rows"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "row"}, {Key: "unique", Value: int32(1)}}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, s.Server, 2, bson.D{
		{Key: "createIndexes", Value: "rows"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "unique", Value: 1}}}, {Key: "name", Value: "unique_1"}, {Key: "unique", Value: true},
		}}},
		{Key: "$db", Value: "app"},
	}))
	const force = "treedb.command_wal.group_commit.forced_total"
	before := writeConcernBenchmarkStat(t, s.Stats(), force)
	reply := serveCommand(t, s.Server, 3, bson.D{
		{Key: "update", Value: "rows"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "row"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "unique", Value: int32(2)}}}}},
		}}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: 1}, {Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, reply)
	raw := bson.Raw(reply)
	if raw.Lookup("n").Int32() != 1 || raw.Lookup("nModified").Int32() != 1 || raw.Lookup("writeConcernError").Type != 0 {
		t.Fatalf("indexed singleton response: %s", raw)
	}
	if after := writeConcernBenchmarkStat(t, s.Stats(), force); after != before+1 {
		t.Fatalf("unique index update forced groups=%d->%d, want one standalone fence", before, after)
	}
	assertStandaloneJournalRouteStats(t, s.Server, map[string]uint64{"UpdateModified.SharedFreshFence": 1})
}
