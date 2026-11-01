package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Proposed operation-scoped receipt contract, not a regression in today's API.
// Two queued strict requests must share their own physically completed native
// publication; unrelated later prefixes and direct fresh Sync remain separate.
func TestStandaloneNativeJournalCoalescedInsertPhysicalReceiptNoRefence(t *testing.T) {
	const wait = 5 * time.Second
	opts := StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	original := s
	t.Cleanup(func() {
		if err := original.Close(); err != nil {
			t.Errorf("checked original fallback Close: %v", err)
		}
	})
	requireStandaloneNativeJournalOwner(t, s)
	if s.Server.Collections != s.Collections || s.Server.standaloneWriteConcernSync != nil {
		t.Fatal("requires original native owner, general shared admission and real physical sync")
	}
	client, cancel, listener, served := startStandaloneMongoClientForTest(t, s)
	active := true
	ctx, cancelContext := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelContext()
	ids := [3]string{"occupy-worker", "batch-one", "batch-two"}
	type result struct {
		id              any
		err             error
		after, returned uint64
	}
	results := [3]chan result{make(chan result, 1), make(chan result, 1), make(chan result, 1)}
	var started, joined [3]bool
	releases := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var releaseOnce [3]sync.Once
	release := func(i int) { releaseOnce[i].Do(func() { close(releases[i]) }) }
	restore := func() {}
	// Register before installing any blocking observer. Always release, join,
	// restore exactly once and check original transport/owner shutdown.
	defer func() {
		for i := range releases {
			release(i)
		}
		for i := range results {
			if !started[i] || joined[i] {
				continue
			}
			select {
			case got := <-results[i]:
				joined[i] = true
				if got.err != nil {
					t.Errorf("cleanup request%d: %v", i, got.err)
				}
			case <-time.After(wait):
				cancelContext()
				t.Errorf("cleanup request%d missing bounded terminal", i)
			}
		}
		restore()
		if active {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, original)
		}
		for i := range results {
			if started[i] && !joined[i] {
				select {
				case got := <-results[i]:
					t.Logf("request%d terminal after cancellation/transport Close: %v", i, got.err)
				case <-time.After(wait):
					t.Errorf("request%d still missing terminal after transport Close", i)
				}
			}
		}
	}()
	if err := client.Database("epochs").CreateCollection(ctx, "rows"); err != nil {
		t.Fatal(err)
	}
	if !s.Server.canShareStandaloneJournalBoundary("insert") {
		t.Fatal("existing collection must authenticate original shared native completion owner")
	}
	if s.Server.InsertCoalescingMaxBatch < 2 || s.Server.InsertCoalescingMaxDelay < 0 {
		t.Fatal("product default insert coalescer unavailable")
	}
	journal := true
	plain := client.Database("epochs").Collection("rows", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1}))
	strict := client.Database("epochs").Collection("rows", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
	before := s.Stats()
	beforeACK := s.Server.StandaloneWriteConcernStats()
	type sample struct {
		lsn, sequence, attempts uint64
		root, path string
	}
	type pair struct { before, after sample }
	var observationsMu sync.Mutex
	var observations [8]pair
	observed := 0
	var sequence, physicalAfter atomic.Uint64
	entered := [3]chan sample{make(chan sample, 1), make(chan sample, 1), make(chan sample, 1)}
	undo := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL || e.LSN == 0 || (e.Point != durabilitycut.BeforeDependencyFileSync && e.Point != durabilitycut.AfterDependencyFileSync) {
			return nil
		}
		if e.StablePath == nil {
			return errors.New("characterization active-file sync missing retained handle")
		}
		// No Stats, storage calls or borrowed handle retention in the observer.
		v := sample{lsn: e.LSN, root: e.Root, path: e.Path, sequence: sequence.Add(1), attempts: s.Server.writeConcernStats.syncAttempts.Load() - beforeACK.SyncAttempts}
		observationsMu.Lock()
		if e.Point == durabilitycut.BeforeDependencyFileSync {
			if observed == len(observations) {
				observationsMu.Unlock()
				return errors.New("unexpected characterization sync observation overflow")
			}
			i := observed
			observations[i].before = v
			observed++
			observationsMu.Unlock()
			if i < 2 {
				entered[i] <- v
				select {
				case <-releases[i]:
				case <-ctx.Done():
					return fmt.Errorf("bounded held physical sync%d: %w", i, ctx.Err())
				}
			}
			return nil
		}
		if observed == 0 {
			observationsMu.Unlock()
			return errors.New("physical After without observed Before")
		}
		p := &observations[observed-1]
		if p.after.sequence != 0 || p.before.lsn != v.lsn || p.before.root != v.root || p.before.path != v.path {
			observationsMu.Unlock()
			return errors.New("physical Before/After identity mismatch")
		}
		p.after = v
		physicalAfter.Store(v.sequence)
		observationsMu.Unlock()
		return nil
	})
	var restoreOnce sync.Once
	restore = func() { restoreOnce.Do(undo) }
	start := func(i int) {
		started[i] = true
		go func() {
			collection := strict
			if i == 0 {
				collection = plain
			}
			r, err := collection.InsertOne(ctx, standaloneNativeJournalDocument(ids[i], i+1))
			got := result{err: err, after: physicalAfter.Load(), returned: sequence.Add(1)}
			if r != nil {
				got.id = r.InsertedID
			}
			results[i] <- got
		}()
	}
	noACK := func(i int, boundary string) {
		t.Helper()
		select {
		case got := <-results[i]:
			joined[i] = true
			t.Fatalf("request%d returned before %s: id=%v err=%v", i, boundary, got.id, got.err)
		default:
		}
	}
	awaitSync := func(i int) sample {
		t.Helper()
		select {
		case v := <-entered[i]:
			return v
		case <-time.After(wait):
			t.Fatalf("missing real physical sync%d", i)
			return sample{}
		}
	}
	poll := func(label string, ready func() bool) {
		t.Helper()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		deadline := time.NewTimer(wait)
		defer deadline.Stop()
		for !ready() {
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatalf("bounded state observation failed: %s", label)
			}
		}
	}
	start(0)
	first := awaitSync(0)
	if first.attempts != 0 {
		t.Fatalf("predecessor ordinary sync has explicit attempts=%d", first.attempts)
	}
	start(1)
	start(2)
	poll("two accepted journal requests queued behind occupied original insert worker", func() bool {
		noACK(0, "predecessor ordinary sync")
		noACK(1, "batch ordinary sync")
		noACK(2, "batch ordinary sync")
		s.Server.insertMu.Lock()
		c := s.Server.insertCoalescers["epochs.rows"]
		queued := 0
		if c != nil {
			queued = len(c.requests)
		}
		s.Server.insertMu.Unlock()
		return queued == 2 && s.Server.writeConcernStats.journalRequests.Load()-beforeACK.JournalRequests == 2
	})
	t.Log("ACTUAL_SAME_COLLECTION_INSERT_QUEUE_PASS queued=2 accepted_journal_requests=2")
	release(0)
	batch := awaitSync(1)
	if batch.lsn <= first.lsn {
		t.Fatalf("target batch ordinary identity: first=%+v batch=%+v", first, batch)
	}
	for i := 1; i < len(ids); i++ {
		noACK(i, "target ordinary physical sync")
		readCtx, readCancel := context.WithTimeout(ctx, time.Second)
		var raw bson.Raw
		err := plain.FindOne(readCtx, bson.D{{Key: "_id", Value: ids[i]}}).Decode(&raw)
		readCancel()
		if err != nil {
			t.Fatalf("installed batch document%d: %v", i, err)
		}
		requireStandaloneNativeJournalDocument(t, raw, ids[i], i+1)
	}
	held := s.Stats()
	delta := func(stats map[string]string, key string) uint64 { return writeConcernBenchmarkStat(t, stats, key) - writeConcernBenchmarkStat(t, before, key) }
	if delta(held, "treedb.command_wal.group_commit.forced_total") != 0 {
		t.Fatalf("target forced before ordinary physical completion: forced=%d", delta(held, "treedb.command_wal.group_commit.forced_total"))
	}
	t.Logf("TARGET_DOCUMENTS_INSTALLED_ORDINARY_HELD_PASS first_lsn=%d batch_lsn=%d target_documents=2 actual_attempts=%d", first.lsn, batch.lsn, batch.attempts)
	release(1)
	// Do not require or block any later explicit epoch. Both old and proposed
	// behavior must finish actual ACKs and checked lifecycles before the receipt
	// contract assertion. No observer/queue lock spans accounting or RPC joins.
	poll("completed accounting proves predecessor plus ONE target native batch", func() bool {
		completed := delta(s.Stats(), "treedb.command_wal.group_commit.commits_total")
		if completed > 2 {
			t.Fatalf("target inserts were not one native batch: ordinary commits=%d want2", completed)
		}
		return completed == 2
	})
	var acknowledgements [3]result
	for i := range results {
		select {
		case got := <-results[i]:
			joined[i] = true
			acknowledgements[i] = got
			if got.err != nil || got.id != ids[i] || (i > 0 && (got.after <= batch.sequence || got.returned <= got.after)) {
				t.Fatalf("request%d actual ACK id=%v err=%v after=%d returned=%d batch_before=%d", i, got.id, got.err, got.after, got.returned, batch.sequence)
			}
			t.Logf("ACTUAL_DRIVER_ACK_PASS request=%d physical_after_sequence=%d driver_return_sequence=%d", i, got.after, got.returned)
		case <-time.After(wait):
			t.Fatalf("request%d missing bounded terminal", i)
		}
	}
	restore()
	observationsMu.Lock()
	pairs := append([]pair(nil), observations[:observed]...)
	observationsMu.Unlock()
	if len(pairs) < 2 {
		t.Fatalf("missing predecessor or coalesced target physical epoch: actual=%d", len(pairs))
	}
	ordinaryDone := pairs[1].after
	if ordinaryDone.lsn != batch.lsn || ordinaryDone.sequence <= batch.sequence {
		t.Fatalf("target ordinary successful After identity: %+v", ordinaryDone)
	}
	for i := 1; i < len(acknowledgements); i++ {
		got := acknowledgements[i]
		if got.after < ordinaryDone.sequence || got.returned <= ordinaryDone.sequence {
			t.Fatalf("request%d ACK preceded its actual physical publication: after=%d returned=%d target_after=%d", i, got.after, got.returned, ordinaryDone.sequence)
		}
	}
	for i, p := range pairs {
		if p.after.sequence <= p.before.sequence || (i >= 2 && (p.before.lsn != batch.lsn || p.before.sequence <= ordinaryDone.sequence)) {
			t.Fatalf("physical epoch%d chronology=%+v", i, p)
		}
		t.Logf("PHYSICAL_EPOCH_PASS epoch=%d lsn=%d attempts=%d before=%d successful_after=%d root=%s path=%s", i, p.before.lsn, p.before.attempts, p.before.sequence, p.after.sequence, p.before.root, p.before.path)
	}
	t.Logf("COALESCED_BATCH_COMPLETED_ACCOUNTING_PASS batch_lsn=%d completed_ordinary_commits=2 target_documents=2 ordinary_after=%d", batch.lsn, ordinaryDone.sequence)
	after := s.Stats()
	afterACK := s.Server.StandaloneWriteConcernStats()
	if delta(after, "treedb.cache.checkpoint.runs") != 0 || delta(after, "treedb.command_wal.group_commit.commits_total") != 2 || delta(after, "treedb.command_wal.group_commit.syncs_total") != uint64(len(pairs)) || afterACK.SyncAttempts-beforeACK.SyncAttempts != 2 || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != 2 || afterACK.SyncFailures != beforeACK.SyncFailures || afterACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
		t.Fatalf("unexpected owner counters: commits=%d forced=%d groups=%d checkpoints=%d ACKs=%+v->%+v", delta(after, "treedb.command_wal.group_commit.commits_total"), delta(after, "treedb.command_wal.group_commit.forced_total"), delta(after, "treedb.command_wal.group_commit.syncs_total"), delta(after, "treedb.cache.checkpoint.runs"), beforeACK, afterACK)
	}
	check := func() {
		t.Helper()
		cursor, err := client.Database("epochs").Collection("rows").Find(ctx, bson.D{})
		if err != nil {
			t.Fatal(err)
		}
		var docs []bson.Raw
		if err := cursor.All(ctx, &docs); err != nil || len(docs) != len(ids) {
			t.Fatalf("exact BSON census count=%d err=%v", len(docs), err)
		}
		seen := make(map[string]bool, len(ids))
		for _, raw := range docs {
			id, ok := raw.Lookup("_id").StringValueOK()
			matched := false
			for i, want := range ids {
				if id == want && ok && !seen[id] {
					requireStandaloneNativeJournalDocument(t, raw, id, i+1)
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("unexpected/duplicate exact BSON census id=%q", id)
			}
			seen[id] = true
		}
	}
	check()
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, original)
	t.Log("ORIGINAL_EXACT_BSON_CHECKED_CLOSE_PASS")
	s, err = OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	fresh := s
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Errorf("checked fresh fallback Close: %v", err)
		}
	})
	requireStandaloneNativeJournalOwner(t, fresh)
	client, cancel, listener, served = startStandaloneMongoClientForTest(t, fresh)
	freshActive := true
	defer func() {
		if freshActive {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, fresh)
		}
	}()
	check()
	freshActive = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, fresh)
	t.Log("FRESH_EXACT_BSON_READBACK_AND_CHECKED_CLOSE_PASS")
	// First intended behavioral failure; setup, chronology, actual ACKs and both
	// checked owner lifecycles above must already succeed. Baseline force I/O
	// remains real and unblocked. This proposes a NEW opt-in contract.
	forced := delta(after, "treedb.command_wal.group_commit.forced_total")
	if len(pairs) != 2 || forced != 0 {
		t.Errorf("NATIVE_OPERATION_RECEIPT_REDUNDANT_POSTDISPATCH_FORCE: actual_epochs=%d want2 original_force_registrations=%d want0 batch_lsn=%d", len(pairs), forced, batch.lsn)
	}
}
