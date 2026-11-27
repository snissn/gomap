//go:build darwin || linux || freebsd || netbsd || openbsd

package mongogateway

import (
	"context"
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

// Use the same three-row official-driver shape as the fixed durable coalescer
// control. The relaxed stream's strict requests wait for exclusive dispatch,
// and each then waits for a physical root checkpoint before acknowledging.
func TestStandaloneRelaxedJournalBSONSetExclusiveCheckpoint(t *testing.T) {
	dir := t.TempDir()
	requireStandaloneExt4(t, dir)
	opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Server.EnableStandaloneJournalRouteStats()
	checkStandaloneWALStream(t, s, true)
	requireStandaloneReceiptTestOwner(t, s)
	if s.Server.canShareStandaloneJournalBoundary("update") || s.Server.UpdateCoalescingMaxBatch < 2 {
		t.Fatal("relaxed actual owner must use exclusive journal admission with the default worker")
	}
	client, cancel, listener, served := startStandaloneMongoClientForTest(t, s)
	active := true
	defer func() {
		if active {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, s)
		}
	}()
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	db := client.Database("epochs")
	if err := db.CreateCollection(ctx, "rows"); err != nil {
		t.Fatal(err)
	}
	plain := db.Collection("rows", options.Collection().SetWriteConcern(writeconcern.W1()))
	strict := db.Collection("rows", options.Collection().SetWriteConcern(writeconcern.Journaled()))
	ids := [3]string{"occupy-worker", "batch-one", "batch-two"}
	for i, id := range ids {
		if _, err := plain.InsertOne(ctx, standaloneReceiptTestDocument(id, i+1)); err != nil {
			t.Fatal(err)
		}
	}
	if physical, err := s.Collections.SyncForStandaloneWriteConcern(); !physical || err != nil {
		t.Fatalf("seed checkpoint physical=%t err=%v", physical, err)
	}
	applied := s.Backend.State().AppliedCommandLSN
	before, beforeACK := s.Stats(), s.Server.StandaloneWriteConcernStats()
	const wait = 5 * time.Second
	enteredAppend, enteredCheckpoint := make(chan struct{}), make(chan struct{})
	releaseAppend, releaseCheckpoint := make(chan struct{}), make(chan struct{})
	var releaseFirst, releaseSecond sync.Once
	unblockAppend := func() { releaseFirst.Do(func() { close(releaseAppend) }) }
	unblockCheckpoint := func() { releaseSecond.Do(func() { close(releaseCheckpoint) }) }
	var appends, checkpointStarts, checkpointCompletions atomic.Uint64
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend:
			appends.Add(1)
		case event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.BeforeCOWCutSwap:
			if appends.Load() != 1 {
				return nil
			}
			close(enteredAppend)
			select {
			case <-releaseAppend:
			case <-ctx.Done():
				return ctx.Err()
			}
		case event.Resource == durabilitycut.ResourceIndex && event.Point == durabilitycut.BeforeIndexDataSync:
			if checkpointStarts.Add(1) != 1 {
				return nil
			}
			close(enteredCheckpoint)
			select {
			case <-releaseCheckpoint:
			case <-ctx.Done():
				return ctx.Err()
			}
		case event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.AfterAppliedLSNAdvance:
			checkpointCompletions.Add(1)
		}
		return nil
	})
	var restoreOnce sync.Once
	restoreObserver := func() { restoreOnce.Do(restore) }
	type result struct {
		err                  error
		matched, modified    int64
		checkpointsBeforeACK uint64
	}
	var done [3]chan result
	defer func() {
		unblockAppend()
		unblockCheckpoint()
		for i, pending := range done {
			if pending != nil {
				select {
				case <-pending:
				case <-time.After(wait):
					t.Errorf("update %d did not join after release", i)
				}
			}
		}
		restoreObserver()
	}()
	start := func(i int) {
		ch := make(chan result, 1)
		done[i] = ch
		collection := strict
		if i == 0 {
			collection = plain
		}
		go func() {
			update := bson.D{{Key: "$set", Value: standaloneReceiptTestDocument(ids[i], i+11)[1:]}}
			reply, err := collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: ids[i]}}, update)
			got := result{err: err, checkpointsBeforeACK: checkpointCompletions.Load()}
			if reply != nil {
				got.matched, got.modified = reply.MatchedCount, reply.ModifiedCount
			}
			ch <- got
		}()
	}
	waitFor := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(wait):
			t.Fatalf("missing %s", what)
		}
	}
	start(0)
	waitFor(enteredAppend, "held appended predecessor COW cut")
	start(1)
	start(2)
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for s.Server.writeConcernStats.journalRequests.Load()-beforeACK.JournalRequests != 2 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("two strict updates did not reach journal admission")
		}
	}
	s.Server.updateMu.Lock()
	coalescer := s.Server.updateCoalescers["epochs.rows"]
	queued := 0
	if coalescer != nil {
		queued = coalescer.queuedLen()
	}
	s.Server.updateMu.Unlock()
	if coalescer == nil || queued != 0 || appends.Load() != 1 {
		t.Fatalf("exclusive strict updates reached the occupied worker: queued=%d appends=%d", queued, appends.Load())
	}
	unblockAppend()
	waitFor(enteredCheckpoint, "held index checkpoint")
	if s.Server.standaloneWriteBoundaryMu.TryRLock() {
		s.Server.standaloneWriteBoundaryMu.RUnlock()
		t.Fatal("relaxed physical checkpoint allowed shared admission")
	}
	for i := 1; i < len(ids); i++ {
		select {
		case got := <-done[i]:
			done[i] = nil
			t.Fatalf("strict update %d returned before physical checkpoint: %+v", i, got)
		default:
		}
	}
	assertStandaloneJournalRouteStats(t, s.Server, nil)
	unblockCheckpoint()
	var early, late uint64
	for i := range ids {
		select {
		case got := <-done[i]:
			done[i] = nil
			if got.err != nil || got.matched != 1 || got.modified != 1 || (i > 0 && got.checkpointsBeforeACK == 0) {
				t.Fatalf("update %d result=%+v", i, got)
			}
			if i == 1 {
				early, late = got.checkpointsBeforeACK, got.checkpointsBeforeACK
			} else if i == 2 {
				early, late = min(early, got.checkpointsBeforeACK), max(late, got.checkpointsBeforeACK)
			}
		case <-time.After(wait):
			t.Fatalf("missing update %d ACK", i)
		}
	}
	restoreObserver()
	after, afterACK := s.Stats(), s.Server.StandaloneWriteConcernStats()
	delta := func(key string) uint64 {
		return writeConcernBenchmarkStat(t, after, key) - writeConcernBenchmarkStat(t, before, key)
	}
	wantApplied := applied + 3
	if appends.Load() != 3 || checkpointCompletions.Load() != 2 || checkpointStarts.Load() < 2 ||
		delta("treedb.cache.checkpoint.runs") != 2 || delta("treedb.command_wal.group_commit.forced_total") != 0 ||
		s.Backend.State().AppliedCommandLSN != wantApplied ||
		afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 ||
		afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != 2 ||
		early < 1 || late != 2 {
		t.Fatalf("relaxed exact checkpoint census: appends=%d index_syncs=%d completed=%d runs=%d forces=%d applied=%d want=%d ackAt=%d,%d before=%+v after=%+v",
			appends.Load(), checkpointStarts.Load(), checkpointCompletions.Load(), delta("treedb.cache.checkpoint.runs"), delta("treedb.command_wal.group_commit.forced_total"), s.Backend.State().AppliedCommandLSN, wantApplied, early, late, beforeACK, afterACK)
	}
	assertStandaloneJournalRouteStats(t, s.Server, map[string]uint64{"UpdateModified.ExclusiveFreshFence": 2})
	check := func(owner *StandaloneServer) {
		t.Helper()
		for i, id := range ids {
			response := serveCommand(t, owner.Server, int32(i+1), bson.D{{Key: "find", Value: "rows"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: id}}}, {Key: "$db", Value: "epochs"}})
			assertOK(t, response)
			docs := cursorFirstBatch(t, response)
			if len(docs) != 1 {
				t.Fatalf("readback for update %d returned %d rows", i, len(docs))
			}
			requireStandaloneReceiptTestDocument(t, docs[0], id, i+11)
		}
	}
	check(s)
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, s)
	fresh, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
	})
	checkStandaloneWALStream(t, fresh, true)
	if got := fresh.Backend.State().AppliedCommandLSN; got != wantApplied {
		t.Fatalf("reopened applied LSN=%d want=%d", got, wantApplied)
	}
	check(fresh)
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
}
