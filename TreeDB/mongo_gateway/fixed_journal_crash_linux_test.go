package mongogateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestStandaloneFixedJournalFenceKillReopen(t *testing.T) {
	const childDir = "GOMAP_FIXED_JOURNAL_FENCE_CRASH_DIR"
	const count = 64
	const requests = count / 2
	open := func(dir string) *StandaloneServer {
		t.Helper()
		owner, err := OpenStandaloneServer(StandaloneOptions{
			Dir: dir, Profile: treedb.ProfileCommandWALDurable,
			DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
		})
		if err != nil {
			t.Fatal(err)
		}
		requireStandaloneNativeJournalOwner(t, owner)
		if got := owner.Stats()["treedb.command_wal.writer.container"]; got != "fixed" {
			t.Fatalf("WAL container = %q, want fixed", got)
		}
		return owner
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	if dir := os.Getenv(childDir); dir != "" {
		owner := open(filepath.Join(dir, "db"))
		client, _, _, _ := startStandaloneMongoClientForTest(t, owner)
		// Precreate the BSON collection so all 32 concurrent multi-document
		// InsertMany requests deterministically take the covered operation-receipt
		// group-commit path instead of racing on cold catalog auto-create.
		if err := client.Database("diagnostic").CreateCollection(ctx, "docs"); err != nil {
			t.Fatalf("precreate diagnostic.docs: %v", err)
		}
		journal := true
		coll := client.Database("diagnostic").Collection("docs", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
		beforeStats := owner.Stats()
		beforeACK := owner.Server.StandaloneWriteConcernStats()
		var group sync.WaitGroup
		failures := make(chan error, requests)
		start := make(chan struct{})
		group.Add(requests)
		for request := range requests {
			go func() {
				defer group.Done()
				<-start
				index := request * 2
				first, second := fmt.Sprintf("key-%02d", index), fmt.Sprintf("key-%02d", index+1)
				result, err := coll.InsertMany(ctx, []any{standaloneNativeJournalDocument(first, index), standaloneNativeJournalDocument(second, index+1)})
				if err != nil || result == nil || !result.Acknowledged || len(result.InsertedIDs) != 2 ||
					result.InsertedIDs[0] != first || result.InsertedIDs[1] != second {
					failures <- fmt.Errorf("insert %s/%s: result=%+v err=%v", first, second, result, err)
				}
			}()
		}
		close(start)
		group.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
		if t.Failed() {
			t.FailNow()
		}
		afterBatchACK := owner.Server.StandaloneWriteConcernStats()
		afterBatchStats := owner.Stats()
		if afterBatchACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != requests ||
			afterBatchACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != requests ||
			afterBatchACK.SyncFailures != beforeACK.SyncFailures ||
			afterBatchACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
			t.Fatalf("covered batch journal ACK proof: before=%+v after=%+v", beforeACK, afterBatchACK)
		}
		if forced := writeConcernBenchmarkStat(t, afterBatchStats, "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, beforeStats, "treedb.command_wal.group_commit.forced_total"); forced != 0 {
			t.Fatalf("covered multi-doc BSON InsertMany forced journal groups = %d, want 0", forced)
		}
		if syncOps := writeConcernBenchmarkStat(t, afterBatchStats, "treedb.command_wal.sync.count_total") - writeConcernBenchmarkStat(t, beforeStats, "treedb.command_wal.sync.count_total"); syncOps < 1 {
			t.Fatalf("covered multi-doc BSON InsertMany WAL sync ops = %d, want >= 1", syncOps)
		}
		if fileSyncs := writeConcernBenchmarkStat(t, afterBatchStats, "treedb.command_wal.file_sync.calls_total") - writeConcernBenchmarkStat(t, beforeStats, "treedb.command_wal.file_sync.calls_total"); fileSyncs < 1 {
			t.Fatalf("covered multi-doc BSON InsertMany WAL file syncs = %d, want >= 1", fileSyncs)
		}
		// Separately exercise the uncovered standalone prefix fence (catalog
		// auto-create) before SIGKILL so both receipt-covered and prefix-fenced
		// crash recovery are verified with zero cache checkpoints.
		uncoveredColl := client.Database("diagnostic").Collection("uncovered_docs", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
		uncoveredRes, err := uncoveredColl.InsertOne(ctx, standaloneNativeJournalDocument("uncovered-00", 1000))
		if err != nil || uncoveredRes == nil || !uncoveredRes.Acknowledged || uncoveredRes.InsertedID != "uncovered-00" {
			t.Fatalf("uncovered auto-create insert result=%+v err=%v", uncoveredRes, err)
		}
		ack := owner.Server.StandaloneWriteConcernStats()
		stats := owner.Stats()
		if ack.JournalAcknowledgements-afterBatchACK.JournalAcknowledgements != 1 ||
			ack.PhysicalSyncBoundaries-afterBatchACK.PhysicalSyncBoundaries != 1 ||
			ack.SyncFailures != afterBatchACK.SyncFailures ||
			ack.UncertainOutcomes != afterBatchACK.UncertainOutcomes {
			t.Fatalf("uncovered journal ACK proof: before=%+v after=%+v", afterBatchACK, ack)
		}
		if forced := writeConcernBenchmarkStat(t, stats, "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, afterBatchStats, "treedb.command_wal.group_commit.forced_total"); forced != 1 {
			t.Fatalf("uncovered forced journal groups = %d, want 1", forced)
		}
		if checkpoints := writeConcernBenchmarkStat(t, stats, "treedb.cache.checkpoint.runs"); checkpoints != 0 {
			t.Fatalf("cache checkpoint runs = %d, want no physical checkpoints before kill", checkpoints)
		}
		if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("32 journal ACKs"), 0600); err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		t.Fatal("parent did not send SIGKILL")
	}

	dir := t.TempDir()
	requireStandaloneExt4(t, dir)
	logPath := filepath.Join(dir, "child.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStandaloneFixedJournalFenceKillReopen$", "-test.v")
	cmd.Env = append(os.Environ(), childDir+"="+dir)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exit := make(chan error, 1)
	go func() { exit <- cmd.Wait() }()
	stopped := false
	defer func() {
		if !stopped {
			_ = cmd.Process.Kill()
			<-exit
		}
	}()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-exit:
			stopped = true
			log, _ := os.ReadFile(logPath)
			t.Fatalf("child exited before crash: %v\n%s", err, log)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
			ready, err := os.ReadFile(filepath.Join(dir, "ready"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if string(ready) == "32 journal ACKs" {
				goto kill
			}
		}
	}

kill:
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = <-exit
	stopped = true
	var killed *exec.ExitError
	if !errors.As(err, &killed) || killed.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child exit = %v, want SIGKILL", err)
	}
	owner := open(filepath.Join(dir, "db"))
	client, stopCtx, listener, served := startStandaloneMongoClientForTest(t, owner)
	defer stopStandaloneMongoClientForTest(t, client, stopCtx, listener, served, owner)
	coll := client.Database("diagnostic").Collection("docs")
	cursor, err := coll.Find(t.Context(), bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cursor.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	rows := make(map[string]bson.Raw, count)
	for cursor.Next(t.Context()) {
		id := cursor.Current.Lookup("_id").StringValue()
		if _, duplicate := rows[id]; duplicate {
			t.Fatalf("duplicate recovered document %q", id)
		}
		rows[id] = append(bson.Raw(nil), cursor.Current...)
	}
	if err := cursor.Err(); err != nil || len(rows) != count {
		t.Fatalf("typed count=%d err=%v, want %d", len(rows), err, count)
	}
	for index := range count {
		id := fmt.Sprintf("key-%02d", index)
		document, found := rows[id]
		if !found {
			t.Fatalf("missing recovered document %q", id)
		}
		requireStandaloneNativeJournalDocument(t, document, id, index)
	}
	var uncoveredDoc bson.Raw
	if err := client.Database("diagnostic").Collection("uncovered_docs").FindOne(t.Context(), bson.D{{Key: "_id", Value: "uncovered-00"}}).Decode(&uncoveredDoc); err != nil {
		t.Fatalf("missing recovered uncovered document: %v", err)
	}
	requireStandaloneNativeJournalDocument(t, uncoveredDoc, "uncovered-00", 1000)
	t.Logf("SIGKILL after %d concurrent receipt ACKs + 1 uncovered prefix-fence ACK with no cache checkpoint; all %d + 1 typed rows verified on fresh owner", requests, count)
}
