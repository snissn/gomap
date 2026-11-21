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
		journal := true
		coll := client.Database("diagnostic").Collection("docs", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
		var group sync.WaitGroup
		failures := make(chan error, count)
		start := make(chan struct{})
		group.Add(count)
		for index := range count {
			go func() {
				defer group.Done()
				<-start
				id := fmt.Sprintf("key-%02d", index)
				result, err := coll.InsertOne(ctx, standaloneNativeJournalDocument(id, index))
				if err != nil || result == nil || !result.Acknowledged || result.InsertedID != id {
					failures <- fmt.Errorf("insert %s: result=%+v err=%v", id, result, err)
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
		ack := owner.Server.StandaloneWriteConcernStats()
		stats := owner.Stats()
		if ack.JournalAcknowledgements != count || ack.SyncFailures != 0 {
			t.Fatalf("journal ACK proof: %+v", ack)
		}
		if steps := writeConcernBenchmarkStat(t, stats, "treedb.command_wal.group_commit.forced_total"); steps != count {
			t.Fatalf("forced journal groups = %d, want %d", steps, count)
		}
		if checkpoints := writeConcernBenchmarkStat(t, stats, "treedb.cache.checkpoint.runs"); checkpoints != 0 {
			t.Fatalf("cache checkpoint runs = %d, want no physical checkpoints before kill", checkpoints)
		}
		if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("64 journal ACKs"), 0600); err != nil {
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
			if string(ready) == "64 journal ACKs" {
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
	t.Logf("SIGKILL after %d real driver ACKs with no cache checkpoint; all typed rows verified on fresh owner", count)
}
