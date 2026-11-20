package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestPublicCOWEarlySealedCommandWALSync(t *testing.T) {
	for _, tc := range []struct {
		name          string
		refuse, close bool
	}{
		{name: "checkpoint-contends"},
		{name: "refusal-retries", refuse: true},
		{name: "close-joins", close: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPublicCOWEarlySealedCommandWALSync(t, tc.refuse, tc.close)
		})
	}
}

func testPublicCOWEarlySealedCommandWALSync(t *testing.T, refuse, closeDuringSync bool) {
	t.Helper()
	// Keep the unrelated periodic value-log checkpoint from inserting an
	// automatic WAL barrier. The ordinary WAL maintenance owner stays active.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.COWMemtableLimits = StandaloneCOWMemtableLimits()
	dir, err := os.Open(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(dir)
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires actual ext4")
	}
	opts.FreshRelaxedStandaloneWALStream = true
	opts.MemtableMode = "cow_btree"
	opts.CommandWALSegmentTargetBytes = 4 << 10
	opts.MaxWALBytes = 2 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if container := database.Stats()["treedb.command_wal.writer.container"]; container != "stream" {
		t.Fatalf("command WAL was opened as %q", container)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("row")
	value, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{value}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	write := func(n int64) error {
		typ, data, err := bson.MarshalValue(n)
		if err != nil {
			return err
		}
		results, accepted, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{
			DocumentID: id,
			Fields:     []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: data}}},
		}})
		if err != nil {
			return err
		}
		if !accepted || len(results) != 1 || !results[0].Matched || !results[0].Modified {
			return fmt.Errorf("unacknowledged update %d: %+v accepted=%t", n, results, accepted)
		}
		return nil
	}
	segments := publicCommandWALSegmentNames(t, filepath.Join(opts.Dir, "maindb"))
	oldName := segments[len(segments)-1]
	oldPath := filepath.Join(opts.Dir, "maindb", "wal", oldName)
	oldInfo, err := os.Stat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeW := prefix.LogicalFrontiers().W
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var oldCalls, oldSyncs, otherCalls, namespaces atomic.Int32
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Point == durabilitycut.AfterNewFileDirectorySync {
			namespaces.Add(1)
		}
		if e.Path != oldPath {
			if e.Point == durabilitycut.AfterDependencyFileSync {
				otherCalls.Add(1)
			}
			return nil
		}
		switch e.Point {
		case durabilitycut.BeforeDependencyFileSync:
			if oldCalls.Add(1) == 1 {
				if e.StablePath == nil {
					return fmt.Errorf("sealed WAL sync lacked its exact handle")
				}
				info, err := e.StablePath.Stat()
				if err != nil {
					return err
				}
				if !os.SameFile(info, oldInfo) || info.Size() <= 0 {
					return fmt.Errorf("sealed WAL sync used a different object or empty extent")
				}
				close(entered)
				<-release
				if refuse {
					return errors.New("decline optional sealed sync before physical I/O")
				}
			}
		case durabilitycut.AfterDependencyFileSync:
			oldSyncs.Add(1)
		}
		return nil
	}))
	defer restore()
	defer unblock()
	var last int64
	for last < 512 {
		last++
		if err := write(last); err != nil {
			t.Fatal(err)
		}
		names := publicCommandWALSegmentNames(t, filepath.Join(opts.Dir, "maindb"))
		if names[len(names)-1] != oldName {
			break
		}
	}
	if last == 512 {
		t.Fatal("relaxed foreground writes did not rotate the WAL")
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sealed WAL did not start a physical sync before the checkpoint")
	}
	last++
	updated := make(chan error, 1)
	go func() { updated <- write(last) }()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relaxed BSON update waited for the sealed-file sync")
	}
	if got := prefix.LogicalFrontiers().W; got != beforeW {
		t.Fatalf("sealed sync advanced W: before=%+v while held=%+v", beforeW, got)
	}
	// The existing active-only generation sync can finish while the detached
	// file is held. It has no authority to advance W.
	active := make(chan error, 1)
	go func() { active <- backend.SyncPrivateCOWGenerationWAL(database.txnOwner) }()
	select {
	case err := <-active:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active successor sync waited for the sealed predecessor")
	}
	if otherCalls.Load() != 1 || prefix.LogicalFrontiers().W != beforeW {
		t.Fatal("expected one active-only sync without W advance")
	}

	started, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		if closeDuringSync {
			finished <- database.Close()
		} else {
			finished <- database.Checkpoint()
		}
	}()
	<-started
	select {
	case err := <-finished:
		t.Fatalf("authoritative operation returned before the prior file completed: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("authoritative operation stalled after releasing the sealed sync")
	}
	if closeDuringSync {
		database = nil
	}
	wantAttempts := int32(1)
	if refuse {
		wantAttempts = 2
	}
	if calls, syncs := oldCalls.Load(), oldSyncs.Load(); calls != wantAttempts || syncs != 1 {
		t.Fatalf("old WAL attempts=%d syncs=%d, want %d attempts and one physical sync", calls, syncs, wantAttempts)
	}
	if otherCalls.Load() < 2 || namespaces.Load() == 0 {
		t.Fatalf("checkpoint missed active WAL or namespace barriers: file=%d directory=%d", otherCalls.Load(), namespaces.Load())
	}
	restore()
	if database != nil {
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	backend, err = reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	col, err = collections.NewCollectionManager(backend).OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	result, err := col.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := bson.Raw(result).Lookup("n").Int64OK(); !ok || got != last {
		t.Fatalf("recovered BSON n=%d typed=%t, want %d", got, ok, last)
	}
}
