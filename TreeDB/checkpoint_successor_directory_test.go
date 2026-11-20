package treedb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestAutomaticCheckpointSuccessorDirectoryAdmitsRelaxedBSON(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := nativeBSONRootCoveredOptions(ProfileCommandWALRelaxed, t.TempDir())
	dir, err := os.Open(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(dir)
	if err = dir.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires a positively identified ext4 filesystem")
	}
	opts.FreshRelaxedStandaloneWALStream = true
	opts.COWMemtableLimits = StandaloneCOWMemtableLimits()
	opts.MaxWALBytes = 2 << 30
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(db.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	rows, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	row := func(n int64) []byte {
		return nativeBSONDocument(t, bson.D{{Key: "_id", Value: "r"}, {Key: "n", Value: n}})
	}
	id := []byte("r")
	if _, err = rows.InsertBatchValidatedBSON([][]byte{id}, [][]byte{row(0)}); err != nil {
		t.Fatal(err)
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	update := func(v bson.RawValue) error {
		res, ok, err := rows.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]collections.BSONSetUpdateBatchItem{{DocumentID: id, Fields: []collections.BSONSetField{{Key: "n", Value: v}}}})
		if err != nil {
			return err
		}
		if !ok || len(res) != 1 || !res[0].Matched || !res[0].Modified {
			return fmt.Errorf("BSON UPDATE result %+v, accepted=%v", res, ok)
		}
		return nil
	}
	if err = update(nativeBSONQuery(t, int64(1))); err != nil {
		t.Fatal(err)
	}
	before := prefix.LogicalFrontiers()
	applied := indexedCRUDAppliedLSN(t, backend)
	stats := db.Stats()
	count := statMapUint64(t, stats, "treedb.cache.auto_checkpoint.count")
	old := stats["treedb.command_wal.writer.path"]
	walDir := filepath.Dir(old)
	parent, err := os.Stat(walDir)
	if err != nil {
		t.Fatal(err)
	}

	// Select the original successful old-file sync, then the fresh canonical
	// create. The first directory event through its retained parent belongs to
	// that rotation. A relaxed ACK may move V/C only while this event is held.
	var oldSynced, parked, completed atomic.Bool
	var successor atomic.Pointer[string]
	var lastAppend atomic.Uint64
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		switch {
		case e.Path == old && e.Point == durabilitycut.AfterDependencyFileSync:
			if _, err := commandWALFixtureSyncIdentity(e); err != nil {
				return err
			}
			oldSynced.Store(true)
		case e.Namespace == durabilitycut.NamespaceCreate && oldSynced.Load() && filepath.Dir(e.NewPath) == walDir && e.NewPath != old && commitlog.IsCommandSegmentName(filepath.Base(e.NewPath)):
			successor.CompareAndSwap(nil, &e.NewPath)
		case e.Path == walDir && successor.Load() != nil && e.StablePath != nil:
			if e.Point != durabilitycut.BeforeNewFileDirectorySync && e.Point != durabilitycut.AfterNewFileDirectorySync {
				return nil
			}
			info, err := e.StablePath.Stat()
			if err != nil || !os.SameFile(info, parent) {
				return fmt.Errorf("successor directory identity changed: %v", err)
			}
			if e.Point == durabilitycut.BeforeNewFileDirectorySync && parked.CompareAndSwap(false, true) {
				close(entered)
				<-release
			} else if e.Point == durabilitycut.AfterNewFileDirectorySync {
				completed.Store(true)
			}
		case e.Point == durabilitycut.AfterDependencyAppend && parked.Load() && !completed.Load():
			if s := successor.Load(); s == nil || e.Path != *s || !lastAppend.CompareAndSwap(0, e.LSN) {
				return fmt.Errorf("unexpected append %s/%d before directory completion", e.Path, e.LSN)
			}
		}
		return nil
	})
	var workers sync.WaitGroup
	t.Cleanup(func() { resume(); workers.Wait(); restore() })
	db.triggerAutoCheckpointForTest()
	if err = waitForPublicCommandWALCheckpointPhase(entered, "automatic checkpoint successor directory"); err != nil {
		t.Fatal(err)
	}
	value := nativeBSONQuery(t, int64(2))
	ack := make(chan error, 1)
	workers.Go(func() { ack <- update(value) })
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(publicCommandWALCheckpointTestTimeout):
		t.Fatal("relaxed BSON UPDATE blocked on the installed successor's directory sync")
	}
	after := prefix.LogicalFrontiers()
	if after.C != before.C+1 || after.V != before.V+1 || after.W != before.W || after.S != before.S || lastAppend.Load() != after.C || indexedCRUDAppliedLSN(t, backend) != applied || completed.Load() {
		t.Fatalf("while directory sync held, frontiers %+v -> %+v, append %d", before, after, lastAppend.Load())
	}

	durable := make(chan error, 1)
	workers.Go(func() { durable <- db.SetSync([]byte("durable"), []byte("yes")) })
	// A missed scheduling turn cannot prove the marker joined the outstanding
	// namespace. Require the SetSync goroutine inside that specific join.
	buf := make([]byte, 256<<10)
	deadline := time.Now().Add(publicCommandWALCheckpointTestTimeout)
	for {
		stacks := buf[:runtime.Stack(buf, true)]
		found := false
		for stack := range bytes.SplitSeq(stacks, []byte("\n\n")) {
			if bytes.Contains(stack, []byte("setSyncWithMVCCInput(")) && bytes.Contains(stack, []byte(".finishCheckpointNamespaceLocked(")) {
				found = true
				break
			}
		}
		if found {
			break
		}
		select {
		case err := <-durable:
			t.Fatalf("durable result before namespace completion: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("durable caller did not join the original namespace sync")
		}
		time.Sleep(time.Millisecond)
	}
	if next := backend.CommandWALNextLSN(); next != after.C+1 || prefix.LogicalFrontiers() != after {
		t.Fatalf("marker advanced before directory sync: next %d, frontiers %+v", next, prefix.LogicalFrontiers())
	}
	resume()
	if err = <-durable; err != nil {
		t.Fatal(err)
	}
	if !completed.Load() || prefix.LogicalFrontiers().W < after.C+1 {
		t.Fatalf("durable ACK without directory completion and WAL coverage: %+v", prefix.LogicalFrontiers())
	}
	if err = waitForPublicCommandWALAutoCheckpointCount(db, count+1); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	rows, err = collections.NewCollectionManager(backend).OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	checkNativeBSONDocument(t, rows, "r", row(2))
	requireRawKVValue(t, db, []byte("durable"), []byte("yes"))
}
