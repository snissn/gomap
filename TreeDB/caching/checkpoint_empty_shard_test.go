package caching

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func openEmptyShardCheckpointDB(t *testing.T, mode string) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		MemtableMode:             mode,
		MemtableShards:           4,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func emptyShardCheckpointKeys(t *testing.T, db *DB) [][]byte {
	t.Helper()
	keys := make([][]byte, len(db.mutableShards))
	remaining := len(keys)
	for i := 0; i < 10000 && remaining > 0; i++ {
		key := []byte(fmt.Sprintf("checkpoint-shard-%08d", i))
		shard := db.shardIndex(key)
		if keys[shard] == nil {
			keys[shard] = key
			remaining--
		}
	}
	if remaining != 0 {
		t.Fatal("could not find a key for every shard")
	}
	return keys
}

func TestCheckpointEmptyAppendOnlyShardReusePreservesSnapshotAndTrimsBacking(t *testing.T) {
	db := openEmptyShardCheckpointDB(t, "append_only")
	keys := emptyShardCheckpointKeys(t, db)
	captured := bytes.Repeat([]byte("captured"), 32)
	if err := db.Set(keys[0], captured); err != nil {
		t.Fatal(err)
	}
	held := db.AcquireSnapshot()
	if held == nil {
		t.Fatal("snapshot is nil")
	}
	defer held.Close()

	// Keep the snapshot's view alive across cutover and reuse. Its immutable
	// roots must continue excluding later writes to an initially empty shard.
	before := make([]memtable.Table, len(db.mutableShards))
	for i := range before {
		before[i] = db.mutableShards[i].mem
	}
	idle := before[1].(*memtable.AppendOnly)
	idle.ReserveAdditionalEntries(1 << 16)
	warmBacking := idle.EntryBackingBytes()
	if warmBacking == 0 {
		t.Fatal("reservation did not create entry backing")
	}
	versionBefore := db.memtables.Load().rootVersion
	queueIDBefore := db.nextQueueID.Load()
	cutoverObserved := false
	db.testAfterCheckpointFrontierCapture = func() {
		cutoverObserved = true
		if db.mutableShards[1].mem != idle {
			t.Error("checkpoint replaced an empty append-only shard")
		}
		if got := idle.EntryBackingBytes(); got >= warmBacking {
			t.Errorf("cutover retained oversized entry backing: before=%d after=%d", warmBacking, got)
		}
	}
	// WAL-off SetSync takes the public point-write checkpoint path.
	if err := db.SetSync(keys[0], []byte("checkpointed")); err != nil {
		t.Fatal(err)
	}
	db.testAfterCheckpointFrontierCapture = nil
	if !cutoverObserved {
		t.Fatal("SetSync did not capture a checkpoint frontier")
	}
	if db.mutableShards[0].mem == before[0] {
		t.Fatal("checkpoint retained the populated shard")
	}
	for i := 1; i < len(before); i++ {
		if db.mutableShards[i].mem != before[i] {
			t.Fatalf("empty shard %d was replaced", i)
		}
		if got := before[i].(*memtable.AppendOnly).EntryBackingBytes(); got != 0 {
			t.Fatalf("empty shard %d backing after checkpoint=%d", i, got)
		}
	}
	if got := db.memtables.Load().rootVersion; got <= versionBefore {
		t.Fatalf("root version did not advance: before=%d after=%d", versionBefore, got)
	}
	if got := db.nextQueueID.Load(); got != queueIDBefore+1 {
		t.Fatalf("checkpoint queue frontier=%d want %d", got, queueIDBefore+1)
	}
	if err := db.SetSync(keys[1], []byte("later")); err != nil {
		t.Fatal(err)
	}
	if found, err := held.Has(keys[1]); err != nil || found {
		t.Fatalf("held snapshot saw later empty-shard write: found=%t err=%v", found, err)
	}
	if got, err := held.Get(keys[0]); err != nil || !bytes.Equal(got, captured) {
		t.Fatalf("held snapshot value=%q err=%v", got, err)
	}
	it, err := held.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	rows := 0
	for ; it.Valid(); it.Next() {
		rows++
		if !bytes.Equal(it.Key(), keys[0]) || !bytes.Equal(it.Value(), captured) {
			t.Fatalf("held iterator observed later data: key=%q value=%q", it.Key(), it.Value())
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("held iterator rows=%d want 1", rows)
	}
	if got, err := db.Get(keys[1]); err != nil || string(got) != "later" {
		t.Fatalf("live reused-shard value=%q err=%v", got, err)
	}
}

func TestCheckpointEmptyShardReusePreservesModeConversion(t *testing.T) {
	for _, mode := range []string{"skiplist", "hash_sorted", "btree", "adaptive"} {
		t.Run(mode, func(t *testing.T) {
			db := openEmptyShardCheckpointDB(t, mode)
			keys := emptyShardCheckpointKeys(t, db)
			before := make([]memtable.Table, len(db.mutableShards))
			for i := range before {
				before[i] = db.mutableShards[i].mem
			}
			if err := db.Set(keys[0], []byte("durable")); err != nil {
				t.Fatal(err)
			}
			if mode == "adaptive" {
				// Model an ordered-successor hint arriving before checkpoint's
				// adaptive decision. Empty shards must convert with populated ones.
				db.memtableMVCCOrdered.Store(true)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for i := range before {
				if db.mutableShards[i].mem == before[i] {
					t.Fatalf("checkpoint retained shard %d in mode %s", i, mode)
				}
				if mode == "adaptive" {
					if _, ok := db.mutableShards[i].mem.(*memtable.BTree); !ok {
						t.Fatalf("ordered shard %d has type %T", i, db.mutableShards[i].mem)
					}
				}
			}
			if got, err := db.Get(keys[0]); err != nil || string(got) != "durable" {
				t.Fatalf("checkpoint value=%q err=%v", got, err)
			}
		})
	}
}

func TestCheckpointEmptyShardReuseWithConcurrentPointReads(t *testing.T) {
	db := openEmptyShardCheckpointDB(t, "append_only")
	keys := emptyShardCheckpointKeys(t, db)
	if err := db.SetSync(keys[0], []byte("stable")); err != nil {
		t.Fatal(err)
	}
	// Leave a mutable source so GetManyView retains the published memtable view
	// rather than taking its backend-only fast path.
	if err := db.Set(keys[0], []byte("stable")); err != nil {
		t.Fatal(err)
	}
	idle := db.mutableShards[1].mem
	stop := make(chan struct{})
	readerPaused := make(chan struct{})
	resumeReader := make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	releaseReader := func() { resumeOnce.Do(func() { close(resumeReader) }) }
	done := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			values, err := db.GetMany(keys)
			if err != nil {
				done <- err
				return
			}
			if string(values[0]) != "stable" {
				done <- fmt.Errorf("GetMany stable value=%q", values[0])
				return
			}
			if err := db.GetManyView(keys, func(i int, _ []byte, value []byte, found bool) error {
				if i == 0 && (!found || string(value) != "stable") {
					return fmt.Errorf("GetManyView stable value=%q found=%t", value, found)
				}
				if i > 0 && found {
					return fmt.Errorf("GetManyView found initially empty shard %d", i)
				}
				if i == 0 {
					pauseOnce.Do(func() {
						close(readerPaused)
						<-resumeReader
					})
				}
				return nil
			}); err != nil {
				done <- err
				return
			}
		}
	}()
	defer func() {
		close(stop)
		releaseReader()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-readerPaused:
	case <-time.After(withRaceTimeout(5 * time.Second)):
		t.Fatal("GetManyView did not reach its retained-view callback")
	}
	checkpointStarted := make(chan struct{})
	checkpointDone := make(chan error, 1)
	var checkpointStartOnce sync.Once
	db.testBeforeCheckpointFrontierCapture = func() { checkpointStartOnce.Do(func() { close(checkpointStarted) }) }
	ctx, cancel := context.WithTimeout(context.Background(), withRaceTimeout(5*time.Second))
	defer cancel()
	go func() { checkpointDone <- db.CheckpointContext(ctx) }()
	select {
	case <-checkpointStarted:
	case <-ctx.Done():
		t.Fatal("checkpoint did not begin cutover while the read callback was held")
	}
	// Establish actual overlap without requiring checkpoint to finish while a
	// reader holds its view: table reset may legitimately wait for that reader.
	releaseReader()
	if err := <-checkpointDone; err != nil {
		t.Fatal(err)
	}
	db.testBeforeCheckpointFrontierCapture = nil
	if db.mutableShards[1].mem != idle {
		t.Fatal("checkpoint replaced the empty shard while the old read view was retained")
	}
	for i := 0; i < 8; i++ {
		if err := db.SetSync(keys[0], []byte("stable")); err != nil {
			t.Fatal(err)
		}
	}
}
