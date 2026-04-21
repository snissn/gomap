package caching

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func unsyncedFlushTestKey(version uint64, nonce uint32) []byte {
	key := make([]byte, 13)
	key[0] = 's'
	binary.BigEndian.PutUint64(key[1:9], version)
	binary.BigEndian.PutUint32(key[9:13], nonce)
	return key
}

func unsyncedFlushScanBounds() ([]byte, []byte) {
	start := make([]byte, 9)
	start[0] = 's'
	binary.BigEndian.PutUint64(start[1:9], uint64(1))
	end := make([]byte, 9)
	end[0] = 's'
	binary.BigEndian.PutUint64(end[1:9], ^uint64(0))
	return start, end
}

func findKeysForDistinctLanes(t *testing.T, db *DB, wantedLanes, perLane int) [][]byte {
	t.Helper()
	seen := make(map[int][][]byte, wantedLanes)
	for i := 0; i < 1<<16; i++ {
		key := []byte{byte(i >> 8), byte(i), byte(i >> 4), byte(i * 7)}
		laneID := db.laneForShardIndex(db.shardIndex(key))
		if len(seen) >= wantedLanes {
			if _, ok := seen[laneID]; !ok {
				continue
			}
		}
		bucket := seen[laneID]
		if len(bucket) >= perLane {
			continue
		}
		seen[laneID] = append(bucket, append([]byte(nil), key...))
		ready := len(seen) >= wantedLanes
		if ready {
			for _, keys := range seen {
				if len(keys) < perLane {
					ready = false
					break
				}
			}
		}
		if ready {
			break
		}
	}
	if len(seen) < wantedLanes {
		t.Fatalf("found %d distinct lanes, want at least %d", len(seen), wantedLanes)
	}
	keys := make([][]byte, 0, wantedLanes*perLane)
	for _, laneKeys := range seen {
		if len(laneKeys) < perLane {
			t.Fatalf("lane produced %d keys, want at least %d", len(laneKeys), perLane)
		}
		keys = append(keys, laneKeys[:perLane]...)
	}
	return keys
}

func waitForLaneVlogDirtyState(t *testing.T, db *DB, laneID int, wantDirty bool) {
	t.Helper()
	waitFor := 2 * time.Second
	if ddl, ok := t.Deadline(); ok {
		if remaining := time.Until(ddl) / 10; remaining > 0 && remaining < waitFor {
			waitFor = remaining
		}
	}
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if db.lanes[laneID].vlogDirty.Load() == wantDirty {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	gotDirty := db.lanes[laneID].vlogDirty.Load()
	t.Fatalf("lane %d vlogDirty=%t want %t after %s", laneID, gotDirty, wantDirty, waitFor)
}

func TestProfileFast_BatchWriteFlushesPointerValueLogBeforeMetadataSync(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = backend.Close() }()
	db, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		RelaxedSync:                true,
		FlushThreshold:             1 << 30,
		JournalLanes:               1,
		MemtableShards:             1,
		IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold:   1,
		ValueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationOff),
		WriterFlushMaxDuration:     0,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	const (
		version   = uint64(9_123_456)
		valueSize = vlogQueueMinValueSize + 512
	)

	prefix := []byte("s/k:staking/")
	firstNodeKey := append(append([]byte(nil), prefix...), unsyncedFlushTestKey(version, 2)...)
	secondNodeKey := append(append([]byte(nil), prefix...), unsyncedFlushTestKey(version, 3)...)
	rootKey := append(append([]byte(nil), prefix...), unsyncedFlushTestKey(version, 1)...)

	firstValue := bytes.Repeat([]byte("A"), valueSize)
	secondValue := bytes.Repeat([]byte("B"), valueSize)
	rootValue := bytes.Repeat([]byte("R"), valueSize)

	b := db.NewBatch()
	if err := b.Set(firstNodeKey, firstValue); err != nil {
		t.Fatalf("set first node: %v", err)
	}
	if err := b.Set(secondNodeKey, secondValue); err != nil {
		t.Fatalf("set second node: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write unsynced batch: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close unsynced batch: %v", err)
	}

	waitForLaneVlogDirtyState(t, db, 0, false)

	meta := db.NewBatch()
	if err := meta.Set(rootKey, rootValue); err != nil {
		t.Fatalf("set root: %v", err)
	}
	if err := meta.WriteSync(); err != nil {
		t.Fatalf("write sync metadata batch: %v", err)
	}
	if err := meta.Close(); err != nil {
		t.Fatalf("close metadata batch: %v", err)
	}

	gotRoot, err := db.Get(rootKey)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	if !bytes.Equal(gotRoot, rootValue) {
		t.Fatalf("root mismatch got_len=%d want_len=%d", len(gotRoot), len(rootValue))
	}

	gotFirst, err := db.Get(firstNodeKey)
	if err != nil {
		t.Fatalf("get first node: %v", err)
	}
	if !bytes.Equal(gotFirst, firstValue) {
		t.Fatalf("first node mismatch got_len=%d want_len=%d", len(gotFirst), len(firstValue))
	}

	gotSecond, err := db.Get(secondNodeKey)
	if err != nil {
		t.Fatalf("get second node: %v", err)
	}
	if !bytes.Equal(gotSecond, secondValue) {
		t.Fatalf("second node mismatch got_len=%d want_len=%d", len(gotSecond), len(secondValue))
	}

	start, end := unsyncedFlushScanBounds()
	start = append(append([]byte(nil), prefix...), start...)
	end = append(append([]byte(nil), prefix...), end...)
	rit, err := db.ReverseIterator(start, end)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	if !rit.Valid() {
		_ = rit.Close()
		t.Fatalf("reverse iterator invalid")
	}
	if got := binary.BigEndian.Uint64(rit.Key()[len(prefix)+1 : len(prefix)+9]); got != version {
		_ = rit.Close()
		t.Fatalf("latest version=%d want=%d", got, version)
	}
	if err := rit.Close(); err != nil {
		t.Fatalf("reverse iterator close: %v", err)
	}
}

func TestCheckpoint_SyncsFlushedValueLogBytesInStrictMode(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = backend.Close() }()
	db, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		RelaxedSync:                false,
		FlushThreshold:             1 << 30,
		JournalLanes:               1,
		MemtableShards:             1,
		IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold:   1,
		ValueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationOff),
		WriterFlushMaxDuration:     0,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if err := b.Set([]byte("strict/key"), bytes.Repeat([]byte("V"), vlogQueueMinValueSize+128)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForLaneVlogDirtyState(t, db, 0, true)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	waitForLaneVlogDirtyState(t, db, 0, false)
	if db.hasDirtyValueLogLanes() {
		t.Fatalf("expected checkpoint to clear pending strict-sync value-log state")
	}
}

func TestCheckpoint_SyncsFlushBarrierValueLogBytesInStrictMode(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = backend.Close() }()
	db, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		RelaxedSync:                false,
		FlushThreshold:             1 << 30,
		JournalLanes:               1,
		MemtableShards:             1,
		IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold:   1,
		ValueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationOff),
		WriterFlushMaxDuration:     0,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if err := b.Set([]byte("strict/flush-barrier"), bytes.Repeat([]byte("F"), vlogQueueMinValueSize+128)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForLaneVlogDirtyState(t, db, 0, true)
	if err := db.flushValueLogLane(&db.lanes[0]); err != nil {
		t.Fatalf("flushValueLogLane: %v", err)
	}
	if db.lanes[0].vlogDirty.Load() {
		t.Fatalf("expected flush barrier to clear reader-visible dirty state")
	}
	if !db.lanes[0].vlogSyncPending.Load() {
		t.Fatalf("expected strict-mode flush barrier to leave sync pending")
	}
	if !db.hasDirtyValueLogLanes() {
		t.Fatalf("expected checkpoint to still observe pending strict-sync value-log state")
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if db.hasDirtyValueLogLanes() {
		t.Fatalf("expected checkpoint to clear pending strict-sync value-log state after flush barrier")
	}
}

func TestProfileFast_BatchWriteFlushesPointerValueLogAcrossMultipleLanes(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = backend.Close() }()
	db, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		RelaxedSync:                true,
		FlushThreshold:             1 << 30,
		JournalLanes:               4,
		MemtableShards:             16,
		IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold:   1,
		ValueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationOff),
		WriterFlushMaxDuration:     0,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	b := db.NewBatch()
	value := bytes.Repeat([]byte("M"), vlogQueueMinValueSize+256)
	keys := findKeysForDistinctLanes(t, db, 2, multiLaneValueLogMinRecords/2+1)
	for i := 0; i < multiLaneValueLogMinRecords; i++ {
		key := keys[i]
		if err := b.Set(key, value); err != nil {
			t.Fatalf("set key %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write unsynced batch: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close unsynced batch: %v", err)
	}

	activeLanes := 0
	for i := range db.lanes {
		waitForLaneVlogDirtyState(t, db, i, false)
		if db.lanes[i].vlogLiveBytes.Load() > 0 {
			activeLanes++
		}
	}
	if activeLanes < 2 {
		t.Fatalf("expected multi-lane pointer fan-out, active_lanes=%d", activeLanes)
	}
}
