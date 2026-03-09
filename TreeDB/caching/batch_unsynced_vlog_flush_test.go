package caching

import (
	"bytes"
	"encoding/binary"
	"testing"

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

func TestProfileFast_BatchWriteFlushesPointerValueLogBeforeMetadataSync(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
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
		WriterFlushMaxMemtables:    0,
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

	if db.lanes[0].vlogDirty.Load() {
		t.Fatalf("expected unsynced fast batch write to flush pointer payload bytes")
	}

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
