package caching

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWRelaxedPointLaneAffinity(t *testing.T) {
	fixture := func(t *testing.T) *DB {
		t.Helper()
		dir := t.TempDir()
		backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: true, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		db, err := Open(dir, backend, Options{ExternalCommandWAL: true, FlushThreshold: 1 << 30, MemtableShards: 2, JournalLanes: 3, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff)})
		if err != nil {
			_ = backend.Close()
			t.Fatal(err)
		}
		c, err := newCOWCache(backend, 2, memtable.DefaultCOWLimits())
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		db.cow = c
		t.Cleanup(func() {
			if !db.closing.Load() {
				_ = db.Close()
			}
		})
		return db
	}
	write := func(db *DB, keys [][]byte, sync bool) error {
		b := db.NewBatchWithSize(len(keys))
		defer b.Close()
		for _, key := range keys {
			if err := b.Set(key, bytes.Repeat([]byte("v"), 256)); err != nil {
				return err
			}
		}
		return b.WriteAfterCommandWALAppendWithPreparedRevision(sync, func() error {
			_, err := db.backend.(*backenddb.DB).AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, len(keys), backenddb.RawKVCommandWALAppendRelaxed)
			return err
		})
	}
	key := []byte("point")
	t.Run("physical-placement", func(t *testing.T) {
		db := fixture(t)
		want := db.laneForShardIndex(db.shardIndex(key))
		db.nextLane = (want + 1) % len(db.lanes)
		for i := 0; i < 4; i++ {
			if err := write(db, [][]byte{key}, false); err != nil {
				t.Fatal(err)
			}
			s := db.AcquireSnapshot()
			e, err := s.GetEntry(key)
			if err != nil {
				_ = s.Close()
				t.Fatal(err)
			}
			lane, _ := valuelog.DecodeFileID(e.ValuePtr.FileID)
			if e.Flags&node.FlagPointer == 0 || int(lane) != want {
				_ = s.Close()
				t.Fatalf("pointer=%t physical lane=%d want=%d", e.Flags&node.FlagPointer != 0, lane, want)
			}
			got, err := s.Get(key)
			_ = s.Close()
			if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("v"), 256)) {
				t.Fatalf("published pointer read=%q err=%v", got, err)
			}
		}
		for i := range db.lanes {
			l := &db.lanes[i]
			l.vlogMu.Lock()
			created := l.vlog != nil
			if w, ok := l.vlog.(interface{ PendingBytes() int }); ok && w.PendingBytes() != 0 {
				t.Error("published producer bytes remain buffered")
			}
			l.vlogMu.Unlock()
			if created != (i == want) {
				t.Fatalf("lane %d writer created=%t; only affinity lane %d should open", i, created, want)
			}
		}
	})
	t.Run("multi-entry-fallback", func(t *testing.T) {
		db := fixture(t)
		want := (db.laneForShardIndex(db.shardIndex(key)) + 1) % len(db.lanes)
		db.nextLane = want
		if err := write(db, [][]byte{key, []byte("other")}, false); err != nil {
			t.Fatal(err)
		}
		s := db.AcquireSnapshot()
		defer s.Close()
		for _, k := range [][]byte{key, []byte("other")} {
			e, err := s.GetEntry(k)
			if err != nil {
				t.Fatal(err)
			}
			lane, _ := valuelog.DecodeFileID(e.ValuePtr.FileID)
			if e.Flags&node.FlagPointer == 0 || int(lane) != want {
				t.Fatalf("multi-entry lane=%d want=%d", lane, want)
			}
		}
	})
	t.Run("busy-release-and-close-progress", func(t *testing.T) {
		db := fixture(t)
		preferred := db.laneForShardIndex(db.shardIndex(key))
		lane, err := db.pickLane(true, preferred)
		if err != nil {
			t.Fatal(err)
		}
		released := false
		defer func() {
			if !released {
				db.releaseLaneSync(lane)
			}
		}()
		done := make(chan error, 1)
		go func() { done <- write(db, [][]byte{key}, false) }()
		deadline := time.Now().Add(5 * time.Second)
		for db.cow.writerMu.TryLock() {
			db.cow.writerMu.Unlock()
			if time.Now().After(deadline) {
				t.Fatal("writer did not enter COW admission")
			}
			runtime.Gosched()
		}
		select {
		case err := <-done:
			t.Fatalf("writer bypassed reserved preferred lane: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		closed := make(chan error, 1)
		go func() { closed <- db.Close() }()
		db.releaseLaneSync(lane)
		released = true
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, backenddb.ErrClosed) && !errors.Is(err, errWALClosed) {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("writer did not progress after lane release")
		}
		cowWait(t, closed)
	})
}
