package caching

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWAggregatePressureJoinsCheckpointBeforeProducer(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprintf("held=%t", held), func(t *testing.T) {
			dir := t.TempDir()
			backendDir := filepath.Join(dir, "backend")
			backend, err := backenddb.Open(backenddb.Options{Dir: backendDir, DisableBackgroundPrune: true, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1 << 30}})
			if err != nil {
				t.Fatal(err)
			}
			// A finite local control exposes parent pressure with little data.
			// The original Dgraph control retains its unchanged default 512MiB.
			limits := memtable.DefaultCOWLimits()
			limits.MaxTotalBytes, limits.MaxRetiredBytes = 64<<20, 64<<20
			limits.MaxGenerationBytes = 8 << 20
			db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", MemtableShards: 4, COWMemtableLimits: limits, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30, ValueLogCompression: uint8(vlogCompressionOff)})
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			defer db.Close()
			// Hold the existing flush fence while arranging a real frozen prefix;
			// every row below enters through the ordinary public raw batch path.
			db.flushMu.Lock()
			locked := true
			defer func() {
				if locked {
					db.flushMu.Unlock()
				}
			}()
			var keys [][]byte
			counts := make([]int, 4)
			const entries = 64
			for candidate := 0; len(keys) < entries && candidate < 1000; candidate++ {
				key := []byte(fmt.Sprintf("pressure-%04d", candidate))
				shard := db.shardIndex(key)
				if counts[shard] < entries/4 {
					counts[shard]++
					keys = append(keys, key)
				}
			}
			if len(keys) != entries {
				t.Fatal("failed to construct fixed all-shard raw group")
			}
			value := bytes.Repeat([]byte("a"), 1024)
			for round := 0; round < 4; round++ {
				b := db.NewBatchWithSize(entries)
				for _, key := range keys {
					if err := b.Set(key, value); err != nil {
						t.Fatal(err)
					}
				}
				if err := b.Write(); err != nil {
					t.Fatal(err)
				}
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.rolloverCOWForFlush(); err != nil {
				t.Fatal(err)
			}
			var old *Snapshot
			if held {
				old, err = db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if old != nil {
						_ = old.Close()
					}
				}()
			}
			b := db.NewBatchWithSize(entries)
			defer b.Close()
			value[0] = 'z'
			for _, key := range keys {
				if err := b.Set(key, value); err != nil {
					t.Fatal(err)
				}
			}
			stats := db.COWMemoryStats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			const headroom = 128 << 10
			// Each shard fits this remaining headroom in isolation. Their
			// simultaneous canonical reservations do not: this is aggregate
			// pressure, not an oversized single-shard command or generation.
			groups := make([][]memtable.COWMutation, len(counts))
			for _, key := range keys {
				i := db.shardIndex(key)
				groups[i] = append(groups[i], memtable.COWMutation{Key: key, Value: value, Flags: node.FlagInline})
			}
			var total uint64
			for i, group := range groups {
				charge, err := db.cow.writers[i].Estimate(group, memtable.COWPrepareOptions{})
				if err != nil || charge.Total() >= headroom {
					t.Fatalf("invalid single-shard pressure setup: charge=%+v err=%v", charge, err)
				}
				total += charge.Total()
			}
			if total <= headroom {
				t.Fatalf("all-shard estimate %d does not exceed headroom %d", total, headroom)
			}
			if stats.TotalBytes+overhead+headroom >= limits.MaxTotalBytes {
				t.Fatalf("invalid pressure setup: %+v", stats)
			}
			pressure, err := db.cow.budget.AcquireExternal(limits.MaxTotalBytes - stats.TotalBytes - overhead - headroom)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			beforeSeq, beforeCheckpoints := db.cow.cut.logicalSeq, db.checkpointRuns.Load()
			result := make(chan error, 1)
			go func() { result <- b.Write() }()
			// Admission must wait for the same existing flush owner, not return a
			// canonical prepare refusal while holding the handoff's writer gate.
			select {
			case err := <-result:
				t.Fatalf("write returned before pressure handoff could run: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			db.flushMu.Unlock()
			locked = false
			select {
			case err = <-result:
			case <-time.After(10 * time.Second):
				t.Fatal("unlocked checkpoint handoff did not finish")
			}
			if db.checkpointRuns.Load() != beforeCheckpoints+1 {
				t.Fatal("aggregate pressure did not join exactly one checkpoint")
			}
			if held {
				if !errors.Is(err, memtable.ErrCOWCapacity) || b.COWReceipt().Assigned || b.COWReceipt().LogicalInstalled || db.cow.cut.logicalSeq != beforeSeq {
					t.Fatalf("nonreclaimable history changed acceptance: receipt=%+v seq=%d err=%v", b.COWReceipt(), db.cow.cut.logicalSeq, err)
				}
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				old = nil
				// The caller can submit the still-unassigned staged batch after its
				// own retained cut ends; no accepted command is internally replayed.
				err = b.Write()
			}
			if err != nil || db.cow.cut.logicalSeq != beforeSeq+1 {
				t.Fatalf("raw command completion seq=%d err=%v", db.cow.cut.logicalSeq, err)
			}
			pressure.Close()
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				if got, err := db.Get(key); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("current %q=(%q,%v)", key, got, err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := db.COWMemoryStats(); stats.TotalBytes != 0 {
				t.Fatalf("original parent not drained: %+v", stats)
			}
			reopened, err := backenddb.Open(backenddb.Options{Dir: backendDir})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, key := range keys {
				if got, err := reopened.Get(key); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("reopened %q=(%q,%v)", key, got, err)
				}
			}
		})
	}
}

func cowFiniteGenerationFixture(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatal(err)
	}
	limits := memtable.DefaultCOWLimits()
	limits.MaxGenerationBytes = 256 << 10
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", MemtableShards: 1, COWMemtableLimits: limits, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCOWOrdinaryOverwriteAutomaticallyRollsBoundedGenerations(t *testing.T) {
	db := cowFiniteGenerationFixture(t)
	value := bytes.Repeat([]byte("a"), 1024)
	if err := db.Set([]byte("key"), value); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	generations := 1
	previous := db.cow.writers[0]
	for i := 0; i < 160; i++ {
		value[0] = byte(i)
		if err := db.Set([]byte("key"), value); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if db.cow.writers[0] != previous {
			generations++
			previous = db.cow.writers[0]
		}
		// Give the independent flush coordinator time to retire unpinned prior
		// generations. No checkpoint or synchronous backend flush is hidden in Set.
		time.Sleep(time.Millisecond)
	}
	if generations < 3 {
		t.Fatalf("generations=%d", generations)
	}
	got, err := old.Get([]byte("key"))
	if err != nil || len(got) != 1024 || got[0] != 'a' {
		t.Fatalf("old cut=(%v,%v)", got, err)
	}
	got, err = db.Get([]byte("key"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("current cut=(%v,%v)", got, err)
	}
}

func TestCOWPredictedRotationRefusesBeforeAppendAndResumesAfterDrain(t *testing.T) {
	db := cowFiniteGenerationFixture(t)
	if err := db.Set([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	c := db.cow
	// The first dirty write already owns its promised empty successor and
	// source slot. Exhaust actual rollover scratch admission instead; the
	// history scalar only triggers rollover, never authorizes allocation.
	stats := c.maintenance.Stats()
	remaining := c.maintenance.Limits().MaxTotalBytes - stats.TotalBytes
	leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if remaining < leaseCharge {
		t.Fatal("fixture has no maintenance admission to exhaust")
	}
	extra, err := c.maintenance.AcquireExternal(remaining - leaseCharge)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	c.writerMu.Lock()
	c.cut.shards[0].history = c.budget.Limits().MaxGenerationBytes
	before := c.cut
	beforeSources := c.budget.Stats().Sources
	c.writerMu.Unlock()
	db.externalCommandWAL = true
	b := db.NewBatchWithSize(1)
	defer b.Close()
	if err := b.Set([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	appended := false
	err = b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { appended = true; return errors.New("unexpected append") })
	if !errors.Is(err, memtable.ErrCOWCapacity) || appended {
		t.Fatalf("refusal=%v appended=%v", err, appended)
	}
	if c.cut != before || c.budget.Stats().Sources != beforeSources {
		t.Fatal("refused rotation changed cut or froze source")
	}
	extra.Close()
	db.externalCommandWAL = false
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("key")); err != nil || string(got) != "new" {
		t.Fatalf("resumed=(%q,%v)", got, err)
	}
}
