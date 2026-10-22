package caching

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWInFlightPressureJoinsOnceAndPreservesRefusal(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backendDir := filepath.Join(dir, "backend")
			backend, err := backenddb.Open(backenddb.Options{Dir: backendDir})
			if err != nil {
				t.Fatal(err)
			}
			limits := memtable.DefaultCOWLimits()
			db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true,
				MemtableMode: mode, MemtableShards: 4, COWMemtableLimits: limits,
				FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30,
				ValueLogCompression: uint8(vlogCompressionOff)})
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if !db.closing.Load() {
					_ = db.Close()
				}
			})
			b := db.NewBatchWithSize(64)
			defer b.Close()
			value := bytes.Repeat([]byte("v"), 128)
			groups := make([][]memtable.COWMutation, 4)
			var keys [][]byte
			for candidate := 0; len(keys) < 64 && candidate < 10000; candidate++ {
				key := []byte(fmt.Sprintf("inflight-%04d", candidate))
				i := db.shardIndex(key)
				if len(groups[i]) == 16 {
					continue
				}
				if err := b.Set(key, value); err != nil {
					t.Fatal(err)
				}
				groups[i] = append(groups[i], memtable.COWMutation{Key: key, Value: value, Flags: node.FlagInline})
				keys = append(keys, key)
			}
			if len(keys) != 64 {
				t.Fatal("could not populate all shards")
			}
			// Stage ordinary input and write scratch before applying producer pressure.
			// The limits remain the production defaults; only an explicit live lease
			// consumes the remaining in-flight allowance.
			if err := b.cowPrepareWriteStorage(); err != nil {
				t.Fatal(err)
			}
			var total, largest uint64
			for i, group := range groups {
				charge, err := db.cow.writers[i].Estimate(group, memtable.COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				total += charge.Total()
				largest = max(largest, charge.Total())
			}
			headroom := largest + (total-largest)/2
			if largest >= headroom || headroom >= total {
				t.Fatalf("invalid aggregate fixture: largest=%d headroom=%d total=%d", largest, headroom, total)
			}
			stats := db.COWMemoryStats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			if stats.ReservedBytes+overhead+headroom >= limits.MaxInFlightBytes {
				t.Fatal("fixture lacks producer headroom")
			}
			filler, err := db.cow.budget.AcquireExternal(limits.MaxInFlightBytes - stats.ReservedBytes - overhead - headroom)
			if err != nil {
				t.Fatal(err)
			}
			defer filler.Close()
			stats = db.COWMemoryStats()
			if stats.ReservedBytes+headroom != limits.MaxInFlightBytes ||
				stats.TotalBytes+total >= limits.MaxTotalBytes ||
				stats.HistoryBytes+stats.ReservedBytes+stats.DeferredBytes+total >= limits.MaxRetiredBytes {
				t.Fatalf("fixture does not isolate in-flight pressure: stats=%+v total=%d", stats, total)
			}
			db.cow.writerMu.Lock()
			loose := b.cowPredictionCanSkipEstimate(groups)
			rollover, pressure, _, predictErr := b.cowGenerationRollover(true)
			db.cow.writerMu.Unlock()
			if loose || rollover != nil || predictErr != nil || !pressure {
				t.Fatalf("in-flight-only aggregate pressure missed: loose=%t rollover=%v pressure=%t err=%v stats=%+v total=%d", loose, rollover != nil, pressure, predictErr, stats, total)
			}
			beforeSeq, beforeLSN := db.cow.cut.logicalSeq, backend.CommandWALNextLSN()
			beforeCheckpoints := db.checkpointRuns.Load()
			// A live external owner cannot be reclaimed by checkpoint. The existing
			// bounded join must still refuse, without assigning or installing the batch.
			err = b.Write()
			if !errors.Is(err, memtable.ErrCOWCapacity) || b.COWReceipt().Assigned || b.COWReceipt().LogicalInstalled ||
				db.cow.cut.logicalSeq != beforeSeq || backend.CommandWALNextLSN() != beforeLSN {
				t.Fatalf("nonreclaimable pressure changed acceptance: receipt=%+v seq=%d lsn=%d err=%v", b.COWReceipt(), db.cow.cut.logicalSeq, backend.CommandWALNextLSN(), err)
			}
			if got := db.checkpointRuns.Load(); got != beforeCheckpoints+1 {
				t.Fatalf("checkpoint joins=%d want 1", got-beforeCheckpoints)
			}
			filler.Close()
			for _, key := range keys {
				if got, err := db.Get(key); err != nil || got != nil {
					t.Fatalf("refused key %q visible: %q %v", key, got, err)
				}
			}
			// Only the caller resubmits this still-unassigned command after releasing
			// its own owner; the pressure loop never replays accepted work.
			if err := b.Write(); err != nil {
				t.Fatal(err)
			}
			if db.cow.cut.logicalSeq != beforeSeq+1 || db.cow.budget.Limits() != limits {
				t.Fatal("resubmission changed limits or published more than once")
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				if got, err := db.Get(key); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("current %q: %q %v", key, got, err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := db.COWMemoryStats(); stats.TotalBytes != 0 {
				t.Fatalf("parent not drained: %+v", stats)
			}
			reopened, err := backenddb.Open(backenddb.Options{Dir: backendDir})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, key := range keys {
				if got, err := reopened.Get(key); err != nil || !bytes.Equal(got, value) {
					t.Fatalf("reopened %q: %q %v", key, got, err)
				}
			}
		})
	}
}
