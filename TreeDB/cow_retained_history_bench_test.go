package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// Construction-only until the retained-history protocol and this exact harness
// have been reviewed. Use fixed -benchtime=128x in fresh paired processes;
// setup, held-version checks, checkpoint and final Close are outside the timer.
// Pointer inputs are deterministic high-entropy bytes: the compression policy
// is unchanged, and the fixture does not multiply compressed decoder envelopes
// merely to retain many distinct logical versions.
func BenchmarkCOWPublicRetainedHistory(b *testing.B) {
	for _, profile := range []Profile{ProfileNoWALFast, ProfileCommandWALRelaxed} {
		for _, pointer := range []bool{false, true} {
			for _, retained := range []int{0, 1, 16, 128} {
				for _, operation := range []string{"CaptureReadRelease", "WriteACK"} {
					b.Run(fmt.Sprintf("%s/Pointer=%t/H=%d/%s", profile, pointer, retained, operation), func(b *testing.B) {
						b.StopTimer()
						if b.N > 128 {
							b.Fatal("bounded harness requires fixed -benchtime=128x or smaller")
						}
						f := newCOWRetainedFixture(b, profile, pointer, retained)
						defer f.close()
						latencies := make([]int64, b.N)
						writeValues := make([][]byte, b.N)
						for i := range writeValues {
							writeValues[i] = cowRetainedValue(len(f.current), uint64(retained+i+1))
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := range latencies {
							b.StartTimer()
							start := time.Now()
							if operation == "WriteACK" {
								if err := f.database.Set(f.key, writeValues[i]); err != nil {
									b.Fatal(err)
								}
							} else {
								snapshot := f.database.AcquireSnapshot()
								if snapshot == nil {
									b.Fatal("current capture unavailable")
								}
								got, err := snapshot.Get(f.key)
								if err != nil || !bytes.Equal(got, f.current) {
									b.Fatalf("current read changed: bytes=%d err=%v", len(got), err)
								}
								if err := snapshot.Close(); err != nil {
									b.Fatal(err)
								}
							}
							latencies[i] = time.Since(start).Nanoseconds()
							b.StopTimer()
						}
						if operation == "WriteACK" {
							f.current = writeValues[len(writeValues)-1]
						}
						f.verifyHeld()
						f.phase("after_operations")
						slices.Sort(latencies)
						for _, p := range []int{50, 95, 99} {
							b.ReportMetric(float64(latencies[(len(latencies)*p+99)/100-1]), fmt.Sprintf("p%d-ns", p))
						}
					})
				}
			}
		}
	}
}

// This test validates ownership and values; its wall time is not performance
// qualification. The benchmark additionally measures the independent ACK path.
func TestCOWPublicRetainedHistoryFixture(t *testing.T) {
	for _, profile := range []Profile{ProfileNoWALFast, ProfileCommandWALRelaxed} {
		for _, pointer := range []bool{false, true} {
			for _, retained := range []int{0, 1, 16, 128} {
				t.Run(fmt.Sprintf("%s/Pointer=%t/H=%d", profile, pointer, retained), func(t *testing.T) {
					f := newCOWRetainedFixture(t, profile, pointer, retained)
					defer f.close()
					for range 128 {
						if err := f.database.Set(f.key, f.nextValue); err != nil {
							t.Fatal(err)
						}
					}
					f.current = f.nextValue
					f.verifyHeld()
					f.phase("after_operations")
				})
			}
		}
	}
}

type cowRetainedFixture struct {
	tb        testing.TB
	database  *DB
	stats     func() memtable.COWStats
	key       []byte
	current   []byte
	nextValue []byte
	held      []Snapshot
	expected  [][]byte
	pointer   bool
}

func newCOWRetainedFixture(tb testing.TB, profile Profile, pointer bool, retained int) *cowRetainedFixture {
	tb.Helper()
	opts := OptionsFor(profile, tb.TempDir())
	opts.MemtableMode, opts.MemtableShards = "cow_btree", 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	opts.BackgroundIndexVacuumInterval, opts.MaxWALBytes = -1, -1
	opts.DisableBackgroundPrune = true
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxTotalBytes, opts.COWMemtableLimits.MaxRetiredBytes = 1<<30, 1<<30
	opts.COWMemtableLimits.MaxGenerationBytes = 256 << 20
	opts.ValueLog.ForcePointers, opts.ValueLog.PointerThreshold = pointer, 1<<20
	valueSize := 64
	if pointer {
		opts.ValueLog.PointerThreshold, valueSize = 1, 4096
	}
	database, err := Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	cache := database.cached
	f := &cowRetainedFixture{tb: tb, database: database, stats: cache.COWMemoryStats, key: []byte("retained/00000000"), pointer: pointer}
	tb.Cleanup(func() {
		if f.database != nil {
			f.close()
		}
	})
	if database.ResolvedProfile() != profile || database.Stats()["treedb.cache.memtable_mode"] != "cow_btree" {
		tb.Fatal("fixture did not resolve the declared profile and COW mode")
	}
	f.current, f.nextValue = cowRetainedValue(valueSize, 0), cowRetainedValue(valueSize, uint64(retained+1))
	batch := database.NewBatchWithSize(1024)
	for i := range 1024 {
		if err := batch.Set([]byte(fmt.Sprintf("retained/%08d", i)), f.current); err != nil {
			_ = batch.Close()
			tb.Fatal(err)
		}
	}
	writeErr, closeErr := batch.Write(), batch.Close()
	if writeErr != nil || closeErr != nil {
		tb.Fatalf("fixture seed write=%v close=%v", writeErr, closeErr)
	}
	f.phase("before_retention")
	for version := 1; version <= retained; version++ {
		value := cowRetainedValue(valueSize, uint64(version))
		if err := database.Set(f.key, value); err != nil {
			tb.Fatal(err)
		}
		snapshot := database.AcquireSnapshot()
		if snapshot == nil {
			tb.Fatal("distinct retained version unavailable")
		}
		f.held, f.expected = append(f.held, snapshot), append(f.expected, value)
		f.check(snapshot, value)
		f.current = value
	}
	f.phase("retention_plateau")
	return f
}

func cowRetainedValue(size int, version uint64) []byte {
	value := make([]byte, size)
	state := uint64(0x937286514abcdef) ^ (version * 0x9e3779b97f4a7c15)
	for i := 0; i < size; i += 8 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		binary.LittleEndian.PutUint64(value[i:], state)
	}
	return value
}

func (f *cowRetainedFixture) check(snapshot Snapshot, expected []byte) {
	f.tb.Helper()
	got, err := snapshot.Get(f.key)
	if err != nil || !bytes.Equal(got, expected) {
		f.tb.Errorf("retained value changed: bytes=%d err=%v", len(got), err)
	}
	entry, err := snapshot.GetEntry(f.key)
	if err != nil || (entry.Flags&node.FlagPointer != 0) != f.pointer {
		f.tb.Errorf("declared pointer layout=%t, entry=%+v err=%v", f.pointer, entry, err)
	}
}

func (f *cowRetainedFixture) verifyHeld() {
	for i, snapshot := range f.held {
		f.check(snapshot, f.expected[i])
	}
}

func (f *cowRetainedFixture) phase(name string) {
	f.tb.Logf("COW_RETAINED_PHASE phase=%s held=%d memory=%+v", name, len(f.held), f.stats())
}

func (f *cowRetainedFixture) close() {
	if f.database == nil {
		return
	}
	f.verifyHeld()
	if err := f.database.Checkpoint(); err != nil {
		f.tb.Error(err)
	}
	f.phase("after_held_checkpoint")
	f.verifyHeld()
	for i, snapshot := range f.held {
		if err := snapshot.Close(); err != nil {
			f.tb.Error(err)
		}
		f.held[i], f.expected[i] = nil, nil
	}
	f.held, f.expected = nil, nil
	f.phase("after_held_release")
	if err := f.database.Checkpoint(); err != nil {
		f.tb.Error(err)
	}
	f.phase("after_checkpoint")
	current := f.database.AcquireSnapshot()
	if current == nil {
		f.tb.Error("final current capture unavailable")
	} else {
		f.check(current, f.current)
		if err := current.Close(); err != nil {
			f.tb.Error(err)
		}
	}
	if err := f.database.Close(); err != nil {
		f.tb.Error(err)
	}
	f.database = nil
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := f.stats()
		if s.TotalBytes == 0 && s.Views == 0 && s.ExternalLeases == 0 {
			break
		}
		if time.Now().After(deadline) {
			f.tb.Errorf("final original owners did not drain: %+v", s)
			break
		}
		time.Sleep(time.Millisecond)
	}
	f.phase("final_close")
}
