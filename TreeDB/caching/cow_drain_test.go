package caching

import (
	"bytes"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"path/filepath"
	"sync/atomic"
	"testing"
	"unsafe"
)

type cowRefusingSemanticHandoff struct {
	*testSemanticState
	refuse atomic.Bool
}

func (s *cowRefusingSemanticHandoff) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
}

func (s *cowRefusingSemanticHandoff) PrepareHandoff(latest backenddb.NativeSemanticState, _ *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	if s.refuse.Load() {
		return nil, backenddb.NativeSemanticHandoffRetryableRefusal{Cause: memtable.ErrCOWCapacity}
	}
	latest.Retain()
	return latest, nil
}

func TestCheckpointFullSourceFrontierJoinsBeforeDirtyAcceptance(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	limits := memtable.DefaultCOWLimits()
	limits.MaxSources, limits.MaxGenerations = 2, 6
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 1, MemtableMode: "cow_btree", COWMemtableLimits: limits})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	}()
	for _, value := range []string{"first", "second"} {
		if err := db.Set([]byte(value), []byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := db.rolloverCOWForFlush(); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.cow.cut.frozen) != limits.MaxSources || db.cow.cut.shards[0].Len() != 0 || db.cow.budget.Stats().Views != 0 {
		t.Fatal("fixture does not fill source frontier with an unpromised empty mutable generation")
	}
	runs := db.checkpointRuns.Load()
	if err := db.Set([]byte("mutable"), []byte("latest")); err != nil {
		t.Fatalf("source pressure failed to drain before dirty acceptance: %v", err)
	}
	if db.checkpointRuns.Load() != runs+1 || db.cow.successors[0].writer == nil || db.COWMemoryStats().Sources > limits.MaxSources {
		t.Fatal("dirty acceptance did not retain its promised successor after one bounded join")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"first": "first", "second": "second", "mutable": "latest"} {
		if got, err := backend.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("covered %s=(%q,%v)", key, got, err)
		}
	}
	if err := db.Set([]byte("after"), []byte("progress")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if stats := db.COWMemoryStats(); stats.Sources != 0 || stats.Generations != 1 || stats.Views != 0 || db.cow.activeCuts.Load() != 1 || db.cow.handoff != nil {
		t.Fatalf("checkpoint did not reclaim frontier: %+v cuts=%d", stats, db.cow.activeCuts.Load())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	stats := db.COWMemoryStats()
	t.Logf("closed producer total=%d external=%d sources=%d generations=%d views=%d", stats.TotalBytes, stats.ExternalBytes, stats.Sources, stats.Generations, stats.Views)
	if stats.TotalBytes != 0 || stats.ExternalBytes != 0 || stats.Sources != 0 || stats.Generations != 0 || stats.Views != 0 {
		t.Fatal("frontier ownership remained after Close")
	}
}

func TestCOWDrainPublishesActivePointersAndTombstones(t *testing.T) {
	db := cowPointerFixture(t)
	oldValue := bytes.Repeat([]byte("old"), 1024)
	for _, key := range []string{"a", "gone"} {
		if err := db.Set([]byte(key), oldValue); err != nil {
			t.Fatal(err)
		}
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	newer := bytes.Repeat([]byte("new"), 1024)
	if err = db.Set([]byte("a"), newer); err != nil {
		t.Fatal(err)
	}
	if err = db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if found, err := db.backend.Has([]byte("a")); err != nil || found {
		t.Fatalf("unexpected pre-drain backend: %v %v", found, err)
	}
	if err = db.Drain(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.backend.Get([]byte("a")); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("Drain omitted current pointer: %q %v", got, err)
	}
	if found, err := db.backend.Has([]byte("gone")); err != nil || found {
		t.Fatalf("Drain omitted tombstone: %v %v", found, err)
	}
	for _, key := range []string{"a", "gone"} {
		if got, err := old.Get([]byte(key)); err != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("old cut %s: %q %v", key, got, err)
		}
	}
	if err = db.Set([]byte("a"), []byte("late")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "late" {
		t.Fatalf("late write: %q %v", got, err)
	}
}

func TestCOWDrainRolloverRefusalPreservesActiveCut(t *testing.T) {
	db := cowPointerFixture(t)
	if err := db.Set([]byte("a"), []byte("active")); err != nil {
		t.Fatal(err)
	}
	before := db.cow.maintenance.Stats()
	cut := db.cow.cut
	parked := db.cow.successors[0]
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.maintenance.AcquireExternal(db.cow.maintenance.Limits().MaxTotalBytes - before.TotalBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Drain()
	pressure.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) || db.cow.cut != cut {
		t.Fatalf("rollover refusal changed cut: %v", err)
	}
	if db.cow.successors[0].writer != parked.writer || db.cow.successors[0].root != parked.root {
		t.Fatal("refusal lost original parked successor")
	}
	if found, err := db.backend.Has([]byte("a")); err != nil || found {
		t.Fatalf("refused Drain published: %v %v", found, err)
	}
	if err = db.Drain(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.backend.Get([]byte("a")); err != nil || string(got) != "active" {
		t.Fatalf("drain retry: %q %v", got, err)
	}
}

func TestCOWDrainReturnsAcceptedHandoffRefusalAndRetriesWithoutReapply(t *testing.T) {
	db := cowPointerFixture(t)
	native := db.backend.(*backenddb.DB)
	if err := db.Set([]byte("a"), []byte("covered")); err != nil {
		t.Fatal(err)
	}
	state := &cowRefusingSemanticHandoff{testSemanticState: newTestSemanticState(nil)}
	db.cow.cut.semanticRootSet = state
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	state.refuse.Store(true)
	if err = db.Drain(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("hidden handoff refusal: %v", err)
	}
	if db.cow.handoff == nil || !db.cow.handoff.accepted || len(db.cow.cut.frozen) == 0 {
		t.Fatal("accepted prefix lost")
	}
	seq := native.State().CommitSeq
	if got, err := native.Get([]byte("a")); err != nil || string(got) != "covered" {
		t.Fatalf("accepted backend: %q %v", got, err)
	}
	state.refuse.Store(false)
	if err = db.Drain(); err != nil {
		t.Fatal(err)
	}
	if native.State().CommitSeq != seq || db.cow.handoff != nil || len(db.cow.cut.frozen) != 0 {
		t.Fatal("retry reapplied accepted prefix")
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "covered" {
		t.Fatalf("old captured pointer: %q %v", got, err)
	}
}
