package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/kvstore"
)

func TestQuicksilverChurn(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.ChurnRounds, c.ChurnPause = 2, time.Millisecond
	dir := t.TempDir()
	opens := 0
	open := func(dir string) (kvstore.DB, error) { opens++; return NewTreeDBPublicCommandWAL(dir) }
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", open, dir)
	if err != nil {
		t.Fatal(err)
	}
	if opens != 3 || len(r.Churn.Rounds) != 2 || len(r.Phases) != 4 {
		t.Fatalf("owner/rounds/phases: %d %+v", opens, r.Churn)
	}
	for i, round := range r.Churn.Rounds {
		if round.Round != i+1 || round.RestoredKeys != c.Keys || round.MutationTargets != c.Updates || round.VerifiedKeys != r.VerifiedKeys || round.VerifiedMisses != r.VerifiedMisses || round.PauseSeconds < c.ChurnPause.Seconds() || round.Seconds < round.PauseSeconds {
			t.Fatalf("round: %+v", round)
		}
		wallPause := float64(round.PauseFinishedUnixNano-round.PauseStartedUnixNano) / 1e9
		if round.Before.CapturedAtUnixNano > round.PauseStartedUnixNano || round.PauseStartedUnixNano <= 0 || round.PauseFinishedUnixNano < round.PauseStartedUnixNano || round.PauseFinishedUnixNano > round.After.CapturedAtUnixNano || math.Abs(wallPause-round.PauseSeconds) > 0.001 {
			t.Fatalf("invalid pause boundaries: start=%d end=%d duration=%g", round.PauseStartedUnixNano, round.PauseFinishedUnixNano, round.PauseSeconds)
		}
		if !reflect.DeepEqual(round.Mutations, r.Mutations) || round.MutationCommitBatches != r.MutationCommitBatches || len(round.Before.Stats) == 0 || len(round.After.Stats) == 0 || round.After.HeapAlloc == 0 {
			t.Fatalf("round accounting/stats: %+v", round)
		}
	}
	files, err := quicksilverFiles(dir)
	if err != nil || !reflect.DeepEqual(files, r.Churn.FinalFiles) {
		t.Fatalf("clean-close census: %v %v", files, err)
	}
	db, err := NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	keys, misses, err := quicksilverVerify(db, c, r.UpdateStride, nil)
	closeErr := db.Close()
	if err != nil || closeErr != nil || keys != r.VerifiedKeys || misses != r.VerifiedMisses {
		t.Fatalf("final reopen oracle: %d %d %v %v", keys, misses, err, closeErr)
	}
}

func TestQuicksilverChurnRejectsBeforeOpen(t *testing.T) {
	for _, modify := range []func(*quicksilverConfig){
		func(c *quicksilverConfig) { c.ChurnRounds = -1 },
		func(c *quicksilverConfig) { c.ChurnRounds = 33 },
		func(c *quicksilverConfig) { c.ChurnPause = -time.Second },
		func(c *quicksilverConfig) { c.ChurnPause = 0 },
		func(c *quicksilverConfig) { c.ChurnPause = 61 * time.Second },
		func(c *quicksilverConfig) { c.ChurnRounds = 0 },
		func(c *quicksilverConfig) { c.Case = "random4k" },
	} {
		c := quicksilverRealisticSmokeConfig()
		c.ChurnRounds, c.ChurnPause = 2, time.Millisecond
		modify(&c)
		_, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", func(string) (kvstore.DB, error) { t.Fatal("invalid config opened DB"); return nil, nil }, t.TempDir())
		if err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	c := quicksilverRealisticSmokeConfig()
	c.ChurnRounds, c.ChurnPause = 2, time.Millisecond
	for _, engine := range []string{"lmdb", "rocksdb", "treedb_backend", "treedb_command_wal", "hashdb"} {
		_, err := runQuicksilverEngine(BenchConfig{}, c, engine, func(string) (kvstore.DB, error) { t.Fatal("unsupported engine opened DB"); return nil, nil }, t.TempDir())
		if err == nil {
			t.Fatalf("accepted %s", engine)
		}
	}
}

func TestQuicksilverChurnGuardClosesOwner(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.ChurnRounds, c.ChurnPause = 2, 6*time.Second
	c.Keys, c.Reads, c.Workers, c.Updates, c.Duration = 8, 8, 1, 8, time.Millisecond
	dir := t.TempDir()
	r, err := runQuicksilverEngine(BenchConfig{MaxWall: 5 * time.Second}, c, "treedb", NewTreeDBPublicCommandWAL, dir)
	if err == nil || r.Churn == nil {
		t.Fatalf("expected guard during characterization: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal("failed DB removed", err)
	}
	db, err := NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatal("failed owner still holds database", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQuicksilverChurnDefaultOmitted(t *testing.T) {
	raw, err := json.Marshal(quicksilverResult{Config: quicksilverRealisticSmokeConfig()})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	json.Unmarshal(raw, &out)
	if _, ok := out["maintenance_churn"]; ok {
		t.Fatal("default adds characterization")
	}
	config := out["config"].(map[string]any)
	if _, ok := config["churn_rounds"]; ok {
		t.Fatal("default adds rounds")
	}
	if _, ok := config["churn_pause_ns"]; ok {
		t.Fatal("default adds pause")
	}
}

func TestQuicksilverChurnConfigDefaults(t *testing.T) {
	oldRounds, oldPause := *quicksilverChurnRounds, *quicksilverChurnPause
	defer func() { *quicksilverChurnRounds, *quicksilverChurnPause = oldRounds, oldPause }()
	*quicksilverChurnRounds, *quicksilverChurnPause = 2, 6*time.Second
	c, err := resolveQuicksilverConfig(BenchConfig{SeedUsed: 24}, map[string]bool{})
	if err != nil || c.ChurnRounds != 2 || c.ChurnPause != 6*time.Second {
		t.Fatalf("default enabled pause: %+v %v", c, err)
	}
	*quicksilverChurnRounds = 0
	c, err = resolveQuicksilverConfig(BenchConfig{SeedUsed: 24}, map[string]bool{})
	if err != nil || c.ChurnRounds != 0 || c.ChurnPause != 0 {
		t.Fatalf("default disabled: %+v %v", c, err)
	}
	if _, err := resolveQuicksilverConfig(BenchConfig{SeedUsed: 24}, map[string]bool{"quicksilver-churn-pause": true}); err == nil {
		t.Fatal("accepted inactive pause")
	}
	c = quicksilverRealisticSmokeConfig()
	c.ChurnRounds, c.ChurnPause = 2, time.Millisecond
	dir := t.TempDir()
	if _, err := runQuicksilverSuite(BenchConfig{DBsArg: "treedb", QuicksilverVerifyDir: dir}, c, ""); err == nil {
		t.Fatal("retained verification accepted churn")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("invalid retained request opened fixture: %v %v", files, err)
	}
}

// Both tests compile on the original harness: unknown JSON fields are ignored,
// so a failure records the missing behavior rather than a missing symbol.
func TestQuicksilverSparseChurnQualification(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys, c.Updates = 101, 13
	c.ChurnRounds, c.ChurnPause = 2, time.Millisecond
	if err := json.Unmarshal([]byte(`{"churn_shape":"sparse"}`), &c); err != nil {
		t.Fatal(err)
	}
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, round := range r.Churn.Rounds {
		if round.RestoredKeys != 10 || round.Mutations.Inserts != 3 || round.Mutations.Deletes != 3 || round.VerifiedKeys != 101 || round.VerifiedMisses != 206 {
			t.Fatalf("sparse restored=%d mutations=%+v proof=%d/%d", round.RestoredKeys, round.Mutations, round.VerifiedKeys, round.VerifiedMisses)
		}
	}
}

func TestQuicksilverRetainedFinalQualification(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys, c.Updates = 101, 13
	dir := t.TempDir()
	initial, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"final_fixture":true}`), &c); err != nil {
		t.Fatal(err)
	}
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.LoadSeconds != 0 || r.DeletedPreparationSeconds != 0 || r.InitialCheckpointMS != 0 || r.InitialVerifiedMisses != initial.VerifiedMisses || r.InitialVerifiedKeys != initial.VerifiedKeys {
		t.Fatalf("retained load=%g preparation=%g checkpoint=%g proof=%d/%d", r.LoadSeconds, r.DeletedPreparationSeconds, r.InitialCheckpointMS, r.InitialVerifiedKeys, r.InitialVerifiedMisses)
	}
	if r.VerifiedKeys != initial.VerifiedKeys || r.VerifiedMisses != initial.VerifiedMisses || r.UpdatedKeys != c.Updates {
		t.Fatalf("final proof=%d/%d updates=%d", r.VerifiedKeys, r.VerifiedMisses, r.UpdatedKeys)
	}
	for _, phase := range r.Phases {
		if phase.ObservedHits != phase.RequestedPresent || phase.RequestedPresent+phase.RequestedAbsent != phase.Ops || phase.DistinctPresentRequests+phase.DistinctAbsentRequests != phase.DistinctAccesses {
			t.Fatalf("final-state reads/accounting: %+v", phase)
		}
	}
}

func TestQuicksilverRetainedFinalRejectsBeforeOpen(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.FinalFixture = true
	for _, engine := range []string{"treedb", "lmdb", "rocksdb", "hashdb"} {
		_, err := runQuicksilverEngine(BenchConfig{}, c, engine, func(string) (kvstore.DB, error) { t.Fatal("opened invalid fixture"); return nil, nil }, t.TempDir())
		if err == nil {
			t.Fatalf("accepted missing %s fixture", engine)
		}
	}
	for _, modify := range []func(*quicksilverConfig){
		func(c *quicksilverConfig) { c.Case = "structured256" },
		func(c *quicksilverConfig) { c.ChurnRounds = 1; c.ChurnPause = time.Millisecond },
		func(c *quicksilverConfig) { c.ChurnShape = "sparse" },
	} {
		bad := c
		modify(&bad)
		_, err := runQuicksilverEngine(BenchConfig{}, bad, "treedb", func(string) (kvstore.DB, error) { t.Fatal("opened invalid configuration"); return nil, nil }, t.TempDir())
		if err == nil {
			t.Fatalf("accepted invalid retained config")
		}
	}
	for _, cfg := range []BenchConfig{
		{DBsArg: "treedb,lmdb", QuicksilverMeasureDir: t.TempDir()},
		{DBsArg: "treedb", QuicksilverMeasureDir: t.TempDir(), QuicksilverVerifyDir: t.TempDir()},
		{DBsArg: "treedb"},
	} {
		if _, err := runQuicksilverSuite(cfg, c, ""); err == nil {
			t.Fatal("accepted invalid retained selection")
		}
	}
}

func TestQuicksilverRetainedFinalWrongFixtureNoWriter(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys, c.Updates = 101, 13
	dir := t.TempDir()
	if _, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir); err != nil {
		t.Fatal(err)
	}
	db, err := NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("extra-fixture-key"), []byte("extra")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	c.FinalFixture = true
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir)
	if err == nil || r.UpdatedKeys != 0 || len(r.Phases) != 0 {
		t.Fatalf("wrong fixture writer=%d phases=%d err=%v", r.UpdatedKeys, len(r.Phases), err)
	}
	// Failure releases ownership and leaves the directory for diagnosis.
	db, err = NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	bad := c
	bad.Seed++
	r, err = runQuicksilverEngine(BenchConfig{}, bad, "treedb", NewTreeDBPublicCommandWAL, dir)
	if err == nil || r.UpdatedKeys != 0 {
		t.Fatalf("wrong seed writer=%d err=%v", r.UpdatedKeys, err)
	}
}

func TestQuicksilverSparseRestoreWritesOnlyTargets(t *testing.T) {
	for _, mode := range []string{"ordinary", "sync"} {
		c := quicksilverRealisticSmokeConfig()
		c.Keys, c.Updates, c.CommitMode = 101, 13, mode
		db := &quicksilverCommitProbeDB{batchDeleteRangeMemoryDB: newBatchDeleteRangeMemoryDB("memory")}
		n, batches, err := quicksilverRestoreSparse(db, c, quicksilverUpdateStride(c.Keys), nil)
		if err != nil || n != 10 || batches != 1 {
			t.Fatalf("restore=%d batches=%d err=%v", n, batches, err)
		}
		for j := 0; j < c.Keys; j++ {
			id := uint64(int64(j)*int64(quicksilverUpdateStride(c.Keys))%int64(c.Keys)) * 2
			v, err := db.Get(quicksilverGenericKey(nil, id, c.Seed, c.Mixture))
			if err != nil {
				t.Fatal(err)
			}
			want := j < c.Updates && j%4 != 2
			if (v != nil) != want {
				t.Fatalf("restore wrote incorrect target %d present=%t", j, v != nil)
			}
		}
		if mode == "ordinary" && (db.ordinary != 1 || db.sync != 0) || mode == "sync" && (db.sync != 1 || db.ordinary != 0) {
			t.Fatalf("ACK dispatch %d/%d", db.ordinary, db.sync)
		}
	}
}

func TestQuicksilverRetainedReaderFailureJoins(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.FinalFixture = true
	d := &quicksilverFailSnapshotDB{entered: make(chan struct{})}
	joined := make(chan struct{})
	_, err := quicksilverReadPhase(d, c, newQuicksilverFixture(c), 3, nil, func(ctx context.Context) error { defer close(joined); <-ctx.Done(); return nil }, nil)
	if err == nil || d.active.Load() != 0 {
		t.Fatalf("retained failure=%v active=%d", err, d.active.Load())
	}
	select {
	case <-joined:
	default:
		t.Fatal("retained writer did not join")
	}
}

type quicksilverFailRetainedWriterDB struct{ kvstore.DB }

func (d quicksilverFailRetainedWriterDB) NewBatch() (kvstore.Batch, error) {
	return nil, errors.New("injected retained mutation failure")
}
func (d quicksilverFailRetainedWriterDB) Checkpoint() error { return d.DB.(checkpointer).Checkpoint() }
func (d quicksilverFailRetainedWriterDB) Iterator(a, b []byte) (kvstore.Iterator, error) {
	return d.DB.(kvstore.RangeScanner).Iterator(a, b)
}
func (d quicksilverFailRetainedWriterDB) ReverseIterator(a, b []byte) (kvstore.Iterator, error) {
	return d.DB.(kvstore.RangeScanner).ReverseIterator(a, b)
}

func TestQuicksilverRetainedIncompleteWriterReleasesOwner(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys, c.Updates, c.ReadBatch = 17, 13, 1
	dir := t.TempDir()
	if _, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", NewTreeDBPublicCommandWAL, dir); err != nil {
		t.Fatal(err)
	}
	c.FinalFixture = true
	open := func(dir string) (kvstore.DB, error) {
		db, err := NewTreeDBPublicCommandWAL(dir)
		if err != nil {
			return nil, err
		}
		return quicksilverFailRetainedWriterDB{db}, nil
	}
	r, err := runQuicksilverEngine(BenchConfig{}, c, "treedb", open, dir)
	if err == nil || r.UpdatedKeys != 0 || r.InitialVerifiedKeys == 0 {
		t.Fatalf("incomplete writer updates=%d preproof=%d err=%v", r.UpdatedKeys, r.InitialVerifiedKeys, err)
	}
	db, err := NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatal("failed retained owner leaked", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQuicksilverRetainedActualDistinctMisses(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.Keys, c.Updates, c.Reads, c.Workers, c.ReadBatch = 101, 13, 10000, 1, 1
	c.FinalFixture = true
	d := &quicksilverObserveMissDB{seen: map[string]struct{}{}}
	p, err := quicksilverReadPhase(d, c, newQuicksilverFixture(c), 1, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.DistinctAccesses != len(d.seen) || p.DistinctPresentRequests != 0 || p.DistinctAbsentRequests != len(d.seen) {
		t.Fatalf("retained distinct total=%d present=%d absent=%d actual=%d", p.DistinctAccesses, p.DistinctPresentRequests, p.DistinctAbsentRequests, len(d.seen))
	}
	stride := quicksilverUpdateStride(c.Keys)
	for j := 1; j < c.Updates; j += 4 {
		id := uint64(int64(j)*int64(stride)%int64(c.Keys)) * 2
		if _, ok := d.seen[string(quicksilverGenericKey(nil, id, c.Seed, c.Mixture))]; !ok {
			t.Fatalf("retained deleted miss missing target %d", j)
		}
	}
}

func TestQuicksilverRetainedConcurrentGeneration(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	c.FinalFixture = true
	for _, state := range []uint8{0, 1, 4, 5} {
		for gen := uint64(0); gen <= 5; gen++ {
			value := make([]byte, quicksilverRealisticSize(&c, 0))
			quicksilverRealisticValue(value, c, 0, gen)
			err := quicksilverCheckRead(&c, 0, value, false, true, state)
			want := state == 0 && gen == 0 || (state == 1 || state == 5) && gen == 1 || state == 4 && gen >= 1 && gen <= 4
			if (err == nil) != want {
				t.Fatalf("retained state=%d gen=%d err=%v", state, gen, err)
			}
		}
		if err := quicksilverCheckRead(&c, 0, nil, false, true, state); err == nil {
			t.Fatal("retained hit accepted missing value")
		}
	}
}
