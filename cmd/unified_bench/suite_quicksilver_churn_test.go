package main

import (
	"encoding/json"
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
