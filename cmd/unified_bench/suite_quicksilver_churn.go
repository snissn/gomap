package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/snissn/gomap/kvstore"
)

// Engine mappings are separate from process Go heap and resident bytes: virtual
// mapped file bytes do not imply residency. Stats retain the engine counters.
type quicksilverChurnSnapshot struct {
	HeapAlloc    uint64            `json:"process_heap_alloc_bytes"`
	RSSBytes     uint64            `json:"process_rss_bytes"`
	RSSSupported bool              `json:"process_rss_supported"`
	Stats        map[string]string `json:"engine_stats"`
}

type quicksilverChurnRound struct {
	Round                 int                      `json:"round"`
	RestoredKeys          int                      `json:"restored_keys"`
	MutationTargets       int                      `json:"mutation_targets"`
	MutationCommitBatches int                      `json:"mutation_commit_batches"`
	Mutations             quicksilverMutations     `json:"mutations"`
	Seconds               float64                  `json:"wall_seconds"`
	WriteSeconds          float64                  `json:"write_seconds"`
	CheckpointSeconds     float64                  `json:"checkpoint_seconds"`
	VerificationSeconds   float64                  `json:"verification_seconds"`
	PauseSeconds          float64                  `json:"pause_seconds"`
	VerifiedKeys          int                      `json:"verified_keys"`
	VerifiedMisses        int                      `json:"verified_misses"`
	Before                quicksilverChurnSnapshot `json:"before"`
	After                 quicksilverChurnSnapshot `json:"after"`
}

type quicksilverChurnResult struct {
	Label                            string                  `json:"label"`
	LeafGenerationPackMaintenanceEnv string                  `json:"leaf_generation_pack_maintenance_env"`
	Rounds                           []quicksilverChurnRound `json:"rounds"`
	FinalFiles                       map[string]int64        `json:"final_files_after_close"`
}

func quicksilverValidateChurnEngine(c quicksilverConfig, engine string) error {
	if c.ChurnRounds == 0 {
		return nil
	}
	switch engine {
	case "treedb", "treedbcached", "treedb_public_command_wal", "treedb_cached_command_wal":
		return nil
	}
	return fmt.Errorf("quicksilver: churn requires a cached public TreeDB engine; got %s", engine)
}

func quicksilverChurnSnapshotOf(db kvstore.DB) (quicksilverChurnSnapshot, error) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	rss, supported, err := currentRSSBytes()
	return quicksilverChurnSnapshot{HeapAlloc: mem.HeapAlloc, RSSBytes: rss, RSSSupported: supported, Stats: quicksilverStats(db)}, err
}

func quicksilverChurn(db kvstore.DB, c quicksilverConfig, res *quicksilverResult, guard *benchGuard) error {
	res.Churn = &quicksilverChurnResult{Label: "bounded write/automatic-maintenance characterization; not warmed read throughput or a steady-state bound", LeafGenerationPackMaintenanceEnv: os.Getenv("TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE")}
	for round := 1; round <= c.ChurnRounds; round++ {
		start := time.Now()
		r := quicksilverChurnRound{Round: round, RestoredKeys: c.Keys, MutationTargets: c.Updates}
		var err error
		if r.Before, err = quicksilverChurnSnapshotOf(db); err != nil {
			return err
		}
		writeStart := time.Now()
		// Restore the original population before repeating deletes and overwrite
		// bursts. Insert identities already present are set again by the same writer.
		for off := 0; off < c.Keys; off += 1000 {
			if err := guard.Checkpoint(); err != nil {
				return err
			}
			if err := quicksilverRealisticWrite(db, c, off, min(1000, c.Keys-off), res.UpdateStride, false); err != nil {
				return err
			}
		}
		if err := quicksilverPrepareDeleted(db, c, guard); err != nil {
			return err
		}
		for off := 0; off < c.Updates; off += 1000 {
			if err := guard.Checkpoint(); err != nil {
				return err
			}
			count := min(1000, c.Updates-off)
			if err := quicksilverRealisticWrite(db, c, off, count, res.UpdateStride, true); err != nil {
				return err
			}
			r.Mutations.add(off, count)
			r.MutationCommitBatches++
			if count >= 4 {
				r.MutationCommitBatches += 3
			}
		}
		r.WriteSeconds = time.Since(writeStart).Seconds()
		checkpointStart := time.Now()
		if err := db.(checkpointer).Checkpoint(); err != nil {
			return err
		}
		r.CheckpointSeconds = time.Since(checkpointStart).Seconds()
		pauseStart := time.Now()
		// Poll the existing wall/RSS guard during the one recorded maintenance pause.
		for remaining := c.ChurnPause; remaining > 0; remaining = c.ChurnPause - time.Since(pauseStart) {
			if err := guard.Checkpoint(); err != nil {
				return err
			}
			time.Sleep(min(remaining, 50*time.Millisecond))
		}
		r.PauseSeconds = time.Since(pauseStart).Seconds()
		proofStart := time.Now()
		r.VerifiedKeys, r.VerifiedMisses, err = quicksilverVerify(db, c, res.UpdateStride, guard)
		if err != nil {
			return err
		}
		r.VerificationSeconds = time.Since(proofStart).Seconds()
		if r.After, err = quicksilverChurnSnapshotOf(db); err != nil {
			return err
		}
		r.Seconds = time.Since(start).Seconds()
		res.Churn.Rounds = append(res.Churn.Rounds, r)
	}
	return nil
}
