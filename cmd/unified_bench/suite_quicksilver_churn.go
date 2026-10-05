package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/snissn/gomap/kvstore"
)

// Engine mappings are separate from process Go heap and resident bytes: virtual
// mapped file bytes do not imply residency. Stats retain the engine counters.
type quicksilverChurnSnapshot struct {
	CapturedAtUnixNano int64             `json:"captured_at_unix_nano"`
	HeapAlloc          uint64            `json:"process_heap_alloc_bytes"`
	RSSBytes           uint64            `json:"process_rss_bytes"`
	RSSSupported       bool              `json:"process_rss_supported"`
	Stats              map[string]string `json:"engine_stats"`
}

type quicksilverChurnRound struct {
	Round                 int                      `json:"round"`
	RestoreCommitBatches  int                      `json:"restore_commit_batches"`
	RestoredKeys          int                      `json:"restored_keys"`
	MutationTargets       int                      `json:"mutation_targets"`
	MutationCommitBatches int                      `json:"mutation_commit_batches"`
	Mutations             quicksilverMutations     `json:"mutations"`
	Seconds               float64                  `json:"wall_seconds"`
	WriteSeconds          float64                  `json:"write_seconds"`
	CheckpointSeconds     float64                  `json:"checkpoint_seconds"`
	VerificationSeconds   float64                  `json:"verification_seconds"`
	PauseSeconds          float64                  `json:"pause_seconds"`
	PauseStartedUnixNano  int64                    `json:"pause_started_unix_nano"`
	PauseFinishedUnixNano int64                    `json:"pause_finished_unix_nano"`
	VerifiedKeys          int                      `json:"verified_keys"`
	VerifiedMisses        int                      `json:"verified_misses"`
	Before                quicksilverChurnSnapshot `json:"before"`
	After                 quicksilverChurnSnapshot `json:"after"`
}

type quicksilverChurnResult struct {
	Shape                            string                  `json:"shape"`
	WriterSemantics                  string                  `json:"writer_semantics"`
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
	return quicksilverChurnSnapshot{CapturedAtUnixNano: time.Now().UnixNano(), HeapAlloc: mem.HeapAlloc, RSSBytes: rss, RSSSupported: supported, Stats: quicksilverStats(db)}, err
}

func quicksilverChurn(db kvstore.DB, c quicksilverConfig, res *quicksilverResult, guard *benchGuard) error {
	res.Churn = &quicksilverChurnResult{Shape: c.ChurnShape, WriterSemantics: "insert identities already exist and are SET again; original targets restored before mutations", Label: "bounded write/automatic-maintenance characterization; not warmed read throughput or a steady-state bound", LeafGenerationPackMaintenanceEnv: os.Getenv("TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE")}
	for round := 1; round <= c.ChurnRounds; round++ {
		start := time.Now()
		r := quicksilverChurnRound{Round: round, RestoredKeys: c.Keys, MutationTargets: c.Updates}
		var err error
		if r.Before, err = quicksilverChurnSnapshotOf(db); err != nil {
			return err
		}
		writeStart := time.Now()
		if c.ChurnShape == "sparse" {
			r.RestoredKeys, r.RestoreCommitBatches, err = quicksilverRestoreSparse(db, c, res.UpdateStride, guard)
			if err != nil {
				return err
			}
		} else {
			// Preserve the original full-population stress behavior.
			for off := 0; off < c.Keys; off += 1000 {
				if err := guard.Checkpoint(); err != nil {
					return err
				}
				if err := quicksilverRealisticWrite(db, c, off, min(1000, c.Keys-off), res.UpdateStride, false); err != nil {
					return err
				}
				r.RestoreCommitBatches++
			}
			if err := quicksilverPrepareDeleted(db, c, guard); err != nil {
				return err
			}
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
		r.PauseStartedUnixNano = pauseStart.UnixNano()
		// Poll the existing wall/RSS guard during the one recorded maintenance pause.
		for remaining := c.ChurnPause; remaining > 0; remaining = c.ChurnPause - time.Since(pauseStart) {
			if err := guard.Checkpoint(); err != nil {
				return err
			}
			time.Sleep(min(remaining, 50*time.Millisecond))
		}
		pauseEnd := time.Now()
		r.PauseFinishedUnixNano = pauseEnd.UnixNano()
		r.PauseSeconds = pauseEnd.Sub(pauseStart).Seconds()
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

// Restore only original update/delete/overwrite targets. Insert targets belong
// to the disjoint inserted population and must not cause original-key writes.
func quicksilverRestoreSparse(db kvstore.DB, c quicksilverConfig, stride int, guard *benchGuard) (keys, batches int, err error) {
	var key [128]byte
	value := make([]byte, 32768)
	for off := 0; off < c.Updates; off += 1000 {
		if err = guard.Checkpoint(); err != nil {
			return
		}
		b, e := db.(kvstore.Batcher).NewBatch()
		if e != nil {
			return keys, batches, e
		}
		e = func() (err error) {
			defer func() { err = errors.Join(err, b.Close()) }()
			for j := off; j < min(off+1000, c.Updates); j++ {
				if j%4 == 2 {
					continue
				}
				id := uint64(int64(j)*int64(stride)%int64(c.Keys)) * 2
				k := quicksilverGenericKey(key[:0], id, c.Seed, c.Mixture)
				v := value[:quicksilverRealisticSize(&c, id)]
				quicksilverRealisticValue(v, c, id, 0)
				if err = b.Set(k, v); err != nil {
					return
				}
				keys++
			}
			return quicksilverCommitBatch(b, c)
		}()
		if e != nil {
			return keys, batches, e
		}
		batches++
	}
	return
}
