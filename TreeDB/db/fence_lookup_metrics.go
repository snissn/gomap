package db

import (
	"fmt"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/lifecycle"
)

type fenceLookupProbeShard struct {
	attempts          atomic.Uint64
	entryScans        atomic.Uint64
	pointerCandidates atomic.Uint64
	readerCalls       atomic.Uint64
	hits              atomic.Uint64
}

func (db *DB) fenceLookupProbeShard(hint int) *fenceLookupProbeShard {
	if db == nil {
		return nil
	}
	idx := hint
	if idx < 0 {
		idx = 0
	} else {
		idx &= lifecycle.FastReaderShardMask
	}
	return &db.fenceLookupProbeShards[idx]
}

func (db *DB) fenceLookupProbeTotals() (attempts, hits, entryScans, pointerCandidates, readerCalls uint64) {
	if db == nil {
		return 0, 0, 0, 0, 0
	}
	for i := range db.fenceLookupProbeShards {
		shard := &db.fenceLookupProbeShards[i]
		attempts += shard.attempts.Load()
		hits += shard.hits.Load()
		entryScans += shard.entryScans.Load()
		pointerCandidates += shard.pointerCandidates.Load()
		readerCalls += shard.readerCalls.Load()
	}
	return attempts, hits, entryScans, pointerCandidates, readerCalls
}

func (db *DB) fenceLookupProbeStatsInto(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	attempts, hits, entryScans, pointerCandidates, readerCalls := db.fenceLookupProbeTotals()
	misses := uint64(0)
	if attempts > hits {
		misses = attempts - hits
	}

	stats["treedb.v2_fenceptr.lookup.probe_attempts"] = fmt.Sprintf("%d", attempts)
	stats["treedb.v2_fenceptr.lookup.probe_hits"] = fmt.Sprintf("%d", hits)
	stats["treedb.v2_fenceptr.lookup.probe_misses"] = fmt.Sprintf("%d", misses)
	stats["treedb.v2_fenceptr.lookup.probe_entry_scans"] = fmt.Sprintf("%d", entryScans)
	stats["treedb.v2_fenceptr.lookup.probe_pointer_candidates"] = fmt.Sprintf("%d", pointerCandidates)
	stats["treedb.v2_fenceptr.lookup.probe_reader_calls"] = fmt.Sprintf("%d", readerCalls)

	if attempts > 0 {
		stats["treedb.v2_fenceptr.lookup.probe_hit_frac"] = fmt.Sprintf("%.6f", float64(hits)/float64(attempts))
		stats["treedb.v2_fenceptr.lookup.avg_entry_scans_per_probe"] = fmt.Sprintf("%.6f", float64(entryScans)/float64(attempts))
		stats["treedb.v2_fenceptr.lookup.avg_pointer_candidates_per_probe"] = fmt.Sprintf("%.6f", float64(pointerCandidates)/float64(attempts))
		stats["treedb.v2_fenceptr.lookup.avg_reader_calls_per_probe"] = fmt.Sprintf("%.6f", float64(readerCalls)/float64(attempts))
		return
	}

	stats["treedb.v2_fenceptr.lookup.probe_hit_frac"] = "0.000000"
	stats["treedb.v2_fenceptr.lookup.avg_entry_scans_per_probe"] = "0.000000"
	stats["treedb.v2_fenceptr.lookup.avg_pointer_candidates_per_probe"] = "0.000000"
	stats["treedb.v2_fenceptr.lookup.avg_reader_calls_per_probe"] = "0.000000"
}
