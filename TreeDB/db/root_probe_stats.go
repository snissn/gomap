package db

import "github.com/snissn/gomap/TreeDB/tree"

type rootProbeStats struct {
	keyFallbackCalls    uint64
	keyFallbackItems    uint64
	prefixFallbackCalls uint64
	prefixFallbackItems uint64
}

func (db *DB) noteRootProbeKeyFallback(stats tree.ProbeFallbackStats) {
	if db == nil || stats.FallbackCalls == 0 {
		return
	}
	db.rootProbeKeyFallbackCalls.Add(stats.FallbackCalls)
	db.rootProbeKeyFallbackItems.Add(stats.FallbackItems)
}

func (db *DB) noteRootProbePrefixFallback(stats tree.ProbeFallbackStats) {
	if db == nil || stats.FallbackCalls == 0 {
		return
	}
	db.rootProbePrefixFallbackCalls.Add(stats.FallbackCalls)
	db.rootProbePrefixFallbackItems.Add(stats.FallbackItems)
}

func (db *DB) rootProbeStatsSnapshot() rootProbeStats {
	if db == nil {
		return rootProbeStats{}
	}
	return rootProbeStats{
		keyFallbackCalls:    db.rootProbeKeyFallbackCalls.Load(),
		keyFallbackItems:    db.rootProbeKeyFallbackItems.Load(),
		prefixFallbackCalls: db.rootProbePrefixFallbackCalls.Load(),
		prefixFallbackItems: db.rootProbePrefixFallbackItems.Load(),
	}
}
