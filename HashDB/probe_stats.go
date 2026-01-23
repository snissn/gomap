package hashdb

// ProbeStats captures light-weight probe metrics for debugging/perf analysis.
// Intended for experimental use; not concurrency-safe for shared DB access.
type ProbeStats struct {
	AddCalls  uint64
	AddGroups uint64
	GetCalls  uint64
	GetGroups uint64
	H2Matches uint64
	HashHits  uint64
	KeyReads  uint64
	KeyBytes  uint64
	ItemReads uint64
	ItemBytes uint64
}

// ProbeStatsSnapshot returns a copy of the current probe stats.
func (h *DB) ProbeStatsSnapshot() ProbeStats {
	return h.probeStats
}

// ResetProbeStats clears probe stats counters.
func (h *DB) ResetProbeStats() {
	h.probeStats = ProbeStats{}
}
