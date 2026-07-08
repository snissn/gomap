package redisserver

import "time"

// Config controls server behavior and engine selection.
type Config struct {
	Addr string
	Dir  string

	Engine string // "hashdb" or "treedb"

	// Authentication. Empty means AUTH is not required.
	Auth string

	// Batch SET optimization (benchmark-focused).
	BatchSets             bool
	BatchSize             int
	BatchFlushOnNonset    bool
	BatchFlushOnNonsetSet bool

	// HashDB options.
	HashDBShards      int
	HashDBCompression bool

	// TreeDB options.
	TreeDBFlushThreshold    int64
	TreeDBValueLogThreshold int
	TreeDBProfile           string
	TreeDBWriteLanes        int
	TreeDBMemtableShards    int

	// TreeDB compaction defaults (used by COMPACT/BGREWRITEAOF).
	CompactDeadRatio         float64
	CompactMinBytes          uint64
	CompactMaxSlabs          int
	CompactMicroBatch        int
	CompactRotateBeforeWrite bool
	CompactCopyBytesPerSec   int64
	CompactCopyBurstBytes    int64

	// Optional custom logger. If nil, the default logger is used.
	Logf func(format string, args ...any)

	// Optional server-level timeout for idle clients.
	IdleClose time.Duration
}

func (c *Config) setDefaults() {
	if c.Addr == "" {
		c.Addr = ":6380"
	}
	if c.Engine == "" {
		c.Engine = "hashdb"
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 16
	}
	if !c.BatchFlushOnNonsetSet {
		c.BatchFlushOnNonset = true
	}
	if c.CompactDeadRatio <= 0 {
		c.CompactDeadRatio = 0.50
	}
	if c.CompactMinBytes == 0 {
		c.CompactMinBytes = 1 * 1024 * 1024
	}
	if c.CompactMaxSlabs == 0 {
		c.CompactMaxSlabs = 1
	}
	if c.CompactMicroBatch == 0 {
		c.CompactMicroBatch = 256
	}
}
