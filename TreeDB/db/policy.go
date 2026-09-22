package db

import "github.com/snissn/gomap/TreeDB/page"

// WritePolicy defines the heuristics and thresholds for write operations.
type WritePolicy struct {
	FlushThreshold  int64 // Size of memtable before flush
	InlineThreshold int   // Max size of value to store inline
}

// DefaultWritePolicy returns the default policy.
func DefaultWritePolicy() WritePolicy {
	return WritePolicy{
		FlushThreshold:  64 * 1024 * 1024, // 64MB (optimized for throughput via Arena)
		InlineThreshold: page.DefaultInlineThreshold,
	}
}
