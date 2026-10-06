package caching

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func validateCOWOpenOptions(opts Options) (memtable.COWLimits, int, error) {
	limits := opts.COWMemtableLimits
	if limits == (memtable.COWLimits{}) {
		limits = memtable.DefaultCOWLimits()
	}
	if err := limits.Validate(); err != nil {
		return limits, 0, err
	}
	shards := opts.MemtableShards
	if shards <= 0 {
		shards = 4
	}
	if shards > 1<<20 {
		return limits, 0, ErrCOWUnsupported
	}
	shards = normalizeShardCount(shards)
	if shards < 1 || shards > limits.MaxSources || shards > limits.MaxGenerations {
		return limits, 0, fmt.Errorf("cow_btree shard count exceeds finite source/generation limits")
	}
	if opts.DomainIngressWorkers > 0 || (!opts.DisableWAL && !opts.ExternalCommandWAL) || opts.ValueLogTemplateMode != 0 {
		return limits, 0, ErrCOWUnsupported
	}
	return limits, shards, nil
}

// The common placement pipeline uses fixed shard locks and counters. Its COW
// compatibility tables have no storage and can never accept mutations. Reads
// use the published COW cut instead of this legacy compatibility surface.
var cowEmptyCompatibilityTable = cowTable{}

// ValidateCOWOpenOptions performs the COW capability and finite-limit checks
// without touching storage. Public Open uses the same checks before opening
// its backend or side stores.
func ValidateCOWOpenOptions(opts Options) (memtable.COWLimits, int, error) {
	return validateCOWOpenOptions(opts)
}
