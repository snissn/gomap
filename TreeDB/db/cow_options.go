package db

import "github.com/snissn/gomap/TreeDB/internal/memtable"

// COWMemtableLimits bounds allocations owned by the explicit cow_btree cache.
// A completely zero value selects finite defaults; partially specified limits
// are rejected. These limits describe retained capacities, not process RSS.
type COWMemtableLimits = memtable.COWLimits

func DefaultCOWMemtableLimits() COWMemtableLimits { return memtable.DefaultCOWLimits() }
