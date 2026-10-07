package valuelog

import (
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Reserve worst-case one physical parent per File before capture publication.
// 2 KiB covers the pool/entry wrappers, fixed-name os.File and its internal
// state, and rounded Go 1.26 map groups/directory including bounded shrink
// overlap. This is an allocation envelope, not a sizeof or kernel-FD claim.
const retirementParentRetentionEnvelope uint64 = 2048

// Lazy and manager-owned. File membership locks may enter mu; mu never enters
// manager or File locks. Entries stay indexed while owners OR borrowers exist.
type retirementParentPool struct {
	mu        sync.Mutex
	entries   map[rootpublication.StableIdentity]*retirementParentHandle
	highWater int
}
type retirementParentHandle struct {
	pool      *retirementParentPool
	key       rootpublication.StableIdentity
	file      *os.File
	owners    int
	borrowers int
}

func (pool *retirementParentPool) acquire(parent *os.File, identity rootpublication.StableIdentity) (*retirementParentHandle, *os.File) {
	identity.Generation = 0 // SamePhysicalIdentity ignores logical generation.
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if retained := pool.entries[identity]; retained != nil {
		retained.owners++
		return retained, parent
	}
	if pool.entries == nil {
		pool.entries = make(map[rootpublication.StableIdentity]*retirementParentHandle)
	}
	retained := &retirementParentHandle{pool: pool, key: identity, file: parent, owners: 1}
	pool.entries[identity] = retained
	if len(pool.entries) > pool.highWater {
		pool.highWater = len(pool.entries)
	}
	return retained, nil
}

// drop returns a unique final close token. Actual Close runs after all locks.
func (parent *retirementParentHandle) drop(owner bool) *os.File {
	pool := parent.pool
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if owner {
		parent.owners--
	} else {
		parent.borrowers--
	}
	if parent.owners != 0 || parent.borrowers != 0 {
		return nil
	}
	delete(pool.entries, parent.key)
	// Keep historical capacity bounded by currently owned/borrowed directories.
	// Clear at zero; halve high-water slack before it can accumulate uncharged
	// backing groups across successive disjoint directory populations.
	if len(pool.entries) == 0 {
		pool.entries = nil
		pool.highWater = 0
	} else if len(pool.entries)*2 < pool.highWater {
		compact := make(map[rootpublication.StableIdentity]*retirementParentHandle, len(pool.entries))
		for key, retained := range pool.entries {
			compact[key] = retained
		}
		pool.entries = compact
		pool.highWater = len(compact)
	}
	return parent.file
}
