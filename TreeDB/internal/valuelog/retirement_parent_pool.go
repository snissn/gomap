package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"sync"
	"unsafe"
)

// One fixed directory plus one current handle per physical parent. There is no
// growing map capacity or shrink copy retained after historical parents depart.
// The envelope includes the fixed heads, handle and os.File internal/name state.
const retirementParentRetentionEnvelope uint64 = 2048
const retirementParentBuckets = 32

// Manager/File membership locks may enter mu; mu never enters their locks.
// Acquisition searches one bucket during ordinary admission. A retained handle
// drops its exact intrusive node without searching any other parent.
type retirementParentPool struct {
	mu    sync.Mutex
	heads [retirementParentBuckets]*retirementParentHandle
	count int
}
type retirementParentHandle struct {
	pool              *retirementParentPool
	key               rootpublication.StableIdentity
	file              *os.File
	owners, borrowers int
	prev, next        *retirementParentHandle
	bucket            uint8
}

func retirementParentBucket(identity rootpublication.StableIdentity) uint8 {
	hash := identity.VolumeID
	for _, b := range identity.ObjectID {
		hash = (hash ^ uint64(b)) * 1099511628211
	}
	for i := 0; i < len(identity.Platform); i++ {
		hash = (hash ^ uint64(identity.Platform[i])) * 1099511628211
	}
	return uint8(hash & (retirementParentBuckets - 1))
}
func (pool *retirementParentPool) acquire(parent *os.File, identity rootpublication.StableIdentity) (*retirementParentHandle, *os.File) {
	identity.Generation = 0
	pool.mu.Lock()
	defer pool.mu.Unlock()
	bucket := retirementParentBucket(identity)
	for retained := pool.heads[bucket]; retained != nil; retained = retained.next {
		if retained.key == identity {
			retained.owners++
			return retained, parent
		}
	}
	retained := &retirementParentHandle{pool: pool, key: identity, file: parent, owners: 1, bucket: bucket, next: pool.heads[bucket]}
	if retained.next != nil {
		retained.next.prev = retained
	}
	pool.heads[bucket] = retained
	pool.count++
	return retained, nil
}

// drop returns a unique final close token, consumed outside all membership locks.
func (parent *retirementParentHandle) drop(owner bool) *os.File {
	file, _ := parent.dropWithWorkV6(owner, nil)
	return file
}

// The same unlink core serves ordinary and typed owners. Typed release charges
// the ref/header, pool directory and each real immediate neighbor it updates.
func (parent *retirementParentHandle) dropWithWorkV6(owner bool, w *iterator.OrdinalScanWork) (*os.File, bool) {
	pool := parent.pool
	pool.mu.Lock()
	defer pool.mu.Unlock()
	final := parent.owners+parent.borrowers == 1
	records := uint64(2)
	bytes := 2*uint64(unsafe.Sizeof(retirementParentHandle{})) + 2*uint64(unsafe.Sizeof(retirementParentPool{}))
	if final {
		if parent.prev != nil {
			records++
			bytes += 2 * uint64(unsafe.Sizeof(retirementParentHandle{}))
		}
		if parent.next != nil {
			records++
			bytes += 2 * uint64(unsafe.Sizeof(retirementParentHandle{}))
		}
	}
	if w != nil && !w.Reserve(records, bytes) {
		return nil, false
	}
	if owner {
		parent.owners--
	} else {
		parent.borrowers--
	}
	if parent.owners != 0 || parent.borrowers != 0 {
		return nil, true
	}
	if parent.prev == nil {
		pool.heads[parent.bucket] = parent.next
	} else {
		parent.prev.next = parent.next
	}
	if parent.next != nil {
		parent.next.prev = parent.prev
	}
	parent.prev, parent.next = nil, nil
	pool.count--
	return parent.file, true
}
