package collections

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// vectorPartitionActiveAuthorityStateV1 is the process-local, mutation-driven
// serving authority for one collection/index lifecycle. Durable lifecycle
// state is still decoded and validated on every cold open. Warm requests only
// compare this bounded state and the DB's coherently published system root.
type vectorPartitionActiveAuthorityStateV1 struct {
	activeGeneration atomic.Uint64
	revision         atomic.Uint64
}

type vectorPartitionActiveAuthorityLeaseV1 struct {
	key      string
	state    *vectorPartitionActiveAuthorityStateV1
	once     sync.Once
	released atomic.Bool
}

type vectorPartitionActiveAuthorityRegistryEntryV1 struct {
	state *vectorPartitionActiveAuthorityStateV1
	refs  uint64
}

var vectorPartitionActiveAuthorityRegistryV1 = struct {
	sync.Mutex
	entries map[string]*vectorPartitionActiveAuthorityRegistryEntryV1
}{entries: make(map[string]*vectorPartitionActiveAuthorityRegistryEntryV1)}

func vectorPartitionActiveAuthorityKeyV1(root, collection, index string) (string, error) {
	canonical, err := canonicalVectorPartitionStorageRootV1(root)
	if err != nil {
		return "", err
	}
	if collection == "" || index == "" {
		return "", fmt.Errorf("%w: empty active authority identity", ErrVectorPartitionManifestInvalid)
	}
	return canonical + "\x00" + collection + "\x00" + index, nil
}

func registerVectorPartitionActiveAuthorityV1(root, collection, index string, generation, sourceSystemRoot uint64) (VectorPartitionActiveAuthorityTokenV1, error) {
	if generation == 0 {
		return VectorPartitionActiveAuthorityTokenV1{}, fmt.Errorf("%w: empty active authority generation", ErrVectorPartitionManifestInvalid)
	}
	key, err := vectorPartitionActiveAuthorityKeyV1(root, collection, index)
	if err != nil {
		return VectorPartitionActiveAuthorityTokenV1{}, err
	}

	vectorPartitionActiveAuthorityRegistryV1.Lock()
	entry := vectorPartitionActiveAuthorityRegistryV1.entries[key]
	if entry == nil {
		state := &vectorPartitionActiveAuthorityStateV1{}
		state.activeGeneration.Store(generation)
		state.revision.Store(1)
		entry = &vectorPartitionActiveAuthorityRegistryEntryV1{state: state}
		vectorPartitionActiveAuthorityRegistryV1.entries[key] = entry
	} else if entry.state.activeGeneration.Load() != generation {
		entry.state.activeGeneration.Store(generation)
		entry.state.revision.Add(1)
	}
	entry.refs++
	revision := entry.state.revision.Load()
	lease := &vectorPartitionActiveAuthorityLeaseV1{key: key, state: entry.state}
	vectorPartitionActiveAuthorityRegistryV1.Unlock()

	return VectorPartitionActiveAuthorityTokenV1{
		state:            entry.state,
		lease:            lease,
		index:            index,
		revision:         revision,
		sourceSystemRoot: sourceSystemRoot,
		generation:       generation,
	}, nil
}

func releaseVectorPartitionActiveAuthorityV1(lease *vectorPartitionActiveAuthorityLeaseV1) {
	if lease == nil {
		return
	}
	lease.once.Do(func() {
		lease.released.Store(true)
		vectorPartitionActiveAuthorityRegistryV1.Lock()
		entry := vectorPartitionActiveAuthorityRegistryV1.entries[lease.key]
		if entry != nil && entry.state == lease.state {
			if entry.refs <= 1 {
				delete(vectorPartitionActiveAuthorityRegistryV1.entries, lease.key)
			} else {
				entry.refs--
			}
		}
		vectorPartitionActiveAuthorityRegistryV1.Unlock()
	})
}

// notifyVectorPartitionActiveAuthorityV1 runs after a durable activation or
// deactivation succeeds while the root storage barrier is still held. BUILD
// publication deliberately does not call it, so staging generation N+1 cannot
// invalidate the still-active generation N.
func notifyVectorPartitionActiveAuthorityV1(root, collection, index string, generation uint64) {
	key, err := vectorPartitionActiveAuthorityKeyV1(root, collection, index)
	if err != nil {
		return
	}
	vectorPartitionActiveAuthorityRegistryV1.Lock()
	entry := vectorPartitionActiveAuthorityRegistryV1.entries[key]
	if entry != nil && entry.state.activeGeneration.Load() != generation {
		entry.state.activeGeneration.Store(generation)
		entry.state.revision.Add(1)
	}
	vectorPartitionActiveAuthorityRegistryV1.Unlock()
}
