package caching

import (
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const cowResourceGenerationOwner = 3
const cowResourceDictionary = 2

// One bounded generation-local resolver uses the same identities and owners
// attached to C1. It is not a root/deletion registry. Entries are immutable
// after insertion, and roots/cursors keep the generation alive while reading.
type cowGenerationResources struct {
	mu      sync.RWMutex
	entries []*cowLiveResource
	lease   *memtable.COWExternalLease
}

type cowLiveResource struct {
	id         memtable.COWResourceID
	context    *cowGenerationResources
	manager    *valuelog.Manager
	set        *valuelog.Set
	pin        *rootpublication.IdentityPin
	definition *dictdb.DictionaryReadDefinition
	lease      *memtable.COWExternalLease
	shard      int
	attached   bool // sequenced preparation state; never consulted by readers
}

func newCOWGenerationResources(budget *memtable.COWBudget) (*cowGenerationResources, error) {
	n := budget.Limits().MaxResources - 1 // the resolver itself occupies one C1 slot
	if n < 0 {
		return nil, memtable.ErrCOWCapacity
	}
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowGenerationResources{}))) +
		memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof((*cowLiveResource)(nil)))) +
		memtable.COWAllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0))))
	lease, err := budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	return &cowGenerationResources{entries: make([]*cowLiveResource, n), lease: lease}, nil
}

func (r *cowGenerationResources) close() { r.entries = nil; r.lease.Close() }
func (r *cowGenerationResources) get(id memtable.COWResourceID) *cowLiveResource {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e != nil && e.id == id {
			return e
		}
	}
	return nil
}
func (r *cowGenerationResources) add(e *cowLiveResource) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, old := range r.entries {
		if old == nil {
			r.entries[i] = e
			e.context = r
			return nil
		}
	}
	return memtable.ErrCOWCapacity
}
func (r *cowLiveResource) close() {
	if r.context != nil {
		r.context.mu.Lock()
		for i, e := range r.context.entries {
			if e == r {
				r.context.entries[i] = nil
				break
			}
		}
		r.context.mu.Unlock()
	}
	if r.definition != nil {
		r.definition.Close()
		r.definition = nil
	}
	if r.set != nil {
		r.manager.Release(r.set)
		r.set = nil
	}
	r.pin.Release()
	r.pin = nil
	r.lease.Close()
}

type cowDictionaryReadProvider interface {
	PrepareDictionaryReadDefinition(uint64, valuelog.COWReadLimits, int, func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error)
}

func (c *cowCache) readLimits() valuelog.COWReadLimits {
	n := c.budget.Limits().MaxGenerationBytes
	return valuelog.COWReadLimits{MaxRecordBytes: n, MaxRawBytes: n, MaxValueBytes: n}
}

func (c *cowCache) admitDictionaryRead(s dictdb.DictionaryReadAllocationSizes) (func(), error) {
	limits := c.budget.Limits()
	if s.Snapshot.ValueLog.MapHint > limits.MaxResources || s.Snapshot.ValueLog.FileCount > limits.MaxResources {
		return nil, memtable.ErrCOWCapacity
	}
	bytes := memtable.COWAllocationCharge(s.Owner) + memtable.COWAllocationCharge(s.Callback) +
		memtable.COWAllocationCharge(s.Definition) + memtable.COWAllocationCharge(s.Payload) + memtable.COWAllocationCharge(s.Pin) +
		memtable.COWAllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0)))) +
		cowSnapshotRetentionCharge(s.Snapshot)
	lease, metadata, err := admitCOWSnapshotRetention(c.budget, bytes, s.Snapshot)
	if err != nil {
		return nil, err
	}
	if metadata != nil && s.CaptureMetadata != nil {
		*s.CaptureMetadata = metadata
		metadata = nil // provider transfers this exact enrollment to Snapshot
	}
	return func() { metadata.Close(); lease.Close() }, nil
}

func prepareCOWEmptyRoot(w *memtable.COWWriter, budget *memtable.COWBudget) (*memtable.COWRoot, *cowGenerationResources, error) {
	resources, err := newCOWGenerationResources(budget)
	if err != nil {
		return nil, nil, err
	}
	p, err := w.Prepare(nil, memtable.COWPrepareOptions{ResourceSlots: 1})
	if err != nil {
		resources.close()
		return nil, nil, err
	}
	ids := [1]memtable.COWResourceID{{Kind: cowResourceGenerationOwner, ID: 1}}
	if err = p.AttachResources(ids[:], resources.close); err != nil {
		retired := p.Cancel()
		retired.Drain()
		resources.close()
		return nil, nil, err
	}
	return p.Publish(), resources, nil
}
