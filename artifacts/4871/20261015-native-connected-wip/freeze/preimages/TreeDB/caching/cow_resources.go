package caching

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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
	releaseMu  sync.Mutex
	released   bool
	closeIndex int
	mu         sync.RWMutex
	entries    []*cowLiveResource
	lease      *memtable.COWExternalLease
}

type cowLiveResource struct {
	releaseMu           sync.Mutex
	releaseCondition    sync.Cond
	releasing, released bool
	releaseErr          error
	failure             rootpublication.OriginalReleaseFailure
	db                  *DB
	resolverIndex       int
	id                  memtable.COWResourceID
	context             *cowGenerationResources
	manager             *valuelog.Manager
	set                 *valuelog.Set
	pin                 *rootpublication.IdentityPin
	definition          *dictdb.DictionaryReadDefinition
	lease               *memtable.COWExternalLease
	shard               int
	attached            bool // sequenced preparation state; never consulted by readers
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

func (r *cowGenerationResources) close() {
	_, _, completion, _ := r.ReleaseCOWResourceWorkV6(nil)
	if completion != nil {
		completion.Close()
	}
}

// The original resolver is the first attached owner and retires last. Its
// original backing is retained until every live resource actually completed.
func (r *cowGenerationResources) ReleaseCOWResourceWorkV6(w *iterator.OrdinalScanWork) (bool, bool, retainedalloc.Lease, error) {
	if r == nil {
		return true, true, nil, nil
	}
	r.releaseMu.Lock()
	defer r.releaseMu.Unlock()
	if r.released {
		return true, true, nil, nil
	}
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))) {
		return false, false, nil, nil
	}
	r.mu.Lock()
	for r.closeIndex < len(r.entries) {
		if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof((*cowLiveResource)(nil)))) {
			r.mu.Unlock()
			return false, false, nil, nil
		}
		if r.entries[r.closeIndex] != nil {
			r.mu.Unlock()
			return false, false, nil, memtable.ErrCOWResourceWork
		}
		r.closeIndex++
	}
	r.entries = nil
	completion := r.lease
	r.lease = nil
	r.released = true
	r.mu.Unlock()
	return true, true, completion, nil
}
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
	_, _, completion, _ := r.ReleaseCOWResourceWorkV6(nil)
	if completion != nil {
		completion.Close()
	}
}

// Completion runs on the SAME resource in the original generation slot.
// No callback, physical I/O or error traversal executes under its mutex.
func (r *cowLiveResource) ReleaseCOWResourceWorkV6(w *iterator.OrdinalScanWork) (bool, bool, retainedalloc.Lease, error) {
	if r == nil {
		return true, true, nil, nil
	}
	r.releaseMu.Lock()
	if r.releaseCondition.L == nil {
		r.releaseCondition.L = &r.releaseMu
	}
	for r.releasing {
		if w != nil {
			r.releaseMu.Unlock()
			return false, false, nil, nil
		}
		r.releaseCondition.Wait()
	}
	if r.releaseErr != nil {
		err := r.releaseErr
		r.releaseMu.Unlock()
		return false, false, nil, err
	}
	if r.released {
		r.releaseMu.Unlock()
		return true, true, nil, nil
	}
	// Dictionary capture still has a void original finalizer. It cannot become
	// a successful bounded physical callback merely through a typed wrapper.
	if w != nil && r.definition != nil {
		r.releaseMu.Unlock()
		return false, false, nil, memtable.ErrCOWResourceWork
	}
	r.releasing = true
	r.releaseMu.Unlock()
	var err error
	ready := true
	if r.definition != nil {
		r.definition.Close()
		r.definition = nil
	}
	if r.set != nil {
		if w == nil {
			unlimited := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
			ready, err = r.manager.ReleaseNativeMVCCSetStep(r.set, &unlimited)
		} else {
			ready, err = r.manager.ReleaseNativeMVCCSetStep(r.set, w)
		}
		if ready && err == nil {
			r.set = nil
			r.manager = nil
		}
	}
	if ready && err == nil && r.pin != nil {
		ready, err = r.pin.ReleaseWithWorkV6(w)
		if ready && err == nil {
			r.pin = nil
		}
	}
	if ready && err == nil && r.context != nil {
		context := r.context
		context.mu.Lock()
		for r.resolverIndex < len(context.entries) {
			if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof((*cowLiveResource)(nil)))) {
				ready = false
				break
			}
			i := r.resolverIndex
			r.resolverIndex++
			if context.entries[i] == r {
				context.entries[i] = nil
				break
			}
		}
		context.mu.Unlock()
		if ready {
			r.context = nil
		}
	}
	hard := err != nil && !errors.Is(err, retainedalloc.ErrWorkRequired) && !errors.Is(err, memtable.ErrCOWResourceWork)
	var completion retainedalloc.Lease
	var reportDB *DB
	if hard {
		err = r.failure.RetainOriginalCleanupFailure(r, err)
		reportDB = r.db
	}
	r.releaseMu.Lock()
	if hard {
		r.releaseErr = err
	}
	if ready && err == nil {
		completion = r.lease
		r.lease = nil
		r.db = nil
		r.released = true
	}
	r.releasing = false
	r.releaseCondition.Broadcast()
	r.releaseMu.Unlock()
	if reportDB != nil {
		reportDB.reportError(err)
	}
	return ready && err == nil, ready, completion, err
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
	if err = p.AttachCheckedResourcesV6(ids[:], resources); err != nil {
		retired := p.Cancel()
		retired.Drain()
		resources.close()
		return nil, nil, err
	}
	return p.Publish(), resources, nil
}
