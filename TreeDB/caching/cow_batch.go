package caching

import (
	"fmt"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const cowResourceValueLogFile = 1

type cowBatchPreparation struct {
	cut          *cowPreparedCut
	scratch      *memtable.COWExternalLease
	files        [][]uint32
	resources    []*cowLiveResource
	dictionaries [][]uint64
}

// PrepareExternalCommandWALPublication is called under the command append
// sequencer, after its final point revisions have been assigned. Cached value
// placement has already finished. No command frame exists yet.
func (b *Batch) PrepareExternalCommandWALPublication() error {
	if b.db.cow == nil {
		return nil
	}
	if b.cowState.prepared != nil {
		return fmt.Errorf("COW batch already prepared")
	}
	if b.cowState.producer != nil && b.cowState.producer.err != nil {
		return b.cowState.producer.err
	}
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeCOWPreparation, durabilitycut.ResourceAuxiliary, b.db.dir); err != nil {
		return err
	}
	c := b.db.cow
	n := len(b.entries)
	shards := len(c.writers)
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchPreparation{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memtable.COWPrepareOptions{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof([]uint32{}))) +
		memtable.COWAllocationCharge(uint64(n)*4)
	bytes += memtable.COWAllocationCharge(uint64(n) * uint64(unsafe.Sizeof((*cowLiveResource)(nil))))
	bytes += memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof((*cowLiveResource)(nil)))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof([]uint64{}))) +
		memtable.COWAllocationCharge(uint64(n)*8)
	reusePrediction := len(b.shardCnts) == shards && len(b.cowState.predictionGroups) == shards && len(b.cowState.predictionStorage) == n
	if !reusePrediction {
		bytes += memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(int(0)))) +
			memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof([]memtable.COWMutation{}))) +
			memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof(memtable.COWMutation{})))
	}
	lease, err := c.budget.AcquireExternal(bytes)
	if err != nil {
		return err
	}
	p := &cowBatchPreparation{scratch: lease, files: make([][]uint32, shards), resources: make([]*cowLiveResource, 0, 2*n), dictionaries: make([][]uint64, shards)}
	b.cowState.prepared = p // caller drains all cancellation, including partial stage
	// Prediction has finished. Reuse its admitted scalar/group/mutation backing,
	// replacing predicted pointers/revisions with the final canonical entries.
	counts, groups, mutations := b.shardCnts, b.cowState.predictionGroups, b.cowState.predictionStorage
	if !reusePrediction {
		counts = make([]int, shards)
		groups = make([][]memtable.COWMutation, shards)
		mutations = make([]memtable.COWMutation, n)
	}
	clear(counts)
	for _, e := range b.entries {
		counts[b.db.shardIndex(e.Key)]++
	}
	opts := make([]memtable.COWPrepareOptions, shards)
	files := make([]uint32, n)
	dictionaries := make([]uint64, n)
	start := 0
	for i, count := range counts {
		groups[i] = mutations[start : start : start+count]
		p.files[i] = files[start : start : start+count]
		p.dictionaries[i] = dictionaries[start : start : start+count]
		start += count
	}
	for _, e := range b.entries {
		i := b.db.shardIndex(e.Key)
		m := memtable.COWMutation{Key: e.Key, Value: e.Value, Ptr: e.ValuePtr, Revision: e.Revision}
		switch e.Type {
		case batch.OpDelete:
			m.Flags = node.FlagTombstone
			m.Value = nil
		case batch.OpPut:
			m.Flags = node.FlagInline
			if e.IsPtr {
				m.Flags = node.FlagPointer
				m.Value = nil
				if b.cowState.producer == nil {
					return ErrCOWUnsupported
				}
				header, ok := b.cowState.producer.frame(e.ValuePtr)
				if !ok {
					return ErrCOWUnsupported
				}
				if header.Flags&valuelog.FrameFlagCompressed != 0 && header.DictID != 0 {
					id := memtable.COWResourceID{Kind: cowResourceDictionary, ID: header.DictID}
					if !c.writers[i].HasResource(id) {
						p.dictionaries[i] = append(p.dictionaries[i], header.DictID)
					}
				}
				id := memtable.COWResourceID{Kind: cowResourceValueLogFile, ID: uint64(e.ValuePtr.FileID)}
				if !c.writers[i].HasResource(id) {
					p.files[i] = append(p.files[i], e.ValuePtr.FileID)
				}
			}
		default:
			return ErrCOWUnsupported
		}
		groups[i] = append(groups[i], m)
	}
	for i := range opts {
		slices.Sort(p.files[i])
		p.files[i] = slices.Compact(p.files[i])
		slices.Sort(p.dictionaries[i])
		p.dictionaries[i] = slices.Compact(p.dictionaries[i])
		opts[i].ResourceSlots = len(p.files[i])
		opts[i].ResourceSlots += len(p.dictionaries[i])
		if len(p.dictionaries[i]) > 0 {
			provider, ok := b.db.dictStore.(cowDictionaryReadProvider)
			if !ok {
				return ErrCOWUnsupported
			}
			for _, id := range p.dictionaries[i] {
				ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowLiveResource{}))) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
				ownerLease, e := c.budget.AcquireExternal(ownerBytes)
				if e != nil {
					return e
				}
				resource := &cowLiveResource{id: memtable.COWResourceID{Kind: cowResourceDictionary, ID: id}, lease: ownerLease, shard: i}
				p.resources = append(p.resources, resource)
				resource.definition, e = provider.PrepareDictionaryReadDefinition(id, c.readLimits(), c.budget.Limits().MaxResources, c.admitDictionaryRead)
				if e != nil {
					return e
				}
			}
		}
		for _, fileID := range p.files[i] {
			shape, ok := b.db.valueLogReader.RegisteredFileRetentionSizes(fileID)
			if !ok {
				return rootpublication.ErrUnresolvedResource
			}
			opts[i].ResourceBytes += memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowLiveResource{}))) +
				memtable.COWAllocationCharge(shape.Wrapper) + memtable.COWAllocationCharge(shape.MapEnvelope) +
				memtable.COWAllocationCharge(shape.FileWrapper) + memtable.COWAllocationCharge(shape.PathEnvelope) +
				memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.IdentityPin{}))) +
				memtable.COWAllocationCharge(512) + // caller's one-entry identity map
				memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
		}
	}
	p.cut, err = c.prepare(groups, opts)
	if err == nil {
		err = durabilitycut.EmitBasic(durabilitycut.AfterCOWPreparation, durabilitycut.ResourceAuxiliary, b.db.dir)
	}
	return err
}

// ExternalCommandWALFinalizer validates the canonical pointer/RID authority
// before attaching independent live leases. It retains no borrowed WAL metadata.
func (b *Batch) ExternalCommandWALFinalizer() backenddb.RawKVCommandWALFinalize {
	if b == nil || b.db == nil || b.db.cow == nil {
		return nil
	}
	return b.finalizeCOWPublication
}

func (b *Batch) finalizeCOWPublication(payload []byte, lookup func(page.ValuePtr) (uint64, bool)) error {
	if b.cowState.prepared == nil || b.cowState.prepared.cut == nil {
		return fmt.Errorf("COW publication was not prepared")
	}
	if err := validateCOWCanonicalEntries(payload, b.entries, lookup); err != nil {
		return err
	}
	for _, resource := range b.cowState.prepared.resources {
		if resource.id.Kind != cowResourceDictionary {
			continue
		}
		shard := resource.shard
		if err := b.cowState.prepared.cut.next.shards[shard].resources.add(resource); err != nil {
			return err
		}
		ids := [1]memtable.COWResourceID{resource.id}
		if err := b.cowState.prepared.cut.prepared[shard].AttachResources(ids[:], resource.close); err != nil {
			return err
		}
		resource.attached = true
	}
	for shard, files := range b.cowState.prepared.files {
		for _, fileID := range files {
			manager := b.db.valueLogReader
			resource := &cowLiveResource{id: memtable.COWResourceID{Kind: cowResourceValueLogFile, ID: uint64(fileID)}, manager: manager}
			b.cowState.prepared.resources = append(b.cowState.prepared.resources, resource)
			registry := manager.StableResourcePinRegistry()
			identity, registered := manager.StableSegmentIdentity(fileID)
			if !registered || registry == nil || registry != b.db.valueLogIdentityPins {
				return fmt.Errorf("COW file %d lacks shared physical deletion authority", fileID)
			}
			physicalPin, err := registry.Pin(identity)
			if err != nil {
				return err
			}
			resource.pin = physicalPin
			set := b.db.valueLogReader.CurrentSubsetNoRefresh(map[uint32]struct{}{fileID: {}})
			resource.set = set
			if len(set.Files) != 1 {
				return fmt.Errorf("COW value-log file %d is not registered", fileID)
			}
			id := [1]memtable.COWResourceID{{Kind: cowResourceValueLogFile, ID: uint64(fileID)}}
			if err := b.cowState.prepared.cut.next.shards[shard].resources.add(resource); err != nil {
				return err
			}
			if err := b.cowState.prepared.cut.prepared[shard].AttachResources(id[:], resource.close); err != nil {
				return err
			}
			resource.attached = true
		}
	}
	return durabilitycut.EmitBasic(durabilitycut.AfterCOWCanonicalPreparation, durabilitycut.ResourceAuxiliary, b.db.dir)
}

func (b *Batch) cancelCOWPublication() *cowBatchPreparation {
	if b.cowState == nil {
		return nil
	}
	p := b.cowState.prepared
	b.cowState.prepared = nil
	if p != nil && p.cut != nil {
		p.cut.cancel()
	}
	return p
}

func (p *cowBatchPreparation) drainCancelled() {
	if p.cut != nil {
		p.cut.drainCancelled()
	}
	for _, resource := range p.resources {
		if !resource.attached {
			resource.close()
		}
	}
	p.scratch.Close()
}

func (b *Batch) writeCOWPublication(syncWrite bool, unlock func()) error {
	checkpoint := syncWrite && b.commandWALAppend == nil
	var err error
	if b.commandWALAppend != nil {
		err = b.commandWALAppend()
	} else {
		finalizer, ok := b.db.backend.(interface {
			FinalizeRawKVEntryScanForCachedPublication(func() error, backenddb.RawKVCommandWALFinalize, func(func(batch.Entry) error) error, int) error
		})
		if !ok {
			err = fmt.Errorf("COW cache requires canonical backend finalization")
		} else {
			err = finalizer.FinalizeRawKVEntryScanForCachedPublication(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, len(b.entries))
		}
	}
	if err != nil {
		p := b.cancelCOWPublication()
		unlock()
		if p != nil {
			p.drainCancelled()
		}
		return err
	}
	if b.cowState.prepared == nil || b.cowState.prepared.cut == nil {
		panic("COW append succeeded without staged publication")
	}
	p := b.cowState.prepared
	b.cowState.prepared = nil
	// Accepted publication is nonfallible. This existing internal observation
	// seam may pause tests, but an observer cannot revoke an accepted frame.
	_ = durabilitycut.EmitBasic(durabilitycut.BeforeCOWCutSwap, durabilitycut.ResourceAuxiliary, b.db.dir)
	old := p.cut.publish()
	var mutableBytes int64
	for _, shard := range p.cut.next.shards {
		mutableBytes += shard.size
	}
	b.db.mutableBytes.Store(mutableBytes)
	// Batch storage is no longer borrowed: C1 copied changed payloads before
	// the append. Release admission and owner before retirement or checkpoint.
	unlock()
	for _, resource := range p.resources {
		if resource.definition != nil {
			resource.definition.ReleaseCapture()
		}
	}
	if old != nil {
		old.drain()
	}
	p.scratch.Close()
	b.db.noteWrite()
	b.updateBatchEntryHint()
	b.updateBatchCopyHint()
	b.Reset()
	if checkpoint {
		return b.db.Checkpoint()
	}
	return nil
}

func (db *DB) cowPoint(key, value []byte, put, syncWrite bool) error {
	b := db.NewBatchWithSize(1)
	var err error
	if put {
		err = b.Set(key, value)
	} else {
		err = b.Delete(key)
	}
	if err == nil {
		if syncWrite {
			err = b.WriteSync()
		} else {
			err = b.Write()
		}
	}
	closeErr := b.Close()
	if err != nil {
		return err
	}
	return closeErr
}
