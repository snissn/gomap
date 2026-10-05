package caching

import (
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const cowResourceValueLogFile = 1

type cowBatchPreparation struct {
	cut     *cowPreparedCut
	scratch *memtable.COWExternalLease
	files   [][]uint32
}

// PrepareExternalCommandWALPublication is called under the command append
// sequencer, after its final point revisions have been assigned. Cached value
// placement has already finished. No command frame exists yet.
func (b *Batch) PrepareExternalCommandWALPublication() error {
	if b.db.cow == nil {
		return nil
	}
	if b.cowPrepared != nil {
		return fmt.Errorf("COW batch already prepared")
	}
	c := b.db.cow
	n := len(b.entries)
	shards := len(c.writers)
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchPreparation{}))) +
		uint64(n)*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWMutation{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof([]memtable.COWMutation{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memtable.COWPrepareOptions{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof([]uint32{}))) +
		uint64(n)*memtable.COWAllocationCharge(4) + memtable.COWAllocationCharge(uint64(shards)*8)
	lease, err := c.budget.AcquireExternal(bytes)
	if err != nil {
		return err
	}
	p := &cowBatchPreparation{scratch: lease, files: make([][]uint32, shards)}
	b.cowPrepared = p // caller drains all cancellation, including partial stage
	counts := make([]int, shards)
	for _, e := range b.entries {
		counts[b.db.shardIndex(e.Key)]++
	}
	groups := make([][]memtable.COWMutation, shards)
	opts := make([]memtable.COWPrepareOptions, shards)
	for i, count := range counts {
		groups[i] = make([]memtable.COWMutation, 0, count)
		p.files[i] = make([]uint32, 0, count)
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
				id := memtable.COWResourceID{Kind: cowResourceValueLogFile, ID: uint64(e.ValuePtr.FileID)}
				if !c.writers[i].HasResource(id) {
					seen := false
					for _, old := range p.files[i] {
						if old == e.ValuePtr.FileID {
							seen = true
							break
						}
					}
					if !seen {
						p.files[i] = append(p.files[i], e.ValuePtr.FileID)
					}
				}
			}
		default:
			return ErrCOWUnsupported
		}
		groups[i] = append(groups[i], m)
	}
	for i := range opts {
		opts[i].ResourceSlots = len(p.files[i])
		// Each distinct file owns its immutable Set, one-slot map (the value-log
		// owner's 512*(hint+1) Go 1.26 metadata envelope), physical Pin and
		// release closure. C1 separately owns its copied ID/callback backing.
		opts[i].ResourceBytes = uint64(len(p.files[i])) * (memtable.COWAllocationCharge(uint64(unsafe.Sizeof(valuelog.Set{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(rootpublication.IdentityPin{}))) +
			memtable.COWAllocationCharge(1024) + memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0)))))
	}
	p.cut, err = c.prepare(groups, opts)
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
	if b.cowPrepared == nil || b.cowPrepared.cut == nil {
		return fmt.Errorf("COW publication was not prepared")
	}
	if err := validateCOWCanonicalEntries(payload, b.entries, lookup); err != nil {
		return err
	}
	for shard, files := range b.cowPrepared.files {
		for _, fileID := range files {
			manager := b.db.valueLogReader
			registry := manager.StableResourcePinRegistry()
			identity, registered := manager.StableSegmentIdentity(fileID)
			if !registered || registry == nil || registry != b.db.valueLogIdentityPins {
				return fmt.Errorf("COW file %d lacks shared physical deletion authority", fileID)
			}
			physicalPin, err := registry.Pin(identity)
			if err != nil {
				return err
			}
			set := b.db.valueLogReader.CurrentSubsetNoRefresh(map[uint32]struct{}{fileID: {}})
			if len(set.Files) != 1 {
				b.db.valueLogReader.Release(set)
				physicalPin.Release()
				return fmt.Errorf("COW value-log file %d is not registered", fileID)
			}
			id := [1]memtable.COWResourceID{{Kind: cowResourceValueLogFile, ID: uint64(fileID)}}
			if err := b.cowPrepared.cut.prepared[shard].AttachResources(id[:], func() { manager.Release(set); physicalPin.Release() }); err != nil {
				b.db.valueLogReader.Release(set)
				physicalPin.Release()
				return err
			}
		}
	}
	return nil
}

func (b *Batch) cancelCOWPublication() *cowBatchPreparation {
	p := b.cowPrepared
	b.cowPrepared = nil
	if p != nil && p.cut != nil {
		p.cut.cancel()
	}
	return p
}

func (p *cowBatchPreparation) drainCancelled() {
	if p.cut != nil {
		p.cut.drainCancelled()
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
	if b.cowPrepared == nil || b.cowPrepared.cut == nil {
		panic("COW append succeeded without staged publication")
	}
	p := b.cowPrepared
	b.cowPrepared = nil
	old := p.cut.publish()
	var mutableBytes int64
	for _, shard := range p.cut.next.shards {
		mutableBytes += shard.size
	}
	b.db.mutableBytes.Store(mutableBytes)
	// Batch storage is no longer borrowed: C1 copied changed payloads before
	// the append. Release admission and owner before retirement or checkpoint.
	unlock()
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
