package caching

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// PublishNativeMVCCUnboundedDiagnosticV6 uses the canonical current composite
// ordering and SAME frozen-prefix handoff. The entire operation is synchronous;
// no prepared allocator candidate or publication turn survives this return.
func (db *DB) PublishNativeMVCCUnboundedDiagnosticV6(prepared *backenddb.NativeMVCCCurrentDeleteV6) (retErr error) {
	if db == nil || db.cow == nil || prepared == nil {
		return ErrCOWUnsupported
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if db.closing.Load() {
		return backenddb.ErrClosed
	}
	c := db.cow
	if c.handoff != nil {
		return ErrCOWUnsupported
	}
	backend, ok := db.backend.(interface{ NativeMVCCDiagnosticAwaitV6() error })
	if !ok {
		return ErrCOWUnsupported
	}
	if err := backend.NativeMVCCDiagnosticAwaitV6(); err != nil {
		return err
	}
	// The rollover uses the original source/retirement owner and successors.
	// No ordinary flush or checkpoint is invoked.
	c.writerMu.Lock()
	rollover, err := c.prepareRollover()
	if err == nil && rollover != nil {
		rollover.install()
	}
	c.writerMu.Unlock()
	if rollover != nil {
		// Checked drain of the SAME retirement. Retained read cuts keep every
		// covered root alive until the exact post-publication handoff.
		if rollover.installed {
			for i := range rollover.retired {
				err = errors.Join(err, rollover.retired[i].DrainWithError())
			}
			if rollover.old != nil {
				err = errors.Join(err, rollover.old.drain())
			}
			if err == nil {
				rollover.scratch.Close()
			}
		} else {
			rollover.drain()
		}
	}
	if err != nil {
		return err
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, snapshot.Close()) }()
	cut := c.retainCut()
	if cut == nil {
		return backenddb.ErrClosed
	}
	lease, err := c.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFlushHandoff{}))))
	if err != nil {
		return errors.Join(err, c.releaseCut(cut))
	}
	handoff := &cowFlushHandoff{cut: cut, lease: lease}
	// This owner supplies the canonical pointer resolver while the raw merge
	// retains persistent pointers and per-entry revisions in the output.
	merged, err := snapshot.buildCOWIteratorLocked(nil, nil, false)
	if err != nil {
		lease.Close()
		return errors.Join(err, c.releaseCut(cut))
	}
	owner, ok := merged.(*cowMergeIterator)
	if !ok {
		lease.Close()
		return errors.Join(ErrCOWUnsupported, merged.Close(), c.releaseCut(cut))
	}
	// Exactly the original frozen prefix, oldest to newest. The batch copies
	// borrowed entry payloads before iterator advance; its final duplicate-key
	// normalization preserves the canonical winner without reading untouched DATA.
	delta := batch.New(nil, int(^uint(0)>>1))
	defer delta.Close()
	for _, root := range cut.frozen {
		it := root.NewIterator(nil, nil)
		metadata, ok := it.(iterator.RevisionUnsafeIterator)
		raw, rawOK := it.(iterator.UnsafeIterator)
		if !ok || !rawOK {
			err = errors.Join(ErrCOWUnsupported, it.Close())
			break
		}
		for it.Valid() {
			v, p, f, r := metadata.UnsafeEntryWithRevision()
			op := batch.Entry{Type: batch.OpPut, Key: bytes.Clone(raw.UnsafeKey()), Value: bytes.Clone(v), ValuePtr: p, IsPtr: f&node.FlagPointer != 0, Revision: r}
			if f&node.FlagTombstone != 0 {
				op.Type = batch.OpDelete
				op.Value = nil
			}
			if err = delta.SetOps([]batch.Entry{op}); err != nil {
				break
			}
			it.Next()
		}
		err = errors.Join(err, it.Error(), it.Close())
		if err != nil {
			break
		}
	}
	if err != nil {
		lease.Close()
		return errors.Join(err, merged.Close(), c.releaseCut(cut))
	}
	raw := &nativeDiagnosticMergeV6{Iterator: owner.Iterator}
	accepted, publishErr := prepared.PublishNativeMVCCUnboundedDiagnosticV6(cut.basis.snapshot, raw, delta)
	closeErr := merged.Close()
	if accepted {
		handoff.accepted = true
		c.handoff = handoff
		// SAME basis capture and exact prefix exclusion, only after actual fence.
		return errors.Join(publishErr, closeErr, db.completeCOWHandoff())
	}
	lease.Close()
	return errors.Join(publishErr, closeErr, c.releaseCut(cut))
}

// nativeDiagnosticMergeV6 preserves the original merge winner's raw entry.
// It borrows the existing admitted merge owner only for this synchronous call.
type nativeDiagnosticMergeV6 struct{ merging.Iterator }

func (it *nativeDiagnosticMergeV6) UnsafeKey() []byte   { return it.Key() }
func (it *nativeDiagnosticMergeV6) UnsafeValue() []byte { return it.Value() }
func (it *nativeDiagnosticMergeV6) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return it.Iterator.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
}
func (it *nativeDiagnosticMergeV6) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	v, p, f, _ := it.UnsafeEntryWithRevision()
	return v, p, f
}
func (it *nativeDiagnosticMergeV6) IsDeleted() bool {
	_, _, f := it.UnsafeEntry()
	return f&node.FlagTombstone != 0
}
