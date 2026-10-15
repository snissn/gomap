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

// nativePrivateOutputV6 is the SAME handoff's private pre-acceptance state.
// Its cut and normalized batch remain owned by the original handoff lease.
// Prefix collection/normalization and final adoption are explicit synchronous
// diagnostic stages; only materialized output Step uses the supplied work.
type nativePrivateOutputV6 struct {
	db          *DB
	handoff     *cowFlushHandoff
	prepared    *backenddb.NativeMVCCCurrentDeleteV6
	delta       *batch.Batch
	outputDelta *batch.Batch
	entries     []batch.Entry
}

func (db *DB) StepNativeMVCCUnboundedDiagnosticOutputV6(prepared *backenddb.NativeMVCCCurrentDeleteV6, w *iterator.OrdinalScanWork) (bool, error) {
	if db == nil || db.cow == nil || prepared == nil {
		return false, ErrCOWUnsupported
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	c := db.cow
	if db.closing.Load() {
		return false, backenddb.ErrClosed
	}
	if c.handoff == nil {
		db.writeMu.Lock()
		defer db.writeMu.Unlock()
		backend, ok := db.backend.(interface{ NativeMVCCDiagnosticAwaitV6() error })
		if !ok {
			return false, ErrCOWUnsupported
		}
		if err := backend.NativeMVCCDiagnosticAwaitV6(); err != nil {
			return false, err
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
			return false, err
		}
		cut := c.retainCut()
		if cut == nil {
			return false, backenddb.ErrClosed
		}
		lease, err := c.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFlushHandoff{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativePrivateOutputV6{}))))
		if err != nil {
			return false, errors.Join(err, c.releaseCut(cut))
		}
		handoff := &cowFlushHandoff{cut: cut, lease: lease}
		// Exactly the original frozen prefix, oldest to newest. The batch copies
		// borrowed entry payloads before iterator advance; its final duplicate-key
		// normalization preserves the canonical winner without reading untouched DATA.
		delta := batch.New(nil, int(^uint(0)>>1))
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
			delta.Close()
			return false, errors.Join(err, c.releaseCut(cut))
		}
		if err = prepared.AppendNativeMVCCDiagnosticDeletesV6(delta); err != nil {
			delta.Close()
			lease.Close()
			return false, errors.Join(err, c.releaseCut(cut))
		}
		// Normalize once in the explicitly synchronous setup, not inside an admitted
		// construction Step. The original batch owns all copied delta payloads.
		outputDelta, prepareErr := prepared.PrepareNativeMVCCMaterializedInputV6(cut.basis.snapshot, delta)
		if prepareErr != nil {
			delta.Close()
			lease.Close()
			return false, errors.Join(prepareErr, c.releaseCut(cut))
		}
		entries, ranges := outputDelta.ApplyPlan()
		if len(ranges) != 0 {
			delta.Close()
			lease.Close()
			return false, errors.Join(ErrCOWUnsupported, c.releaseCut(cut))
		}

		private := &nativePrivateOutputV6{db: db, handoff: handoff, prepared: prepared, delta: delta, outputDelta: outputDelta, entries: entries}
		handoff.native = private
		c.handoff = handoff
	}
	h := c.handoff
	if h.accepted || h.native == nil || h.native.prepared != prepared {
		return false, ErrCOWUnsupported
	}
	return prepared.StepNativeMVCCMaterializedOutputV6(h.cut.basis.snapshot, h.native.entries, h.native, w)
}

// ClosePrivateNativeOutputV6 does not authorize prefix removal. Its original
// lease is returned after producer aliases are detached; caller clears its
// scope alias before refunding the containing handoff/preparation.
func (p *nativePrivateOutputV6) ClosePrivateNativeOutputV6() (*memtable.COWExternalLease, error) {
	if p == nil || p.db == nil {
		return nil, nil
	}
	db := p.db
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	c := db.cow
	h := p.handoff
	if c == nil || h == nil || c.handoff != h || h.accepted {
		return nil, ErrCOWUnsupported
	}
	if err := c.releaseCut(h.cut); err != nil {
		return nil, err
	}
	p.delta.Close()
	p.outputDelta.Close()
	lease := h.lease
	c.handoff = nil
	*h = cowFlushHandoff{}
	*p = nativePrivateOutputV6{}
	return lease, nil
}

// PublishNativeMVCCUnboundedDiagnosticV6 adopts the exact prepared materialized
// DATA through SAME constructor/current validation/fence/Consume/checked Finish.
// This final stretch is synchronous and has NO Q32 claim.
func (db *DB) PublishNativeMVCCUnboundedDiagnosticV6(prepared *backenddb.NativeMVCCCurrentDeleteV6) (retErr error) {
	if db == nil || db.cow == nil || prepared == nil {
		return ErrCOWUnsupported
	}
	// Compatibility callers drive the SAME producer synchronously.
	if db.cow.handoff == nil {
		w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		ready, err := db.StepNativeMVCCUnboundedDiagnosticOutputV6(prepared, w)
		if err != nil {
			return err
		}
		if !ready {
			return ErrCOWUnsupported
		}
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	c := db.cow
	h := c.handoff
	if h == nil || h.accepted || h.native == nil || h.native.prepared != prepared {
		return ErrCOWUnsupported
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, snapshot.Close()) }()
	merged, err := snapshot.buildCOWIteratorLocked(nil, nil, false)
	if err != nil {
		return err
	}
	owner, ok := merged.(*cowMergeIterator)
	if !ok {
		return errors.Join(ErrCOWUnsupported, merged.Close())
	}
	raw := &nativeDiagnosticMergeV6{Iterator: owner.Iterator}
	accepted, publishErr := prepared.PublishNativeMVCCUnboundedDiagnosticV6(h.cut.basis.snapshot, raw, h.native.delta)
	closeErr := merged.Close()
	if accepted {
		h.accepted = true
		h.native.delta.Close()
		h.native.outputDelta.Close()
		*h.native = nativePrivateOutputV6{}
		h.native = nil
		prepared.DetachNativeMVCCPrivateOutputScopeV6()
		return errors.Join(publishErr, closeErr, db.completeCOWHandoff())
	}
	return errors.Join(publishErr, closeErr)
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
