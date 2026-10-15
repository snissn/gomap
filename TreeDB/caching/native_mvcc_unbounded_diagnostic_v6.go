package caching

import (
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
// The constructor owns this containing lease from its first setup allocation;
// no publication/group lock survives a Step return.
type nativePrivateOutputV6 struct {
	db           *DB
	handoff      *cowFlushHandoff
	prepared     *backenddb.NativeMVCCCurrentDeleteV6
	rollover     nativeRolloverWorkV6
	prefix       nativeFrozenDeltaWorkV6
	closing      NativeMVCCCaptureV6
	phase        uint8
	closingPhase uint8
}

func (db *DB) StepNativeMVCCUnboundedDiagnosticOutputV6(prepared *backenddb.NativeMVCCCurrentDeleteV6, w *iterator.OrdinalScanWork) (bool, error) {
	return db.stepNativeMVCCMaterializedOutputV6(prepared, true, w)
}

// StepNativeMVCCMaterializedOutputV6 retains original rollover, ordered prefix
// operands and materialized DATA frames. It does not enter allocator prepare,
// visible install or publication. Those require the separate final caller.
func (db *DB) StepNativeMVCCMaterializedOutputV6(prepared *backenddb.NativeMVCCCurrentDeleteV6, w *iterator.OrdinalScanWork) (bool, error) {
	return db.stepNativeMVCCMaterializedOutputV6(prepared, false, w)
}
func (db *DB) stepNativeMVCCMaterializedOutputV6(prepared *backenddb.NativeMVCCCurrentDeleteV6, diagnostic bool, w *iterator.OrdinalScanWork) (bool, error) {
	if db == nil || prepared == nil || w == nil {
		return false, ErrCOWUnsupported
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	if db.closing.Load() {
		return false, backenddb.ErrClosed
	}
	if db.cow == nil {
		return prepared.StepNativeMVCCBackendMaterializedOutputV6(diagnostic, w)
	}
	c := db.cow
	var private *nativePrivateOutputV6
	if scope := prepared.NativeMVCCPrivateOutputScopeV6(); scope != nil {
		var ok bool
		private, ok = scope.(*nativePrivateOutputV6)
		if !ok || private.db != db || private.prepared != prepared {
			return false, ErrCOWUnsupported
		}
	} else {
		if c.handoff != nil {
			return false, ErrCOWUnsupported
		}
		size := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFlushHandoff{}))) +
			memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativePrivateOutputV6{})))
		// Binding is admitted before birth; its scalar write cannot then yield
		// with an allocated but unrooted private preparation.
		if !w.Reserve(4, 2*size+128) {
			return false, nil
		}
		lease, e := c.budget.AcquireExternal(size)
		if e != nil {
			return false, e
		}
		handoff := &cowFlushHandoff{lease: lease}
		private = &nativePrivateOutputV6{db: db, handoff: handoff, prepared: prepared}
		handoff.native = private
		if e := prepared.BindNativeMVCCPrivateOutputScopeV6(private); e != nil {
			handoff.native = nil
			*private = nativePrivateOutputV6{}
			*handoff = cowFlushHandoff{}
			lease.Close()
			return false, e
		}
	}
	h := private.handoff
	if h == nil || h.accepted || private.closingPhase != 0 {
		return false, ErrCOWUnsupported
	}
	for {
		switch private.phase {
		case 0:
			if c.handoff != nil && c.handoff != h {
				return false, ErrCOWUnsupported
			}
			ready, e := private.rollover.Step(db, w)
			if !ready || e != nil {
				return false, e
			}
			h.cut = private.rollover.TakeCut()
			if h.cut == nil {
				return false, ErrCOWUnsupported
			}
			// This exact immutable prefix is now original handoff custody.
			// Ordinary point writes use its successor writers. Incompatible
			// flush cannot consume a private (not accepted) prefix.
			c.handoff = h
			private.phase = 1
		case 1:
			ready, e := private.prefix.Step(db, h.cut, w)
			if !ready || e != nil {
				return false, e
			}
			private.phase = 2
		case 2:
			entries, ready, e := prepared.StepNativeMVCCMaterializedInputV6(h.cut.basis.snapshot, private.prefix.entries, w)
			if !ready || e != nil {
				return false, e
			}
			return prepared.StepNativeMVCCMaterializedOutputPolicyV6(h.cut.basis.snapshot, entries, private, diagnostic, w)
		default:
			return false, ErrCOWUnsupported
		}
	}
}

// ClosePrivateNativeOutputV6 is the ordinary synchronous driver of the SAME
// checked stages. Native callers use ClosePrivateNativeOutputStepV6 directly.
func (p *nativePrivateOutputV6) ClosePrivateNativeOutputV6() (*memtable.COWExternalLease, error) {
	w := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	ready, lease, e := p.ClosePrivateNativeOutputStepV6(&w)
	if !ready && e == nil {
		return nil, ErrCOWUnsupported
	}
	return lease, e
}
func (p *nativePrivateOutputV6) ClosePrivateNativeOutputStepV6(w *iterator.OrdinalScanWork) (bool, *memtable.COWExternalLease, error) {
	if p == nil || p.db == nil {
		return true, nil, nil
	}
	db := p.db
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	c, h := db.cow, p.handoff
	if c == nil || h == nil || h.accepted || (c.handoff != nil && c.handoff != h) {
		return false, nil, ErrCOWUnsupported
	}
	if p.closingPhase == 0 {
		p.closingPhase = 1
	}
	if p.closingPhase == 1 {
		ready, lease, e := p.prefix.CloseStep(w)
		if !ready || e != nil {
			return false, nil, e
		}
		if lease != nil {
			lease.Close()
		}
		p.closingPhase = 2
	}
	if p.closingPhase == 2 {
		ready, e := p.rollover.CloseStep(w)
		if !ready || e != nil {
			return false, nil, e
		}
		p.closingPhase = 3
	}
	if p.closingPhase == 3 {
		if h.cut != nil && p.closing.composite.cut == nil {
			p.closing.composite.cut, p.closing.composite.cache = h.cut, c
			p.closing.Snapshot = h.cut.basis.snapshot
			h.cut = nil
		}
		if p.closing.composite.cut != nil {
			ready, e := p.closing.closeNativeCOWCaptureV6(w)
			if !ready || e != nil {
				return false, nil, e
			}
		}
		p.closingPhase = 4
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*p))+2*uint64(unsafe.Sizeof(*h))) || !h.lease.AdmitRetentionClose(w) {
		return false, nil, nil
	}
	lease := h.lease
	if c.handoff == h {
		c.handoff = nil
	}
	*h = cowFlushHandoff{}
	*p = nativePrivateOutputV6{}
	return true, lease, nil
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
	delta := batch.New(nil, int(^uint(0)>>1))
	defer delta.Close()
	if err = delta.SetOps(h.native.prefix.entries); err != nil {
		return errors.Join(err, merged.Close())
	}
	accepted, publishErr := prepared.PublishNativeMVCCUnboundedDiagnosticV6(h.cut.basis.snapshot, raw, delta)
	closeErr := merged.Close()
	if accepted {
		h.accepted = true
		unlimited := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
		ready, lease, e := h.native.prefix.CloseStep(&unlimited)
		if !ready || e != nil {
			return errors.Join(publishErr, closeErr, e)
		}
		if lease != nil {
			lease.Close()
		}
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
