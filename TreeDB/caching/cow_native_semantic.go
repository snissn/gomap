package caching

import (
	"context"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// NativeSemanticCapture retains the exact raw+semantic+disk cut, never a new
// backend capture. State and Snapshot are borrowed until Close. This internal
// capability is admitted before allocation and belongs to this public owner.
type NativeSemanticCapture struct {
	cache *cowCache
	cut   *cowReadCut
	lease *memtable.COWExternalLease
}

func (db *DB) CaptureNativeSemanticState(owner *transactionowner.Owner) (*NativeSemanticCapture, error) {
	if db == nil || db.cow == nil || owner == nil || db.cow.txnOwner != owner {
		return nil, ErrCOWUnsupported
	}
	lease, err := db.cow.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeSemanticCapture{}))))
	if err != nil {
		return nil, err
	}
	cut := db.cow.retainCut()
	if cut == nil {
		lease.Close()
		return nil, backenddb.ErrClosed
	}
	return &NativeSemanticCapture{cache: db.cow, cut: cut, lease: lease}, nil
}
func (c *NativeSemanticCapture) State() backenddb.NativeSemanticState { return c.cut.semanticRootSet }
func (c *NativeSemanticCapture) Snapshot() *backenddb.Snapshot        { return c.cut.basis.snapshot }
func (c *NativeSemanticCapture) LogicalSequence() uint64              { return c.cut.logicalSeq }
func (c *NativeSemanticCapture) AdmitNativeBytes(bytes uint64) (backenddb.NativeAllocationReceipt, error) {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil, backenddb.ErrClosed
	}
	return c.cache.budget.AdmitNativeBytes(bytes)
}

func (c *NativeSemanticCapture) NativeAllocator() backenddb.NativeSemanticAllocator {
	if c == nil || c.cut == nil || c.cache == nil {
		return nil
	}
	return c.cache.budget
}
func (c *NativeSemanticCapture) Close() error {
	if c == nil || c.cut == nil {
		return nil
	}
	cut, cache, lease := c.cut, c.cache, c.lease
	c.cut, c.cache, c.lease = nil, nil, nil
	err := cache.releaseCut(cut)
	lease.Close() // final containing-owner access follows alias scrubbing
	return err
}

// NativeSemanticState borrows the immutable state of this original snapshot.
func (s *Snapshot) NativeSemanticState() backenddb.NativeSemanticState {
	if s == nil || s.cowCut == nil {
		return nil
	}
	return s.cowCut.semanticRootSet
}

// cowNativeOwner is independently admitted before assignment, never reused with
// Batch storage. Accepted custody retains only sparse native plans/assets.
type cowNativeOwner struct {
	cache             *cowCache
	plan              *backenddb.NativePreparedPublication
	custody           backenddb.NativeSemanticCustody
	opening           *cowReadCut
	lease             *memtable.COWExternalLease
	failed            error
	failedPreparation *cowBatchPreparation
	failedResult      backenddb.NativeFinalizedPublication
	sealTarget        rootpublication.PhysicalSealTarget
	sealReceipt       *backenddb.NativeSealReceipt
}

// The generic callback cannot carry containing-owner refunds safely. The
// original queue and cancellation paths consume the by-value completion seam.
func (o *cowNativeOwner) ReleaseAccepted() error { return rootpublication.ErrPublisherProtocol }
func (o *cowNativeOwner) ReleaseAcceptedCompletion() (rootpublication.AcceptedCleanupCompletion, error) {
	if o.failed != nil {
		return rootpublication.AcceptedCleanupCompletion{}, o.failed
	}
	// All paths detach the opening cut BEFORE ownership reaches the queue or this
	// destructor. Neither success nor cancellation may use o after this refund.
	if o.opening != nil || o.plan != nil {
		panic("native custody transferred before private aliases detached")
	}
	cache, custody, lease, seal := o.cache, o.custody, o.lease, o.sealReceipt
	o.cache, o.custody, o.lease, o.sealReceipt, o.sealTarget = nil, nil, nil, nil, nil
	if custody != nil {
		record := custody.ReleaseNativeCustody()
		cache.semanticMu.Lock()
		cache.enqueueSemanticLocked(record)
		cache.semanticMu.Unlock()
		cache.wakeSemanticCleanup()
	}
	var sealRefund backenddb.NativeAllocationReceipt
	if seal != nil {
		sealRefund = seal.ReleaseNativeSeal()
		seal = nil
	}
	return rootpublication.AcceptedCleanupCompletion{Refund: lease, RequestRefund: sealRefund}, nil
}

// SetNativeSemanticPublication attaches a preadmitted native composite to the
// ordinary batch authority. On error the caller retains State and Custody. On
// success the batch consumes State/Custody on installation or cancellation;
// Basis stays borrowed from the caller's retained capture until Write returns.
func (b *Batch) SetNativeSemanticPublication(owner *transactionowner.Owner, native *backenddb.NativePreparedPublication) error {
	if b == nil || b.db == nil || b.db.cow == nil || b.cowState == nil || b.closed || b.cowState.err != nil || b.cowState.native != nil || native == nil || (native.State == nil && native.Finalizer == nil) || (native.Finalizer != nil && native.Custody == nil) || owner == nil || b.db.cow.txnOwner != owner {
		return ErrCOWUnsupported
	}
	if native.Finalizer != nil {
		if _, ok := native.Custody.(backenddb.NativeAssignedFailureCustody); !ok || native.Resources == nil {
			return ErrCOWUnsupported
		}
	}
	if err := b.AuthorizeCOWAppend(owner); err != nil {
		return err
	}
	c := b.db.cow
	lease, err := c.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowNativeOwner{}))))
	if err != nil {
		return err
	}
	opening := c.retainCut()
	if opening == nil {
		lease.Close()
		return backenddb.ErrClosed
	}
	// Raw effects retain ordinary owned/canonical pointer/revision preparation.
	if err = b.cowSetOps(native.Entries); err != nil {
		_ = c.releaseCut(opening)
		lease.Close()
		return err
	}
	b.cowState.native = &cowNativeOwner{cache: c, plan: native, custody: native.Custody, opening: opening, lease: lease}
	native.Custody = nil
	return nil
}

func (b *Batch) releaseNativePublication() {
	owner := b.cowState.native
	if owner == nil {
		return
	}
	b.cowState.native = nil
	cache, opening, native := owner.cache, owner.opening, owner.plan
	owner.opening, owner.plan = nil, nil
	if native != nil && native.State != nil {
		record := native.State.Release()
		native.State = nil
		cache.semanticMu.Lock()
		cache.enqueueSemanticLocked(record)
		cache.semanticMu.Unlock()
		cache.wakeSemanticCleanup()
	}
	// Opening survives until BOTH private state and sparse custody have queued.
	// ReleaseAccepted refunds owner; no caller reads its fields afterward.
	custody := owner.custody
	owner.custody = nil
	if custody != nil {
		record := custody.ReleaseNativeCustody()
		cache.semanticMu.Lock()
		cache.enqueueSemanticLocked(record)
		cache.semanticMu.Unlock()
		cache.wakeSemanticCleanup()
	}
	_ = cache.releaseCut(opening)
	completion, err := owner.ReleaseAcceptedCompletion()
	owner = nil
	if err != nil {
		panic(err)
	}
	completion.Complete()
}

// parkAssignedNative transfers the SAME original adapter to its prebound FIFO
// entry. Original raw rollback drains after gates end while opening still holds
// the physical basis. Only actual failed rollback retains that original owner;
// normal assigned failure parks sparse native custody without raw neighbors.
func (b *Batch) parkAssignedNative(p *cowBatchPreparation, err error) (*cowNativeOwner, *cowReadCut) {
	owner := b.cowState.native
	if owner == nil {
		return nil, nil
	}
	b.cowState.native = nil
	owner.failed = err
	result := owner.failedResult
	owner.failedResult = backenddb.NativeFinalizedPublication{}
	if ledger, ok := owner.custody.(backenddb.NativeAssignedFailureCustody); ok {
		ledger.RetainAssignedNativeFailure(result, err)
	}
	// An owned State returned before a later carrier failure is distinct from
	// private Next. Sparse original ledger already owns its partial resources.
	if result.State != nil {
		record := result.State.Release()
		result.State = nil
		b.db.cow.semanticMu.Lock()
		b.db.cow.enqueueSemanticLocked(record)
		b.db.cow.semanticMu.Unlock()
		b.db.cow.wakeSemanticCleanup()
	}
	opening := owner.opening
	owner.opening, owner.plan = nil, nil
	if p != nil {
		if p.mutation.Valid() {
			p.mutation.Cancel()
			p.mutation = txnMutationOwner{}
			b.db.cow.updateTxnStats()
		}
		if p.cut != nil {
			p.cut.native = nil
			p.cut.cancel()
		}
	}
	b.cowState.prepared = nil
	b.cowState.accepted = rootpublication.AcceptedTicket{}
	return owner, opening
}

func (b *Batch) drainAssignedNative(owner *cowNativeOwner, opening *cowReadCut, p *cowBatchPreparation, err error) error {
	if p != nil {
		if cleanupErr := p.drainCancelled(); cleanupErr != nil {
			// SAME original failed physical fields/charge remain, never retried.
			owner.failedPreparation = p
			err = joinCOWCleanupError(err, cleanupErr)
		}
	}
	if opening != nil {
		err = joinCOWCleanupError(err, b.db.cow.releaseCut(opening))
	}
	return err
}

// CaptureNativeCut implements the one backend-bound complete-cut capability.
func (db *DB) CaptureNativeCut() (backenddb.NativeSemanticCapture, error) {
	if db == nil || db.cow == nil {
		return nil, ErrCOWUnsupported
	}
	return db.CaptureNativeSemanticState(db.cow.txnOwner)
}

// WriteNativePublication uses the ordinary original batch/P -> raw order. The
// typed finalizer runs privately after the sole canonical frame is assigned;
// logical install follows only a complete successful native+raw preparation.
func (db *DB) WriteNativePublication(native *backenddb.NativePreparedPublication, intent *backenddb.CommandWALIntent, sync bool) (result backenddb.NativePublicationResult, err error) {
	if db == nil {
		return result, backenddb.ErrClosed
	}
	backend, ok := db.backend.(*backenddb.DB)
	if !ok || db.cow == nil || native == nil || (intent != nil && intent.AssignedLSN() != 0) {
		return result, ErrCOWUnsupported
	}
	owner := db.cow.txnOwner
	if intent == nil {
		if db.externalCommandWAL || native.Finalizer != nil {
			return result, ErrCOWUnsupported
		}
		if err = backend.PreflightPrivateCOWCommand(owner); err != nil {
			return result, err
		}
		b := db.NewBatchWithSize(len(native.Entries))
		var sealed *backenddb.NativeSealReceipt
		defer func() {
			err = joinCOWCleanupError(err, b.Close())
			if sealed != nil {
				if !result.Consumed {
					if refund := sealed.ReleaseNativeSeal(); refund != nil {
						panic("untransferred native seal had one edge")
					}
				}
				refund := sealed.ReleaseNativeSeal()
				sealed = nil
				if refund != nil {
					refund.Close()
				}
			}
		}()
		if sync {
			lease, admissionErr := db.cow.budget.AdmitNativeBytes(backenddb.NativeSealReceiptAllocationBytes())
			if admissionErr != nil {
				return result, admissionErr
			}
			sealed = backenddb.NewNativeSealReceipt(lease)
		}
		if err = b.SetNativeSemanticPublication(owner, native); err != nil {
			return result, err
		}
		result.Consumed = true
		if sealed != nil {
			b.cowState.native.sealTarget = sealed
			b.cowState.native.sealReceipt = sealed
			err = b.WriteSync()
		} else {
			err = b.Write()
		}
		receipt := b.COWReceipt()
		result.LogicalInstalled, result.LogicalSequence = receipt.LogicalInstalled, receipt.LogicalSeq
		if err == nil && sealed != nil {
			result.Seal = sealed.Receipt()
			err = result.Seal.Wait(context.Background())
		}
		return result, err
	}
	if !db.externalCommandWAL {
		return result, ErrCOWUnsupported
	}
	if err = backend.ValidateNativeCommandIntent(owner, intent); err != nil {
		return result, err
	}
	b := db.NewBatchWithSize(len(native.Entries))
	defer func() { err = joinCOWCleanupError(err, b.Close()) }()
	if err = b.SetNativeSemanticPublication(owner, native); err != nil {
		return result, err
	}
	result.Consumed = true
	b.cowState.native.sealTarget = intent.NativeSealTarget()
	err = b.WriteAfterCommandWALAppendWithPreparedRevision(sync, func() error {
		if err := b.AcquireCOWAppendGuard(owner); err != nil {
			return err
		}
		guard := b.COWAppendGuard()
		if err := backend.FinalizeNativeRawUnderGuard(guard, b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, len(b.entries)); err != nil {
			return err
		}
		if err := b.cowState.accepted.BindPreassignedCustody(b.cowState.native); err != nil {
			return err
		}
		lsn, appendErr := guard.Append(intent, sync)
		receiptErr := b.RecordCOWAppend(owner, lsn, appendErr, sync)
		if appendErr != nil {
			return appendErr
		}
		if receiptErr != nil {
			return receiptErr
		}
		if lsn == 0 {
			return backenddb.ErrCommandWALRejected
		}
		if native.Finalizer != nil {
			finalized, err := backend.FinalizeNativeUnderGuard(guard, intent, native)
			if err != nil {
				b.cowState.native.failedResult = finalized
				return err
			}
			cut := b.cowState.prepared.cut
			if cut.next.semanticRootSet != nil {
				// Its original complete-version edge ends only after the admitted sparse
				// plan owns all replacement resources. Opening still retains the old cut.
				record := cut.next.semanticRootSet.Release()
				db.cow.semanticMu.Lock()
				db.cow.enqueueSemanticLocked(record)
				db.cow.semanticMu.Unlock()
				db.cow.wakeSemanticCleanup()
			}
			cut.next.semanticRootSet = finalized.State
			native.Projections = finalized.Projections
		}
		return nil
	})
	receipt := b.COWReceipt()
	result.AssignedLSN, result.LogicalSequence = receipt.AssignedLSN, receipt.LogicalSeq
	result.Assigned, result.Uncertain, result.LogicalInstalled = receipt.Assigned, receipt.Uncertain, receipt.LogicalInstalled
	result.WALCompleted, result.WALStable = receipt.WALCompleted, receipt.WALStable
	if err == nil && intent.RequestsExactSeal() {
		err = db.Checkpoint() // all producer/raw/teardown progress gates have ended
		if err == nil {
			result.Seal = intent.ExactSealedReceipt()
			err = result.Seal.Wait(context.Background())
		}
	}
	return result, err
}
