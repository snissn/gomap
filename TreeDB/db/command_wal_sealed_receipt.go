package db

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// commandWALSealReceipt belongs to an explicit sealed-completion request.
// Ordinary backend requests own this heap cell; bounded native requests admit
// its containing NativeSealReceipt before allocation. Commands without an
// explicit sealed request allocate no cell. Only the
// existing candidate acceptance boundary can bind its exact physical receipt.
type commandWALSealReceipt = NativeSealReceipt

func (c *NativeSealReceipt) bind(receipt rootpublication.PhysicalSealReceipt) {
	c.mu.Lock()
	if !c.receipt.Valid() {
		c.receipt = receipt
	}
	c.mu.Unlock()
}

// PrepareSealedReceipt reserves the original request's completion cell before
// command assignment. It is needed by deterministic apply and prepared APIs;
// a current AppliedLSN sample cannot manufacture this receipt later. A successful
// request transfers admission/sealed waiting to this original caller: after
// materialization returns, release all progress-critical leases then wait the
// ExactSealedReceipt before returning a sealed result.
func (intent *CommandWALIntent) PrepareSealedReceipt() error {
	if intent == nil || intent.inner.lsn != 0 {
		return ErrCommandWALRejected
	}
	if intent.inner.sealedReceipt == nil {
		intent.inner.sealedReceipt = &commandWALSealReceipt{}
	}
	return nil
}

// PrepareSealedReceiptWithAdmission reserves the explicit native request from
// its SAME original allocator before allocation. The caller must defer
// ReleaseNativeSealedReceipt on every outcome. An accepted adapter independently
// retains the original cell before assignment and ends that edge at disposal.
func (intent *CommandWALIntent) PrepareSealedReceiptWithAdmission(allocator NativeSemanticAllocator) error {
	if intent == nil || intent.inner.lsn != 0 || allocator == nil || intent.inner.sealedReceipt != nil {
		return ErrCommandWALRejected
	}
	lease, err := allocator.AdmitNativeBytes(NativeSealReceiptAllocationBytes())
	if err != nil {
		return err
	}
	cell := &NativeSealReceipt{lease: lease}
	cell.refs.Store(1)
	intent.inner.sealedReceipt = cell
	return nil
}

func (intent *CommandWALIntent) NativeSealedReceiptAdmitted() bool {
	return intent != nil && intent.inner.sealedReceipt != nil && intent.inner.sealedReceipt.lease != nil
}

// TakeNativeSealReceipt transfers exactly one additional original request edge
// to the accepted adapter. No assignment or allocation occurs here.
func (intent *CommandWALIntent) TakeNativeSealReceipt() (*NativeSealReceipt, error) {
	if intent == nil || intent.inner.lsn != 0 || intent.inner.sealedReceipt == nil || intent.inner.sealedReceipt.lease == nil {
		return nil, ErrCommandWALRejected
	}
	cell := intent.inner.sealedReceipt
	if !cell.claimed.CompareAndSwap(false, true) {
		return nil, ErrCommandWALRejected
	}
	cell.refs.Add(1)
	return cell, nil
}

// ReleaseNativeSealedReceipt ends the caller's original edge after it has copied
// the independent physical receipt. It is idempotent; assigned failure leaves
// the adapter's SAME cell/charge in terminal custody.
func (intent *CommandWALIntent) ReleaseNativeSealedReceipt() {
	if intent == nil || intent.inner.sealedReceipt == nil || intent.inner.sealedReceipt.lease == nil {
		return
	}
	cell := intent.inner.sealedReceipt
	intent.inner.sealedReceipt = nil
	refund := cell.ReleaseNativeSeal()
	cell = nil
	if refund != nil {
		refund.Close()
	}
}

func (intent *CommandWALIntent) ExactSealedReceipt() rootpublication.PhysicalSealReceipt {
	if intent == nil || intent.inner.lsn == 0 || intent.inner.sealedReceipt == nil {
		return rootpublication.PhysicalSealReceipt{}
	}
	cell := intent.inner.sealedReceipt
	cell.mu.Lock()
	receipt := cell.receipt
	cell.mu.Unlock()
	return receipt
}

func (c *NativeSealReceipt) BindAcceptedPhysicalSeal(receipt rootpublication.PhysicalSealReceipt) {
	c.bind(receipt)
}

// NativeSealTarget returns only the original preadmitted receipt cell.
func (intent *CommandWALIntent) NativeSealTarget() rootpublication.PhysicalSealTarget {
	if intent == nil || intent.inner.sealedReceipt == nil {
		return nil
	}
	return intent.inner.sealedReceipt
}
func (intent *CommandWALIntent) RequestsExactSeal() bool {
	return intent != nil && intent.inner.sealedReceipt != nil
}

// NativeSealReceipt is one explicit native WAL or NoWAL request's exact cell.
// Its caller admits AllocationBytes before allocation and keeps that independent
// lease until the returned by-value physical receipt has been copied out.
type NativeSealReceipt struct {
	mu      sync.Mutex
	receipt rootpublication.PhysicalSealReceipt
	refs    atomic.Int32
	claimed atomic.Bool
	lease   NativeAllocationReceipt
}

func NewNativeSealReceipt(lease NativeAllocationReceipt) *NativeSealReceipt {
	c := &NativeSealReceipt{lease: lease}
	c.refs.Store(2)
	return c
}

// ReleaseNativeSeal detaches the independently admitted final receipt. The
// caller clears its cell alias before completing the returned accounting edge.
func (c *NativeSealReceipt) ReleaseNativeSeal() NativeAllocationReceipt {
	if c == nil {
		return nil
	}
	refs := c.refs.Add(-1)
	if refs < 0 {
		panic("native seal request edge underflow")
	}
	if refs != 0 {
		return nil
	}
	lease := c.lease
	c.lease = nil
	c.mu.Lock()
	c.receipt = rootpublication.PhysicalSealReceipt{}
	c.mu.Unlock()
	return lease
}

func NativeSealReceiptAllocationBytes() uint64 { return uint64(unsafe.Sizeof(NativeSealReceipt{})) }
func (c *NativeSealReceipt) Receipt() rootpublication.PhysicalSealReceipt {
	c.mu.Lock()
	receipt := c.receipt
	c.mu.Unlock()
	return receipt
}
