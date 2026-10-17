package db

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// commandWALSealReceipt is admitted with an explicit sealed-completion request
// before assignment. Ordinary commands allocate no receipt cell. Only the
// existing candidate acceptance boundary can bind its exact physical receipt.
type commandWALSealReceipt struct {
	mu      sync.Mutex
	receipt rootpublication.PhysicalSealReceipt
}

func (c *commandWALSealReceipt) bind(receipt rootpublication.PhysicalSealReceipt) {
	c.mu.Lock()
	if !c.receipt.Valid() {
		c.receipt = receipt
	}
	c.mu.Unlock()
}

// PrepareSealedReceipt reserves the original request's completion cell before
// command assignment. It is needed by deterministic apply and prepared APIs;
// a current AppliedLSN sample cannot manufacture this receipt later.
func (intent *CommandWALIntent) PrepareSealedReceipt() error {
	if intent == nil || intent.inner.lsn != 0 {
		return ErrCommandWALRejected
	}
	if intent.inner.sealedReceipt == nil {
		intent.inner.sealedReceipt = &commandWALSealReceipt{}
	}
	return nil
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

func (c *commandWALSealReceipt) BindAcceptedPhysicalSeal(receipt rootpublication.PhysicalSealReceipt) {
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

// NativeSealReceipt is one explicit NoWAL sync request's exact physical cell.
// Its caller admits AllocationBytes before allocation and keeps that independent
// lease until the returned by-value physical receipt has been copied out.
type NativeSealReceipt struct {
	cell  commandWALSealReceipt
	refs  atomic.Int32
	lease NativeAllocationReceipt
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
	return lease
}

func NativeSealReceiptAllocationBytes() uint64 { return uint64(unsafe.Sizeof(NativeSealReceipt{})) }
func (c *NativeSealReceipt) BindAcceptedPhysicalSeal(receipt rootpublication.PhysicalSealReceipt) {
	c.cell.bind(receipt)
}
func (c *NativeSealReceipt) Receipt() rootpublication.PhysicalSealReceipt {
	c.cell.mu.Lock()
	receipt := c.cell.receipt
	c.cell.mu.Unlock()
	return receipt
}
