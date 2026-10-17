package db

import (
	"sync"

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
