package db

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// NativeMVCCPrivateOutputScopeV6 is the original cache handoff's checked
// pre-acceptance cleanup edge. It never authorizes removal of a frozen prefix.
type NativeMVCCPrivateOutputScopeV6 interface {
	ClosePrivateNativeOutputV6() (*memtable.COWExternalLease, error)
}

// StepNativeMVCCMaterializedOutputV6 advances only private materialized DATA.
// The cache keeps the exact covered prefix/basis and sorted delta alive. No
// candidate, BuildGroup, allocator transaction pointer or lock is retained.
// Initial diagnostic prefix collection and final publication remain explicitly
// synchronous; only this output producer claims admitted continuation work.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCMaterializedOutputV6(basis *Snapshot, entries []batch.Entry, scope NativeMVCCPrivateOutputScopeV6, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.phase != 3 || basis == nil || scope == nil || c.snapshot.idx != basis.idx {
		return false, ErrNativeMVCCSnapshotWork
	}
	if c.outputScope != nil && c.outputScope != scope {
		return false, ErrNativeMVCCSnapshotWork
	}
	c.outputScope = scope
	if c.output == nil {
		if !c.outputInputReady {
			return false, ErrNativeMVCCSnapshotWork
		}
		output, ready, e := zipper.NewMaterializedWriteWork(basis.idx.zipper, basis.idx.metadata, c.outputInput.Base.Ref, entries, w)
		if output != nil {
			c.output = output
			output.EnableUnboundedLeafPersistenceDiagnosticV6()
			c.outputSequence = c.outputInput.Sequence + 1
			output.BindMaterializedInputAccountingV6(&c.outputInput)
			if c.outputReader == nil {
				return false, ErrNativeMVCCSnapshotWork
			}
			c.outputReader.Snapshot = basis
			output.BindFrameReaderV6(c.outputReader)
		}
		if !ready || e != nil {
			return false, e
		}
	}
	if c.outputAdoptionReady {
		return true, nil
	}
	ready, e := c.output.Step(basis.idx.allocator, w)
	if ready && e == nil {
		// Construction has completed in the live allocator. Adoption is a
		// separate stage: no allocator candidate or publication lock exists.
		c.outputAdoptionReady = true
		return false, nil
	}
	return ready, e
}

// AppendNativeMVCCDiagnosticDeletesV6 prepares the original captured-prefix
// batch before retained output construction. Fresh winner/floor checks still
// occur at the actual final publication cut, through the original Request.
func (c *NativeMVCCCurrentDeleteV6) AppendNativeMVCCDiagnosticDeletesV6(delta *batch.Batch) error {
	if c == nil || c.phase != 3 || delta == nil {
		return ErrNativeMVCCSnapshotWork
	}
	records := c.records
	if len(records) == 0 {
		records = []node.LeafEntry{c.record}
	}
	for i := range records {
		if e := delta.SetOps([]batch.Entry{{Type: batch.OpDelete, Key: bytes.Clone(records[i].Key), Revision: records[i].Revision}}); e != nil {
			return e
		}
	}
	return nil
}
func (c *NativeMVCCCurrentDeleteV6) DetachNativeMVCCPrivateOutputScopeV6() { c.outputScope = nil }

func (c *NativeMVCCCurrentDeleteV6) PrepareNativeMVCCMaterializedInputV6(basis *Snapshot, prefix *batch.Batch) (*batch.Batch, error) {
	if c == nil || c.phase != 3 || basis == nil || c.snapshot.idx != basis.idx || c.outputInputReady {
		return nil, ErrNativeMVCCSnapshotWork
	}
	output, state, err := basis.idx.zipper.PrepareMaterializedInputV6(basis.treeRoot, prefix)
	if err == nil {
		c.outputInput = state
		c.outputInputReady = true
	}
	return output, err
}

func (c *NativeMVCCCurrentDeleteV6) BindNativeMVCCOutputReaderV6(reader *NativeMVCCFrameReaderV6) {
	c.outputReader = reader
}
