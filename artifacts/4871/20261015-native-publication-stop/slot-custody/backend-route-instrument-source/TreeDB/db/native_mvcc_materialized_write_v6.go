package db

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/zipper"
	"unsafe"
)

// NativeMVCCPrivateOutputScopeV6 is the original cache handoff's checked
// pre-acceptance cleanup edge. It never authorizes removal of a frozen prefix.
type NativeMVCCPrivateOutputScopeV6 interface {
	ClosePrivateNativeOutputV6() (*memtable.COWExternalLease, error)
}
type NativeMVCCPrivateOutputStepScopeV6 interface {
	ClosePrivateNativeOutputStepV6(*iterator.OrdinalScanWork) (bool, *memtable.COWExternalLease, error)
}

// The preborn constructor owns partial setup as soon as the first private
// allocation succeeds. No cache-global registry or independently issued owner.
func (c *NativeMVCCCurrentDeleteV6) NativeMVCCPrivateOutputScopeV6() NativeMVCCPrivateOutputScopeV6 {
	if c == nil {
		return nil
	}
	return c.outputScope
}

// BindNativeMVCCPrivateOutputScopeV6 is the fixed transfer following the cache's
// admitted containing birth. The caller funds this scalar with that allocation
// before creating any object, so refusal cannot orphan a new private owner.
func (c *NativeMVCCCurrentDeleteV6) BindNativeMVCCPrivateOutputScopeV6(scope NativeMVCCPrivateOutputScopeV6) error {
	if c == nil || c.phase != 3 || scope == nil {
		return ErrNativeMVCCSnapshotWork
	}
	if c.outputScope != nil && c.outputScope != scope {
		return ErrNativeMVCCSnapshotWork
	}
	c.outputScope = scope
	return nil
}

// Input metadata and borrowed payloads stay stationary through output frames.
// Every selected row belongs to this original constructor's verified vector.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCMaterializedInputV6(basis *Snapshot, prefix []batch.Entry, w *iterator.OrdinalScanWork) ([]batch.Entry, bool, error) {
	if c == nil || c.phase != 3 || basis == nil || c.snapshot.idx != basis.idx || c.outputReader == nil {
		return nil, false, ErrNativeMVCCSnapshotWork
	}
	if c.inputWork == nil {
		records := c.records
		if len(records) == 0 {
			records = unsafe.Slice(&c.record, 1)
		}
		c.outputReader.Snapshot = basis
		input, ready, e := zipper.NewMaterializedInputWorkV6(basis.idx.metadata, c.outputReader, basis.treeRoot, prefix, records, w)
		if input != nil {
			c.inputWork = input
		}
		if !ready || e != nil {
			return nil, false, e
		}
	}
	ready, e := c.inputWork.Step(w)
	if !ready || e != nil {
		return nil, false, e
	}
	entries, state, ready := c.inputWork.Result()
	if ready {
		c.outputInput = state
		c.outputInputReady = true
	}
	return entries, ready, nil
}
func (c *NativeMVCCCurrentDeleteV6) closeMaterializedInputWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c.inputWork == nil {
		return true, nil
	}
	ready, owner, charge, e := c.inputWork.CloseStep(w)
	if !ready || e != nil {
		return false, e
	}
	c.inputWork = nil
	owner.RemovePending(charge)
	return true, nil
}

// StepNativeMVCCMaterializedOutputV6 advances only private materialized DATA.
// The cache keeps the exact covered prefix/basis and sorted delta alive. No
// candidate, BuildGroup, allocator transaction pointer or lock is retained.
// Initial diagnostic prefix collection and final publication remain explicitly
// synchronous; only this output producer claims admitted continuation work.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCMaterializedOutputV6(basis *Snapshot, entries []batch.Entry, scope NativeMVCCPrivateOutputScopeV6, w *iterator.OrdinalScanWork) (bool, error) {
	return c.StepNativeMVCCMaterializedOutputPolicyV6(basis, entries, scope, true, w)
}

// A diagnostic choice is fixed before the first output frame is born. Bounded
// callers never acquire the ordinary unmetered append through this adapter.
// StepNativeMVCCBackendMaterializedOutputV6 uses the already retained exact
// backend Snapshot when capture found the maintained empty ordinary cache.
// There is no frozen cache prefix or additional handoff edge on this route;
// the SAME constructor owns input, private DATA and cancellation throughout.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCBackendMaterializedOutputV6(diagnostic bool, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.source != nil || c.snapshot == nil || c.outputScope != nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	entries, ready, err := c.StepNativeMVCCMaterializedInputV6(c.snapshot, nil, w)
	if !ready || err != nil {
		return false, err
	}
	return c.StepNativeMVCCMaterializedOutputPolicyV6(c.snapshot, entries, nil, diagnostic, w)
}

func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCMaterializedOutputPolicyV6(basis *Snapshot, entries []batch.Entry, scope NativeMVCCPrivateOutputScopeV6, diagnostic bool, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.phase != 3 || basis == nil || (scope == nil && c.source != nil) || c.snapshot.idx != basis.idx {
		return false, ErrNativeMVCCSnapshotWork
	}
	if c.outputScope != nil && c.outputScope != scope {
		return false, ErrNativeMVCCSnapshotWork
	}
	c.outputScope = scope
	if c.output != nil && c.outputDiagnostic != diagnostic {
		return false, ErrNativeMVCCSnapshotWork
	}
	if c.output == nil {
		if !c.outputInputReady {
			return false, ErrNativeMVCCSnapshotWork
		}
		output, ready, e := zipper.NewMaterializedWriteWork(basis.idx.zipper, basis.idx.metadata, c.outputInput.Base.Ref, entries, w)
		if output != nil {
			c.output = output
			c.outputDiagnostic = diagnostic
			if diagnostic {
				output.EnableUnboundedLeafPersistenceDiagnosticV6()
			}
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
		retired, allocated := c.outputInput.RetiredCount, 0
		c.output.VisitPages(func(id, old uint64) {
			if id != 0 {
				allocated++
			}
			if old != 0 {
				retired++
				fmt.Printf("NATIVE_GEOMETRY retired_id=%d\n", old)
			}
		})
		fmt.Printf("NATIVE_GEOMETRY stage=materialized-output retired_ids=%d installed_output_pages=%d prefix_entries=%d modified_allocator_paths=UNAVAILABLE metadata_images=UNAVAILABLE successor_classes=UNAVAILABLE\n", retired, allocated, len(entries))

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
