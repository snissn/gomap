package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"slices"
	"strings"
	"unsafe"
)

// This is temporary construction storage, not another publication/retirement
// authority. One canonical whole-directory validation creates independently
// admitted record copies, reused by all entries of this exact retained root.
// Each string allocation is admitted before the callback copies iterator bytes.
type resourceImportRecord struct {
	ownerKey   string
	obligation StableLogicalObligation
}
type resourceImportDirectory struct {
	closeAttempted bool
	closeErr       error
	allocation     *resourceAllocation
	next           *resourceImportDirectory
	directory      *DependencyDirectoryV2
	records        []resourceImportRecord
}

func (source *resourceImportSource) borrowDirectory(owner *retainedalloc.Owner, directory *DependencyDirectoryV2) (*resourceImportDirectory, error) {
	if directory == nil {
		return nil, nil
	}
	for d := source.directories; d != nil; d = d.next {
		if d.directory == directory {
			return d, nil
		}
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceImportDirectory{})))
	if err := diagnosticChargeAdd(&charge, directory.ref.LogicalCount, uint64(unsafe.Sizeof(resourceImportRecord{}))); err != nil {
		return nil, err
	}
	maxInt := uint64(int(^uint(0) >> 1))
	if directory.ref.LogicalCount > maxInt {
		return nil, retainedalloc.ErrCapacity
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	if err = directory.Retain(); err != nil {
		allocation.drop()
		allocation.refund()
		return nil, err
	}
	d := &resourceImportDirectory{allocation: allocation, directory: directory, records: make([]resourceImportRecord, 0, int(directory.ref.LogicalCount))}
	err = directory.walkBorrowed(owner, func(key []byte, o StableLogicalObligation) error {
		if len(d.records) == cap(d.records) {
			return ErrDependencyManifestFormat
		}
		var addition uint64
		if err := diagnosticChargeAdd(&addition, 1, uint64(len(key))); err != nil {
			return err
		}
		for _, text := range [...]string{o.Class, o.Kind, o.Namespace, string(o.Reachability)} {
			n := retainedalloc.AllocationCharge(uint64(len(text)))
			if n > ^uint64(0)-addition {
				return retainedalloc.ErrCapacity
			}
			addition += n
		}
		if addition > ^uint64(0)-allocation.charge {
			return retainedalloc.ErrCapacity
		}
		if err := owner.AddPending(addition); err != nil {
			return err
		}
		allocation.charge += addition
		o.Class = strings.Clone(o.Class)
		o.Kind = strings.Clone(o.Kind)
		o.Namespace = strings.Clone(o.Namespace)
		o.Reachability = ReachabilityField(strings.Clone(string(o.Reachability)))
		d.records = append(d.records, resourceImportRecord{ownerKey: string(key), obligation: o})
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, d.close())
	}
	slices.SortFunc(d.records, func(a, b resourceImportRecord) int {
		if n := strings.Compare(a.ownerKey, b.ownerKey); n != 0 {
			return n
		}
		return compareResourceObligation(a.obligation, b.obligation)
	})
	d.next = source.directories
	source.directories = d
	return d, nil
}
func (d *resourceImportDirectory) close() error {
	if d == nil {
		return nil
	}
	if d.closeAttempted {
		return d.closeErr
	}
	d.closeAttempted = true
	if d.directory != nil {
		consumed, err := d.directory.ReleaseOutcome()
		if consumed {
			d.directory = nil
		}
		if err == nil && !consumed {
			err = ErrResourceOwnership
		}
		if err != nil {
			if d.allocation != nil {
				err = d.allocation.cleanupFailed(d, err)
			}
			d.closeErr = err
			return err
		}
	}
	clear(d.records)
	d.records = nil
	d.next = nil
	allocation := d.allocation
	d.allocation = nil
	if allocation != nil && allocation.drop() {
		allocation.refund()
	}
	return nil
}
