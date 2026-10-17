package collections

import (
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativePendingSource is the original physical-source owner. Views and list
// positions share this exact root and source-slot receipt. New active writes
// use another generation; they never extend a captured prefix.
type nativePendingSource struct {
	refs   atomic.Int64
	root   *memtable.COWRoot
	refund *cowbtree.OwnedMapRefund
}

// A list position is accounting metadata, separately owned from its source.
// Sparse handoff may clone newer positions to drop an accepted older prefix
// without duplicating source slots or retaining an ancestor engine version.
type nativePendingSources struct {
	refs   atomic.Int64
	source *nativePendingSource
	older  *nativePendingSources
	refund *cowbtree.OwnedMapRefund
}

func newNativePendingSource(root *memtable.COWRoot) (*nativePendingSource, error) {
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativePendingSource{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	lease, err := root.AcquireCapturedSource(charge)
	if err != nil {
		return nil, err
	}
	if !root.Retain() {
		lease.Close()
		return nil, memtable.ErrCOWClosed
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	s := &nativePendingSource{root: root, refund: refund}
	s.refs.Store(1)
	return s, nil
}

func (s *nativePendingSource) retain() {
	if s == nil || s.refs.Add(1) <= 1 {
		panic("collections: retain released native source")
	}
}
func (s *nativePendingSource) release() (done cowbtree.OwnedMapCompletion) {
	if s == nil {
		return done
	}
	refs := s.refs.Add(-1)
	if refs < 0 {
		panic("collections: native source released twice")
	}
	if refs != 0 {
		return done
	}
	root, refund := s.root, s.refund
	s.root, s.refund = nil, nil
	retirement := root.Release()
	root = nil
	retirement.Drain()
	return refund.Completion()
}

// newNativePendingSources adopts source and older references only on success.
// admit is original producer admission during rollover; accepted handoff uses
// its existing preadmitted maintenance construction capacity instead.
func newNativePendingSources(source *nativePendingSource, older *nativePendingSources, admit cowbtree.OwnedMapAdmit) (*nativePendingSources, error) {
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativePendingSources{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	s := &nativePendingSources{source: source, older: older, refund: refund}
	s.refs.Store(1)
	return s, nil
}

func (s *nativePendingSources) retain() {
	if s != nil && s.refs.Add(1) <= 1 {
		panic("collections: retain released native source list")
	}
}

// release runs on the existing semantic worker. Its finite walk visits only
// nodes whose actual last edge was removed. Parent/value aliases are detached
// before returned independent receipts refund their containing allocations.
func (s *nativePendingSources) release() (done cowbtree.OwnedMapCompletion) {
	for s != nil {
		refs := s.refs.Add(-1)
		if refs < 0 {
			panic("collections: native source list released twice")
		}
		if refs != 0 {
			return done
		}
		source, older, refund := s.source, s.older, s.refund
		s.source, s.older, s.refund = nil, nil, nil
		done.Append(source.release())
		source = nil
		done.Append(refund.Completion())
		s = nil
		s = older
	}
	return done
}

// get resolves newest captured effects first, preserving tombstones as real
// effects. Reads consult the exact enclosing disk snapshot only on absence.
func (s *nativePendingSources) get(key []byte) (memtable.COWRecord, bool) {
	for ; s != nil; s = s.older {
		if row, ok := s.source.root.Get(key); ok {
			return row, true
		}
	}
	return memtable.COWRecord{}, false
}
