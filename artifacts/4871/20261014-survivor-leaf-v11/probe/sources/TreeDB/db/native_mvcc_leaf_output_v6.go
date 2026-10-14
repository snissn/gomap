package db

import (
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// NewNativeMVCCLeafOutputV6 admits one original level builder against the
// captured DATA generation's SAME metadata owner. The caller owns that capture
// throughout output and checked cancellation; this method adds no root edge.
func (s *Snapshot) NewNativeMVCCLeafOutputV6(opts bulk.BuildOptions, w *iterator.OrdinalScanWork) (*bulk.LeafOutput, bool, error) {
	if s == nil {
		return nil, false, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return nil, false, err
	}
	defer s.endRead()
	if s.idx == nil || s.idx.metadata == nil {
		return nil, false, ErrClosed
	}
	return bulk.NewLeafOutput(s.idx.metadata, opts, w)
}
func (s *Snapshot) StoreNativeMVCCLeafOutputV6(o *bulk.LeafOutput, w *iterator.OrdinalScanWork) (uint64, bool, error) {
	if s == nil {
		return 0, false, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return 0, false, err
	}
	defer s.endRead()
	if s.idx == nil || s.idx.allocator == nil || s.idx.pager == nil {
		return 0, false, ErrClosed
	}
	return o.Store(s.idx.allocator, s.idx.pager, w)
}
