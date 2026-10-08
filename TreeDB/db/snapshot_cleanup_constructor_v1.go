package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Recovery/scanning owns a fresh private holder whose entire fixed constructor
// geometry was reserved on the original Pager Scope before any object birth.
func (db *DB) newScanSnapshotV1(p *pager.Pager, idx *indexGen) (*Snapshot, error) {
	if p == nil {
		return nil, ErrClosed
	}
	ownIndex := idx == nil
	s, err := db.newSnapshotHolderPagerRoleV1(p, residentcredit.PrivateScanV1, ownIndex)
	if err != nil {
		return nil, err
	}
	if ownIndex {
		idx = &indexGen{pager: p}
	}
	s.db = db
	s.idx = idx
	s.scanOnly = true
	s.state = &DBState{}
	s.closed.Store(false)
	s.readState.Store(0)
	return s, nil
}

// ReserveOriginalConstructorV1 prepays a concrete ordinary adapter while the
// exact Snapshot read admission retains its original Pager creator. It grants
// no coverage for opaque callback environments or selected finite admission.
func (s *Snapshot) ReserveOriginalConstructorV1(raw uint64) (err error) {
	if s == nil {
		return ErrClosed
	}
	if err = s.beginRead(); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.endReadChecked()) }()
	creator := s.pagerCreator
	if creator == nil {
		return ErrClosed
	}
	n, e := allocclass.ClassBytes(raw, true)
	if e != nil {
		return e
	}
	return creator.ReserveOriginalLifetime(n)
}
