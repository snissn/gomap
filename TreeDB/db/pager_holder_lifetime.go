package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/zipper"
	"unsafe"
)

func retainPagerHolder(p *pager.Pager, raw uint64) (*residentcredit.Scope, error) {
	if p == nil {
		return nil, ErrClosed
	}
	creator, err := p.RetainConstructorLifetime()
	if err != nil || creator == nil {
		return creator, err
	}
	class, err := allocclass.ClassBytes(raw, true)
	if err != nil {
		class = raw
	} // unsupported ordinary census; Pager coverage false
	if err := creator.ReserveStableMetadata(class); err != nil {
		creator.ReleaseStableMetadata()
		return nil, err
	}
	return creator, nil
}
// indexConstructorBackingClassV1 measures each actual constructor allocation,
// including full initial backing capacity, before summing its rounded class.
// The fixed value array and traversal create no separate census allocation.
// Later registry/graveyard growth and opaque children remain uncertified.
func indexConstructorBackingClassV1(hasAllocator bool) (uint64, error) {
	raw := [5]struct { bytes uint64; scan bool }{
		{uint64(unsafe.Sizeof(indexGen{})), true},
		{uint64(unsafe.Sizeof(lifecycle.ReaderRegistry{})), true},
		{uint64(unsafe.Sizeof(lifecycle.Graveyard{})), true},
		{lifecycle.GraveyardInitialBatchBackingBytes(), true},
		{0, false},
	}
	if hasAllocator {
		raw[4].bytes = uint64(unsafe.Sizeof(allocatorownership.ManagedWriter{}))
	}
	var total uint64
	for _, allocation := range raw {
		class, err := allocclass.ClassBytes(allocation.bytes, allocation.scan)
		if err != nil {
			// Unsupported ordinary layouts retain a raw census. The Pager's
			// incomplete coverage still refuses every finite capacity loan.
			class = allocation.bytes
		}
		if class > ^uint64(0)-total {
			return 0, residentcredit.ErrLimit
		}
		total += class
	}
	return total, nil
}

func newConstructorIndexGen(id uint64, p *pager.Pager, a *freelist.Allocator, z *zipper.Zipper) (*indexGen, error) {
	if p == nil {
		return nil, ErrClosed
	}
	class, err := indexConstructorBackingClassV1(a != nil)
	if err != nil {
		return nil, err
	}
	creator, err := p.RetainConstructorLifetime()
	if err != nil {
		return nil, err
	}
	if creator != nil {
		// One atomic reservation dominates all child births and writer bind.
		// Refusal releases only this attempted hold, without partial debit.
		if err := creator.ReserveStableMetadata(class); err != nil {
			creator.ReleaseStableMetadata()
			return nil, err
		}
	}
	gen := newIndexGen(id, p, a, z)
	gen.creator = creator
	return gen, nil
}
func (db *DB) newSnapshotHolder(idx *indexGen) (*Snapshot, error) {
	return db.newSnapshotHolderRoleV1(idx, 0, false)
}
func (db *DB) newSnapshotHolderRoleV1(idx *indexGen, role residentcredit.PrivateSnapshotRoleV1, ownIndex bool) (*Snapshot, error) {
	if idx == nil {
		return nil, ErrClosed
	}
	return db.newSnapshotHolderPagerRoleV1(idx.pager, role, ownIndex)
}
func (db *DB) newSnapshotHolderPagerRoleV1(p *pager.Pager, role residentcredit.PrivateSnapshotRoleV1, ownIndex bool) (*Snapshot, error) {
	if p == nil {
		return nil, ErrClosed
	}
	creator, err := p.RetainConstructorLifetime()
	if err != nil {
		return nil, err
	}
	var backing residentcredit.PrivateSnapshotBackingV1
	retainedCompletion := false
	born := false
	defer func() {
		if !born {
			if backing != nil {
				backing.ReleasePrivateSnapshotBackingV1()
			}
			if retainedCompletion {
				creator.ReleaseStableMetadata()
			}
			creator.ReleaseStableMetadata()
		}
	}()
	if role != 0 {
		raw := residentcredit.PrivateSnapshotLayoutV1{Holder: uint64(unsafe.Sizeof(Snapshot{}))}
		if role == residentcredit.PrivatePointReadV1 {
			raw.Point = uint64(unsafe.Sizeof(oneShotRead{}))
		} else {
			raw.State = uint64(unsafe.Sizeof(DBState{}))
			if ownIndex {
				raw.Index = uint64(unsafe.Sizeof(indexGen{}))
			}
		}
		backing, err = creator.ReservePrivateSnapshotBackingV1(role, raw)
		if err != nil {
			return nil, err
		}
	} else {
		n, e := allocclass.ClassBytes(uint64(unsafe.Sizeof(Snapshot{})), true)
		if e != nil {
			n = uint64(unsafe.Sizeof(Snapshot{}))
		}
		if err = creator.ReserveStableMetadata(n); err != nil {
			return nil, err
		}
	}
	if err = creator.RetainOriginalLifetime(); err != nil {
		return nil, err
	}
	retainedCompletion = true
	s := db.snapPool.Get()
	s.pagerCreator = creator
	// Initialize the inline mutex/control exactly once, before any observer.
	s.originalCleanupStorage.snapshot = s
	s.originalCleanupStorage.creator = creator
	s.originalCleanupStorage.refs = 1
	s.originalCleanupStorage.privateBacking = backing
	s.originalCleanup = &s.originalCleanupStorage
	born = true
	return s, nil
}
