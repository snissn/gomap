package db

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/vlog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/zipper"
)

func openReadOnly(opts Options) (*DB, error) {
	var lock *lockfile.Lock
	lockPath := filepath.Join(opts.Dir, "LOCK")
	if l, err := lockfile.AcquireShared(lockPath); err == nil {
		lock = l
	} else if !(errors.Is(err, os.ErrNotExist) || errors.Is(err, lockfile.ErrUnsupported)) {
		return nil, err
	}

	idxPath := filepath.Join(opts.Dir, indexFileName)
	if _, err := os.Stat(idxPath); err != nil {
		_ = lock.Close()
		return nil, err
	}

	p, err := pager.OpenReadOnly(idxPath, opts.ChunkSize)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	sm, err := slab.NewSlabManagerReadOnly(opts.Dir, slab.Options{
		Compression: opts.SlabCompression,
	})
	if err != nil {
		_ = p.Close()
		_ = lock.Close()
		return nil, err
	}
	sm.SetDisableReadChecksum(opts.DisableReadChecksum)

	vlogDir := filepath.Join(opts.Dir, "wal")
	vm, err := vlog.NewManager(vlogDir)
	if err != nil {
		_ = p.Close()
		_ = sm.Close()
		_ = lock.Close()
		return nil, err
	}
	vm.SetDisableReadChecksum(opts.DisableReadChecksum)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)
	gen := newIndexGen(1, p, alloc, z)

	db := &DB{
		readOnly:                  true,
		slabManager:               sm,
		valueLogManager:           vm,
		lock:                      lock,
		adaptive:                  adaptive.New(),
		keepRecent:                opts.KeepRecent,
		leafFillTargetPPM:         opts.LeafFillTargetPPM,
		internalFillTargetPPM:     opts.InternalFillTargetPPM,
		piggybackCompaction:       !opts.DisablePiggybackCompaction,
		maintenanceDeleteMinRatio: opts.MaintenanceDeleteMinRatio,
		repairSlabTailOnOpen:      false,
		dir:                       opts.Dir,
		chunkSize:                 opts.ChunkSize,
		preferAppendAlloc:         opts.PreferAppendAlloc,
		freelistRegionPages:       opts.FreelistRegionPages,
		freelistRegionRadius:      opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: page.DefaultInlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},

		idxAll:      map[uint64]*indexGen{gen.id: gen},
		idxNext:     gen.id + 1,
		snapPool:    NewSnapshotPool(),
		notifyError: opts.NotifyError,
	}
	db.idx.Store(gen)

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)
	gen.zipper.SetMaintenanceDeleteMinRatio(opts.MaintenanceDeleteMinRatio)

	if err := db.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}

	initialState := &DBState{
		CommitSeq:        db.meta.CommitSeq,
		RootPageID:       db.meta.UserRootPageID,
		SystemRootPageID: db.meta.SystemRootPageID,
		SlabSet:          sm.CurrentSlabSet(),
		ValueLogSet:      vm.CurrentSet(),
	}
	db.state.Store(initialState)

	// No WAL replay, no background workers in read-only mode.
	return db, nil
}

func openReadOnlyNoLock(opts Options) (*DB, error) {
	idxPath := filepath.Join(opts.Dir, indexFileName)
	if _, err := os.Stat(idxPath); err != nil {
		return nil, err
	}

	p, err := pager.OpenReadOnly(idxPath, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	sm, err := slab.NewSlabManagerReadOnly(opts.Dir, slab.Options{
		Compression: opts.SlabCompression,
	})
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	sm.SetDisableReadChecksum(opts.DisableReadChecksum)

	vlogDir := filepath.Join(opts.Dir, "wal")
	vm, err := vlog.NewManager(vlogDir)
	if err != nil {
		_ = p.Close()
		_ = sm.Close()
		return nil, err
	}
	vm.SetDisableReadChecksum(opts.DisableReadChecksum)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)
	gen := newIndexGen(1, p, alloc, z)

	db := &DB{
		readOnly:                  true,
		slabManager:               sm,
		valueLogManager:           vm,
		adaptive:                  adaptive.New(),
		keepRecent:                opts.KeepRecent,
		leafFillTargetPPM:         opts.LeafFillTargetPPM,
		internalFillTargetPPM:     opts.InternalFillTargetPPM,
		piggybackCompaction:       !opts.DisablePiggybackCompaction,
		maintenanceDeleteMinRatio: opts.MaintenanceDeleteMinRatio,
		repairSlabTailOnOpen:      false,
		dir:                       opts.Dir,
		chunkSize:                 opts.ChunkSize,
		preferAppendAlloc:         opts.PreferAppendAlloc,
		freelistRegionPages:       opts.FreelistRegionPages,
		freelistRegionRadius:      opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: page.DefaultInlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},

		idxAll:      map[uint64]*indexGen{gen.id: gen},
		idxNext:     gen.id + 1,
		snapPool:    NewSnapshotPool(),
		notifyError: opts.NotifyError,
	}
	db.idx.Store(gen)

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)
	gen.zipper.SetMaintenanceDeleteMinRatio(opts.MaintenanceDeleteMinRatio)

	if err := db.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}

	initialState := &DBState{
		CommitSeq:        db.meta.CommitSeq,
		RootPageID:       db.meta.UserRootPageID,
		SystemRootPageID: db.meta.SystemRootPageID,
		SlabSet:          sm.CurrentSlabSet(),
		ValueLogSet:      vm.CurrentSet(),
	}
	db.state.Store(initialState)

	return db, nil
}
