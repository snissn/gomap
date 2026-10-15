package db

import (
	"context"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// All native operands borrow the SAME admitted scope only for this synchronous
// original executor. No func environment, observer path, or nested publisher is
// allocated on this route. Its containing producer stays live through Finish.
func (runtime *rootPublicationRuntimeV1) executeNativePreparedSealStorageV6(seal *rootPublicationSealV1, s *nativeAuthorityScopeV6, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || s == nil || s.active || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if durabilitycut.Enabled() || seal.primary == nil || seal.primary.promotion == nil {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	s.runtime, s.seal, s.work = runtime, seal, w
	return executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{work: w, native: s, resources: seal.resources})
}
func (s *nativeAuthorityScopeV6) materializeV6() error {
	if !s.work.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	ready, err := s.runtime.materializePrimarySealWithWorkV6(s.seal, s.work)
	if !ready && err == nil {
		err = rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	return err
}
func (s *nativeAuthorityScopeV6) syncIndexV6() error {
	w, seal := s.work, s.seal
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))+uint64(unsafe.Sizeof(*seal))+uint64(unsafe.Sizeof(primaryPublicationSealV5{}))) {
		return rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if seal.primary == nil {
		return rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if ready, err := seal.primary.token.SyncThroughWithWorkV6(w); !ready || err != nil {
		if err == nil {
			err = rootpublication.ErrNativeMVCCRootWorkRequiredV6
		}
		return err
	}
	if ready, err := seal.primary.token.StabilizeNamespaceWithWorkV6(w); !ready || err != nil {
		if err == nil {
			err = rootpublication.ErrNativeMVCCRootWorkRequiredV6
		}
		return err
	}
	ready, err := seal.token.SyncThroughWithWorkV6(w)
	if !ready && err == nil {
		err = rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	return err
}
func (s *nativeAuthorityScopeV6) installAuthorityV6() (bool, error) {
	w, runtime, seal := s.work, s.runtime, s.seal
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(rootpublication.PublishResult{}))+32) {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	ready, err := s.begin(runtime, seal, w)
	if !ready || err != nil {
		if err == nil {
			err = rootpublication.ErrNativeMVCCRootWorkRequiredV6
		}
		return false, err
	}
	if runtime.db.testFailWriteMeta.Load() {
		return false, errTestWriteMetaFailpoint
	}
	_, ready, mutated, err := seal.idx.primary.Pager().WritePrimaryCapsuleWithTypedFenceStatusV6(2+seal.target*3, seal.primary.promotion.Image, s, w)
	if !ready && err == nil {
		err = rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	return mutated, err
}

// The actual fault guard is fixed and preadmitted by installAuthorityV6 before
// the first copy. It only reads its SAME original flag; observer mode refused.
func (s *nativeAuthorityScopeV6) BeforePrimaryCapsuleFenceV6() error {
	if s.runtime.db.testFailSyncMeta.Load() {
		return errTestSyncMetaFailpoint
	}
	return nil
}
func (s *nativeAuthorityScopeV6) clearPublicationEnvironmentV6() {
	if s == nil {
		return
	}
	s.Close()
	s.work = nil
	s.runtime = nil
	s.seal = nil
	s.failureAdmitted = false
}

// The ordinary callback representation and operation order remain unchanged.
func (runtime *rootPublicationRuntimeV1) executeOrdinaryPreparedSealStorageV1(ctx context.Context, seal *rootPublicationSealV1) (bool, error) {
	db := runtime.db
	var installAuthority func() (bool, error)
	if seal.primary != nil && seal.primary.promotion != nil {
		installAuthority = func() (bool, error) {
			if err := runtime.emitPrimaryAuthorityCutV6(seal, durabilitycut.BeforeMetaWrite); err != nil {
				return false, err
			}
			if db.testFailWriteMeta.Load() {
				return false, errTestWriteMetaFailpoint
			}
			if err := ctx.Err(); err != nil {
				return false, err
			}
			beforeFence := func() error {
				if err := runtime.emitPrimaryAuthorityCutV6(seal, durabilitycut.AfterMetaWrite); err != nil {
					return err
				}
				if err := runtime.emitPrimaryAuthorityCutV6(seal, durabilitycut.BeforeMetaSync); err != nil {
					return err
				}
				if db.testFailSyncMeta.Load() {
					return errTestSyncMetaFailpoint
				}
				return ctx.Err()
			}
			_, _, err := seal.idx.primary.Pager().WritePrimaryCapsuleWithFenceV6(2+seal.target*3, seal.primary.promotion.Image, beforeFence, nil)
			if err != nil {
				return true, err
			}
			if err := runtime.emitPrimaryAuthorityCutV6(seal, durabilitycut.AfterMetaSync); err != nil {
				return true, err
			}
			return true, nil
		}
	}
	return executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
		installAuthority: installAuthority, resources: seal.resources,
		materialize: func() error { return runtime.materializeSeal(seal) },
		syncIndex: func() error {
			if seal.primary != nil {
				if err := seal.primary.token.SyncThrough(); err != nil {
					return err
				}
				if err := seal.primary.token.Namespace().Stabilize(); err != nil {
					return err
				}
			}
			return seal.token.SyncThrough()
		},
		sink: durablePagerSinkV1{pager: seal.idx.pager}, target: seal.target, meta: seal.meta,
		syncMeta: func() error {
			return seal.token.WithPinnedFile(func(file *os.File) error { return seal.idx.pager.SyncPagesWithStableFile(file, []uint64{seal.target}) })
		},
		dir: db.dir, indexPath: filepath.Join(db.dir, indexFileName),
		beforeMetaWrite: func() error {
			if db.testFailWriteMeta.Load() {
				return errTestWriteMetaFailpoint
			}
			return ctx.Err()
		},
		beforeMetaSync: func() error {
			if db.testFailSyncMeta.Load() {
				return errTestSyncMetaFailpoint
			}
			return ctx.Err()
		},
	})
}

// Keep the source import dependency explicit for the original storage sink.
var _ freelist.AppendPageSink = durablePagerSinkV1{}
