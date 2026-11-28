package treedb

import (
	"context"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func wireMainBackendDictCallbacks(backend *db.DB, store *dictdb.Store, readOnly bool) {
	if backend == nil || store == nil {
		return
	}
	var (
		put                func(context.Context, []byte) (uint64, error)
		setCurrentForClass func(context.Context, string, uint64) error
		setLeafPayloadMode func(context.Context, uint64, bool) error
		publishRewriteLeaf func(context.Context, []byte, string, bool) (uint64, error)
	)
	if !readOnly {
		put = store.PutDictBytes
		setCurrentForClass = store.SetCurrentForClass
		setLeafPayloadMode = store.SetLeafPayloadMode
		publishRewriteLeaf = store.PublishRewriteLeafDict
	}
	backend.SetDictCallbacks(
		nil,
		store.GetCurrentForClass,
		store.GetLeafPayloadMode,
		put,
		setCurrentForClass,
		setLeafPayloadMode,
		publishRewriteLeaf,
	)
}

// OpenBackend opens the TreeDB backend directly (no caching layer) with
// in-DB SystemRootPageID dictionary and template support.
//
// This is intended for maintenance tooling (e.g. treemap vlog-gc) that must
// avoid cached-layer side effects but still needs value-log decode/publication
// plumbing.
func OpenBackend(opts Options) (*db.DB, func() error, error) {
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return nil, nil, err
	}
	unlock, err := acquireOrdinaryOpenOwnership(opts)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	dir, err := resolveOpenDir(opts.Dir)
	if err != nil {
		return nil, nil, err
	}

	// Apply persisted storage/runtime knobs so direct maintenance opens agree
	// with the on-disk format and compression policy used by the DB.
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := db.CommandWALRequiredFeatureEnabled(dir)
		if err != nil {
			return nil, nil, err
		}
		opts.CommandWAL = opts.CommandWAL || requiresCommandWAL
	} else {
		if cfg, ok, err := db.LoadFormatConfig(dir); err != nil {
			return nil, nil, err
		} else if ok {
			opts.CommandWAL = opts.CommandWAL || cfg.RequiresCommandWALV1()
			cfg.ApplyToOptions(&opts)
		}
	}

	opts.Dir = dir
	backend, err := db.Open(opts)
	if err != nil {
		return nil, nil, err
	}
	wireMainBackendDictCallbacks(backend, dictdb.New(backend), opts.ReadOnly)
	return backend, backend.Close, nil
}

// OpenBackendWithCachedLeafLog opens a backend-root lifecycle with cached leaf-
// and value-log services, including IndexOuterLeavesInValueLog. The backend owns
// logical publication. Requested physical/profile policy is preserved, but a
// requested cow_btree cache uses ordinary btree services without a COW journal
// or native semantic bridge. Native callers use Open and NativeSemanticBackend.
func OpenBackendWithCachedLeafLog(opts Options) (*db.DB, func() error, error) {
	database, err := openBackendWithCachedServices(opts)
	if err != nil {
		return nil, nil, err
	}
	if database.backend == nil {
		_ = database.Close()
		return nil, nil, db.ErrClosed
	}
	return database.backend, database.Close, nil
}

// OpenBackendWithCachedLeafLogStats is OpenBackendWithCachedLeafLog with a
// read-only snapshot of the public cached DB statistics. Raw-backend callers
// can keep using OpenBackendWithCachedLeafLog; operator diagnostics may retain
// only this narrow callback. The callback is live-only: callers must stop
// invoking it before calling cleanup.
func OpenBackendWithCachedLeafLogStats(opts Options) (*db.DB, func() error, func() map[string]string, error) {
	backend, cleanup, stats, _, err := OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	return backend, cleanup, stats, err
}

// OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance adds the
// narrow deferred-build maintenance control used by the document service.
func OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts Options) (*db.DB, func() error, func() map[string]string, *DeferredVectorBuildMaintenance, error) {
	database, err := openBackendWithCachedServices(opts)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if database.backend == nil {
		_ = database.Close()
		return nil, nil, nil, nil, db.ErrClosed
	}
	return database.backend, database.Close, database.Stats, database.DeferredVectorBuildMaintenance(), nil
}

// Resolve public policy before choosing the backend-only cache lifecycle. In
// particular, do not rewrite MemtableMode before physical options are applied.
func openBackendWithCachedServices(opts Options) (*DB, error) {
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return nil, err
	}
	if err := resolveOpenMemtablePolicy(&opts, true); err != nil {
		return nil, err
	}
	if err := validateCOWOpenOptions(&opts); err != nil {
		return nil, err
	}
	unlock, err := acquireOrdinaryOpenOwnership(opts)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return openResolvedWithCacheRole(opts, true)
}
