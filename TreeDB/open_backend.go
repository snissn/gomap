package treedb

import (
	"github.com/snissn/gomap/TreeDB/db"
)

// OpenBackend opens the TreeDB backend directly (no caching layer) while wiring
// side-store lookups (dictdb/templatedb) when present.
//
// This is intended for maintenance tooling (e.g. treemap vlog-gc) that must
// avoid cached-layer side effects but still needs value-log decode plumbing.
func OpenBackend(opts Options) (*db.DB, func() error, error) {
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return nil, nil, err
	}
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return nil, nil, err
	}
	opts.DisableSideStores = layout.disableSideStores

	// Apply persisted storage/runtime knobs so direct maintenance opens agree
	// with the on-disk format and compression policy used by the DB.
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := db.CommandWALRequiredFeatureEnabled(layout.mainDir)
		if err != nil {
			return nil, nil, err
		}
		opts.CommandWAL = opts.CommandWAL || requiresCommandWAL
	} else {
		if cfg, ok, err := db.LoadFormatConfig(layout.mainDir); err != nil {
			return nil, nil, err
		} else if ok {
			opts.CommandWAL = opts.CommandWAL || cfg.RequiresCommandWALV1()
			cfg.ApplyToOptions(&opts)
		}
	}

	var closers []func() error
	sideCleanup, err := wireSideStoreLookups(layout.rootDir, &opts)
	if err != nil {
		return nil, nil, err
	}
	closers = append(closers, sideCleanup)

	opts.Dir = layout.mainDir
	backend, err := db.Open(opts)
	if err != nil {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i]()
		}
		return nil, nil, err
	}
	closers = append(closers, backend.Close)

	cleanup := func() error {
		var first error
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i](); err != nil && first == nil {
				first = err
			}
		}
		return first
	}

	return backend, cleanup, nil
}

// OpenBackendWithCachedLeafLog opens TreeDB through the cached layer and returns
// the underlying backend. This is for native-root callers that need backend root
// APIs while also requiring cached-layer wiring such as the leaf-page value log
// used by IndexOuterLeavesInValueLog.
func OpenBackendWithCachedLeafLog(opts Options) (*db.DB, func() error, error) {
	database, err := Open(opts)
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
	database, err := Open(opts)
	if err != nil {
		return nil, nil, nil, err
	}
	if database.backend == nil {
		_ = database.Close()
		return nil, nil, nil, db.ErrClosed
	}
	return database.backend, database.Close, database.Stats, nil
}
