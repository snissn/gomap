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
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return nil, nil, err
	}
	opts.DisableSideStores = layout.disableSideStores

	// Apply persisted index encoding knobs so direct backend opens agree with the
	// on-disk index format.
	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := db.LoadFormatConfig(layout.mainDir); err != nil {
			return nil, nil, err
		} else if ok {
			cfg.ApplyIndexFormatToOptions(&opts)
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
