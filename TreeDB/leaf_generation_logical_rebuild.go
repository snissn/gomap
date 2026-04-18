package treedb

import treedbdb "github.com/snissn/gomap/TreeDB/db"

// LeafGenerationLogicalRebuildStats summarizes a frozen leaf-only logical
// rebuild that rewrites outer-leaf pages and swaps a fresh index.
type LeafGenerationLogicalRebuildStats = treedbdb.LeafGenerationLogicalRebuildStats

// LeafGenerationLogicalRebuildOffline rebuilds outer-leaf pages logically into
// a fresh leaf_vlog directory and swaps it with a fresh index.db under the
// offline DB lock. Value-log pointers remain unchanged.
func LeafGenerationLogicalRebuildOffline(opts Options) (LeafGenerationLogicalRebuildStats, error) {
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return LeafGenerationLogicalRebuildStats{}, err
	}
	opts.Dir = layout.mainDir
	opts.DisableSideStores = layout.disableSideStores

	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := treedbdb.LoadFormatConfig(layout.mainDir); err != nil {
			return LeafGenerationLogicalRebuildStats{}, err
		} else if ok {
			cfg.ApplyToOptions(&opts)
		}
	}

	sideCleanup, err := wireSideStoreLookups(layout.rootDir, &opts)
	if err != nil {
		return LeafGenerationLogicalRebuildStats{}, err
	}
	defer func() { _ = sideCleanup() }()

	stats, err := treedbdb.LeafGenerationLogicalRebuildOffline(opts)
	if err != nil {
		return LeafGenerationLogicalRebuildStats{}, err
	}
	return LeafGenerationLogicalRebuildStats(stats), nil
}
