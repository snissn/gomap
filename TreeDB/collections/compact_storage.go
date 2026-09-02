package collections

import (
	"context"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// CompactStorageOptions controls collection-aware storage compaction.
type CompactStorageOptions = backenddb.CompactStorageOptions

// CompactStorageStats summarizes collection root-overlay compaction plus the
// underlying TreeDB storage compaction report.
type CompactStorageStats struct {
	RootOverlays map[string]CollectionRootOverlayCompactionStats `json:"root_overlays,omitempty"`
	Storage      backenddb.CompactStorageStats                   `json:"storage"`
}

// CompactStoragePlan reports collection-aware storage compaction debt without
// folding root overlays or mutating storage.
func (c *Collection) CompactStoragePlan(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	opts.DryRun = true
	return c.CompactStorage(ctx, opts)
}

// CompactStorage folds this collection's root overlays and then runs the
// recommended full TreeDB storage compaction sequence.
func (c *Collection) CompactStorage(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	var stats CompactStorageStats
	if c == nil {
		return stats, errCollectionNil
	}
	if c.db == nil {
		return stats, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.DryRun {
		storage, err := c.db.CompactStoragePlan(ctx, backenddb.CompactStorageOptions(opts))
		stats.Storage = storage
		return stats, err
	}
	rootResult, err := c.compactRootOverlays(ctx)
	if err != nil {
		return stats, err
	}
	stats.RootOverlays = map[string]CollectionRootOverlayCompactionStats{
		c.meta.Name: rootResult.stats,
	}
	if err := checkpointCollectionCompactStorageFoldedRoots(c.db, rootResult); err != nil {
		return stats, err
	}
	storage, err := c.db.CompactStorage(ctx, backenddb.CompactStorageOptions(opts))
	if err != nil {
		return stats, err
	}
	stats.Storage = storage
	return stats, nil
}

// CompactStoragePlan reports collection-manager storage compaction debt without
// folding root overlays or mutating storage.
func (m *CollectionManager) CompactStoragePlan(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	opts.DryRun = true
	return m.CompactStorage(ctx, opts)
}

// CompactStorage folds root overlays for all known collections and then runs
// the recommended full TreeDB storage compaction sequence.
func (m *CollectionManager) CompactStorage(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	var stats CompactStorageStats
	if m == nil {
		return stats, errCollectionManagerNil
	}
	if m.db == nil {
		return stats, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.DryRun {
		storage, err := m.db.CompactStoragePlan(ctx, backenddb.CompactStorageOptions(opts))
		stats.Storage = storage
		return stats, err
	}
	metas, err := m.ListCollections()
	if err != nil {
		return stats, err
	}
	stats.RootOverlays = make(map[string]CollectionRootOverlayCompactionStats, len(metas))
	rootResults := make([]collectionRootOverlayCompactionResult, 0, len(metas))
	for _, meta := range metas {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		collection, err := m.OpenCollection(meta.Name)
		if err != nil {
			return stats, err
		}
		rootResult, err := collection.compactRootOverlays(ctx)
		if err != nil {
			return stats, err
		}
		stats.RootOverlays[meta.Name] = rootResult.stats
		rootResults = append(rootResults, rootResult)
	}
	if err := checkpointCollectionCompactStorageFoldedRoots(m.db, rootResults...); err != nil {
		return stats, err
	}
	storage, err := m.db.CompactStorage(ctx, backenddb.CompactStorageOptions(opts))
	if err != nil {
		return stats, err
	}
	stats.Storage = storage
	return stats, nil
}

func checkpointCollectionCompactStorageFoldedRoots(db *backenddb.DB, results ...collectionRootOverlayCompactionResult) error {
	if db == nil {
		return errCollectionDBNil
	}
	for _, result := range results {
		if result.systemRootID != 0 {
			// Backend storage compaction protects collection roots by scanning
			// the current system-root descriptors; make the folded descriptors
			// durable before rewrite/pack/GC phases refresh their snapshots.
			return db.Checkpoint()
		}
	}
	return nil
}

func appendCollectionCompactStorageProtectedRootIDs(dst []uint64, src []uint64) []uint64 {
	for _, rootID := range src {
		if rootID == 0 {
			continue
		}
		seen := false
		for _, existing := range dst {
			if existing == rootID {
				seen = true
				break
			}
		}
		if !seen {
			dst = append(dst, rootID)
		}
	}
	return dst
}
