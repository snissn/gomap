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
	rootStats, err := c.CompactRootOverlays(ctx)
	if err != nil {
		return stats, err
	}
	stats.RootOverlays = map[string]CollectionRootOverlayCompactionStats{
		c.meta.Name: rootStats,
	}
	storage, err := c.db.CompactStorage(ctx, backenddb.CompactStorageOptions(opts))
	if err != nil {
		return stats, err
	}
	stats.Storage = storage
	return stats, nil
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
	metas, err := m.ListCollections()
	if err != nil {
		return stats, err
	}
	stats.RootOverlays = make(map[string]CollectionRootOverlayCompactionStats, len(metas))
	for _, meta := range metas {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		collection, err := m.OpenCollection(meta.Name)
		if err != nil {
			return stats, err
		}
		rootStats, err := collection.CompactRootOverlays(ctx)
		if err != nil {
			return stats, err
		}
		stats.RootOverlays[meta.Name] = rootStats
	}
	storage, err := m.db.CompactStorage(ctx, backenddb.CompactStorageOptions(opts))
	if err != nil {
		return stats, err
	}
	stats.Storage = storage
	return stats, nil
}
