package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/page"
)

// ErrLeafGenerationMaintenanceLimit means the native maintenance input exceeds
// the caller's admitted storage footprint. No partial reachability result may
// be used to reclaim storage after this error.
var ErrLeafGenerationMaintenanceLimit = errors.New("leaf generation maintenance footprint limit exceeded")

// LeafGenerationMaintenanceLimits admits the input footprint before native
// planning, packing, and GC. NativeEntries bounds directory entries and each
// captured generation/file table; NativeBytes bounds on-disk bytes, including
// nested collection assets; PagerPages bounds each captured index generation.
//
// All-zero limits preserve legacy behavior. Otherwise every field must be
// positive. These are per-phase footprint admission limits, not cumulative I/O
// counters or a quota on concurrent growth. Each independent snapshot is
// checked again; pack retries and GC's separate recovery scan are not free work.
type LeafGenerationMaintenanceLimits struct {
	NativeEntries int
	NativeBytes   int64
	PagerPages    uint64
}

func (l LeafGenerationMaintenanceLimits) enabled() bool {
	return l != (LeafGenerationMaintenanceLimits{})
}

func (l LeafGenerationMaintenanceLimits) validate() error {
	if l.enabled() && (l.NativeEntries <= 0 || l.NativeBytes <= 0 || l.PagerPages == 0) {
		return fmt.Errorf("%w: all footprint limits must be positive", ErrLeafGenerationMaintenanceLimit)
	}
	return nil
}

func (l LeafGenerationMaintenanceLimits) admitDirectory(ctx context.Context, root string) error {
	if err := l.validate(); err != nil || !l.enabled() {
		return err
	}
	entries, bytes := 0, int64(0)
	var visit func(string, int) error
	visit = func(path string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth > 32 {
			return fmt.Errorf("%w: directory depth", ErrLeafGenerationMaintenanceLimit)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: unsupported directory %q", ErrLeafGenerationMaintenanceLimit, path)
		}
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		defer dir.Close()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Read at most the remaining allowance plus one rejection witness.
			batchSize := 64
			if remaining := l.NativeEntries - entries; remaining < batchSize {
				batchSize = remaining + 1
			}
			batch, readErr := dir.ReadDir(batchSize)
			for _, entry := range batch {
				if entries >= l.NativeEntries {
					return fmt.Errorf("%w: directory entries", ErrLeafGenerationMaintenanceLimit)
				}
				entries++
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("%w: symlink %q", ErrLeafGenerationMaintenanceLimit, entry.Name())
				}
				if info.IsDir() {
					if err := visit(filepath.Join(path, entry.Name()), depth+1); err != nil {
						return err
					}
				} else {
					if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > l.NativeBytes-bytes {
						return fmt.Errorf("%w: native bytes or unsupported file", ErrLeafGenerationMaintenanceLimit)
					}
					bytes += info.Size()
				}
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			if readErr != nil {
				return readErr
			}
		}
	}
	return visit(root, 0)
}

func (l LeafGenerationMaintenanceLimits) admitManifest(manifest *leafGenerationManifest) error {
	if !l.enabled() || manifest == nil {
		return nil
	}
	if len(manifest.Generations) > l.NativeEntries {
		return fmt.Errorf("%w: manifest generations", ErrLeafGenerationMaintenanceLimit)
	}
	files := 0
	for _, gen := range manifest.Generations {
		if len(gen.FileIDs) > l.NativeEntries-files {
			return fmt.Errorf("%w: manifest file references", ErrLeafGenerationMaintenanceLimit)
		}
		files += len(gen.FileIDs)
	}
	return nil
}

func (l LeafGenerationMaintenanceLimits) admitSnapshot(ctx context.Context, snap *Snapshot) error {
	if err := l.validate(); err != nil || !l.enabled() {
		return err
	}
	if snap == nil || snap.state == nil || snap.idx == nil || snap.idx.pager == nil {
		return ErrClosed
	}
	pages := snap.idx.pager.PageCount()
	if pages > l.PagerPages || pages > uint64(l.NativeBytes/page.PageSize) {
		return fmt.Errorf("%w: captured pager pages", ErrLeafGenerationMaintenanceLimit)
	}
	if view := snap.state.LeafGenerations; view != nil {
		if len(view.Generations) > l.NativeEntries || len(view.FileToGeneration) > l.NativeEntries {
			return fmt.Errorf("%w: captured generation/file table", ErrLeafGenerationMaintenanceLimit)
		}
		if err := l.admitManifest(view.sourceManifest); err != nil {
			return err
		}
	}
	if set := snap.state.ValueLogSet; set != nil {
		if len(set.Files) > l.NativeEntries {
			return fmt.Errorf("%w: captured value-log files", ErrLeafGenerationMaintenanceLimit)
		}
		bytes := int64(pages) * page.PageSize
		for _, file := range set.Files {
			if err := ctx.Err(); err != nil {
				return err
			}
			if file == nil || file.File == nil {
				return fmt.Errorf("%w: unavailable captured file", ErrLeafGenerationMaintenanceLimit)
			}
			info, err := file.File.Stat()
			if err != nil {
				return err
			}
			if info.Size() < 0 || info.Size() > l.NativeBytes-bytes {
				return fmt.Errorf("%w: captured native bytes", ErrLeafGenerationMaintenanceLimit)
			}
			bytes += info.Size()
		}
	}
	return nil
}

// Admission is repeated before independent phases. In particular GC must not
// refresh/reconcile a directory before the finite directory check, and pack
// must check the snapshot it actually rewrites, not only the earlier plan.
func (db *DB) admitLeafGenerationMaintenance(ctx context.Context, limits LeafGenerationMaintenanceLimits) error {
	if !limits.enabled() {
		return nil
	}
	if err := limits.admitDirectory(ctx, db.dir); err != nil {
		return err
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return ErrClosed
	}
	defer snap.Close()
	if err := limits.admitSnapshot(ctx, snap); err != nil {
		return err
	}
	db.mu.RLock()
	err := limits.admitManifest(db.leafGenerationManifest)
	db.mu.RUnlock()
	return err
}
