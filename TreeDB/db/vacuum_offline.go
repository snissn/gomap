package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

type vacuumFailpoint string

const (
	vacuumFailNone           vacuumFailpoint = ""
	vacuumFailAfterNewSync   vacuumFailpoint = "after_new_sync"
	vacuumFailAfterReady     vacuumFailpoint = "after_ready"
	vacuumFailAfterRenameOld vacuumFailpoint = "after_rename_old"
	vacuumFailAfterRenameNew vacuumFailpoint = "after_rename_new"
)

var errVacuumFailpoint = errors.New("vacuum failpoint")

// VacuumIndexOffline rewrites index.db into a fresh file and swaps it in.
//
// This is intended to reclaim space (reduce `index.db` chunk count) and restore
// locality after long churn. It is an offline operation (requires exclusive
// open lock).
func VacuumIndexOffline(opts Options) error {
	return vacuumIndexOffline(opts, vacuumFailNone)
}

func vacuumIndexOffline(opts Options, fail vacuumFailpoint) error {
	if opts.Dir == "" {
		return errors.New("db dir required")
	}
	if err := applyFormatConfigForMaintenance(&opts); err != nil {
		return err
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	opts.DisableBackgroundPrune = true
	opts.ReadOnly = true

	lock, err := lockfile.Acquire(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	if err := recoverIndexSwap(opts.Dir); err != nil {
		return err
	}
	if err := collectionwal.RequireCleanForOfflineMaintenance(opts.Dir); err != nil {
		return err
	}

	// Open the DB without acquiring a second lock (we already hold it).
	d, err := openReadOnlyNoLock(opts)
	if err != nil {
		return err
	}

	state := d.State()
	if state == nil {
		_ = d.Close()
		return fmt.Errorf("vacuum: missing db state")
	}
	if state.ValueLogSet != nil {
		d.valueLogManager.Acquire(state.ValueLogSet)
		defer d.valueLogManager.Release(state.ValueLogSet)
	}

	indexPath := filepath.Join(opts.Dir, indexFileName)
	newPath := filepath.Join(opts.Dir, indexNewFileName)
	bakPath := filepath.Join(opts.Dir, indexBakFileName)
	readyPath := filepath.Join(opts.Dir, indexReadyFileName)

	// Clean up any previous partial artifacts (best-effort).
	_ = os.Remove(newPath)
	_ = os.Remove(readyPath)

	newPager, err := pager.Open(newPath, opts.ChunkSize)
	if err != nil {
		_ = d.Close()
		return err
	}

	if _, err := newPager.Alloc(2); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	alloc := &pagerAllocator{p: newPager}
	reader := newValueReader(state.ValueLogSet)

	collectionRootReplacements, err := vacuumRewriteCollectionRoots(d.Pager(), reader, state.SystemRootPageID, alloc, newPager)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	buildOpts := bulk.BuildOptions{
		LeafPrefixCompression: opts.LeafPrefixCompression,
		LeafColumnar:          opts.IndexColumnarLeaves,
		PackedValuePtr:        opts.IndexPackedValuePtr,
		InternalBaseDelta:     opts.IndexInternalBaseDelta,
	}
	sysRoot, err := vacuumBuildSystemRoot(d.Pager(), reader, state.SystemRootPageID, alloc, newPager, buildOpts, collectionRootReplacements)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	userIter := tree.New(d.Pager(), reader, state.RootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	userRoot, err := bulk.BuildWithOptions(userIter, alloc, newPager, buildOpts)
	_ = userIter.Close()
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	meta := d.meta
	meta.CommitSeq++
	meta.UserRootPageID = userRoot
	meta.SystemRootPageID = sysRoot
	meta.FreelistHeadID = 0
	meta.TotalPages = newPager.PageCount()

	if err := writeMetaToPager(newPager, MetaPage0ID, meta); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	if err := writeMetaToPager(newPager, MetaPage1ID, meta); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}

	if err := newPager.Sync(); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	if fail == vacuumFailAfterNewSync {
		_ = newPager.Close()
		_ = d.Close()
		return errVacuumFailpoint
	}

	if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return err
	}
	if runtime.GOOS != "windows" {
		if dir, err := os.Open(opts.Dir); err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}
	if fail == vacuumFailAfterReady {
		_ = newPager.Close()
		_ = d.Close()
		return errVacuumFailpoint
	}

	if err := newPager.Close(); err != nil {
		_ = d.Close()
		return err
	}
	if err := d.Close(); err != nil {
		return err
	}

	// Swap in the new index file.
	_ = os.Remove(bakPath)
	if err := os.Rename(indexPath, bakPath); err != nil {
		return err
	}
	if fail == vacuumFailAfterRenameOld {
		return errVacuumFailpoint
	}
	if err := os.Rename(newPath, indexPath); err != nil {
		_ = os.Rename(bakPath, indexPath)
		return err
	}
	if fail == vacuumFailAfterRenameNew {
		return errVacuumFailpoint
	}

	_ = os.Remove(readyPath)
	_ = os.Remove(bakPath)
	if runtime.GOOS != "windows" {
		if dir, err := os.Open(opts.Dir); err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}

	return nil
}

func writeMetaToPager(p *pager.Pager, pageID uint64, meta page.MetaPageBody) error {
	data, err := p.GetForWrite(pageID)
	if err != nil {
		return err
	}
	meta.Encode(data[page.PageHeaderSize:])
	n := node.NewNode(data)
	n.SetPageID(pageID)
	n.SetType(page.PageTypeMeta)
	n.SetCount(0)
	n.UpdateChecksum()
	return nil
}
