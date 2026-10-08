package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// PhysicalSnapshotCutV1 owns a fully published index generation and both
// recovery slots. It exports the captured high-water extent, replacing bytes
// already eligible for reuse with zeroes. It never reads mutable meta slots or
// reusable pages after capture. The stable-index lease delays online vacuum;
// ordinary writes and root publication can continue.
//
// The separately opened index handle remains readable through DB.Close. A retained
// directory lock prevents a new writer from reusing pages after the source closes. The
// caller must also capture the exact external dependency files before releasing
// its higher-level storage barrier; this object exports only index.db.
type PhysicalSnapshotCutV1 struct {
	mu            sync.Mutex
	file          *os.File
	directoryLock *lockfile.Lock
	roots         *RecoverableRootSet
	generation    *freelist.PublishedGenerationLeaseV1
	meta          [2][page.PageSize]byte
	parentIDs     [2]uint64
	parents       [2][page.PageSize]byte
	oldest        uint64
	state         StateToken
}

// CapturePhysicalSnapshotCutV1 requires a completed checkpoint and refuses any
// intervening visible/unpublished work. A caller may retry checkpoint+capture;
// capture itself never waits for a publisher while holding writer admission.
func (db *DB) CapturePhysicalSnapshotCutV1(ctx context.Context) (*PhysicalSnapshotCutV1, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	unlockAdmission := db.lockCommandWALQuiescentAdmission()
	defer unlockAdmission()
	unlockPublish := db.lockCommandWALRawPublish()
	defer unlockPublish()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return nil, err
	}
	var roots *RecoverableRootSet
	var err error
	if db.readOnly {
		// Only an actual locked read-only owner can export an immutable physical
		// cut. In particular, openReadOnlyNoLock is not an export authority.
		if db.lock == nil {
			return nil, fmt.Errorf("physical snapshot requires directory lock: %w", ErrReadOnly)
		}
		roots, err = db.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx)
	} else {
		roots, err = db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
	}
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			roots.Release()
		}
	}()
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	db.rootReuseMu.Lock()
	defer db.rootReuseMu.Unlock()
	current := db.durableRoot
	if current.pending != nil || len(current.ambiguous) != 0 || current.record.CommitSeq == 0 || current.record.CommitSeq != roots.visible.CommitSeq || roots.idx != db.idx.Load() {
		return nil, fmt.Errorf("%w: physical snapshot requires fully published current root", ErrRecoverableRootSetStale)
	}
	generation, err := roots.idx.allocator.AcquirePublishedGenerationLeaseV1(current.record.Freelist)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !owned {
			generation.Close()
		}
	}()
	if generation.GenerationRefV1().HighWater != current.record.TotalPages {
		return nil, errors.New("treedb: physical snapshot allocator extent differs from root record")
	}
	oldest, err := oldestRecoverableSlotCommitV1(current.slotCommit)
	if err != nil {
		return nil, err
	}
	directoryLock, err := db.lock.Retain()
	if err != nil {
		return nil, fmt.Errorf("physical snapshot directory ownership: %w", err)
	}
	cut := &PhysicalSnapshotCutV1{roots: roots, generation: generation, oldest: oldest, state: roots.visible, directoryLock: directoryLock}
	err = roots.idx.pager.WithStableResourceFile(func(source *os.File) error {
		info, err := source.Stat()
		if err != nil {
			return err
		}
		file, err := os.Open(source.Name())
		if err != nil {
			return err
		}
		opened, err := file.Stat()
		if err != nil || !os.SameFile(info, opened) {
			return errors.Join(err, errors.New("treedb: physical snapshot index identity changed"), file.Close())
		}
		cut.file = file
		for slot := range cut.meta {
			if _, err := file.ReadAt(cut.meta[slot][:], int64(slot*page.PageSize)); err != nil {
				return err
			}
		}
		// Recovery reads exactly one parent record for each slot. The older
		// slot's parent may already be retired below the registry boundary;
		// retain these two bounded images while publication is excluded rather
		// than reading reusable bytes during deferred export.
		pageSource := &snapshotIndexPageStoreV1{file: file, pageCount: generation.GenerationRefV1().HighWater}
		for slot, record := range current.slotRecord {
			if record.CommitSeq == 0 || record.ParentRecordPageID == 0 {
				continue
			}
			if err := validateDurableRootLineageV1(pageSource, record); err != nil {
				return fmt.Errorf("physical snapshot slot %d lineage: %w", slot, err)
			}
			cut.parentIDs[slot] = record.ParentRecordPageID
			if _, err := file.ReadAt(cut.parents[slot][:], int64(record.ParentRecordPageID)*page.PageSize); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if cut.file != nil {
			err = errors.Join(err, cut.file.Close())
		}
		return nil, errors.Join(err, directoryLock.Close())
	}
	owned = true
	return cut, nil
}

func (cut *PhysicalSnapshotCutV1) StateToken() StateToken { return cut.state }
func (cut *PhysicalSnapshotCutV1) SizeBytes() int64 {
	return int64(cut.generation.GenerationRefV1().HighWater) * page.PageSize
}

// WriteToContext emits one sequential index image using a single page buffer.
// Close waits for an admitted write and releases all leases exactly once.
func (cut *PhysicalSnapshotCutV1) WriteToContext(ctx context.Context, dst io.Writer) error {
	if cut == nil {
		return ErrClosed
	}
	cut.mu.Lock()
	defer cut.mu.Unlock()
	if cut.file == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var buffer [page.PageSize]byte
	for id := uint64(0); id < cut.generation.GenerationRefV1().HighWater; id++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if id < 2 {
			buffer = cut.meta[id]
		} else if id == cut.parentIDs[0] {
			buffer = cut.parents[0]
		} else if id == cut.parentIDs[1] {
			buffer = cut.parents[1]
		} else {
			unused, err := cut.generation.SnapshotPageUnusedV1(id, cut.oldest)
			if err != nil {
				return err
			}
			if unused {
				clear(buffer[:])
			} else if _, err := cut.file.ReadAt(buffer[:], int64(id)*page.PageSize); err != nil {
				return err
			}
		}
		if n, err := dst.Write(buffer[:]); err != nil {
			return err
		} else if n != len(buffer) {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (cut *PhysicalSnapshotCutV1) Close() error {
	if cut == nil {
		return nil
	}
	cut.mu.Lock()
	defer cut.mu.Unlock()
	if cut.file == nil {
		return nil
	}
	err := cut.file.Close()
	cut.file = nil
	cut.roots.Release()
	cut.roots = nil
	err = errors.Join(err, cut.directoryLock.Close())
	cut.directoryLock = nil
	cut.generation.Close()
	return err
}

// CapturePhysicalSnapshotSideStoreV1 asks the registered store owner to flush
// and capture its index. It never opens a second owner by pathname.
func (db *DB) CapturePhysicalSnapshotSideStoreV1(ctx context.Context, name string) (*PhysicalSnapshotCutV1, error) {
	if db == nil || db.closing.Load() {
		return nil, ErrClosed
	}
	if name != "dictdb" && name != "templatedb" {
		return nil, errors.New("treedb: invalid snapshot side-store name")
	}
	capture := db.physicalSnapshotSideStoreCapture
	if capture == nil {
		return nil, fmt.Errorf("treedb: snapshot side store %q has no registered owner", name)
	}
	return capture(ctx, name)
}

// ValidateStorageDirectoryV1 binds side-file discovery to the exact directory
// containing this cut's retained index, rather than a replacement at its path.
func (cut *PhysicalSnapshotCutV1) ValidateStorageDirectoryV1(directory *os.File) error {
	if cut == nil {
		return ErrClosed
	}
	cut.mu.Lock()
	defer cut.mu.Unlock()
	if cut.file == nil {
		return ErrClosed
	}
	return rootpublication.ValidateStableChildLink(directory, cut.file, "index.db")
}
