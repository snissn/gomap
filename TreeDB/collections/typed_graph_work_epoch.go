package collections

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Renewable work admission is deliberately distinct from a lifetime disk
// quota. Native residual pressure requires explicit native maintenance; this
// entrypoint never invokes an unbounded CompactStorage scan.
type typedGraphWorkEpochLimits struct {
	NativeEntries, ColumnSegments, ManifestRecords, LifecycleEntries int
	NativeBytes, ColumnBytes, ManifestBytes, RetainedBytes           int64
	PagerPages                                                       uint64
}

type typedGraphWorkEpochStats struct {
	Epoch         uint64
	Native        typedGraphNativeResidual
	Pager         backenddb.IndexVacuumFreelistDebtSnapshot
	Columns       ColumnAssetGCStats
	RetainedBytes int64
}

func (c *Collection) renewTypedGraphWorkEpoch(ctx context.Context, limits typedGraphWorkEpochLimits) (out typedGraphWorkEpochStats, err error) {
	if c == nil || c.db == nil || ctx == nil || limits.NativeEntries <= 0 || limits.ColumnSegments <= 0 || limits.ManifestRecords <= 0 || limits.LifecycleEntries <= 0 || limits.NativeBytes <= 0 || limits.ColumnBytes <= 0 || limits.ManifestBytes <= 0 || limits.RetainedBytes <= 0 || limits.PagerPages == 0 {
		return out, ErrVectorIndexSnapshotMismatch
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil || !coord.typedGraphFoldActive.CompareAndSwap(false, true) {
		return out, ErrConcurrentMutation
	}
	defer coord.typedGraphFoldActive.Store(false)
	coord.typedPublicationDebtMu.Lock()
	if coord.typedGraphWorkEpochLimits == nil {
		copy := limits
		coord.typedGraphWorkEpochLimits = &copy
	}
	match := *coord.typedGraphWorkEpochLimits == limits
	coord.typedPublicationDebtMu.Unlock()
	if !match {
		return out, ErrConcurrentMutation
	}
	unlock := c.lockCollectionSchemaWrite()
	defer unlock()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
		return out, err
	}
	err = WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		mutation := c.lockMutation()
		defer mutation.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.db.CheckStorageMaintenanceReady(); err != nil {
			return err
		}
		// Partition lifecycle has separate decoders and budgets. This selected
		// column_graph path must not implicitly admit them into this envelope.
		partitionDir := filepath.Join(c.db.Dir(), "vector_partitions")
		if dir, err := os.Open(partitionDir); err == nil {
			entries, readErr := dir.ReadDir(1)
			closeErr := dir.Close()
			if len(entries) != 0 {
				return ErrHybridSearchUnsupported
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Finite directory admission precedes any reclamation call. Byte
		// admission also conservatively includes column files nested in Dir.
		var err error
		out.Native, err = inspectTypedGraphNativeResidual(ctx, c.db.Dir(), limits.NativeEntries, limits.NativeBytes)
		if err != nil {
			return err
		}
		var ok bool
		out.Pager, ok = c.db.IndexVacuumFreelistDebtSnapshot()
		if !ok {
			return backenddb.ErrClosed
		}
		if out.Pager.TotalPages > limits.PagerPages {
			return errTypedGraphOverlayFoldNeeded
		}
		coord.typedPublicationDebtMu.Lock()
		err = typedGraphWorkEpochRetainedAdmission(coord, limits, &out)
		coord.typedPublicationDebtMu.Unlock()
		if err != nil {
			return err
		}
		out.Columns, err = c.columnAssetGC(ctx, ColumnAssetGCOptions{
			MaxSegmentEntries: limits.ColumnSegments, MaxManifestRecords: limits.ManifestRecords,
			MaxManifestBytes: limits.ManifestBytes, MaxLifecycleEntries: limits.LifecycleEntries,
			maxReplayAssetBytes: limits.ColumnBytes,
		})
		if err != nil {
			return err
		}
		if err := c.pruneTypedGraphRetiredCandidates(limits.LifecycleEntries); err != nil {
			return err
		}
		plan, err := c.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{
			MaxSegmentEntries: limits.ColumnSegments, MaxManifestRecords: limits.ManifestRecords,
			MaxManifestBytes: limits.ManifestBytes, MaxLifecycleEntries: limits.LifecycleEntries,
		})
		if err != nil {
			return err
		}
		if !plan.Complete || plan.Segments.BytesTotal > limits.ColumnBytes {
			return errTypedGraphOverlayFoldNeeded
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		coord.typedPublicationDebtMu.Lock()
		defer coord.typedPublicationDebtMu.Unlock()
		if err := typedGraphWorkEpochRetainedAdmission(coord, limits, &out); err != nil {
			return err
		}
		if coord.typedGraphWorkEpoch == ^uint64(0) {
			return ErrVectorIndexSnapshotMismatch
		}
		coord.typedGraphWorkEpoch++
		out.Epoch = coord.typedGraphWorkEpoch
		coord.typedPublicationEncodedBytes = 0
		coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts = 0, 0
		return nil
	})
	return out, err
}

// The fold exclusion owns this transition; ordinary unconfigured fold callers
// retain their previous behavior. The existing registry owns the record until
// successful GC proves its files absent, not until the capture lease closes.
func (c *Collection) retainTypedGraphRetiredCandidates(refs []ColumnAssetRef) error {
	coord := c.collectionSchemaCoordinator()
	coord.typedPublicationDebtMu.Lock()
	limits := coord.typedGraphWorkEpochLimits
	coord.typedPublicationDebtMu.Unlock()
	if limits == nil || len(refs) == 0 {
		return nil
	}
	records, err := c.columnAssetLifecycleRegistrySnapshotWithLimit(limits.LifecycleEntries)
	if err != nil {
		return err
	}
	remaining := limits.LifecycleEntries
	for _, record := range records {
		if err := consumeColumnAssetLifecycleEntries(&remaining, 1, len(record.Refs), len(record.Segments)); err != nil {
			return err
		}
	}
	if err := consumeColumnAssetLifecycleEntries(&remaining, 1, len(refs)); err != nil {
		return err
	}
	_, err = c.registerColumnAssetLifecycleRecord(columnAssetLifecycleRegistryRetired, "typed_graph_epoch", "captured_fold", "retained cleanup provenance", refs, nil)
	return err
}

// Called only after successful GC, while fold/schema/storage/mutation authority
// is still held. Mixed/pinned survivors retain their candidate provenance.
func (c *Collection) pruneTypedGraphRetiredCandidates(maxEntries int) error {
	records, err := c.columnAssetLifecycleRegistrySnapshotWithLimit(maxEntries)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Class != columnAssetLifecycleRegistryRetired {
			continue
		}
		kept := record.Refs[:0]
		var bytes int64
		for _, ref := range record.Refs {
			path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), ref)
			if err != nil {
				return err
			}
			_, err = os.Lstat(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			kept = append(kept, ref)
			bytes = addColumnAssetReachabilityBytes(bytes, positiveColumnAssetReachabilityLength(ref.Length))
		}
		if len(kept) == len(record.Refs) {
			continue
		}
		columnAssetLifecycleProcessRegistries.Lock()
		if current, ok := columnAssetLifecycleProcessRegistries.records[record.ID]; ok && current.Class == columnAssetLifecycleRegistryRetired {
			if len(kept) == 0 {
				delete(columnAssetLifecycleProcessRegistries.records, record.ID)
			} else {
				current.Refs = make([]ColumnAssetRef, len(kept))
				copy(current.Refs, kept)
				current.RefBytes = bytes
				columnAssetLifecycleProcessRegistries.records[record.ID] = current
			}
		}
		columnAssetLifecycleProcessRegistries.Unlock()
	}
	return nil
}

// The debt lock is held by the caller. Owner accounting is nested only for
// arithmetic, never while closing resources or entering a backend operation.
func typedGraphWorkEpochRetainedAdmission(coord *collectionSchemaCoordinator, limits typedGraphWorkEpochLimits, out *typedGraphWorkEpochStats) error {
	state := coord.typedPublication.Load()
	if state == nil || state.invalid || state.reconciling != nil || coord.typedPublicationPending != (typedGraphPublicationCost{}) {
		return ErrVectorIndexSnapshotMismatch
	}
	n := int64(0)
	charge := func(count, size int64) bool {
		if count < 0 || size <= 0 || count > (limits.RetainedBytes-n)/size {
			return false
		}
		n += count * size
		return true
	}
	if !charge(state.servingMetadataBytes, 1) || !charge(state.admittedPayloadBytes, 1) || !charge(1, int64(reflect.TypeFor[typedGraphPublicationState]().Size())) || !charge(int64(cap(state.rows)), int64(reflect.TypeFor[columnPhysicalVisibleRow]().Size())) || !charge(int64(state.valueSlots), int64(reflect.TypeFor[columnDeclaredValue]().Size())) || !charge(int64(cap(state.invNorms)), 4) {
		return errTypedGraphOwnerBudget
	}
	a := &coord.typedGraphOwners
	a.Lock()
	defer a.Unlock()
	for _, term := range []int64{a.stateBytes, a.assetBytes, a.baseAssetBytes, a.baseDescriptorBytes, a.baseBackingBytes} {
		if !charge(term, 1) {
			return errTypedGraphOwnerBudget
		}
	}
	out.RetainedBytes = n
	return nil
}

// File lengths are residual maintenance telemetry, not allocated blocks or a
// physical amplification bound for encoded work. Unknown and empty entries
// count too. The enclosing lifecycle also inventories the column asset root.
type typedGraphNativeResidual struct {
	Entries int
	Bytes   int64
}

func inspectTypedGraphNativeResidual(ctx context.Context, root string, maxEntries int, maxBytes int64) (out typedGraphNativeResidual, err error) {
	if maxEntries <= 0 || maxBytes <= 0 {
		return out, ErrVectorIndexSnapshotMismatch
	}
	var visit func(string, int) error
	visit = func(path string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Bound open descriptors and recursive directory headers independently
		// of caller entry limits. TreeDB's ordinary layout is much shallower.
		if depth > 32 {
			return errTypedGraphOverlayFoldNeeded
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrHybridSearchUnsupported
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
			batch, readErr := dir.ReadDir(64)
			for _, entry := range batch {
				if out.Entries >= maxEntries {
					return errTypedGraphOverlayFoldNeeded
				}
				out.Entries++
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if info.Mode()&os.ModeSymlink != 0 {
					return ErrHybridSearchUnsupported
				}
				if info.IsDir() {
					if err := visit(filepath.Join(path, entry.Name()), depth+1); err != nil {
						return err
					}
				} else {
					if !info.Mode().IsRegular() {
						return ErrHybridSearchUnsupported
					}
					if info.Size() < 0 || info.Size() > maxBytes-out.Bytes {
						return errTypedGraphOverlayFoldNeeded
					}
					out.Bytes += info.Size()
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
	err = visit(root, 0)
	return out, err
}
