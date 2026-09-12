package collections

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// A token exists only on the synthetic synchronous drainer while its caller
// holds schema-exclusive admission. Public and background flushes never get it.
type typedGraphReconcileToken struct{ coord *collectionSchemaCoordinator }

type typedGraphColdLimits struct {
	ManifestRecords  int
	ManifestBytes    int64
	AssetBytes       int64
	DecodedTermBytes int64 // each decoded/header/payload term, not summed process heap
}

// Reconciliation is explicit cold setup, not a query or replay side effect.
func (c *Collection) reconcileTypedGraphPublication(limits typedGraphPublicationLimits, cold typedGraphColdLimits) (err error) {
	return c.reconcileTypedGraphPublicationWithContext(context.Background(), limits, cold)
}

func (c *Collection) reconcileTypedGraphPublicationWithContext(ctx context.Context, limits typedGraphPublicationLimits, cold typedGraphColdLimits) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if limits.EncodedOutputBytes < 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	if c == nil || c.db == nil || !c.db.CommandWALEnabled() || limits.Rows <= 0 || limits.Tombstones < 0 || limits.ValueSlots <= 0 || limits.OwnedBytes <= 0 || cold.ManifestRecords <= 0 || cold.ManifestBytes <= 0 || cold.AssetBytes <= 0 || cold.DecodedTermBytes <= 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	unlock := c.lockCollectionSchemaWrite()
	defer unlock()
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	before := coord.typedPublication.Load()
	if before == nil || !before.invalid {
		// Pre-existing feature-off writes cannot be retroactively reserved.
		// Drain before enabling. Healthy initialized writes use normal receipts.
		if err := c.flushCollectionWriteDomainsForSchemaMutation(); err != nil {
			return err
		}
		before = coord.typedPublication.Load()
	}
	token := &typedGraphReconcileToken{coord: coord}
	marker := &typedGraphPublicationState{limits: limits, invalid: true, reconciling: token}
	if before != nil {
		if before.limits != limits {
			return ErrVectorIndexSnapshotMismatch
		}
		*marker = *before
		marker.invalid, marker.reconciling = true, token
	}
	marked := before == nil || before.invalid
	if marked {
		if !coord.typedPublication.CompareAndSwap(before, marker) {
			return ErrVectorIndexSnapshotMismatch
		}
	}
	defer func() {
		if state := coord.typedPublication.Load(); state != nil && state.reconciling == token {
			invalid := *state
			invalid.reconciling = nil
			coord.typedPublication.CompareAndSwap(state, &invalid)
		}
	}()
	if before != nil && before.invalid {
		for _, domain := range coord.snapshotDomains() {
			if err := flushCollectionWriteDomainWithTypedReconcile(c.db, domain, false, token); err != nil {
				return err
			}
		}
	}
	typedGraphPublicationAfterAcceptedHook.RLock()
	beforeCapture := typedGraphPublicationAfterAcceptedHook.reconcileBeforeCapture
	typedGraphPublicationAfterAcceptedHook.RUnlock()
	if beforeCapture != nil {
		beforeCapture(c)
	}
	var snap *backenddb.Snapshot
	var current *CollectionReadView
	var lease *ColumnAssetLifecyclePinSet
	var suffix typedGraphOverlaySuffix
	defer func() {
		if current != nil {
			_ = current.Close()
		}
		if lease != nil {
			_ = lease.Close()
		}
		if snap != nil {
			_ = snap.Close()
		}
	}()
	err = WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		snap = c.db.AcquireSnapshot()
		if snap == nil {
			return backenddb.ErrClosed
		}
		catalog, err := loadCollectionCatalog(snap, c.collectionName())
		if err != nil {
			return err
		}
		if catalog == nil || catalog.typedGraphBase == nil || catalog.meta.Options.ColumnStore == nil || len(catalog.meta.VectorIndexes) != 1 {
			return ErrVectorIndexSnapshotMismatch
		}
		if before != nil && !before.invalid && before.matches(catalog) {
			// No pending work and same exact authority: no manifest/asset decode.
			return nil
		}
		if !marked {
			if !coord.typedPublication.CompareAndSwap(before, marker) {
				return ErrVectorIndexSnapshotMismatch
			}
			marked = true
		}
		cfg := *catalog.meta.Options.ColumnStore
		if len(cfg.Columns) == 0 || len(cfg.Columns) > limits.ValueSlots {
			return errTypedGraphOverlayFoldNeeded
		}
		base := catalog.typedGraphBase
		root := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
		baseRoot := base.roots[collectionColumnManifestRootName(catalog.meta.Name)]
		if err := validateTypedGraphColdManifestBudget(ctx, snap, root, cold); err != nil {
			return err
		}
		if baseRoot != root {
			if err := validateTypedGraphColdManifestBudget(ctx, snap, baseRoot, cold); err != nil {
				return err
			}
		}
		baseCatalog, err := base.catalog(c, snap)
		if err != nil {
			return err
		}
		if !typedGraphBaseSchemaMatches(base.meta, catalog.meta) {
			return ErrVectorIndexSnapshotMismatch
		}
		baseView, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(snap, baseCatalog, catalog.meta.Name, baseRoot, *base.meta.Options.ColumnStore, true, columnManifestScanNoSidecars())
		if err != nil {
			return err
		}
		view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(snap, catalog, catalog.meta.Name, root, cfg, true, columnManifestScanNoSidecars())
		if err != nil {
			return err
		}
		key := columnVectorGraphManifestRecordKey(catalog.meta.VectorIndexes[0].Name)
		oldGraph, err := snap.GetAtRoot(baseRoot, key)
		if err != nil {
			return err
		}
		newGraph, err := snap.GetAtRoot(root, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(oldGraph, newGraph) {
			return ErrVectorIndexSnapshotMismatch
		}
		suffix, err = checkedTypedGraphOverlaySuffix(baseView, view, typedGraphOverlayLimits{Rows: limits.Rows, Tombstones: limits.Tombstones, Bytes: cold.AssetBytes})
		if err != nil {
			return err
		}
		if suffix.rows > limits.ValueSlots/len(cfg.Columns) {
			return errTypedGraphOverlayFoldNeeded
		}
		// Bound decoder/header working space separately from persisted bytes.
		// FP32 planes are checked again by prepareRows before their allocation.
		if int64(limits.ValueSlots) > cold.DecodedTermBytes/int64(typedGraphDeclaredValueHeaderBytes()) {
			return errTypedGraphOverlayFoldNeeded
		}
		if int64(suffix.rows) > cold.DecodedTermBytes/int64(reflect.TypeFor[columnPhysicalVisibleRow]().Size()) {
			return errTypedGraphOverlayFoldNeeded
		}
		leaseView := suffix.view
		leaseView.GraphAssetRefs = nil // no lazy graph consumer in cold row preparation
		refs := columnPhysicalScanSnapshotViewAssetRefs(leaseView)
		lease, err = c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "typed_graph_cold_suffix", Refs: refs})
		if err != nil {
			return err
		}
		current = newCollectionReadViewAtSnapshot(c, snap, catalog, false, "")
		return nil
	})
	if err != nil {
		return err
	}
	if coord.typedPublication.Load() == before {
		return nil
	}
	var cost typedGraphPublicationCost
	rows, err := suffix.prepareRowsWithAccounting(current, min(limits.OwnedBytes, cold.DecodedTermBytes), limits.ValueSlots, &cost)
	if err != nil {
		return err
	}
	if cost.rows > limits.Rows || cost.tombstones > limits.Tombstones || cost.slots > limits.ValueSlots || cost.bytes > limits.OwnedBytes {
		return errTypedGraphOverlayFoldNeeded
	}
	slices.SortFunc(rows, func(a, b columnPhysicalVisibleRow) int { return bytes.Compare(a.ID, b.ID) })
	next := &typedGraphPublicationState{catalog: current.catalog, limits: limits, rows: rows, invNorms: make([]float32, len(rows)), physicalRows: cost.rows, tombstones: cost.tombstones, valueSlots: cost.slots, admittedPayloadBytes: cost.bytes, installedAssetBytes: suffix.bytes}
	if err := next.prepareEncodedBounds(); err != nil {
		return err
	}
	vectorColumn := -1
	for i, column := range current.catalog.meta.Options.ColumnStore.Columns {
		if column.Path == current.catalog.meta.VectorIndexes[0].Field {
			vectorColumn = i
			break
		}
	}
	if vectorColumn < 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	for i, row := range rows {
		if row.Deleted {
			continue
		}
		next.invNorms[i], err = columnVectorGraphInvNorm(row.Values[vectorColumn].Float32Vector)
		if err != nil {
			return err
		}
	}
	return WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		check := c.db.AcquireSnapshot()
		if check == nil {
			return backenddb.ErrClosed
		}
		latest, checkErr := loadCollectionCatalog(check, current.catalog.meta.Name)
		_ = check.Close()
		if checkErr != nil {
			return checkErr
		}
		if !next.matches(latest) {
			return ErrVectorIndexSnapshotMismatch
		}
		coord.typedPublicationDebtMu.Lock()
		defer coord.typedPublicationDebtMu.Unlock()
		if !coord.typedPublication.CompareAndSwap(marker, next) {
			return ErrVectorIndexSnapshotMismatch
		}
		coord.typedPublicationDebt = cost
		coord.typedPublicationPending = typedGraphPublicationCost{}
		coord.typedPublicationBuffered = 0
		return nil
	})
}

func typedGraphDeclaredValueHeaderBytes() uintptr {
	return reflect.TypeFor[columnDeclaredValue]().Size()
}

func validateTypedGraphColdManifestBudget(ctx context.Context, snap *backenddb.Snapshot, root uint64, limits typedGraphColdLimits) error {
	err := validateColumnManifestScanBudget(ctx, snap, root, limits.ManifestRecords, limits.ManifestBytes)
	if errors.Is(err, ErrColumnAssetReachabilityManifestLimit) {
		return errTypedGraphOverlayFoldNeeded
	}
	return err
}
