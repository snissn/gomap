package collections

import (
	"context"
	"errors"
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

var errTypedGraphServingCurrentKeyChanged = errors.New("collections: typed graph serving key changed during holder acquisition")

// typedGraphServingPinProof certifies that one registered lifecycle pin owns a
// publication closure containing the exact base closure named by key. It is
// minted only after publication or snapshot validation and is checked against
// the live registry on every holder acquisition.
type typedGraphServingPinProof struct {
	// key deliberately omits key.db so a quarantined cleanup guardian cannot
	// retain a closed DB. dbID preserves the instance identity, and live use
	// still requires the caller's DB pointer to resolve to that exact ID.
	key  columnVectorGraphSharedPreparedSearchKey
	id   uint64
	dbID uint64
}

func typedGraphServingCurrentKeyChanged() error {
	return errors.Join(errTypedGraphServingCurrentKeyChanged, ErrVectorIndexSnapshotMismatch)
}

// typedGraphServingHolderCapability is the only serving-holder reference
// exposed outside the cache. Every ref owns its caller's complete lifecycle
// pin; the holder's lease set separately owns its exact-base guardian.
type typedGraphServingHolderCapability struct {
	ref *columnVectorGraphSharedPreparedSearchRef
	pin *ColumnAssetLifecyclePinSet

	once     sync.Once
	closeErr error
}

func (c *typedGraphServingHolderCapability) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		// Logical holder resources and its pool close before the caller pin. A
		// failed physical close retains the exact-base guardian, never this
		// caller's possibly broader pin.
		c.closeErr = errors.Join(c.ref.release(), c.pin.Close())
		c.ref, c.pin = nil, nil
	})
	return c.closeErr
}

func typedGraphServingPinContainsBase(baseRefs, pinRefs []ColumnAssetRef) error {
	if len(baseRefs) == 0 || len(pinRefs) < len(baseRefs) {
		return fmt.Errorf("collections: serving holder pin does not contain the base closure: %w", ErrVectorIndexSnapshotMismatch)
	}
	pinned := make(map[ColumnAssetRef]struct{}, len(pinRefs))
	for i, ref := range pinRefs {
		if err := validateColumnAssetRefForPlan(ref); err != nil {
			return fmt.Errorf("collections: serving holder pin ref[%d]: %w", i, err)
		}
		pinned[ref] = struct{}{}
	}
	for _, ref := range baseRefs {
		if _, ok := pinned[ref]; !ok {
			return fmt.Errorf("collections: serving holder pin omits base ref %+v: %w", ref, ErrVectorIndexSnapshotMismatch)
		}
	}
	return nil
}

func (p *ColumnAssetLifecyclePinSet) bindTypedGraphServingKey(key columnVectorGraphSharedPreparedSearchKey) error {
	if p == nil || !key.valid() || key.family != columnVectorGraphSharedPreparedSearchKeyServing {
		return ErrVectorIndexSnapshotMismatch
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.id == 0 || p.source != ColumnAssetLifecyclePinSourcePreparedQuery || p.servingProof != (typedGraphServingPinProof{}) {
		return ErrVectorIndexSnapshotMismatch
	}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	dbID := columnAssetLifecycleProcessPins.dbIDs[key.db]
	record, ok := columnAssetLifecycleProcessPins.pins[p.id]
	if !ok || dbID == 0 || record.Scope.dbID != dbID || record.Scope.collection != key.collection || record.Scope.namespace != key.namespace || record.Source != p.source {
		return ErrVectorIndexSnapshotMismatch
	}
	key.db = nil
	p.servingProof = typedGraphServingPinProof{key: key, id: p.id, dbID: dbID}
	return nil
}

func (p *ColumnAssetLifecyclePinSet) authorizesTypedGraphServingKey(key columnVectorGraphSharedPreparedSearchKey) bool {
	if p == nil || !key.valid() || key.family != columnVectorGraphSharedPreparedSearchKeyServing {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	proof := p.servingProof
	keyIdentity := key
	keyIdentity.db = nil
	if p.closed || p.id == 0 || p.source != ColumnAssetLifecyclePinSourcePreparedQuery || proof.key != keyIdentity || proof.id != p.id || proof.dbID == 0 {
		return false
	}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	record, ok := columnAssetLifecycleProcessPins.pins[p.id]
	return ok && columnAssetLifecycleProcessPins.dbIDs[key.db] == proof.dbID && record.Scope.dbID == proof.dbID && record.Scope.collection == key.collection && record.Scope.namespace == key.namespace && record.Source == p.source
}

// acquireTypedGraphServingLifecyclePin registers the immutable publication
// closure without cloning or revalidating it on every request. prepareServingRefs
// established both the complete-base certificate and cached byte charge before
// the state became visible; arbitrary lifecycle-pin callers cannot enter here.
func (c *Collection) acquireTypedGraphServingLifecyclePin(state *typedGraphPublicationState, key columnVectorGraphSharedPreparedSearchKey, owner string) (*ColumnAssetLifecyclePinSet, error) {
	if c == nil || c.db == nil || state == nil || !state.servingAdmitted || state.servingBase == nil || owner == "" || key.db != c.db || key.collection != c.collectionName() || key.namespace == "" || state.servingBaseRefsDigest != key.refsDigest || state.servingBaseRefsCount != key.refsCount || len(state.servingRefs) < key.refsCount || state.servingPinBytes <= 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	current, err := state.servingBase.servingPreparedSearchKey(c)
	if err != nil || current != key {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	opts := ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: owner, Refs: state.servingRefs}
	scope, err := c.columnAssetLifecyclePinSetScope(opts)
	if err != nil || scope.collection != key.collection || scope.namespace != key.namespace {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	pin, err := c.registerColumnAssetLifecyclePinSetOwned(opts, scope, state.servingPinBytes)
	if err != nil {
		return nil, err
	}
	if err := pin.bindTypedGraphServingKey(key); err != nil {
		return nil, errors.Join(err, pin.Close())
	}
	return pin, nil
}

// typedGraphServingHolderBuild is intentionally supplied a builder-owned DB
// snapshot and the current installed metadata that passed the full-key
// handshake. Production builders must consume only these arguments; in
// particular they must not close over a request snapshot or decoded catalog.
type typedGraphServingHolderBuild func(*backenddb.Snapshot, *typedGraphServingBaseMetadata, *columnServingSegmentLeaseSet) (*columnVectorGraphSharedPreparedSearch, error)

// acquireServingHolder is the keeper entry point. It first acquires an exact
// caller pin and snapshot token through the same current-publication handshake
// used by the holder builder, then transfers that pin into the capability. The
// holder build, if needed, performs its own independent snapshot handshake.
func (b *typedGraphServingBaseMetadata) acquireServingHolder(
	ctx context.Context,
	c *Collection,
	pinRefs []ColumnAssetRef,
	pinOwner string,
	limits typedGraphPhysicalResourceLimits,
) (*typedGraphServingHolderCapability, uint64, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, 0, err
	}
	if b == nil || c == nil || c.db == nil || pinOwner == "" {
		return nil, 0, 0, ErrVectorIndexSnapshotMismatch
	}
	key, err := b.servingPreparedSearchKey(c)
	if err != nil {
		return nil, 0, 0, err
	}
	snap, _, pin, err := c.acquireColumnVectorGraphServingPinnedSnapshot(ctx, key, pinRefs, pinOwner, true)
	if err != nil {
		return nil, 0, 0, err
	}
	token, ok := snap.StateToken()
	if !ok {
		return nil, 0, 0, errors.Join(backenddb.ErrClosed, snap.Close(), pin.Close())
	}
	if closeErr := snap.Close(); closeErr != nil {
		return nil, 0, 0, errors.Join(closeErr, pin.Close())
	}
	capability, err := b.acquireServingHolderWithOwnedPinAndBuildMode(ctx, c, pin, limits, true, func(snap *backenddb.Snapshot, current *typedGraphServingBaseMetadata, pool *columnServingSegmentLeaseSet) (*columnVectorGraphSharedPreparedSearch, error) {
		return c.buildColumnVectorGraphServingPreparedSearchFromBase(snap, current, pool)
	})
	if err != nil {
		return nil, 0, 0, err
	}
	return capability, token.CommitSeq, token.SystemRootPageID, nil
}

// acquireServingHolderWithOwnedPin is the request-owner entry point. pin must
// already have been acquired under the caller snapshot's schema/storage gate.
// Ownership transfers on entry and is released on every failure path.
func (b *typedGraphServingBaseMetadata) acquireServingHolderWithOwnedPin(ctx context.Context, c *Collection, pin *ColumnAssetLifecyclePinSet, limits typedGraphPhysicalResourceLimits) (*typedGraphServingHolderCapability, error) {
	return b.acquireServingHolderWithOwnedPinAndBuildMode(ctx, c, pin, limits, false, func(snap *backenddb.Snapshot, current *typedGraphServingBaseMetadata, pool *columnServingSegmentLeaseSet) (*columnVectorGraphSharedPreparedSearch, error) {
		return c.buildColumnVectorGraphServingPreparedSearchFromBase(snap, current, pool)
	})
}

func (b *typedGraphServingBaseMetadata) acquireServingHolderWithOwnedPinAndBuild(ctx context.Context, c *Collection, pin *ColumnAssetLifecyclePinSet, limits typedGraphPhysicalResourceLimits, build typedGraphServingHolderBuild) (*typedGraphServingHolderCapability, error) {
	return b.acquireServingHolderWithOwnedPinAndBuildMode(ctx, c, pin, limits, false, build)
}

func (b *typedGraphServingBaseMetadata) acquireServingHolderWithOwnedPinAndBuildMode(ctx context.Context, c *Collection, pin *ColumnAssetLifecyclePinSet, limits typedGraphPhysicalResourceLimits, admissionBuild bool, build typedGraphServingHolderBuild) (*typedGraphServingHolderCapability, error) {
	if b == nil || c == nil || c.db == nil || c.db.IsClosing() || pin == nil || build == nil {
		return nil, errors.Join(ErrVectorIndexSnapshotMismatch, pin.Close())
	}
	key, err := b.servingPreparedSearchKey(c)
	if err != nil {
		return nil, errors.Join(err, pin.Close())
	}
	if !pin.authorizesTypedGraphServingKey(key) {
		return nil, errors.Join(ErrVectorIndexSnapshotMismatch, pin.Close())
	}
	ref, err := c.acquireColumnVectorGraphServingPreparedSearch(ctx, key, limits, admissionBuild, build)
	if err != nil {
		return nil, errors.Join(err, pin.Close())
	}
	return &typedGraphServingHolderCapability{ref: ref, pin: pin}, nil
}

// acquireColumnVectorGraphServingPreparedSearch uses a synchronous first
// builder. That builder ignores request cancellation after the initial check,
// owns its own validated snapshot, and completes construction or rollback
// before the first miss observes cancellation. Later waiters select on ctx and
// may leave promptly without affecting the builder.
func (c *Collection) acquireColumnVectorGraphServingPreparedSearch(
	ctx context.Context,
	key columnVectorGraphSharedPreparedSearchKey,
	limits typedGraphPhysicalResourceLimits,
	admissionBuild bool,
	build typedGraphServingHolderBuild,
) (*columnVectorGraphSharedPreparedSearchRef, error) {
	if c == nil || c.db == nil || (c.manager != nil && c.manager.isClosing()) || key.family != columnVectorGraphSharedPreparedSearchKeyServing || !key.valid() || key.db != c.db || key.collection != c.collectionName() || build == nil || limits.Segments <= 0 || limits.Descriptors <= 0 || limits.MappedBytes <= 0 || limits.FallbackBytes <= 0 || limits.InventoryBytes <= 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	root, err := normalizeColumnServingRoot(c.db.ColumnAssetRootDir())
	if err != nil || root != key.assetRoot {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.vectorPreparedSearchMu.Lock()
	if c.vectorPreparedSearch == nil {
		c.vectorPreparedSearch = make(map[columnVectorGraphSharedPreparedSearchKey]*columnVectorGraphSharedPreparedSearchCacheEntry)
	}
	entry := c.vectorPreparedSearch[key]
	if entry == nil {
		entry = &columnVectorGraphSharedPreparedSearchCacheEntry{
			ready: make(chan struct{}), building: true, serving: true, waiters: 1, servingLimits: limits,
		}
		c.vectorPreparedSearch[key] = entry
		c.vectorPreparedSearchMisses++
		c.vectorPreparedSearchBuilds++
		c.vectorPreparedSearchMu.Unlock()

		holder, buildErr := c.buildColumnVectorGraphServingPreparedSearch(key, limits, admissionBuild, build)
		c.finishColumnVectorGraphServingPreparedSearchBuild(key, entry, holder, buildErr)
		return c.waitColumnVectorGraphServingPreparedSearch(ctx, key, entry)
	}
	if !entry.serving || entry.servingLimits != limits {
		c.vectorPreparedSearchMu.Unlock()
		return nil, ErrVectorIndexSnapshotMismatch
	}
	entry.waiters++
	if entry.building {
		c.vectorPreparedSearchWaits++
	} else {
		c.vectorPreparedSearchHits++
	}
	c.vectorPreparedSearchMu.Unlock()
	return c.waitColumnVectorGraphServingPreparedSearch(ctx, key, entry)
}

// acquireColumnVectorGraphServingPinnedSnapshot performs the common authority
// handoff for caller pins and the builder guardian. Schema admission plus the
// storage barrier protect snapshot capture and pin registration. The snapshot
// catalog must match one ready installed publication; full-key equality, not
// pointer equality, permits relocation to rebind equivalent current metadata.
// A nil pinRefs requests the current base's exact closure (the guardian case).
func (c *Collection) acquireColumnVectorGraphServingPinnedSnapshot(ctx context.Context, key columnVectorGraphSharedPreparedSearchKey, pinRefs []ColumnAssetRef, pinOwner string, admissionBuild bool) (_ *backenddb.Snapshot, _ *typedGraphServingBaseMetadata, _ *ColumnAssetLifecyclePinSet, err error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil || pinOwner == "" || c.db.IsClosing() || (c.manager != nil && c.manager.isClosing()) {
		return nil, nil, nil, ErrVectorIndexSnapshotMismatch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	var snap *backenddb.Snapshot
	var base *typedGraphServingBaseMetadata
	var pin *ColumnAssetLifecyclePinSet
	err = WithVectorPartitionStorageBarrierWithContextV1(ctx, c.db.Dir(), func() error {
		if c.db.IsClosing() || (c.manager != nil && c.manager.isClosing()) {
			return backenddb.ErrClosed
		}
		candidate := c.db.AcquireSnapshot()
		if candidate == nil {
			return backenddb.ErrClosed
		}
		cleanupCandidate := true
		defer func() {
			if cleanupCandidate {
				_ = candidate.Close()
			}
		}()
		if _, ok := candidate.StateToken(); !ok {
			return backenddb.ErrClosed
		}
		catalog, loadErr := loadCollectionCatalog(candidate, c.collectionName())
		if loadErr != nil {
			return loadErr
		}
		state := coord.typedPublication.Load()
		if state == nil || state.invalid || (!admissionBuild && !state.servingAdmitted) || state.servingBase == nil || !state.matches(catalog) {
			return ErrVectorIndexSnapshotMismatch
		}
		currentKey, keyErr := state.servingBase.servingPreparedSearchKey(c)
		if keyErr != nil {
			return ErrVectorIndexSnapshotMismatch
		}
		if currentKey != key {
			return typedGraphServingCurrentKeyChanged()
		}
		digest, digestErr := digestTypedGraphServingBaseRefs(state.servingBase.refs)
		if digestErr != nil || digest != key.refsDigest {
			return ErrVectorIndexSnapshotMismatch
		}
		ownedRefs := pinRefs
		if ownedRefs == nil {
			ownedRefs = state.servingBase.refs
		}
		if pinErr := typedGraphServingPinContainsBase(state.servingBase.refs, ownedRefs); pinErr != nil {
			return pinErr
		}
		ownedRefs = append([]ColumnAssetRef(nil), ownedRefs...)
		guard, pinErr := c.acquireColumnAssetLifecyclePinSetOwned(ColumnAssetLifecyclePinSetOptions{
			Source: ColumnAssetLifecyclePinSourcePreparedQuery,
			Owner:  pinOwner,
			Refs:   ownedRefs,
		})
		if pinErr != nil {
			return pinErr
		}
		// The read lock/barrier prevents destructive retirement until this exact
		// pin is registered. Recheck the publication after registration so no
		// unvalidated metadata can escape the handshake.
		latest := coord.typedPublication.Load()
		if latest == nil || latest.invalid || (!admissionBuild && !latest.servingAdmitted) || latest.servingBase == nil || !latest.matches(catalog) || c.db.IsClosing() || (c.manager != nil && c.manager.isClosing()) {
			_ = guard.Close()
			return ErrVectorIndexSnapshotMismatch
		}
		latestKey, keyErr := latest.servingBase.servingPreparedSearchKey(c)
		if keyErr != nil {
			_ = guard.Close()
			return ErrVectorIndexSnapshotMismatch
		}
		if latestKey != key {
			_ = guard.Close()
			return typedGraphServingCurrentKeyChanged()
		}
		if typedGraphServingPinContainsBase(latest.servingBase.refs, ownedRefs) != nil {
			_ = guard.Close()
			return ErrVectorIndexSnapshotMismatch
		}
		if bindErr := guard.bindTypedGraphServingKey(key); bindErr != nil {
			_ = guard.Close()
			return bindErr
		}
		snap, base, pin = candidate, latest.servingBase, guard
		cleanupCandidate = false
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return snap, base, pin, nil
}

func (c *Collection) acquireColumnVectorGraphServingBuildContext(key columnVectorGraphSharedPreparedSearchKey, admissionBuild bool) (*backenddb.Snapshot, *typedGraphServingBaseMetadata, *ColumnAssetLifecyclePinSet, error) {
	// The first cache-miss caller may be canceled while construction runs. The
	// synchronous builder nevertheless completes from its own background-lived
	// snapshot and exact guardian; only the caller's later wait observes cancel.
	return c.acquireColumnVectorGraphServingPinnedSnapshot(context.Background(), key, nil, "typed_graph_serving_holder_guardian", admissionBuild)
}

func (c *Collection) buildColumnVectorGraphServingPreparedSearchFromBase(snap *backenddb.Snapshot, base *typedGraphServingBaseMetadata, pool *columnServingSegmentLeaseSet) (*columnVectorGraphSharedPreparedSearch, error) {
	if c == nil || c.db == nil || snap == nil || base == nil || pool == nil || base.view.Catalog == nil || len(base.view.Catalog.meta.VectorIndexes) != 1 || base.preparedKey == "" || !pool.matchesAuthority(c.db.ColumnAssetRootDir(), base.view.AssetNamespace, base.refs) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	view := base.view
	view.snapshot = snap
	access, err := newColumnVectorGraphServingSourceAccess(context.Background(), pool)
	if err != nil {
		return nil, err
	}
	return c.buildColumnVectorGraphSharedPreparedSearchFromViewWithSourceAccess(snap, view.Catalog.meta.VectorIndexes[0], base.graph, view, access)
}

func (c *Collection) buildColumnVectorGraphServingPreparedSearch(
	key columnVectorGraphSharedPreparedSearchKey,
	limits typedGraphPhysicalResourceLimits,
	admissionBuild bool,
	build typedGraphServingHolderBuild,
) (_ *columnVectorGraphSharedPreparedSearch, err error) {
	snap, base, guardian, err := c.acquireColumnVectorGraphServingBuildContext(key, admissionBuild)
	if err != nil {
		return nil, err
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, errors.Join(ErrVectorIndexSnapshotMismatch, snap.Close(), guardian.Close())
	}
	pool, err := newColumnServingSegmentLeaseSet(key, base.refs, guardian, &coord.typedGraphPhysical, limits)
	if err != nil {
		return nil, errors.Join(err, snap.Close(), guardian.Close())
	}
	guardian = nil // pool now owns the exact-base guardian
	holder, buildErr := build(snap, base, pool)
	poolAttached := false
	if buildErr == nil && holder == nil {
		buildErr = errors.New("collections: serving prepared build returned nil holder")
	}
	if buildErr == nil {
		if holder.servingSegments != nil {
			buildErr = errors.New("collections: serving prepared builder tried to replace its physical pool")
		} else {
			holder.servingSegments = pool
			poolAttached = true
			holder.key = key
			if !holder.ready() {
				buildErr = errColumnVectorGraphSharedPreparedSearchNotEligible
			}
		}
	}
	if buildErr == nil && (c.db.IsClosing() || (c.manager != nil && c.manager.isClosing())) {
		buildErr = backenddb.ErrClosed
	}
	buildErr = errors.Join(buildErr, snap.Close())
	if buildErr == nil {
		return holder, nil
	}

	var cleanupErr error
	retained := (*columnServingSegmentLeaseSet)(nil)
	if holder != nil {
		cleanupErr = holder.close()
		retained = holder.detachRetainedServingSegments()
	}
	if !poolAttached {
		cleanupErr = errors.Join(cleanupErr, pool.Close())
		if pool.cleanupRetained() {
			retained = pool
		}
	}
	if retained != nil {
		cleanupErr = errors.Join(cleanupErr, retained.ledger.quarantine(retained, buildErr, cleanupErr))
	}
	return nil, errors.Join(buildErr, cleanupErr)
}

func (c *Collection) finishColumnVectorGraphServingPreparedSearchBuild(
	key columnVectorGraphSharedPreparedSearchKey,
	entry *columnVectorGraphSharedPreparedSearchCacheEntry,
	holder *columnVectorGraphSharedPreparedSearch,
	buildErr error,
) {
	var cleanupHolder *columnVectorGraphSharedPreparedSearch
	c.vectorPreparedSearchMu.Lock()
	if c.vectorPreparedSearch[key] != entry || !entry.building {
		cleanupHolder = holder
		c.vectorPreparedSearchMu.Unlock()
		_ = c.closeColumnVectorGraphSharedPreparedSearchHolder(cleanupHolder)
		return
	}
	entry.holder, entry.err, entry.building = holder, buildErr, false
	close(entry.ready)
	if entry.waiters == 0 && entry.refs == 0 {
		delete(c.vectorPreparedSearch, key)
		cleanupHolder = entry.holder
	}
	c.vectorPreparedSearchMu.Unlock()
	if cleanupHolder != nil {
		_ = c.closeColumnVectorGraphSharedPreparedSearchHolder(cleanupHolder)
	}
}

func (c *Collection) waitColumnVectorGraphServingPreparedSearch(ctx context.Context, key columnVectorGraphSharedPreparedSearchKey, entry *columnVectorGraphSharedPreparedSearchCacheEntry) (*columnVectorGraphSharedPreparedSearchRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, c.cancelColumnVectorGraphServingPreparedSearchWait(key, entry, err)
	}
	select {
	case <-entry.ready:
		if err := ctx.Err(); err != nil {
			return nil, c.cancelColumnVectorGraphServingPreparedSearchWait(key, entry, err)
		}
		var cleanupHolder *columnVectorGraphSharedPreparedSearch
		c.vectorPreparedSearchMu.Lock()
		if entry.waiters > 0 {
			entry.waiters--
		}
		if c.vectorPreparedSearch[key] != entry {
			c.vectorPreparedSearchMu.Unlock()
			return nil, ErrVectorIndexSnapshotMismatch
		}
		if entry.err == nil && entry.holder != nil && entry.holder.ready() {
			entry.refs++
			holder := entry.holder
			c.vectorPreparedSearchMu.Unlock()
			return &columnVectorGraphSharedPreparedSearchRef{collection: c, key: key, holder: holder}, nil
		}
		err := entry.err
		if err == nil {
			err = errColumnVectorGraphSharedPreparedSearchNotEligible
		}
		if entry.waiters == 0 && entry.refs == 0 && !entry.building {
			delete(c.vectorPreparedSearch, key)
			cleanupHolder = entry.holder
		}
		c.vectorPreparedSearchMu.Unlock()
		if cleanupHolder != nil {
			err = errors.Join(err, c.closeColumnVectorGraphSharedPreparedSearchHolder(cleanupHolder))
		}
		return nil, err
	case <-ctx.Done():
		return nil, c.cancelColumnVectorGraphServingPreparedSearchWait(key, entry, ctx.Err())
	}
}

func (c *Collection) cancelColumnVectorGraphServingPreparedSearchWait(key columnVectorGraphSharedPreparedSearchKey, entry *columnVectorGraphSharedPreparedSearchCacheEntry, waitErr error) error {
	var cleanupHolder *columnVectorGraphSharedPreparedSearch
	c.vectorPreparedSearchMu.Lock()
	if entry.waiters > 0 {
		entry.waiters--
	}
	if c.vectorPreparedSearch[key] == entry && entry.waiters == 0 && entry.refs == 0 && !entry.building {
		delete(c.vectorPreparedSearch, key)
		cleanupHolder = entry.holder
	}
	c.vectorPreparedSearchMu.Unlock()
	if cleanupHolder != nil {
		waitErr = errors.Join(waitErr, c.closeColumnVectorGraphSharedPreparedSearchHolder(cleanupHolder))
	}
	return waitErr
}
