package db

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/zipper"
)

func wrapReadOnlyValueLogRecoveryError(err error) error {
	if errors.Is(err, rootpublication.ErrRecoveryRequired) {
		return errors.Join(err, ErrRecoveryRequired)
	}
	return err
}

func openReadOnly(opts Options) (*DB, error) {
	if err := applyReadOnlyDefaults(&opts); err != nil {
		return nil, err
	}
	NormalizeFlushAdmissionOptions(&opts)
	if err := ensureNoLegacyMixedWALValueSegments(opts.Dir); err != nil {
		return nil, err
	}
	var lock *lockfile.Lock
	lockPath := filepath.Join(opts.Dir, "LOCK")
	if l, err := lockfile.AcquireShared(lockPath); err == nil {
		lock = l
	} else if !(errors.Is(err, os.ErrNotExist) || errors.Is(err, lockfile.ErrUnsupported)) {
		return nil, err
	}
	if err := collectionwal.RequireCleanForReadOnlyOpen(opts.Dir); err != nil {
		_ = lock.Close()
		return nil, err
	}

	idxPath := filepath.Join(opts.Dir, indexFileName)
	if _, err := os.Stat(idxPath); err != nil {
		_ = lock.Close()
		return nil, err
	}

	p, err := pager.OpenReadOnlyWithOptions(idxPath, opts.ChunkSize, pager.OpenOptions{
		MmapPopulate:   opts.PagerMmapPopulate,
		PrefetchOnRead: opts.PagerPrefetchOnRead,
	})
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	layout := resolveStorageLayout(opts.Dir)
	valueLogIdentityPins := rootpublication.NewIdentityPinRegistry()
	vm, err := valuelog.NewReadOnlyManagerForBoundedRecoveryWithStableResourcePinRegistry(layout.valueVLogDir, valueLogIdentityPins)
	if err != nil {
		_ = p.Close()
		_ = lock.Close()
		return nil, wrapReadOnlyValueLogRecoveryError(err)
	}
	if err := vm.AddScanDirForBoundedRecovery(layout.leafVLogDir); err != nil {
		_ = vm.Close()
		_ = p.Close()
		_ = lock.Close()
		return nil, wrapReadOnlyValueLogRecoveryError(err)
	}
	vm.SetDisableReadChecksum(opts.ValueLog.ReadIntegrity == IntegritySkipChecksums)
	vm.SetCurrentWritableMmapEnabled(opts.ValueLog.CurrentWritableMmap)
	vm.SetMultiCurrentWritableLane(valuelog.ReservedLeafLogLaneID, opts.IndexOuterLeavesInValueLog)
	vm.SetDictLookup(opts.ValueLog.DictLookup)
	vm.SetTemplateLookup(opts.ValueLog.TemplateLookup, opts.ValueLog.TemplateDecodeOptions)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)
	gen := newIndexGen(1, p, alloc, z)

	adaptiveCtrl, inlineThreshold := resolveInlineThresholdAndAdaptive(opts)
	db := &DB{
		readOnly:                       true,
		resolvedProfile:                opts.ResolvedProfile,
		deprecatedProfileAlias:         opts.DeprecatedProfileAlias,
		durability:                     opts.Durability,
		valueLogManager:                vm,
		valueLogIdentityPins:           valueLogIdentityPins,
		lock:                           lock,
		adaptive:                       adaptiveCtrl,
		flushAdmission:                 FlushAdmissionDecisionForOptions(opts),
		keepRecent:                     opts.KeepRecent,
		valueLogCompression:            opts.ValueLog.Compression,
		valueLogAutoPolicy:             opts.ValueLog.AutoPolicy,
		valueLogBlockCodec:             opts.ValueLog.BlockCodec,
		valueLogDictLookup:             opts.ValueLog.DictLookup,
		valueLogDictCurrentForClass:    opts.ValueLog.DictCurrentForClass,
		valueLogDictLeafPayloadMode:    opts.ValueLog.DictLeafPayloadMode,
		valueLogDictPut:                opts.ValueLog.DictPut,
		valueLogDictSetCurrentForClass: opts.ValueLog.DictSetCurrentForClass,
		valueLogDictSetLeafPayloadMode: opts.ValueLog.DictSetLeafPayloadMode,
		valueLogDomainThresholds:       NormalizeValueLogDomainThresholds(opts.ValueLog.DomainInlineThresholds),
		leafPrefixCompression:          opts.LeafPrefixCompression,
		indexColumnarLeaves:            opts.IndexColumnarLeaves,
		indexPackedValuePtr:            opts.IndexPackedValuePtr,
		indexInternalBaseDelta:         opts.IndexInternalBaseDelta,
		indexOuterLeavesInValueLog:     opts.IndexOuterLeavesInValueLog,
		indexAdaptiveLeafEncoding:      opts.IndexAdaptiveLeafEncoding,
		leafFillTargetPPM:              opts.LeafFillTargetPPM,
		internalFillTargetPPM:          opts.InternalFillTargetPPM,
		piggybackCompaction:            !opts.DisablePiggybackCompaction,
		maintenanceOpsPerCoalesce:      opts.MaintenanceOpsPerCoalesce,
		dir:                            opts.Dir,
		columnAssetRootDir:             layout.columnAssetDir,
		chunkSize:                      opts.ChunkSize,
		walMaxSegmentBytes:             opts.WALMaxSegmentBytes,
		commandWALSegmentTargetBytes:   opts.CommandWALSegmentTargetBytes,
		commandWAL:                     opts.CommandWAL,
		commandWALStatsScan:            opts.CommandWALStatsScan,
		preferAppendAlloc:              opts.PreferAppendAlloc,
		freelistRegionPages:            opts.FreelistRegionPages,
		freelistRegionRadius:           opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: inlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},

		idxAll:      map[uint64]*indexGen{gen.id: gen},
		idxNext:     gen.id + 1,
		snapPool:    NewSnapshotPool(),
		notifyError: opts.NotifyError,
	}
	db.initializeLeafGenerationManifestStore(layout.leafVLogDir, valueLogIdentityPins)
	db.idx.Store(gen)

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)
	gen.zipper.SetOuterLeavesInValueLog(opts.IndexOuterLeavesInValueLog)
	db.leafPageReadCache = newLeafPageReadCache(configuredLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries))
	gen.zipper.SetLeafPageReader(db.leafPageReader(vm))
	gen.zipper.SetMaintenanceOpsPerCoalesce(opts.MaintenanceOpsPerCoalesce)

	if err := db.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.seedEntryRevisionFloor()
	if err := requireNoUnappliedCommandWALFrames(opts.Dir, db.meta.AppliedCommandLSN, opts.WALMaxSegmentBytes); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := requireNoLegacyCachedRedoJournalReplay(opts.Dir, db, opts.WALMaxSegmentBytes); err != nil {
		_ = db.Close()
		return nil, err
	}
	if db.commandWAL {
		db.cacheCommandWALRequiredFeatureStats()
	}
	if opts.IndexOuterLeavesInValueLog {
		manifest, selectedExact, err := db.loadSelectedDurableLeafGenerationManifest()
		if err == nil && !selectedExact {
			manifest, err = loadOrCreateLeafGenerationManifestWithStore(layout.leafVLogDir, db.meta.CommitSeq, true, db.leafGenerationManifestStore)
		}
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.leafGenerationManifest = manifest
	}

	initialState := &DBState{
		CommitSeq:                  db.meta.CommitSeq,
		RootPageID:                 db.meta.UserRootPageID,
		SystemRootPageID:           db.meta.SystemRootPageID,
		AppliedCommandLSN:          db.meta.AppliedCommandLSN,
		MaxEntryRevision:           page.EntryRevision(db.meta.MaxEntryRevision),
		ValueLogSet:                vm.CurrentSetNoRefresh(),
		LeafGenerations:            db.currentLeafGenerationView(),
		LeafGenerationStateVersion: db.leafGenerationStateVersion,
	}
	db.state.Store(initialState)
	db.publishSnapshotView(gen, initialState, vm)

	// No WAL replay, no background workers in read-only mode.
	return db, nil
}

func openReadOnlyNoLock(opts Options) (*DB, error) {
	if err := applyReadOnlyDefaults(&opts); err != nil {
		return nil, err
	}
	NormalizeFlushAdmissionOptions(&opts)
	if err := ensureNoLegacyMixedWALValueSegments(opts.Dir); err != nil {
		return nil, err
	}
	if err := collectionwal.RequireCleanForReadOnlyOpen(opts.Dir); err != nil {
		return nil, err
	}
	idxPath := filepath.Join(opts.Dir, indexFileName)
	if _, err := os.Stat(idxPath); err != nil {
		return nil, err
	}

	p, err := pager.OpenReadOnlyWithOptions(idxPath, opts.ChunkSize, pager.OpenOptions{
		MmapPopulate:   opts.PagerMmapPopulate,
		PrefetchOnRead: opts.PagerPrefetchOnRead,
	})
	if err != nil {
		return nil, err
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	layout := resolveStorageLayout(opts.Dir)
	valueLogIdentityPins := rootpublication.NewIdentityPinRegistry()
	vm, err := valuelog.NewReadOnlyManagerForBoundedRecoveryWithStableResourcePinRegistry(layout.valueVLogDir, valueLogIdentityPins)
	if err != nil {
		_ = p.Close()
		return nil, wrapReadOnlyValueLogRecoveryError(err)
	}
	if err := vm.AddScanDirForBoundedRecovery(layout.leafVLogDir); err != nil {
		_ = vm.Close()
		_ = p.Close()
		return nil, wrapReadOnlyValueLogRecoveryError(err)
	}
	vm.SetDisableReadChecksum(opts.ValueLog.ReadIntegrity == IntegritySkipChecksums)
	vm.SetCurrentWritableMmapEnabled(opts.ValueLog.CurrentWritableMmap)
	vm.SetMultiCurrentWritableLane(valuelog.ReservedLeafLogLaneID, opts.IndexOuterLeavesInValueLog)
	vm.SetDictLookup(opts.ValueLog.DictLookup)
	vm.SetTemplateLookup(opts.ValueLog.TemplateLookup, opts.ValueLog.TemplateDecodeOptions)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)
	gen := newIndexGen(1, p, alloc, z)

	adaptiveCtrl, inlineThreshold := resolveInlineThresholdAndAdaptive(opts)
	db := &DB{
		readOnly:                       true,
		resolvedProfile:                opts.ResolvedProfile,
		deprecatedProfileAlias:         opts.DeprecatedProfileAlias,
		durability:                     opts.Durability,
		valueLogManager:                vm,
		valueLogIdentityPins:           valueLogIdentityPins,
		adaptive:                       adaptiveCtrl,
		flushAdmission:                 FlushAdmissionDecisionForOptions(opts),
		keepRecent:                     opts.KeepRecent,
		valueLogCompression:            opts.ValueLog.Compression,
		valueLogAutoPolicy:             opts.ValueLog.AutoPolicy,
		valueLogBlockCodec:             opts.ValueLog.BlockCodec,
		valueLogDictLookup:             opts.ValueLog.DictLookup,
		valueLogDictCurrentForClass:    opts.ValueLog.DictCurrentForClass,
		valueLogDictLeafPayloadMode:    opts.ValueLog.DictLeafPayloadMode,
		valueLogDictPut:                opts.ValueLog.DictPut,
		valueLogDictSetCurrentForClass: opts.ValueLog.DictSetCurrentForClass,
		valueLogDictSetLeafPayloadMode: opts.ValueLog.DictSetLeafPayloadMode,
		valueLogDomainThresholds:       NormalizeValueLogDomainThresholds(opts.ValueLog.DomainInlineThresholds),
		leafPrefixCompression:          opts.LeafPrefixCompression,
		indexColumnarLeaves:            opts.IndexColumnarLeaves,
		indexPackedValuePtr:            opts.IndexPackedValuePtr,
		indexInternalBaseDelta:         opts.IndexInternalBaseDelta,
		indexOuterLeavesInValueLog:     opts.IndexOuterLeavesInValueLog,
		indexAdaptiveLeafEncoding:      opts.IndexAdaptiveLeafEncoding,
		leafFillTargetPPM:              opts.LeafFillTargetPPM,
		internalFillTargetPPM:          opts.InternalFillTargetPPM,
		piggybackCompaction:            !opts.DisablePiggybackCompaction,
		maintenanceOpsPerCoalesce:      opts.MaintenanceOpsPerCoalesce,
		dir:                            opts.Dir,
		columnAssetRootDir:             layout.columnAssetDir,
		chunkSize:                      opts.ChunkSize,
		walMaxSegmentBytes:             opts.WALMaxSegmentBytes,
		commandWALSegmentTargetBytes:   opts.CommandWALSegmentTargetBytes,
		commandWAL:                     opts.CommandWAL,
		commandWALStatsScan:            opts.CommandWALStatsScan,
		preferAppendAlloc:              opts.PreferAppendAlloc,
		freelistRegionPages:            opts.FreelistRegionPages,
		freelistRegionRadius:           opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: inlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},

		idxAll:      map[uint64]*indexGen{gen.id: gen},
		idxNext:     gen.id + 1,
		snapPool:    NewSnapshotPool(),
		notifyError: opts.NotifyError,
	}
	db.initializeLeafGenerationManifestStore(layout.leafVLogDir, valueLogIdentityPins)
	db.idx.Store(gen)

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)
	gen.zipper.SetOuterLeavesInValueLog(opts.IndexOuterLeavesInValueLog)
	db.leafPageReadCache = newLeafPageReadCache(configuredLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries))
	gen.zipper.SetLeafPageReader(db.leafPageReader(vm))
	gen.zipper.SetMaintenanceOpsPerCoalesce(opts.MaintenanceOpsPerCoalesce)

	if err := db.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.seedEntryRevisionFloor()
	if err := requireNoUnappliedCommandWALFrames(opts.Dir, db.meta.AppliedCommandLSN, opts.WALMaxSegmentBytes); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := requireNoLegacyCachedRedoJournalReplay(opts.Dir, db, opts.WALMaxSegmentBytes); err != nil {
		_ = db.Close()
		return nil, err
	}
	if db.commandWAL {
		db.cacheCommandWALRequiredFeatureStats()
	}
	if opts.IndexOuterLeavesInValueLog {
		manifest, selectedExact, err := db.loadSelectedDurableLeafGenerationManifest()
		if err == nil && !selectedExact {
			manifest, err = loadOrCreateLeafGenerationManifestWithStore(layout.leafVLogDir, db.meta.CommitSeq, true, db.leafGenerationManifestStore)
		}
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.leafGenerationManifest = manifest
	}

	initialState := &DBState{
		CommitSeq:                  db.meta.CommitSeq,
		RootPageID:                 db.meta.UserRootPageID,
		SystemRootPageID:           db.meta.SystemRootPageID,
		AppliedCommandLSN:          db.meta.AppliedCommandLSN,
		MaxEntryRevision:           page.EntryRevision(db.meta.MaxEntryRevision),
		ValueLogSet:                vm.CurrentSetNoRefresh(),
		LeafGenerations:            db.currentLeafGenerationView(),
		LeafGenerationStateVersion: db.leafGenerationStateVersion,
	}
	db.state.Store(initialState)
	db.publishSnapshotView(gen, initialState, vm)

	return db, nil
}

func applyReadOnlyDefaults(opts *Options) error {
	if _, err := resolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries); err != nil {
		return err
	}
	if opts.ChunkSize == 0 {
		// Open() applies the same default before dispatching to read-only mode.
		// openReadOnlyNoLock callers bypass that path, but the pager cannot map an
		// existing non-empty index with a zero chunk size.
		opts.ChunkSize = defaultChunkSize
	}
	return nil
}
