package collections

import (
	"strconv"
	"strings"
	"sync"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const collectionTypedColumnOneShotCacheMaxEntries = 8

type collectionTypedColumnOneShotCacheSlot struct {
	commitSeq                uint64
	systemRoot               uint64
	manifestGeneration       uint64
	activeManifestChecksum   uint64
	recoveryManifestChecksum uint64
	appliedCommandLSN        uint64
	readIntegrity            ColumnAssetReadIntegrity
	kind                     ColumnPhysicalQueryKind
	groupColumn              string
	valueColumn              string
	distinctColumn           string
	topK                     int
	topKOrder                ColumnPhysicalQueryTopKOrder
	skipEmptyGroupKey        bool
	predicates               string
}

type collectionTypedColumnOneShotCacheEntry struct {
	slot    collectionTypedColumnOneShotCacheSlot
	runner  *columnTypedColumnPhysicalQueryRunner
	mu      sync.Mutex
	lastUse uint64
}

type collectionTypedColumnOneShotCacheSnapshot struct {
	Entries       int
	CacheHits     uint64
	CacheMisses   uint64
	CacheBuilds   uint64
	Invalidations uint64
}

func (entry *collectionTypedColumnOneShotCacheEntry) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if entry == nil {
		return ColumnPhysicalQueryResult{}, backenddb.ErrClosed
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.runner == nil {
		return ColumnPhysicalQueryResult{}, backenddb.ErrClosed
	}
	return entry.runner.run(view, req)
}

func (entry *collectionTypedColumnOneShotCacheEntry) close() {
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.runner != nil {
		entry.runner.close()
		entry.runner = nil
	}
}

func (c *Collection) runColumnTypedColumnOneShotWithCache(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, plan columnTypedColumnPhysicalQueryPlan, start time.Time) (ColumnPhysicalQueryResult, bool, error) {
	if c == nil || view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if req.AggregateMetadataName != "" {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	slot := collectionTypedColumnOneShotCacheSlotFor(view, req)

	c.typedColumnOneShotMu.Lock()
	entry := c.typedColumnOneShot[slot]
	if entry != nil {
		c.typedColumnOneShotHits++
		c.typedColumnOneShotClock++
		entry.lastUse = c.typedColumnOneShotClock
		c.typedColumnOneShotMu.Unlock()
		result, err := entry.run(view, req)
		result.Diagnostics.TypedColumnOneShotCacheHit = true
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	c.typedColumnOneShotMisses++
	c.typedColumnOneShotBuilds++
	c.typedColumnOneShotMu.Unlock()

	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.TypedColumnOneShotCacheMiss = true
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	buildStart := time.Now()
	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunnerWithOptions(view, req, &readCache, columnTypedColumnPhysicalQueryRunnerPrepareOptions{
		prepareAggregateSummaries:                 columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req),
		prepareDenseGroupCountDistinctGlobalRanks: columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req),
		plannedQuery:    plan,
		hasPlannedQuery: true,
	})
	buildNanos := time.Since(buildStart).Nanoseconds()
	if err != nil || !candidate {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.TypedColumnOneShotCacheMiss = true
		result.Diagnostics.TypedColumnOneShotCacheBuild = true
		result.Diagnostics.TypedColumnOneShotBuildNanos = buildNanos
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, candidate, err
	}
	entry = &collectionTypedColumnOneShotCacheEntry{slot: slot, runner: runner}

	c.typedColumnOneShotMu.Lock()
	if c.typedColumnOneShot == nil {
		c.typedColumnOneShot = make(map[collectionTypedColumnOneShotCacheSlot]*collectionTypedColumnOneShotCacheEntry, collectionTypedColumnOneShotCacheMaxEntries)
	}
	if current := c.typedColumnOneShot[slot]; current != nil {
		c.typedColumnOneShotHits++
		c.typedColumnOneShotClock++
		current.lastUse = c.typedColumnOneShotClock
		c.typedColumnOneShotMu.Unlock()
		entry.close()
		result, err := current.run(view, req)
		result.Diagnostics.TypedColumnOneShotCacheMiss = true
		result.Diagnostics.TypedColumnOneShotCacheBuild = true
		applyColumnTypedColumnOneShotBuildDiagnostics(&result, runner, buildNanos, 0)
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	var evicted *collectionTypedColumnOneShotCacheEntry
	cacheStoreStart := time.Now()
	if len(c.typedColumnOneShot) >= collectionTypedColumnOneShotCacheMaxEntries {
		evicted = collectionTypedColumnOneShotEvictOldest(c.typedColumnOneShot)
		c.typedColumnOneShotInvalidations++
	}
	c.typedColumnOneShotClock++
	entry.lastUse = c.typedColumnOneShotClock
	c.typedColumnOneShot[slot] = entry
	cacheStoreNanos := time.Since(cacheStoreStart).Nanoseconds()
	c.typedColumnOneShotMu.Unlock()
	if evicted != nil {
		evicted.close()
	}
	if c.manager != nil && !c.manager.registerCollectionHandleIfOpen(c) {
		c.typedColumnOneShotMu.Lock()
		if c.typedColumnOneShot[slot] == entry {
			delete(c.typedColumnOneShot, slot)
			c.typedColumnOneShotInvalidations++
		}
		c.typedColumnOneShotMu.Unlock()
		entry.close()
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.TypedColumnOneShotCacheMiss = true
		result.Diagnostics.TypedColumnOneShotCacheBuild = true
		applyColumnTypedColumnOneShotBuildDiagnostics(&result, runner, buildNanos, cacheStoreNanos)
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, backenddb.ErrClosed
	}

	result, err := entry.run(view, req)
	result.Diagnostics.TypedColumnOneShotCacheMiss = true
	result.Diagnostics.TypedColumnOneShotCacheBuild = true
	applyColumnTypedColumnOneShotBuildDiagnostics(&result, runner, buildNanos, cacheStoreNanos)
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, true, err
}

func applyColumnTypedColumnOneShotBuildDiagnostics(result *ColumnPhysicalQueryResult, runner *columnTypedColumnPhysicalQueryRunner, buildNanos, cacheStoreNanos int64) {
	if result == nil {
		return
	}
	result.Diagnostics.TypedColumnOneShotBuildNanos = buildNanos
	result.Diagnostics.TypedColumnOneShotCacheStoreNanos = cacheStoreNanos
	if runner != nil {
		runner.prepareDiagnostics.applyTo(&result.Diagnostics)
	}
}

func collectionTypedColumnOneShotEvictOldest(entries map[collectionTypedColumnOneShotCacheSlot]*collectionTypedColumnOneShotCacheEntry) *collectionTypedColumnOneShotCacheEntry {
	var oldestSlot collectionTypedColumnOneShotCacheSlot
	var oldestUse uint64
	haveOldest := false
	for slot, entry := range entries {
		if entry == nil {
			delete(entries, slot)
			return nil
		}
		if !haveOldest || entry.lastUse < oldestUse {
			oldestSlot = slot
			oldestUse = entry.lastUse
			haveOldest = true
		}
	}
	if haveOldest {
		entry := entries[oldestSlot]
		delete(entries, oldestSlot)
		return entry
	}
	return nil
}

func (c *Collection) hasCollectionTypedColumnOneShotCacheEntries() bool {
	if c == nil {
		return false
	}
	c.typedColumnOneShotMu.Lock()
	defer c.typedColumnOneShotMu.Unlock()
	return len(c.typedColumnOneShot) > 0
}

func (c *Collection) closeCollectionTypedColumnOneShotCache() {
	if c == nil {
		return
	}
	var entries []*collectionTypedColumnOneShotCacheEntry
	c.typedColumnOneShotMu.Lock()
	for _, entry := range c.typedColumnOneShot {
		if entry != nil {
			entries = append(entries, entry)
		}
	}
	c.typedColumnOneShot = nil
	c.typedColumnOneShotMu.Unlock()
	for _, entry := range entries {
		entry.close()
	}
	if c.manager != nil && !c.hasDirtyNativeVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
}

func (m *CollectionManager) closeCollectionTypedColumnOneShotCaches() {
	if m == nil {
		return
	}
	m.collectionsMu.RLock()
	collections := make([]*Collection, 0, len(m.collections))
	for collection := range m.collections {
		if collection != nil {
			collections = append(collections, collection)
		}
	}
	m.collectionsMu.RUnlock()
	for _, collection := range collections {
		collection.closeCollectionTypedColumnOneShotCache()
	}
}

func (c *Collection) runColumnPhysicalQueryTypedColumnOneShotInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if !columnTypedColumnOneShotCacheRequestCandidate(req) || !columnTypedColumnPhysicalQueryTouchesTypedColumnPart(view.FullConfig, req) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	start := time.Now()
	plan, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req)
	if err != nil || !candidate {
		return ColumnPhysicalQueryResult{}, candidate, err
	}
	if view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	return c.runColumnTypedColumnOneShotWithCache(view, req, plan, start)
}

func columnTypedColumnOneShotCacheRequestCandidate(req ColumnPhysicalQueryRequest) bool {
	return req.AggregateMetadataName == ""
}

func collectionTypedColumnOneShotCacheSlotFor(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) collectionTypedColumnOneShotCacheSlot {
	return collectionTypedColumnOneShotCacheSlot{
		commitSeq:                view.CommitSeq,
		systemRoot:               view.SystemRoot,
		manifestGeneration:       view.Diagnostics.ManifestGeneration,
		activeManifestChecksum:   view.Diagnostics.ActiveManifestChecksum,
		recoveryManifestChecksum: view.Diagnostics.RecoveryManifestChecksum,
		appliedCommandLSN:        view.Diagnostics.AppliedCommandLSN,
		readIntegrity:            req.ColumnAssetReadIntegrity,
		kind:                     req.Kind,
		groupColumn:              req.GroupColumn,
		valueColumn:              req.ValueColumn,
		distinctColumn:           req.DistinctColumn,
		topK:                     req.TopK,
		topKOrder:                req.TopKOrder,
		skipEmptyGroupKey:        req.SkipEmptyGroupKey,
		predicates:               collectionTypedColumnOneShotPredicateKey(req),
	}
}

func collectionTypedColumnOneShotPredicateKey(req ColumnPhysicalQueryRequest) string {
	if len(req.Predicates) == 0 {
		return ""
	}
	var b strings.Builder
	for _, predicate := range req.Predicates {
		writeColumnPhysicalCacheKeyPart(&b, predicate.Column)
		writeColumnPhysicalCacheKeyPart(&b, string(columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)))
		writeColumnPhysicalCacheKeyPart(&b, predicate.Value)
		b.WriteString(strconv.Itoa(len(predicate.Values)))
		b.WriteByte(';')
		for _, value := range predicate.Values {
			writeColumnPhysicalCacheKeyPart(&b, value)
		}
	}
	return b.String()
}

func writeColumnPhysicalCacheKeyPart(b *strings.Builder, value string) {
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(':')
	b.WriteString(value)
	b.WriteByte(';')
}

func (c *Collection) typedColumnOneShotCacheSnapshotForTest() collectionTypedColumnOneShotCacheSnapshot {
	if c == nil {
		return collectionTypedColumnOneShotCacheSnapshot{}
	}
	c.typedColumnOneShotMu.Lock()
	defer c.typedColumnOneShotMu.Unlock()
	snapshot := collectionTypedColumnOneShotCacheSnapshot{
		CacheHits:     c.typedColumnOneShotHits,
		CacheMisses:   c.typedColumnOneShotMisses,
		CacheBuilds:   c.typedColumnOneShotBuilds,
		Invalidations: c.typedColumnOneShotInvalidations,
	}
	if c.typedColumnOneShot != nil {
		snapshot.Entries = len(c.typedColumnOneShot)
	}
	return snapshot
}
