package collections

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

type collectionTypedColumnOneShotTopKCacheSlot struct {
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

type collectionTypedColumnOneShotTopKCacheEntry struct {
	slot   collectionTypedColumnOneShotTopKCacheSlot
	runner *columnTypedColumnPhysicalQueryRunner
	mu     sync.Mutex
}

type collectionTypedColumnOneShotTopKCacheSnapshot struct {
	Entries       int
	CacheHits     uint64
	CacheMisses   uint64
	CacheBuilds   uint64
	Invalidations uint64
}

func (c *Collection) runColumnTypedColumnOneShotTopKWithCache(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, plan columnTypedColumnPhysicalQueryPlan, start time.Time) (ColumnPhysicalQueryResult, bool, error) {
	if c == nil || view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if !columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req) || req.AggregateMetadataName != "" {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	slot := collectionTypedColumnOneShotTopKCacheSlotFor(view, req)

	c.typedColumnOneShotTopKMu.Lock()
	entry := c.typedColumnOneShotTopK
	if entry != nil && entry.slot == slot {
		c.typedColumnOneShotTopKHits++
		c.typedColumnOneShotTopKMu.Unlock()
		entry.mu.Lock()
		result, err := entry.runner.run(view, req)
		entry.mu.Unlock()
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	if entry != nil {
		c.typedColumnOneShotTopKInvalidations++
		c.typedColumnOneShotTopK = nil
	}
	c.typedColumnOneShotTopKMisses++
	c.typedColumnOneShotTopKBuilds++
	c.typedColumnOneShotTopKMu.Unlock()

	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunner(view, req, &readCache, false)
	if err != nil || !candidate {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, candidate, err
	}
	entry = &collectionTypedColumnOneShotTopKCacheEntry{slot: slot, runner: runner}

	c.typedColumnOneShotTopKMu.Lock()
	if current := c.typedColumnOneShotTopK; current != nil && current.slot != slot {
		c.typedColumnOneShotTopKInvalidations++
	}
	c.typedColumnOneShotTopK = entry
	c.typedColumnOneShotTopKMu.Unlock()

	entry.mu.Lock()
	result, err := entry.runner.run(view, req)
	entry.mu.Unlock()
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, true, err
}

func (c *Collection) runColumnPhysicalQueryTypedColumnOneShotTopKInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if !columnTypedColumnOneShotTopKCacheRequestCandidate(req) || !columnTypedColumnPhysicalQueryTouchesTypedColumnPart(view.FullConfig, req) {
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
	return c.runColumnTypedColumnOneShotTopKWithCache(view, req, plan, start)
}

func columnTypedColumnOneShotTopKCacheRequestCandidate(req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupMinInt64 &&
		req.AggregateMetadataName == "" &&
		req.DistinctColumn == "" &&
		req.GroupColumn != "" &&
		req.ValueColumn != "" &&
		req.TopK > 0 &&
		req.TopKOrder == ColumnPhysicalQueryTopKInt64Asc
}

func collectionTypedColumnOneShotTopKCacheSlotFor(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) collectionTypedColumnOneShotTopKCacheSlot {
	return collectionTypedColumnOneShotTopKCacheSlot{
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
		predicates:               collectionTypedColumnOneShotTopKPredicateKey(req),
	}
}

func collectionTypedColumnOneShotTopKPredicateKey(req ColumnPhysicalQueryRequest) string {
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

func (c *Collection) typedColumnOneShotTopKCacheSnapshotForTest() collectionTypedColumnOneShotTopKCacheSnapshot {
	if c == nil {
		return collectionTypedColumnOneShotTopKCacheSnapshot{}
	}
	c.typedColumnOneShotTopKMu.Lock()
	defer c.typedColumnOneShotTopKMu.Unlock()
	snapshot := collectionTypedColumnOneShotTopKCacheSnapshot{
		CacheHits:     c.typedColumnOneShotTopKHits,
		CacheMisses:   c.typedColumnOneShotTopKMisses,
		CacheBuilds:   c.typedColumnOneShotTopKBuilds,
		Invalidations: c.typedColumnOneShotTopKInvalidations,
	}
	if c.typedColumnOneShotTopK != nil {
		snapshot.Entries = 1
	}
	return snapshot
}
