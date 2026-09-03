package collections

import "encoding/json"

// Scalar probe-result cache bounds. Cached sets are shared read-only across
// queries; downstream allow-set consumers only test membership, so sharing is
// safe. Entries are keyed by snapshot state, and every hybrid search flushes
// buffered writes before probing, so equal keys imply equal results.
const (
	scalarProbeCacheMaxEntries = 128
	scalarProbeCacheMaxBytes   = 8 << 20
)

type scalarProbeCacheKey struct {
	state  hybridSearchStateToken
	filter string
}

type scalarProbeCacheFilterDigest struct {
	Index string             `json:"index"`
	Value any                `json:"value,omitempty"`
	Range *IndexRangeOptions `json:"range,omitempty"`
}

type scalarProbeCacheEntry struct {
	set               hybridScalarAllowSet
	lookups           uint64
	inputIDs          uint64
	finalIDs          uint64
	prefilter         uint64
	intersectionSteps uint64
	bytes             uint64
}

func scalarProbeCacheEntryBytes(set hybridScalarAllowSet) uint64 {
	var n uint64
	for id := range set {
		n += uint64(len(id)) + 32
	}
	return n
}

func (c *Collection) scalarProbeCacheKeyForPlan(plan hybridSearchExecutionPlan, limit int, filters []HybridScalarFilter) (scalarProbeCacheKey, bool) {
	if c == nil || len(filters) == 0 {
		return scalarProbeCacheKey{}, false
	}
	tok := hybridSearchDBStateToken(c.db)
	if !tok.available {
		return scalarProbeCacheKey{}, false
	}
	digests := make([]scalarProbeCacheFilterDigest, 0, len(filters))
	for i := range filters {
		digests = append(digests, scalarProbeCacheFilterDigest{
			Index: filters[i].IndexName,
			Value: filters[i].Value,
			Range: filters[i].Range,
		})
	}
	raw, err := json.Marshal(struct {
		Strategy string                         `json:"strategy"`
		Filters  []scalarProbeCacheFilterDigest `json:"filters"`
		Limit    int                            `json:"limit"`
	}{
		Strategy: string(plan.scalarFilterStrategy),
		Filters:  digests,
		Limit:    limit,
	})
	if err != nil {
		return scalarProbeCacheKey{}, false
	}
	return scalarProbeCacheKey{state: tok, filter: string(raw)}, true
}

func (c *Collection) scalarProbeCacheLookup(key scalarProbeCacheKey) (scalarProbeCacheEntry, bool) {
	if c == nil {
		return scalarProbeCacheEntry{}, false
	}
	c.scalarProbeCacheMu.Lock()
	defer c.scalarProbeCacheMu.Unlock()
	entry, ok := c.scalarProbeCache[key]
	if !ok {
		c.scalarProbeCacheMisses++
		return scalarProbeCacheEntry{}, false
	}
	c.scalarProbeCacheHits++
	return entry, true
}

func (c *Collection) scalarProbeCacheStoreForResult(key scalarProbeCacheKey, ok bool, set hybridScalarAllowSet, stats HybridSearchStats) {
	if !ok || set == nil || stats.Truncated != 0 {
		return
	}
	c.scalarProbeCacheStore(key, scalarProbeCacheEntry{
		set:               set,
		lookups:           stats.ScalarFilterLookups,
		inputIDs:          stats.ScalarFilterInputIDs,
		finalIDs:          stats.ScalarFilterFinalIDs,
		prefilter:         stats.ScalarPrefilterIDs,
		intersectionSteps: stats.ScalarFilterIntersectionSteps,
		bytes:             scalarProbeCacheEntryBytes(set),
	})
}

func (c *Collection) scalarProbeCacheStore(key scalarProbeCacheKey, entry scalarProbeCacheEntry) {
	if c == nil || entry.bytes > scalarProbeCacheMaxBytes {
		return
	}
	c.scalarProbeCacheMu.Lock()
	defer c.scalarProbeCacheMu.Unlock()
	if old, ok := c.scalarProbeCache[key]; ok {
		c.scalarProbeCacheBytes -= old.bytes
	}
	if c.scalarProbeCache == nil {
		c.scalarProbeCache = make(map[scalarProbeCacheKey]scalarProbeCacheEntry)
	}
	for len(c.scalarProbeCacheOrder) >= scalarProbeCacheMaxEntries ||
		c.scalarProbeCacheBytes+entry.bytes > scalarProbeCacheMaxBytes {
		if len(c.scalarProbeCacheOrder) == 0 {
			return
		}
		victim := c.scalarProbeCacheOrder[0]
		c.scalarProbeCacheOrder[0] = scalarProbeCacheKey{}
		c.scalarProbeCacheOrder = c.scalarProbeCacheOrder[1:]
		if evicted, ok := c.scalarProbeCache[victim]; ok {
			delete(c.scalarProbeCache, victim)
			c.scalarProbeCacheBytes -= evicted.bytes
			c.scalarProbeCacheEvictions++
		}
	}
	c.scalarProbeCache[key] = entry
	c.scalarProbeCacheOrder = append(c.scalarProbeCacheOrder, key)
	c.scalarProbeCacheBytes += entry.bytes
}

func (c *Collection) scalarProbeCacheStats() (hits, misses uint64) {
	if c == nil {
		return 0, 0
	}
	c.scalarProbeCacheMu.Lock()
	defer c.scalarProbeCacheMu.Unlock()
	return c.scalarProbeCacheHits, c.scalarProbeCacheMisses
}
