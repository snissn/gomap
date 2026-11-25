package collections

import (
	"bytes"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type compiledIndexStateLeaf struct {
	indexName string
	lower     []byte
	lowerSet  bool
	lowerInc  bool
	upper     []byte
	upperSet  bool
	upperInc  bool
}

func compileHybridIndexStateFilter(meta CollectionMeta, filter HybridScalarFilter) ([]compiledIndexStateLeaf, error) {
	if !persistIndexStateForDocumentFormat(meta.Options.DocumentFormat) {
		return nil, fmt.Errorf("%w: index_state_postfilter requires a collection format that persists scalar index state", ErrHybridSearchUnsupported)
	}
	rawLeaves := filter.And
	if len(rawLeaves) == 0 {
		rawLeaves = []HybridScalarFilter{filter}
	}
	compiled := make([]compiledIndexStateLeaf, 0, len(rawLeaves))
	for _, leaf := range rawLeaves {
		idx, ok := findIndex(meta.Indexes, leaf.IndexName)
		if !ok {
			return nil, fmt.Errorf("%w: hybrid scalar filter index %q is unavailable", ErrHybridSearchIndexUnavailable, leaf.IndexName)
		}
		if orderedBSONIndexRequiresCompoundRangeAPI(idx) {
			return nil, fmt.Errorf("%w: hybrid scalar filter index %q requires compound range lookup", ErrHybridSearchUnsupported, leaf.IndexName)
		}
		var cLeaf compiledIndexStateLeaf
		cLeaf.indexName = idx.Name
		if leaf.Range != nil {
			if !leaf.Range.Lower.Unbounded {
				enc, err := encodeIndexScalar(idx.ValueType, leaf.Range.Lower.Value)
				if err != nil {
					return nil, fmt.Errorf("%w: hybrid scalar filter index %q lower bound: %v", ErrHybridSearchUnsupported, leaf.IndexName, err)
				}
				cLeaf.lower = enc
				cLeaf.lowerSet = true
				cLeaf.lowerInc = leaf.Range.Lower.Inclusive
			}
			if !leaf.Range.Upper.Unbounded {
				enc, err := encodeIndexScalar(idx.ValueType, leaf.Range.Upper.Value)
				if err != nil {
					return nil, fmt.Errorf("%w: hybrid scalar filter index %q upper bound: %v", ErrHybridSearchUnsupported, leaf.IndexName, err)
				}
				cLeaf.upper = enc
				cLeaf.upperSet = true
				cLeaf.upperInc = leaf.Range.Upper.Inclusive
			}
		} else {
			enc, err := encodeIndexScalar(idx.ValueType, leaf.Value)
			if err != nil {
				return nil, fmt.Errorf("%w: hybrid scalar filter index %q value: %v", ErrHybridSearchUnsupported, leaf.IndexName, err)
			}
			cLeaf.lower = enc
			cLeaf.lowerSet = true
			cLeaf.lowerInc = true
			cLeaf.upper = enc
			cLeaf.upperSet = true
			cLeaf.upperInc = true
		}
		compiled = append(compiled, cLeaf)
	}
	return compiled, nil
}

func matchCompiledIndexStateFilter(state documentIndexState, leaves []compiledIndexStateLeaf) bool {
	if len(leaves) == 0 {
		return true
	}
	if len(state) == 0 {
		return false
	}
	for i := range leaves {
		leaf := &leaves[i]
		vals := state[leaf.indexName]
		if len(vals) == 0 {
			return false
		}
		matched := false
		for _, val := range vals {
			if leaf.lowerSet {
				cmp := bytes.Compare(val, leaf.lower)
				if cmp < 0 || (cmp == 0 && !leaf.lowerInc) {
					continue
				}
			}
			if leaf.upperSet {
				cmp := bytes.Compare(val, leaf.upper)
				if cmp > 0 || (cmp == 0 && !leaf.upperInc) {
					continue
				}
			}
			matched = true
			break
		}
		if !matched {
			return false
		}
	}
	return true
}

func (c *Collection) compileHybridIndexStateFilterForPlan(plan hybridSearchExecutionPlan) ([]compiledIndexStateLeaf, error) {
	if plan.scalarFilter == nil {
		return nil, nil
	}
	if plan.readView != nil && plan.readView.nativeRead != nil {
		return compileHybridIndexStateFilter(plan.readView.nativeRead.meta, *plan.scalarFilter)
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, fmt.Errorf("%w: snapshot unavailable", ErrHybridSearchIndexUnavailable)
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, fmt.Errorf("%w: catalog load failed: %v", ErrHybridSearchIndexUnavailable, err)
	}
	if catalog == nil {
		return nil, fmt.Errorf("%w: collection not found", ErrHybridSearchIndexUnavailable)
	}
	return compileHybridIndexStateFilter(catalog.meta, *plan.scalarFilter)
}

func (c *Collection) filterHybridResultsByIndexState(
	plan hybridSearchExecutionPlan,
	compiled []compiledIndexStateLeaf,
	results []HybridSearchResult,
	limit int,
	baseState hybridSearchStateToken,
	stats *HybridSearchStats,
) ([]HybridSearchResult, error) {
	if len(compiled) == 0 || len(results) == 0 || limit <= 0 {
		if stats != nil {
			stats.CandidatesAfterFilter = uint64(len(results))
		}
		return results, nil
	}

	var snap *backenddb.Snapshot
	var catalog *collectionCatalog
	if plan.readView != nil && plan.readView.nativeRead != nil {
		snap = plan.readView.nativeRead.capture.Snapshot()
		catalog = plan.readView.catalog
	} else {
		snap = c.db.AcquireSnapshot()
		if snap == nil {
			return nil, fmt.Errorf("%w: snapshot unavailable for index-state postfilter", ErrHybridSearchIndexUnavailable)
		}
		defer func() { _ = snap.Close() }()
		if err := hybridSearchCheckExecutionSnapshot(c, plan, baseState); err != nil {
			return nil, err
		}
		var err error
		catalog, err = c.catalogForSnapshot(snap)
		if err != nil || catalog == nil {
			return nil, fmt.Errorf("%w: catalog unavailable for index-state postfilter", ErrHybridSearchIndexUnavailable)
		}
	}

	stateRootName := collectionIndexStateRootName(catalog.meta.Name)
	filtered := make([]HybridSearchResult, 0, min(len(results), limit))
	var scratch []byte
	for _, r := range results {
		if len(filtered) >= limit {
			break
		}
		if stats != nil {
			stats.ScalarPostfilterChecks++
		}
		raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, stateRootName, r.ID, scratch[:0])
		if err != nil {
			return nil, fmt.Errorf("%w: index-state point lookup failed: %v", ErrHybridSearchIndexUnavailable, err)
		}
		scratch = raw
		var state documentIndexState
		if found && len(raw) > 0 {
			state, err = decodeDocumentIndexState(raw)
			if err != nil {
				return nil, fmt.Errorf("%w: decode index-state failed: %v", ErrHybridSearchIndexUnavailable, err)
			}
		}
		if !matchCompiledIndexStateFilter(state, compiled) {
			if stats != nil {
				stats.ScalarFilterRejected++
			}
			continue
		}
		if stats != nil {
			stats.ScalarFilterMatched++
		}
		r.Rank = len(filtered) + 1
		filtered = append(filtered, r)
	}
	if stats != nil {
		stats.CandidatesAfterFilter = uint64(len(filtered))
		if len(filtered) < len(results) && len(filtered) == limit {
			stats.Truncated++
		}
	}
	return filtered, nil
}
