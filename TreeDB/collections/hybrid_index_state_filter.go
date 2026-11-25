package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
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

func admitCompiledIndexStateBound(admit func(uint64) error, idx IndexDefinition, val any) error {
	if admit == nil {
		return nil
	}
	charge, err := nativeIndexQueryCharge(idx, val)
	if err != nil {
		return err
	}
	return admit(charge)
}

func compileHybridIndexStateFilter(meta CollectionMeta, filter HybridScalarFilter, admit func(uint64) error) ([]compiledIndexStateLeaf, error) {
	if !persistIndexStateForDocumentFormat(meta.Options.DocumentFormat) {
		return nil, fmt.Errorf("%w: index_state_postfilter requires a collection format that persists scalar index state", ErrHybridSearchUnsupported)
	}
	rawLeaves := filter.And
	if len(rawLeaves) == 0 {
		rawLeaves = []HybridScalarFilter{filter}
	}
	if admit != nil && len(rawLeaves) > 0 {
		elemSize := uint64(unsafe.Sizeof(compiledIndexStateLeaf{}))
		if uint64(len(rawLeaves)) > ^uint64(0)/elemSize {
			return nil, memtable.ErrCOWCapacity
		}
		if err := admit(memtable.COWAllocationCharge(uint64(len(rawLeaves)) * elemSize)); err != nil {
			return nil, err
		}
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
				if err := admitCompiledIndexStateBound(admit, idx, leaf.Range.Lower.Value); err != nil {
					return nil, err
				}
				enc, err := encodeIndexScalar(idx.ValueType, leaf.Range.Lower.Value)
				if err != nil {
					return nil, fmt.Errorf("%w: hybrid scalar filter index %q lower bound: %v", ErrHybridSearchUnsupported, leaf.IndexName, err)
				}
				cLeaf.lower = enc
				cLeaf.lowerSet = true
				cLeaf.lowerInc = leaf.Range.Lower.Inclusive
			}
			if !leaf.Range.Upper.Unbounded {
				if err := admitCompiledIndexStateBound(admit, idx, leaf.Range.Upper.Value); err != nil {
					return nil, err
				}
				enc, err := encodeIndexScalar(idx.ValueType, leaf.Range.Upper.Value)
				if err != nil {
					return nil, fmt.Errorf("%w: hybrid scalar filter index %q upper bound: %v", ErrHybridSearchUnsupported, leaf.IndexName, err)
				}
				cLeaf.upper = enc
				cLeaf.upperSet = true
				cLeaf.upperInc = leaf.Range.Upper.Inclusive
			}
		} else {
			if err := admitCompiledIndexStateBound(admit, idx, leaf.Value); err != nil {
				return nil, err
			}
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

func matchCompiledIndexStateLeafValue(val []byte, leaf *compiledIndexStateLeaf) bool {
	if leaf.lowerSet {
		cmp := bytes.Compare(val, leaf.lower)
		if cmp < 0 || (cmp == 0 && !leaf.lowerInc) {
			return false
		}
	}
	if leaf.upperSet {
		cmp := bytes.Compare(val, leaf.upper)
		if cmp > 0 || (cmp == 0 && !leaf.upperInc) {
			return false
		}
	}
	return true
}

// matchEncodedDocumentIndexState validates and evaluates compiled scalar filter
// leaves directly against a persisted documentIndexState record without heap
// allocations, preserving exact parity with decodeDocumentIndexState.
func matchEncodedDocumentIndexState(raw []byte, leaves []compiledIndexStateLeaf) (bool, error) {
	if len(leaves) == 0 {
		return true, nil
	}
	if len(raw) == 0 {
		return false, nil
	}
	if raw[0] != documentIndexStateVersion {
		return false, fmt.Errorf("collections: unsupported index state version %d", raw[0])
	}
	pos := 1
	if len(raw)-pos < 2 {
		return false, errors.New("collections: malformed index state")
	}
	count := int(binary.BigEndian.Uint16(raw[pos:]))
	pos += 2

	var matchedStack [hybridScalarMaxConjuncts]bool
	var matched []bool
	if len(leaves) <= len(matchedStack) {
		matched = matchedStack[:len(leaves)]
	} else {
		matched = make([]bool, len(leaves))
	}

	for i := 0; i < count; i++ {
		if len(raw)-pos < 2 {
			return false, errors.New("collections: malformed index state name length")
		}
		nameLen := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2
		if nameLen == 0 || len(raw)-pos < nameLen {
			return false, errors.New("collections: malformed index state name")
		}
		nameBytes := raw[pos : pos+nameLen]
		pos += nameLen

		if len(raw)-pos < 2 {
			return false, errors.New("collections: malformed index state value count")
		}
		valueCount := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2

		// Reset match state for any leaf referencing this index name so duplicate
		// index names in raw (if any) mirror decodeDocumentIndexState overwrite semantics.
		hasLeaf := false
		for li := range leaves {
			if len(leaves[li].indexName) == nameLen && string(nameBytes) == leaves[li].indexName {
				matched[li] = false
				hasLeaf = true
			}
		}

		for j := 0; j < valueCount; j++ {
			if len(raw)-pos < 2 {
				return false, errors.New("collections: malformed index state value length")
			}
			valueLen := int(binary.BigEndian.Uint16(raw[pos:]))
			pos += 2
			if valueLen == 0 || len(raw)-pos < valueLen {
				return false, errors.New("collections: malformed index state value")
			}
			val := raw[pos : pos+valueLen]
			pos += valueLen
			if !hasLeaf {
				continue
			}
			for li := range leaves {
				if matched[li] {
					continue
				}
				if len(leaves[li].indexName) == nameLen && string(nameBytes) == leaves[li].indexName {
					if matchCompiledIndexStateLeafValue(val, &leaves[li]) {
						matched[li] = true
					}
				}
			}
		}
	}
	if pos != len(raw) {
		return false, errors.New("collections: trailing index state bytes")
	}
	for _, ok := range matched {
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (c *Collection) compileHybridIndexStateFilterForPlan(plan hybridSearchExecutionPlan) ([]compiledIndexStateLeaf, error) {
	if plan.scalarFilter == nil {
		return nil, nil
	}
	if plan.readView != nil && plan.readView.nativeRead != nil {
		var admit func(uint64) error
		if plan.nativeScratch != nil {
			admit = plan.nativeScratch.admitFn
		}
		return compileHybridIndexStateFilter(plan.readView.nativeRead.meta, *plan.scalarFilter, admit)
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
	return compileHybridIndexStateFilter(catalog.meta, *plan.scalarFilter, nil)
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
	var admit func(uint64) error
	if plan.readView != nil && plan.readView.nativeRead != nil {
		snap = plan.readView.nativeRead.capture.Snapshot()
		catalog = plan.readView.catalog
		if plan.nativeScratch != nil {
			admit = plan.nativeScratch.admitFn
		}
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

	capFiltered := min(len(results), limit)
	if admit != nil && capFiltered > 0 {
		elemSize := uint64(unsafe.Sizeof(HybridSearchResult{}))
		if uint64(capFiltered) > ^uint64(0)/elemSize {
			return nil, memtable.ErrCOWCapacity
		}
		if err := admit(memtable.COWAllocationCharge(uint64(capFiltered) * elemSize)); err != nil {
			return nil, err
		}
	}

	stateRootName := collectionIndexStateRootName(catalog.meta.Name)
	filtered := make([]HybridSearchResult, 0, capFiltered)
	var scratch []byte
	for _, r := range results {
		if len(filtered) >= limit {
			break
		}
		if stats != nil {
			stats.ScalarPostfilterChecks++
		}
		var matched bool
		if catalog.nativeText != nil {
			var matchErr error
			found, err := catalog.nativeText.withRecord(stateRootName, r.ID, func(raw []byte) error {
				if len(raw) == 0 {
					matched = false
					return nil
				}
				var mErr error
				matched, mErr = matchEncodedDocumentIndexState(raw, compiled)
				if mErr != nil {
					matchErr = mErr
					return mErr
				}
				return nil
			})
			if err != nil {
				if matchErr != nil {
					return nil, fmt.Errorf("%w: decode index-state failed: %v", ErrHybridSearchIndexUnavailable, matchErr)
				}
				if errors.Is(err, memtable.ErrCOWCapacity) {
					return nil, err
				}
				return nil, fmt.Errorf("%w: index-state point lookup failed: %w", ErrHybridSearchIndexUnavailable, err)
			}
			if !found {
				matched = false
			}
		} else {
			raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, stateRootName, r.ID, scratch[:0])
			if err != nil {
				return nil, fmt.Errorf("%w: index-state point lookup failed: %v", ErrHybridSearchIndexUnavailable, err)
			}
			scratch = raw
			if found && len(raw) > 0 {
				matched, err = matchEncodedDocumentIndexState(raw, compiled)
				if err != nil {
					return nil, fmt.Errorf("%w: decode index-state failed: %v", ErrHybridSearchIndexUnavailable, err)
				}
			}
		}
		if !matched {
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
