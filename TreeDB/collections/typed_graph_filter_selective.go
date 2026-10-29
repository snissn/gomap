package collections

import (
	"context"
	"errors"
	"fmt"
)

// Use the existing locator/GetMany chunk bound for selective discovery too.
const typedGraphFilterProbeRows = 512

var errTypedGraphFilterProbeIncomplete = errors.New("collections: typed graph filter probe incomplete")

func prepareTypedGraphAND(ctx context.Context, plan *typedGraphPreparedFilter, lookup hybridScalarLookupView, leaves []HybridScalarFilter, limits typedGraphFilterLimits) (hybridScalarIDs, error) {
	definitions := make([]IndexDefinition, len(leaves))
	selective := true
	for i, leaf := range leaves {
		if err := ctx.Err(); err != nil {
			return hybridScalarIDs{}, err
		}
		idx, ok := findIndex(lookup.catalog.meta.Indexes, leaf.IndexName)
		if !ok || orderedBSONIndexRequiresCompoundRangeAPI(idx) {
			return hybridScalarIDs{}, ErrHybridSearchIndexUnavailable
		}
		if shouldDedupeIndexDocumentIDs(idx, lookup.catalog.meta.Options) {
			return hybridScalarIDs{}, ErrHybridSearchUnsupported
		}
		definitions[i] = idx
		selective = selective && leaf.Range == nil && idx.ValueType == IndexValueString && len(idx.Components) == 0
	}
	var prefixes [][]byte
	var setupBytes []int
	if selective {
		// Validate every value before a complete empty driver may end preparation.
		for i, leaf := range leaves {
			if _, ok := leaf.Value.(string); !ok {
				return hybridScalarIDs{}, fmt.Errorf("%w: hybrid scalar filter index %q requires string value, got %T", ErrHybridSearchIndexUnavailable, definitions[i].Name, leaf.Value)
			}
		}
		prefixes, setupBytes = make([][]byte, len(leaves)), make([]int, len(leaves))
		for i, leaf := range leaves {
			if err := ctx.Err(); err != nil {
				return hybridScalarIDs{}, err
			}
			remaining := limits.MappingWork - plan.mappingWork
			value := leaf.Value.(string)
			// Escaped typed-v1 strings use at most two bytes per input byte + terminator.
			if remaining < 2 || len(value) > (remaining-2)/2 {
				return hybridScalarIDs{}, errTypedGraphSearchBudget
			}
			bound := 2*len(value) + 2
			plan.mappingWork += bound
			_, prefix, err := appendIndexScalar(make([]byte, 0, bound), definitions[i].ValueType, value)
			if err != nil {
				return hybridScalarIDs{}, err
			}
			prefixes[i] = prefix
			// The unchanged EQ visitor encodes/copies at most six prefix payloads.
			// Store one bound; multiplication is checked immediately before each call.
			setupBytes[i] = bound
		}
		var driver hybridScalarIDs
		driverIndex := -1
		for i, leaf := range leaves {
			set, complete, err := probeTypedGraphANDLeaf(ctx, plan, &lookup, leaf, limits, typedGraphFilterProbeRows, setupBytes[i])
			if err != nil {
				return hybridScalarIDs{}, err
			}
			if complete && (driverIndex < 0 || set.len() < driver.len()) {
				driver, driverIndex = set, i
			}
		}
		if driverIndex >= 0 {
			for i, idx := range definitions {
				if i == driverIndex {
					continue
				}
				if err := refineTypedGraphAND(ctx, plan, driver, idx, prefixes[i], limits); err != nil {
					return hybridScalarIDs{}, err
				}
			}
			return driver, ctx.Err()
		}
	}
	// No complete selective driver (or unsupported optimization shape). Retain
	// full intersection under the remaining cumulative source/work budgets.
	var allowed hybridScalarIDs
	for i, leaf := range leaves {
		setup := 0
		if selective {
			setup = setupBytes[i]
		}
		set, _, err := probeTypedGraphANDLeaf(ctx, plan, &lookup, leaf, limits, 0, setup)
		if err != nil {
			return hybridScalarIDs{}, err
		}
		if i == 0 {
			allowed = set
			continue
		}
		checked := 0
		for id := range allowed.all {
			if checked&255 == 0 {
				if err := ctx.Err(); err != nil {
					return hybridScalarIDs{}, err
				}
			}
			checked++
			if ok := set.contains(id); !ok {
				allowed.delete(id)
			}
		}
	}
	return allowed, ctx.Err()
}

func probeTypedGraphANDLeaf(ctx context.Context, plan *typedGraphPreparedFilter, lookup *hybridScalarLookupView, leaf HybridScalarFilter, limits typedGraphFilterLimits, probeRows, prefixBound int) (hybridScalarIDs, bool, error) {
	if err := ctx.Err(); err != nil {
		return hybridScalarIDs{}, false, err
	}
	if plan.inspectedEntries == limits.InspectedEntries {
		return hybridScalarIDs{}, false, errTypedGraphSearchBudget
	}
	if prefixBound > (limits.MappingWork-plan.mappingWork)/6 {
		return hybridScalarIDs{}, false, errTypedGraphSearchBudget
	}
	plan.mappingWork += 6 * prefixBound
	limit := limits.SourceIDs
	if probeRows > 0 {
		limit = probeRows + 1
	} // Fixed 512: P+1 cannot overflow.
	copied, inspected := 0, 0
	exhausted := false
	set, _, truncated, err := lookup.leafProbeBeforeCopy(leaf, limit, limits.InspectedEntries-plan.inspectedEntries, &inspected, func(id []byte) error {
		if copied&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		// Only this private callback outcome certifies intentional incomplete
		// discovery. The lookahead is inspected but neither charged nor copied.
		if probeRows > 0 && copied == probeRows {
			return errTypedGraphFilterProbeIncomplete
		}
		if plan.sourceIDs == limits.SourceIDs || len(id) > limits.SourceBytes-plan.sourceBytes {
			exhausted = true
			return errTypedGraphSearchBudget
		}
		plan.sourceIDs++
		plan.sourceBytes += len(id)
		copied++
		return nil
	})
	plan.inspectedEntries += inspected
	// Preserve the typed preparation budget class rather than the generic
	// scalar visitor's unavailable-index wrapper around our callback error.
	if exhausted {
		err = errTypedGraphSearchBudget
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return hybridScalarIDs{}, false, errors.Join(err, ctxErr)
	}
	if errors.Is(err, errTypedGraphFilterProbeIncomplete) {
		return hybridScalarIDs{}, false, nil
	}
	if err != nil {
		return hybridScalarIDs{}, false, err
	}
	if truncated {
		return hybridScalarIDs{}, false, errTypedGraphSearchBudget
	}
	return set, true, nil
}

func refineTypedGraphAND(ctx context.Context, plan *typedGraphPreparedFilter, allowed hybridScalarIDs, idx IndexDefinition, prefix []byte, limits typedGraphFilterLimits) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if allowed.len() == 0 {
		return nil
	}
	remaining := limits.MappingWork - plan.mappingWork
	if allowed.len() > typedGraphFilterProbeRows || allowed.len() > remaining {
		return errTypedGraphSearchBudget
	}
	// Reserve the request-count bound while admitting all temporary encoded
	// key payload bytes before allocation. The request units are charged only
	// immediately before submission; callback errors keep the submitted charge.
	payloadLimit := remaining - allowed.len()
	ids := make([]string, 0, allowed.len())
	keyBytes := 0
	for id := range allowed.all {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(prefix) > payloadLimit-keyBytes || len(id) > payloadLimit-keyBytes-len(prefix) {
			return errTypedGraphSearchBudget
		}
		keyBytes += len(prefix) + len(id)
		ids = append(ids, id)
	}
	plan.mappingWork += keyBytes
	arena := make([]byte, 0, keyBytes)
	keys := make([][]byte, len(ids))
	for i, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		arena, keys[i], err = appendIndexEntryKeyForValueType(arena, idx.ValueType, prefix, []byte(id))
		if err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	plan.mappingWork += len(keys)
	var callbackErr error
	err := collectionGetManyViewAtCatalogRoot(plan.overlay.current.snapshot, plan.overlay.current.catalog, collectionSecondaryRootName(plan.overlay.current.catalog.meta.Name, idx.Name), keys, func(i int, _ []byte, _ []byte, found bool) error {
		// GetMany's missing-root fallback must not swallow a callback error that
		// happens to wrap ErrKeyNotFound, or apply later membership results.
		if callbackErr != nil {
			return callbackErr
		}
		callbackErr = ctx.Err()
		if callbackErr != nil {
			return callbackErr
		}
		if !found {
			allowed.delete(ids[i])
		}
		return nil
	})
	if callbackErr != nil {
		return callbackErr
	}
	return errors.Join(err, ctx.Err())
}
