package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func (s *TypedColumnInt64PredicateAggregateSession) validatePreparedInt64AggregateColumn() error {
	if s == nil {
		return errors.New("collections: nil typed-column int64 predicate aggregate session")
	}
	column := s.aggregateColumn
	if column.Field.ValueType != ColumnStoreValueInt64 || column.Field.Nullable || column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingDeltaVarint || column.Definition.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("%w: typed-column int64 predicate aggregate column %q is not encoded as non-null scalar int64", ErrColumnQueryPlanUnsupported, s.req.Column)
	}
	if err := requireTypedColumnAdapterCapability(column, typedColumnInt64PredicateSemanticOperation(s.req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", s.req.Column)); err != nil {
		return err
	}
	if err := requireTypedColumnAdapterCapability(column, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", s.req.Column)); err != nil {
		return err
	}
	return nil
}

func (s *TypedColumnInt64PredicateAggregateSession) prepareTargetedAggregateState() error {
	if s == nil {
		return errors.New("collections: nil typed-column int64 predicate aggregate session")
	}
	if err := s.validatePreparedInt64AggregateColumn(); err != nil {
		return err
	}
	state := &typedColumnPreparedScanState{partsByRef: make(map[ColumnAssetRef]*typedColumnPreparedPartState, len(s.refsByGeneration))}
	prepareResult := &TypedColumnInt64PredicateAggregateResult{}
	beforeStats := mappedresource.Stats{}
	if s.resourceManager != nil {
		beforeStats = s.resourceManager.Stats()
	}
	beforeHits := s.readCache.hits
	beforeMisses := s.readCache.misses
	updateCacheDeltas := func() {
		prepareResult.Diagnostics.SegmentFileCacheHits = s.readCache.hits - beforeHits
		prepareResult.Diagnostics.SegmentFileCacheMisses = s.readCache.misses - beforeMisses
	}
	requests := []typedColumnPreparedColumnRequest{
		{
			Field:          s.aggregateColumn.Field,
			Role:           typedcolumn.ColumnRolePredicate,
			Operation:      typedColumnInt64PredicateSemanticOperation(s.req.Kind),
			IncludePruning: true,
		},
		{
			Field:     s.aggregateColumn.Field,
			Role:      typedcolumn.ColumnRoleMeasure,
			Operation: columnsemantics.OpSum,
		},
	}
	for _, physical := range s.view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef, ok := s.refsByGeneration[physical.Ref.Generation]
		if !ok {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate missing typed ref for physical generation=%d", physical.Ref.Generation)
		}
		if err := s.ensureCachedVerifyFullAssetValidated(typedRef.Ref, prepareResult, updateCacheDeltas); err != nil {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate validate generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		readRange := func(offset int, length int, section bool) ([]byte, error) {
			return s.readTypedColumnRange(typedRef.Ref, offset, length, section, prepareResult, updateCacheDeltas)
		}
		blockSelection := func(g typedcolumn.EncodedGranule, rows int) (typedcolumn.RowSelection, bool, error) {
			if g.HasMinMax && !typedColumnInt64PredicateMayMatch(typedColumnInt64PredicateAggregateScanRequest(s.req), g.Min, g.Max) {
				selection, err := typedcolumn.NewEmptyRowSelection(rows)
				return selection, false, err
			}
			selection, err := typedcolumn.NewAllRowSelection(rows)
			if err != nil {
				return typedcolumn.RowSelection{}, false, err
			}
			return selection, !typedColumnInt64PredicateCoversGranule(typedColumnInt64PredicateAggregateScanRequest(s.req), g), nil
		}
		part, partDiag, err := typedColumnPreparePartStateFromRanges(typedRef.Ref, physical.Ref, typedRef.Rows, physical.Rows, s.fields, s.schemaHash, requests, readRange, blockSelection)
		if err != nil {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate prepare generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if partDiag.Fallback {
			return fmt.Errorf("%w: %s", ErrColumnQueryPlanUnsupported, partDiag.FallbackReason)
		}
		if err := validateTypedColumnPreparedInt64AggregatePart(part, s.aggregateColumn); err != nil {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate prepare generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		state.partsByRef[typedRef.Ref] = part
		typedColumnPreparedStateDiagnosticsAdd(&state.diagnostics, partDiag)
	}
	if s.resourceManager != nil {
		afterStats := s.resourceManager.Stats()
		prepareResult.Diagnostics.FallbackReads += int(afterStats.FallbackReads - beforeStats.FallbackReads)
	}
	updateCacheDeltas()
	prepareResult.Diagnostics.DecodedMetadataBytes += state.diagnostics.ManifestBytes + state.diagnostics.DescriptorBytes
	addTypedColumnInt64PredicateAggregateDiagnostics(&s.prepareDiagnostics, prepareResult.Diagnostics)
	s.preparedState = state
	return nil
}

func validateTypedColumnPreparedInt64AggregatePart(part *typedColumnPreparedPartState, adapterColumn typedColumnAdapterColumn) error {
	if part == nil {
		return errors.New("collections: typed-column int64 aggregate prepared state is missing")
	}
	primaryName := typedColumnAdapterPrimaryIDColumn
	primary, primaryFound := part.PhysicalColumns[primaryName]
	if !primaryFound {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared image missing primary-id column %q", primaryName)
	}
	if primary.Definition.Type != typedcolumn.ColumnTypeInt64 || primary.Definition.Encoding != typedcolumn.EncodingRawInt64 || primary.Definition.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared image primary-id column %q type=%s encoding=%s compression=%s want type=%s encoding=%s compression=%s", primaryName, primary.Definition.Type, primary.Definition.Encoding, primary.Definition.Compression, typedcolumn.ColumnTypeInt64, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)
	}
	preparedColumn, ok := part.Columns[adapterColumn.Definition.Name]
	if !ok || preparedColumn == nil {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared state missing column %q", adapterColumn.Definition.Name)
	}
	if preparedColumn.Plan.Definition.Type != typedcolumn.ColumnTypeInt64 || preparedColumn.Plan.Definition.Encoding != typedcolumn.EncodingDeltaVarint || preparedColumn.Plan.Definition.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q type=%s encoding=%s compression=%s", adapterColumn.Definition.Name, preparedColumn.Plan.Definition.Type, preparedColumn.Plan.Definition.Encoding, preparedColumn.Plan.Definition.Compression)
	}
	return nil
}

func (s *TypedColumnInt64PredicateAggregateSession) scanPreparedAggregateStatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, preparedPart *typedColumnPreparedPartState, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) error {
	preparedColumn, ok := preparedPart.Columns[s.aggregateColumn.Definition.Name]
	if !ok || preparedColumn == nil {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate prepared state missing column %q", s.aggregateColumn.Definition.Name)
	}
	var visibility *typedColumnLatestPhysicalPart
	if s.resolver != nil {
		var ok bool
		visibility, ok = s.resolver.partForGeneration(physical.Ref.Generation)
		if !ok {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate missing latest-visible physical generation=%d", physical.Ref.Generation)
		}
	}
	partPruned, err := s.scanPreparedAggregateColumnState(preparedColumn, typedRef.Ref, visibility, result, updateCacheDeltas)
	if err != nil {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate scan generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
	}
	if partPruned {
		result.Diagnostics.PartsPruned++
	} else {
		result.Diagnostics.PartsDecoded++
	}
	return nil
}

func (s *TypedColumnInt64PredicateAggregateSession) scanPreparedAggregateColumnState(preparedColumn *typedColumnPreparedColumnState, ref ColumnAssetRef, visibility *typedColumnLatestPhysicalPart, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) (bool, error) {
	if preparedColumn == nil {
		return false, errors.New("collections: typed-column int64 predicate aggregate nil prepared column")
	}
	decodedAny := false
	payloadRead := false
	var err error
	for _, block := range preparedColumn.BlockPlans {
		result.Diagnostics.BlocksConsidered++
		if block.CandidateSelection.IsEmpty() {
			result.Diagnostics.BlocksPruned++
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, block.CandidateSelection)
			continue
		}
		var payload []byte
		if block.PayloadLength > 0 {
			payload, err = s.readTypedColumnRange(ref, block.PayloadOffset, block.PayloadLength, false, result, updateCacheDeltas)
			if err != nil {
				return false, fmt.Errorf("read column %q block %d payload: %w", preparedColumn.Plan.Definition.Name, block.Index, err)
			}
			payloadRead = true
		}
		if len(payload) != block.PayloadLength {
			return false, fmt.Errorf("typed-column int64 aggregate column %q block %d payload bytes=%d want %d", preparedColumn.Plan.Definition.Name, block.Index, len(payload), block.PayloadLength)
		}
		granule := block.Granule
		granule.Payload = payload
		granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: block.PayloadLength}
		values, err := s.aggregateScratch.reader.DecodeInt64Into(s.aggregateScratch.values[:0], granule)
		if err != nil {
			return false, err
		}
		s.aggregateScratch.values = values
		if len(values) != block.Descriptor.RowCount {
			return false, fmt.Errorf("decoded rows value=%d want %d", len(values), block.Descriptor.RowCount)
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.DecodedHeapCopyBytes += uint64(granule.RawBytes)
		result.Diagnostics.RowsScanned += len(values)

		selection := block.CandidateSelection
		if block.NeedsPredicate {
			selection, err = typedColumnInt64PredicateAggregateBlockSelection(typedColumnInt64PredicateAggregateScanRequest(s.req), granule, values, &s.aggregateScratch)
			if err != nil {
				return false, err
			}
		}
		if visibility != nil && !selection.IsEmpty() {
			visibilitySelection, err := typedColumnInt64VisibilitySelectionForBlock(visibility, block.Descriptor.FirstRow, block.Descriptor.RowCount, &s.aggregateScratch)
			if err != nil {
				return false, err
			}
			selection, err = typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &selection, Visibility: &visibilitySelection}, &s.aggregateScratch.selection)
			if err != nil {
				return false, err
			}
			result.Diagnostics.SelectionCompositions++
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		if selection.IsEmpty() {
			continue
		}
		if err := addTypedColumnInt64AggregateSelectedValues(result, values, selection); err != nil {
			return false, err
		}
	}
	if payloadRead {
		result.Diagnostics.DirectTypedColumnAssetReads++
	}
	return !decodedAny && len(preparedColumn.BlockPlans) != 0, nil
}
