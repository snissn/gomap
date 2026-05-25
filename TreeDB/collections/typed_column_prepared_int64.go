package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func (s *TypedColumnInt64PredicateAggregateSession) validatePreparedInt64AggregateColumn() error {
	if s == nil {
		return errors.New("collections: nil typed-column int64 predicate aggregate session")
	}
	column := s.aggregateColumn
	if column.Field.ValueType != ColumnStoreValueInt64 || column.Field.Nullable || column.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return fmt.Errorf("%w: typed-column int64 predicate aggregate column %q is not a non-null scalar int64 typed-column", ErrColumnQueryPlanUnsupported, s.req.Column)
	}
	if err := requireTypedColumnAdapterCapability(column, typedColumnInt64PredicateSemanticOperation(s.req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", s.req.Column)); err != nil {
		return err
	}
	if err := requireTypedColumnAdapterCapability(column, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", s.req.Column)); err != nil {
		return err
	}
	if err := requireTypedColumnLayoutCapability(column, typedColumnInt64PredicateSemanticOperation(s.req.Kind), fmt.Sprintf("typed-column int64 predicate aggregate column %q", s.req.Column)); err != nil {
		return err
	}
	if err := requireTypedColumnLayoutCapability(column, columnsemantics.OpSum, fmt.Sprintf("typed-column int64 predicate aggregate column %q", s.req.Column)); err != nil {
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
	if preparedColumn.Plan.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q type=%s", adapterColumn.Definition.Name, preparedColumn.Plan.Definition.Type)
	}
	if cap := preparedColumn.Plan.Layout.Supports(columnlayout.OpInt64NumericReducer); !cap.Supported() {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q layout capability %s", adapterColumn.Definition.Name, cap.Error())
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

func typedColumnInt64RawPayloadRows(raw []byte, rows int) error {
	if rows < 0 {
		return fmt.Errorf("typed-column raw int64 negative rows=%d", rows)
	}
	if len(raw)%8 != 0 || len(raw)/8 != rows {
		return fmt.Errorf("typed-column raw int64 payload bytes=%d rows=%d", len(raw), rows)
	}
	return nil
}

func typedColumnInt64RawPredicateSelection(req TypedColumnInt64PredicateScanRequest, g typedcolumn.EncodedGranule, raw []byte, scratch *typedColumnInt64PredicateAggregateScanScratch) (typedcolumn.RowSelection, error) {
	if err := typedColumnInt64RawPayloadRows(raw, g.Rows); err != nil {
		return typedcolumn.RowSelection{}, err
	}
	if g.Rows == 0 {
		return typedcolumn.NewEmptyRowSelection(0)
	}
	if typedColumnInt64PredicateCoversGranule(req, g) {
		return typedcolumn.NewAllRowSelection(g.Rows)
	}
	if scratch == nil {
		var local typedColumnInt64PredicateAggregateScanScratch
		scratch = &local
	}
	scratch.predicateRows = scratch.predicateRows[:0]
	for row := 0; row < g.Rows; row++ {
		value := int64(binary.LittleEndian.Uint64(raw[row*8:]))
		if typedColumnInt64PredicateMatches(req, value) {
			scratch.predicateRows = append(scratch.predicateRows, row)
		}
	}
	return typedColumnInt64PredicateRowsSelection(g.Rows, scratch)
}

func addTypedColumnInt64AggregateRawRow(result *TypedColumnInt64PredicateAggregateResult, raw []byte, rows int, row int) error {
	if row < 0 || row >= rows {
		return fmt.Errorf("typed-column int64 aggregate raw row=%d rows=%d", row, rows)
	}
	value := int64(binary.LittleEndian.Uint64(raw[row*8:]))
	if err := addTypedColumnInt64PredicateAggregateValue(result, value); err != nil {
		return err
	}
	result.Diagnostics.RowsMatched++
	return nil
}

func addTypedColumnInt64AggregateSelectedRawValues(result *TypedColumnInt64PredicateAggregateResult, raw []byte, rows int, selection typedcolumn.RowSelection) error {
	if err := typedColumnInt64RawPayloadRows(raw, rows); err != nil {
		return err
	}
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return nil
	case typedcolumn.RowSelectionAll:
		for row := 0; row < rows; row++ {
			if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row); err != nil {
				return err
			}
		}
		return nil
	case typedcolumn.RowSelectionRange:
		start, end, ok := selection.SingleRange()
		if !ok || start < 0 || end < start || end > rows {
			return fmt.Errorf("typed-column int64 aggregate invalid raw range selection [%d,%d) rows=%d", start, end, rows)
		}
		for row := start; row < end; row++ {
			if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row); err != nil {
				return err
			}
		}
		return nil
	case typedcolumn.RowSelectionRanges:
		for _, r := range selection.Ranges() {
			if r.Start < 0 || r.End < r.Start || r.End > rows {
				return fmt.Errorf("typed-column int64 aggregate invalid raw ranges selection [%d,%d) rows=%d", r.Start, r.End, rows)
			}
			for row := r.Start; row < r.End; row++ {
				if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row); err != nil {
					return err
				}
			}
		}
		return nil
	case typedcolumn.RowSelectionSparse:
		for _, row := range selection.SparseRows() {
			if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row); err != nil {
				return err
			}
		}
		return nil
	case typedcolumn.RowSelectionBitmap:
		for wordIndex, word := range selection.BitmapWords() {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row >= rows {
					break
				}
				if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row); err != nil {
					return err
				}
				word &^= uint64(1) << uint(bit)
			}
		}
		return nil
	default:
		return fmt.Errorf("typed-column int64 aggregate unsupported raw selection shape %s", selection.Shape().Kind)
	}
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
				if preparedColumn.Plan.Layout.Reducers.Int64FixedWidthRaw {
					return false, fmt.Errorf("raw layout read column %q block %d payload: %w", preparedColumn.Plan.Definition.Name, block.Index, err)
				}
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
		if preparedColumn.Plan.Layout.Reducers.Int64FixedWidthRaw {
			if granule.Rows != block.Descriptor.RowCount {
				return false, fmt.Errorf("raw layout column %q block %d granule rows=%d want descriptor row_count=%d: %s", preparedColumn.Plan.Definition.Name, block.Index, granule.Rows, block.Descriptor.RowCount, columnlayout.ReasonDescriptorRowCountMismatch)
			}
			if err := preparedColumn.Plan.Layout.ValidateGranulePayload(granule, payload); err != nil {
				return false, err
			}
			decodedAny = true
			result.Diagnostics.BlocksDecoded++
			result.Diagnostics.RowsScanned += granule.Rows

			selection := block.CandidateSelection
			if block.NeedsPredicate {
				selection, err = typedColumnInt64RawPredicateSelection(typedColumnInt64PredicateAggregateScanRequest(s.req), granule, payload, &s.aggregateScratch)
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
			if err := addTypedColumnInt64AggregateSelectedRawValues(result, payload, granule.Rows, selection); err != nil {
				return false, err
			}
			continue
		}
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
