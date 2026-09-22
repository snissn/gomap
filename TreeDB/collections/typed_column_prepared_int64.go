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
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
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

func typedColumnInt64PruningPredicate(req TypedColumnInt64PredicateScanRequest) (typedcolumn.Int64PruningPredicate, bool) {
	switch req.Kind {
	case TypedColumnInt64PredicateAll:
		return typedcolumn.Int64PruningPredicate{Kind: typedcolumn.Int64PruningPredicateAll}, true
	case TypedColumnInt64PredicateEqual:
		return typedcolumn.Int64PruningPredicate{Kind: typedcolumn.Int64PruningPredicateEqual, Value: req.Value}, true
	case TypedColumnInt64PredicateRange:
		if req.Low > req.High {
			return typedcolumn.Int64PruningPredicate{}, false
		}
		return typedcolumn.Int64PruningPredicate{Kind: typedcolumn.Int64PruningPredicateRange, Low: req.Low, High: req.High}, true
	default:
		return typedcolumn.Int64PruningPredicate{}, false
	}
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
	includeStats := s.req.ColumnAssetReadIntegrity != ColumnAssetReadIntegritySkipChecksums
	pruningPredicate, pruningOK := typedColumnInt64PruningPredicate(typedColumnInt64PredicateAggregateScanRequest(s.req))
	includePruning := pruningOK && pruningPredicate.Kind != typedcolumn.Int64PruningPredicateAll
	requests := []typedColumnPreparedColumnRequest{
		{
			Field:                    s.aggregateColumn.Field,
			Role:                     typedcolumn.ColumnRolePredicate,
			Operation:                typedColumnInt64PredicateSemanticOperation(s.req.Kind),
			IncludePruning:           includePruning,
			HasInt64PruningPredicate: includePruning,
			Int64PruningPredicate:    pruningPredicate,
		},
		{
			Field:        s.aggregateColumn.Field,
			Role:         typedcolumn.ColumnRoleMeasure,
			Operation:    columnsemantics.OpSum,
			IncludeStats: includeStats,
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
	prepareResult.Diagnostics.DecodedMetadataBytes += state.diagnostics.ManifestBytes + state.diagnostics.DescriptorBytes + state.diagnostics.ContractBytes + state.diagnostics.DecodedMetadataBytes
	prepareResult.Diagnostics.DirectViewCertified += state.diagnostics.DirectViewCertified
	prepareResult.Diagnostics.StreamingCertified += state.diagnostics.StreamingCertified
	prepareResult.Diagnostics.StatsCertified += state.diagnostics.StatsCertified
	prepareResult.Diagnostics.PruningCertified += state.diagnostics.PruningCertified
	prepareResult.Diagnostics.CertificationFailures += state.diagnostics.CertificationFailures
	prepareResult.Diagnostics.CertificationFailureReason = state.diagnostics.CertificationFailureReason
	prepareResult.Diagnostics.StatsValidationFailures += state.diagnostics.StatsValidationFailures
	prepareResult.Diagnostics.StatsValidationFailureReason = state.diagnostics.StatsValidationFailureReason
	prepareResult.Diagnostics.PruningBlocks += state.diagnostics.PruningBlocks
	prepareResult.Diagnostics.PruningRows += state.diagnostics.PruningRows
	prepareResult.Diagnostics.PruningFallbackBlocks += state.diagnostics.PruningFallbackBlocks
	prepareResult.Diagnostics.PruningFallbackReason = state.diagnostics.PruningFallbackReason
	prepareResult.Diagnostics.PruningValidationFailures += state.diagnostics.PruningValidationFailures
	prepareResult.Diagnostics.PruningValidationFailureReason = state.diagnostics.PruningValidationFailureReason
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
	if primary.Definition.Type != typedcolumn.ColumnTypeInt64 {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared image primary-id column %q type=%s want %s", primaryName, primary.Definition.Type, typedcolumn.ColumnTypeInt64)
	}
	switch primary.Definition.Encoding {
	case typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
	default:
		return fmt.Errorf("collections: typed-column int64 aggregate prepared image primary-id column %q encoding=%s is unsupported", primaryName, primary.Definition.Encoding)
	}
	if err := validateTypedColumnProductionCompression(primary.Definition.Compression); err != nil {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared image primary-id column %q compression=%s: %w", primaryName, primary.Definition.Compression, err)
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
	if preparedColumn.Plan.Layout.Reducers.Int64FixedWidthRaw && !preparedColumn.Certification.DirectViewCertified {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q lacks certified fixed-width direct-view contract", adapterColumn.Definition.Name)
	}
	if preparedColumn.Plan.Layout.Reducers.Int64Streaming && !preparedColumn.Certification.StreamingCertified {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q lacks certified streaming contract", adapterColumn.Definition.Name)
	}
	reducer, err := typedColumnPreparedInt64AggregateReducer(preparedColumn)
	if err != nil {
		return err
	}
	preparedColumn.AggregateReducer = reducer
	preparedColumn.AggregateReducerReady = true
	return nil
}

func typedColumnPreparedInt64AggregateReducer(preparedColumn *typedColumnPreparedColumnState) (typedkernel.PreparedReducer, error) {
	if preparedColumn == nil {
		return typedkernel.PreparedReducer{}, errors.New("collections: typed-column int64 aggregate prepared reducer missing column state")
	}
	desc := columnsemantics.Descriptor{
		Logical:             preparedColumn.Plan.Logical,
		Physical:            preparedColumn.Plan.Definition.Type,
		Encoding:            preparedColumn.Plan.Definition.Encoding,
		Nullable:            preparedColumn.Plan.Field.Nullable,
		DictionaryOrder:     preparedColumn.Plan.Layout.Descriptor.DictionaryOrder,
		DictionaryCollation: preparedColumn.Plan.Layout.Descriptor.DictionaryCollation,
	}
	reducer, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: preparedColumn.Plan.Operation, Semantic: desc, Layout: preparedColumn.Plan.Layout})
	if err != nil {
		return typedkernel.PreparedReducer{}, fmt.Errorf("collections: typed-column int64 aggregate prepared column %q kernel dispatch: %w", preparedColumn.Plan.Definition.Name, err)
	}
	return reducer, nil
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
	if visibility != nil && typedColumnPreparedPartHasLogicalSortKey(preparedPart) {
		return typedColumnSortedMutationVisibilityUnsupported("typed-column int64 predicate aggregate")
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

func addTypedColumnInt64AggregateRawRow(result *TypedColumnInt64PredicateAggregateResult, raw []byte, rows int, row int, expression TypedColumnInt64AggregateExpression) error {
	if row < 0 || row >= rows {
		return fmt.Errorf("typed-column int64 aggregate raw row=%d rows=%d", row, rows)
	}
	value := int64(binary.LittleEndian.Uint64(raw[row*8:]))
	if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, value); err != nil {
		return err
	}
	result.Diagnostics.RowsMatched++
	return nil
}

func addTypedColumnInt64AggregateSelectedRawValues(result *TypedColumnInt64PredicateAggregateResult, raw []byte, rows int, selection typedcolumn.RowSelection, expression TypedColumnInt64AggregateExpression) error {
	if err := typedColumnInt64RawPayloadRows(raw, rows); err != nil {
		return err
	}
	switch selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return nil
	case typedcolumn.RowSelectionAll:
		for row := 0; row < rows; row++ {
			if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row, expression); err != nil {
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
			if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row, expression); err != nil {
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
				if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row, expression); err != nil {
					return err
				}
			}
		}
		return nil
	case typedcolumn.RowSelectionSparse:
		for _, row := range selection.SparseRows() {
			if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row, expression); err != nil {
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
				if err := addTypedColumnInt64AggregateRawRow(result, raw, rows, row, expression); err != nil {
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

func addTypedColumnInt64AggregateKernelResult(result *TypedColumnInt64PredicateAggregateResult, out typedkernel.AggregateResult) error {
	if result == nil {
		return errors.New("collections: nil typed-column int64 aggregate result")
	}
	if out.NonNulls < 0 || out.NonNulls > int64(maxCollectionInt) {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate invalid kernel count=%d", out.NonNulls)
	}
	if out.Sum > 0 && result.Sum > typedColumnInt64PredicateAggregateMaxSum-out.Sum {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", result.Sum, out.Sum)
	}
	if out.Sum < 0 && result.Sum < typedColumnInt64PredicateAggregateMinSum-out.Sum {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", result.Sum, out.Sum)
	}
	result.Count += out.NonNulls
	result.Sum += out.Sum
	result.Diagnostics.RowsMatched += int(out.NonNulls)
	return nil
}

func addTypedColumnInt64AggregateKernelValues(result *TypedColumnInt64PredicateAggregateResult, reducer typedkernel.PreparedReducer, values []int64, selection typedcolumn.RowSelection, expression TypedColumnInt64AggregateExpression, scratch *typedkernel.Scratch) error {
	if !typedColumnInt64AggregateExpressionIsIdentity(expression) {
		return addTypedColumnInt64AggregateSelectedValues(result, values, selection, expression)
	}
	out, err := reducer.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Values: values}, scratch)
	if err != nil {
		return err
	}
	return addTypedColumnInt64AggregateKernelResult(result, out)
}

func addTypedColumnInt64AggregateKernelCursor(result *TypedColumnInt64PredicateAggregateResult, reducer typedkernel.PreparedReducer, cursor *typedcolumn.Int64Cursor, rows int, selection typedcolumn.RowSelection, expression TypedColumnInt64AggregateExpression, scratch *typedkernel.Scratch) error {
	if expression == TypedColumnInt64AggregateSecondOfDaySquare {
		return addTypedColumnInt64AggregateSecondOfDaySquareSelectedCursor(result, cursor, rows, selection)
	}
	if !typedColumnInt64AggregateExpressionIsIdentity(expression) {
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return err
			}
			if !selection.Contains(row) {
				continue
			}
			if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, value); err != nil {
				return err
			}
			result.Diagnostics.RowsMatched++
		}
		return cursor.Finish()
	}
	out, err := reducer.Reduce(typedkernel.ReduceRequest{Rows: rows, Selection: selection, Int64Cursor: cursor}, scratch)
	if err != nil {
		return err
	}
	return addTypedColumnInt64AggregateKernelResult(result, out)
}

func addTypedColumnInt64AggregateSecondOfDaySquareSelectedCursor(result *TypedColumnInt64PredicateAggregateResult, cursor *typedcolumn.Int64Cursor, rows int, selection typedcolumn.RowSelection) error {
	if cursor == nil {
		return errors.New("collections: nil typed-column int64 aggregate cursor")
	}
	if cursor.Rows() != rows {
		return fmt.Errorf("collections: typed-column int64 aggregate cursor rows=%d want %d", cursor.Rows(), rows)
	}
	count := selection.Count()
	if count == 0 {
		for row := 0; row < rows; row++ {
			if _, err := cursor.Next(); err != nil {
				return err
			}
		}
		return cursor.Finish()
	}
	checkOverflow := typedColumnInt64AggregateSecondOfDaySquareCountCanOverflow(count)
	var sum int64
	switch selection.Kind() {
	case typedcolumn.RowSelectionAll:
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return err
			}
			if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, value, checkOverflow); err != nil {
				return err
			}
		}
	case typedcolumn.RowSelectionRange:
		start, end, ok := selection.SingleRange()
		if !ok || start < 0 || end < start || end > rows {
			return fmt.Errorf("typed-column int64 aggregate invalid cursor range selection [%d,%d) rows=%d", start, end, rows)
		}
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return err
			}
			if row >= start && row < end {
				if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, value, checkOverflow); err != nil {
					return err
				}
			}
		}
	case typedcolumn.RowSelectionRanges:
		ranges := selection.Ranges()
		rangeIndex := 0
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return err
			}
			for rangeIndex < len(ranges) && row >= ranges[rangeIndex].End {
				rangeIndex++
			}
			if rangeIndex >= len(ranges) {
				continue
			}
			r := ranges[rangeIndex]
			if r.Start < 0 || r.End < r.Start || r.End > rows {
				return fmt.Errorf("typed-column int64 aggregate invalid cursor ranges selection [%d,%d) rows=%d", r.Start, r.End, rows)
			}
			if row >= r.Start {
				if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, value, checkOverflow); err != nil {
					return err
				}
			}
		}
	case typedcolumn.RowSelectionSparse:
		sparse := selection.SparseRows()
		sparseIndex := 0
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return err
			}
			if sparseIndex >= len(sparse) {
				continue
			}
			selected := sparse[sparseIndex]
			if selected < 0 || selected >= rows {
				return fmt.Errorf("typed-column int64 aggregate sparse row=%d rows=%d", selected, rows)
			}
			if row == selected {
				if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, value, checkOverflow); err != nil {
					return err
				}
				sparseIndex++
			}
		}
	case typedcolumn.RowSelectionBitmap:
		words := selection.BitmapWords()
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return err
			}
			wordIndex := row / 64
			if wordIndex >= len(words) || words[wordIndex]&(uint64(1)<<uint(row%64)) == 0 {
				continue
			}
			if err := addTypedColumnInt64AggregateSecondOfDaySquareLocal(&sum, value, checkOverflow); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("typed-column int64 aggregate unsupported cursor selection shape %s", selection.Shape().Kind)
	}
	if err := cursor.Finish(); err != nil {
		return err
	}
	return addTypedColumnInt64AggregateSecondOfDaySquareBatch(result, count, sum)
}

func recordTypedColumnInt64KernelBlock(diag *TypedColumnInt64PredicateScanDiagnostics, fullCovered bool, cursor bool) {
	if diag == nil {
		return
	}
	diag.KernelBlocks++
	if fullCovered {
		diag.KernelFullCoveredBlocks++
	} else {
		diag.KernelSelectedBlocks++
	}
	if cursor {
		diag.KernelCursorBlocks++
	}
}

func recordTypedColumnInt64KernelFallbackBlock(diag *TypedColumnInt64PredicateScanDiagnostics) {
	if diag == nil {
		return
	}
	diag.KernelFallbackBlocks++
}

func recordTypedColumnInt64StatsBlock(diag *TypedColumnInt64PredicateScanDiagnostics, rows int) {
	if diag == nil {
		return
	}
	diag.StatsBlocks++
	diag.StatsFullCoveredBlocks++
	diag.StatsRows += rows
}

func recordTypedColumnInt64StatsFallbackBlock(diag *TypedColumnInt64PredicateScanDiagnostics, reason string) {
	if diag == nil {
		return
	}
	diag.StatsFallbackBlocks++
	if reason != "" {
		diag.StatsFallbackReason = reason
	}
}

func recordTypedColumnInt64PruningBlock(diag *TypedColumnInt64PredicateScanDiagnostics, rows int) {
	if diag == nil {
		return
	}
	diag.PruningBlocks++
	diag.PruningRows += rows
}

func recordTypedColumnInt64PruningFallbackBlock(diag *TypedColumnInt64PredicateScanDiagnostics, reason string) {
	if diag == nil {
		return
	}
	diag.PruningFallbackBlocks++
	if reason != "" {
		diag.PruningFallbackReason = reason
	}
}

func recordTypedColumnInt64FastDecodePlan(diag *TypedColumnInt64PredicateScanDiagnostics, plan typeddecode.Plan) {
	if diag == nil {
		return
	}
	switch plan.Path {
	case typeddecode.PathDirectView:
		diag.FastDecodeDirectViewPlans++
	case typeddecode.PathStreaming:
		diag.FastDecodeStreamingPlans++
		if plan.Reason != "" && plan.Reason != typeddecode.ReasonSupported {
			recordTypedColumnInt64FallbackReasonCounter(diag, plan.Reason)
		}
	case typeddecode.PathMaterialize:
		diag.FastDecodeMaterializePlans++
	case typeddecode.PathUnsupported:
		diag.FastDecodeUnsupportedPlans++
		if plan.Reason != "" && plan.Reason != typeddecode.ReasonSupported {
			recordTypedColumnInt64FallbackReasonCounter(diag, plan.Reason)
		}
	}
	if plan.Reason != "" && plan.Reason != typeddecode.ReasonSupported {
		diag.FastDecodeFallbackReason = string(plan.Reason)
	}
}

func recordTypedColumnInt64DirectViewStatus(diag *TypedColumnInt64PredicateScanDiagnostics, status typeddecode.Status) {
	recordTypedColumnInt64DirectViewStatusWithHandle(diag, status, nil)
}

func recordTypedColumnInt64DirectViewStatusWithHandle(diag *TypedColumnInt64PredicateScanDiagnostics, status typeddecode.Status, handle *mappedresource.Handle) {
	if diag == nil {
		return
	}
	if status.Direct() {
		diag.DirectViewSuccesses++
		if handle != nil && handle.Source() == mappedresource.SourceHeapCopy {
			diag.FastDecodeHeapCopyTypedViews++
		} else {
			diag.FastDecodeMmapDirectViews++
		}
		return
	}
	diag.DirectViewFailures++
	recordTypedColumnInt64FallbackReasonCounter(diag, status.Reason)
	if status.Reason != "" && status.Reason != typeddecode.ReasonSupported {
		diag.FastDecodeFallbackReason = string(status.Reason)
	}
}

func recordTypedColumnInt64ScratchDecode(diag *TypedColumnInt64PredicateScanDiagnostics, reason typeddecode.Reason) {
	if diag == nil {
		return
	}
	diag.FastDecodeScratchDecodes++
	if reason != "" && reason != typeddecode.ReasonSupported {
		diag.FastDecodeStreamingFallbacks++
		diag.FastDecodeFallbackReason = string(reason)
	}
}

func recordTypedColumnInt64FallbackReasonCounter(diag *TypedColumnInt64PredicateScanDiagnostics, reason typeddecode.Reason) {
	if diag == nil || reason == "" || reason == typeddecode.ReasonSupported {
		return
	}
	switch reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonUnaligned:
		diag.FastDecodeAbsoluteUnaligned++
	case typeddecode.ReasonActualPointerUnaligned:
		diag.FastDecodeActualUnaligned++
	case typeddecode.ReasonStaleHandle:
		diag.FastDecodeStaleHandles++
	case typeddecode.ReasonNotWriterCertified, typeddecode.ReasonWrongEndian, typeddecode.ReasonLengthMultipleMismatch, typeddecode.ReasonPayloadLengthMismatch, typeddecode.ReasonRowCountMismatch, typeddecode.ReasonDimensionMismatch, typeddecode.ReasonCompressed, typeddecode.ReasonNullableWrapper, typeddecode.ReasonValidationFailed:
		diag.FastDecodeCertificationFailure++
	}
}

func typedColumnInt64DirectViewFallbackAllowed(status typeddecode.Status) bool {
	if status.Direct() {
		return false
	}
	if status.Path != typeddecode.PathStreaming {
		return false
	}
	switch status.Reason {
	case typeddecode.ReasonAbsoluteOffsetUnaligned, typeddecode.ReasonActualPointerUnaligned, typeddecode.ReasonUnaligned, typeddecode.ReasonHandleSourceUnsupported, typeddecode.ReasonWrongEndian, typeddecode.ReasonNotWriterCertified:
		return true
	default:
		return false
	}
}

func addTypedColumnInt64AggregateStreamingValues(result *TypedColumnInt64PredicateAggregateResult, req TypedColumnInt64PredicateScanRequest, expression TypedColumnInt64AggregateExpression, granule typedcolumn.EncodedGranule, block typedColumnPreparedBlockPlan, reducer typedkernel.PreparedReducer, visibility *typedColumnLatestPhysicalPart, scratch *typedColumnInt64PredicateAggregateScanScratch) error {
	if result == nil {
		return errors.New("collections: nil typed-column int64 aggregate result")
	}
	if scratch == nil {
		var local typedColumnInt64PredicateAggregateScanScratch
		scratch = &local
	}
	rows := granule.Rows
	if rows != block.Descriptor.RowCount {
		return fmt.Errorf("typed-column int64 streaming rows=%d want block rows=%d", rows, block.Descriptor.RowCount)
	}
	result.Diagnostics.RowsScanned += rows
	selection := block.CandidateSelection
	identityExpression := typedColumnInt64AggregateExpressionIsIdentity(expression)
	if selection.IsAll() && !block.NeedsPredicate && visibility == nil && identityExpression {
		count, sum, err := scratch.reader.CountSumInt64(granule)
		if err != nil {
			return err
		}
		if count != rows {
			return fmt.Errorf("typed-column int64 streaming count=%d want rows=%d", count, rows)
		}
		if err := addTypedColumnInt64AggregateKernelResult(result, typedkernel.AggregateResult{Op: reducer.Operation(), NonNulls: int64(count), Sum: sum, HasValue: true}); err != nil {
			return err
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		recordTypedColumnInt64KernelBlock(&result.Diagnostics, true, false)
		return nil
	}
	if selection.IsAll() && !block.NeedsPredicate && identityExpression {
		if visibility != nil {
			visibilitySelection, err := typedColumnInt64VisibilitySelectionForBlock(visibility, block.Descriptor.FirstRow, block.Descriptor.RowCount, scratch)
			if err != nil {
				return err
			}
			selection, err = typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &selection, Visibility: &visibilitySelection}, &scratch.selection)
			if err != nil {
				return err
			}
			result.Diagnostics.SelectionCompositions++
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		if selection.IsEmpty() {
			return nil
		}
		cursor, err := scratch.reader.Int64Cursor(granule)
		if err != nil {
			return err
		}
		if err := addTypedColumnInt64AggregateKernelCursor(result, reducer, &cursor, rows, selection, expression, &scratch.kernel); err != nil {
			return err
		}
		recordTypedColumnInt64KernelBlock(&result.Diagnostics, visibility == nil && selection.IsAll(), true)
		return nil
	}
	if selection.IsAll() && !block.NeedsPredicate && visibility == nil && expression == TypedColumnInt64AggregateSecondOfDaySquare {
		recordTypedColumnInt64KernelFallbackBlock(&result.Diagnostics)
		cursor, err := scratch.reader.Int64Cursor(granule)
		if err != nil {
			return err
		}
		if err := addTypedColumnInt64AggregateSecondOfDaySquareSelectedCursor(result, &cursor, rows, selection); err != nil {
			return err
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		return nil
	}
	recordTypedColumnInt64KernelFallbackBlock(&result.Diagnostics)
	cursor, err := scratch.reader.Int64Cursor(granule)
	if err != nil {
		return err
	}
	needFinalSelection := block.NeedsPredicate || visibility != nil || !selection.IsAll()
	if needFinalSelection {
		scratch.predicateRows = scratch.predicateRows[:0]
	}
	for row := 0; row < rows; row++ {
		value, err := cursor.Next()
		if err != nil {
			return err
		}
		if !selection.Contains(row) {
			continue
		}
		if block.NeedsPredicate && !typedColumnInt64PredicateMatches(req, value) {
			continue
		}
		if visibility != nil && !visibility.rowVisible(block.Descriptor.FirstRow+row) {
			continue
		}
		if err := addTypedColumnInt64PredicateAggregateExpressionValue(result, expression, value); err != nil {
			return err
		}
		result.Diagnostics.RowsMatched++
		if needFinalSelection {
			scratch.predicateRows = append(scratch.predicateRows, row)
		}
	}
	if err := cursor.Finish(); err != nil {
		return err
	}
	if needFinalSelection {
		finalSelection, err := typedColumnInt64PredicateRowsSelection(rows, scratch)
		if err != nil {
			return err
		}
		if visibility != nil {
			result.Diagnostics.SelectionCompositions++
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, finalSelection)
		return nil
	}
	recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
	return nil
}

func addTypedColumnInt64AggregateStatsBlock(result *TypedColumnInt64PredicateAggregateResult, preparedColumn *typedColumnPreparedColumnState, block *typedColumnPreparedBlockPlan, expression TypedColumnInt64AggregateExpression) (bool, error) {
	if result == nil || preparedColumn == nil {
		return false, nil
	}
	if !preparedColumn.Int64StatsReady {
		if preparedColumn.StatsFallbackReason != "" {
			recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, preparedColumn.StatsFallbackReason)
		}
		return false, nil
	}
	if !typedColumnInt64AggregateExpressionIsIdentity(expression) {
		recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, "aggregate_expression")
		return false, nil
	}
	if !block.CandidateSelection.IsAll() || block.NeedsPredicate {
		recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, typedcolumn.ColumnStatsReasonSelectionUnsupported)
		return false, nil
	}
	ok, reason := preparedColumn.Int64Stats.CanAnswer(typedcolumn.ColumnStatsOpSum, typedcolumn.ColumnStatsSelectionFullBlock)
	if !ok {
		recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, reason)
		return false, nil
	}
	blockStats, ok := preparedColumn.Int64Stats.Block(block.Index)
	if !ok {
		return false, fmt.Errorf("collections: typed-column int64 stats missing block %d for column %q", block.Index, preparedColumn.Plan.Definition.Name)
	}
	if blockStats.FirstRow != block.Descriptor.FirstRow || blockStats.RowCount != block.Descriptor.RowCount || blockStats.ValueCount != block.Descriptor.RowCount {
		return false, fmt.Errorf("collections: typed-column int64 stats block %d row identity mismatch", block.Index)
	}
	ok, reason = blockStats.CanAnswer(typedcolumn.ColumnStatsOpSum)
	if !ok {
		recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, reason)
		return false, nil
	}
	if err := addTypedColumnInt64AggregateKernelResult(result, typedkernel.AggregateResult{Op: preparedColumn.AggregateReducer.Operation(), NonNulls: int64(blockStats.ValueCount), Sum: blockStats.Sum, HasValue: true}); err != nil {
		return false, err
	}
	recordTypedColumnSelectionDiagnostics(&result.Diagnostics, block.CandidateSelection)
	recordTypedColumnInt64StatsBlock(&result.Diagnostics, blockStats.ValueCount)
	return true, nil
}

func (s *TypedColumnInt64PredicateAggregateSession) addTypedColumnInt64AggregateTypedViewValues(result *TypedColumnInt64PredicateAggregateResult, preparedColumn *typedColumnPreparedColumnState, block *typedColumnPreparedBlockPlan, granule typedcolumn.EncodedGranule, values []int64, visibility *typedColumnLatestPhysicalPart) error {
	if result == nil || preparedColumn == nil || block == nil {
		return errors.New("collections: typed-column int64 aggregate typed-view path missing state")
	}
	result.Diagnostics.BlocksDecoded++
	result.Diagnostics.RowsScanned += len(values)
	selection := block.CandidateSelection
	if block.NeedsPredicate {
		predicateSelection, err := typedColumnInt64PredicateAggregateBlockSelection(typedColumnInt64PredicateAggregateScanRequest(s.req), granule, values, &s.aggregateScratch)
		if err != nil {
			return err
		}
		selection = predicateSelection
		if !block.CandidateSelection.IsAll() {
			composed, err := typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &predicateSelection, Visibility: &block.CandidateSelection}, &s.aggregateScratch.selection)
			if err != nil {
				return err
			}
			selection = composed
			result.Diagnostics.SelectionCompositions++
		}
	}
	if visibility != nil && !selection.IsEmpty() {
		visibilitySelection, err := typedColumnInt64VisibilitySelectionForBlock(visibility, block.Descriptor.FirstRow, block.Descriptor.RowCount, &s.aggregateScratch)
		if err != nil {
			return err
		}
		composed, err := typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &selection, Visibility: &visibilitySelection}, &s.aggregateScratch.selection)
		if err != nil {
			return err
		}
		selection = composed
		result.Diagnostics.SelectionCompositions++
	}
	recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
	if selection.IsEmpty() {
		return nil
	}
	if err := addTypedColumnInt64AggregateKernelValues(result, preparedColumn.AggregateReducer, values, selection, s.req.Expression, &s.aggregateScratch.kernel); err != nil {
		return err
	}
	if typedColumnInt64AggregateExpressionIsIdentity(s.req.Expression) {
		recordTypedColumnInt64KernelBlock(&result.Diagnostics, !block.NeedsPredicate && visibility == nil && selection.IsAll(), false)
	} else {
		recordTypedColumnInt64KernelFallbackBlock(&result.Diagnostics)
	}
	return nil
}

func (s *TypedColumnInt64PredicateAggregateSession) scanPreparedAggregateColumnState(preparedColumn *typedColumnPreparedColumnState, ref ColumnAssetRef, visibility *typedColumnLatestPhysicalPart, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) (bool, error) {
	if preparedColumn == nil {
		return false, errors.New("collections: typed-column int64 predicate aggregate nil prepared column")
	}
	if !preparedColumn.AggregateReducerReady {
		return false, fmt.Errorf("collections: typed-column int64 predicate aggregate prepared column %q missing kernel reducer", preparedColumn.Plan.Definition.Name)
	}
	decodedAny := false
	statsAny := false
	payloadRead := false
	identityExpression := typedColumnInt64AggregateExpressionIsIdentity(s.req.Expression)
	columnPlan := typeddecode.Int64ReducerPlan(preparedColumn.Plan.Layout, preparedColumn.Certification)
	recordTypedColumnInt64FastDecodePlan(&result.Diagnostics, columnPlan)
	var err error
	for blockIdx := range preparedColumn.BlockPlans {
		block := &preparedColumn.BlockPlans[blockIdx]
		result.Diagnostics.BlocksConsidered++
		if preparedColumn.Int64PruningReady && !block.CandidateSelection.IsAll() {
			recordTypedColumnInt64PruningBlock(&result.Diagnostics, block.CandidateSelection.Count())
		}
		if block.CandidateSelection.IsEmpty() {
			result.Diagnostics.BlocksPruned++
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, block.CandidateSelection)
			continue
		}
		if preparedColumn.PruningFallbackReason != "" {
			recordTypedColumnInt64PruningFallbackBlock(&result.Diagnostics, preparedColumn.PruningFallbackReason)
		}
		if visibility == nil && s.req.ColumnAssetReadIntegrity != ColumnAssetReadIntegritySkipChecksums {
			usedStats, err := addTypedColumnInt64AggregateStatsBlock(result, preparedColumn, block, s.req.Expression)
			if err != nil {
				return false, err
			}
			if usedStats {
				statsAny = true
				continue
			}
		} else if !identityExpression && preparedColumn.Int64StatsReady && block.CandidateSelection.IsAll() && !block.NeedsPredicate {
			recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, "aggregate_expression")
		} else if s.req.ColumnAssetReadIntegrity == ColumnAssetReadIntegritySkipChecksums && preparedColumn.Int64StatsReady && block.CandidateSelection.IsAll() && !block.NeedsPredicate {
			recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, "skip_checksums")
		} else if preparedColumn.Int64StatsReady && block.CandidateSelection.IsAll() && !block.NeedsPredicate {
			recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, "visibility_selection")
		} else if !preparedColumn.Int64StatsReady && preparedColumn.StatsFallbackReason != "" {
			recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, preparedColumn.StatsFallbackReason)
		}
		var payload []byte
		var handle *mappedresource.Handle
		if block.PayloadLength > 0 {
			payload, handle, err = s.readTypedColumnRangeHandle(ref, block.PayloadOffset, block.PayloadLength, false, result, updateCacheDeltas)
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

		if columnPlan.Path == typeddecode.PathUnsupported {
			return false, fmt.Errorf("collections: typed-column int64 aggregate fast decode unsupported for column %q: %s", preparedColumn.Plan.Definition.Name, columnPlan.Status().String())
		}
		streamingFallbackReason := typeddecode.Reason("")
		if columnPlan.DirectCandidate() {
			if block.Index < 0 || block.Index >= len(preparedColumn.Certification.Blocks) {
				return false, fmt.Errorf("collections: typed-column int64 aggregate direct-view block index=%d outside certification blocks=%d", block.Index, len(preparedColumn.Certification.Blocks))
			}
			blockStatus := typeddecode.ValidateDirectViewBlock(typeddecode.DirectViewBlockRequest{Plan: columnPlan, Certification: preparedColumn.Certification, Block: preparedColumn.Certification.Blocks[block.Index], Rows: granule.Rows, PayloadBytes: len(payload), AssetOffset: int64(ref.Offset), HasAssetOffset: true})
			if blockStatus.Direct() {
				values, viewStatus := typeddecode.Int64View(s.resourceManager, handle, typeddecode.ResourceViewOptions{ExpectedElements: granule.Rows, RequireMapped: true})
				recordTypedColumnInt64DirectViewStatusWithHandle(&result.Diagnostics, viewStatus, handle)
				if viewStatus.Direct() {
					decodedAny = true
					if err := s.addTypedColumnInt64AggregateTypedViewValues(result, preparedColumn, block, granule, values, visibility); err != nil {
						return false, err
					}
					continue
				}
				if viewStatus.Reason == typeddecode.ReasonHandleSourceUnsupported {
					heapValues, heapStatus := typeddecode.Int64View(s.resourceManager, handle, typeddecode.ResourceViewOptions{ExpectedElements: granule.Rows, RequireMapped: false})
					if heapStatus.Direct() {
						recordTypedColumnInt64DirectViewStatusWithHandle(&result.Diagnostics, heapStatus, handle)
						decodedAny = true
						if err := s.addTypedColumnInt64AggregateTypedViewValues(result, preparedColumn, block, granule, heapValues, visibility); err != nil {
							return false, err
						}
						continue
					}
					recordTypedColumnInt64FallbackReasonCounter(&result.Diagnostics, heapStatus.Reason)
					if !typedColumnInt64DirectViewFallbackAllowed(heapStatus) {
						return false, fmt.Errorf("collections: typed-column int64 aggregate heap typed view failed closed for column %q block %d: %s", preparedColumn.Plan.Definition.Name, block.Index, heapStatus.String())
					}
					streamingFallbackReason = heapStatus.Reason
				} else if !typedColumnInt64DirectViewFallbackAllowed(viewStatus) {
					return false, fmt.Errorf("collections: typed-column int64 aggregate direct view failed closed for column %q block %d: %s", preparedColumn.Plan.Definition.Name, block.Index, viewStatus.String())
				} else {
					streamingFallbackReason = viewStatus.Reason
				}
			} else if !typedColumnInt64DirectViewFallbackAllowed(blockStatus) {
				recordTypedColumnInt64DirectViewStatus(&result.Diagnostics, blockStatus)
				return false, fmt.Errorf("collections: typed-column int64 aggregate direct-view contract failed for column %q block %d: %s", preparedColumn.Plan.Definition.Name, block.Index, blockStatus.String())
			} else {
				recordTypedColumnInt64DirectViewStatus(&result.Diagnostics, blockStatus)
				streamingFallbackReason = blockStatus.Reason
			}
		}

		if err := typedColumnPreparedGranuleLayout(preparedColumn.Plan, granule).ValidateGranulePayload(granule, payload); err != nil {
			return false, err
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		if streamingFallbackReason == "" && columnPlan.Path == typeddecode.PathStreaming && columnPlan.Reason != typeddecode.ReasonVariableWidth {
			streamingFallbackReason = columnPlan.Reason
		}
		recordTypedColumnInt64ScratchDecode(&result.Diagnostics, streamingFallbackReason)
		if err := addTypedColumnInt64AggregateStreamingValues(result, typedColumnInt64PredicateAggregateScanRequest(s.req), s.req.Expression, granule, *block, preparedColumn.AggregateReducer, visibility, &s.aggregateScratch); err != nil {
			return false, err
		}
	}
	if payloadRead {
		result.Diagnostics.DirectTypedColumnAssetReads++
	}
	return !decodedAny && !statsAny && len(preparedColumn.BlockPlans) != 0, nil
}
