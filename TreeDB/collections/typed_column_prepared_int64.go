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
	prepareResult.Diagnostics.DecodedMetadataBytes += state.diagnostics.ManifestBytes + state.diagnostics.DescriptorBytes + state.diagnostics.ContractBytes
	prepareResult.Diagnostics.DirectViewCertified += state.diagnostics.DirectViewCertified
	prepareResult.Diagnostics.StreamingCertified += state.diagnostics.StreamingCertified
	prepareResult.Diagnostics.StatsCertified += state.diagnostics.StatsCertified
	prepareResult.Diagnostics.PruningCertified += state.diagnostics.PruningCertified
	prepareResult.Diagnostics.CertificationFailures += state.diagnostics.CertificationFailures
	prepareResult.Diagnostics.CertificationFailureReason = state.diagnostics.CertificationFailureReason
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
	if preparedColumn.Plan.Layout.Reducers.Int64FixedWidthRaw && !preparedColumn.Certification.DirectViewCertified {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q lacks certified fixed-width direct-view contract", adapterColumn.Definition.Name)
	}
	if preparedColumn.Plan.Layout.Reducers.Int64Streaming && !preparedColumn.Certification.StreamingCertified {
		return fmt.Errorf("collections: typed-column int64 aggregate prepared column %q lacks certified streaming contract", adapterColumn.Definition.Name)
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

func recordTypedColumnInt64FastDecodePlan(diag *TypedColumnInt64PredicateScanDiagnostics, plan typeddecode.Plan) {
	if diag == nil {
		return
	}
	switch plan.Path {
	case typeddecode.PathDirectView:
		diag.FastDecodeDirectViewPlans++
	case typeddecode.PathStreaming:
		diag.FastDecodeStreamingPlans++
	case typeddecode.PathMaterialize:
		diag.FastDecodeMaterializePlans++
	case typeddecode.PathUnsupported:
		diag.FastDecodeUnsupportedPlans++
	}
	if plan.Reason != "" && plan.Reason != typeddecode.ReasonSupported {
		diag.FastDecodeFallbackReason = string(plan.Reason)
	}
}

func recordTypedColumnInt64DirectViewStatus(diag *TypedColumnInt64PredicateScanDiagnostics, status typeddecode.Status) {
	if diag == nil {
		return
	}
	if status.Direct() {
		diag.DirectViewSuccesses++
		return
	}
	diag.DirectViewFailures++
	if status.Reason != "" && status.Reason != typeddecode.ReasonSupported {
		diag.FastDecodeFallbackReason = string(status.Reason)
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
	case typeddecode.ReasonUnaligned, typeddecode.ReasonHandleSourceUnsupported, typeddecode.ReasonWrongEndian, typeddecode.ReasonNotWriterCertified:
		return true
	default:
		return false
	}
}

func addTypedColumnInt64AggregateStreamingValues(result *TypedColumnInt64PredicateAggregateResult, req TypedColumnInt64PredicateScanRequest, granule typedcolumn.EncodedGranule, block typedColumnPreparedBlockPlan, visibility *typedColumnLatestPhysicalPart, scratch *typedColumnInt64PredicateAggregateScanScratch) error {
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
	if selection.IsAll() && !block.NeedsPredicate && visibility == nil {
		count, sum, err := scratch.reader.CountSumInt64(granule)
		if err != nil {
			return err
		}
		if count != rows {
			return fmt.Errorf("typed-column int64 streaming count=%d want rows=%d", count, rows)
		}
		if sum > 0 && result.Sum > typedColumnInt64PredicateAggregateMaxSum-sum {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", result.Sum, sum)
		}
		if sum < 0 && result.Sum < typedColumnInt64PredicateAggregateMinSum-sum {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", result.Sum, sum)
		}
		result.Count += int64(count)
		result.Sum += sum
		result.Diagnostics.RowsMatched += count
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		return nil
	}
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
		if err := addTypedColumnInt64PredicateAggregateValue(result, value); err != nil {
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

func (s *TypedColumnInt64PredicateAggregateSession) scanPreparedAggregateColumnState(preparedColumn *typedColumnPreparedColumnState, ref ColumnAssetRef, visibility *typedColumnLatestPhysicalPart, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) (bool, error) {
	if preparedColumn == nil {
		return false, errors.New("collections: typed-column int64 predicate aggregate nil prepared column")
	}
	decodedAny := false
	payloadRead := false
	columnPlan := typeddecode.Int64ReducerPlan(preparedColumn.Plan.Layout, preparedColumn.Certification)
	recordTypedColumnInt64FastDecodePlan(&result.Diagnostics, columnPlan)
	var err error
	for _, block := range preparedColumn.BlockPlans {
		result.Diagnostics.BlocksConsidered++
		if block.CandidateSelection.IsEmpty() {
			result.Diagnostics.BlocksPruned++
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, block.CandidateSelection)
			continue
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
		if columnPlan.DirectCandidate() {
			if block.Index < 0 || block.Index >= len(preparedColumn.Certification.Blocks) {
				return false, fmt.Errorf("collections: typed-column int64 aggregate direct-view block index=%d outside certification blocks=%d", block.Index, len(preparedColumn.Certification.Blocks))
			}
			blockStatus := typeddecode.ValidateDirectViewBlock(typeddecode.DirectViewBlockRequest{Plan: columnPlan, Certification: preparedColumn.Certification, Block: preparedColumn.Certification.Blocks[block.Index], Rows: granule.Rows, PayloadBytes: len(payload)})
			if blockStatus.Direct() {
				values, viewStatus := typeddecode.Int64View(s.resourceManager, handle, typeddecode.ResourceViewOptions{ExpectedElements: granule.Rows, RequireMapped: true})
				recordTypedColumnInt64DirectViewStatus(&result.Diagnostics, viewStatus)
				if viewStatus.Direct() {
					decodedAny = true
					result.Diagnostics.BlocksDecoded++
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
					continue
				}
				if !typedColumnInt64DirectViewFallbackAllowed(viewStatus) {
					return false, fmt.Errorf("collections: typed-column int64 aggregate direct view failed closed for column %q block %d: %s", preparedColumn.Plan.Definition.Name, block.Index, viewStatus.String())
				}
			} else if !typedColumnInt64DirectViewFallbackAllowed(blockStatus) {
				recordTypedColumnInt64DirectViewStatus(&result.Diagnostics, blockStatus)
				return false, fmt.Errorf("collections: typed-column int64 aggregate direct-view contract failed for column %q block %d: %s", preparedColumn.Plan.Definition.Name, block.Index, blockStatus.String())
			} else {
				recordTypedColumnInt64DirectViewStatus(&result.Diagnostics, blockStatus)
			}
		}

		if err := preparedColumn.Plan.Layout.ValidateGranulePayload(granule, payload); err != nil {
			return false, err
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		if err := addTypedColumnInt64AggregateStreamingValues(result, typedColumnInt64PredicateAggregateScanRequest(s.req), granule, block, visibility, &s.aggregateScratch); err != nil {
			return false, err
		}
	}
	if payloadRead {
		result.Diagnostics.DirectTypedColumnAssetReads++
	}
	return !decodedAny && len(preparedColumn.BlockPlans) != 0, nil
}
