package collections

import (
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
)

type TypedColumnBoolPredicateScanKind string

const (
	TypedColumnBoolPredicateAll   TypedColumnBoolPredicateScanKind = "all"
	TypedColumnBoolPredicateEqual TypedColumnBoolPredicateScanKind = "equal"
	TypedColumnBoolPredicateRange TypedColumnBoolPredicateScanKind = "range"
)

type TypedColumnBoolPredicateAggregateRequest struct {
	Column                   string
	Kind                     TypedColumnBoolPredicateScanKind
	Value                    bool
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

type TypedColumnBoolPredicateAggregateResult struct {
	Rows        int64
	NonNulls    int64
	TrueCount   int64
	FalseCount  int64
	Diagnostics TypedColumnInt64PredicateScanDiagnostics
}

type TypedColumnBoolPredicateAggregateSession struct {
	view                columnPhysicalScanSnapshotView
	closeView           func()
	req                 TypedColumnBoolPredicateAggregateRequest
	fields              []TypedStorageField
	aggregateColumn     typedColumnAdapterColumn
	schemaHash          uint64
	refsByGeneration    map[uint64]columnManifestAssetRefForScan
	validatedRefs       map[ColumnAssetRef]struct{}
	preparedState       *typedColumnPreparedScanState
	aggregateScratch    typedColumnInt64PredicateAggregateScanScratch
	readCache           columnPhysicalAssetReadCache
	resourceManager     *mappedresource.Manager
	resolver            *typedColumnLatestRowResolver
	prepareDiagnostics  TypedColumnInt64PredicateScanDiagnostics
	beginForegroundRead func() func()
	closed              bool
}

type TypedColumnBoolPredicateAggregateSessionDiagnostics = TypedColumnInt64PredicateAggregateSessionDiagnostics

var errTypedColumnBoolPredicateAggregateSessionClosed = errors.New("collections: typed-column bool predicate aggregate session is closed")

func (c *Collection) RunTypedColumnBoolPredicateAggregate(req TypedColumnBoolPredicateAggregateRequest) (TypedColumnBoolPredicateAggregateResult, error) {
	if err := validateTypedColumnBoolPredicateAggregateRequest(req); err != nil {
		return TypedColumnBoolPredicateAggregateResult{}, err
	}
	start := time.Now()
	hintCfg, hintColumn, hintDeclared, hintErr := c.typedColumnBoolPredicateCatalogColumn(req.Column)
	if hintErr != nil {
		return TypedColumnBoolPredicateAggregateResult{}, hintErr
	}
	if hintDeclared && columnStoreColumnIsTypedColumnPart(hintColumn) && (hintColumn.ValueType != ColumnStoreValueBool || hintColumn.Nullable) {
		return TypedColumnBoolPredicateAggregateResult{}, typedColumnBoolPredicateUnsupportedColumnError(req.Column, hintColumn)
	}
	hintTypedColumnOwner := hintDeclared && hintColumn.ValueType == ColumnStoreValueBool && columnStoreColumnIsTypedColumnPart(hintColumn)
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		if hintTypedColumnOwner {
			return TypedColumnBoolPredicateAggregateResult{}, err
		}
		_ = hintCfg
		return TypedColumnBoolPredicateAggregateResult{}, fmt.Errorf("%w: typed-column bool predicate aggregate requires enabled typed_column_part column store", ErrColumnQueryPlanUnsupported)
	}
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column)
	if !ok {
		return TypedColumnBoolPredicateAggregateResult{Diagnostics: typedColumnInt64PredicateDiagnosticsFromView(view)}, fmt.Errorf("%w: typed-column bool predicate aggregate column %q is undeclared", ErrColumnQueryPlanUnsupported, req.Column)
	}
	if col.ValueType != ColumnStoreValueBool || col.Nullable {
		return TypedColumnBoolPredicateAggregateResult{Diagnostics: typedColumnInt64PredicateDiagnosticsFromView(view)}, typedColumnBoolPredicateUnsupportedColumnError(req.Column, col)
	}
	if !columnStoreColumnIsTypedColumnPart(col) {
		return TypedColumnBoolPredicateAggregateResult{Diagnostics: typedColumnInt64PredicateDiagnosticsFromView(view)}, fmt.Errorf("%w: typed-column bool predicate aggregate column %q is not owned by typed_column_part", ErrColumnQueryPlanUnsupported, req.Column)
	}
	session, diag, err := c.prepareTypedColumnBoolPredicateAggregateSessionFromView(view, nil, req)
	if err != nil {
		return TypedColumnBoolPredicateAggregateResult{Diagnostics: diag}, err
	}
	defer func() { _ = session.Close() }()
	includeDiagnostics := session.prepareDiagnostics
	includeDiagnostics.PruningBlocks = 0
	includeDiagnostics.PruningRows = 0
	includeDiagnostics.PruningFallbackBlocks = 0
	includeDiagnostics.PruningFallbackReason = ""
	return session.run(start, includeDiagnostics)
}

func (c *Collection) PrepareTypedColumnBoolPredicateAggregate(req TypedColumnBoolPredicateAggregateRequest) (*TypedColumnBoolPredicateAggregateSession, error) {
	if err := validateTypedColumnBoolPredicateAggregateRequest(req); err != nil {
		return nil, err
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if err != nil {
		if closeView != nil {
			closeView()
		}
		return nil, err
	}
	release := true
	defer func() {
		if release && closeView != nil {
			closeView()
		}
	}()
	session, _, err := c.prepareTypedColumnBoolPredicateAggregateSessionFromView(view, closeView, req)
	if err != nil {
		return nil, err
	}
	if session.view.snapshot != nil {
		session.view.snapshot.DetachForegroundRead()
		session.view.snapshot = nil
	}
	release = false
	return session, nil
}

func validateTypedColumnBoolPredicateAggregateRequest(req TypedColumnBoolPredicateAggregateRequest) error {
	if req.Column == "" {
		return errors.New("collections: typed-column bool predicate aggregate requires column")
	}
	switch req.Kind {
	case TypedColumnBoolPredicateAll, TypedColumnBoolPredicateEqual:
		return nil
	case TypedColumnBoolPredicateRange:
		return fmt.Errorf("%w: typed-column bool predicate aggregate does not support range predicates: %s", ErrColumnQueryPlanUnsupported, columnsemantics.ReasonBoolRangeUnsupported)
	default:
		return fmt.Errorf("%w: unsupported typed-column bool predicate kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
}

func typedColumnBoolPredicateSemanticOperation(kind TypedColumnBoolPredicateScanKind) columnsemantics.Operation {
	switch kind {
	case TypedColumnBoolPredicateAll:
		return columnsemantics.OpAllRows
	case TypedColumnBoolPredicateEqual:
		return columnsemantics.OpEquality
	case TypedColumnBoolPredicateRange:
		return columnsemantics.OpOrderedRange
	default:
		return columnsemantics.OpUnknownPredicateKind
	}
}

func (c *Collection) typedColumnBoolPredicateCatalogColumn(column string) (ColumnStoreConfig, ColumnStoreColumn, bool, error) {
	if c == nil {
		return ColumnStoreConfig{}, ColumnStoreColumn{}, false, errCollectionNil
	}
	if c.db == nil {
		return ColumnStoreConfig{}, ColumnStoreColumn{}, false, errCollectionDBNil
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	catalog := c.catalog
	if catalog == nil || catalog.meta.Options.ColumnStore == nil || !catalog.meta.Options.ColumnStore.Enabled {
		return ColumnStoreConfig{}, ColumnStoreColumn{}, false, nil
	}
	cfg := catalog.meta.Options.ColumnStore.copy()
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, column)
	return cfg, col, ok, nil
}

func typedColumnBoolPredicateUnsupportedColumnError(column string, col ColumnStoreColumn) error {
	return fmt.Errorf("%w: typed-column bool predicate aggregate column %q has type=%q nullable=%v", ErrColumnQueryPlanUnsupported, column, col.ValueType, col.Nullable)
}

func typedColumnAdapterHasBoolPredicateColumn(fields []TypedStorageField, column string) (bool, error) {
	adapterColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, column)
	if err != nil || !ok {
		return ok, err
	}
	if adapterColumn.Field.ValueType != ColumnStoreValueBool || adapterColumn.Field.Nullable || adapterColumn.Definition.Type != typedcolumn.ColumnTypeBool || adapterColumn.Definition.Encoding != typedcolumn.EncodingBoolBitpackRLE {
		return false, fmt.Errorf("%w: typed-column bool predicate aggregate column %q is not a non-null bool bitpack/RLE typed-column (type=%q nullable=%v physical=%s encoding=%s)", ErrColumnQueryPlanUnsupported, column, adapterColumn.Field.ValueType, adapterColumn.Field.Nullable, adapterColumn.Definition.Type, adapterColumn.Definition.Encoding)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpEquality, fmt.Sprintf("typed-column bool predicate aggregate column %q", column)); err != nil {
		return false, err
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, columnsemantics.OpBoolCounts, fmt.Sprintf("typed-column bool predicate aggregate column %q", column)); err != nil {
		return false, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, columnsemantics.OpEquality, fmt.Sprintf("typed-column bool predicate aggregate column %q", column)); err != nil {
		return false, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, columnsemantics.OpBoolCounts, fmt.Sprintf("typed-column bool predicate aggregate column %q", column)); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Collection) prepareTypedColumnBoolPredicateAggregateSessionFromView(view columnPhysicalScanSnapshotView, closeView func(), req TypedColumnBoolPredicateAggregateRequest) (*TypedColumnBoolPredicateAggregateSession, TypedColumnInt64PredicateScanDiagnostics, error) {
	diag := typedColumnInt64PredicateDiagnosticsFromView(view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	if ok, err := typedColumnAdapterHasBoolPredicateColumn(fields, req.Column); err != nil {
		return nil, diag, err
	} else if !ok {
		return nil, diag, fmt.Errorf("collections: typed-column bool predicate aggregate column %q is not owned by typed_column_part", req.Column)
	}
	aggregateColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, req.Column)
	if err != nil {
		return nil, diag, err
	}
	if !ok {
		return nil, diag, fmt.Errorf("collections: typed-column bool predicate aggregate column %q is not owned by typed_column_part", req.Column)
	}
	refsByGeneration, err := typedColumnInt64PredicateAggregateRefsByGeneration(view)
	if err != nil {
		return nil, diag, err
	}
	if len(refsByGeneration) == 0 {
		return nil, diag, errors.New("collections: missing typed_column_part assets for typed-column bool predicate aggregate")
	}
	if view.MutationParts == 0 {
		if _, err := validateTypedColumnPhysicalAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
			return nil, diag, typedColumnPhysicalAssetPairingAggregateError(err)
		}
	} else if err := validateTypedColumnMultipartAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		return nil, diag, err
	} else if typedColumnRefsHaveSortKey(refsByGeneration) {
		return nil, diag, typedColumnSortedMutationVisibilityUnsupported("typed-column bool predicate aggregate")
	}

	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return nil, diag, err
	}
	readCache.returnViews = true
	readCache.trustCachedVerifyFileIdentity = true
	scope := mappedresource.Scope{Kind: mappedresource.ScopePreparedQuery, ID: "typed-column-bool-predicate-aggregate", Namespace: view.AssetNamespace, Collection: view.CollectionName, Generation: view.Diagnostics.ManifestGeneration, Reason: "typed-column bool predicate aggregate session"}
	if err := readCache.useMappedResourceManager(mgr, scope, "typed-column bool predicate aggregate session"); err != nil {
		_ = readCache.close()
		return nil, diag, err
	}

	session := &TypedColumnBoolPredicateAggregateSession{view: view, closeView: closeView, req: req, fields: fields, aggregateColumn: aggregateColumn, schemaHash: cfg.SchemaHash, refsByGeneration: refsByGeneration, validatedRefs: make(map[ColumnAssetRef]struct{}, len(refsByGeneration)), readCache: readCache, resourceManager: mgr}
	session.beginForegroundRead = c.db.BeginForegroundRead
	if session.useTargetedAggregateRanges() {
		if err := session.prepareTargetedAggregateState(); err != nil {
			if session.preparedState != nil {
				session.preparedState.close()
			}
			_ = session.readCache.close()
			return nil, diag, err
		}
	}
	if view.MutationParts != 0 {
		beforeStats := mgr.Stats()
		beforeHits := session.readCache.hits
		beforeMisses := session.readCache.misses
		var resolverDiagnostics TypedColumnInt64PredicateScanDiagnostics
		resolver, err := buildTypedColumnLatestRowResolver(view, &session.readCache, &resolverDiagnostics)
		if err != nil {
			if session.preparedState != nil {
				session.preparedState.close()
			}
			_ = session.readCache.close()
			return nil, diag, err
		}
		afterStats := mgr.Stats()
		resolverDiagnostics.SegmentFileCacheHits = session.readCache.hits - beforeHits
		resolverDiagnostics.SegmentFileCacheMisses = session.readCache.misses - beforeMisses
		resolverDiagnostics.MappedBytes = afterStats.TotalMappedBytes - beforeStats.TotalMappedBytes
		resolverDiagnostics.HeapCopyBytes = afterStats.TotalHeapCopyBytes - beforeStats.TotalHeapCopyBytes
		resolverDiagnostics.FallbackReads = int(afterStats.FallbackReads - beforeStats.FallbackReads)
		addTypedColumnInt64PredicateAggregateDiagnostics(&session.prepareDiagnostics, resolverDiagnostics)
		session.resolver = resolver
	}
	return session, diag, nil
}

func (s *TypedColumnBoolPredicateAggregateSession) validatePreparedBoolAggregateColumn() error {
	if s == nil {
		return errors.New("collections: nil typed-column bool predicate aggregate session")
	}
	column := s.aggregateColumn
	if column.Field.ValueType != ColumnStoreValueBool || column.Field.Nullable || column.Definition.Type != typedcolumn.ColumnTypeBool || column.Definition.Encoding != typedcolumn.EncodingBoolBitpackRLE {
		return fmt.Errorf("%w: typed-column bool predicate aggregate column %q is not a non-null bool bitpack/RLE typed-column (type=%q nullable=%v physical=%s encoding=%s)", ErrColumnQueryPlanUnsupported, s.req.Column, column.Field.ValueType, column.Field.Nullable, column.Definition.Type, column.Definition.Encoding)
	}
	for _, op := range []columnsemantics.Operation{typedColumnBoolPredicateSemanticOperation(s.req.Kind), columnsemantics.OpBoolCounts} {
		if err := requireTypedColumnAdapterCapability(column, op, fmt.Sprintf("typed-column bool predicate aggregate column %q", s.req.Column)); err != nil {
			return err
		}
		if err := requireTypedColumnLayoutCapability(column, op, fmt.Sprintf("typed-column bool predicate aggregate column %q", s.req.Column)); err != nil {
			return err
		}
	}
	return nil
}

func (s *TypedColumnBoolPredicateAggregateSession) prepareTargetedAggregateState() error {
	if s == nil {
		return errors.New("collections: nil typed-column bool predicate aggregate session")
	}
	if err := s.validatePreparedBoolAggregateColumn(); err != nil {
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
		{Field: s.aggregateColumn.Field, Role: typedcolumn.ColumnRolePredicate, Operation: typedColumnBoolPredicateSemanticOperation(s.req.Kind)},
		{Field: s.aggregateColumn.Field, Role: typedcolumn.ColumnRoleMeasure, Operation: columnsemantics.OpBoolCounts, IncludeStats: true},
	}
	for _, physical := range s.view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef, ok := s.refsByGeneration[physical.Ref.Generation]
		if !ok {
			return fmt.Errorf("collections: typed-column bool predicate aggregate missing typed ref for physical generation=%d", physical.Ref.Generation)
		}
		if err := s.ensureCachedVerifyFullAssetValidated(typedRef.Ref, prepareResult, updateCacheDeltas); err != nil {
			return fmt.Errorf("collections: typed-column bool predicate aggregate validate generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		readRange := func(offset int, length int, section bool) ([]byte, error) {
			return s.readTypedColumnRange(typedRef.Ref, offset, length, section, prepareResult, updateCacheDeltas)
		}
		blockSelection := func(g typedcolumn.EncodedGranule, rows int) (typedcolumn.RowSelection, bool, error) {
			if s.req.Kind == TypedColumnBoolPredicateEqual {
				want := int64(0)
				if s.req.Value {
					want = 1
				}
				if g.HasMinMax && (want < g.Min || want > g.Max) {
					selection, err := typedcolumn.NewEmptyRowSelection(rows)
					return selection, false, err
				}
				selection, err := typedcolumn.NewAllRowSelection(rows)
				if err != nil {
					return typedcolumn.RowSelection{}, false, err
				}
				return selection, !(g.HasMinMax && g.Min == want && g.Max == want), nil
			}
			selection, err := typedcolumn.NewAllRowSelection(rows)
			return selection, false, err
		}
		part, partDiag, err := typedColumnPreparePartStateFromRanges(typedRef.Ref, physical.Ref, typedRef.Rows, physical.Rows, s.fields, s.schemaHash, requests, readRange, blockSelection)
		if err != nil {
			return fmt.Errorf("collections: typed-column bool predicate aggregate prepare generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if partDiag.Fallback {
			return fmt.Errorf("%w: %s", ErrColumnQueryPlanUnsupported, partDiag.FallbackReason)
		}
		if err := validateTypedColumnPreparedBoolAggregatePart(part, s.aggregateColumn, s.req); err != nil {
			return fmt.Errorf("collections: typed-column bool predicate aggregate prepare generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
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
	prepareResult.Diagnostics.PruningFallbackBlocks += state.diagnostics.PruningFallbackBlocks
	prepareResult.Diagnostics.PruningFallbackReason = state.diagnostics.PruningFallbackReason
	addTypedColumnInt64PredicateAggregateDiagnostics(&s.prepareDiagnostics, prepareResult.Diagnostics)
	s.preparedState = state
	return nil
}

func validateTypedColumnPreparedBoolAggregatePart(part *typedColumnPreparedPartState, adapterColumn typedColumnAdapterColumn, req TypedColumnBoolPredicateAggregateRequest) error {
	if part == nil {
		return errors.New("collections: typed-column bool aggregate prepared state is missing")
	}
	preparedColumn, ok := part.Columns[adapterColumn.Definition.Name]
	if !ok || preparedColumn == nil {
		return fmt.Errorf("collections: typed-column bool aggregate prepared state missing column %q", adapterColumn.Definition.Name)
	}
	if preparedColumn.Plan.Definition.Type != typedcolumn.ColumnTypeBool || preparedColumn.Plan.Definition.Encoding != typedcolumn.EncodingBoolBitpackRLE {
		return fmt.Errorf("collections: typed-column bool aggregate prepared column %q type=%s encoding=%s", adapterColumn.Definition.Name, preparedColumn.Plan.Definition.Type, preparedColumn.Plan.Definition.Encoding)
	}
	if cap := preparedColumn.Plan.Layout.SupportsSemanticOperation(columnsemantics.OpBoolCounts); !cap.Supported() {
		return fmt.Errorf("collections: typed-column bool aggregate prepared column %q layout capability %s", adapterColumn.Definition.Name, cap.Error())
	}
	reducer, err := typedColumnPreparedBoolAggregateReducer(preparedColumn)
	if err != nil {
		return err
	}
	preparedColumn.AggregateReducer = reducer
	preparedColumn.AggregateReducerReady = true
	preparedColumn.StatsFallbackReason = typedcolumn.ColumnStatsReasonUnsupportedPayload
	if req.Kind == TypedColumnBoolPredicateEqual {
		preparedColumn.PruningFallbackReason = string(columnsemantics.ReasonPruningPayloadUnsupported)
	}
	return nil
}

func typedColumnPreparedBoolAggregateReducer(preparedColumn *typedColumnPreparedColumnState) (typedkernel.PreparedReducer, error) {
	if preparedColumn == nil {
		return typedkernel.PreparedReducer{}, errors.New("collections: typed-column bool aggregate prepared reducer missing column state")
	}
	desc := columnsemantics.Descriptor{Logical: preparedColumn.Plan.Logical, Physical: preparedColumn.Plan.Definition.Type, Encoding: preparedColumn.Plan.Definition.Encoding, Nullable: preparedColumn.Plan.Field.Nullable, DictionaryOrder: preparedColumn.Plan.Layout.Descriptor.DictionaryOrder, DictionaryCollation: preparedColumn.Plan.Layout.Descriptor.DictionaryCollation}
	reducer, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: columnsemantics.OpBoolCounts, Semantic: desc, Layout: preparedColumn.Plan.Layout})
	if err != nil {
		return typedkernel.PreparedReducer{}, fmt.Errorf("collections: typed-column bool aggregate prepared column %q kernel dispatch: %w", preparedColumn.Plan.Definition.Name, err)
	}
	return reducer, nil
}

func (s *TypedColumnBoolPredicateAggregateSession) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.aggregateScratch = typedColumnInt64PredicateAggregateScanScratch{}
	if s.preparedState != nil {
		s.preparedState.close()
	}
	s.refsByGeneration = nil
	s.validatedRefs = nil
	s.resolver = nil
	s.beginForegroundRead = nil
	s.view = columnPhysicalScanSnapshotView{}
	var closeErr error
	if err := s.readCache.close(); err != nil {
		closeErr = err
	}
	if s.closeView != nil {
		s.closeView()
		s.closeView = nil
	}
	return closeErr
}

func (s *TypedColumnBoolPredicateAggregateSession) Diagnostics() TypedColumnBoolPredicateAggregateSessionDiagnostics {
	if s == nil {
		return TypedColumnBoolPredicateAggregateSessionDiagnostics{Closed: true}
	}
	stats := mappedresource.Stats{}
	if s.resourceManager != nil {
		stats = s.resourceManager.Stats()
	}
	return TypedColumnBoolPredicateAggregateSessionDiagnostics{Closed: s.closed, ColumnAssetReadIntegrity: columnAssetReadIntegrityLabel(s.req.ColumnAssetReadIntegrity), SegmentFileCacheHits: s.readCache.hits, SegmentFileCacheMisses: s.readCache.misses, ActiveResourceHandles: stats.ActiveHandles, ActiveMappedBytes: stats.ActiveMappedBytes, ActiveHeapCopyBytes: stats.ActiveHeapCopyBytes, TotalResourceAcquires: stats.TotalAcquires, TotalResourceReleases: stats.TotalReleases, TotalMappedBytes: stats.TotalMappedBytes, TotalHeapCopyBytes: stats.TotalHeapCopyBytes, FallbackReads: stats.FallbackReads}
}

func (s *TypedColumnBoolPredicateAggregateSession) Run() (TypedColumnBoolPredicateAggregateResult, error) {
	if s == nil || s.closed {
		return s.run(time.Now(), TypedColumnInt64PredicateScanDiagnostics{})
	}
	endForegroundRead := noCollectionForegroundReadEnd
	if s.beginForegroundRead != nil {
		endForegroundRead = s.beginForegroundRead()
	}
	defer endForegroundRead()
	return s.run(time.Now(), TypedColumnInt64PredicateScanDiagnostics{})
}

func (s *TypedColumnBoolPredicateAggregateSession) run(start time.Time, includeDiagnostics TypedColumnInt64PredicateScanDiagnostics) (TypedColumnBoolPredicateAggregateResult, error) {
	if s == nil || s.closed {
		return TypedColumnBoolPredicateAggregateResult{}, errTypedColumnBoolPredicateAggregateSessionClosed
	}
	diag := typedColumnInt64PredicateDiagnosticsFromView(s.view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(s.req.ColumnAssetReadIntegrity)
	addTypedColumnInt64PredicateAggregateDiagnostics(&diag, includeDiagnostics)
	result := TypedColumnBoolPredicateAggregateResult{Diagnostics: diag}
	beforeStats := mappedresource.Stats{}
	if s.resourceManager != nil {
		beforeStats = s.resourceManager.Stats()
	}
	beforeHits := s.readCache.hits
	beforeMisses := s.readCache.misses
	updateCacheDeltas := func() {
		result.Diagnostics.SegmentFileCacheHits = includeDiagnostics.SegmentFileCacheHits + s.readCache.hits - beforeHits
		result.Diagnostics.SegmentFileCacheMisses = includeDiagnostics.SegmentFileCacheMisses + s.readCache.misses - beforeMisses
	}
	var rawScratch []byte
	useTargetedRanges := s.useTargetedAggregateRanges()
	for _, physical := range s.view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef := s.refsByGeneration[physical.Ref.Generation]
		result.Diagnostics.PartsConsidered++
		var err error
		if useTargetedRanges {
			err = s.runTargetedAggregatePart(typedRef, physical, &result, updateCacheDeltas)
		} else {
			rawScratch, err = s.runFullAssetAggregatePart(typedRef, physical, rawScratch, &result, updateCacheDeltas)
		}
		if err != nil {
			return result, err
		}
	}
	if s.resourceManager != nil {
		afterStats := s.resourceManager.Stats()
		result.Diagnostics.FallbackReads += int(afterStats.FallbackReads - beforeStats.FallbackReads)
	}
	updateCacheDeltas()
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, nil
}

func (s *TypedColumnBoolPredicateAggregateSession) useTargetedAggregateRanges() bool {
	if s == nil {
		return false
	}
	switch s.req.ColumnAssetReadIntegrity {
	case ColumnAssetReadIntegrityCachedVerify, ColumnAssetReadIntegritySkipChecksums:
		return true
	default:
		return false
	}
}

func (s *TypedColumnBoolPredicateAggregateSession) runTargetedAggregatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, result *TypedColumnBoolPredicateAggregateResult, updateCacheDeltas func()) error {
	preparedPart, ok := typedColumnPreparedStatePart(s.preparedState, typedRef.Ref)
	if !ok || preparedPart == nil {
		return fmt.Errorf("collections: typed-column bool predicate aggregate missing prepared state generation=%d part_id=%d", typedRef.Ref.Generation, typedRef.Ref.PartID)
	}
	return s.scanPreparedAggregateStatePart(typedRef, physical, preparedPart, result, updateCacheDeltas)
}

func (s *TypedColumnBoolPredicateAggregateSession) runFullAssetAggregatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, rawScratch []byte, result *TypedColumnBoolPredicateAggregateResult, updateCacheDeltas func()) ([]byte, error) {
	raw, err := s.readCache.read(typedRef.Ref, rawScratch)
	updateCacheDeltas()
	if err != nil {
		return rawScratch, fmt.Errorf("collections: typed-column bool predicate aggregate read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
	}
	rawScratch = raw
	if s.readCache.lastView {
		result.Diagnostics.MappedBytes += uint64(len(raw))
	} else {
		result.Diagnostics.HeapCopyBytes += uint64(len(raw))
	}
	result.Diagnostics.DirectTypedColumnAssetReads++
	result.Diagnostics.FullAssetReads++
	result.Diagnostics.FullAssetBytes += uint64(len(raw))
	result.Diagnostics.PhysicalBytesScanned += int64(len(raw))
	adapterPart, err := typedColumnAdapterPartFromBytes(typedColumnAdapterOptions{Fields: s.fields, SchemaVersion: uint32(s.schemaHash)}, raw)
	if err != nil {
		return rawScratch, fmt.Errorf("collections: typed-column bool predicate aggregate decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
	}
	if adapterPart.Part.Descriptor.PartID != typedRef.Ref.PartID || adapterPart.Part.Descriptor.SchemaVersion != uint32(s.schemaHash) || adapterPart.Part.Descriptor.RowCount != typedRef.Rows || adapterPart.Part.Descriptor.RowCount != physical.Rows {
		return rawScratch, fmt.Errorf("collections: typed_column_part bool aggregate image/ref mismatch part_id=%d typed_ref_part_id=%d rows=%d typed_manifest_rows=%d physical_rows=%d schema=%d want %d", adapterPart.Part.Descriptor.PartID, typedRef.Ref.PartID, adapterPart.Part.Descriptor.RowCount, typedRef.Rows, physical.Rows, adapterPart.Part.Descriptor.SchemaVersion, uint32(s.schemaHash))
	}
	return rawScratch, s.scanFullAggregatePart(typedRef, physical, adapterPart.Part, result)
}

func (s *TypedColumnBoolPredicateAggregateSession) scanFullAggregatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, part *typedcolumn.ColumnPart, result *TypedColumnBoolPredicateAggregateResult) error {
	var visibility *typedColumnLatestPhysicalPart
	if s.resolver != nil {
		var ok bool
		visibility, ok = s.resolver.partForGeneration(physical.Ref.Generation)
		if !ok {
			return fmt.Errorf("collections: typed-column bool predicate aggregate missing latest-visible physical generation=%d", physical.Ref.Generation)
		}
	}
	if visibility != nil && typedColumnPartDescriptorHasLogicalSortKey(part.Descriptor) {
		return typedColumnSortedMutationVisibilityUnsupported("typed-column bool predicate aggregate")
	}
	partPruned, err := scanTypedColumnBoolPredicateAggregatePartWithVisibilityAndScratch(part, s.aggregateColumn.Definition.Name, s.req, result, visibility, &s.aggregateScratch)
	if err != nil {
		return fmt.Errorf("collections: typed-column bool predicate aggregate scan generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
	}
	if partPruned {
		result.Diagnostics.PartsPruned++
	} else {
		result.Diagnostics.PartsDecoded++
	}
	return nil
}

func (s *TypedColumnBoolPredicateAggregateSession) ensureCachedVerifyFullAssetValidated(ref ColumnAssetRef, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) error {
	if s.req.ColumnAssetReadIntegrity != ColumnAssetReadIntegrityCachedVerify {
		return nil
	}
	if _, ok := s.validatedRefs[ref]; ok {
		return nil
	}
	n, err := s.readCache.validateFullRef(ref)
	updateCacheDeltas()
	if err != nil {
		return err
	}
	s.validatedRefs[ref] = struct{}{}
	result.Diagnostics.FullAssetReads++
	result.Diagnostics.FullAssetBytes += uint64(n)
	result.Diagnostics.PhysicalBytesScanned += int64(n)
	return nil
}

func (s *TypedColumnBoolPredicateAggregateSession) readTypedColumnRange(ref ColumnAssetRef, offset int, length int, section bool, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) ([]byte, error) {
	raw, _, err := s.readTypedColumnRangeHandle(ref, offset, length, section, result, updateCacheDeltas)
	return raw, err
}

func (s *TypedColumnBoolPredicateAggregateSession) readTypedColumnRangeHandle(ref ColumnAssetRef, offset int, length int, section bool, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) ([]byte, *mappedresource.Handle, error) {
	if offset < 0 || length <= 0 {
		return nil, nil, fmt.Errorf("collections: typed-column range offset=%d length=%d is invalid", offset, length)
	}
	raw, handle, err := s.readCache.readRangeHandle(ref, int64(offset), int64(length))
	updateCacheDeltas()
	if err != nil {
		return nil, nil, err
	}
	if s.readCache.lastView {
		result.Diagnostics.MappedBytes += uint64(len(raw))
	} else {
		result.Diagnostics.HeapCopyBytes += uint64(len(raw))
	}
	if section {
		result.Diagnostics.SectionBytesRead += uint64(len(raw))
	} else {
		result.Diagnostics.RangeBytesRead += uint64(len(raw))
	}
	result.Diagnostics.PhysicalBytesScanned += int64(len(raw))
	return raw, handle, nil
}

func (s *TypedColumnBoolPredicateAggregateSession) readBoolTypedColumnRangeHandle(ref ColumnAssetRef, offset int, length int, section bool, result *TypedColumnBoolPredicateAggregateResult, updateCacheDeltas func()) ([]byte, *mappedresource.Handle, error) {
	if offset < 0 || length <= 0 {
		return nil, nil, fmt.Errorf("collections: typed-column range offset=%d length=%d is invalid", offset, length)
	}
	raw, handle, err := s.readCache.readRangeHandle(ref, int64(offset), int64(length))
	updateCacheDeltas()
	if err != nil {
		return nil, nil, err
	}
	if s.readCache.lastView {
		result.Diagnostics.MappedBytes += uint64(len(raw))
	} else {
		result.Diagnostics.HeapCopyBytes += uint64(len(raw))
	}
	if section {
		result.Diagnostics.SectionBytesRead += uint64(len(raw))
	} else {
		result.Diagnostics.RangeBytesRead += uint64(len(raw))
	}
	result.Diagnostics.PhysicalBytesScanned += int64(len(raw))
	return raw, handle, nil
}

func (s *TypedColumnBoolPredicateAggregateSession) scanPreparedAggregateStatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, preparedPart *typedColumnPreparedPartState, result *TypedColumnBoolPredicateAggregateResult, updateCacheDeltas func()) error {
	preparedColumn, ok := preparedPart.Columns[s.aggregateColumn.Definition.Name]
	if !ok || preparedColumn == nil {
		return fmt.Errorf("collections: typed-column bool predicate aggregate prepared state missing column %q", s.aggregateColumn.Definition.Name)
	}
	var visibility *typedColumnLatestPhysicalPart
	if s.resolver != nil {
		var ok bool
		visibility, ok = s.resolver.partForGeneration(physical.Ref.Generation)
		if !ok {
			return fmt.Errorf("collections: typed-column bool predicate aggregate missing latest-visible physical generation=%d", physical.Ref.Generation)
		}
	}
	if visibility != nil && typedColumnPreparedPartHasLogicalSortKey(preparedPart) {
		return typedColumnSortedMutationVisibilityUnsupported("typed-column bool predicate aggregate")
	}
	partPruned, err := s.scanPreparedAggregateColumnState(preparedColumn, typedRef.Ref, visibility, result, updateCacheDeltas)
	if err != nil {
		return fmt.Errorf("collections: typed-column bool predicate aggregate scan generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
	}
	if partPruned {
		result.Diagnostics.PartsPruned++
	} else {
		result.Diagnostics.PartsDecoded++
	}
	return nil
}

func (s *TypedColumnBoolPredicateAggregateSession) scanPreparedAggregateColumnState(preparedColumn *typedColumnPreparedColumnState, ref ColumnAssetRef, visibility *typedColumnLatestPhysicalPart, result *TypedColumnBoolPredicateAggregateResult, updateCacheDeltas func()) (bool, error) {
	if preparedColumn == nil {
		return false, errors.New("collections: typed-column bool predicate aggregate nil prepared column")
	}
	if !preparedColumn.AggregateReducerReady {
		return false, fmt.Errorf("collections: typed-column bool predicate aggregate prepared column %q missing kernel reducer", preparedColumn.Plan.Definition.Name)
	}
	decodedAny := false
	payloadRead := false
	result.Diagnostics.FastDecodeStreamingPlans++
	for blockIdx := range preparedColumn.BlockPlans {
		block := &preparedColumn.BlockPlans[blockIdx]
		result.Diagnostics.BlocksConsidered++
		if block.CandidateSelection.IsEmpty() {
			result.Diagnostics.BlocksPruned++
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, block.CandidateSelection)
			continue
		}
		if preparedColumn.StatsFallbackReason != "" {
			recordTypedColumnInt64StatsFallbackBlock(&result.Diagnostics, preparedColumn.StatsFallbackReason)
		}
		if preparedColumn.PruningFallbackReason != "" {
			recordTypedColumnInt64PruningFallbackBlock(&result.Diagnostics, preparedColumn.PruningFallbackReason)
		}
		payload, _, err := s.readBoolTypedColumnRangeHandle(ref, block.PayloadOffset, block.PayloadLength, false, result, updateCacheDeltas)
		if err != nil {
			return false, fmt.Errorf("read column %q block %d payload: %w", preparedColumn.Plan.Definition.Name, block.Index, err)
		}
		payloadRead = true
		if len(payload) != block.PayloadLength {
			return false, fmt.Errorf("typed-column bool aggregate column %q block %d payload bytes=%d want %d", preparedColumn.Plan.Definition.Name, block.Index, len(payload), block.PayloadLength)
		}
		granule := block.Granule
		granule.Payload = payload
		granule.PayloadRef = typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: block.PayloadLength}
		if err := typedColumnPreparedGranuleLayout(preparedColumn.Plan, granule).ValidateGranulePayload(granule, payload); err != nil {
			return false, err
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.RowsScanned += granule.Rows
		selection := block.CandidateSelection
		if block.NeedsPredicate {
			predicateSelection, err := s.aggregateScratch.reader.SelectBool(granule, selection, s.req.Value, &s.aggregateScratch.boolSelection)
			if err != nil {
				return false, err
			}
			selection = predicateSelection
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
		if err := addTypedColumnBoolAggregateKernelGranule(result, preparedColumn.AggregateReducer, &granule, &s.aggregateScratch.reader, selection, &s.aggregateScratch.kernel); err != nil {
			return false, err
		}
		recordTypedColumnInt64KernelBlock(&result.Diagnostics, !block.NeedsPredicate && visibility == nil && selection.IsAll(), false)
	}
	if payloadRead {
		result.Diagnostics.DirectTypedColumnAssetReads++
	}
	return !decodedAny && len(preparedColumn.BlockPlans) != 0, nil
}

func scanTypedColumnBoolPredicateAggregatePartWithVisibilityAndScratch(part *typedcolumn.ColumnPart, valueColumn string, req TypedColumnBoolPredicateAggregateRequest, result *TypedColumnBoolPredicateAggregateResult, visibility *typedColumnLatestPhysicalPart, scratch *typedColumnInt64PredicateAggregateScanScratch) (bool, error) {
	if part == nil {
		return false, errors.New("nil typed-column part")
	}
	valueCol, ok := part.Columns[valueColumn]
	if !ok {
		return false, fmt.Errorf("missing value column %q", valueColumn)
	}
	if valueCol.Definition.Type != typedcolumn.ColumnTypeBool || valueCol.Definition.Encoding != typedcolumn.EncodingBoolBitpackRLE {
		return false, fmt.Errorf("value column %q is not bool bitpack/RLE", valueColumn)
	}
	if scratch == nil {
		var localScratch typedColumnInt64PredicateAggregateScanScratch
		scratch = &localScratch
	}
	desc := columnsemantics.Descriptor{Logical: columnsemantics.LogicalBool, Physical: typedcolumn.ColumnTypeBool, Encoding: typedcolumn.EncodingBoolBitpackRLE}
	layout := typedColumnLayoutCapabilitiesForAdapterColumn(typedColumnAdapterColumn{Field: TypedStorageField{Name: valueColumn, Path: valueColumn, ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart}, Definition: valueCol.Definition})
	reducer, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: columnsemantics.OpBoolCounts, Semantic: desc, Layout: layout})
	if err != nil {
		return false, err
	}
	decodedAny := false
	for _, block := range valueCol.Blocks {
		result.Diagnostics.BlocksConsidered++
		g := block.Granule
		selection, needsPredicate, err := typedColumnBoolBlockSelection(req, g, block.Descriptor.RowCount)
		if err != nil {
			return false, err
		}
		if selection.IsEmpty() {
			result.Diagnostics.BlocksPruned++
			recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
			continue
		}
		decodedAny = true
		result.Diagnostics.BlocksDecoded++
		result.Diagnostics.DecodedHeapCopyBytes += uint64(g.RawBytes)
		result.Diagnostics.RowsScanned += g.Rows
		if needsPredicate {
			selection, err = scratch.reader.SelectBool(g, selection, req.Value, &scratch.boolSelection)
			if err != nil {
				return false, err
			}
		}
		if visibility != nil && !selection.IsEmpty() {
			visibilitySelection, err := typedColumnInt64VisibilitySelectionForBlock(visibility, block.Descriptor.FirstRow, block.Descriptor.RowCount, scratch)
			if err != nil {
				return false, err
			}
			selection, err = typedcolumn.ComposeRowSelectionsInto(block.Descriptor.RowCount, typedcolumn.RowSelectionComponents{Predicate: &selection, Visibility: &visibilitySelection}, &scratch.selection)
			if err != nil {
				return false, err
			}
			result.Diagnostics.SelectionCompositions++
		}
		recordTypedColumnSelectionDiagnostics(&result.Diagnostics, selection)
		if selection.IsEmpty() {
			continue
		}
		if err := addTypedColumnBoolAggregateKernelGranule(result, reducer, &g, &scratch.reader, selection, &scratch.kernel); err != nil {
			return false, err
		}
		recordTypedColumnInt64KernelBlock(&result.Diagnostics, !needsPredicate && visibility == nil && selection.IsAll(), false)
	}
	return !decodedAny && len(valueCol.Blocks) != 0, nil
}

func typedColumnBoolBlockSelection(req TypedColumnBoolPredicateAggregateRequest, g typedcolumn.EncodedGranule, rows int) (typedcolumn.RowSelection, bool, error) {
	if req.Kind == TypedColumnBoolPredicateEqual {
		want := int64(0)
		if req.Value {
			want = 1
		}
		if g.HasMinMax && (want < g.Min || want > g.Max) {
			selection, err := typedcolumn.NewEmptyRowSelection(rows)
			return selection, false, err
		}
		selection, err := typedcolumn.NewAllRowSelection(rows)
		if err != nil {
			return typedcolumn.RowSelection{}, false, err
		}
		return selection, !(g.HasMinMax && g.Min == want && g.Max == want), nil
	}
	selection, err := typedcolumn.NewAllRowSelection(rows)
	return selection, false, err
}

func addTypedColumnBoolAggregateKernelGranule(result *TypedColumnBoolPredicateAggregateResult, reducer typedkernel.PreparedReducer, granule *typedcolumn.EncodedGranule, reader *typedcolumn.GranuleReader, selection typedcolumn.RowSelection, scratch *typedkernel.Scratch) error {
	out, err := reducer.Reduce(typedkernel.ReduceRequest{Rows: granule.Rows, Selection: selection, BoolGranule: *granule, HasBoolGranule: true, BoolReader: reader}, scratch)
	if err != nil {
		return err
	}
	return addTypedColumnBoolAggregateKernelResult(result, out)
}

func addTypedColumnBoolAggregateKernelResult(result *TypedColumnBoolPredicateAggregateResult, out typedkernel.AggregateResult) error {
	if result == nil {
		return errors.New("collections: nil typed-column bool aggregate result")
	}
	if out.Rows < 0 || out.NonNulls < 0 || out.TrueCount < 0 || out.FalseCount < 0 {
		return fmt.Errorf("collections: typed-column bool aggregate invalid negative kernel counts: %+v", out)
	}
	if out.TrueCount+out.FalseCount != out.NonNulls || out.Rows != out.NonNulls {
		return fmt.Errorf("collections: typed-column bool aggregate inconsistent kernel counts: %+v", out)
	}
	result.Rows += out.Rows
	result.NonNulls += out.NonNulls
	result.TrueCount += out.TrueCount
	result.FalseCount += out.FalseCount
	result.Diagnostics.RowsMatched += int(out.Rows)
	return nil
}
