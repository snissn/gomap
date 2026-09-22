package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type TypedColumnInt64PredicateScanKind string

const (
	TypedColumnInt64PredicateAll   TypedColumnInt64PredicateScanKind = "all"
	TypedColumnInt64PredicateEqual TypedColumnInt64PredicateScanKind = "equal"
	TypedColumnInt64PredicateRange TypedColumnInt64PredicateScanKind = "range"
)

type TypedColumnInt64AggregateExpression string

const (
	TypedColumnInt64AggregateIdentity          TypedColumnInt64AggregateExpression = "identity"
	TypedColumnInt64AggregateSecondOfDaySquare TypedColumnInt64AggregateExpression = "second_of_day_square"
)

const (
	typedColumnInt64AggregateSecondUS  = int64(1_000_000)
	typedColumnInt64AggregateDaySecond = int64(86_400)
)

type TypedColumnInt64PredicateScanRequest struct {
	Column                   string
	Kind                     TypedColumnInt64PredicateScanKind
	Value                    int64
	Low                      int64
	High                     int64
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

type TypedColumnInt64PredicateAggregateRequest struct {
	Column                   string
	Kind                     TypedColumnInt64PredicateScanKind
	Value                    int64
	Low                      int64
	High                     int64
	Expression               TypedColumnInt64AggregateExpression
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

type TypedColumnInt64PredicateScanRow struct {
	Generation uint64
	PartID     uint64
	RowIndex   int
	PrimaryID  int64
	DocumentID []byte
	Value      int64
}

type TypedColumnInt64PredicateScanDiagnostics struct {
	ManifestRoot               uint64
	ManifestGeneration         uint64
	RecoveryManifestGeneration uint64
	AppliedCommandLSN          uint64
	ManifestRecords            int
	AssetRefs                  int
	MutationParts              int

	RowsScanned int
	RowsMatched int

	PartsConsidered  int
	PartsPruned      int
	PartsDecoded     int
	BlocksConsidered int
	BlocksPruned     int
	BlocksDecoded    int

	SelectionEmptyBlocks  int
	SelectionAllBlocks    int
	SelectionRangeBlocks  int
	SelectionRangesBlocks int
	SelectionBitmapBlocks int
	SelectionSparseBlocks int
	SelectionCompositions int

	DirectTypedColumnAssetReads    int
	FullAssetReads                 int
	FallbackReads                  int
	CodesMatched                   int
	DictionaryBytesDecoded         uint64
	FullAssetBytes                 uint64
	SectionBytesRead               uint64
	RangeBytesRead                 uint64
	MappedBytes                    uint64
	HeapCopyBytes                  uint64
	DecodedMetadataBytes           uint64
	DecodedHeapCopyBytes           uint64
	MaterializedBytes              uint64
	FastDecodeDirectViewPlans      int
	FastDecodeStreamingPlans       int
	FastDecodeMaterializePlans     int
	FastDecodeUnsupportedPlans     int
	FastDecodeMmapDirectViews      int
	FastDecodeHeapCopyTypedViews   int
	FastDecodeScratchDecodes       int
	FastDecodeStreamingFallbacks   int
	FastDecodeCertificationFailure int
	FastDecodeAbsoluteUnaligned    int
	FastDecodeActualUnaligned      int
	FastDecodeStaleHandles         int
	DirectViewSuccesses            int
	DirectViewFailures             int
	KernelBlocks                   int
	KernelFullCoveredBlocks        int
	KernelSelectedBlocks           int
	KernelCursorBlocks             int
	KernelFallbackBlocks           int
	StatsBlocks                    int
	StatsFullCoveredBlocks         int
	StatsFallbackBlocks            int
	StatsRows                      int
	PruningBlocks                  int
	PruningRows                    int
	PruningFallbackBlocks          int
	PruningFallbackReason          string
	StatsFallbackReason            string
	StatsValidationFailures        int
	StatsValidationFailureReason   string
	PruningValidationFailures      int
	PruningValidationFailureReason string
	FastDecodeFallbackReason       string
	DirectViewCertified            int
	StreamingCertified             int
	StatsCertified                 int
	PruningCertified               int
	CertificationFailures          int
	CertificationFailureReason     string
	PhysicalBytesScanned           int64
	RowLocatorDecodes              int
	PhysicalRowIDLookups           int
	PhysicalRowAssetReads          int
	RowMaterializations            int
	DocumentMaterializations       int
	DocumentReconstructions        int
	SegmentFileCacheHits           uint64
	SegmentFileCacheMisses         uint64
	ColumnAssetReadIntegrity       string
	Fallback                       bool
	FallbackReason                 string
	ScanNanos                      int64
}

type TypedColumnInt64PredicateScanResult struct {
	Rows        []TypedColumnInt64PredicateScanRow
	Diagnostics TypedColumnInt64PredicateScanDiagnostics
}

type TypedColumnInt64PredicateAggregateResult struct {
	Count       int64
	Sum         int64
	Avg         float64
	Diagnostics TypedColumnInt64PredicateScanDiagnostics
}

// TypedColumnInt64PredicateAggregateSession owns an explicit prepared lifetime
// for repeated typed-column int64 predicate aggregate scans over one immutable
// snapshot. It keeps the column physical asset read cache and mappedresource
// handles alive between Run calls, and releases them on Close.
//
// A session is not safe for concurrent Run and Close calls; callers that share a
// session between goroutines must provide external synchronization.
type TypedColumnInt64PredicateAggregateSession struct {
	view                columnPhysicalScanSnapshotView
	closeView           func()
	req                 TypedColumnInt64PredicateAggregateRequest
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

// TypedColumnInt64PredicateAggregateSessionDiagnostics reports scoped resource
// state for a prepared aggregate session without exposing internal resource
// manager types in the public API.
type TypedColumnInt64PredicateAggregateSessionDiagnostics struct {
	Closed                   bool
	ColumnAssetReadIntegrity string
	SegmentFileCacheHits     uint64
	SegmentFileCacheMisses   uint64
	ActiveResourceHandles    int64
	ActiveMappedBytes        int64
	ActiveHeapCopyBytes      int64
	TotalResourceAcquires    uint64
	TotalResourceReleases    uint64
	TotalMappedBytes         uint64
	TotalHeapCopyBytes       uint64
	FallbackReads            uint64
}

var errTypedColumnInt64PredicateAggregateSessionClosed = errors.New("collections: typed-column int64 predicate aggregate session is closed")

const (
	typedColumnInt64PredicateAggregateMaxSum           = int64(1<<63 - 1)
	typedColumnInt64PredicateAggregateMinSum           = -typedColumnInt64PredicateAggregateMaxSum - 1
	typedColumnInt64AggregateSecondOfDaySquareMaxValue = (typedColumnInt64AggregateDaySecond - 1) * (typedColumnInt64AggregateDaySecond - 1)
)

func addTypedColumnInt64PredicateAggregateValue(result *TypedColumnInt64PredicateAggregateResult, value int64) error {
	if result == nil {
		return errors.New("collections: nil typed-column int64 predicate aggregate result")
	}
	if value > 0 && result.Sum > typedColumnInt64PredicateAggregateMaxSum-value {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", result.Sum, value)
	}
	if value < 0 && result.Sum < typedColumnInt64PredicateAggregateMinSum-value {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate sum overflow current=%d value=%d", result.Sum, value)
	}
	result.Count++
	result.Sum += value
	return nil
}

func addTypedColumnInt64PredicateAggregateExpressionValue(result *TypedColumnInt64PredicateAggregateResult, expression TypedColumnInt64AggregateExpression, value int64) error {
	if expression == TypedColumnInt64AggregateSecondOfDaySquare {
		return addTypedColumnInt64PredicateAggregateSecondOfDaySquareValue(result, value)
	}
	transformed, err := typedColumnInt64AggregateExpressionValue(expression, value)
	if err != nil {
		return err
	}
	return addTypedColumnInt64PredicateAggregateValue(result, transformed)
}

func addTypedColumnInt64PredicateAggregateSecondOfDaySquareValue(result *TypedColumnInt64PredicateAggregateResult, value int64) error {
	return addTypedColumnInt64PredicateAggregateValue(result, typedColumnInt64AggregateSecondOfDaySquareValue(value))
}

// RunTypedColumnInt64PredicateScan executes the scoped #1757 scalar predicate MVP.
// When the requested int64 field is owned by typed_column_part, the predicate is
// evaluated directly over durable typed_column_part assets and fails closed if
// those assets are unavailable or inconsistent. Non typed-column ownership keeps
// the existing typed-row/document fallback behavior and is identified in
// Diagnostics.Fallback.
func (c *Collection) RunTypedColumnInt64PredicateScan(req TypedColumnInt64PredicateScanRequest) (TypedColumnInt64PredicateScanResult, error) {
	if err := validateTypedColumnInt64PredicateScanRequest(req); err != nil {
		return TypedColumnInt64PredicateScanResult{}, err
	}
	start := time.Now()
	hintCfg, hintColumn, hintDeclared, hintErr := c.typedColumnInt64PredicateCatalogColumn(req.Column)
	if hintErr != nil {
		return TypedColumnInt64PredicateScanResult{}, hintErr
	}
	if hintDeclared && columnStoreColumnIsTypedColumnPart(hintColumn) && (hintColumn.ValueType != ColumnStoreValueInt64 || hintColumn.Nullable) {
		return TypedColumnInt64PredicateScanResult{}, typedColumnInt64PredicateUnsupportedColumnError(req.Column, hintColumn)
	}
	hintTypedColumnOwner := hintDeclared && hintColumn.ValueType == ColumnStoreValueInt64 && columnStoreColumnIsTypedColumnPart(hintColumn)
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		if hintTypedColumnOwner {
			return TypedColumnInt64PredicateScanResult{}, err
		}
		return c.runTypedColumnInt64PredicateScanDocumentFallback(req, hintCfg, "column_store_unavailable", start)
	}
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column)
	if !ok {
		return c.runTypedColumnInt64PredicateScanDocumentFallback(req, cfg, "undeclared_column", start)
	}
	if col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return TypedColumnInt64PredicateScanResult{}, typedColumnInt64PredicateUnsupportedColumnError(req.Column, col)
	}
	if !columnStoreColumnIsTypedColumnPart(col) {
		if view.MutationParts != 0 {
			return c.runTypedColumnInt64PredicateScanDocumentFallback(req, cfg, "mutation_visibility_requires_document_reconstruction", start)
		}
		return c.runTypedColumnInt64PredicateScanPhysicalFallback(view, req, cfg, "typed_column_not_selected", start)
	}
	return c.runTypedColumnInt64PredicateScanDirect(view, req, cfg, start)
}

func typedColumnInt64PredicateUnsupportedColumnError(column string, col ColumnStoreColumn) error {
	return fmt.Errorf("%w: typed-column int64 predicate column %q has type=%q nullable=%v", ErrColumnQueryPlanUnsupported, column, col.ValueType, col.Nullable)
}

// RunTypedColumnInt64PredicateAggregate executes a narrow count/sum/avg path for
// int64 predicates. When the requested int64 field is owned by typed_column_part,
// the aggregate is evaluated directly over durable typed_column_part assets. The
// direct path does not decode row locators, scan physical row assets, materialize
// result rows, or reconstruct documents. If no usable typed-column store is
// available for the field, the method falls back to a full document scan and marks
// Diagnostics.Fallback/FallbackReason.
func (c *Collection) RunTypedColumnInt64PredicateAggregate(req TypedColumnInt64PredicateAggregateRequest) (TypedColumnInt64PredicateAggregateResult, error) {
	if err := validateTypedColumnInt64PredicateAggregateRequest(req); err != nil {
		return TypedColumnInt64PredicateAggregateResult{}, err
	}
	start := time.Now()
	hintCfg, hintColumn, hintDeclared, hintErr := c.typedColumnInt64PredicateCatalogColumn(req.Column)
	if hintErr != nil {
		return TypedColumnInt64PredicateAggregateResult{}, hintErr
	}
	if hintDeclared && columnStoreColumnIsTypedColumnPart(hintColumn) && (hintColumn.ValueType != ColumnStoreValueInt64 || hintColumn.Nullable) {
		return TypedColumnInt64PredicateAggregateResult{}, typedColumnInt64PredicateUnsupportedColumnError(req.Column, hintColumn)
	}
	hintTypedColumnOwner := hintDeclared && hintColumn.ValueType == ColumnStoreValueInt64 && columnStoreColumnIsTypedColumnPart(hintColumn)
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		if hintTypedColumnOwner {
			return TypedColumnInt64PredicateAggregateResult{}, err
		}
		return c.runTypedColumnInt64PredicateAggregateDocumentFallback(req, hintCfg, "column_store_unavailable", start)
	}
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column)
	if !ok {
		return c.runTypedColumnInt64PredicateAggregateDocumentFallback(req, cfg, "undeclared_column", start)
	}
	if col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return TypedColumnInt64PredicateAggregateResult{}, typedColumnInt64PredicateUnsupportedColumnError(req.Column, col)
	}
	if !columnStoreColumnIsTypedColumnPart(col) {
		return c.runTypedColumnInt64PredicateAggregateDocumentFallback(req, cfg, "typed_column_not_selected", start)
	}
	return c.runTypedColumnInt64PredicateAggregateDirect(view, req, cfg, start)
}

// PrepareTypedColumnInt64PredicateAggregate prepares a scoped typed-column int64
// predicate aggregate session over the current recovery-authoritative snapshot.
// The prepared path is intentionally direct typed-column only: unsupported
// columns or unavailable typed-column assets fail closed instead of falling back
// to document scans. In cached_verify mode, the session reuses the file identity
// captured when a segment reader is opened; use verify mode for per-run checksum
// validation during a long-lived session. Call Close when finished to release
// mapped assets and the pinned snapshot. Cached-verify prepared sessions
// validate each immutable typed-column asset ref once before using targeted
// section/range reads on later hot scans; verify mode keeps the full-asset
// validation path per Run, and skip-checksums remains an unsafe benchmark ceiling.
func (c *Collection) PrepareTypedColumnInt64PredicateAggregate(req TypedColumnInt64PredicateAggregateRequest) (*TypedColumnInt64PredicateAggregateSession, error) {
	if err := validateTypedColumnInt64PredicateAggregateRequest(req); err != nil {
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
	session, _, err := c.prepareTypedColumnInt64PredicateAggregateSessionFromView(view, closeView, req)
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

func validateTypedColumnInt64PredicateAggregateRequest(req TypedColumnInt64PredicateAggregateRequest) error {
	if err := validateTypedColumnInt64PredicateScanRequest(TypedColumnInt64PredicateScanRequest{
		Column: req.Column,
		Kind:   req.Kind,
		Value:  req.Value,
		Low:    req.Low,
		High:   req.High,
	}); err != nil {
		return err
	}
	_, err := normalizeTypedColumnInt64AggregateExpression(req.Expression)
	return err
}

func typedColumnInt64PredicateAggregateScanRequest(req TypedColumnInt64PredicateAggregateRequest) TypedColumnInt64PredicateScanRequest {
	return TypedColumnInt64PredicateScanRequest{
		Column:                   req.Column,
		Kind:                     req.Kind,
		Value:                    req.Value,
		Low:                      req.Low,
		High:                     req.High,
		ColumnAssetReadIntegrity: req.ColumnAssetReadIntegrity,
	}
}

func normalizeTypedColumnInt64AggregateExpression(expression TypedColumnInt64AggregateExpression) (TypedColumnInt64AggregateExpression, error) {
	switch expression {
	case "", TypedColumnInt64AggregateIdentity:
		return TypedColumnInt64AggregateIdentity, nil
	case TypedColumnInt64AggregateSecondOfDaySquare:
		return expression, nil
	default:
		return "", fmt.Errorf("%w: unsupported typed-column int64 aggregate expression %q", ErrColumnQueryPlanUnsupported, expression)
	}
}

func typedColumnInt64AggregateExpressionIsIdentity(expression TypedColumnInt64AggregateExpression) bool {
	normalized, err := normalizeTypedColumnInt64AggregateExpression(expression)
	return err == nil && normalized == TypedColumnInt64AggregateIdentity
}

func typedColumnInt64AggregateExpressionValue(expression TypedColumnInt64AggregateExpression, value int64) (int64, error) {
	normalized, err := normalizeTypedColumnInt64AggregateExpression(expression)
	if err != nil {
		return 0, err
	}
	switch normalized {
	case TypedColumnInt64AggregateIdentity:
		return value, nil
	case TypedColumnInt64AggregateSecondOfDaySquare:
		return typedColumnInt64AggregateSecondOfDaySquareValue(value), nil
	default:
		return 0, fmt.Errorf("%w: unsupported typed-column int64 aggregate expression %q", ErrColumnQueryPlanUnsupported, expression)
	}
}

func typedColumnInt64AggregateSecondOfDaySquareValue(value int64) int64 {
	seconds := typedColumnInt64AggregateFloorUnixSeconds(value)
	secondOfDay := seconds % typedColumnInt64AggregateDaySecond
	if secondOfDay < 0 {
		secondOfDay += typedColumnInt64AggregateDaySecond
	}
	return secondOfDay * secondOfDay
}

func typedColumnInt64AggregateFloorUnixSeconds(timeUS int64) int64 {
	seconds := timeUS / typedColumnInt64AggregateSecondUS
	if timeUS < 0 && timeUS%typedColumnInt64AggregateSecondUS != 0 {
		seconds--
	}
	return seconds
}

func validateTypedColumnInt64PredicateScanRequest(req TypedColumnInt64PredicateScanRequest) error {
	if req.Column == "" {
		return errors.New("collections: typed-column int64 predicate scan requires column")
	}
	switch req.Kind {
	case TypedColumnInt64PredicateAll:
	case TypedColumnInt64PredicateEqual:
	case TypedColumnInt64PredicateRange:
		if req.Low > req.High {
			return errors.New("collections: typed-column int64 predicate range low is greater than high")
		}
	default:
		return fmt.Errorf("%w: unsupported typed-column int64 predicate kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
	return nil
}

type typedColumnPhysicalAssetPairingReasonError struct {
	reason ColumnPublishOperation
}

func (e typedColumnPhysicalAssetPairingReasonError) Error() string {
	return fmt.Sprintf("collections: typed-column physical asset pairing requires insert-only physical refs, got %s", e.reason)
}

func validateTypedColumnPhysicalAssetPairing(refsByGeneration map[uint64]columnManifestAssetRefForScan, assetRefs []columnManifestAssetRefForScan) (map[uint64]struct{}, error) {
	physicalRefsByGeneration := make(map[uint64]struct{}, len(assetRefs))
	for _, physical := range assetRefs {
		if physical.Reason != ColumnPublishOperationInsert {
			return nil, typedColumnPhysicalAssetPairingReasonError{reason: physical.Reason}
		}
		if _, ok := refsByGeneration[physical.Ref.Generation]; !ok {
			return nil, fmt.Errorf("collections: missing typed_column_part asset for generation=%d", physical.Ref.Generation)
		}
		if _, exists := physicalRefsByGeneration[physical.Ref.Generation]; exists {
			return nil, fmt.Errorf("collections: duplicate physical row asset ref for generation=%d", physical.Ref.Generation)
		}
		physicalRefsByGeneration[physical.Ref.Generation] = struct{}{}
	}
	for generation := range refsByGeneration {
		if _, ok := physicalRefsByGeneration[generation]; !ok {
			return nil, fmt.Errorf("collections: missing physical row asset for typed_column_part generation=%d", generation)
		}
	}
	return physicalRefsByGeneration, nil
}

func validateTypedColumnMultipartAssetPairing(refsByGeneration map[uint64]columnManifestAssetRefForScan, assetRefs []columnManifestAssetRefForScan) error {
	physicalRefsByGeneration := make(map[uint64]struct{}, len(assetRefs))
	for _, physical := range assetRefs {
		if physical.Ref.Kind != ColumnAssetKindTCS1PartImage {
			return fmt.Errorf("collections: typed-column physical asset pairing requires typed-row physical refs, got %s", physical.Ref.Kind)
		}
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef, ok := refsByGeneration[physical.Ref.Generation]
		if !ok {
			return fmt.Errorf("collections: missing typed_column_part asset for generation=%d", physical.Ref.Generation)
		}
		if typedRef.Rows != physical.Rows {
			return fmt.Errorf("collections: typed_column_part rows=%d do not match physical rows=%d for generation=%d", typedRef.Rows, physical.Rows, physical.Ref.Generation)
		}
		if _, exists := physicalRefsByGeneration[physical.Ref.Generation]; exists {
			return fmt.Errorf("collections: duplicate physical row asset ref for generation=%d", physical.Ref.Generation)
		}
		physicalRefsByGeneration[physical.Ref.Generation] = struct{}{}
	}
	for generation := range refsByGeneration {
		if _, ok := physicalRefsByGeneration[generation]; !ok {
			return fmt.Errorf("collections: missing physical row asset for typed_column_part generation=%d", generation)
		}
	}
	return nil
}

func typedColumnPhysicalAssetPairingScanError(err error) error {
	var reasonErr typedColumnPhysicalAssetPairingReasonError
	if errors.As(err, &reasonErr) {
		return fmt.Errorf("collections: typed-column int64 predicate scan requires insert-only physical refs, got %s", reasonErr.reason)
	}
	return err
}

func typedColumnPhysicalAssetPairingAggregateError(err error) error {
	var reasonErr typedColumnPhysicalAssetPairingReasonError
	if errors.As(err, &reasonErr) {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate requires insert-only physical refs, got %s", reasonErr.reason)
	}
	return err
}

func typedColumnRefsHaveSortKey(refsByGeneration map[uint64]columnManifestAssetRefForScan) bool {
	for _, ref := range refsByGeneration {
		if len(ref.SortKey) != 0 {
			return true
		}
	}
	return false
}

func typedColumnSortedMutationVisibilityUnsupported(operation string) error {
	return fmt.Errorf("%w: %s with mutation visibility requires primary-id ordered typed_column_part assets; sorted typed-column mutation visibility is deferred", ErrColumnQueryPlanUnsupported, operation)
}

func typedColumnPhysicalRowIndexFromPrimaryID(primaryID int64, rows int) (int, error) {
	if primaryID < 0 || primaryID >= int64(rows) {
		return 0, fmt.Errorf("primary_id=%d outside physical rows=%d", primaryID, rows)
	}
	return int(primaryID), nil
}

func (c *Collection) runTypedColumnInt64PredicateScanDirect(view columnPhysicalScanSnapshotView, req TypedColumnInt64PredicateScanRequest, cfg ColumnStoreConfig, start time.Time) (TypedColumnInt64PredicateScanResult, error) {
	diag := typedColumnInt64PredicateDiagnosticsFromView(view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	fields := columnStoreTypedColumnPartFields(cfg)
	if ok, err := typedColumnAdapterHasInt64PredicateColumn(fields, req.Column); err != nil {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	} else if !ok {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: typed-column int64 predicate column %q is not owned by typed_column_part", req.Column)
	}
	refsByGeneration := make(map[uint64]columnManifestAssetRefForScan, len(view.TypedColumnPartRefs))
	for _, ref := range view.TypedColumnPartRefs {
		if ref.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			continue
		}
		if ref.Ref.PartID != typedColumnPartAssetPartID {
			continue
		}
		if ref.Role == ColumnManifestPartRoleTombstone || ref.Reason == ColumnPublishOperationDelete {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: typed-column int64 predicate scan got tombstone typed ref generation=%d", ref.Ref.Generation)
		}
		// Current durable typed-column publication emits one typed_column_part
		// locator per generation (part_id=2) paired with that generation's
		// physical row locator part (part_id=1). Duplicate generations are a
		// manifest invariant violation and fail closed here.
		if _, exists := refsByGeneration[ref.Ref.Generation]; exists {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: duplicate typed_column_part ref for generation=%d", ref.Ref.Generation)
		}
		refsByGeneration[ref.Ref.Generation] = ref
	}
	if len(refsByGeneration) == 0 {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, errors.New("collections: missing typed_column_part assets for typed-column int64 predicate scan")
	}
	if view.MutationParts == 0 {
		if _, err := validateTypedColumnPhysicalAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, typedColumnPhysicalAssetPairingScanError(err)
		}
	} else if err := validateTypedColumnMultipartAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	} else if typedColumnRefsHaveSortKey(refsByGeneration) {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, typedColumnSortedMutationVisibilityUnsupported("typed-column int64 predicate scan")
	}

	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	}
	readCache.returnViews = true
	if err := readCache.useMappedResourceManager(mgr, mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "typed-column-int64-predicate-scan", Namespace: view.AssetNamespace, Generation: view.Diagnostics.ManifestGeneration, Reason: "typed-column int64 predicate scan"}, "typed-column int64 predicate scan"); err != nil {
		_ = readCache.close()
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	}
	defer func() { _ = readCache.close() }()

	result := TypedColumnInt64PredicateScanResult{Diagnostics: diag}
	var resolver *typedColumnLatestRowResolver
	if view.MutationParts != 0 {
		resolver, err = buildTypedColumnLatestRowResolver(view, &readCache, &result.Diagnostics)
		if err != nil {
			return result, err
		}
	}
	var rawScratch []byte
	for _, physical := range view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef := refsByGeneration[physical.Ref.Generation]
		result.Diagnostics.PartsConsidered++
		raw, err := readCache.read(typedRef.Ref, rawScratch)
		result.Diagnostics.SegmentFileCacheHits = readCache.hits
		result.Diagnostics.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			return result, fmt.Errorf("collections: typed-column int64 predicate read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		rawScratch = raw
		result.Diagnostics.DirectTypedColumnAssetReads++
		result.Diagnostics.PhysicalBytesScanned += int64(len(raw))
		adapterPart, adapterColumn, manifestBytes, err := typedColumnAdapterPrepareInt64PredicateScanPart(fields, raw, typedRef.Ref.PartID, typedRef.Rows, physical.Rows, cfg.SchemaHash, req.Column)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column int64 predicate decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		result.Diagnostics.DecodedMetadataBytes += uint64(manifestBytes)
		matchedStart := len(result.Rows)
		var visibility *typedColumnLatestPhysicalPart
		if resolver != nil {
			var ok bool
			visibility, ok = resolver.partForGeneration(physical.Ref.Generation)
			if !ok {
				return result, fmt.Errorf("collections: typed-column int64 predicate missing latest-visible physical generation=%d", physical.Ref.Generation)
			}
		}
		if visibility != nil && typedColumnAdapterPartHasLogicalSortKey(adapterPart) {
			return result, typedColumnSortedMutationVisibilityUnsupported("typed-column int64 predicate scan")
		}
		partPruned, err := scanTypedColumnInt64PredicatePartWithVisibility(adapterPart.Part, adapterColumn.Definition.Name, req, typedRef.Ref.Generation, typedRef.Ref.PartID, &result, visibility)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column int64 predicate scan generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if partPruned {
			result.Diagnostics.PartsPruned++
		} else {
			result.Diagnostics.PartsDecoded++
		}
		if resolver == nil && len(result.Rows) > matchedStart {
			result.Diagnostics.PhysicalRowAssetReads++
			physicalRaw, err := readCache.read(physical.Ref, rawScratch)
			result.Diagnostics.SegmentFileCacheHits = readCache.hits
			result.Diagnostics.SegmentFileCacheMisses = readCache.misses
			if err != nil {
				return result, fmt.Errorf("collections: typed-column int64 predicate physical id read generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			rawScratch = physicalRaw
			result.Diagnostics.PhysicalBytesScanned += int64(len(physicalRaw))
			result.Diagnostics.PhysicalRowIDLookups++
			ids, err := typedColumnInt64PredicatePhysicalRowIDs(physicalRaw, physical.Ref, view.CollectionName, view.Config, physical.Rows, result.Rows[matchedStart:])
			if err != nil {
				return result, fmt.Errorf("collections: typed-column int64 predicate physical id decode generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			for rowIdx := matchedStart; rowIdx < len(result.Rows); rowIdx++ {
				matched := &result.Rows[rowIdx]
				physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(matched.PrimaryID, physical.Rows)
				if err != nil {
					return result, err
				}
				documentID, ok := ids[physicalRowIndex]
				if !ok {
					return result, fmt.Errorf("collections: typed-column int64 predicate missing physical document id for row_index=%d", physicalRowIndex)
				}
				matched.Generation = physical.Ref.Generation
				matched.PartID = physical.Ref.PartID
				matched.RowIndex = physicalRowIndex
				matched.DocumentID = documentID
			}
		}
	}
	stats := mgr.Stats()
	result.Diagnostics.MappedBytes = stats.TotalMappedBytes
	result.Diagnostics.HeapCopyBytes = stats.TotalHeapCopyBytes
	result.Diagnostics.FallbackReads = int(stats.FallbackReads)
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, nil
}

func (c *Collection) runTypedColumnInt64PredicateAggregateDirect(view columnPhysicalScanSnapshotView, req TypedColumnInt64PredicateAggregateRequest, _ ColumnStoreConfig, start time.Time) (TypedColumnInt64PredicateAggregateResult, error) {
	session, diag, err := c.prepareTypedColumnInt64PredicateAggregateSessionFromView(view, nil, req)
	if err != nil {
		return TypedColumnInt64PredicateAggregateResult{Diagnostics: diag}, err
	}
	defer func() { _ = session.Close() }()
	includeDiagnostics := session.prepareDiagnostics
	includeDiagnostics.PruningBlocks = 0
	includeDiagnostics.PruningRows = 0
	includeDiagnostics.PruningFallbackBlocks = 0
	includeDiagnostics.PruningFallbackReason = ""
	return session.run(start, includeDiagnostics)
}

func (c *Collection) prepareTypedColumnInt64PredicateAggregateSessionFromView(view columnPhysicalScanSnapshotView, closeView func(), req TypedColumnInt64PredicateAggregateRequest) (*TypedColumnInt64PredicateAggregateSession, TypedColumnInt64PredicateScanDiagnostics, error) {
	diag := typedColumnInt64PredicateDiagnosticsFromView(view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	if ok, err := typedColumnAdapterHasInt64PredicateColumn(fields, req.Column); err != nil {
		return nil, diag, err
	} else if !ok {
		return nil, diag, fmt.Errorf("collections: typed-column int64 predicate aggregate column %q is not owned by typed_column_part", req.Column)
	}
	aggregateColumn, ok, err := typedColumnInt64PredicateAdapterColumn(fields, req.Column)
	if err != nil {
		return nil, diag, err
	}
	if !ok {
		return nil, diag, fmt.Errorf("collections: typed-column int64 predicate aggregate column %q is not owned by typed_column_part", req.Column)
	}
	refsByGeneration, err := typedColumnInt64PredicateAggregateRefsByGeneration(view)
	if err != nil {
		return nil, diag, err
	}
	if len(refsByGeneration) == 0 {
		return nil, diag, errors.New("collections: missing typed_column_part assets for typed-column int64 predicate aggregate")
	}
	if view.MutationParts == 0 {
		if _, err := validateTypedColumnPhysicalAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
			return nil, diag, typedColumnPhysicalAssetPairingAggregateError(err)
		}
	} else if err := validateTypedColumnMultipartAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		return nil, diag, err
	} else if typedColumnRefsHaveSortKey(refsByGeneration) {
		return nil, diag, typedColumnSortedMutationVisibilityUnsupported("typed-column int64 predicate aggregate")
	}

	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return nil, diag, err
	}
	readCache.returnViews = true
	readCache.trustCachedVerifyFileIdentity = true
	scope := mappedresource.Scope{Kind: mappedresource.ScopePreparedQuery, ID: "typed-column-int64-predicate-aggregate", Namespace: view.AssetNamespace, Collection: view.CollectionName, Generation: view.Diagnostics.ManifestGeneration, Reason: "typed-column int64 predicate aggregate session"}
	if err := readCache.useMappedResourceManager(mgr, scope, "typed-column int64 predicate aggregate session"); err != nil {
		_ = readCache.close()
		return nil, diag, err
	}

	session := &TypedColumnInt64PredicateAggregateSession{
		view:             view,
		closeView:        closeView,
		req:              req,
		fields:           fields,
		aggregateColumn:  aggregateColumn,
		schemaHash:       cfg.SchemaHash,
		refsByGeneration: refsByGeneration,
		validatedRefs:    make(map[ColumnAssetRef]struct{}, len(refsByGeneration)),
		readCache:        readCache,
		resourceManager:  mgr,
	}
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

func typedColumnInt64PredicateAggregateRefsByGeneration(view columnPhysicalScanSnapshotView) (map[uint64]columnManifestAssetRefForScan, error) {
	refsByGeneration := make(map[uint64]columnManifestAssetRefForScan, len(view.TypedColumnPartRefs))
	for _, ref := range view.TypedColumnPartRefs {
		if ref.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			continue
		}
		if ref.Ref.PartID != typedColumnPartAssetPartID {
			continue
		}
		if ref.Role == ColumnManifestPartRoleTombstone || ref.Reason == ColumnPublishOperationDelete {
			return nil, fmt.Errorf("collections: typed-column int64 predicate aggregate got tombstone typed ref generation=%d", ref.Ref.Generation)
		}
		if _, exists := refsByGeneration[ref.Ref.Generation]; exists {
			return nil, fmt.Errorf("collections: duplicate typed_column_part ref for generation=%d", ref.Ref.Generation)
		}
		refsByGeneration[ref.Ref.Generation] = ref
	}
	return refsByGeneration, nil
}

// Close releases mapped asset handles and the pinned snapshot owned by the
// prepared aggregate session. It is safe to call multiple times.
func (s *TypedColumnInt64PredicateAggregateSession) Close() error {
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
	s.view = columnPhysicalScanSnapshotView{}
	s.beginForegroundRead = nil
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

// Diagnostics returns current scoped resource state for the prepared session.
func (s *TypedColumnInt64PredicateAggregateSession) Diagnostics() TypedColumnInt64PredicateAggregateSessionDiagnostics {
	if s == nil {
		return TypedColumnInt64PredicateAggregateSessionDiagnostics{Closed: true}
	}
	stats := mappedresource.Stats{}
	if s.resourceManager != nil {
		stats = s.resourceManager.Stats()
	}
	return TypedColumnInt64PredicateAggregateSessionDiagnostics{
		Closed:                   s.closed,
		ColumnAssetReadIntegrity: columnAssetReadIntegrityLabel(s.req.ColumnAssetReadIntegrity),
		SegmentFileCacheHits:     s.readCache.hits,
		SegmentFileCacheMisses:   s.readCache.misses,
		ActiveResourceHandles:    stats.ActiveHandles,
		ActiveMappedBytes:        stats.ActiveMappedBytes,
		ActiveHeapCopyBytes:      stats.ActiveHeapCopyBytes,
		TotalResourceAcquires:    stats.TotalAcquires,
		TotalResourceReleases:    stats.TotalReleases,
		TotalMappedBytes:         stats.TotalMappedBytes,
		TotalHeapCopyBytes:       stats.TotalHeapCopyBytes,
		FallbackReads:            stats.FallbackReads,
	}
}

// Run executes one hot aggregate scan against the prepared snapshot. Setup and
// warmup work done before Run is not included in the returned ScanNanos.
func (s *TypedColumnInt64PredicateAggregateSession) Run() (TypedColumnInt64PredicateAggregateResult, error) {
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

func (s *TypedColumnInt64PredicateAggregateSession) run(start time.Time, includeDiagnostics TypedColumnInt64PredicateScanDiagnostics) (TypedColumnInt64PredicateAggregateResult, error) {
	if s == nil || s.closed {
		return TypedColumnInt64PredicateAggregateResult{}, errTypedColumnInt64PredicateAggregateSessionClosed
	}
	diag := typedColumnInt64PredicateDiagnosticsFromView(s.view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(s.req.ColumnAssetReadIntegrity)
	addTypedColumnInt64PredicateAggregateDiagnostics(&diag, includeDiagnostics)
	result := TypedColumnInt64PredicateAggregateResult{Diagnostics: diag}
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
	if result.Count != 0 {
		result.Avg = float64(result.Sum) / float64(result.Count)
	}
	if s.resourceManager != nil {
		afterStats := s.resourceManager.Stats()
		result.Diagnostics.FallbackReads += int(afterStats.FallbackReads - beforeStats.FallbackReads)
	}
	updateCacheDeltas()
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, nil
}

func (s *TypedColumnInt64PredicateAggregateSession) useTargetedAggregateRanges() bool {
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

func (s *TypedColumnInt64PredicateAggregateSession) runFullAssetAggregatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, rawScratch []byte, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) ([]byte, error) {
	raw, err := s.readCache.read(typedRef.Ref, rawScratch)
	updateCacheDeltas()
	if err != nil {
		return rawScratch, fmt.Errorf("collections: typed-column int64 predicate aggregate read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
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
	adapterPart, adapterColumn, manifestBytes, err := typedColumnAdapterPrepareInt64PredicateAggregatePart(s.fields, raw, typedRef.Ref.PartID, typedRef.Rows, physical.Rows, s.schemaHash, s.req.Column)
	if err != nil {
		return rawScratch, fmt.Errorf("collections: typed-column int64 predicate aggregate decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
	}
	result.Diagnostics.DecodedMetadataBytes += uint64(manifestBytes)
	return rawScratch, s.scanPreparedAggregatePart(typedRef, physical, adapterPart, adapterColumn, result)
}

func (s *TypedColumnInt64PredicateAggregateSession) runTargetedAggregatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) error {
	preparedPart, ok := typedColumnPreparedStatePart(s.preparedState, typedRef.Ref)
	if !ok || preparedPart == nil {
		return fmt.Errorf("collections: typed-column int64 predicate aggregate missing prepared state generation=%d part_id=%d", typedRef.Ref.Generation, typedRef.Ref.PartID)
	}
	return s.scanPreparedAggregateStatePart(typedRef, physical, preparedPart, result, updateCacheDeltas)
}

func (s *TypedColumnInt64PredicateAggregateSession) ensureCachedVerifyFullAssetValidated(ref ColumnAssetRef, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) error {
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

func (s *TypedColumnInt64PredicateAggregateSession) readTypedColumnRange(ref ColumnAssetRef, offset int, length int, section bool, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) ([]byte, error) {
	raw, _, err := s.readTypedColumnRangeHandle(ref, offset, length, section, result, updateCacheDeltas)
	return raw, err
}

func (s *TypedColumnInt64PredicateAggregateSession) readTypedColumnRangeHandle(ref ColumnAssetRef, offset int, length int, section bool, result *TypedColumnInt64PredicateAggregateResult, updateCacheDeltas func()) ([]byte, *mappedresource.Handle, error) {
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

func (s *TypedColumnInt64PredicateAggregateSession) scanPreparedAggregatePart(typedRef columnManifestAssetRefForScan, physical columnManifestAssetRefForScan, adapterPart *typedColumnAdapterPart, adapterColumn typedColumnAdapterColumn, result *TypedColumnInt64PredicateAggregateResult) error {
	var visibility *typedColumnLatestPhysicalPart
	if s.resolver != nil {
		var ok bool
		visibility, ok = s.resolver.partForGeneration(physical.Ref.Generation)
		if !ok {
			return fmt.Errorf("collections: typed-column int64 predicate aggregate missing latest-visible physical generation=%d", physical.Ref.Generation)
		}
	}
	if visibility != nil && typedColumnAdapterPartHasLogicalSortKey(adapterPart) {
		return typedColumnSortedMutationVisibilityUnsupported("typed-column int64 predicate aggregate")
	}
	partPruned, err := scanTypedColumnInt64PredicateAggregatePartWithExpressionAndScratch(adapterPart.Part, adapterColumn.Definition.Name, typedColumnInt64PredicateAggregateScanRequest(s.req), s.req.Expression, result, visibility, &s.aggregateScratch)
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

func addTypedColumnInt64PredicateAggregateDiagnostics(dst *TypedColumnInt64PredicateScanDiagnostics, src TypedColumnInt64PredicateScanDiagnostics) {
	if dst == nil {
		return
	}
	dst.RowsScanned += src.RowsScanned
	dst.RowsMatched += src.RowsMatched
	dst.PartsConsidered += src.PartsConsidered
	dst.PartsPruned += src.PartsPruned
	dst.PartsDecoded += src.PartsDecoded
	dst.BlocksConsidered += src.BlocksConsidered
	dst.BlocksPruned += src.BlocksPruned
	dst.BlocksDecoded += src.BlocksDecoded
	dst.SelectionEmptyBlocks += src.SelectionEmptyBlocks
	dst.SelectionAllBlocks += src.SelectionAllBlocks
	dst.SelectionRangeBlocks += src.SelectionRangeBlocks
	dst.SelectionRangesBlocks += src.SelectionRangesBlocks
	dst.SelectionBitmapBlocks += src.SelectionBitmapBlocks
	dst.SelectionSparseBlocks += src.SelectionSparseBlocks
	dst.SelectionCompositions += src.SelectionCompositions
	dst.DirectTypedColumnAssetReads += src.DirectTypedColumnAssetReads
	dst.FullAssetReads += src.FullAssetReads
	dst.FallbackReads += src.FallbackReads
	dst.CodesMatched += src.CodesMatched
	dst.DictionaryBytesDecoded += src.DictionaryBytesDecoded
	dst.FullAssetBytes += src.FullAssetBytes
	dst.SectionBytesRead += src.SectionBytesRead
	dst.RangeBytesRead += src.RangeBytesRead
	dst.MappedBytes += src.MappedBytes
	dst.HeapCopyBytes += src.HeapCopyBytes
	dst.DecodedMetadataBytes += src.DecodedMetadataBytes
	dst.DecodedHeapCopyBytes += src.DecodedHeapCopyBytes
	dst.MaterializedBytes += src.MaterializedBytes
	dst.FastDecodeDirectViewPlans += src.FastDecodeDirectViewPlans
	dst.FastDecodeStreamingPlans += src.FastDecodeStreamingPlans
	dst.FastDecodeMaterializePlans += src.FastDecodeMaterializePlans
	dst.FastDecodeUnsupportedPlans += src.FastDecodeUnsupportedPlans
	dst.FastDecodeMmapDirectViews += src.FastDecodeMmapDirectViews
	dst.FastDecodeHeapCopyTypedViews += src.FastDecodeHeapCopyTypedViews
	dst.FastDecodeScratchDecodes += src.FastDecodeScratchDecodes
	dst.FastDecodeStreamingFallbacks += src.FastDecodeStreamingFallbacks
	dst.FastDecodeCertificationFailure += src.FastDecodeCertificationFailure
	dst.FastDecodeAbsoluteUnaligned += src.FastDecodeAbsoluteUnaligned
	dst.FastDecodeActualUnaligned += src.FastDecodeActualUnaligned
	dst.FastDecodeStaleHandles += src.FastDecodeStaleHandles
	dst.DirectViewSuccesses += src.DirectViewSuccesses
	dst.DirectViewFailures += src.DirectViewFailures
	dst.KernelBlocks += src.KernelBlocks
	dst.KernelFullCoveredBlocks += src.KernelFullCoveredBlocks
	dst.KernelSelectedBlocks += src.KernelSelectedBlocks
	dst.KernelCursorBlocks += src.KernelCursorBlocks
	dst.KernelFallbackBlocks += src.KernelFallbackBlocks
	dst.StatsBlocks += src.StatsBlocks
	dst.StatsFullCoveredBlocks += src.StatsFullCoveredBlocks
	dst.StatsFallbackBlocks += src.StatsFallbackBlocks
	dst.StatsRows += src.StatsRows
	dst.PruningBlocks += src.PruningBlocks
	dst.PruningRows += src.PruningRows
	dst.PruningFallbackBlocks += src.PruningFallbackBlocks
	if src.PruningFallbackReason != "" {
		dst.PruningFallbackReason = src.PruningFallbackReason
	}
	if src.StatsFallbackReason != "" {
		dst.StatsFallbackReason = src.StatsFallbackReason
	}
	dst.StatsValidationFailures += src.StatsValidationFailures
	if src.StatsValidationFailureReason != "" {
		dst.StatsValidationFailureReason = src.StatsValidationFailureReason
	}
	dst.PruningValidationFailures += src.PruningValidationFailures
	if src.PruningValidationFailureReason != "" {
		dst.PruningValidationFailureReason = src.PruningValidationFailureReason
	}
	if src.FastDecodeFallbackReason != "" {
		dst.FastDecodeFallbackReason = src.FastDecodeFallbackReason
	}
	dst.DirectViewCertified += src.DirectViewCertified
	dst.StreamingCertified += src.StreamingCertified
	dst.StatsCertified += src.StatsCertified
	dst.PruningCertified += src.PruningCertified
	dst.CertificationFailures += src.CertificationFailures
	if src.CertificationFailureReason != "" {
		dst.CertificationFailureReason = src.CertificationFailureReason
	}
	dst.PhysicalBytesScanned += src.PhysicalBytesScanned
	dst.RowLocatorDecodes += src.RowLocatorDecodes
	dst.PhysicalRowIDLookups += src.PhysicalRowIDLookups
	dst.PhysicalRowAssetReads += src.PhysicalRowAssetReads
	dst.RowMaterializations += src.RowMaterializations
	dst.DocumentMaterializations += src.DocumentMaterializations
	dst.DocumentReconstructions += src.DocumentReconstructions
	dst.SegmentFileCacheHits += src.SegmentFileCacheHits
	dst.SegmentFileCacheMisses += src.SegmentFileCacheMisses
}

func (c *Collection) typedColumnInt64PredicateCatalogColumn(column string) (ColumnStoreConfig, ColumnStoreColumn, bool, error) {
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

func typedColumnInt64PredicatePhysicalRowIDs(raw []byte, ref ColumnAssetRef, collection string, cfg ColumnStoreConfig, physicalRows int, matchedRows []TypedColumnInt64PredicateScanRow) (map[int][]byte, error) {
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(cfg.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	wanted := make(map[int]struct{}, len(matchedRows))
	for _, row := range matchedRows {
		physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(row.PrimaryID, physicalRows)
		if err != nil {
			return nil, err
		}
		wanted[physicalRowIndex] = struct{}{}
	}
	ids := make(map[int][]byte, len(wanted))
	_, err := scanColumnPhysicalAssetRowsWithManifestOperation(raw, ref, collection, &cfg, projection, ColumnPublishOperationInsert, func(row columnPhysicalScanRowView) error {
		if row.Deleted {
			return fmt.Errorf("physical row[%d] is deleted", row.RowIndex)
		}
		if _, ok := wanted[row.RowIndex]; ok {
			ids[row.RowIndex] = bytes.Clone(row.ID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for rowIndex := range wanted {
		if ids[rowIndex] == nil {
			return nil, fmt.Errorf("missing physical document id for row_index=%d", rowIndex)
		}
	}
	return ids, nil
}

func typedColumnInt64PredicateMayMatch(req TypedColumnInt64PredicateScanRequest, minValue, maxValue int64) bool {
	switch req.Kind {
	case TypedColumnInt64PredicateAll:
		return true
	case TypedColumnInt64PredicateEqual:
		return req.Value >= minValue && req.Value <= maxValue
	case TypedColumnInt64PredicateRange:
		return req.High >= minValue && req.Low <= maxValue
	default:
		return false
	}
}

func typedColumnInt64PredicateMatches(req TypedColumnInt64PredicateScanRequest, value int64) bool {
	switch req.Kind {
	case TypedColumnInt64PredicateAll:
		return true
	case TypedColumnInt64PredicateEqual:
		return value == req.Value
	case TypedColumnInt64PredicateRange:
		return value >= req.Low && value <= req.High
	default:
		return false
	}
}

func (c *Collection) runTypedColumnInt64PredicateScanPhysicalFallback(view columnPhysicalScanSnapshotView, req TypedColumnInt64PredicateScanRequest, cfg ColumnStoreConfig, reason string, start time.Time) (TypedColumnInt64PredicateScanResult, error) {
	result := TypedColumnInt64PredicateScanResult{Diagnostics: typedColumnInt64PredicateDiagnosticsFromView(view)}
	result.Diagnostics.Fallback = true
	result.Diagnostics.FallbackReason = reason
	scanDiag, err := c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ProjectedColumns:  []string{req.Column},
		RequireInsertOnly: true,
		ReadIntegrity:     req.ColumnAssetReadIntegrity,
		Visitor: func(row columnPhysicalScanRowView) error {
			if row.Deleted || len(row.Values) == 0 {
				return nil
			}
			value, err := columnPhysicalQueryInt64Value(row.Values[0])
			if err != nil {
				return err
			}
			result.Diagnostics.RowsScanned++
			if typedColumnInt64PredicateMatches(req, value) {
				result.Rows = append(result.Rows, TypedColumnInt64PredicateScanRow{Generation: row.Generation, PartID: row.PartID, RowIndex: row.RowIndex, PrimaryID: int64(row.RowIndex), DocumentID: bytes.Clone(row.ID), Value: value})
				result.Diagnostics.RowsMatched++
			}
			return nil
		},
	})
	result.Diagnostics.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	result.Diagnostics.FallbackReads = scanDiag.DecodedBlocks
	result.Diagnostics.PhysicalRowAssetReads = scanDiag.DecodedBlocks
	result.Diagnostics.PhysicalBytesScanned = scanDiag.PhysicalBytesScanned
	result.Diagnostics.SegmentFileCacheHits = scanDiag.SegmentFileCacheHits
	result.Diagnostics.SegmentFileCacheMisses = scanDiag.SegmentFileCacheMisses
	if err != nil {
		return result, err
	}
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	_ = cfg
	return result, nil
}

func (c *Collection) runTypedColumnInt64PredicateAggregateDocumentFallback(req TypedColumnInt64PredicateAggregateRequest, cfg ColumnStoreConfig, reason string, start time.Time) (TypedColumnInt64PredicateAggregateResult, error) {
	result := TypedColumnInt64PredicateAggregateResult{}
	result.Diagnostics.Fallback = true
	result.Diagnostics.FallbackReason = reason
	result.Diagnostics.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	path := req.Column
	if col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column); ok {
		path = col.Path
	}
	fallbackCfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{{Name: req.Column, Path: path, ValueType: ColumnStoreValueInt64}}}
	_, err := c.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		result.Diagnostics.RowMaterializations++
		result.Diagnostics.DocumentMaterializations++
		result.Diagnostics.MaterializedBytes += uint64(len(record.ID) + len(record.Document))
		result.Diagnostics.FallbackReads++
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(fallbackCfg, []columnWriteDocument{{ID: record.ID, Document: record.Document}})
		if err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0].Values) != 1 {
			return false, errors.New("collections: document fallback failed to extract int64 predicate aggregate column")
		}
		value, err := columnPhysicalQueryInt64Value(rows[0].Values[0])
		if err != nil {
			return false, err
		}
		result.Diagnostics.RowsScanned++
		if typedColumnInt64PredicateMatches(typedColumnInt64PredicateAggregateScanRequest(req), value) {
			if err := addTypedColumnInt64PredicateAggregateExpressionValue(&result, req.Expression, value); err != nil {
				return false, err
			}
			result.Diagnostics.RowsMatched++
		}
		return true, nil
	})
	if result.Count != 0 {
		result.Avg = float64(result.Sum) / float64(result.Count)
	}
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, err
}

func (c *Collection) runTypedColumnInt64PredicateScanDocumentFallback(req TypedColumnInt64PredicateScanRequest, cfg ColumnStoreConfig, reason string, start time.Time) (TypedColumnInt64PredicateScanResult, error) {
	result := TypedColumnInt64PredicateScanResult{}
	result.Diagnostics.Fallback = true
	result.Diagnostics.FallbackReason = reason
	result.Diagnostics.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	path := req.Column
	if col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column); ok {
		path = col.Path
	}
	fallbackCfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{{Name: req.Column, Path: path, ValueType: ColumnStoreValueInt64}}}
	_, err := c.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		result.Diagnostics.RowMaterializations++
		result.Diagnostics.DocumentMaterializations++
		result.Diagnostics.MaterializedBytes += uint64(len(record.ID) + len(record.Document))
		result.Diagnostics.FallbackReads++
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(fallbackCfg, []columnWriteDocument{{ID: record.ID, Document: record.Document}})
		if err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0].Values) != 1 {
			return false, errors.New("collections: document fallback failed to extract int64 predicate column")
		}
		value, err := columnPhysicalQueryInt64Value(rows[0].Values[0])
		if err != nil {
			return false, err
		}
		result.Diagnostics.RowsScanned++
		if typedColumnInt64PredicateMatches(req, value) {
			result.Rows = append(result.Rows, TypedColumnInt64PredicateScanRow{DocumentID: bytes.Clone(record.ID), Value: value})
			result.Diagnostics.RowsMatched++
		}
		return true, nil
	})
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, err
}

func typedColumnInt64PredicateDiagnosticsFromView(view columnPhysicalScanSnapshotView) TypedColumnInt64PredicateScanDiagnostics {
	return TypedColumnInt64PredicateScanDiagnostics{
		ManifestRoot:               view.Diagnostics.ManifestRoot,
		ManifestGeneration:         view.Diagnostics.ManifestGeneration,
		RecoveryManifestGeneration: view.Diagnostics.RecoveryManifestGeneration,
		AppliedCommandLSN:          view.Diagnostics.AppliedCommandLSN,
		ManifestRecords:            view.Diagnostics.ManifestRecords,
		AssetRefs:                  view.Diagnostics.AssetRefs,
		MutationParts:              view.Diagnostics.MutationParts,
	}
}
