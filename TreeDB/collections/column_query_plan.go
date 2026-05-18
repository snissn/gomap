package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

type ColumnQueryPlanKind string

const (
	ColumnQueryPlanRowStoreBaseline   ColumnQueryPlanKind = "row_store_baseline"
	ColumnQueryPlanBTreeIndexBaseline ColumnQueryPlanKind = "b_tree_index_baseline"
	ColumnQueryPlanSerialColumnScan   ColumnQueryPlanKind = "serial_column_scan"
	ColumnQueryPlanAggregateMetadata  ColumnQueryPlanKind = "aggregate_metadata"
	ColumnQueryPlanParallelColumnScan ColumnQueryPlanKind = "parallel_column_scan"
)

var ErrColumnQueryPlanUnsupported = errors.New("collections: column query plan unsupported")

type ColumnQueryPredicateOperator string

const (
	ColumnQueryPredicateEqual          ColumnQueryPredicateOperator = "="
	ColumnQueryPredicateGreaterOrEqual ColumnQueryPredicateOperator = ">="
	ColumnQueryPredicateGreaterThan    ColumnQueryPredicateOperator = ">"
	ColumnQueryPredicateLessOrEqual    ColumnQueryPredicateOperator = "<="
	ColumnQueryPredicateLessThan       ColumnQueryPredicateOperator = "<"
)

type ColumnQueryPredicate struct {
	Column string
	// Operator participates in planner candidate ordering: equality and
	// unspecified predicates are preferred before range predicates for index
	// baselines. Unknown operators are ignored rather than driving B-tree
	// baseline selection; physical predicate pushdown is a later column-store
	// milestone.
	Operator ColumnQueryPredicateOperator
}

type ColumnQueryPlannerCapabilities struct {
	SerialColumnScan   bool
	AggregateMetadata  bool
	ParallelColumnScan bool
	PhysicalAssetCount int
	// PartCount is a fallback schedulable-unit estimate when GranuleCount is
	// not populated by the column asset layer.
	PartCount int
	// GranuleCount is the authoritative schedulable-unit count for parallel
	// planning when the column asset layer exposes adaptive marks/granules.
	GranuleCount            int
	MaxParallelWorkers      int
	DecodedBlockCacheHits   uint64
	DecodedBlockCacheMisses uint64
	PlannerCandidateBudget  int
}

// ColumnQueryPlanRequest describes one planner decision. Column, index, and
// aggregate metadata names are matched case-sensitively after trimming
// surrounding whitespace; callers should canonicalize catalog names before
// storing them rather than relying on planner-time normalization. Physical
// column plans also require every non-empty ProjectedColumns and predicate
// column entry to be declared in the collection's ColumnStore config; row/B-tree
// baselines read source documents and do not enforce that projection gate.
type ColumnQueryPlanRequest struct {
	Name                  string
	ProjectedColumns      []string
	Predicates            []ColumnQueryPredicate
	CandidateIndexColumns []string
	AggregateMetadataName string
	EstimatedRows         int
	ForceKind             ColumnQueryPlanKind
	Capabilities          ColumnQueryPlannerCapabilities
}

type ColumnQueryPlanDiagnostics struct {
	Reason                  string
	CandidatePlans          int
	ProjectedColumns        int
	Predicates              int
	RecoveryAuthoritative   bool
	PhysicalAssetCount      int
	PartCount               int
	GranuleCount            int
	MaxParallelWorkers      int
	WorkerCount             int
	ScheduledGranules       int
	SkippedGranules         int
	DecodedBlockCacheHits   uint64
	DecodedBlockCacheMisses uint64
	SelectedIndexName       string
	SelectedIndexField      string
	SelectedManifestRoot    uint64
	SelectedManifestGen     uint64
	RecoveryManifestGen     uint64
	AppliedCommandLSN       uint64
	UnsupportedPlanKind     ColumnQueryPlanKind
	UnsupportedPlanReason   string
}

type ColumnQueryPlan struct {
	Kind        ColumnQueryPlanKind
	Supported   bool
	IndexName   string
	IndexField  string
	Diagnostics ColumnQueryPlanDiagnostics
}

func (c *Collection) PlanColumnQuery(req ColumnQueryPlanRequest) (ColumnQueryPlan, error) {
	if c == nil {
		return ColumnQueryPlan{}, errCollectionNil
	}
	c.catalogMu.RLock()
	catalog := c.catalog
	systemRoot := c.catalogSystemRoot
	commitSeq := c.catalogCommitSeq
	c.catalogMu.RUnlock()
	if catalog == nil {
		return ColumnQueryPlan{}, errCollectionNotFound
	}
	identity, identityOK := columnStoreCacheIdentity(catalog, systemRoot, commitSeq)
	return planColumnQueryForCatalog(catalog, identity, identityOK, req), nil
}

func planColumnQueryForCatalog(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest) ColumnQueryPlan {
	diag := ColumnQueryPlanDiagnostics{
		CandidatePlans:          columnQueryPlannerCandidateCount(catalog, identity, identityOK, req),
		ProjectedColumns:        len(req.ProjectedColumns),
		Predicates:              len(req.Predicates),
		RecoveryAuthoritative:   columnQueryManifestRecoveryAuthoritative(identity, identityOK),
		PhysicalAssetCount:      req.Capabilities.PhysicalAssetCount,
		PartCount:               req.Capabilities.PartCount,
		GranuleCount:            req.Capabilities.GranuleCount,
		MaxParallelWorkers:      req.Capabilities.MaxParallelWorkers,
		DecodedBlockCacheHits:   req.Capabilities.DecodedBlockCacheHits,
		DecodedBlockCacheMisses: req.Capabilities.DecodedBlockCacheMisses,
		SelectedManifestRoot:    identity.ManifestRoot,
		SelectedManifestGen:     identity.ManifestGeneration,
		RecoveryManifestGen:     identity.RecoveryAuthoritativeGeneration,
		AppliedCommandLSN:       identity.RecoveryAuthoritativeAppliedCommandLSN,
	}
	if reason := physicalColumnQueryUnsupportedReasonForFallback(catalog, req); reason != "" {
		diag.UnsupportedPlanReason = reason
	}

	if req.ForceKind != "" {
		return forcedColumnQueryPlan(catalog, identity, identityOK, req, diag)
	}
	if ok, plan := aggregateColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
		return plan
	}
	if ok, plan := parallelColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
		return plan
	}
	if ok, plan := serialColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
		return plan
	}
	if idx, ok := selectColumnQueryBTreeIndex(catalog, req); ok {
		diag.SelectedIndexName = idx.Name
		diag.SelectedIndexField = idx.Field
		diag.Reason = "selected matching collection secondary index for full-scan B-tree baseline"
		return ColumnQueryPlan{Kind: ColumnQueryPlanBTreeIndexBaseline, Supported: true, IndexName: idx.Name, IndexField: idx.Field, Diagnostics: diag}
	}
	diag.Reason = "row-store fallback"
	return ColumnQueryPlan{Kind: ColumnQueryPlanRowStoreBaseline, Supported: true, Diagnostics: diag}
}

func forcedColumnQueryPlan(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, diag ColumnQueryPlanDiagnostics) ColumnQueryPlan {
	switch req.ForceKind {
	case ColumnQueryPlanRowStoreBaseline:
		diag.Reason = "forced row-store baseline"
		return ColumnQueryPlan{Kind: ColumnQueryPlanRowStoreBaseline, Supported: true, Diagnostics: diag}
	case ColumnQueryPlanBTreeIndexBaseline:
		idx, ok := selectColumnQueryBTreeIndex(catalog, req)
		if !ok {
			return unsupportedColumnQueryPlan(ColumnQueryPlanBTreeIndexBaseline, "no matching collection secondary index", diag)
		}
		diag.SelectedIndexName = idx.Name
		diag.SelectedIndexField = idx.Field
		diag.Reason = "forced matching collection secondary index for full-scan B-tree baseline"
		return ColumnQueryPlan{Kind: ColumnQueryPlanBTreeIndexBaseline, Supported: true, IndexName: idx.Name, IndexField: idx.Field, Diagnostics: diag}
	case ColumnQueryPlanSerialColumnScan:
		if ok, plan := serialColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
			plan.Diagnostics.Reason = "forced serial physical column scan"
			return plan
		}
		return unsupportedColumnQueryPlan(ColumnQueryPlanSerialColumnScan, physicalColumnQueryUnsupportedReasonForCatalog(catalog, identity, identityOK, req, ColumnQueryPlanSerialColumnScan), diag)
	case ColumnQueryPlanAggregateMetadata:
		if ok, plan := aggregateColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
			plan.Diagnostics.Reason = "forced aggregate metadata plan"
			return plan
		}
		return unsupportedColumnQueryPlan(ColumnQueryPlanAggregateMetadata, aggregateColumnQueryUnsupportedReason(catalog, identity, identityOK, req), diag)
	case ColumnQueryPlanParallelColumnScan:
		if ok, plan := parallelColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
			plan.Diagnostics.Reason = "forced parallel physical column scan"
			return plan
		}
		return unsupportedColumnQueryPlan(ColumnQueryPlanParallelColumnScan, physicalColumnQueryUnsupportedReasonForCatalog(catalog, identity, identityOK, req, ColumnQueryPlanParallelColumnScan), diag)
	default:
		return unsupportedColumnQueryPlan(req.ForceKind, fmt.Sprintf("unknown column query plan kind %q", req.ForceKind), diag)
	}
}

func serialColumnQueryPlan(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, diag ColumnQueryPlanDiagnostics) (bool, ColumnQueryPlan) {
	if !physicalColumnQuerySupported(catalog, identity, identityOK, req, ColumnQueryPlanSerialColumnScan) {
		return false, ColumnQueryPlan{}
	}
	diag.Reason = "selected serial physical column scan"
	diag.ScheduledGranules = req.Capabilities.GranuleCount
	diag.WorkerCount = 1
	return true, ColumnQueryPlan{Kind: ColumnQueryPlanSerialColumnScan, Supported: true, Diagnostics: diag}
}

func aggregateColumnQueryPlan(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, diag ColumnQueryPlanDiagnostics) (bool, ColumnQueryPlan) {
	if strings.TrimSpace(req.AggregateMetadataName) == "" {
		return false, ColumnQueryPlan{}
	}
	if !catalogHasColumnAggregateMetadata(catalog, req.AggregateMetadataName) {
		return false, ColumnQueryPlan{}
	}
	if !physicalColumnQuerySupported(catalog, identity, identityOK, req, ColumnQueryPlanAggregateMetadata) {
		return false, ColumnQueryPlan{}
	}
	diag.Reason = "selected aggregate metadata"
	diag.ScheduledGranules = 0
	return true, ColumnQueryPlan{Kind: ColumnQueryPlanAggregateMetadata, Supported: true, Diagnostics: diag}
}

func parallelColumnQueryPlan(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, diag ColumnQueryPlanDiagnostics) (bool, ColumnQueryPlan) {
	if !physicalColumnQuerySupported(catalog, identity, identityOK, req, ColumnQueryPlanParallelColumnScan) {
		return false, ColumnQueryPlan{}
	}
	if parallelColumnQueryShapeUnsupportedReason(req) != "" {
		return false, ColumnQueryPlan{}
	}
	workers := columnQueryParallelWorkerCount(req.Capabilities)
	diag.WorkerCount = workers
	diag.ScheduledGranules = req.Capabilities.GranuleCount
	diag.Reason = "selected parallel physical column scan"
	return true, ColumnQueryPlan{Kind: ColumnQueryPlanParallelColumnScan, Supported: true, Diagnostics: diag}
}

func physicalColumnQuerySupported(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, kind ColumnQueryPlanKind) bool {
	if !columnQueryManifestRecoveryAuthoritative(identity, identityOK) || req.Capabilities.PhysicalAssetCount <= 0 {
		return false
	}
	if !columnQueryCatalogHasEnabledColumnStore(catalog) {
		return false
	}
	if _, ok := missingColumnStoreRequestColumn(catalog, req); ok {
		return false
	}
	switch kind {
	case ColumnQueryPlanSerialColumnScan:
		return req.Capabilities.SerialColumnScan
	case ColumnQueryPlanAggregateMetadata:
		return req.Capabilities.AggregateMetadata
	case ColumnQueryPlanParallelColumnScan:
		return req.Capabilities.ParallelColumnScan
	default:
		return false
	}
}

func physicalColumnQueryUnsupportedReason(identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, kind ColumnQueryPlanKind) string {
	switch {
	case req.Capabilities.PhysicalAssetCount <= 0:
		return "no durable physical column assets are available"
	case !columnQueryManifestRecoveryAuthoritative(identity, identityOK):
		return "active column manifest is not recovery-authoritative"
	}
	switch kind {
	case ColumnQueryPlanSerialColumnScan:
		if !req.Capabilities.SerialColumnScan {
			return "serial physical column scan capability is disabled"
		}
	case ColumnQueryPlanAggregateMetadata:
		if !req.Capabilities.AggregateMetadata {
			return "aggregate metadata capability is disabled"
		}
		if strings.TrimSpace(req.AggregateMetadataName) == "" {
			return "query did not request aggregate metadata"
		}
	case ColumnQueryPlanParallelColumnScan:
		if !req.Capabilities.ParallelColumnScan {
			return "parallel physical column scan capability is disabled"
		}
		if reason := parallelColumnQueryShapeUnsupportedReason(req); reason != "" {
			return reason
		}
	}
	return "physical column plan is not supported"
}

func physicalColumnQueryUnsupportedReasonForCatalog(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, kind ColumnQueryPlanKind) string {
	if !columnQueryCatalogHasEnabledColumnStore(catalog) {
		return "collection has no enabled column store"
	}
	if missing, ok := missingColumnStoreRequestColumn(catalog, req); ok {
		return fmt.Sprintf("requested column %q is not declared in column store", missing)
	}
	return physicalColumnQueryUnsupportedReason(identity, identityOK, req, kind)
}

func physicalColumnQueryUnsupportedReasonForFallback(catalog *collectionCatalog, req ColumnQueryPlanRequest) string {
	if missing, ok := missingColumnStoreRequestColumn(catalog, req); ok {
		return fmt.Sprintf("requested column %q is not declared in column store", missing)
	}
	return ""
}

func parallelColumnQueryShapeUnsupportedReason(req ColumnQueryPlanRequest) string {
	if req.Capabilities.MaxParallelWorkers <= 1 {
		return "parallel scan requires at least two workers"
	}
	if columnQueryParallelWorkUnits(req.Capabilities) <= 1 {
		return "parallel scan requires more than one available granule or part"
	}
	if columnQueryParallelWorkerCount(req.Capabilities) <= 1 {
		return "parallel scan requires more than one worker after clamping to available granules or parts"
	}
	return ""
}

func columnQueryParallelWorkerCount(caps ColumnQueryPlannerCapabilities) int {
	workers := caps.MaxParallelWorkers
	if workers <= 0 {
		return 0
	}
	if units := columnQueryParallelWorkUnits(caps); units > 0 && workers > units {
		return units
	}
	return workers
}

func columnQueryParallelWorkUnits(caps ColumnQueryPlannerCapabilities) int {
	if caps.GranuleCount > 0 {
		return caps.GranuleCount
	}
	return caps.PartCount
}

func aggregateColumnQueryUnsupportedReason(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest) string {
	if missing, ok := missingColumnStoreRequestColumn(catalog, req); ok {
		return fmt.Sprintf("requested column %q is not declared in column store", missing)
	}
	if !columnQueryCatalogHasEnabledColumnStore(catalog) {
		return "collection has no enabled column store"
	}
	name := strings.TrimSpace(req.AggregateMetadataName)
	if name == "" {
		return physicalColumnQueryUnsupportedReason(identity, identityOK, req, ColumnQueryPlanAggregateMetadata)
	}
	if !catalogHasColumnAggregateMetadata(catalog, name) {
		return fmt.Sprintf("unknown aggregate metadata %q", name)
	}
	return physicalColumnQueryUnsupportedReason(identity, identityOK, req, ColumnQueryPlanAggregateMetadata)
}

func columnQueryCatalogHasEnabledColumnStore(catalog *collectionCatalog) bool {
	return catalog != nil &&
		catalog.meta.Options.ColumnStore != nil &&
		catalog.meta.Options.ColumnStore.Enabled
}

func catalogHasColumnAggregateMetadata(catalog *collectionCatalog, name string) bool {
	name = strings.TrimSpace(name)
	if !columnQueryCatalogHasEnabledColumnStore(catalog) || name == "" {
		return false
	}
	for _, aggregate := range catalog.meta.Options.ColumnStore.AggregateMetadata {
		// Metadata can be supplied by pre-alpha JSON fixtures, so trim at the
		// planner boundary until catalog loading owns canonicalization.
		if strings.TrimSpace(aggregate.Name) == name {
			return true
		}
	}
	return false
}

func missingColumnStoreRequestColumn(catalog *collectionCatalog, req ColumnQueryPlanRequest) (string, bool) {
	if len(req.ProjectedColumns) == 0 && len(req.Predicates) == 0 {
		return "", false
	}
	if !columnQueryCatalogHasEnabledColumnStore(catalog) {
		for _, column := range req.ProjectedColumns {
			column = strings.TrimSpace(column)
			if column != "" {
				return column, true
			}
		}
		for _, pred := range req.Predicates {
			column := strings.TrimSpace(pred.Column)
			if column != "" {
				return column, true
			}
		}
		return "", false
	}
	declared := catalog.meta.Options.ColumnStore.Columns
	for _, column := range req.ProjectedColumns {
		column = strings.TrimSpace(column)
		if column != "" && !columnStoreColumnDeclared(declared, column) {
			return column, true
		}
	}
	for _, pred := range req.Predicates {
		column := strings.TrimSpace(pred.Column)
		if column != "" && !columnStoreColumnDeclared(declared, column) {
			return column, true
		}
	}
	return "", false
}

func columnStoreColumnDeclared(declared []ColumnStoreColumn, name string) bool {
	// Match the request contract: names remain case-sensitive but surrounding
	// whitespace from pre-alpha fixtures is ignored at the planner boundary.
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, declaredColumn := range declared {
		if strings.TrimSpace(declaredColumn.Name) == name {
			return true
		}
	}
	return false
}

func unsupportedColumnQueryPlan(kind ColumnQueryPlanKind, reason string, diag ColumnQueryPlanDiagnostics) ColumnQueryPlan {
	diag.UnsupportedPlanKind = kind
	diag.UnsupportedPlanReason = reason
	diag.Reason = reason
	return ColumnQueryPlan{Kind: kind, Supported: false, Diagnostics: diag}
}

func columnQueryManifestRecoveryAuthoritative(identity ColumnStoreCacheIdentity, ok bool) bool {
	return ok &&
		identity.ManifestGeneration != 0 &&
		identity.RecoveryAuthoritativeGeneration == identity.ManifestGeneration
}

func columnQueryPlannerCandidateCount(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest) int {
	if req.ForceKind != "" {
		return 1
	}
	count := 1 // row-store fallback
	count += columnQueryBTreeCandidateCount(catalog, req)
	if physicalColumnQuerySupported(catalog, identity, identityOK, req, ColumnQueryPlanSerialColumnScan) {
		count++
	}
	if strings.TrimSpace(req.AggregateMetadataName) != "" &&
		catalogHasColumnAggregateMetadata(catalog, req.AggregateMetadataName) &&
		physicalColumnQuerySupported(catalog, identity, identityOK, req, ColumnQueryPlanAggregateMetadata) {
		count++
	}
	if physicalColumnQuerySupported(catalog, identity, identityOK, req, ColumnQueryPlanParallelColumnScan) &&
		parallelColumnQueryShapeUnsupportedReason(req) == "" {
		count++
	}
	// CandidatePlans reports the request-feasible plans the planner may
	// evaluate, capped by the configured diagnostic budget when present.
	if budget := req.Capabilities.PlannerCandidateBudget; budget > 0 && count > budget {
		return budget
	}
	return count
}

func columnQueryBTreeCandidateCount(catalog *collectionCatalog, req ColumnQueryPlanRequest) int {
	if catalog == nil {
		return 0
	}
	lookup := newColumnQueryIndexLookup(catalog.meta.Indexes)
	var seen map[string]struct{}
	addCandidate := func(column string) {
		column = strings.TrimSpace(column)
		if column == "" {
			return
		}
		idx, ok := lookup.find(column)
		if !ok {
			return
		}
		field := strings.TrimSpace(idx.Field)
		if field == "" {
			return
		}
		if seen == nil {
			seen = make(map[string]struct{}, 4)
		}
		seen[field] = struct{}{}
	}
	for _, candidate := range req.CandidateIndexColumns {
		addCandidate(candidate)
	}
	for _, pred := range req.Predicates {
		if !columnQueryPredicateOperatorEqualityLike(pred.Operator) && !columnQueryPredicateOperatorRangeLike(pred.Operator) {
			continue
		}
		addCandidate(pred.Column)
	}
	return len(seen)
}

func selectColumnQueryBTreeIndex(catalog *collectionCatalog, req ColumnQueryPlanRequest) (IndexDefinition, bool) {
	if catalog == nil {
		return IndexDefinition{}, false
	}
	// Keep common small catalogs allocation-free while avoiding repeated linear
	// scans for larger user-defined index lists.
	lookup := newColumnQueryIndexLookup(catalog.meta.Indexes)
	for _, candidate := range req.CandidateIndexColumns {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if idx, ok := lookup.find(candidate); ok {
			return idx, true
		}
	}
	for _, pred := range req.Predicates {
		if !columnQueryPredicateOperatorEqualityLike(pred.Operator) {
			continue
		}
		candidate := strings.TrimSpace(pred.Column)
		if candidate == "" {
			continue
		}
		if idx, ok := lookup.find(candidate); ok {
			return idx, true
		}
	}
	for _, pred := range req.Predicates {
		if !columnQueryPredicateOperatorRangeLike(pred.Operator) {
			continue
		}
		candidate := strings.TrimSpace(pred.Column)
		if candidate == "" {
			continue
		}
		if idx, ok := lookup.find(candidate); ok {
			return idx, true
		}
	}
	return IndexDefinition{}, false
}

type columnQueryIndexLookup struct {
	indexes []IndexDefinition
	byField map[string]IndexDefinition
}

func newColumnQueryIndexLookup(indexes []IndexDefinition) columnQueryIndexLookup {
	lookup := columnQueryIndexLookup{indexes: indexes}
	if len(indexes) <= 8 {
		return lookup
	}
	lookup.byField = make(map[string]IndexDefinition, len(indexes))
	for _, idx := range indexes {
		field := strings.TrimSpace(idx.Field)
		if field == "" {
			continue
		}
		if _, exists := lookup.byField[field]; !exists {
			lookup.byField[field] = idx
		}
	}
	return lookup
}

func (l columnQueryIndexLookup) find(field string) (IndexDefinition, bool) {
	field = strings.TrimSpace(field)
	if field == "" {
		return IndexDefinition{}, false
	}
	if l.byField != nil {
		idx, ok := l.byField[field]
		return idx, ok
	}
	for _, idx := range l.indexes {
		if strings.TrimSpace(idx.Field) == field {
			return idx, true
		}
	}
	return IndexDefinition{}, false
}

func columnQueryPredicateOperatorEqualityLike(op ColumnQueryPredicateOperator) bool {
	return op == "" || op == ColumnQueryPredicateEqual
}

func columnQueryPredicateOperatorRangeLike(op ColumnQueryPredicateOperator) bool {
	switch op {
	case ColumnQueryPredicateGreaterOrEqual, ColumnQueryPredicateGreaterThan, ColumnQueryPredicateLessOrEqual, ColumnQueryPredicateLessThan:
		return true
	default:
		return false
	}
}

// ColumnSkipScanBound describes one side of a mark-pruning range. When
// Unbounded is true, Key is ignored and should be nil or empty.
type ColumnSkipScanBound struct {
	Key       []byte
	Inclusive bool
	Unbounded bool
}

type ColumnSkipScanPredicate struct {
	// Position is the zero-based sort-key position. PlanColumnSkipScan only uses
	// bounded predicates that form a contiguous left prefix from position zero;
	// predicates beyond the maximum possible prefix length for this predicate
	// set are ignored for pruning rather than allocating sparse scratch.
	Position int
	Lower    ColumnSkipScanBound
	Upper    ColumnSkipScanBound
}

type ColumnSkipScanMark struct {
	Name    string
	Rows    int
	MinKeys [][]byte
	MaxKeys [][]byte
}

// ColumnSkipScanResult contains reusable planner scratch. Do not copy a live
// result between workloads; pass the same pointer back to PlanColumnSkipScanInto
// for reuse or assign a zero value to release retained scratch.
type ColumnSkipScanResult struct {
	LeftPrefixColumns int
	ScheduledMarks    []int
	SkippedMarks      []int
	ScheduledRows     int
	SkippedRows       int

	predicateScratch []ColumnSkipScanPredicate
	predicateSet     []bool
}

func PlanColumnSkipScan(predicates []ColumnSkipScanPredicate, marks []ColumnSkipScanMark) ColumnSkipScanResult {
	var result ColumnSkipScanResult
	PlanColumnSkipScanInto(&result, predicates, marks)
	return result
}

// PlanColumnSkipScanInto reuses result.ScheduledMarks and result.SkippedMarks as
// caller-owned scratch. Assign a zero ColumnSkipScanResult to release retained
// slice capacity between unrelated planning workloads.
//
// Predicate positions are interpreted as dense zero-based sort-key positions
// for the left-prefix contract. Only bounded predicates in the contiguous prefix
// starting at position zero can prune marks. A range predicate can terminate the
// prefix as its final column; predicates after that range are ignored because
// they are not safe left-prefix pruning inputs. Sparse later-column predicates
// are ignored until every lower position exists, positions >= len(predicates)
// are ignored rather than allocating sparse scratch, and duplicate positions use
// the last bounded predicate supplied.
func PlanColumnSkipScanInto(result *ColumnSkipScanResult, predicates []ColumnSkipScanPredicate, marks []ColumnSkipScanMark) {
	if result == nil {
		return
	}
	result.LeftPrefixColumns = 0
	result.ScheduledMarks = result.ScheduledMarks[:0]
	result.SkippedMarks = result.SkippedMarks[:0]
	result.ScheduledRows = 0
	result.SkippedRows = 0
	// A contiguous left prefix can never be longer than the number of predicates,
	// so positions >= len(predicates) cannot extend the prefix. This keeps sparse
	// later-column predicates from allocating position-sized scratch.
	maxLeftPrefixColumns := len(predicates)
	result.predicateScratch = ensureColumnSkipScanPredicateScratch(result.predicateScratch, maxLeftPrefixColumns)
	result.predicateSet = ensureColumnSkipScanBoolScratch(result.predicateSet, maxLeftPrefixColumns)
	prefixPredicates := result.predicateScratch[:0]
	if len(predicates) > 0 {
		prefixPredicates = result.predicateScratch[:maxLeftPrefixColumns]
		predicateSet := result.predicateSet[:maxLeftPrefixColumns]
		result.LeftPrefixColumns = columnSkipScanLeftPrefixPredicates(prefixPredicates, predicateSet, predicates)
		prefixPredicates = prefixPredicates[:result.LeftPrefixColumns]
	}
	for i, mark := range marks {
		if len(prefixPredicates) > 0 && columnSkipScanMarkDisjoint(mark, prefixPredicates) {
			result.SkippedMarks = append(result.SkippedMarks, i)
			result.SkippedRows += mark.Rows
			continue
		}
		result.ScheduledMarks = append(result.ScheduledMarks, i)
		result.ScheduledRows += mark.Rows
	}
}

func ensureColumnSkipScanPredicateScratch(scratch []ColumnSkipScanPredicate, length int) []ColumnSkipScanPredicate {
	if cap(scratch) < length {
		return make([]ColumnSkipScanPredicate, length)
	}
	return scratch[:length]
}

func ensureColumnSkipScanBoolScratch(scratch []bool, length int) []bool {
	if cap(scratch) < length {
		return make([]bool, length)
	}
	return scratch[:length]
}

func columnSkipScanLeftPrefixPredicates(byPosition []ColumnSkipScanPredicate, hasPosition []bool, predicates []ColumnSkipScanPredicate) int {
	clear(hasPosition)
	for _, pred := range predicates {
		if pred.Position < 0 || pred.Position >= len(byPosition) || !columnSkipScanPredicateHasBound(pred) {
			continue
		}
		// Preserve "last predicate wins" for duplicate positions while bounding
		// prefix discovery by dense scratch, not arbitrary sparse positions.
		byPosition[pred.Position] = pred
		hasPosition[pred.Position] = true
	}
	prefix := 0
	for prefix < len(byPosition) {
		if !hasPosition[prefix] {
			break
		}
		pred := byPosition[prefix]
		prefix++
		if !columnSkipScanPredicateIsEquality(pred) {
			break
		}
	}
	return prefix
}

func columnSkipScanPredicateHasBound(pred ColumnSkipScanPredicate) bool {
	return (!pred.Lower.Unbounded && len(pred.Lower.Key) > 0) ||
		(!pred.Upper.Unbounded && len(pred.Upper.Key) > 0)
}

func columnSkipScanPredicateIsEquality(pred ColumnSkipScanPredicate) bool {
	return !pred.Lower.Unbounded &&
		!pred.Upper.Unbounded &&
		pred.Lower.Inclusive &&
		pred.Upper.Inclusive &&
		len(pred.Lower.Key) > 0 &&
		bytes.Equal(pred.Lower.Key, pred.Upper.Key)
}

func columnSkipScanMarkDisjoint(mark ColumnSkipScanMark, prefixPredicates []ColumnSkipScanPredicate) bool {
	// prefixPredicates is dense and ordered by column position
	// [0:LeftPrefixColumns]; missing prefix slots are rejected before this call.
	for pos, pred := range prefixPredicates {
		if pos >= len(mark.MinKeys) || pos >= len(mark.MaxKeys) {
			return false
		}
		minKey := mark.MinKeys[pos]
		maxKey := mark.MaxKeys[pos]
		if !pred.Lower.Unbounded && len(pred.Lower.Key) > 0 {
			cmp := bytes.Compare(maxKey, pred.Lower.Key)
			if cmp < 0 || (cmp == 0 && !pred.Lower.Inclusive) {
				return true
			}
		}
		if !pred.Upper.Unbounded && len(pred.Upper.Key) > 0 {
			cmp := bytes.Compare(minKey, pred.Upper.Key)
			if cmp > 0 || (cmp == 0 && !pred.Upper.Inclusive) {
				return true
			}
		}
		if !columnSkipScanPredicateIsEquality(pred) {
			return false
		}
		if !bytes.Equal(minKey, maxKey) {
			return false
		}
	}
	return false
}
