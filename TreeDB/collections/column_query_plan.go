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
	Column   string
	Operator ColumnQueryPredicateOperator
}

type ColumnQueryPlannerCapabilities struct {
	SerialColumnScan        bool
	AggregateMetadata       bool
	ParallelColumnScan      bool
	PhysicalAssetCount      int
	PartCount               int
	GranuleCount            int
	MaxParallelWorkers      int
	DecodedBlockCacheHits   uint64
	DecodedBlockCacheMisses uint64
	PlannerCandidateBudget  int
}

// ColumnQueryPlanRequest describes one planner decision. Column, index, and
// aggregate metadata names are matched case-sensitively after trimming
// surrounding whitespace; callers should canonicalize catalog names before
// storing them rather than relying on planner-time normalization.
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
		CandidatePlans:          columnQueryPlannerCandidateCount(req),
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

	if req.ForceKind != "" {
		return forcedColumnQueryPlan(catalog, identity, identityOK, req, diag)
	}
	if ok, plan := aggregateColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
		return plan
	}
	if ok, plan := parallelColumnQueryPlan(identity, identityOK, req, diag); ok {
		return plan
	}
	if ok, plan := serialColumnQueryPlan(identity, identityOK, req, diag); ok {
		return plan
	}
	if idx, ok := selectColumnQueryBTreeIndex(catalog, req); ok {
		diag.SelectedIndexName = idx.Name
		diag.SelectedIndexField = idx.Field
		diag.Reason = "selected matching collection secondary index"
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
		diag.Reason = "forced matching collection secondary index"
		return ColumnQueryPlan{Kind: ColumnQueryPlanBTreeIndexBaseline, Supported: true, IndexName: idx.Name, IndexField: idx.Field, Diagnostics: diag}
	case ColumnQueryPlanSerialColumnScan:
		if ok, plan := serialColumnQueryPlan(identity, identityOK, req, diag); ok {
			plan.Diagnostics.Reason = "forced serial physical column scan"
			return plan
		}
		return unsupportedColumnQueryPlan(ColumnQueryPlanSerialColumnScan, physicalColumnQueryUnsupportedReason(identity, identityOK, req, ColumnQueryPlanSerialColumnScan), diag)
	case ColumnQueryPlanAggregateMetadata:
		if ok, plan := aggregateColumnQueryPlan(catalog, identity, identityOK, req, diag); ok {
			plan.Diagnostics.Reason = "forced aggregate metadata plan"
			return plan
		}
		return unsupportedColumnQueryPlan(ColumnQueryPlanAggregateMetadata, aggregateColumnQueryUnsupportedReason(catalog, identity, identityOK, req), diag)
	case ColumnQueryPlanParallelColumnScan:
		if ok, plan := parallelColumnQueryPlan(identity, identityOK, req, diag); ok {
			plan.Diagnostics.Reason = "forced parallel physical column scan"
			return plan
		}
		return unsupportedColumnQueryPlan(ColumnQueryPlanParallelColumnScan, physicalColumnQueryUnsupportedReason(identity, identityOK, req, ColumnQueryPlanParallelColumnScan), diag)
	default:
		return unsupportedColumnQueryPlan(req.ForceKind, fmt.Sprintf("unknown column query plan kind %q", req.ForceKind), diag)
	}
}

func serialColumnQueryPlan(identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, diag ColumnQueryPlanDiagnostics) (bool, ColumnQueryPlan) {
	if !physicalColumnQuerySupported(identity, identityOK, req, ColumnQueryPlanSerialColumnScan) {
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
	if !physicalColumnQuerySupported(identity, identityOK, req, ColumnQueryPlanAggregateMetadata) {
		return false, ColumnQueryPlan{}
	}
	diag.Reason = "selected aggregate metadata"
	diag.ScheduledGranules = 0
	return true, ColumnQueryPlan{Kind: ColumnQueryPlanAggregateMetadata, Supported: true, Diagnostics: diag}
}

func parallelColumnQueryPlan(identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, diag ColumnQueryPlanDiagnostics) (bool, ColumnQueryPlan) {
	if !physicalColumnQuerySupported(identity, identityOK, req, ColumnQueryPlanParallelColumnScan) {
		return false, ColumnQueryPlan{}
	}
	workers := req.Capabilities.MaxParallelWorkers
	if workers <= 1 {
		return false, ColumnQueryPlan{}
	}
	if req.Capabilities.PartCount <= 1 && req.Capabilities.GranuleCount <= workers {
		return false, ColumnQueryPlan{}
	}
	diag.WorkerCount = workers
	diag.ScheduledGranules = req.Capabilities.GranuleCount
	diag.Reason = "selected parallel physical column scan"
	return true, ColumnQueryPlan{Kind: ColumnQueryPlanParallelColumnScan, Supported: true, Diagnostics: diag}
}

func physicalColumnQuerySupported(identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest, kind ColumnQueryPlanKind) bool {
	if !columnQueryManifestRecoveryAuthoritative(identity, identityOK) || req.Capabilities.PhysicalAssetCount <= 0 {
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
	case !columnQueryManifestRecoveryAuthoritative(identity, identityOK):
		return "active column manifest is not recovery-authoritative"
	case req.Capabilities.PhysicalAssetCount <= 0:
		return "no durable physical column assets are available"
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
		if req.Capabilities.MaxParallelWorkers <= 1 {
			return "parallel scan requires more than one worker"
		}
		if req.Capabilities.PartCount <= 1 && req.Capabilities.GranuleCount <= req.Capabilities.MaxParallelWorkers {
			return "parallel scan requires more than one part or more granules than workers"
		}
	}
	return "physical column plan is not supported"
}

func aggregateColumnQueryUnsupportedReason(catalog *collectionCatalog, identity ColumnStoreCacheIdentity, identityOK bool, req ColumnQueryPlanRequest) string {
	name := strings.TrimSpace(req.AggregateMetadataName)
	if name == "" {
		return "query did not request aggregate metadata"
	}
	if !catalogHasColumnAggregateMetadata(catalog, name) {
		return fmt.Sprintf("unknown aggregate metadata %q", name)
	}
	return physicalColumnQueryUnsupportedReason(identity, identityOK, req, ColumnQueryPlanAggregateMetadata)
}

func catalogHasColumnAggregateMetadata(catalog *collectionCatalog, name string) bool {
	name = strings.TrimSpace(name)
	if catalog == nil || catalog.meta.Options.ColumnStore == nil || name == "" {
		return false
	}
	for _, aggregate := range catalog.meta.Options.ColumnStore.AggregateMetadata {
		if strings.TrimSpace(aggregate.Name) == name {
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
		identity.ManifestRoot != 0 &&
		identity.ManifestGeneration != 0 &&
		identity.RecoveryAuthoritativeGeneration == identity.ManifestGeneration
}

func columnQueryPlannerCandidateCount(req ColumnQueryPlanRequest) int {
	if req.ForceKind != "" {
		return 1
	}
	if req.Capabilities.PlannerCandidateBudget > 0 {
		return req.Capabilities.PlannerCandidateBudget
	}
	count := 1 // row-store fallback
	count += len(req.CandidateIndexColumns)
	if req.Capabilities.SerialColumnScan {
		count++
	}
	if req.Capabilities.AggregateMetadata {
		count++
	}
	if req.Capabilities.ParallelColumnScan {
		count++
	}
	return count
}

func selectColumnQueryBTreeIndex(catalog *collectionCatalog, req ColumnQueryPlanRequest) (IndexDefinition, bool) {
	if catalog == nil {
		return IndexDefinition{}, false
	}
	for _, candidate := range req.CandidateIndexColumns {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		for _, idx := range catalog.meta.Indexes {
			if strings.TrimSpace(idx.Field) == candidate {
				return idx, true
			}
		}
	}
	for _, pred := range req.Predicates {
		candidate := strings.TrimSpace(pred.Column)
		if candidate == "" {
			continue
		}
		for _, idx := range catalog.meta.Indexes {
			if strings.TrimSpace(idx.Field) == candidate {
				return idx, true
			}
		}
	}
	return IndexDefinition{}, false
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
	for i := range byPosition {
		byPosition[i] = ColumnSkipScanPredicate{}
	}
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
