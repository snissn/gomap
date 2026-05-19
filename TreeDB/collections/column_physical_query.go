package collections

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ColumnPhysicalQueryKind names the small M13B physical aggregate/projection
// shapes supported directly by the column asset scanner.
type ColumnPhysicalQueryKind string

const (
	// ColumnPhysicalQueryGroupCount counts rows by a string group column.
	ColumnPhysicalQueryGroupCount         ColumnPhysicalQueryKind = "group_count"
	ColumnPhysicalQueryGroupCountDistinct ColumnPhysicalQueryKind = "group_count_distinct"
	ColumnPhysicalQueryHourCount          ColumnPhysicalQueryKind = "hour_count"
	ColumnPhysicalQueryGroupMinInt64      ColumnPhysicalQueryKind = "group_min_int64"
	ColumnPhysicalQueryGroupMaxInt64      ColumnPhysicalQueryKind = "group_max_int64"
	ColumnPhysicalQueryGroupInt64Span     ColumnPhysicalQueryKind = "group_int64_span"
)

const columnPhysicalQueryHourUS = int64(3_600_000_000)

// ColumnPhysicalQueryRequest describes one explicit physical column query. It
// does not invoke planner routing; M14 owns forced/automatic route selection.
type ColumnPhysicalQueryRequest struct {
	Kind           ColumnPhysicalQueryKind
	GroupColumn    string
	ValueColumn    string
	DistinctColumn string
}

// ColumnPhysicalQueryGroup is one reduced result row. Count is populated for
// count-style queries; Int64 is populated for min/max/span-style queries.
type ColumnPhysicalQueryGroup struct {
	Key   string
	Count int
	Int64 int64
}

// ColumnPhysicalQueryDiagnostics reports scan and reduce work for a physical
// query without counting full-document row materialization.
type ColumnPhysicalQueryDiagnostics struct {
	ManifestRoot               uint64
	ManifestGeneration         uint64
	RecoveryManifestGeneration uint64
	AppliedCommandLSN          uint64
	ManifestRecords            int
	AssetRefs                  int
	MutationParts              int
	DecodedBlocks              int
	ScheduledGranules          int
	SkippedGranules            int
	RowsScanned                int
	DeletedRows                int
	ProjectedColumns           int
	RowMaterializations        int
	PhysicalBytesScanned       int64
	ReduceRows                 int
	VisibilityRows             int
	ReconstructionRows         int
	ResultGroups               int
	ScanNanos                  int64
	VisibilityNanos            int64
	ReduceNanos                int64
	ReconstructionNanos        int64
}

// ColumnPhysicalQueryResult is the reduced result and diagnostics from an
// explicit physical column query.
type ColumnPhysicalQueryResult struct {
	Groups      []ColumnPhysicalQueryGroup
	Diagnostics ColumnPhysicalQueryDiagnostics
}

// RunColumnPhysicalQuery executes an explicit serial physical column query over
// the recovery-authoritative manifest. Insert-only manifests use the direct
// scanner path; mutation-bearing manifests fall back to the M13C visibility
// overlay before reducing.
func (c *Collection) RunColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	cfg, err := c.columnPhysicalQueryColumnStoreConfig()
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	if cfg.PhysicalMutationParts > 0 {
		return c.runColumnPhysicalQueryWithVisibility(cfg, req)
	}
	exec, err := newColumnPhysicalQueryExecutor(cfg, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	scanStart := time.Now()
	diag, err := c.scanColumnPhysicalRows(columnPhysicalScanRequest{
		ProjectedColumns:  exec.projected,
		Visitor:           exec.visit,
		RequireInsertOnly: true,
	})
	scanNanos := time.Since(scanStart).Nanoseconds()
	result := ColumnPhysicalQueryResult{
		Diagnostics: columnPhysicalQueryDiagnosticsFromScan(diag),
	}
	result.Diagnostics.ScanNanos = scanNanos
	result.Diagnostics.ReduceRows = exec.reduceRows
	if err != nil {
		if errors.Is(err, errColumnPhysicalQueryNeedsVisibility) {
			return c.runColumnPhysicalQueryWithVisibility(cfg, req)
		}
		return result, err
	}
	result.Groups = exec.groups()
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, nil
}

// RunColumnPhysicalQueryParallel executes an insert-only physical query by
// partitioning immutable manifest refs across worker-local serial scanners.
// Mutation-bearing manifests stay fail-closed until partitioned visibility
// reconstruction is available.
func (c *Collection) RunColumnPhysicalQueryParallel(req ColumnPhysicalQueryRequest, maxWorkers int) (ColumnPhysicalQueryResult, error) {
	if maxWorkers <= 1 {
		return c.RunColumnPhysicalQuery(req)
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotView()
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	if view.MutationParts > 0 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: parallel physical column query requires insert-only manifest until partitioned visibility execution lands", ErrColumnQueryPlanUnsupported)
	}
	cfg := view.Config
	merged, err := newColumnPhysicalQueryExecutor(cfg, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}

	type workerResult struct {
		exec *columnPhysicalQueryExecutor
		diag columnPhysicalScanDiagnostics
		err  error
	}
	results := make([]workerResult, maxWorkers)
	start := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < maxWorkers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec, err := newColumnPhysicalQueryExecutor(cfg, req)
			if err != nil {
				results[worker].err = err
				return
			}
			diag, err := c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
				ProjectedColumns:    exec.projected,
				Visitor:             exec.visit,
				RequireInsertOnly:   true,
				RefOrdinalModulo:    maxWorkers,
				RefOrdinalRemainder: worker,
			})
			results[worker] = workerResult{exec: exec, diag: diag, err: err}
		}()
	}
	wg.Wait()

	result := ColumnPhysicalQueryResult{}
	for worker := range results {
		workerResult := results[worker]
		result.Diagnostics = mergeColumnPhysicalQueryDiagnostics(result.Diagnostics, columnPhysicalQueryDiagnosticsFromScan(workerResult.diag))
		if workerResult.err != nil {
			result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
			return result, workerResult.err
		}
		if err := merged.mergeFrom(workerResult.exec); err != nil {
			result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
			return result, err
		}
	}
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	result.Groups = merged.groups()
	result.Diagnostics.ReduceRows = merged.reduceRows
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, nil
}

var errColumnPhysicalQueryNeedsVisibility = errors.New("collections: physical column query requires mutation visibility overlay")

func (c *Collection) runColumnPhysicalQueryWithVisibility(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	exec, err := newColumnPhysicalQueryExecutor(cfg, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	visibilityStart := time.Now()
	visible, err := c.scanColumnPhysicalVisibleRows(exec.projected)
	visibilityNanos := time.Since(visibilityStart).Nanoseconds()
	result := ColumnPhysicalQueryResult{
		Diagnostics: columnPhysicalQueryDiagnosticsFromScan(visible.Diagnostics),
	}
	result.Diagnostics.VisibilityRows = len(visible.Rows)
	result.Diagnostics.ScanNanos = visibilityNanos
	result.Diagnostics.VisibilityNanos = visibilityNanos
	if err != nil {
		return result, err
	}
	reduceStart := time.Now()
	for _, row := range visible.Rows {
		if row.Deleted {
			continue
		}
		if err := exec.visitValues(row.Values); err != nil {
			return result, err
		}
	}
	result.Diagnostics.ReduceNanos = time.Since(reduceStart).Nanoseconds()
	result.Groups = exec.groups()
	result.Diagnostics.ReduceRows = exec.reduceRows
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, nil
}

func (c *Collection) columnPhysicalQueryColumnStoreConfig() (ColumnStoreConfig, error) {
	if c == nil {
		return ColumnStoreConfig{}, errCollectionNil
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	if c.catalog == nil {
		return ColumnStoreConfig{}, errCollectionNotFound
	}
	cfg := c.catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled {
		return ColumnStoreConfig{}, fmt.Errorf("%w: physical column query requires enabled column_store", ErrColumnQueryPlanUnsupported)
	}
	return cfg.copy(), nil
}

func columnPhysicalQueryDiagnosticsFromScan(diag columnPhysicalScanDiagnostics) ColumnPhysicalQueryDiagnostics {
	return ColumnPhysicalQueryDiagnostics{
		ManifestRoot:               diag.ManifestRoot,
		ManifestGeneration:         diag.ManifestGeneration,
		RecoveryManifestGeneration: diag.RecoveryManifestGeneration,
		AppliedCommandLSN:          diag.AppliedCommandLSN,
		ManifestRecords:            diag.ManifestRecords,
		AssetRefs:                  diag.AssetRefs,
		MutationParts:              diag.MutationParts,
		DecodedBlocks:              diag.DecodedBlocks,
		ScheduledGranules:          diag.ScheduledGranules,
		SkippedGranules:            diag.SkippedGranules,
		RowsScanned:                diag.RowsScanned,
		DeletedRows:                diag.DeletedRows,
		ProjectedColumns:           diag.ProjectedColumns,
		RowMaterializations:        diag.RowMaterializations,
		PhysicalBytesScanned:       diag.PhysicalBytesScanned,
	}
}

func mergeColumnPhysicalQueryDiagnostics(left, right ColumnPhysicalQueryDiagnostics) ColumnPhysicalQueryDiagnostics {
	if left.ManifestRoot == 0 {
		left.ManifestRoot = right.ManifestRoot
		left.ManifestGeneration = right.ManifestGeneration
		left.RecoveryManifestGeneration = right.RecoveryManifestGeneration
		left.AppliedCommandLSN = right.AppliedCommandLSN
		left.ManifestRecords = right.ManifestRecords
		left.AssetRefs = right.AssetRefs
		left.ProjectedColumns = right.ProjectedColumns
	}
	if right.ManifestRecords > left.ManifestRecords {
		left.ManifestRecords = right.ManifestRecords
	}
	if right.AssetRefs > left.AssetRefs {
		left.AssetRefs = right.AssetRefs
	}
	if right.ProjectedColumns > left.ProjectedColumns {
		left.ProjectedColumns = right.ProjectedColumns
	}
	left.MutationParts += right.MutationParts
	left.DecodedBlocks += right.DecodedBlocks
	left.ScheduledGranules += right.ScheduledGranules
	left.SkippedGranules += right.SkippedGranules
	left.RowsScanned += right.RowsScanned
	left.DeletedRows += right.DeletedRows
	left.RowMaterializations += right.RowMaterializations
	left.PhysicalBytesScanned += right.PhysicalBytesScanned
	left.ReduceRows += right.ReduceRows
	left.VisibilityRows += right.VisibilityRows
	left.ReconstructionRows += right.ReconstructionRows
	return left
}

type columnPhysicalQueryExecutor struct {
	kind        ColumnPhysicalQueryKind
	projected   []string
	groupIdx    int
	valueIdx    int
	distinctIdx int
	interner    columnPhysicalQueryStringInterner

	counts       map[string]int
	distinct     map[string]map[string]struct{}
	hourCounts   [24]int
	int64Values  map[string]int64
	int64Spans   map[string]columnPhysicalQuerySpan
	reduceRows   int
	resultGroups []ColumnPhysicalQueryGroup
}

type columnPhysicalQuerySpan struct {
	min int64
	max int64
}

func newColumnPhysicalQueryExecutor(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (*columnPhysicalQueryExecutor, error) {
	exec := &columnPhysicalQueryExecutor{
		kind:        req.Kind,
		groupIdx:    -1,
		valueIdx:    -1,
		distinctIdx: -1,
	}
	addProjection := func(name string, wantType ColumnStoreValueType, role string) (int, error) {
		if name == "" {
			return -1, fmt.Errorf("%w: physical column query %s column is required", ErrColumnQueryPlanUnsupported, role)
		}
		col, ok := columnPhysicalQueryDeclaredColumn(cfg, name)
		if !ok {
			return -1, fmt.Errorf("%w: physical column query requested undeclared column %q", ErrColumnQueryPlanUnsupported, name)
		}
		if col.ValueType != wantType {
			return -1, fmt.Errorf("%w: physical column query %s column %q has type %q, want %q", ErrColumnQueryPlanUnsupported, role, name, col.ValueType, wantType)
		}
		for idx, existing := range exec.projected {
			if existing == name {
				return idx, nil
			}
		}
		exec.projected = append(exec.projected, name)
		return len(exec.projected) - 1, nil
	}

	var err error
	switch req.Kind {
	case ColumnPhysicalQueryGroupCount:
		exec.groupIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		exec.counts = make(map[string]int)
	case ColumnPhysicalQueryGroupCountDistinct:
		exec.groupIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		if err == nil {
			exec.distinctIdx, err = addProjection(req.DistinctColumn, ColumnStoreValueString, "distinct")
		}
		exec.distinct = make(map[string]map[string]struct{})
	case ColumnPhysicalQueryHourCount:
		exec.valueIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		exec.groupIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		if err == nil {
			exec.valueIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
		}
		exec.int64Values = make(map[string]int64)
	case ColumnPhysicalQueryGroupInt64Span:
		exec.groupIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		if err == nil {
			exec.valueIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
		}
		exec.int64Spans = make(map[string]columnPhysicalQuerySpan)
	default:
		err = fmt.Errorf("%w: unsupported physical column query kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
	if err != nil {
		return nil, err
	}
	return exec, nil
}

func columnPhysicalQueryDeclaredColumn(cfg ColumnStoreConfig, name string) (ColumnStoreColumn, bool) {
	for _, col := range cfg.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnStoreColumn{}, false
}

func (e *columnPhysicalQueryExecutor) visit(row columnPhysicalScanRowView) error {
	if row.Deleted || row.Operation != ColumnPublishOperationInsert {
		return fmt.Errorf("%w: operation=%s deleted=%v", errColumnPhysicalQueryNeedsVisibility, row.Operation, row.Deleted)
	}
	return e.visitValues(row.Values)
}

func (e *columnPhysicalQueryExecutor) visitValues(values []columnDeclaredValue) error {
	e.reduceRows++
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		key, err := e.stringKey(values[e.groupIdx])
		if err != nil {
			return err
		}
		e.counts[key]++
	case ColumnPhysicalQueryGroupCountDistinct:
		key, err := e.stringKey(values[e.groupIdx])
		if err != nil {
			return err
		}
		distinct, err := e.stringKey(values[e.distinctIdx])
		if err != nil {
			return err
		}
		set := e.distinct[key]
		if set == nil {
			set = make(map[string]struct{})
			e.distinct[key] = set
		}
		set[distinct] = struct{}{}
	case ColumnPhysicalQueryHourCount:
		value, err := columnPhysicalQueryInt64Value(values[e.valueIdx])
		if err != nil {
			return err
		}
		e.hourCounts[columnPhysicalQueryUTCHour(value)]++
	case ColumnPhysicalQueryGroupMinInt64:
		key, value, err := e.stringInt64Values(values)
		if err != nil {
			return err
		}
		if cur, ok := e.int64Values[key]; !ok || value < cur {
			e.int64Values[key] = value
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		key, value, err := e.stringInt64Values(values)
		if err != nil {
			return err
		}
		if cur, ok := e.int64Values[key]; !ok || value > cur {
			e.int64Values[key] = value
		}
	case ColumnPhysicalQueryGroupInt64Span:
		key, value, err := e.stringInt64Values(values)
		if err != nil {
			return err
		}
		cur, ok := e.int64Spans[key]
		if !ok {
			e.int64Spans[key] = columnPhysicalQuerySpan{min: value, max: value}
			return nil
		}
		if value < cur.min {
			cur.min = value
		}
		if value > cur.max {
			cur.max = value
		}
		e.int64Spans[key] = cur
	}
	return nil
}

func (e *columnPhysicalQueryExecutor) stringInt64Values(values []columnDeclaredValue) (string, int64, error) {
	key, err := e.stringKey(values[e.groupIdx])
	if err != nil {
		return "", 0, err
	}
	value, err := columnPhysicalQueryInt64Value(values[e.valueIdx])
	if err != nil {
		return "", 0, err
	}
	return key, value, nil
}

func (e *columnPhysicalQueryExecutor) stringKey(value columnDeclaredValue) (string, error) {
	if value.Type != ColumnStoreValueString {
		return "", fmt.Errorf("%w: physical column query expected string value, got %q", ErrColumnQueryPlanUnsupported, value.Type)
	}
	if value.Null {
		return "", fmt.Errorf("%w: physical column query does not support null string group values yet", ErrColumnQueryPlanUnsupported)
	}
	if value.StringBytes != nil {
		return e.interner.internBytes(value.StringBytes), nil
	}
	return e.interner.internString(value.String), nil
}

func (e *columnPhysicalQueryExecutor) groups() []ColumnPhysicalQueryGroup {
	e.resultGroups = e.resultGroups[:0]
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		for key, count := range e.counts {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Count: count})
		}
	case ColumnPhysicalQueryGroupCountDistinct:
		for key, set := range e.distinct {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Count: len(set)})
		}
	case ColumnPhysicalQueryHourCount:
		for hour, count := range e.hourCounts {
			if count == 0 {
				continue
			}
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: columnPhysicalQueryHourKey(hour), Count: count})
		}
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range e.int64Values {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: value})
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range e.int64Spans {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
		}
	}
	sort.Slice(e.resultGroups, func(i, j int) bool {
		return e.resultGroups[i].Key < e.resultGroups[j].Key
	})
	return e.resultGroups
}

func (e *columnPhysicalQueryExecutor) mergeFrom(other *columnPhysicalQueryExecutor) error {
	if e == nil || other == nil {
		return errors.New("collections: cannot merge nil physical column query executor")
	}
	if e.kind != other.kind {
		return fmt.Errorf("collections: cannot merge physical column query kind %q into %q", other.kind, e.kind)
	}
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		for key, count := range other.counts {
			e.counts[key] += count
		}
	case ColumnPhysicalQueryGroupCountDistinct:
		for key, otherSet := range other.distinct {
			set := e.distinct[key]
			if set == nil {
				set = make(map[string]struct{}, len(otherSet))
				e.distinct[key] = set
			}
			for value := range otherSet {
				set[value] = struct{}{}
			}
		}
	case ColumnPhysicalQueryHourCount:
		for hour, count := range other.hourCounts {
			e.hourCounts[hour] += count
		}
	case ColumnPhysicalQueryGroupMinInt64:
		for key, value := range other.int64Values {
			if cur, ok := e.int64Values[key]; !ok || value < cur {
				e.int64Values[key] = value
			}
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range other.int64Values {
			if cur, ok := e.int64Values[key]; !ok || value > cur {
				e.int64Values[key] = value
			}
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range other.int64Spans {
			cur, ok := e.int64Spans[key]
			if !ok {
				e.int64Spans[key] = span
				continue
			}
			if span.min < cur.min {
				cur.min = span.min
			}
			if span.max > cur.max {
				cur.max = span.max
			}
			e.int64Spans[key] = cur
		}
	default:
		return fmt.Errorf("%w: unsupported physical column query kind %q", ErrColumnQueryPlanUnsupported, e.kind)
	}
	e.reduceRows += other.reduceRows
	return nil
}

func columnPhysicalQueryInt64Value(value columnDeclaredValue) (int64, error) {
	if value.Type != ColumnStoreValueInt64 {
		return 0, fmt.Errorf("%w: physical column query expected int64 value, got %q", ErrColumnQueryPlanUnsupported, value.Type)
	}
	if value.Null {
		return 0, fmt.Errorf("%w: physical column query does not support null int64 values yet", ErrColumnQueryPlanUnsupported)
	}
	return value.Int64, nil
}

func columnPhysicalQueryUTCHour(timeUS int64) int {
	hours := timeUS / columnPhysicalQueryHourUS
	if timeUS < 0 && timeUS%columnPhysicalQueryHourUS != 0 {
		hours--
	}
	hour := int(hours % 24)
	if hour < 0 {
		hour += 24
	}
	return hour
}

func columnPhysicalQueryHourKey(hour int) string {
	if hour < 0 || hour >= 24 {
		return "hour_invalid"
	}
	return [...]string{
		"hour_00", "hour_01", "hour_02", "hour_03", "hour_04", "hour_05",
		"hour_06", "hour_07", "hour_08", "hour_09", "hour_10", "hour_11",
		"hour_12", "hour_13", "hour_14", "hour_15", "hour_16", "hour_17",
		"hour_18", "hour_19", "hour_20", "hour_21", "hour_22", "hour_23",
	}[hour]
}

type columnPhysicalQueryStringInterner struct {
	buckets map[uint64][]columnPhysicalQueryStringEntry
}

type columnPhysicalQueryStringEntry struct {
	key string
}

func (i *columnPhysicalQueryStringInterner) internBytes(raw []byte) string {
	if i.buckets == nil {
		i.buckets = make(map[uint64][]columnPhysicalQueryStringEntry, 16)
	}
	hash := columnPhysicalQueryHashBytes(raw)
	bucket := i.buckets[hash]
	for _, entry := range bucket {
		if columnPhysicalQueryStringEqualBytes(entry.key, raw) {
			return entry.key
		}
	}
	key := string(raw)
	i.buckets[hash] = append(bucket, columnPhysicalQueryStringEntry{key: key})
	return key
}

func (i *columnPhysicalQueryStringInterner) internString(raw string) string {
	if i.buckets == nil {
		i.buckets = make(map[uint64][]columnPhysicalQueryStringEntry, 16)
	}
	hash := columnPhysicalQueryHashString(raw)
	bucket := i.buckets[hash]
	for _, entry := range bucket {
		if entry.key == raw {
			return entry.key
		}
	}
	i.buckets[hash] = append(bucket, columnPhysicalQueryStringEntry{key: raw})
	return raw
}

func columnPhysicalQueryHashBytes(raw []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, b := range raw {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

func columnPhysicalQueryHashString(raw string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := 0; i < len(raw); i++ {
		hash ^= uint64(raw[i])
		hash *= prime64
	}
	return hash
}

func columnPhysicalQueryStringEqualBytes(s string, raw []byte) bool {
	if len(s) != len(raw) {
		return false
	}
	for i := range raw {
		if s[i] != raw[i] {
			return false
		}
	}
	return true
}
