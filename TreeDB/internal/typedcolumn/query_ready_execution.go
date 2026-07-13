package typedcolumn

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"
)

// QueryReadyOperatorKind names the shared encoded reductions used by the
// JSONBench q1-q5 and qexpr production cells. These are physical operations,
// not query names or persisted precomputed answers.
type QueryReadyOperatorKind string

const (
	QueryReadyOperatorGroupCount            QueryReadyOperatorKind = "group_count"
	QueryReadyOperatorGroupCountAndDistinct QueryReadyOperatorKind = "group_count_and_distinct"
	QueryReadyOperatorGroupHourCount        QueryReadyOperatorKind = "group_hour_count"
	QueryReadyOperatorGroupMinInt64         QueryReadyOperatorKind = "group_min_int64"
	QueryReadyOperatorGroupInt64Span        QueryReadyOperatorKind = "group_int64_span"
	QueryReadyOperatorSumSecondOfDaySquare  QueryReadyOperatorKind = "sum_second_of_day_square"
)

type QueryReadyOperatorOrder string

const (
	QueryReadyOrderInt64Asc  QueryReadyOperatorOrder = "int64_asc"
	QueryReadyOrderInt64Desc QueryReadyOperatorOrder = "int64_desc"
)

// QueryReadyStringPredicate is an equality/IN predicate. One value is
// equality; multiple values are an IN set. Values are translated once into a
// stable execution domain while every part retains its local on-disk codes.
type QueryReadyStringPredicate struct {
	Column string
	Values []string
}

type QueryReadyOperatorRequest struct {
	Kind              QueryReadyOperatorKind
	GroupColumn       string
	ValueColumn       string
	DistinctColumn    string
	Predicates        []QueryReadyStringPredicate
	TopK              int
	Order             QueryReadyOperatorOrder
	SkipEmptyGroupKey bool
}

type QueryReadyOperatorGroup struct {
	Key           string
	Hour          int
	Count         int
	DistinctCount int
	Int64         int64
}

// QueryReadyExecutionStats makes path selection and work placement observable.
// Fallback/materialization counters are deliberately explicit zero-valued
// guardrails for callers and benchmark reports.
type QueryReadyExecutionStats struct {
	EncodedBaseDeltaExecutions int
	PreparedParts              int
	BaseParts                  int
	DeltaParts                 int
	RowsCandidate              int
	RowsVisible                int
	RowsSuperseded             int
	RowsDeleted                int
	RowsScanned                int
	BaseRowsScanned            int
	DeltaRowsScanned           int
	RowsMatched                int
	GroupsConsidered           int
	GroupsReturned             int
	Predicates                 int
	DictionaryDomains          int
	CodeTranslations           int
	DecodedBlocks              int
	DecodedBytes               int64
	ScratchBytes               int64
	PreparationNanos           int64
	BaseScanNanos              int64
	DeltaMergeNanos            int64
	PredicateNanos             int64
	GroupingNanos              int64
	OrderingTopKNanos          int64
	MaterializationNanos       int64
	DocumentMaterializations   int
	LegacyScanFallbacks        int
	PrecomputedAnswers         int
	Fallbacks                  int
}

type QueryReadyOperatorResult struct {
	Groups []QueryReadyOperatorGroup
	Stats  QueryReadyExecutionStats
}

type queryReadyExecutionDomain struct {
	values      []string
	byPart      [][]int
	emptyGlobal int
}

type queryReadyExecutionPredicate struct {
	projected int
	domain    queryReadyExecutionDomain
	allowed   []bool
}

type queryReadyExecutionPart struct {
	part        *ColumnPart
	scanner     *ColumnPartScanner
	role        PartRole
	visible     partSetVisibleRows
	values      [][]int64
	nulls       [][]bool
	defaults    [][]bool
	decodedRows int
}

// QueryReadyOperator is immutable in plan/domain state and reuses bounded
// runner-owned scan/reduction scratch. It is not safe for concurrent use.
type QueryReadyOperator struct {
	request           QueryReadyOperatorRequest
	reader            *QueryReadyBaseDeltaReader
	projected         []string
	projectedByName   map[string]int
	parts             []queryReadyExecutionPart
	domains           map[string]queryReadyExecutionDomain
	predicates        []queryReadyExecutionPredicate
	groupProjected    int
	valueProjected    int
	distinctProjected int
	groupDomain       queryReadyExecutionDomain
	distinctDomain    queryReadyExecutionDomain
	prepareNanos      int64
	counts            []int
	distinctBits      []uint64
	hourCounts        []int
	int64Values       []int64
	int64Max          []int64
	seen              []bool
	resultGroups      []QueryReadyOperatorGroup
}

const queryReadyMaxGroupDistinctCells = 64 << 20

// PrepareQueryReadyOperator builds query-shaped domains and bounded scratch
// from an already-open snapshot-correct base-plus-delta reader. It never reads
// documents, invokes a legacy collection scan, or persists a final answer.
func PrepareQueryReadyOperator(reader *QueryReadyBaseDeltaReader, request QueryReadyOperatorRequest) (*QueryReadyOperator, error) {
	started := time.Now()
	if reader == nil || reader.reader == nil {
		return nil, errors.New("typedcolumn: query-ready operator requires base-plus-delta reader")
	}
	if err := validateQueryReadyOperatorRequest(request); err != nil {
		return nil, err
	}
	runner := &QueryReadyOperator{
		request: request, reader: reader,
		projectedByName: make(map[string]int), domains: make(map[string]queryReadyExecutionDomain),
		groupProjected: -1, valueProjected: -1, distinctProjected: -1,
	}
	addProjection := func(column string) int {
		if column == "" {
			return -1
		}
		if index, ok := runner.projectedByName[column]; ok {
			return index
		}
		index := len(runner.projected)
		runner.projected = append(runner.projected, column)
		runner.projectedByName[column] = index
		return index
	}
	runner.groupProjected = addProjection(request.GroupColumn)
	runner.valueProjected = addProjection(request.ValueColumn)
	runner.distinctProjected = addProjection(request.DistinctColumn)
	for _, predicate := range request.Predicates {
		addProjection(predicate.Column)
	}

	stringColumns := make(map[string]struct{})
	if request.GroupColumn != "" {
		stringColumns[request.GroupColumn] = struct{}{}
	}
	if request.DistinctColumn != "" {
		stringColumns[request.DistinctColumn] = struct{}{}
	}
	for _, predicate := range request.Predicates {
		stringColumns[predicate.Column] = struct{}{}
	}
	for column := range stringColumns {
		domain, err := buildQueryReadyExecutionDomain(reader, column)
		if err != nil {
			return nil, err
		}
		runner.domains[column] = domain
	}
	for _, predicate := range request.Predicates {
		domain := runner.domains[predicate.Column]
		allowed := make([]bool, len(domain.values))
		for _, value := range predicate.Values {
			index, ok := slices.BinarySearch(domain.values, value)
			if ok {
				allowed[index] = true
			}
		}
		runner.predicates = append(runner.predicates, queryReadyExecutionPredicate{projected: runner.projectedByName[predicate.Column], domain: domain, allowed: allowed})
	}
	if request.GroupColumn != "" {
		runner.groupDomain = runner.domains[request.GroupColumn]
	}
	if request.DistinctColumn != "" {
		runner.distinctDomain = runner.domains[request.DistinctColumn]
	}
	for partIndex, loaded := range reader.reader.parts {
		for _, column := range runner.projected {
			partColumn, ok := loaded.Part.Columns[column]
			if !ok {
				return nil, fmt.Errorf("typedcolumn: query-ready operator part %d missing column %s", loaded.Part.Descriptor.PartID, column)
			}
			_, stringColumn := stringColumns[column]
			if stringColumn && (partColumn.Definition.Type != ColumnTypeLowCardinalityCode || (partColumn.Definition.Encoding != EncodingLowCardinalityUint32 && partColumn.Definition.Encoding != EncodingNullableInt64)) {
				return nil, fmt.Errorf("typedcolumn: query-ready operator column %s type=%s encoding=%s want low-cardinality code", column, partColumn.Definition.Type, partColumn.Definition.Encoding)
			}
			if column == request.ValueColumn && (partColumn.Definition.Type != ColumnTypeInt64 || partColumn.Definition.Encoding == EncodingNullableInt64) {
				return nil, fmt.Errorf("typedcolumn: query-ready operator value column %s type=%s encoding=%s want non-null int64", column, partColumn.Definition.Type, partColumn.Definition.Encoding)
			}
		}
		runner.parts = append(runner.parts, queryReadyExecutionPart{
			part: loaded.Part, scanner: loaded.Part.NewScanner(), role: loaded.Ref.Role, visible: reader.reader.visibleRowsForPart(partIndex), values: make([][]int64, len(runner.projected)), nulls: make([][]bool, len(runner.projected)), defaults: make([][]bool, len(runner.projected)),
		})
	}
	groups := len(runner.groupDomain.values)
	switch request.Kind {
	case QueryReadyOperatorGroupCount:
		runner.counts = make([]int, groups)
	case QueryReadyOperatorGroupCountAndDistinct:
		distinct := len(runner.distinctDomain.values)
		if groups > 0 && distinct > queryReadyMaxGroupDistinctCells/groups {
			return nil, fmt.Errorf("typedcolumn: query-ready group-distinct dimensions=%dx%d exceed cell bound=%d", groups, distinct, queryReadyMaxGroupDistinctCells)
		}
		cells := groups * distinct
		runner.counts = make([]int, groups)
		runner.distinctBits = make([]uint64, (cells+63)/64)
	case QueryReadyOperatorGroupHourCount:
		runner.hourCounts = make([]int, groups*24)
	case QueryReadyOperatorGroupMinInt64:
		runner.int64Values, runner.seen = make([]int64, groups), make([]bool, groups)
	case QueryReadyOperatorGroupInt64Span:
		runner.int64Values, runner.int64Max, runner.seen = make([]int64, groups), make([]int64, groups), make([]bool, groups)
	case QueryReadyOperatorSumSecondOfDaySquare:
	}
	resultCapacity := groups
	if request.Kind == QueryReadyOperatorGroupHourCount {
		resultCapacity *= 24
	}
	if request.Kind == QueryReadyOperatorSumSecondOfDaySquare {
		resultCapacity = 1
	}
	runner.resultGroups = make([]QueryReadyOperatorGroup, 0, resultCapacity)
	runner.prepareNanos = time.Since(started).Nanoseconds()
	return runner, nil
}

// PrepareOperator binds one operator to the immutable file-backed generation
// selected by Open. Decoding here is query preparation, never open/warmup work;
// payload values remain encoded and are scanned by the operator on Run.
func (p *QueryReadyPreparedGeneration) PrepareOperator(request QueryReadyOperatorRequest) (*QueryReadyOperator, error) {
	started := time.Now()
	if p == nil || p.Closed() {
		return nil, errors.New("typedcolumn: query-ready prepared generation is closed")
	}
	refs := make([]PartRef, 0, len(p.parts))
	dictionaries := make([]map[string]map[int64]string, 0, len(p.parts))
	stats := QueryReadyBaseDeltaStats{TotalParts: len(p.parts), TombstonesApplied: len(p.tombstones)}
	for index, preparedPart := range p.parts {
		part, err := ColumnPartFromImageWithOptions(preparedPart.View.Image, ColumnPartImageReadOptions{
			IncludeRowLocators:  true,
			ValidateRowLocators: true,
		})
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: prepare query-ready operator part[%d]: %w", index, err)
		}
		refs = append(refs, PartRef{Role: preparedPart.Role, GenerationID: preparedPart.Generation, Part: part})
		decoded, err := preparedPart.View.Image.Dictionaries()
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: prepare query-ready operator dictionaries part[%d]: %w", index, err)
		}
		inverse := make(map[string]map[int64]string, len(decoded))
		for column, values := range decoded {
			byCode := make(map[int64]string, len(values))
			for value, code := range values {
				byCode[code] = value
			}
			inverse[column] = byCode
			stats.LocalDictionaryDecodes++
		}
		dictionaries = append(dictionaries, inverse)
		stats.PartsDecoded++
		stats.RowsMerged += preparedPart.View.Dependency.Rows
		stats.BytesDecoded += int64(preparedPart.View.Dependency.ImageBytes)
		if preparedPart.Role == PartRoleBase {
			stats.BaseInternalParts++
		} else {
			stats.DeltaParts++
		}
	}
	partSet, err := NewPartSetReader(refs, p.tombstones)
	if err != nil {
		return nil, err
	}
	reader := &QueryReadyBaseDeltaReader{reader: partSet, dictionaries: dictionaries, stats: stats}
	runner, err := PrepareQueryReadyOperator(reader, request)
	if err != nil {
		return nil, err
	}
	runner.prepareNanos = time.Since(started).Nanoseconds()
	return runner, nil
}

func validateQueryReadyOperatorRequest(request QueryReadyOperatorRequest) error {
	if request.TopK < 0 {
		return errors.New("typedcolumn: query-ready operator top-K cannot be negative")
	}
	for i, predicate := range request.Predicates {
		if predicate.Column == "" || len(predicate.Values) == 0 {
			return fmt.Errorf("typedcolumn: query-ready predicate[%d] requires column and values", i)
		}
	}
	switch request.Kind {
	case QueryReadyOperatorGroupCount:
		if request.GroupColumn == "" {
			return errors.New("typedcolumn: query-ready group count requires group column")
		}
	case QueryReadyOperatorGroupCountAndDistinct:
		if request.GroupColumn == "" || request.DistinctColumn == "" {
			return errors.New("typedcolumn: query-ready count-distinct requires group and distinct columns")
		}
	case QueryReadyOperatorGroupHourCount, QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupInt64Span:
		if request.GroupColumn == "" || request.ValueColumn == "" {
			return errors.New("typedcolumn: query-ready grouped int64 operator requires group and value columns")
		}
	case QueryReadyOperatorSumSecondOfDaySquare:
		if request.ValueColumn == "" {
			return errors.New("typedcolumn: query-ready expression requires value column")
		}
	default:
		return fmt.Errorf("typedcolumn: unsupported query-ready operator %q", request.Kind)
	}
	if request.TopK > 0 && request.Order != QueryReadyOrderInt64Asc && request.Order != QueryReadyOrderInt64Desc {
		return errors.New("typedcolumn: query-ready top-K requires int64 order")
	}
	return nil
}

func buildQueryReadyExecutionDomain(reader *QueryReadyBaseDeltaReader, column string) (queryReadyExecutionDomain, error) {
	set := make(map[string]struct{})
	for partIndex, dictionaries := range reader.dictionaries {
		partColumn, ok := reader.reader.parts[partIndex].Part.Columns[column]
		if !ok {
			return queryReadyExecutionDomain{}, fmt.Errorf("typedcolumn: query-ready part %d missing column %s", partIndex, column)
		}
		if partColumn.Definition.Encoding == EncodingNullableInt64 {
			set[""] = struct{}{}
		}
		values, ok := dictionaries[column]
		if !ok {
			return queryReadyExecutionDomain{}, fmt.Errorf("typedcolumn: query-ready part %d missing dictionary for %s", partIndex, column)
		}
		for _, value := range values {
			set[value] = struct{}{}
		}
	}
	domain := queryReadyExecutionDomain{values: make([]string, 0, len(set)), byPart: make([][]int, len(reader.dictionaries)), emptyGlobal: -1}
	for value := range set {
		domain.values = append(domain.values, value)
	}
	sort.Strings(domain.values)
	if empty, ok := slices.BinarySearch(domain.values, ""); ok {
		domain.emptyGlobal = empty
	}
	for partIndex, dictionaries := range reader.dictionaries {
		local := dictionaries[column]
		maxCode := int64(-1)
		for code := range local {
			if code > maxCode {
				maxCode = code
			}
		}
		translation := make([]int, int(maxCode)+1)
		for i := range translation {
			translation[i] = -1
		}
		for code, value := range local {
			global, ok := slices.BinarySearch(domain.values, value)
			if !ok || code < 0 || int(code) >= len(translation) {
				return queryReadyExecutionDomain{}, errors.New("typedcolumn: invalid query-ready dictionary translation")
			}
			translation[code] = global
		}
		domain.byPart[partIndex] = translation
	}
	return domain, nil
}

func scanQueryReadyColumn(scanner *ColumnPartScanner, name string, valuesDst []int64, nullsDst, defaultsDst []bool, visible partSetVisibleRows) ([]int64, []bool, []bool, PartScanDiagnostics, error) {
	column, ok := scanner.part.Columns[name]
	if !ok {
		return nil, nil, nil, PartScanDiagnostics{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Encoding != EncodingNullableInt64 {
		var values []int64
		var diagnostics PartScanDiagnostics
		var err error
		if visible.All {
			values, diagnostics, err = scanner.scanColumnInto(name, valuesDst)
		} else {
			values, diagnostics, err = scanner.scanColumnRowsInto(name, valuesDst, visible.Rows)
		}
		return values, nullsDst[:0], defaultsDst[:0], diagnostics, err
	}
	rows := visible.Rows
	outputRows := len(rows)
	if visible.All {
		outputRows = scanner.part.Descriptor.RowCount
	}
	values := ensureInt64Len(valuesDst[:0], outputRows)
	nulls := ensureBoolLen(nullsDst[:0], outputRows)
	defaults := ensureBoolLen(defaultsDst[:0], outputRows)
	if outputRows == 0 {
		return values, nulls, defaults, PartScanDiagnostics{}, nil
	}
	if !visible.All && (rows[0] < 0 || rows[len(rows)-1] >= scanner.part.Descriptor.RowCount) {
		return nil, nil, nil, PartScanDiagnostics{}, fmt.Errorf("typedcolumn: visible row range [%d,%d] outside part rows=%d", rows[0], rows[len(rows)-1], scanner.part.Descriptor.RowCount)
	}
	var diagnostics PartScanDiagnostics
	selected := 0
	coveredStart, coveredEnd := -1, -1
	for _, block := range column.Blocks {
		first := block.Descriptor.FirstRow
		limit := first + block.Descriptor.RowCount
		start := selected
		if !visible.All {
			for selected < len(rows) && rows[selected] < first {
				return nil, nil, nil, diagnostics, fmt.Errorf("typedcolumn: visible row %d before block first row %d", rows[selected], first)
			}
			start = selected
			for selected < len(rows) && rows[selected] < limit {
				selected++
			}
			if start == selected {
				continue
			}
		}
		decodedValues, decodedNulls, decodedDefaults, err := scanner.reader.DecodeNullableInt64Into(scanner.values[:0], scanner.nulls[:0], scanner.defaults[:0], block.Granule)
		if err != nil {
			return nil, nil, nil, diagnostics, err
		}
		scanner.values, scanner.nulls, scanner.defaults = decodedValues, decodedNulls, decodedDefaults
		if len(decodedValues) != block.Descriptor.RowCount || len(decodedNulls) != block.Descriptor.RowCount || len(decodedDefaults) != block.Descriptor.RowCount {
			return nil, nil, nil, diagnostics, fmt.Errorf("typedcolumn: nullable block rows=%d decoded values/nulls/defaults=%d/%d/%d", block.Descriptor.RowCount, len(decodedValues), len(decodedNulls), len(decodedDefaults))
		}
		if err := validateNullableDecodedCarrierValues(column.Definition.Type, decodedValues); err != nil {
			return nil, nil, nil, diagnostics, err
		}
		if visible.All {
			copy(values[first:limit], decodedValues)
			copy(nulls[first:limit], decodedNulls)
			copy(defaults[first:limit], decodedDefaults)
		} else {
			for output, partRow := range rows[start:selected] {
				decodedRow := partRow - first
				destination := start + output
				values[destination], nulls[destination], defaults[destination] = decodedValues[decodedRow], decodedNulls[decodedRow], decodedDefaults[decodedRow]
			}
		}
		diagnostics.BlocksDecoded++
		diagnostics.BytesDecoded += block.Granule.RawBytes
		coveredStart, coveredEnd = extendGranuleCoverage(coveredStart, coveredEnd, block.Descriptor.FirstGranule, block.Descriptor.LastGranule, &diagnostics.GranulesDecoded)
	}
	if !visible.All && selected != len(rows) {
		return nil, nil, nil, diagnostics, fmt.Errorf("typedcolumn: %d visible rows outside column %s blocks", len(rows)-selected, name)
	}
	if coveredStart >= 0 {
		diagnostics.GranulesDecoded += coveredEnd - coveredStart + 1
	}
	diagnostics.RowsScanned = outputRows
	return values, nulls, defaults, diagnostics, nil
}

func (r *QueryReadyOperator) Run() (QueryReadyOperatorResult, error) {
	if r == nil || r.reader == nil {
		return QueryReadyOperatorResult{}, errors.New("typedcolumn: nil query-ready operator")
	}
	stats := QueryReadyExecutionStats{EncodedBaseDeltaExecutions: 1, PreparedParts: len(r.parts), Predicates: len(r.predicates), DictionaryDomains: len(r.domains), PreparationNanos: r.prepareNanos}
	visibility := r.reader.reader.VisibilityStats()
	stats.RowsCandidate, stats.RowsVisible = visibility.InputRows, visibility.VisibleRows
	stats.RowsSuperseded, stats.RowsDeleted = visibility.SupersededRows, visibility.DeletedRows
	for _, part := range r.parts {
		if part.role == PartRoleBase {
			stats.BaseParts++
		} else {
			stats.DeltaParts++
		}
	}
	clear(r.counts)
	clear(r.distinctBits)
	clear(r.hourCounts)
	clear(r.int64Values)
	clear(r.int64Max)
	clear(r.seen)
	r.resultGroups = r.resultGroups[:0]
	var expressionSum int64
	for partIndex := range r.parts {
		part := &r.parts[partIndex]
		phaseStart := time.Now()
		scanner := part.scanner
		for projected, column := range r.projected {
			values, nulls, defaults, diag, err := scanQueryReadyColumn(scanner, column, part.values[projected], part.nulls[projected], part.defaults[projected], part.visible)
			if err != nil {
				return QueryReadyOperatorResult{Stats: stats}, err
			}
			part.values[projected] = values
			part.nulls[projected], part.defaults[projected] = nulls, defaults
			stats.DecodedBlocks += diag.BlocksDecoded
			stats.DecodedBytes += int64(diag.BytesDecoded)
		}
		rows := len(part.visible.Rows)
		if part.visible.All {
			rows = part.part.Descriptor.RowCount
		}
		part.decodedRows = rows
		stats.RowsScanned += rows
		if part.role == PartRoleBase {
			stats.BaseRowsScanned += rows
			stats.BaseScanNanos += time.Since(phaseStart).Nanoseconds()
		} else {
			stats.DeltaRowsScanned += rows
			stats.DeltaMergeNanos += time.Since(phaseStart).Nanoseconds()
		}
		predicateStart := time.Now()
		for row := 0; row < rows; row++ {
			matched, err := r.rowMatches(partIndex, part, row, &stats)
			if err != nil {
				return QueryReadyOperatorResult{Stats: stats}, err
			}
			if !matched {
				continue
			}
			stats.RowsMatched++
			if err := r.reduceRow(partIndex, part, row, &expressionSum, &stats); err != nil {
				return QueryReadyOperatorResult{Stats: stats}, err
			}
		}
		stats.PredicateNanos += time.Since(predicateStart).Nanoseconds()
	}
	shapeStart := time.Now()
	r.shapeGroups(expressionSum, &stats)
	stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
	orderStart := time.Now()
	r.orderAndLimit()
	stats.OrderingTopKNanos = time.Since(orderStart).Nanoseconds()
	stats.GroupsReturned = len(r.resultGroups)
	stats.ScratchBytes = r.scratchBytes()
	return QueryReadyOperatorResult{Groups: r.resultGroups, Stats: stats}, nil
}

func (r *QueryReadyOperator) rowMatches(partIndex int, part *queryReadyExecutionPart, row int, stats *QueryReadyExecutionStats) (bool, error) {
	for _, predicate := range r.predicates {
		if r.rowAbsent(part, predicate.projected, row) {
			if predicate.domain.emptyGlobal < 0 || !predicate.allowed[predicate.domain.emptyGlobal] {
				return false, nil
			}
			continue
		}
		local := part.values[predicate.projected][row]
		if local < 0 || int(local) >= len(predicate.domain.byPart[partIndex]) || predicate.domain.byPart[partIndex][local] < 0 {
			return false, fmt.Errorf("typedcolumn: query-ready predicate code=%d outside domain", local)
		}
		global := predicate.domain.byPart[partIndex][local]
		stats.CodeTranslations++
		if !predicate.allowed[global] {
			return false, nil
		}
	}
	return true, nil
}

func (r *QueryReadyOperator) globalCode(domain queryReadyExecutionDomain, partIndex int, local int64, stats *QueryReadyExecutionStats) (int, error) {
	if local < 0 || partIndex < 0 || partIndex >= len(domain.byPart) || int(local) >= len(domain.byPart[partIndex]) || domain.byPart[partIndex][local] < 0 {
		return 0, fmt.Errorf("typedcolumn: query-ready local code=%d outside domain", local)
	}
	stats.CodeTranslations++
	return domain.byPart[partIndex][local], nil
}

func (r *QueryReadyOperator) globalCodeForRow(domain queryReadyExecutionDomain, partIndex int, part *queryReadyExecutionPart, projected, row int, stats *QueryReadyExecutionStats) (int, error) {
	if r.rowAbsent(part, projected, row) {
		if domain.emptyGlobal < 0 {
			return 0, errors.New("typedcolumn: query-ready nullable row has no empty semantic domain")
		}
		return domain.emptyGlobal, nil
	}
	return r.globalCode(domain, partIndex, part.values[projected][row], stats)
}

func (r *QueryReadyOperator) rowAbsent(part *queryReadyExecutionPart, projected, row int) bool {
	return len(part.nulls[projected]) != 0 && (part.nulls[projected][row] || part.defaults[projected][row])
}

func (r *QueryReadyOperator) reduceRow(partIndex int, part *queryReadyExecutionPart, row int, expressionSum *int64, stats *QueryReadyExecutionStats) error {
	if r.request.Kind == QueryReadyOperatorSumSecondOfDaySquare {
		seconds := floorUnixSeconds(part.values[r.valueProjected][row])
		secondOfDay := seconds % 86_400
		if secondOfDay < 0 {
			secondOfDay += 86_400
		}
		term := secondOfDay * secondOfDay
		if *expressionSum > math.MaxInt64-term {
			return fmt.Errorf("typedcolumn: query-ready second-of-day square sum overflow current=%d value=%d", *expressionSum, term)
		}
		*expressionSum += term
		return nil
	}
	group, err := r.globalCodeForRow(r.groupDomain, partIndex, part, r.groupProjected, row, stats)
	if err != nil {
		return err
	}
	switch r.request.Kind {
	case QueryReadyOperatorGroupCount:
		r.counts[group]++
	case QueryReadyOperatorGroupCountAndDistinct:
		distinct, err := r.globalCodeForRow(r.distinctDomain, partIndex, part, r.distinctProjected, row, stats)
		if err != nil {
			return err
		}
		r.counts[group]++
		cell := group*len(r.distinctDomain.values) + distinct
		r.distinctBits[cell/64] |= uint64(1) << uint(cell%64)
	case QueryReadyOperatorGroupHourCount:
		secondOfDay := floorUnixSeconds(part.values[r.valueProjected][row]) % 86_400
		if secondOfDay < 0 {
			secondOfDay += 86_400
		}
		hour := int(secondOfDay / 3_600)
		r.hourCounts[group*24+hour]++
	case QueryReadyOperatorGroupMinInt64:
		value := part.values[r.valueProjected][row]
		if !r.seen[group] || value < r.int64Values[group] {
			r.int64Values[group] = value
			r.seen[group] = true
		}
	case QueryReadyOperatorGroupInt64Span:
		value := part.values[r.valueProjected][row]
		if !r.seen[group] {
			r.int64Values[group], r.int64Max[group], r.seen[group] = value, value, true
		} else {
			if value < r.int64Values[group] {
				r.int64Values[group] = value
			}
			if value > r.int64Max[group] {
				r.int64Max[group] = value
			}
		}
	}
	return nil
}

func floorUnixSeconds(micros int64) int64 {
	seconds := micros / 1_000_000
	if micros < 0 && micros%1_000_000 != 0 {
		seconds--
	}
	return seconds
}

func (r *QueryReadyOperator) shapeGroups(expressionSum int64, stats *QueryReadyExecutionStats) {
	switch r.request.Kind {
	case QueryReadyOperatorGroupCount:
		for group, count := range r.counts {
			if count != 0 {
				r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Count: count})
			}
		}
	case QueryReadyOperatorGroupCountAndDistinct:
		cardinality := len(r.distinctDomain.values)
		for group, count := range r.counts {
			if count == 0 {
				continue
			}
			distinct := 0
			for value := 0; value < cardinality; value++ {
				cell := group*cardinality + value
				if r.distinctBits[cell/64]&(uint64(1)<<uint(cell%64)) != 0 {
					distinct++
				}
			}
			r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Count: count, DistinctCount: distinct})
		}
	case QueryReadyOperatorGroupHourCount:
		for group, key := range r.groupDomain.values {
			for hour := 0; hour < 24; hour++ {
				if count := r.hourCounts[group*24+hour]; count != 0 {
					r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: key, Hour: hour, Count: count})
				}
			}
		}
	case QueryReadyOperatorGroupMinInt64:
		for group, seen := range r.seen {
			if seen && (!r.request.SkipEmptyGroupKey || r.groupDomain.values[group] != "") {
				r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Int64: r.int64Values[group]})
			}
		}
	case QueryReadyOperatorGroupInt64Span:
		for group, seen := range r.seen {
			if seen && (!r.request.SkipEmptyGroupKey || r.groupDomain.values[group] != "") {
				r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Int64: r.int64Max[group] - r.int64Values[group]})
			}
		}
	case QueryReadyOperatorSumSecondOfDaySquare:
		if stats.RowsMatched > 0 {
			r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Count: stats.RowsMatched, Int64: expressionSum})
		}
	}
	stats.GroupsConsidered = len(r.resultGroups)
}

func (r *QueryReadyOperator) orderAndLimit() {
	if r.request.TopK == 0 {
		slices.SortFunc(r.resultGroups, func(left, right QueryReadyOperatorGroup) int {
			if left.Key < right.Key {
				return -1
			}
			if left.Key > right.Key {
				return 1
			}
			if r.request.Kind == QueryReadyOperatorGroupHourCount {
				return left.Hour - right.Hour
			}
			return 0
		})
		return
	}
	slices.SortFunc(r.resultGroups, func(left, right QueryReadyOperatorGroup) int {
		switch r.request.Kind {
		case QueryReadyOperatorGroupCount, QueryReadyOperatorGroupCountAndDistinct:
			if left.Count != right.Count {
				if left.Count > right.Count {
					return -1
				}
				return 1
			}
		case QueryReadyOperatorGroupHourCount:
			if left.Hour != right.Hour {
				return left.Hour - right.Hour
			}
		case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupInt64Span:
			if left.Int64 != right.Int64 {
				if r.request.Order == QueryReadyOrderInt64Desc {
					if left.Int64 > right.Int64 {
						return -1
					}
					return 1
				}
				if left.Int64 < right.Int64 {
					return -1
				}
				return 1
			}
		}
		if left.Key < right.Key {
			return -1
		}
		if left.Key > right.Key {
			return 1
		}
		return 0
	})
	if len(r.resultGroups) > r.request.TopK {
		r.resultGroups = r.resultGroups[:r.request.TopK]
	}
}

func (r *QueryReadyOperator) scratchBytes() int64 {
	bytes := int64(cap(r.counts)+cap(r.hourCounts))*8 + int64(cap(r.seen)) + int64(cap(r.int64Values)+cap(r.int64Max)+cap(r.distinctBits))*8
	for _, domain := range r.domains {
		for _, translation := range domain.byPart {
			bytes += int64(cap(translation)) * 8
		}
	}
	for _, predicate := range r.predicates {
		bytes += int64(cap(predicate.allowed))
	}
	for _, part := range r.parts {
		for _, values := range part.values {
			bytes += int64(cap(values)) * 8
		}
		for projected := range part.nulls {
			bytes += int64(cap(part.nulls[projected]) + cap(part.defaults[projected]))
		}
		if part.scanner != nil {
			bytes += int64(cap(part.scanner.values)+cap(part.scanner.scratch))*8 + int64(cap(part.scanner.codes))*4
			bytes += int64(cap(part.scanner.bools) + cap(part.scanner.nulls) + cap(part.scanner.defaults))
		}
	}
	return bytes
}
