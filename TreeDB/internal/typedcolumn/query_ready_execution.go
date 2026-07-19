package typedcolumn

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// QueryReadyOperatorKind names the shared encoded reductions used by the
// JSONBench q1-q5 and qexpr production cells. These are physical operations,
// not query names or persisted precomputed answers.
type QueryReadyOperatorKind string

const (
	QueryReadyOperatorGroupCount            QueryReadyOperatorKind = "group_count"
	QueryReadyOperatorGroupCountDistinct    QueryReadyOperatorKind = "group_count_distinct"
	QueryReadyOperatorGroupCountAndDistinct QueryReadyOperatorKind = "group_count_and_distinct"
	QueryReadyOperatorHourCount             QueryReadyOperatorKind = "hour_count"
	QueryReadyOperatorGroupHourCount        QueryReadyOperatorKind = "group_hour_count"
	QueryReadyOperatorGroupMinInt64         QueryReadyOperatorKind = "group_min_int64"
	QueryReadyOperatorGroupMaxInt64         QueryReadyOperatorKind = "group_max_int64"
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
	EncodedBaseDeltaExecutions        int
	PreparedParts                     int
	BaseParts                         int
	DeltaParts                        int
	RowsCandidate                     int
	RowsVisible                       int
	RowsSuperseded                    int
	RowsDeleted                       int
	RowsScanned                       int
	BaseRowsScanned                   int
	DeltaRowsScanned                  int
	RowsMatched                       int
	GroupsConsidered                  int
	GroupsReturned                    int
	Predicates                        int
	DictionaryDomains                 int
	CodeTranslations                  int
	DecodedBlocks                     int
	DecodedBytes                      int64
	ScratchBytes                      int64
	PreparationNanos                  int64
	BaseScanNanos                     int64
	DeltaMergeNanos                   int64
	PredicateNanos                    int64
	ReductionNanos                    int64
	FusedPredicateReductionExecutions int
	FusedPredicateReductionWorkers    int
	FusedPredicateReductionNanos      int64
	GroupingNanos                     int64
	OrderingTopKNanos                 int64
	MaterializationNanos              int64
	DocumentMaterializations          int
	LegacyScanFallbacks               int
	PrecomputedAnswers                int
	Fallbacks                         int
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
	projected    int
	domain       queryReadyExecutionDomain
	allowed      []bool
	allowedLocal [][]bool
}

type queryReadyExecutionPart struct {
	part        *ColumnPart
	scanner     *ColumnPartScanner
	role        PartRole
	visible     partSetVisibleRows
	values      [][]int64
	nulls       [][]bool
	defaults    [][]bool
	direct      []QueryReadyExecutionColumnView
	predicates  []queryReadyDirectPredicatePart
	decodedRows int
}

type queryReadyDirectPredicatePart struct {
	column       QueryReadyExecutionColumnView
	allowed8     *[256]bool
	emptyAllowed bool
}

func (p *queryReadyDirectPredicatePart) matches8(row int) bool {
	if len(p.column.absent) != 0 && p.column.absentAtUnchecked(row) {
		return p.emptyAllowed
	}
	return p.allowed8[p.column.values[row]]
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
	matchedRows       []int
	resultGroups      []QueryReadyOperatorGroup
	fusedMin          []atomic.Int64
	fusedMax          []atomic.Int64
	fusedSeen         []atomic.Bool
	fusedWorkerErrors []error
	fusedWorkerRows   []int
	fusedWorkerCodes  []int
}

const queryReadyMaxGroupDistinctCells = 64 << 20

// PrepareQueryReadyOperator binds query-shaped predicates and bounded scratch
// to an already-open snapshot-correct base-plus-delta reader. Prepared file
// generations reuse domains built once during explicit open. It never reads
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
		allowedLocal := make([][]bool, len(domain.byPart))
		for partIndex, translation := range domain.byPart {
			local := make([]bool, len(translation))
			for code, global := range translation {
				local[code] = global >= 0 && global < len(allowed) && allowed[global]
			}
			allowedLocal[partIndex] = local
		}
		runner.predicates = append(runner.predicates, queryReadyExecutionPredicate{projected: runner.projectedByName[predicate.Column], domain: domain, allowed: allowed, allowedLocal: allowedLocal})
	}
	if request.GroupColumn != "" {
		runner.groupDomain = runner.domains[request.GroupColumn]
	}
	if request.DistinctColumn != "" {
		runner.distinctDomain = runner.domains[request.DistinctColumn]
	}
	maxVisibleRows := 0
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
		visible := reader.reader.visibleRowsForPart(partIndex)
		visibleRows := len(visible.Rows)
		if visible.All {
			visibleRows = loaded.Part.Descriptor.RowCount
		}
		if visibleRows > maxVisibleRows {
			maxVisibleRows = visibleRows
		}
		direct := make([]QueryReadyExecutionColumnView, len(runner.projected))
		if partIndex < len(reader.executions) {
			for projected, column := range runner.projected {
				direct[projected], _ = reader.executions[partIndex].Column(column)
			}
		}
		directPredicates := make([]queryReadyDirectPredicatePart, len(runner.predicates))
		for predicateIndex := range runner.predicates {
			predicate := &runner.predicates[predicateIndex]
			column := direct[predicate.projected]
			directPredicate := queryReadyDirectPredicatePart{column: column, emptyAllowed: predicate.domain.emptyGlobal >= 0 && predicate.allowed[predicate.domain.emptyGlobal]}
			if column.kind == QueryReadyExecutionColumnCode && column.codeWidth == 1 && partIndex < len(predicate.allowedLocal) {
				table := new([256]bool)
				for code, allowed := range predicate.allowedLocal[partIndex] {
					if code >= len(table) {
						break
					}
					table[code] = allowed
				}
				directPredicate.allowed8 = table
			}
			directPredicates[predicateIndex] = directPredicate
		}
		runner.parts = append(runner.parts, queryReadyExecutionPart{
			part: loaded.Part, scanner: loaded.Part.NewScanner(), role: loaded.Ref.Role, visible: visible, values: make([][]int64, len(runner.projected)), nulls: make([][]bool, len(runner.projected)), defaults: make([][]bool, len(runner.projected)),
			direct: direct, predicates: directPredicates,
		})
	}
	runner.matchedRows = make([]int, 0, maxVisibleRows)
	groups := len(runner.groupDomain.values)
	switch request.Kind {
	case QueryReadyOperatorGroupCount:
		runner.counts = make([]int, groups)
	case QueryReadyOperatorGroupCountDistinct, QueryReadyOperatorGroupCountAndDistinct:
		distinct := len(runner.distinctDomain.values)
		if groups > 0 && distinct > queryReadyMaxGroupDistinctCells/groups {
			return nil, fmt.Errorf("typedcolumn: query-ready group-distinct dimensions=%dx%d exceed cell bound=%d", groups, distinct, queryReadyMaxGroupDistinctCells)
		}
		cells := groups * distinct
		runner.counts = make([]int, groups)
		runner.distinctBits = make([]uint64, (cells+63)/64)
	case QueryReadyOperatorHourCount:
		runner.hourCounts = make([]int, 24)
	case QueryReadyOperatorGroupHourCount:
		runner.hourCounts = make([]int, groups*24)
	case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64:
		runner.int64Values, runner.seen = make([]int64, groups), make([]bool, groups)
	case QueryReadyOperatorGroupInt64Span:
		runner.int64Values, runner.int64Max, runner.seen = make([]int64, groups), make([]int64, groups), make([]bool, groups)
	case QueryReadyOperatorSumSecondOfDaySquare:
	}
	resultCapacity := groups
	if request.Kind == QueryReadyOperatorHourCount {
		resultCapacity = 24
	} else if request.Kind == QueryReadyOperatorGroupHourCount {
		resultCapacity *= 24
	}
	if request.Kind == QueryReadyOperatorSumSecondOfDaySquare {
		resultCapacity = 1
	}
	if runner.usesBoundedTopKShape() && request.TopK < resultCapacity {
		resultCapacity = request.TopK
	}
	if len(request.Predicates) == 3 && (request.Kind == QueryReadyOperatorGroupMinInt64 || request.Kind == QueryReadyOperatorGroupMaxInt64 || request.Kind == QueryReadyOperatorGroupInt64Span) {
		workers := min(runtime.GOMAXPROCS(0), len(runner.parts), 8)
		if workers > 1 {
			runner.fusedMin = make([]atomic.Int64, groups)
			runner.fusedSeen = make([]atomic.Bool, groups)
			if request.Kind == QueryReadyOperatorGroupMaxInt64 || request.Kind == QueryReadyOperatorGroupInt64Span {
				runner.fusedMax = make([]atomic.Int64, groups)
			}
			runner.fusedWorkerErrors = make([]error, workers)
			runner.fusedWorkerRows = make([]int, workers)
			runner.fusedWorkerCodes = make([]int, workers)
		}
	}
	runner.resultGroups = make([]QueryReadyOperatorGroup, 0, resultCapacity)
	runner.prepareNanos = time.Since(started).Nanoseconds()
	return runner, nil
}

// PrepareOperator binds one operator to the immutable file-backed generation
// selected by Open. Query-independent structural, visibility, dictionary, and
// domain state is reused; payload values remain mapped direct vectors scanned
// by the operator on Run.
func (p *QueryReadyPreparedGeneration) PrepareOperator(request QueryReadyOperatorRequest) (*QueryReadyOperator, error) {
	started := time.Now()
	if p == nil || p.Closed() {
		return nil, errors.New("typedcolumn: query-ready prepared generation is closed")
	}
	if p.execution == nil {
		return nil, errors.New("typedcolumn: query-ready prepared generation has no execution state")
	}
	runner, err := PrepareQueryReadyOperator(p.execution, request)
	if err != nil {
		return nil, err
	}
	runner.prepareNanos = time.Since(started).Nanoseconds()
	return runner, nil
}

func prepareQueryReadyGenerationExecutionState(parts []QueryReadyPreparedPartView, tombstones []Tombstone) (*QueryReadyBaseDeltaReader, QueryReadyGenerationOpenStats, error) {
	refs := make([]PartRef, 0, len(parts))
	dictionaries := make([]map[string]map[int64]string, 0, len(parts))
	executions := make([]QueryReadyExecutionPartView, 0, len(parts))
	readerStats := QueryReadyBaseDeltaStats{TotalParts: len(parts), TombstonesApplied: len(tombstones)}
	openStats := QueryReadyGenerationOpenStats{}
	denseDisjointScan := len(tombstones) == 0
	for _, preparedPart := range parts {
		if preparedPart.Role != PartRoleBase || preparedPart.View.Dependency.PrimaryIDMode != QueryReadyPrimaryIDDensePartLocal {
			denseDisjointScan = false
			break
		}
	}
	for index, preparedPart := range parts {
		readOptions := ColumnPartImageReadOptions{}
		if !denseDisjointScan {
			readOptions.IncludeRowLocators = true
			readOptions.ValidateRowLocators = true
		}
		part, err := ColumnPartFromImageWithOptions(preparedPart.View.Image, readOptions)
		if err != nil {
			return nil, QueryReadyGenerationOpenStats{}, fmt.Errorf("typedcolumn: prepare query-ready operator part[%d]: %w", index, err)
		}
		refs = append(refs, PartRef{Role: preparedPart.Role, GenerationID: preparedPart.Generation, Part: part, PrimaryIDMode: preparedPart.View.Dependency.PrimaryIDMode, PrimaryIDBase: preparedPart.View.Dependency.PrimaryIDBase})
		executions = append(executions, preparedPart.View.Execution)
		decoded, err := preparedPart.View.Image.Dictionaries()
		if err != nil {
			return nil, QueryReadyGenerationOpenStats{}, fmt.Errorf("typedcolumn: prepare query-ready operator dictionaries part[%d]: %w", index, err)
		}
		inverse := make(map[string]map[int64]string, len(decoded))
		for column, values := range decoded {
			byCode := make(map[int64]string, len(values))
			for value, code := range values {
				byCode[code] = value
			}
			inverse[column] = byCode
			readerStats.LocalDictionaryDecodes++
		}
		dictionaries = append(dictionaries, inverse)
		readerStats.PartsDecoded++
		readerStats.RowsMerged += preparedPart.View.Dependency.Rows
		readerStats.BytesDecoded += int64(preparedPart.View.Dependency.ImageBytes)
		openStats.PartsDecoded++
		for _, section := range preparedPart.View.Image.Sections {
			if section.Kind != ColumnPartImageSectionDictionaries {
				continue
			}
			decodedBytes := section.RawBytes
			if decodedBytes == 0 {
				decodedBytes = section.Length
			}
			if decodedBytes < 0 || int64(decodedBytes) > math.MaxInt64-openStats.PayloadBytesDecoded {
				return nil, QueryReadyGenerationOpenStats{}, errors.New("typedcolumn: query-ready dictionary byte accounting overflow")
			}
			openStats.PayloadBytesDecoded += int64(decodedBytes)
		}
		if preparedPart.Role == PartRoleBase {
			readerStats.BaseInternalParts++
		} else {
			readerStats.DeltaParts++
		}
	}
	var partSet *PartSetReader
	var err error
	if denseDisjointScan {
		partSet, err = newDenseDisjointScanPartSetReader(refs)
	} else {
		partSet, err = NewPartSetReader(refs, tombstones)
	}
	if err != nil {
		return nil, QueryReadyGenerationOpenStats{}, err
	}
	reader := &QueryReadyBaseDeltaReader{reader: partSet, dictionaries: dictionaries, executions: executions, domains: make(map[string]queryReadyExecutionDomain), stats: readerStats}
	domainColumns := make(map[string]struct{})
	for _, execution := range executions {
		for name, column := range execution.columns {
			if column.kind == QueryReadyExecutionColumnCode {
				domainColumns[name] = struct{}{}
			}
		}
	}
	names := make([]string, 0, len(domainColumns))
	for name := range domainColumns {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		domain, err := buildQueryReadyExecutionDomain(reader, name)
		if err != nil {
			return nil, QueryReadyGenerationOpenStats{}, fmt.Errorf("typedcolumn: prepare query-ready domain %s: %w", name, err)
		}
		reader.domains[name] = domain
		reader.stats.GlobalDictionaryConstructions++
		openStats.DomainsConstructed++
	}
	return reader, openStats, nil
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
	case QueryReadyOperatorGroupCountDistinct, QueryReadyOperatorGroupCountAndDistinct:
		if request.GroupColumn == "" || request.DistinctColumn == "" {
			return errors.New("typedcolumn: query-ready count-distinct requires group and distinct columns")
		}
	case QueryReadyOperatorHourCount:
		if request.ValueColumn == "" {
			return errors.New("typedcolumn: query-ready hour count requires value column")
		}
	case QueryReadyOperatorGroupHourCount, QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64, QueryReadyOperatorGroupInt64Span:
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
	if reader != nil && reader.domains != nil {
		if domain, ok := reader.domains[column]; ok {
			return domain, nil
		}
	}
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
			definition := partColumn.Definition
			if definition.Type != ColumnTypeLowCardinalityCode || definition.Encoding != EncodingNullableInt64 || definition.Cardinality != 0 {
				return queryReadyExecutionDomain{}, fmt.Errorf("typedcolumn: query-ready part %d missing dictionary for %s", partIndex, column)
			}
			values = map[int64]string{}
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
	if r.canRunDirect() {
		return r.runDirect(stats)
	}
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
		fusedStarted := time.Now()
		if matched, handled, err := r.reduceFusedDecodedGroupedInt64(partIndex, part, rows, &stats); handled {
			fusedElapsed := time.Since(fusedStarted).Nanoseconds()
			stats.FusedPredicateReductionExecutions = 1
			stats.FusedPredicateReductionWorkers = 1
			stats.FusedPredicateReductionNanos += fusedElapsed
			if part.role == PartRoleBase {
				stats.BaseScanNanos += fusedElapsed
			} else {
				stats.DeltaMergeNanos += fusedElapsed
			}
			stats.RowsMatched += matched
			if err != nil {
				return QueryReadyOperatorResult{Stats: stats}, err
			}
			continue
		}
		r.matchedRows = r.matchedRows[:0]
		predicateStart := time.Now()
		for row := 0; row < rows; row++ {
			matched, err := r.rowMatches(partIndex, part, row, &stats)
			if err != nil {
				stats.PredicateNanos += time.Since(predicateStart).Nanoseconds()
				return QueryReadyOperatorResult{Stats: stats}, err
			}
			if matched {
				r.matchedRows = append(r.matchedRows, row)
			}
		}
		stats.PredicateNanos += time.Since(predicateStart).Nanoseconds()
		stats.RowsMatched += len(r.matchedRows)
		reductionStart := time.Now()
		for _, row := range r.matchedRows {
			if err := r.reduceRow(partIndex, part, row, &expressionSum, &stats); err != nil {
				stats.ReductionNanos += time.Since(reductionStart).Nanoseconds()
				return QueryReadyOperatorResult{Stats: stats}, err
			}
		}
		stats.ReductionNanos += time.Since(reductionStart).Nanoseconds()
	}
	shapeStart := time.Now()
	if err := r.shapeGroups(expressionSum, &stats); err != nil {
		stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
		return QueryReadyOperatorResult{Stats: stats}, err
	}
	stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
	orderStart := time.Now()
	r.orderAndLimit()
	stats.OrderingTopKNanos = time.Since(orderStart).Nanoseconds()
	stats.GroupsReturned = len(r.resultGroups)
	stats.ScratchBytes = r.scratchBytes()
	return QueryReadyOperatorResult{Groups: r.resultGroups, Stats: stats}, nil
}

func (r *QueryReadyOperator) reduceFusedDecodedGroupedInt64(partIndex int, part *queryReadyExecutionPart, rows int, stats *QueryReadyExecutionStats) (int, bool, error) {
	switch r.request.Kind {
	case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64, QueryReadyOperatorGroupInt64Span:
	default:
		return 0, false, nil
	}
	if len(r.predicates) == 0 {
		return 0, false, nil
	}
	matchedRows := 0
	var unusedExpressionSum int64
	for row := 0; row < rows; row++ {
		matched, err := r.rowMatches(partIndex, part, row, stats)
		if err != nil {
			return matchedRows, true, err
		}
		if !matched {
			continue
		}
		if err := r.reduceRow(partIndex, part, row, &unusedExpressionSum, stats); err != nil {
			return matchedRows, true, err
		}
		matchedRows++
	}
	return matchedRows, true, nil
}

func (r *QueryReadyOperator) canRunDirect() bool {
	if r == nil || len(r.parts) == 0 {
		return false
	}
	for i := range r.parts {
		part := &r.parts[i]
		if !part.visible.All || len(part.direct) != len(r.projected) {
			return false
		}
		for projected := range r.projected {
			column := part.direct[projected]
			if column.rows != part.part.Descriptor.RowCount || (column.kind != QueryReadyExecutionColumnCode && column.kind != QueryReadyExecutionColumnInt64) {
				return false
			}
		}
	}
	return true
}

func (r *QueryReadyOperator) runDirect(stats QueryReadyExecutionStats) (QueryReadyOperatorResult, error) {
	if result, handled := r.runDirectFusedParallel(stats); handled {
		return result.result, result.err
	}
	var expressionSum int64
	for partIndex := range r.parts {
		part := &r.parts[partIndex]
		rows := part.part.Descriptor.RowCount
		stats.RowsScanned += rows
		if part.role == PartRoleBase {
			stats.BaseRowsScanned += rows
		} else {
			stats.DeltaRowsScanned += rows
		}
		for _, column := range part.direct {
			stats.DecodedBytes += int64(len(column.values) + len(column.absent))
		}
		var selected []int
		predicateStarted := time.Now()
		if len(r.predicates) != 0 {
			if err := r.selectDirectRows(partIndex, part, rows); err != nil {
				stats.PredicateNanos += time.Since(predicateStarted).Nanoseconds()
				return QueryReadyOperatorResult{Stats: stats}, err
			}
			selected = r.matchedRows
			stats.RowsMatched += len(selected)
		} else {
			stats.RowsMatched += rows
		}
		stats.PredicateNanos += time.Since(predicateStarted).Nanoseconds()
		reductionStarted := time.Now()
		if err := r.reduceDirectPart(partIndex, part, rows, selected, &expressionSum, &stats); err != nil {
			stats.ReductionNanos += time.Since(reductionStarted).Nanoseconds()
			return QueryReadyOperatorResult{Stats: stats}, err
		}
		elapsed := time.Since(reductionStarted).Nanoseconds()
		stats.ReductionNanos += elapsed
		if part.role == PartRoleBase {
			stats.BaseScanNanos += elapsed
		} else {
			stats.DeltaMergeNanos += elapsed
		}
	}
	shapeStart := time.Now()
	if err := r.shapeGroups(expressionSum, &stats); err != nil {
		stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
		return QueryReadyOperatorResult{Stats: stats}, err
	}
	stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
	orderStart := time.Now()
	r.orderAndLimit()
	stats.OrderingTopKNanos = time.Since(orderStart).Nanoseconds()
	stats.GroupsReturned = len(r.resultGroups)
	stats.ScratchBytes = r.scratchBytes()
	return QueryReadyOperatorResult{Groups: r.resultGroups, Stats: stats}, nil
}

type queryReadyFusedParallelResult struct {
	result QueryReadyOperatorResult
	err    error
}

func (r *QueryReadyOperator) runDirectFusedParallel(stats QueryReadyExecutionStats) (queryReadyFusedParallelResult, bool) {
	switch r.request.Kind {
	case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64, QueryReadyOperatorGroupInt64Span:
	default:
		return queryReadyFusedParallelResult{}, false
	}
	workers := len(r.fusedWorkerRows)
	if workers < 2 || len(r.fusedMin) != len(r.groupDomain.values) || len(r.fusedSeen) != len(r.groupDomain.values) {
		return queryReadyFusedParallelResult{}, false
	}
	if (r.request.Kind == QueryReadyOperatorGroupMaxInt64 || r.request.Kind == QueryReadyOperatorGroupInt64Span) && len(r.fusedMax) != len(r.groupDomain.values) {
		return queryReadyFusedParallelResult{}, false
	}
	for partIndex := range r.parts {
		part := &r.parts[partIndex]
		rows := part.part.Descriptor.RowCount
		if len(part.predicates) != 3 || len(r.predicates) != 3 || partIndex >= len(r.groupDomain.byPart) {
			return queryReadyFusedParallelResult{}, false
		}
		for predicateIndex := range part.predicates {
			predicate := &part.predicates[predicateIndex]
			if predicate.allowed8 == nil || len(predicate.column.values) < rows {
				return queryReadyFusedParallelResult{}, false
			}
		}
		groupColumn := part.direct[r.groupProjected]
		valueColumn := part.direct[r.valueProjected]
		if groupColumn.kind != QueryReadyExecutionColumnCode || valueColumn.kind != QueryReadyExecutionColumnInt64 ||
			groupColumn.rows < rows || valueColumn.rows < rows {
			return queryReadyFusedParallelResult{}, false
		}
	}

	for group := range r.fusedSeen {
		r.fusedSeen[group].Store(false)
		r.fusedMin[group].Store(math.MaxInt64)
		if len(r.fusedMax) != 0 {
			r.fusedMax[group].Store(math.MinInt64)
		}
	}
	clear(r.fusedWorkerErrors)
	clear(r.fusedWorkerRows)
	clear(r.fusedWorkerCodes)
	started := time.Now()
	var wait sync.WaitGroup
	wait.Add(workers - 1)
	runWorker := func(worker int) {
		if worker != 0 {
			defer wait.Done()
		}
		matched, translated := 0, 0
		for partIndex := worker; partIndex < len(r.parts); partIndex += workers {
			part := &r.parts[partIndex]
			rows := part.part.Descriptor.RowCount
			p0, p1, p2 := &part.predicates[0], &part.predicates[1], &part.predicates[2]
			groupColumn := part.direct[r.groupProjected]
			valueColumn := part.direct[r.valueProjected]
			groupTranslation := r.groupDomain.byPart[partIndex]
			for row := range p0.column.values[:rows] {
				if !p0.matches8(row) || !p1.matches8(row) || !p2.matches8(row) {
					continue
				}
				group := r.groupDomain.emptyGlobal
				if !groupColumn.absentAtUnchecked(row) {
					local := groupColumn.codeAtUnchecked(row)
					if int(local) >= len(groupTranslation) || groupTranslation[local] < 0 {
						r.fusedWorkerErrors[worker] = fmt.Errorf("typedcolumn: query-ready fused direct group code=%d outside domain", local)
						return
					}
					group = groupTranslation[local]
					translated++
				} else if group < 0 {
					r.fusedWorkerErrors[worker] = errors.New("typedcolumn: query-ready fused direct nullable group has no empty domain")
					return
				}
				value := valueColumn.int64AtUnchecked(row)
				r.fusedSeen[group].Store(true)
				switch r.request.Kind {
				case QueryReadyOperatorGroupMinInt64:
					atomicMinInt64(&r.fusedMin[group], value)
				case QueryReadyOperatorGroupMaxInt64:
					atomicMaxInt64(&r.fusedMax[group], value)
				case QueryReadyOperatorGroupInt64Span:
					atomicMinInt64(&r.fusedMin[group], value)
					atomicMaxInt64(&r.fusedMax[group], value)
				}
				matched++
			}
		}
		r.fusedWorkerRows[worker], r.fusedWorkerCodes[worker] = matched, translated
	}
	for worker := 1; worker < workers; worker++ {
		go runWorker(worker)
	}
	runWorker(0)
	wait.Wait()
	elapsed := time.Since(started).Nanoseconds()
	for worker := range r.fusedWorkerErrors {
		if err := r.fusedWorkerErrors[worker]; err != nil {
			stats.FusedPredicateReductionExecutions = 1
			stats.FusedPredicateReductionWorkers = workers
			stats.FusedPredicateReductionNanos = elapsed
			return queryReadyFusedParallelResult{result: QueryReadyOperatorResult{Stats: stats}, err: err}, true
		}
		stats.RowsMatched += r.fusedWorkerRows[worker]
		stats.CodeTranslations += r.fusedWorkerCodes[worker]
	}
	for partIndex := range r.parts {
		part := &r.parts[partIndex]
		rows := part.part.Descriptor.RowCount
		stats.RowsScanned += rows
		if part.role == PartRoleBase {
			stats.BaseRowsScanned += rows
		} else {
			stats.DeltaRowsScanned += rows
		}
		for _, column := range part.direct {
			stats.DecodedBytes += int64(len(column.values) + len(column.absent))
		}
	}
	for group := range r.fusedSeen {
		if !r.fusedSeen[group].Load() {
			continue
		}
		r.seen[group] = true
		switch r.request.Kind {
		case QueryReadyOperatorGroupMinInt64:
			r.int64Values[group] = r.fusedMin[group].Load()
		case QueryReadyOperatorGroupMaxInt64:
			r.int64Values[group] = r.fusedMax[group].Load()
		case QueryReadyOperatorGroupInt64Span:
			r.int64Values[group], r.int64Max[group] = r.fusedMin[group].Load(), r.fusedMax[group].Load()
		}
	}
	stats.FusedPredicateReductionExecutions = 1
	stats.FusedPredicateReductionWorkers = workers
	stats.FusedPredicateReductionNanos = elapsed
	if stats.DeltaRowsScanned == 0 {
		stats.BaseScanNanos = elapsed
	} else if stats.BaseRowsScanned == 0 {
		stats.DeltaMergeNanos = elapsed
	} else {
		// The fused worker pool scans base and append-only delta parts in one
		// interval. Attribute that indivisible interval to the public scan
		// total instead of dropping it when both roles are present.
		stats.BaseScanNanos = elapsed
	}
	shapeStart := time.Now()
	if err := r.shapeGroups(0, &stats); err != nil {
		stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
		return queryReadyFusedParallelResult{result: QueryReadyOperatorResult{Stats: stats}, err: err}, true
	}
	stats.GroupingNanos = time.Since(shapeStart).Nanoseconds()
	orderStart := time.Now()
	r.orderAndLimit()
	stats.OrderingTopKNanos = time.Since(orderStart).Nanoseconds()
	stats.GroupsReturned = len(r.resultGroups)
	stats.ScratchBytes = r.scratchBytes()
	return queryReadyFusedParallelResult{result: QueryReadyOperatorResult{Groups: r.resultGroups, Stats: stats}}, true
}

func atomicMinInt64(cell *atomic.Int64, value int64) {
	for current := cell.Load(); value < current; current = cell.Load() {
		if cell.CompareAndSwap(current, value) {
			return
		}
	}
}

func atomicMaxInt64(cell *atomic.Int64, value int64) {
	for current := cell.Load(); value > current; current = cell.Load() {
		if cell.CompareAndSwap(current, value) {
			return
		}
	}
}

func (r *QueryReadyOperator) selectDirectRows(partIndex int, part *queryReadyExecutionPart, rows int) error {
	r.matchedRows = r.matchedRows[:0]
	fast8 := len(part.predicates) == len(r.predicates) && len(part.predicates) > 0 && len(part.predicates) <= 3
	if fast8 {
		for index := range part.predicates {
			predicate := &part.predicates[index]
			if predicate.allowed8 == nil || len(predicate.column.values) < rows {
				fast8 = false
				break
			}
		}
	}
	if fast8 {
		switch len(part.predicates) {
		case 1:
			p0 := &part.predicates[0]
			for row := range p0.column.values[:rows] {
				if p0.matches8(row) {
					r.matchedRows = append(r.matchedRows, row)
				}
			}
		case 2:
			p0, p1 := &part.predicates[0], &part.predicates[1]
			for row := range p0.column.values[:rows] {
				if p0.matches8(row) && p1.matches8(row) {
					r.matchedRows = append(r.matchedRows, row)
				}
			}
		case 3:
			p0, p1, p2 := &part.predicates[0], &part.predicates[1], &part.predicates[2]
			for row := range p0.column.values[:rows] {
				if p0.matches8(row) && p1.matches8(row) && p2.matches8(row) {
					r.matchedRows = append(r.matchedRows, row)
				}
			}
		}
		return nil
	}
	for row := 0; row < rows; row++ {
		matched, err := r.directRowMatches(partIndex, part, row)
		if err != nil {
			return err
		}
		if matched {
			r.matchedRows = append(r.matchedRows, row)
		}
	}
	return nil
}

func (r *QueryReadyOperator) directRowMatches(partIndex int, part *queryReadyExecutionPart, row int) (bool, error) {
	for predicateIndex := range r.predicates {
		predicate := &r.predicates[predicateIndex]
		column := part.direct[predicate.projected]
		if column.absentAtUnchecked(row) {
			if predicate.domain.emptyGlobal < 0 || !predicate.allowed[predicate.domain.emptyGlobal] {
				return false, nil
			}
			continue
		}
		local := column.codeAtUnchecked(row)
		if partIndex >= len(predicate.allowedLocal) || int(local) >= len(predicate.allowedLocal[partIndex]) {
			return false, fmt.Errorf("typedcolumn: query-ready direct predicate code=%d outside domain", local)
		}
		if !predicate.allowedLocal[partIndex][local] {
			return false, nil
		}
	}
	return true, nil
}

func (r *QueryReadyOperator) reduceDirectPart(partIndex int, part *queryReadyExecutionPart, rows int, selected []int, expressionSum *int64, stats *QueryReadyExecutionStats) error {
	rowAt := func(index int) int {
		if selected == nil {
			return index
		}
		return selected[index]
	}
	selectedRows := rows
	if selected != nil {
		selectedRows = len(selected)
	}
	switch r.request.Kind {
	case QueryReadyOperatorSumSecondOfDaySquare:
		column := part.direct[r.valueProjected]
		for index := 0; index < selectedRows; index++ {
			seconds := floorUnixSeconds(column.int64AtUnchecked(rowAt(index)))
			secondOfDay := seconds % 86_400
			if secondOfDay < 0 {
				secondOfDay += 86_400
			}
			term := secondOfDay * secondOfDay
			if *expressionSum > math.MaxInt64-term {
				return fmt.Errorf("typedcolumn: query-ready second-of-day square sum overflow current=%d value=%d", *expressionSum, term)
			}
			*expressionSum += term
		}
		return nil
	case QueryReadyOperatorHourCount:
		column := part.direct[r.valueProjected]
		for index := 0; index < selectedRows; index++ {
			secondOfDay := floorUnixSeconds(column.int64AtUnchecked(rowAt(index))) % 86_400
			if secondOfDay < 0 {
				secondOfDay += 86_400
			}
			r.hourCounts[int(secondOfDay/3_600)]++
		}
		return nil
	}
	groupColumn := part.direct[r.groupProjected]
	groupTranslation := r.groupDomain.byPart[partIndex]
	if r.request.Kind == QueryReadyOperatorGroupCount && selected == nil && groupColumn.codeWidth == 1 {
		values := groupColumn.values[:rows]
		for row, localCode := range values {
			group := r.groupDomain.emptyGlobal
			if !groupColumn.absentAtUnchecked(row) {
				if int(localCode) >= len(groupTranslation) || groupTranslation[localCode] < 0 {
					return fmt.Errorf("typedcolumn: query-ready direct group code=%d outside domain", localCode)
				}
				group = groupTranslation[localCode]
				stats.CodeTranslations++
			} else if group < 0 {
				return errors.New("typedcolumn: query-ready direct nullable group has no empty domain")
			}
			r.counts[group]++
		}
		return nil
	}
	for index := 0; index < selectedRows; index++ {
		row := rowAt(index)
		group := r.groupDomain.emptyGlobal
		if !groupColumn.absentAtUnchecked(row) {
			local := groupColumn.codeAtUnchecked(row)
			if int(local) >= len(groupTranslation) || groupTranslation[local] < 0 {
				return fmt.Errorf("typedcolumn: query-ready direct group code=%d outside domain", local)
			}
			group = groupTranslation[local]
			stats.CodeTranslations++
		} else if group < 0 {
			return errors.New("typedcolumn: query-ready direct nullable group has no empty domain")
		}
		switch r.request.Kind {
		case QueryReadyOperatorGroupCount:
			r.counts[group]++
		case QueryReadyOperatorGroupCountDistinct, QueryReadyOperatorGroupCountAndDistinct:
			distinctColumn := part.direct[r.distinctProjected]
			distinct := r.distinctDomain.emptyGlobal
			if !distinctColumn.absentAtUnchecked(row) {
				local := distinctColumn.codeAtUnchecked(row)
				translation := r.distinctDomain.byPart[partIndex]
				if int(local) >= len(translation) || translation[local] < 0 {
					return fmt.Errorf("typedcolumn: query-ready direct distinct code=%d outside domain", local)
				}
				distinct = translation[local]
				stats.CodeTranslations++
			} else if distinct < 0 {
				return errors.New("typedcolumn: query-ready direct nullable distinct has no empty domain")
			}
			r.counts[group]++
			cell := group*len(r.distinctDomain.values) + distinct
			r.distinctBits[cell/64] |= uint64(1) << uint(cell%64)
		case QueryReadyOperatorGroupHourCount:
			secondOfDay := floorUnixSeconds(part.direct[r.valueProjected].int64AtUnchecked(row)) % 86_400
			if secondOfDay < 0 {
				secondOfDay += 86_400
			}
			r.hourCounts[group*24+int(secondOfDay/3_600)]++
		case QueryReadyOperatorGroupMinInt64:
			value := part.direct[r.valueProjected].int64AtUnchecked(row)
			if !r.seen[group] || value < r.int64Values[group] {
				r.int64Values[group], r.seen[group] = value, true
			}
		case QueryReadyOperatorGroupMaxInt64:
			value := part.direct[r.valueProjected].int64AtUnchecked(row)
			if !r.seen[group] || value > r.int64Values[group] {
				r.int64Values[group], r.seen[group] = value, true
			}
		case QueryReadyOperatorGroupInt64Span:
			value := part.direct[r.valueProjected].int64AtUnchecked(row)
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
	}
	return nil
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
	if r.request.Kind == QueryReadyOperatorHourCount {
		secondOfDay := floorUnixSeconds(part.values[r.valueProjected][row]) % 86_400
		if secondOfDay < 0 {
			secondOfDay += 86_400
		}
		r.hourCounts[int(secondOfDay/3_600)]++
		return nil
	}
	group, err := r.globalCodeForRow(r.groupDomain, partIndex, part, r.groupProjected, row, stats)
	if err != nil {
		return err
	}
	switch r.request.Kind {
	case QueryReadyOperatorGroupCount:
		r.counts[group]++
	case QueryReadyOperatorGroupCountDistinct, QueryReadyOperatorGroupCountAndDistinct:
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
	case QueryReadyOperatorGroupMaxInt64:
		value := part.values[r.valueProjected][row]
		if !r.seen[group] || value > r.int64Values[group] {
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

func (r *QueryReadyOperator) shapeGroups(expressionSum int64, stats *QueryReadyExecutionStats) error {
	groupsConsidered := 0
	switch r.request.Kind {
	case QueryReadyOperatorGroupCount:
		for group, count := range r.counts {
			if count != 0 {
				r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Count: count})
			}
		}
	case QueryReadyOperatorGroupCountDistinct, QueryReadyOperatorGroupCountAndDistinct:
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
			groupResult := QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Count: count, DistinctCount: distinct}
			if r.request.Kind == QueryReadyOperatorGroupCountDistinct {
				groupResult.Count, groupResult.DistinctCount = distinct, 0
			}
			r.resultGroups = append(r.resultGroups, groupResult)
		}
	case QueryReadyOperatorHourCount:
		for hour, count := range r.hourCounts {
			if count != 0 {
				r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Hour: hour, Count: count})
			}
		}
	case QueryReadyOperatorGroupHourCount:
		for group, key := range r.groupDomain.values {
			for hour := 0; hour < 24; hour++ {
				if count := r.hourCounts[group*24+hour]; count != 0 {
					r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Key: key, Hour: hour, Count: count})
				}
			}
		}
	case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64:
		for group, seen := range r.seen {
			if seen && (!r.request.SkipEmptyGroupKey || r.groupDomain.values[group] != "") {
				candidate := QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Int64: r.int64Values[group]}
				if r.usesBoundedTopKShape() {
					groupsConsidered++
					r.insertBoundedTopK(candidate)
				} else {
					r.resultGroups = append(r.resultGroups, candidate)
				}
			}
		}
	case QueryReadyOperatorGroupInt64Span:
		for group, seen := range r.seen {
			if seen && (!r.request.SkipEmptyGroupKey || r.groupDomain.values[group] != "") {
				minimum, maximum := r.int64Values[group], r.int64Max[group]
				if minimum < 0 && maximum > math.MaxInt64+minimum {
					return fmt.Errorf("typedcolumn: query-ready int64 span overflow minimum=%d maximum=%d", minimum, maximum)
				}
				candidate := QueryReadyOperatorGroup{Key: r.groupDomain.values[group], Int64: maximum - minimum}
				if r.usesBoundedTopKShape() {
					groupsConsidered++
					r.insertBoundedTopK(candidate)
				} else {
					r.resultGroups = append(r.resultGroups, candidate)
				}
			}
		}
	case QueryReadyOperatorSumSecondOfDaySquare:
		if stats.RowsMatched > 0 {
			r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{Count: stats.RowsMatched, Int64: expressionSum})
		}
	}
	if r.usesBoundedTopKShape() {
		stats.GroupsConsidered = groupsConsidered
	} else {
		stats.GroupsConsidered = len(r.resultGroups)
	}
	return nil
}

func (r *QueryReadyOperator) usesBoundedTopKShape() bool {
	if r == nil || r.request.TopK <= 0 {
		return false
	}
	switch r.request.Kind {
	case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64, QueryReadyOperatorGroupInt64Span:
		return true
	default:
		return false
	}
}

func (r *QueryReadyOperator) insertBoundedTopK(candidate QueryReadyOperatorGroup) {
	limit := r.request.TopK
	index, _ := slices.BinarySearchFunc(r.resultGroups, candidate, func(existing, candidate QueryReadyOperatorGroup) int {
		return r.compareOrderedGroup(existing, candidate)
	})
	if index >= limit {
		return
	}
	if len(r.resultGroups) < limit {
		r.resultGroups = append(r.resultGroups, QueryReadyOperatorGroup{})
	}
	copy(r.resultGroups[index+1:], r.resultGroups[index:len(r.resultGroups)-1])
	r.resultGroups[index] = candidate
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
			if r.request.Kind == QueryReadyOperatorHourCount || r.request.Kind == QueryReadyOperatorGroupHourCount {
				return left.Hour - right.Hour
			}
			return 0
		})
		return
	}
	if r.usesBoundedTopKShape() {
		return
	}
	slices.SortFunc(r.resultGroups, r.compareOrderedGroup)
	if len(r.resultGroups) > r.request.TopK {
		r.resultGroups = r.resultGroups[:r.request.TopK]
	}
}

func (r *QueryReadyOperator) compareOrderedGroup(left, right QueryReadyOperatorGroup) int {
	switch r.request.Kind {
	case QueryReadyOperatorGroupCount, QueryReadyOperatorGroupCountDistinct, QueryReadyOperatorGroupCountAndDistinct:
		if left.Count != right.Count {
			if left.Count > right.Count {
				return -1
			}
			return 1
		}
	case QueryReadyOperatorHourCount, QueryReadyOperatorGroupHourCount:
		if left.Hour != right.Hour {
			return left.Hour - right.Hour
		}
	case QueryReadyOperatorGroupMinInt64, QueryReadyOperatorGroupMaxInt64, QueryReadyOperatorGroupInt64Span:
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
}

func (r *QueryReadyOperator) scratchBytes() int64 {
	bytes := int64(cap(r.counts)+cap(r.hourCounts)+cap(r.matchedRows))*8 + int64(cap(r.seen)) + int64(cap(r.int64Values)+cap(r.int64Max)+cap(r.distinctBits))*8
	bytes += int64(cap(r.fusedMin)+cap(r.fusedMax))*8 + int64(cap(r.fusedSeen))*4
	bytes += int64(cap(r.fusedWorkerErrors))*16 + int64(cap(r.fusedWorkerRows)+cap(r.fusedWorkerCodes))*8
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
