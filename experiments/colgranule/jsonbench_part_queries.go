package colgranule

import (
	"fmt"
	"sort"
	"time"
)

type JSONBenchPartQueryTiming struct {
	Query        string                        `json:"query"`
	Description  string                        `json:"description"`
	Engine       string                        `json:"engine"`
	Attempts     []JSONBenchPartQueryAttempt   `json:"attempts"`
	Best         time.Duration                 `json:"best"`
	BestCache    string                        `json:"best_cache"`
	ResultRows   int                           `json:"result_rows"`
	ResultDigest uint64                        `json:"result_digest"`
	Diagnostics  JSONBenchPartQueryDiagnostics `json:"diagnostics"`
}

type JSONBenchPartQueryAttempt struct {
	Cache        string                        `json:"cache"`
	Duration     time.Duration                 `json:"duration"`
	ResultRows   int                           `json:"result_rows"`
	ResultDigest uint64                        `json:"result_digest"`
	Diagnostics  JSONBenchPartQueryDiagnostics `json:"diagnostics"`
}

type JSONBenchPartQueryDiagnostics struct {
	RowsScanned        int      `json:"rows_scanned"`
	GranulesConsidered int      `json:"granules_considered"`
	GranulesSkipped    int      `json:"granules_skipped"`
	GranulesDecoded    int      `json:"granules_decoded"`
	BlocksDecoded      int      `json:"blocks_decoded"`
	BytesDecoded       int      `json:"bytes_decoded"`
	ColumnsProjected   []string `json:"columns_projected"`
	AggregateKernel    string   `json:"aggregate_kernel"`
	CacheState         string   `json:"cache_state"`
}

type jsonBenchPartQueryScratch struct {
	arena     AggregateArena
	scanner   *ColumnPartScanner
	projected map[string][]int64
	granules  []EncodedGranule
	counts    map[int64]int64
	unique    map[int64]map[int64]struct{}
	events    []int64
	first     map[int64]int64
	spans     map[int64]jsonBenchPartSpan
	q4Pairs   []jsonBenchPartTimePair
	q5Pairs   []jsonBenchPartSpanPair
}

type jsonBenchPartQueryRunner func(*ColumnPart, queryCodeSet, *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error)

func BuildJSONBenchColumnPart(ds JSONBenchDataset, rowsPerGranule int) (*ColumnPart, error) {
	opts, err := JSONBenchColumnPartOptions(ds, rowsPerGranule)
	if err != nil {
		return nil, err
	}
	return BuildColumnPart(1, opts, ColumnBatch{Rows: ds.Rows, Columns: ds.Columns})
}

func JSONBenchColumnPartOptions(ds JSONBenchDataset, rowsPerGranule int) (ColumnStoreOptions, error) {
	if ds.Rows <= 0 {
		return ColumnStoreOptions{}, fmt.Errorf("colgranule: JSONBench part requires positive rows, got %d", ds.Rows)
	}
	if rowsPerGranule <= 0 {
		rowsPerGranule = DefaultRowsPerGranule
	}
	names := ds.ColumnNames()
	defs := make([]ColumnDefinition, 0, len(names))
	for _, name := range names {
		values := ds.Columns[name]
		if len(values) != ds.Rows {
			return ColumnStoreOptions{}, fmt.Errorf("colgranule: column %s rows=%d want=%d", name, len(values), ds.Rows)
		}
		def := ColumnDefinition{
			Name:        name,
			Type:        ColumnTypeInt64,
			Encoding:    EncodingDeltaVarint,
			Compression: CompressionLZ4,
		}
		if isJSONBenchMonotonicColumn(name) {
			def.Encoding = EncodingDoubleDeltaVarint
		}
		if isJSONBenchBoolColumn(name) {
			def.Type = ColumnTypeBool
			def.Encoding = EncodingBoolBitpackRLE
		}
		if cardinality, ok := jsonBenchCodeCardinality(ds, name); ok && cardinality <= maxCodeCardinality {
			def.Type = ColumnTypeLowCardinalityCode
			def.Encoding = EncodingLowCardinalityUint32
			def.Cardinality = cardinality
		}
		defs = append(defs, def)
	}
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns:       defs,
		LogicalPrimaryKey: LogicalPrimaryKey{
			Columns: []string{"row_index"},
		},
		SortKey: SortKey{
			Columns: []SortKeyColumn{{Column: "time_us"}},
		},
		PartPolicy: ColumnPartPolicy{
			RowsPerGranule:        rowsPerGranule,
			DefaultCodecBlockRows: rowsPerGranule,
		},
	}, nil
}

func RunJSONBenchPartQueries(ds JSONBenchDataset, rowsPerGranule int, attempts int) ([]JSONBenchPartQueryTiming, error) {
	if attempts <= 0 {
		attempts = 3
	}
	part, err := BuildJSONBenchColumnPart(ds, rowsPerGranule)
	if err != nil {
		return nil, err
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		return nil, err
	}
	queries := []struct {
		name        string
		description string
		run         jsonBenchPartQueryRunner
	}{
		{"Q1", "Top event types", runJSONBenchPartQ1},
		{"Q2", "Top event types with unique users", runJSONBenchPartQ2},
		{"Q3", "Event counts by hour", runJSONBenchPartQ3},
		{"Q4", "Top 3 post veterans", runJSONBenchPartQ4},
		{"Q5", "Top 3 users with longest activity", runJSONBenchPartQ5},
	}
	out := make([]JSONBenchPartQueryTiming, 0, len(queries))
	for _, q := range queries {
		timing := JSONBenchPartQueryTiming{
			Query:       q.name,
			Description: q.description,
			Engine:      "encoded_column_part",
			Attempts:    make([]JSONBenchPartQueryAttempt, 0, attempts),
		}
		scratch := &jsonBenchPartQueryScratch{
			scanner:   part.NewScanner(),
			projected: make(map[string][]int64, 6),
		}
		for i := 0; i < attempts; i++ {
			cache := "cold"
			if i > 0 {
				cache = "warm"
			}
			start := time.Now()
			rows, digest, diagnostics, err := q.run(part, codes, scratch)
			if err != nil {
				return nil, fmt.Errorf("%s encoded part query: %w", q.name, err)
			}
			elapsed := time.Since(start)
			diagnostics.CacheState = cache
			attempt := JSONBenchPartQueryAttempt{
				Cache:        cache,
				Duration:     elapsed,
				ResultRows:   rows,
				ResultDigest: digest,
				Diagnostics:  diagnostics,
			}
			timing.Attempts = append(timing.Attempts, attempt)
			timing.ResultRows = rows
			timing.ResultDigest = digest
			if timing.Best == 0 || elapsed < timing.Best {
				timing.Best = elapsed
				timing.BestCache = cache
				timing.Diagnostics = diagnostics
			}
		}
		out = append(out, timing)
	}
	return out, nil
}

func runJSONBenchPartQ1(part *ColumnPart, _ queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	const column = "commit_collection_code"
	blocks, err := scratch.columnGranules(part, column)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	cardinality, err := partCodeCardinality(part, column)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts, err := scratch.arena.GroupedCountCodes(blocks, cardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	rows, digest := digestUint64Counts(counts)
	diagnostics := partColumnDiagnostics(part, []string{column}, "grouped_count_codes")
	return rows, digest, diagnostics, nil
}

func (s *jsonBenchPartQueryScratch) columnGranules(part *ColumnPart, column string) ([]EncodedGranule, error) {
	c, ok := part.Columns[column]
	if !ok {
		return nil, fmt.Errorf("colgranule: missing column %s", column)
	}
	if cap(s.granules) < len(c.Blocks) {
		s.granules = make([]EncodedGranule, len(c.Blocks))
	} else {
		s.granules = s.granules[:len(c.Blocks)]
	}
	for i, block := range c.Blocks {
		s.granules[i] = block.Granule
	}
	return s.granules, nil
}

func runJSONBenchPartQ2(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	projection := []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code"}
	scan, err := scanPartProjection(part, scratch, projection)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	did := scan.Columns["did_code"]
	counts := scratch.resetCounts()
	unique, events := scratch.resetUnique()
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate {
			continue
		}
		event := collection[i]
		if counts[event] == 0 {
			events = append(events, event)
		}
		counts[event]++
		if unique[event] == nil {
			unique[event] = make(map[int64]struct{})
		}
		unique[event][did[i]] = struct{}{}
	}
	scratch.events = events
	digest := digestCounts(counts)
	sort.Slice(events, func(i, j int) bool { return events[i] < events[j] })
	for _, event := range events {
		users := unique[event]
		digest = digestMix(digest, uint64(event), uint64(len(users)))
	}
	diagnostics := queryDiagnosticsFromScan(scan.Diagnostics, projection, "projected_scan_group_count_distinct")
	return len(counts), digest, diagnostics, nil
}

func runJSONBenchPartQ3(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	projection := []string{"kind_code", "commit_operation_code", "commit_collection_code", "time_us"}
	scan, err := scanPartProjection(part, scratch, projection)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	timeUS := scan.Columns["time_us"]
	counts := scratch.resetCounts()
	for i := range collection {
		event := collection[i]
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate {
			continue
		}
		if event != codes.collectionPost && event != codes.collectionRepost && event != codes.collectionLike {
			continue
		}
		hour := unixMicroHour(timeUS[i])
		counts[event*100+hour]++
	}
	diagnostics := queryDiagnosticsFromScan(scan.Diagnostics, projection, "projected_scan_group_count_hour")
	return len(counts), digestCounts(counts), diagnostics, nil
}

func runJSONBenchPartQ4(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	projection := []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"}
	scan, err := scanPartProjection(part, scratch, projection)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	did := scan.Columns["did_code"]
	timeUS := scan.Columns["time_us"]
	first := scratch.resetFirst()
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate || collection[i] != codes.collectionPost {
			continue
		}
		user := did[i]
		if prev, ok := first[user]; !ok || timeUS[i] < prev {
			first[user] = timeUS[i]
		}
	}
	top := scratch.q4Pairs[:0]
	for user, t := range first {
		top = append(top, jsonBenchPartTimePair{user: user, t: t})
	}
	scratch.q4Pairs = top
	sort.Slice(top, func(i, j int) bool {
		if top[i].t == top[j].t {
			return top[i].user < top[j].user
		}
		return top[i].t < top[j].t
	})
	if len(top) > 3 {
		top = top[:3]
	}
	var digest uint64
	for _, p := range top {
		digest = digestMix(digest, uint64(p.user), uint64(p.t))
	}
	diagnostics := queryDiagnosticsFromScan(scan.Diagnostics, projection, "projected_scan_min_by_user")
	return len(top), digest, diagnostics, nil
}

func runJSONBenchPartQ5(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	projection := []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"}
	scan, err := scanPartProjection(part, scratch, projection)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kind := scan.Columns["kind_code"]
	operation := scan.Columns["commit_operation_code"]
	collection := scan.Columns["commit_collection_code"]
	did := scan.Columns["did_code"]
	timeUS := scan.Columns["time_us"]
	spans := scratch.resetSpans()
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate || collection[i] != codes.collectionPost {
			continue
		}
		user := did[i]
		s, ok := spans[user]
		if !ok {
			spans[user] = jsonBenchPartSpan{min: timeUS[i], max: timeUS[i]}
			continue
		}
		if timeUS[i] < s.min {
			s.min = timeUS[i]
		}
		if timeUS[i] > s.max {
			s.max = timeUS[i]
		}
		spans[user] = s
	}
	top := scratch.q5Pairs[:0]
	for user, s := range spans {
		top = append(top, jsonBenchPartSpanPair{user: user, span: s.max - s.min})
	}
	scratch.q5Pairs = top
	sort.Slice(top, func(i, j int) bool {
		if top[i].span == top[j].span {
			return top[i].user < top[j].user
		}
		return top[i].span > top[j].span
	})
	if len(top) > 3 {
		top = top[:3]
	}
	var digest uint64
	for _, p := range top {
		digest = digestMix(digest, uint64(p.user), uint64(p.span))
	}
	diagnostics := queryDiagnosticsFromScan(scan.Diagnostics, projection, "projected_scan_span_by_user")
	return len(top), digest, diagnostics, nil
}

type jsonBenchPartSpan struct {
	min int64
	max int64
}

type jsonBenchPartTimePair struct {
	user int64
	t    int64
}

type jsonBenchPartSpanPair struct {
	user int64
	span int64
}

func (s *jsonBenchPartQueryScratch) resetCounts() map[int64]int64 {
	if s.counts == nil {
		s.counts = make(map[int64]int64, 128)
	} else {
		for k := range s.counts {
			delete(s.counts, k)
		}
	}
	return s.counts
}

func (s *jsonBenchPartQueryScratch) resetUnique() (map[int64]map[int64]struct{}, []int64) {
	if s.unique == nil {
		s.unique = make(map[int64]map[int64]struct{}, 16)
	} else {
		for _, users := range s.unique {
			for user := range users {
				delete(users, user)
			}
		}
	}
	s.events = s.events[:0]
	return s.unique, s.events
}

func (s *jsonBenchPartQueryScratch) resetFirst() map[int64]int64 {
	if s.first == nil {
		s.first = make(map[int64]int64, 128*1024)
	} else {
		for k := range s.first {
			delete(s.first, k)
		}
	}
	return s.first
}

func (s *jsonBenchPartQueryScratch) resetSpans() map[int64]jsonBenchPartSpan {
	if s.spans == nil {
		s.spans = make(map[int64]jsonBenchPartSpan, 128*1024)
	} else {
		for k := range s.spans {
			delete(s.spans, k)
		}
	}
	return s.spans
}

func digestUint64Counts(counts []uint64) (int, uint64) {
	var digest uint64
	rows := 0
	for code, count := range counts {
		if count == 0 {
			continue
		}
		rows++
		digest = digestMix(digest, uint64(code), count)
	}
	return rows, digest
}

func scanPartProjection(part *ColumnPart, scratch *jsonBenchPartQueryScratch, projection []string) (ProjectedScanResult, error) {
	if scratch.scanner == nil {
		scratch.scanner = part.NewScanner()
	}
	if scratch.projected == nil {
		scratch.projected = make(map[string][]int64, len(projection))
	}
	return scratch.scanner.ScanProjectedInto(scratch.projected, projection)
}

func queryDiagnosticsFromScan(scan PartScanDiagnostics, columns []string, kernel string) JSONBenchPartQueryDiagnostics {
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:        scan.RowsScanned,
		GranulesConsidered: scan.GranulesConsidered,
		GranulesDecoded:    scan.GranulesDecoded,
		BlocksDecoded:      scan.BlocksDecoded,
		BytesDecoded:       scan.BytesDecoded,
		ColumnsProjected:   append([]string(nil), columns...),
		AggregateKernel:    kernel,
	}
}

func partColumnDiagnostics(part *ColumnPart, columns []string, kernel string) JSONBenchPartQueryDiagnostics {
	var blocks int
	var bytes int
	var granulesDecoded int
	for _, name := range columns {
		column := part.Columns[name]
		columnGranules, err := countGranulesCoveredByBlocks(column.Blocks)
		if err == nil && columnGranules > granulesDecoded {
			granulesDecoded = columnGranules
		}
		for _, block := range column.Blocks {
			blocks++
			bytes += block.Granule.RawBytes
		}
	}
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:        part.Descriptor.RowCount,
		GranulesConsidered: len(part.Descriptor.Granules),
		GranulesDecoded:    granulesDecoded,
		BlocksDecoded:      blocks,
		BytesDecoded:       bytes,
		ColumnsProjected:   append([]string(nil), columns...),
		AggregateKernel:    kernel,
	}
}

func partCodeCardinality(part *ColumnPart, column string) (uint32, error) {
	c, ok := part.Columns[column]
	if !ok {
		return 0, fmt.Errorf("colgranule: missing column %s", column)
	}
	if c.Definition.Type != ColumnTypeLowCardinalityCode {
		return 0, fmt.Errorf("colgranule: column %s is %s, not low-cardinality code", column, c.Definition.Type)
	}
	return c.Definition.Cardinality, nil
}

func jsonBenchCodeCardinality(ds JSONBenchDataset, column string) (uint32, bool) {
	dict := ds.Dictionaries[column]
	if dict == nil {
		return 0, false
	}
	maxCode := int64(0)
	for _, code := range dict {
		if code > maxCode {
			maxCode = code
		}
	}
	if maxCode < 0 || maxCode >= maxCodeCardinality {
		return 0, false
	}
	return uint32(maxCode + 1), true
}

func isJSONBenchMonotonicColumn(name string) bool {
	switch name {
	case "row_index", "time_us", "record_created_at_unix_ms":
		return true
	default:
		return false
	}
}

func isJSONBenchBoolColumn(name string) bool {
	switch name {
	case "record_has_reply", "record_has_subject":
		return true
	default:
		return false
	}
}
