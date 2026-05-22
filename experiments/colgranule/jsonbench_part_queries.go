package colgranule

import (
	"errors"
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
	arena       AggregateArena
	scanner     *ColumnPartScanner
	projected   map[string][]int64
	granules    []EncodedGranule
	codeReaders [4]GranuleReader
	timeReader  GranuleReader
	q2Counts    []uint64
	q2Seen      []uint64
	q3Counts    []uint64
	q4Seen      []uint64
	q5Seen      []uint64
	q5Min       []int64
	q5Max       []int64
	q5Users     []uint32
	counts      map[int64]int64
	unique      map[int64]map[int64]struct{}
	events      []int64
	first       map[int64]int64
	spans       map[int64]jsonBenchPartSpan
	q4Pairs     []jsonBenchPartTimePair
	q5Pairs     []jsonBenchPartSpanPair
}

type jsonBenchPartQueryRunner func(*ColumnPart, queryCodeSet, *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error)

var (
	jsonBenchPartQ2Columns = []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code"}
	jsonBenchPartQ3Columns = []string{"kind_code", "commit_operation_code", "commit_collection_code", "hour_of_day"}
	jsonBenchPartQ4Columns = []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"}
	jsonBenchPartQ5Columns = []string{"kind_code", "commit_operation_code", "commit_collection_code", "did_code", "time_us"}
)

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
	kindBlocks, err := lowCardinalityBlocks(part, "kind_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	operationBlocks, err := lowCardinalityBlocks(part, "commit_operation_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	collectionBlocks, err := lowCardinalityBlocks(part, "commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didColumn, ok := part.Columns["did_code"]
	if !ok {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, errors.New("colgranule: missing column did_code")
	}
	if didColumn.Definition.Type != ColumnTypeLowCardinalityCode {
		return runJSONBenchPartQ2Projected(part, codes, scratch)
	}
	if len(didColumn.Blocks) == 0 {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, errors.New("colgranule: column did_code has no blocks")
	}
	didBlocks := didColumn.Blocks
	if err := validateAlignedBlocks(kindBlocks, operationBlocks, collectionBlocks, didBlocks); err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	collectionCardinality, err := partCodeCardinality(part, "commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didCardinality, err := partCodeCardinality(part, "did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts, seen, didWords, err := scratch.resetQ2Dense(collectionCardinality, didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	for blockIndex := range kindBlocks {
		kindHeader, err := scratch.codeHeader(0, kindBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		operationHeader, err := scratch.codeHeader(1, operationBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		collectionHeader, err := scratch.codeHeader(2, collectionBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		didHeader, err := scratch.codeHeader(3, didBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		rows := kindBlocks[blockIndex].Descriptor.RowCount
		for row := 0; row < rows; row++ {
			kind := readUint32Code(kindHeader.data, kindHeader.width, row)
			if kind >= kindHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
			}
			if int64(kind) != codes.kindCommit {
				continue
			}
			operation := readUint32Code(operationHeader.data, operationHeader.width, row)
			if operation >= operationHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
			}
			if int64(operation) != codes.operationCreate {
				continue
			}
			event := readUint32Code(collectionHeader.data, collectionHeader.width, row)
			if event >= collectionHeader.cardinality || event >= collectionCardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionCardinality)
			}
			did := readUint32Code(didHeader.data, didHeader.width, row)
			if did >= didHeader.cardinality || did >= didCardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", did, didCardinality)
			}
			counts[event]++
			seen[int(event)*didWords+int(did/64)] |= uint64(1) << uint(did%64)
		}
	}
	rows, digest := digestDenseQ2(counts, seen, didWords)
	diagnostics := partColumnDiagnostics(part, jsonBenchPartQ2Columns, "fused_dense_group_count_distinct_codes")
	return rows, digest, diagnostics, nil
}

func runJSONBenchPartQ2Projected(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	scan, err := scanPartProjection(part, scratch, jsonBenchPartQ2Columns)
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
	diagnostics := queryDiagnosticsFromScan(scan.Diagnostics, jsonBenchPartQ2Columns, "projected_scan_group_count_distinct")
	return len(counts), digest, diagnostics, nil
}

func runJSONBenchPartQ3(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	kindBlocks, err := lowCardinalityBlocks(part, "kind_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	operationBlocks, err := lowCardinalityBlocks(part, "commit_operation_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	collectionBlocks, err := lowCardinalityBlocks(part, "commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	hourBlocks, err := lowCardinalityBlocks(part, "hour_of_day")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	if err := validateAlignedBlocks(kindBlocks, operationBlocks, collectionBlocks, hourBlocks); err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	collectionCardinality, err := partCodeCardinality(part, "commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	counts, err := scratch.resetQ3Dense(collectionCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	for blockIndex := range kindBlocks {
		kindHeader, err := scratch.codeHeader(0, kindBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		operationHeader, err := scratch.codeHeader(1, operationBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		collectionHeader, err := scratch.codeHeader(2, collectionBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		hourHeader, err := scratch.codeHeader(3, hourBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		rows := kindBlocks[blockIndex].Descriptor.RowCount
		for row := 0; row < rows; row++ {
			kind := readUint32Code(kindHeader.data, kindHeader.width, row)
			if kind >= kindHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
			}
			if int64(kind) != codes.kindCommit {
				continue
			}
			operation := readUint32Code(operationHeader.data, operationHeader.width, row)
			if operation >= operationHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
			}
			if int64(operation) != codes.operationCreate {
				continue
			}
			event := readUint32Code(collectionHeader.data, collectionHeader.width, row)
			if event >= collectionHeader.cardinality || event >= collectionCardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionCardinality)
			}
			if int64(event) != codes.collectionPost && int64(event) != codes.collectionRepost && int64(event) != codes.collectionLike {
				continue
			}
			hour := readUint32Code(hourHeader.data, hourHeader.width, row)
			if hour >= hourHeader.cardinality || hour >= jsonBenchHoursPerDay {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: hour_of_day code %d outside cardinality %d", hour, hourHeader.cardinality)
			}
			counts[int(event)*jsonBenchHoursPerDay+int(hour)]++
		}
	}
	rows, digest := digestDenseQ3(counts)
	diagnostics := partColumnDiagnostics(part, jsonBenchPartQ3Columns, "fused_dense_group_count_hour_codes")
	return rows, digest, diagnostics, nil
}

func runJSONBenchPartQ4(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	if err := requireTimeUSAscendingSortKey(part); err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	kindBlocks, err := lowCardinalityBlocks(part, "kind_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	operationBlocks, err := lowCardinalityBlocks(part, "commit_operation_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	collectionBlocks, err := lowCardinalityBlocks(part, "commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didBlocks, err := lowCardinalityBlocks(part, "did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	timeBlocks, err := int64Blocks(part, "time_us")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	if err := validateAlignedBlocks(kindBlocks, operationBlocks, collectionBlocks, didBlocks, timeBlocks); err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didCardinality, err := partCodeCardinality(part, "did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, err := scratch.resetQ4Seen(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	top := scratch.q4Pairs[:0]
	rowsScanned := 0
	blocksDecoded := 0
	bytesDecoded := 0
	lastGranule := -1
	for blockIndex := range kindBlocks {
		kindHeader, err := scratch.codeHeader(0, kindBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		operationHeader, err := scratch.codeHeader(1, operationBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		collectionHeader, err := scratch.codeHeader(2, collectionBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		didHeader, err := scratch.codeHeader(3, didBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		timeCursor, err := scratch.timeReader.int64Cursor(timeBlocks[blockIndex].Granule)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		blocksDecoded += 5
		bytesDecoded += kindBlocks[blockIndex].Granule.RawBytes
		bytesDecoded += operationBlocks[blockIndex].Granule.RawBytes
		bytesDecoded += collectionBlocks[blockIndex].Granule.RawBytes
		bytesDecoded += didBlocks[blockIndex].Granule.RawBytes
		if kindBlocks[blockIndex].Descriptor.LastGranule > lastGranule {
			lastGranule = kindBlocks[blockIndex].Descriptor.LastGranule
		}
		rows := kindBlocks[blockIndex].Descriptor.RowCount
		for row := 0; row < rows; row++ {
			timestamp, err := timeCursor.Next()
			if err != nil {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, err
			}
			rowsScanned++
			if len(top) == 3 && timestamp > top[2].t {
				scratch.q4Pairs = top
				bytesDecoded += timeCursor.RawBytesRead()
				diagnostics := queryDiagnosticsFromDecodedPrefix(jsonBenchPartQ4Columns, "sort_key_early_stop_min_by_user", rowsScanned, lastGranule, blocksDecoded, bytesDecoded)
				return len(top), digestQ4Top(top), diagnostics, nil
			}
			kind := readUint32Code(kindHeader.data, kindHeader.width, row)
			if kind >= kindHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
			}
			if int64(kind) != codes.kindCommit {
				continue
			}
			operation := readUint32Code(operationHeader.data, operationHeader.width, row)
			if operation >= operationHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
			}
			if int64(operation) != codes.operationCreate {
				continue
			}
			event := readUint32Code(collectionHeader.data, collectionHeader.width, row)
			if event >= collectionHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionHeader.cardinality)
			}
			if int64(event) != codes.collectionPost {
				continue
			}
			user := readUint32Code(didHeader.data, didHeader.width, row)
			if user >= didHeader.cardinality || user >= didCardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", user, didCardinality)
			}
			if bitsetTestAndSet(seen, user) {
				continue
			}
			top = insertQ4Top(top, jsonBenchPartTimePair{user: int64(user), t: timestamp})
		}
		if err := timeCursor.Finish(); err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		bytesDecoded += timeCursor.RawBytesRead()
	}
	scratch.q4Pairs = top
	diagnostics := queryDiagnosticsFromDecodedPrefix(jsonBenchPartQ4Columns, "sort_key_early_stop_min_by_user", rowsScanned, lastGranule, blocksDecoded, bytesDecoded)
	return len(top), digestQ4Top(top), diagnostics, nil
}

func runJSONBenchPartQ5(part *ColumnPart, codes queryCodeSet, scratch *jsonBenchPartQueryScratch) (int, uint64, JSONBenchPartQueryDiagnostics, error) {
	kindBlocks, err := lowCardinalityBlocks(part, "kind_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	operationBlocks, err := lowCardinalityBlocks(part, "commit_operation_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	collectionBlocks, err := lowCardinalityBlocks(part, "commit_collection_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didBlocks, err := lowCardinalityBlocks(part, "did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	timeBlocks, err := int64Blocks(part, "time_us")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	if err := validateAlignedBlocks(kindBlocks, operationBlocks, collectionBlocks, didBlocks, timeBlocks); err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	didCardinality, err := partCodeCardinality(part, "did_code")
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	seen, minTime, maxTime, users, err := scratch.resetQ5Dense(didCardinality)
	if err != nil {
		return 0, 0, JSONBenchPartQueryDiagnostics{}, err
	}
	for blockIndex := range kindBlocks {
		kindHeader, err := scratch.codeHeader(0, kindBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		operationHeader, err := scratch.codeHeader(1, operationBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		collectionHeader, err := scratch.codeHeader(2, collectionBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		didHeader, err := scratch.codeHeader(3, didBlocks[blockIndex])
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		timeValues, err := scratch.timeReader.DecodeInt64(timeBlocks[blockIndex].Granule)
		if err != nil {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, err
		}
		rows := kindBlocks[blockIndex].Descriptor.RowCount
		if len(timeValues) != rows {
			return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: q5 time/code row mismatch time=%d codes=%d", len(timeValues), rows)
		}
		for row := 0; row < rows; row++ {
			kind := readUint32Code(kindHeader.data, kindHeader.width, row)
			if kind >= kindHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: kind code %d outside cardinality %d", kind, kindHeader.cardinality)
			}
			if int64(kind) != codes.kindCommit {
				continue
			}
			operation := readUint32Code(operationHeader.data, operationHeader.width, row)
			if operation >= operationHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: operation code %d outside cardinality %d", operation, operationHeader.cardinality)
			}
			if int64(operation) != codes.operationCreate {
				continue
			}
			event := readUint32Code(collectionHeader.data, collectionHeader.width, row)
			if event >= collectionHeader.cardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: collection code %d outside cardinality %d", event, collectionHeader.cardinality)
			}
			if int64(event) != codes.collectionPost {
				continue
			}
			user := readUint32Code(didHeader.data, didHeader.width, row)
			if user >= didHeader.cardinality || user >= didCardinality {
				return 0, 0, JSONBenchPartQueryDiagnostics{}, fmt.Errorf("colgranule: did code %d outside cardinality %d", user, didCardinality)
			}
			timestamp := timeValues[row]
			if bitsetTestAndSet(seen, user) {
				if timestamp < minTime[user] {
					minTime[user] = timestamp
				}
				if timestamp > maxTime[user] {
					maxTime[user] = timestamp
				}
				continue
			}
			minTime[user] = timestamp
			maxTime[user] = timestamp
			users = append(users, user)
		}
	}
	scratch.q5Users = users
	top := scratch.q5Pairs[:0]
	for _, user := range users {
		top = insertQ5Top(top, jsonBenchPartSpanPair{user: int64(user), span: maxTime[user] - minTime[user]})
	}
	scratch.q5Pairs = top
	diagnostics := partColumnDiagnostics(part, jsonBenchPartQ5Columns, "fused_dense_span_by_user")
	return len(top), digestQ5Top(top), diagnostics, nil
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

func lowCardinalityBlocks(part *ColumnPart, column string) ([]ColumnBlock, error) {
	return typedColumnBlocks(part, column, ColumnTypeLowCardinalityCode)
}

func int64Blocks(part *ColumnPart, column string) ([]ColumnBlock, error) {
	return typedColumnBlocks(part, column, ColumnTypeInt64)
}

func typedColumnBlocks(part *ColumnPart, column string, columnType ColumnType) ([]ColumnBlock, error) {
	c, ok := part.Columns[column]
	if !ok {
		return nil, fmt.Errorf("colgranule: missing column %s", column)
	}
	if c.Definition.Type != columnType {
		return nil, fmt.Errorf("colgranule: column %s type=%s want %s", column, c.Definition.Type, columnType)
	}
	if len(c.Blocks) == 0 {
		return nil, fmt.Errorf("colgranule: column %s has no blocks", column)
	}
	return c.Blocks, nil
}

func validateAlignedBlocks(reference []ColumnBlock, others ...[]ColumnBlock) error {
	for _, blocks := range others {
		if len(blocks) != len(reference) {
			return fmt.Errorf("colgranule: aligned kernel block count=%d want %d", len(blocks), len(reference))
		}
		for i := range reference {
			refDesc := reference[i].Descriptor
			desc := blocks[i].Descriptor
			if desc.FirstRow != refDesc.FirstRow || desc.RowCount != refDesc.RowCount {
				return fmt.Errorf("colgranule: aligned kernel block %d rows=%d+%d want %d+%d", i, desc.FirstRow, desc.RowCount, refDesc.FirstRow, refDesc.RowCount)
			}
			if desc.FirstGranule != refDesc.FirstGranule || desc.LastGranule != refDesc.LastGranule {
				return fmt.Errorf("colgranule: aligned kernel block %d granules=%d..%d want %d..%d", i, desc.FirstGranule, desc.LastGranule, refDesc.FirstGranule, refDesc.LastGranule)
			}
		}
	}
	return nil
}

func requireTimeUSAscendingSortKey(part *ColumnPart) error {
	if len(part.Descriptor.SortKey) != 1 {
		return fmt.Errorf("colgranule: q4 early-stop requires single-column sort key, got %d columns", len(part.Descriptor.SortKey))
	}
	key := part.Descriptor.SortKey[0]
	if key.Column != "time_us" || key.Direction != SortKeyAsc {
		return fmt.Errorf("colgranule: q4 early-stop requires ascending time_us sort key, got %+v", key)
	}
	return nil
}

func (s *jsonBenchPartQueryScratch) codeHeader(reader int, block ColumnBlock) (uint32CodesHeader, error) {
	if reader < 0 || reader >= len(s.codeReaders) {
		return uint32CodesHeader{}, fmt.Errorf("colgranule: invalid code reader %d", reader)
	}
	raw, err := s.codeReaders[reader].decompressPayload(block.Granule)
	if err != nil {
		return uint32CodesHeader{}, err
	}
	return parseUint32CodesHeader(raw, block.Descriptor.RowCount)
}

func (s *jsonBenchPartQueryScratch) resetQ4Seen(didCardinality uint32) ([]uint64, error) {
	if didCardinality == 0 {
		return nil, errors.New("colgranule: invalid q4 did cardinality 0")
	}
	words := (int(didCardinality) + 63) / 64
	if words <= 0 || words > maxAggregateCells {
		return nil, fmt.Errorf("colgranule: q4 seen words=%d exceeds cap %d", words, maxAggregateCells)
	}
	if cap(s.q4Seen) < words {
		s.q4Seen = make([]uint64, words)
	} else {
		s.q4Seen = s.q4Seen[:words]
	}
	clear(s.q4Seen)
	s.q4Pairs = s.q4Pairs[:0]
	return s.q4Seen, nil
}

func (s *jsonBenchPartQueryScratch) resetQ5Dense(didCardinality uint32) ([]uint64, []int64, []int64, []uint32, error) {
	if didCardinality == 0 {
		return nil, nil, nil, nil, errors.New("colgranule: invalid q5 did cardinality 0")
	}
	cardinality := int(didCardinality)
	words := (cardinality + 63) / 64
	if words <= 0 || words > maxAggregateCells {
		return nil, nil, nil, nil, fmt.Errorf("colgranule: q5 seen words=%d exceeds cap %d", words, maxAggregateCells)
	}
	if cardinality > maxAggregateCells {
		return nil, nil, nil, nil, fmt.Errorf("colgranule: q5 dense cardinality=%d exceeds cap %d", cardinality, maxAggregateCells)
	}
	if cap(s.q5Seen) < words {
		s.q5Seen = make([]uint64, words)
	} else {
		s.q5Seen = s.q5Seen[:words]
	}
	clear(s.q5Seen)
	if cap(s.q5Min) < cardinality {
		s.q5Min = make([]int64, cardinality)
	} else {
		s.q5Min = s.q5Min[:cardinality]
	}
	if cap(s.q5Max) < cardinality {
		s.q5Max = make([]int64, cardinality)
	} else {
		s.q5Max = s.q5Max[:cardinality]
	}
	s.q5Users = s.q5Users[:0]
	s.q5Pairs = s.q5Pairs[:0]
	return s.q5Seen, s.q5Min, s.q5Max, s.q5Users, nil
}

func (s *jsonBenchPartQueryScratch) resetQ2Dense(eventCardinality uint32, didCardinality uint32) ([]uint64, []uint64, int, error) {
	if eventCardinality == 0 || didCardinality == 0 {
		return nil, nil, 0, fmt.Errorf("colgranule: invalid q2 cardinalities event=%d did=%d", eventCardinality, didCardinality)
	}
	didWords := (int(didCardinality) + 63) / 64
	seenWords := int(eventCardinality) * didWords
	if seenWords <= 0 || seenWords > maxAggregateCells {
		return nil, nil, 0, fmt.Errorf("colgranule: q2 distinct bitset words=%d exceeds cap %d", seenWords, maxAggregateCells)
	}
	if cap(s.q2Counts) < int(eventCardinality) {
		s.q2Counts = make([]uint64, eventCardinality)
	} else {
		s.q2Counts = s.q2Counts[:eventCardinality]
	}
	clear(s.q2Counts)
	if cap(s.q2Seen) < seenWords {
		s.q2Seen = make([]uint64, seenWords)
	} else {
		s.q2Seen = s.q2Seen[:seenWords]
	}
	clear(s.q2Seen)
	return s.q2Counts, s.q2Seen, didWords, nil
}

func (s *jsonBenchPartQueryScratch) resetQ3Dense(eventCardinality uint32) ([]uint64, error) {
	if eventCardinality == 0 {
		return nil, errors.New("colgranule: invalid q3 event cardinality 0")
	}
	cells := int(eventCardinality) * jsonBenchHoursPerDay
	if cells <= 0 || cells > maxAggregateCells {
		return nil, fmt.Errorf("colgranule: q3 aggregate cells=%d exceeds cap %d", cells, maxAggregateCells)
	}
	if cap(s.q3Counts) < cells {
		s.q3Counts = make([]uint64, cells)
	} else {
		s.q3Counts = s.q3Counts[:cells]
	}
	clear(s.q3Counts)
	return s.q3Counts, nil
}

func bitsetTestAndSet(words []uint64, code uint32) bool {
	word := int(code / 64)
	bit := uint(code % 64)
	mask := uint64(1) << bit
	seen := words[word]&mask != 0
	words[word] |= mask
	return seen
}

func insertQ4Top(top []jsonBenchPartTimePair, candidate jsonBenchPartTimePair) []jsonBenchPartTimePair {
	if len(top) == 3 && !q4PairLess(candidate, top[2]) {
		return top
	}
	if len(top) < 3 {
		top = append(top, candidate)
	} else {
		top[2] = candidate
	}
	for i := len(top) - 1; i > 0 && q4PairLess(top[i], top[i-1]); i-- {
		top[i], top[i-1] = top[i-1], top[i]
	}
	return top
}

func q4PairLess(left jsonBenchPartTimePair, right jsonBenchPartTimePair) bool {
	if left.t == right.t {
		return left.user < right.user
	}
	return left.t < right.t
}

func insertQ5Top(top []jsonBenchPartSpanPair, candidate jsonBenchPartSpanPair) []jsonBenchPartSpanPair {
	if len(top) == 3 && !q5PairLess(candidate, top[2]) {
		return top
	}
	if len(top) < 3 {
		top = append(top, candidate)
	} else {
		top[2] = candidate
	}
	for i := len(top) - 1; i > 0 && q5PairLess(top[i], top[i-1]); i-- {
		top[i], top[i-1] = top[i-1], top[i]
	}
	return top
}

func q5PairLess(left jsonBenchPartSpanPair, right jsonBenchPartSpanPair) bool {
	if left.span == right.span {
		return left.user < right.user
	}
	return left.span > right.span
}

func digestQ4Top(top []jsonBenchPartTimePair) uint64 {
	var digest uint64
	for _, p := range top {
		digest = digestMix(digest, uint64(p.user), uint64(p.t))
	}
	return digest
}

func digestQ5Top(top []jsonBenchPartSpanPair) uint64 {
	var digest uint64
	for _, p := range top {
		digest = digestMix(digest, uint64(p.user), uint64(p.span))
	}
	return digest
}

func queryDiagnosticsFromDecodedPrefix(columns []string, kernel string, rowsScanned int, lastGranule int, blocksDecoded int, bytesDecoded int) JSONBenchPartQueryDiagnostics {
	granulesDecoded := 0
	if lastGranule >= 0 {
		granulesDecoded = lastGranule + 1
	}
	return JSONBenchPartQueryDiagnostics{
		RowsScanned:        rowsScanned,
		GranulesConsidered: granulesDecoded,
		GranulesDecoded:    granulesDecoded,
		BlocksDecoded:      blocksDecoded,
		BytesDecoded:       bytesDecoded,
		ColumnsProjected:   append([]string(nil), columns...),
		AggregateKernel:    kernel,
	}
}

func digestDenseQ2(counts []uint64, seen []uint64, didWords int) (int, uint64) {
	var digest uint64
	rows := 0
	for event, count := range counts {
		if count == 0 {
			continue
		}
		rows++
		digest = digestMix(digest, uint64(event), count)
	}
	for event, count := range counts {
		if count == 0 {
			continue
		}
		distinct := 0
		eventSeen := seen[event*didWords : event*didWords+didWords]
		for _, word := range eventSeen {
			distinct += popcount64(word)
		}
		digest = digestMix(digest, uint64(event), uint64(distinct))
	}
	return rows, digest
}

func digestDenseQ3(counts []uint64) (int, uint64) {
	var digest uint64
	rows := 0
	for cell, count := range counts {
		if count == 0 {
			continue
		}
		rows++
		event := cell / jsonBenchHoursPerDay
		hour := cell % jsonBenchHoursPerDay
		digest = digestMix(digest, uint64(event*100+hour), count)
	}
	return rows, digest
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
	if column == "hour_of_day" {
		return jsonBenchHoursPerDay, true
	}
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
