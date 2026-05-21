package colgranule

import (
	"errors"
	"fmt"
)

const maxAggregateCells = 1 << 20

type AggregateArena struct {
	reader       GranuleReader
	counts       []uint64
	bucketCounts []uint64
	codeBits     []uint64
	seenInt64    map[int64]struct{}
}

func (a *AggregateArena) Reset() {
	a.counts = a.counts[:0]
	a.bucketCounts = a.bucketCounts[:0]
	a.codeBits = a.codeBits[:0]
	clear(a.seenInt64)
}

// GroupedCountCodes returns arena-owned count storage. The returned slice is
// valid only until the next AggregateArena operation or Reset.
func (a *AggregateArena) GroupedCountCodes(granules []EncodedGranule, cardinality uint32) ([]uint64, error) {
	counts, err := a.prepareCounts(granules, cardinality)
	if err != nil {
		return nil, err
	}
	clear(counts)
	for _, g := range granules {
		if err := a.forEachCode(g, func(code uint32) error {
			if int(code) >= len(counts) {
				return fmt.Errorf("colgranule: code %d outside counts", code)
			}
			counts[code]++
			return nil
		}); err != nil {
			return nil, err
		}
	}
	a.counts = counts
	return counts, nil
}

// FilteredGroupedCountCodes returns arena-owned count storage. The returned
// slice is valid only until the next AggregateArena operation or Reset.
func (a *AggregateArena) FilteredGroupedCountCodes(codeGranules []EncodedGranule, filterGranules []EncodedGranule, filter Int64RangePredicate, cardinality uint32) ([]uint64, PredicateDiagnostics, error) {
	var diagnostics PredicateDiagnostics
	if len(codeGranules) != len(filterGranules) {
		return nil, diagnostics, fmt.Errorf("colgranule: code granules=%d filter granules=%d", len(codeGranules), len(filterGranules))
	}
	counts, err := a.prepareCounts(codeGranules, cardinality)
	if err != nil {
		return nil, diagnostics, err
	}
	clear(counts)
	if filter.Empty() {
		a.counts = counts
		return counts, diagnostics, nil
	}
	for i, codeGranule := range codeGranules {
		filterGranule := filterGranules[i]
		diagnostics.Considered++
		if filterGranule.Rows != codeGranule.Rows {
			return nil, diagnostics, errors.New("colgranule: filter/code row mismatch")
		}
		if filterGranule.HasMinMax && (filter.High < filterGranule.Min || filter.Low > filterGranule.Max) {
			diagnostics.SkippedByMinMax++
			continue
		}
		filterValues, err := a.reader.DecodeInt64(filterGranule)
		if err != nil {
			return nil, diagnostics, err
		}
		if len(filterValues) != codeGranule.Rows {
			return nil, diagnostics, errors.New("colgranule: filter/code row mismatch")
		}
		codeRaw, err := a.reader.decompressPayload(codeGranule)
		if err != nil {
			return nil, diagnostics, err
		}
		header, err := parseUint32CodesHeader(codeRaw, codeGranule.Rows)
		if err != nil {
			return nil, diagnostics, err
		}
		diagnostics.Decoded++
		for row, value := range filterValues {
			if value < filter.Low || value > filter.High {
				continue
			}
			code := readUint32Code(header.data, header.width, row)
			if code >= header.cardinality || int(code) >= len(counts) {
				return nil, diagnostics, fmt.Errorf("colgranule: code %d outside counts", code)
			}
			counts[code]++
			diagnostics.Matched++
		}
	}
	a.counts = counts
	return counts, diagnostics, nil
}

func (a *AggregateArena) MinMaxInt64(granules []EncodedGranule, filter *Int64RangePredicate) (int64, int64, bool, error) {
	has := false
	min, max := int64(0), int64(0)
	for _, g := range granules {
		if filter == nil && g.HasMinMax {
			min, max, has = updateOptionalMinMax(min, max, has, g.Min)
			min, max, has = updateOptionalMinMax(min, max, has, g.Max)
			continue
		}
		values, err := a.reader.DecodeInt64(g)
		if err != nil {
			return 0, 0, false, err
		}
		for _, v := range values {
			if filter != nil && (v < filter.Low || v > filter.High) {
				continue
			}
			min, max, has = updateOptionalMinMax(min, max, has, v)
		}
	}
	return min, max, has, nil
}

func (a *AggregateArena) ExactDistinctCodes(granules []EncodedGranule, cardinality uint32) (int, error) {
	cardinality, err := inferCodeCardinality(granules, cardinality)
	if err != nil {
		return 0, err
	}
	words := (int(cardinality) + 63) / 64
	if words > maxAggregateCells {
		return 0, fmt.Errorf("colgranule: code distinct words=%d exceeds cap %d", words, maxAggregateCells)
	}
	if cap(a.codeBits) < words {
		a.codeBits = make([]uint64, words)
	} else {
		a.codeBits = a.codeBits[:words]
	}
	clear(a.codeBits)
	for _, g := range granules {
		if err := a.forEachCode(g, func(code uint32) error {
			if code >= cardinality {
				return fmt.Errorf("colgranule: code %d outside cardinality %d", code, cardinality)
			}
			if int(code/64) >= len(a.codeBits) {
				return fmt.Errorf("colgranule: code %d outside cardinality %d", code, cardinality)
			}
			a.codeBits[code/64] |= 1 << uint(code%64)
			return nil
		}); err != nil {
			return 0, err
		}
	}
	distinct := 0
	for _, word := range a.codeBits {
		distinct += popcount64(word)
	}
	return distinct, nil
}

func (a *AggregateArena) ExactDistinctInt64(granules []EncodedGranule) (int, error) {
	if a.seenInt64 == nil {
		a.seenInt64 = make(map[int64]struct{})
	} else {
		clear(a.seenInt64)
	}
	for _, g := range granules {
		values, err := a.reader.DecodeInt64(g)
		if err != nil {
			return 0, err
		}
		for _, v := range values {
			a.seenInt64[v] = struct{}{}
		}
	}
	return len(a.seenInt64), nil
}

type TimeBucketedCounts struct {
	BucketWidth int64
	MinBucket   int64
	Buckets     int
	Cardinality uint32
	Counts      []uint64
}

func (c TimeBucketedCounts) Count(bucket int64, code uint32) uint64 {
	if c.BucketWidth <= 0 || code >= c.Cardinality {
		return 0
	}
	bucketOffset := bucket - c.MinBucket
	if bucketOffset < 0 || bucketOffset >= int64(c.Buckets) {
		return 0
	}
	cell, err := bucketCellIndex(int(bucketOffset), c.Buckets, code, c.Cardinality, len(c.Counts))
	if err != nil {
		return 0
	}
	return c.Counts[cell]
}

// TimeBucketedCountCodes returns a result whose Counts slice is arena-owned.
// Counts is valid only until the next AggregateArena operation or Reset.
func (a *AggregateArena) TimeBucketedCountCodes(codeGranules []EncodedGranule, timeGranules []EncodedGranule, bucketWidth int64, cardinality uint32) (TimeBucketedCounts, error) {
	if len(codeGranules) != len(timeGranules) {
		return TimeBucketedCounts{}, fmt.Errorf("colgranule: code granules=%d time granules=%d", len(codeGranules), len(timeGranules))
	}
	if bucketWidth <= 0 {
		return TimeBucketedCounts{}, fmt.Errorf("colgranule: invalid bucket width %d", bucketWidth)
	}
	cardinality, err := inferCodeCardinality(codeGranules, cardinality)
	if err != nil {
		return TimeBucketedCounts{}, err
	}
	minTime, maxTime, ok, err := a.MinMaxInt64(timeGranules, nil)
	if err != nil {
		return TimeBucketedCounts{}, err
	}
	if !ok {
		return TimeBucketedCounts{BucketWidth: bucketWidth, Cardinality: cardinality}, nil
	}
	minBucket := floorDiv(minTime, bucketWidth)
	maxBucket := floorDiv(maxTime, bucketWidth)
	buckets, cells, err := boundedBucketCells(minBucket, maxBucket, cardinality)
	if err != nil {
		return TimeBucketedCounts{}, err
	}
	if cap(a.bucketCounts) < cells {
		a.bucketCounts = make([]uint64, cells)
	} else {
		a.bucketCounts = a.bucketCounts[:cells]
	}
	clear(a.bucketCounts)
	for i, codeGranule := range codeGranules {
		times, err := a.reader.DecodeInt64(timeGranules[i])
		if err != nil {
			return TimeBucketedCounts{}, err
		}
		codeRaw, err := a.reader.decompressPayload(codeGranule)
		if err != nil {
			return TimeBucketedCounts{}, err
		}
		header, err := parseUint32CodesHeader(codeRaw, codeGranule.Rows)
		if err != nil {
			return TimeBucketedCounts{}, err
		}
		if len(times) != codeGranule.Rows {
			return TimeBucketedCounts{}, errors.New("colgranule: time/code row mismatch")
		}
		for row, timestamp := range times {
			code := readUint32Code(header.data, header.width, row)
			if code >= header.cardinality || code >= cardinality {
				return TimeBucketedCounts{}, fmt.Errorf("colgranule: code %d outside cardinality %d", code, cardinality)
			}
			bucketIndex64 := floorDiv(timestamp, bucketWidth) - minBucket
			if bucketIndex64 < 0 || bucketIndex64 >= int64(buckets) {
				return TimeBucketedCounts{}, fmt.Errorf("colgranule: timestamp %d outside bucket range [%d,%d]", timestamp, minBucket, maxBucket)
			}
			bucketIndex := int(bucketIndex64)
			cell, err := bucketCellIndex(bucketIndex, buckets, code, cardinality, len(a.bucketCounts))
			if err != nil {
				return TimeBucketedCounts{}, err
			}
			a.bucketCounts[cell]++
		}
	}
	return TimeBucketedCounts{
		BucketWidth: bucketWidth,
		MinBucket:   minBucket,
		Buckets:     buckets,
		Cardinality: cardinality,
		Counts:      a.bucketCounts,
	}, nil
}

func (a *AggregateArena) prepareCounts(granules []EncodedGranule, cardinality uint32) ([]uint64, error) {
	cardinality, err := inferCodeCardinality(granules, cardinality)
	if err != nil {
		return nil, err
	}
	if cardinality > maxAggregateCells {
		return nil, fmt.Errorf("colgranule: cardinality %d exceeds cap %d", cardinality, maxAggregateCells)
	}
	if cap(a.counts) < int(cardinality) {
		a.counts = make([]uint64, cardinality)
	} else {
		a.counts = a.counts[:cardinality]
	}
	return a.counts, nil
}

func (a *AggregateArena) forEachCode(g EncodedGranule, fn func(uint32) error) error {
	raw, err := a.reader.decompressPayload(g)
	if err != nil {
		return err
	}
	header, err := parseUint32CodesHeader(raw, g.Rows)
	if err != nil {
		return err
	}
	for row := 0; row < g.Rows; row++ {
		code := readUint32Code(header.data, header.width, row)
		if code >= header.cardinality {
			return fmt.Errorf("colgranule: code %d outside cardinality %d", code, header.cardinality)
		}
		if err := fn(code); err != nil {
			return err
		}
	}
	return nil
}

func inferCodeCardinality(granules []EncodedGranule, cardinality uint32) (uint32, error) {
	if cardinality > maxCodeCardinality {
		return 0, fmt.Errorf("colgranule: cardinality %d exceeds cap %d", cardinality, maxCodeCardinality)
	}
	if cardinality != 0 {
		return cardinality, nil
	}
	maxCardinality := cardinality
	var reader GranuleReader
	for _, g := range granules {
		raw, err := reader.decompressPayload(g)
		if err != nil {
			return 0, err
		}
		header, err := parseUint32CodesHeader(raw, g.Rows)
		if err != nil {
			return 0, err
		}
		if maxCardinality == 0 || header.cardinality > maxCardinality {
			maxCardinality = header.cardinality
		}
	}
	if maxCardinality == 0 {
		return 0, errors.New("colgranule: empty code cardinality")
	}
	return maxCardinality, nil
}

func boundedBucketCells(minBucket int64, maxBucket int64, cardinality uint32) (int, int, error) {
	if maxBucket < minBucket {
		return 0, 0, fmt.Errorf("colgranule: invalid bucket range [%d,%d]", minBucket, maxBucket)
	}
	if cardinality == 0 {
		return 0, 0, errors.New("colgranule: empty code cardinality")
	}
	maxBuckets := maxAggregateCells / int(cardinality)
	if maxBuckets == 0 {
		return 0, 0, fmt.Errorf("colgranule: aggregate cardinality=%d exceeds cell cap %d", cardinality, maxAggregateCells)
	}
	maxSpan := int64(maxBuckets - 1)
	const minInt64 = -1 << 63
	if maxBucket >= minInt64+maxSpan && maxBucket-maxSpan > minBucket {
		return 0, 0, fmt.Errorf("colgranule: aggregate buckets exceed cap %d", maxBuckets)
	}
	buckets := int(maxBucket - minBucket + 1)
	cells := buckets * int(cardinality)
	return buckets, cells, nil
}

func bucketCellIndex(bucketIndex int, buckets int, code uint32, cardinality uint32, cells int) (int, error) {
	if bucketIndex < 0 || bucketIndex >= buckets {
		return 0, fmt.Errorf("colgranule: bucket index %d outside bucket count %d", bucketIndex, buckets)
	}
	if code >= cardinality {
		return 0, fmt.Errorf("colgranule: code %d outside cardinality %d", code, cardinality)
	}
	cell := bucketIndex*int(cardinality) + int(code)
	if cell < 0 || cell >= cells {
		return 0, fmt.Errorf("colgranule: bucket cell %d outside counts length %d", cell, cells)
	}
	return cell, nil
}

func floorDiv(v int64, width int64) int64 {
	q := v / width
	r := v % width
	if r != 0 && ((r < 0) != (width < 0)) {
		q--
	}
	return q
}

func popcount64(v uint64) int {
	v = v - ((v >> 1) & 0x5555555555555555)
	v = (v & 0x3333333333333333) + ((v >> 2) & 0x3333333333333333)
	return int((((v + (v >> 4)) & 0x0f0f0f0f0f0f0f0f) * 0x0101010101010101) >> 56)
}
