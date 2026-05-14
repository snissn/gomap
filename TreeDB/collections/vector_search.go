package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
)

// VectorMetric selects the exact distance function used by collection vector
// search. Smaller returned distances are better for every metric.
type VectorMetric uint8

const (
	VectorMetricCosine VectorMetric = iota
	VectorMetricL2
	VectorMetricInnerProduct
)

// VectorSearchOptions configures exact vector search over collection primary
// documents.
type VectorSearchOptions struct {
	// Field is the JSON field path containing a numeric vector. Nested fields can
	// be addressed with dot notation, for example "embedding.body".
	Field string
	// Metric selects the distance function. The zero value is cosine distance.
	Metric VectorMetric
	// TopK is the maximum number of nearest documents to return.
	TopK int
	// Filter is an optional metadata filter applied before vector extraction and
	// distance calculation. The callback receives owned stored-document bytes.
	Filter func(DocumentRecord) (bool, error)
	// IndexRangeFilter optionally restricts exact vector search to document IDs
	// returned by an existing scalar secondary-index range. Range.Limit is ignored
	// for correctness; the full logical range is scanned.
	IndexRangeFilter *VectorIndexRangeFilter
}

// VectorSearchResult is one exact vector-search match.
type VectorSearchResult struct {
	DocumentID []byte
	Distance   float32
	Document   []byte
}

// VectorIndexRangeFilter restricts vector search to document IDs produced by a
// scalar collection secondary-index range.
type VectorIndexRangeFilter struct {
	IndexName string
	Range     IndexRangeOptions
}

// SearchVectorsExact scans live collection documents, extracts the configured
// vector field from each document, computes exact distances, and returns the
// nearest TopK matches. The collection primary row remains the canonical vector
// storage; missing or null vector fields are skipped.
func (c *Collection) SearchVectorsExact(query []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	fieldPath, err := parseVectorFieldPath(opts.Field)
	if err != nil {
		return nil, err
	}
	if opts.TopK <= 0 {
		return nil, errors.New("collections: vector search TopK must be positive")
	}
	if len(query) == 0 {
		return nil, errors.New("collections: vector query cannot be empty")
	}
	if err := validateFloat32Vector(query); err != nil {
		return nil, fmt.Errorf("collections: vector query: %w", err)
	}
	metric, err := normalizeVectorMetric(opts.Metric)
	if err != nil {
		return nil, err
	}
	if metric == VectorMetricCosine && vectorNormSquared(query) == 0 {
		return nil, errors.New("collections: cosine vector query cannot have zero magnitude")
	}
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()

	matches := make([]VectorSearchResult, 0, opts.TopK)
	addMatch := func(record DocumentRecord, distance float32) {
		result := VectorSearchResult{
			DocumentID: bytes.Clone(record.ID),
			Distance:   distance,
			Document:   bytes.Clone(record.Document),
		}
		matches = append(matches, result)
		sortVectorSearchResults(matches)
		if len(matches) > opts.TopK {
			matches = matches[:opts.TopK]
		}
	}
	processRecord := func(record DocumentRecord) error {
		if opts.Filter != nil {
			include, err := opts.Filter(record)
			if err != nil || !include {
				return err
			}
		}
		jsonDoc, err := materializer.StoredDocumentJSON(record.Document)
		if err != nil {
			return err
		}
		vector, ok, err := vectorFromJSONField(jsonDoc, fieldPath)
		if err != nil {
			return fmt.Errorf("collections: vector field %q in document %q: %w", opts.Field, record.ID, err)
		}
		if !ok {
			return nil
		}
		if len(vector) != len(query) {
			return fmt.Errorf("collections: vector field %q in document %q has dimension %d, want %d", opts.Field, record.ID, len(vector), len(query))
		}
		distance, err := exactVectorDistance(query, vector, metric)
		if err != nil {
			return fmt.Errorf("collections: vector field %q in document %q: %w", opts.Field, record.ID, err)
		}
		addMatch(record, distance)
		return nil
	}

	if opts.IndexRangeFilter != nil {
		ids, _, err := c.vectorSearchIndexRangeDocumentIDs(opts.IndexRangeFilter, 0)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			document, err := c.Get(id)
			if err != nil {
				return nil, err
			}
			if document == nil {
				continue
			}
			if err := processRecord(DocumentRecord{ID: bytes.Clone(id), Document: document}); err != nil {
				return nil, err
			}
		}
		return matches, nil
	}

	_, err = c.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		if err := processRecord(record); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func (c *Collection) vectorSearchIndexRangeDocumentIDs(filter *VectorIndexRangeFilter, limit int) ([][]byte, bool, error) {
	if filter == nil {
		return nil, false, nil
	}
	if limit < 0 {
		return nil, false, errors.New("collections: vector index range filter limit cannot be negative")
	}
	opts := filter.Range
	opts.Limit = limit
	ids, truncated, err := c.FindByIndexRange(filter.IndexName, opts)
	if err != nil {
		return nil, false, err
	}
	if ids == nil {
		return [][]byte{}, truncated, nil
	}
	return ids, truncated, nil
}

func normalizeVectorMetric(metric VectorMetric) (VectorMetric, error) {
	switch metric {
	case VectorMetricCosine, VectorMetricL2, VectorMetricInnerProduct:
		return metric, nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

func parseVectorFieldPath(field string) ([]string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil, errors.New("collections: vector search field cannot be empty")
	}
	parts := strings.Split(field, ".")
	out := parts[:0]
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("collections: invalid vector search field path %q", field)
		}
		out = append(out, part)
	}
	return out, nil
}

func vectorFromJSONField(document []byte, fieldPath []string) ([]float32, bool, error) {
	_, dataType, _, err := jsonparser.Get(document, fieldPath...)
	if err == jsonparser.KeyPathNotFoundError || dataType == jsonparser.Null {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if dataType != jsonparser.Array {
		return nil, false, errors.New("not a numeric array")
	}
	out := make([]float32, 0, 64)
	var parseErr error
	_, err = jsonparser.ArrayEach(document, func(value []byte, dataType jsonparser.ValueType, _ int, err error) {
		if parseErr != nil {
			return
		}
		if err != nil {
			parseErr = err
			return
		}
		n, err := parseVectorJSONFloat32(value, dataType, len(out))
		if err != nil {
			parseErr = err
			return
		}
		out = append(out, n)
	}, fieldPath...)
	if err != nil {
		return nil, false, err
	}
	if parseErr != nil {
		return nil, false, parseErr
	}
	if len(out) == 0 {
		return nil, false, errors.New("empty vector")
	}
	return out, true, nil
}

func parseVectorJSONFloat32(value []byte, dataType jsonparser.ValueType, index int) (float32, error) {
	raw := string(value)
	switch dataType {
	case jsonparser.Number:
	case jsonparser.Object:
		extended, ok, err := vectorExtendedJSONNumberString(value)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("element %d is not numeric", index)
		}
		raw = extended
	default:
		return 0, fmt.Errorf("element %d is not numeric", index)
	}
	n, err := strconv.ParseFloat(raw, 32)
	if err != nil {
		return 0, fmt.Errorf("element %d is not numeric", index)
	}
	if math.IsNaN(n) || math.IsInf(n, 0) || n > math.MaxFloat32 || n < -math.MaxFloat32 {
		return 0, fmt.Errorf("element %d is not a finite float32", index)
	}
	return float32(n), nil
}

func vectorExtendedJSONNumberString(raw []byte) (string, bool, error) {
	count := 0
	var field string
	var value []byte
	var valueType jsonparser.ValueType
	err := jsonparser.ObjectEach(raw, func(key, rawValue []byte, dataType jsonparser.ValueType, _ int) error {
		count++
		if count == 1 {
			field = string(key)
			value = rawValue
			valueType = dataType
		}
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if count != 1 || !isExtendedJSONNumberField(field) {
		return "", false, nil
	}
	if valueType != jsonparser.String {
		return "", true, fmt.Errorf("extended JSON numeric wrapper %s must contain a string", field)
	}
	if bytes.IndexByte(value, '\\') >= 0 {
		unescaped, err := jsonparser.Unescape(value, nil)
		if err != nil {
			return "", true, err
		}
		return string(unescaped), true, nil
	}
	return string(value), true, nil
}

func exactVectorDistance(left, right []float32, metric VectorMetric) (float32, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("dimension %d, want %d", len(right), len(left))
	}
	switch metric {
	case VectorMetricCosine:
		var dot, leftNorm, rightNorm float64
		for i := range left {
			l := float64(left[i])
			r := float64(right[i])
			dot += l * r
			leftNorm += l * l
			rightNorm += r * r
		}
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("cosine vectors cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i := range left {
			d := float64(left[i] - right[i])
			sum += d * d
		}
		return float32(sum), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i := range left {
			dot += float64(left[i]) * float64(right[i])
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

func vectorNormSquared(vector []float32) float64 {
	var sum float64
	for _, v := range vector {
		f := float64(v)
		sum += f * f
	}
	return sum
}

func validateFloat32Vector(vector []float32) error {
	for i, v := range vector {
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("element %d is not finite", i)
		}
	}
	return nil
}

func sortVectorSearchResults(results []VectorSearchResult) {
	slices.SortFunc(results, func(left, right VectorSearchResult) int {
		if left.Distance < right.Distance {
			return -1
		}
		if left.Distance > right.Distance {
			return 1
		}
		return bytes.Compare(left.DocumentID, right.DocumentID)
	})
}
