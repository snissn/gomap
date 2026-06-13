package collections

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ColumnRetainedPayloadEncodingStatus reports the retained payload body encoding
// currently implied by column-store metadata.
func ColumnRetainedPayloadEncodingStatus(cfg *ColumnStoreConfig) (encoding, status string) {
	if cfg == nil || !cfg.Enabled {
		return string(ColumnRetainedPayloadEncodingNone), "not_configured"
	}
	policy := columnRetainedPayloadAuditPolicy(cfg)
	switch policy {
	case ColumnRetainedPayloadNone:
		return string(ColumnRetainedPayloadEncodingNone), "inactive_no_retained_payload"
	case ColumnRetainedPayloadFull, "":
		return string(ColumnRetainedPayloadEncodingJSON), "active_json_full_retained_payload"
	case ColumnRetainedPayloadNonColumn:
		switch encoding := columnRetainedPayloadEffectiveEncoding(cfg); encoding {
		case ColumnRetainedPayloadEncodingTemplateV1:
			return string(ColumnRetainedPayloadEncodingTemplateV1), "active_template_v1_non_column_retained_payload"
		case ColumnRetainedPayloadEncodingJSON:
			return string(ColumnRetainedPayloadEncodingJSON), "active_legacy_json_non_column_retained_payload"
		case ColumnRetainedPayloadEncodingSemanticStreamV1:
			return string(ColumnRetainedPayloadEncodingSemanticStreamV1), "active_semantic_stream_v1_non_column_retained_payload"
		default:
			return string(ColumnRetainedPayloadEncodingUnavailable), fmt.Sprintf("unavailable_unsupported_retained_payload_encoding_%s", encoding)
		}
	default:
		return string(ColumnRetainedPayloadEncodingUnavailable), fmt.Sprintf("unknown_retained_payload_policy_%s", cfg.RetainedPayload)
	}
}

// ColumnRetainedPayloadCompressionStatus reports the durable storage compression
// policy expected for retained payload bodies. Retained payload bytes are stored
// through TreeDB's persistent value-log/leaf-log path, whose default compression
// mode resolves to auto grouped-frame compression.
func ColumnRetainedPayloadCompressionStatus(cfg *ColumnStoreConfig) (compression, policy, status string) {
	if cfg == nil || !cfg.Enabled {
		return "none", "not_configured", "not_configured"
	}
	switch policyValue := columnRetainedPayloadAuditPolicy(cfg); policyValue {
	case ColumnRetainedPayloadNone:
		return "none", "none", "inactive_no_retained_payload"
	case ColumnRetainedPayloadFull, "":
		return "value_log_grouped_frame", "default_value_log_auto", "active_value_log_auto_grouped_frame_full_retained_payload"
	case ColumnRetainedPayloadNonColumn:
		if columnRetainedPayloadEffectiveEncoding(cfg) == ColumnRetainedPayloadEncodingSemanticStreamV1 {
			return "semantic_stream_v1_blocks", "retained_semantic_stream_v1_side_root", "active_semantic_stream_v1_non_column_retained_payload"
		}
		return "value_log_grouped_frame", "default_value_log_auto_storage_first", "active_value_log_auto_grouped_frame_non_column_retained_payload"
	default:
		return "unavailable", "unavailable", fmt.Sprintf("unknown_retained_payload_policy_%s", policyValue)
	}
}

type ColumnRetainedPayloadPathAudit struct {
	RetainedPayloadPolicy         ColumnRetainedPayloadPolicy `json:"retained_payload_policy"`
	RetainedPayloadEncoding       string                      `json:"retained_payload_encoding"`
	RetainedPayloadEncodingStatus string                      `json:"retained_payload_encoding_status"`
	RetainedPayloadBytes          int                         `json:"retained_payload_bytes"`
	Paths                         []ColumnRetainedPayloadPath `json:"paths"`
	Violations                    []string                    `json:"violations,omitempty"`
}

type ColumnRetainedPayloadPath struct {
	Path    string `json:"path"`
	Present bool   `json:"present"`
}

// ColumnRetainedPayloadCollectionAuditOptions controls the retained-payload
// path audit over the collection's persisted primary rows.
type ColumnRetainedPayloadCollectionAuditOptions struct {
	Paths                   []string
	MaxDocuments            int
	IncludeShapeStats       bool
	ShapeMaxDepth           int
	ShapeMaxPaths           int
	IncludeValueFamilyStats bool
	ValueFamilyMaxDepth     int
	ValueFamilyMaxPaths     int
	ValueFamilyMaxUnique    int
	IncludeSemanticStreams  bool
	SemanticStreamMaxDepth  int
	SemanticStreamMaxPaths  int
	// IncludeSemanticStreamBlockLayout parses semantic-stream-v1 side-root
	// blocks directly. It is only valid for semantic-stream-v1 collections.
	IncludeSemanticStreamBlockLayout bool
	SemanticStreamBlockMaxPaths      int
}

type ColumnRetainedPayloadCollectionPathViolation struct {
	DocumentID string `json:"document_id"`
	Path       string `json:"path"`
}

// ColumnRetainedPayloadShapePathStat summarizes decoded retained-payload JSON
// values by path and kind. JSONBytes is per-path encoded value size and is not
// additive across parent and child paths.
type ColumnRetainedPayloadShapePathStat struct {
	Path         string `json:"path"`
	ValueKind    string `json:"value_kind"`
	Occurrences  int64  `json:"occurrences"`
	Documents    int64  `json:"documents"`
	JSONBytes    int64  `json:"json_bytes"`
	StringBytes  int64  `json:"string_bytes,omitempty"`
	MaxJSONBytes int    `json:"max_json_bytes,omitempty"`
}

type ColumnRetainedPayloadLengthBucket struct {
	Bucket string `json:"bucket"`
	Count  int64  `json:"count"`
}

// ColumnRetainedPayloadValueFamilyStat summarizes decoded retained-payload
// string leaves by path. Compression oracles encode one JSON string per line,
// so OracleInputBytes includes one separator byte per occurrence.
type ColumnRetainedPayloadValueFamilyStat struct {
	Path                  string                              `json:"path"`
	Occurrences           int64                               `json:"occurrences"`
	Documents             int64                               `json:"documents"`
	JSONBytes             int64                               `json:"json_bytes"`
	StringBytes           int64                               `json:"string_bytes"`
	OracleInputBytes      int64                               `json:"oracle_input_bytes"`
	MinLength             int                                 `json:"min_length"`
	MaxLength             int                                 `json:"max_length"`
	MeanLength            float64                             `json:"mean_length"`
	TrackedUniqueValues   int64                               `json:"tracked_unique_values"`
	UniqueValuesTruncated bool                                `json:"unique_values_truncated,omitempty"`
	RepeatedValues        int64                               `json:"repeated_values,omitempty"`
	CommonPrefix          string                              `json:"common_prefix,omitempty"`
	CommonPrefixBytes     int                                 `json:"common_prefix_bytes,omitempty"`
	CommonPrefixTruncated bool                                `json:"common_prefix_truncated,omitempty"`
	CommonSuffix          string                              `json:"common_suffix,omitempty"`
	CommonSuffixBytes     int                                 `json:"common_suffix_bytes,omitempty"`
	CommonSuffixTruncated bool                                `json:"common_suffix_truncated,omitempty"`
	LengthBuckets         []ColumnRetainedPayloadLengthBucket `json:"length_buckets,omitempty"`
	GzipBytes             int64                               `json:"gzip_bytes,omitempty"`
	GzipToInputRatio      float64                             `json:"gzip_to_input_ratio,omitempty"`
	ZSTDBytes             int64                               `json:"zstd_bytes,omitempty"`
	ZSTDToInputRatio      float64                             `json:"zstd_to_input_ratio,omitempty"`
}

// ColumnRetainedPayloadSemanticStreamStat estimates the scalar stream bytes a
// ClickHouse-like semantic retained payload layout would need by path and kind.
// It is an oracle over decoded values, not a claim about the current on-disk
// format: object shape/presence metadata and reconstruction layout are separate
// format choices.
type ColumnRetainedPayloadSemanticStreamStat struct {
	Path             string  `json:"path"`
	ValueKind        string  `json:"value_kind"`
	Occurrences      int64   `json:"occurrences"`
	Documents        int64   `json:"documents"`
	JSONBytes        int64   `json:"json_bytes"`
	StringBytes      int64   `json:"string_bytes,omitempty"`
	StreamInputBytes int64   `json:"stream_input_bytes"`
	MinStreamBytes   int     `json:"min_stream_bytes"`
	MaxStreamBytes   int     `json:"max_stream_bytes"`
	ZSTDBytes        int64   `json:"zstd_bytes,omitempty"`
	ZSTDToInputRatio float64 `json:"zstd_to_input_ratio,omitempty"`
}

type ColumnRetainedPayloadCollectionAuditResult struct {
	Collection                               string                                          `json:"collection"`
	Status                                   string                                          `json:"status"`
	RetainedPayloadPolicy                    ColumnRetainedPayloadPolicy                     `json:"retained_payload_policy"`
	RetainedPayloadEncoding                  string                                          `json:"retained_payload_encoding"`
	RetainedPayloadEncodingStatus            string                                          `json:"retained_payload_encoding_status"`
	RetainedPayloadCompression               string                                          `json:"retained_payload_compression"`
	RetainedPayloadCompressionPolicy         string                                          `json:"retained_payload_compression_policy"`
	RetainedPayloadCompressionStatus         string                                          `json:"retained_payload_compression_status"`
	DeclaredPaths                            []string                                        `json:"declared_paths"`
	CheckedRows                              int                                             `json:"checked_rows"`
	RetainedPayloadBytes                     int64                                           `json:"retained_payload_bytes"`
	RetainedPayloadShape                     []ColumnRetainedPayloadShapePathStat            `json:"retained_payload_shape,omitempty"`
	RetainedPayloadShapeTruncated            bool                                            `json:"retained_payload_shape_truncated,omitempty"`
	RetainedPayloadValueFamilies             []ColumnRetainedPayloadValueFamilyStat          `json:"retained_payload_value_families,omitempty"`
	RetainedPayloadValueFamiliesTruncated    bool                                            `json:"retained_payload_value_families_truncated,omitempty"`
	RetainedPayloadSemanticStreams           []ColumnRetainedPayloadSemanticStreamStat       `json:"retained_payload_semantic_streams,omitempty"`
	RetainedPayloadSemanticStreamsTruncated  bool                                            `json:"retained_payload_semantic_streams_truncated,omitempty"`
	RetainedPayloadSemanticStreamInputBytes  int64                                           `json:"retained_payload_semantic_stream_input_bytes,omitempty"`
	RetainedPayloadSemanticStreamZSTDBytes   int64                                           `json:"retained_payload_semantic_stream_zstd_bytes,omitempty"`
	RetainedPayloadSemanticStreamBlockLayout *ColumnRetainedSemanticStreamV1BlockLayoutAudit `json:"retained_payload_semantic_stream_block_layout,omitempty"`
	Truncated                                bool                                            `json:"truncated,omitempty"`
	Violations                               []ColumnRetainedPayloadCollectionPathViolation  `json:"violations,omitempty"`
	Errors                                   []string                                        `json:"errors,omitempty"`
}

// AuditColumnRetainedPayloadPathsAbsent verifies that declared typed JSON paths
// are absent from a retained payload body. It fails closed on malformed retained
// payloads and on any declared path that remains present.
func AuditColumnRetainedPayloadPathsAbsent(cfg ColumnStoreConfig, retained []byte, paths []string) (ColumnRetainedPayloadPathAudit, error) {
	return auditColumnRetainedPayloadPathsAbsentWithResolver(cfg, retained, paths, nil)
}

func auditColumnRetainedPayloadPathsAbsentWithResolver(cfg ColumnStoreConfig, retained []byte, paths []string, resolver templateV1Resolver) (ColumnRetainedPayloadPathAudit, error) {
	policy := columnRetainedPayloadAuditPolicy(&cfg)
	encoding, status := ColumnRetainedPayloadEncodingStatus(&cfg)
	audit := ColumnRetainedPayloadPathAudit{
		RetainedPayloadPolicy:         policy,
		RetainedPayloadEncoding:       encoding,
		RetainedPayloadEncodingStatus: status,
		RetainedPayloadBytes:          len(retained),
	}
	obj, err := decodeColumnRetainedPayloadObject(cfg, retained, resolver)
	if err != nil {
		return audit, fmt.Errorf("collections: retained payload path audit: %w", err)
	}
	return auditColumnRetainedPayloadObjectPathsAbsent(audit, obj, paths)
}

func auditColumnRetainedPayloadObjectPathsAbsent(audit ColumnRetainedPayloadPathAudit, obj map[string]any, paths []string) (ColumnRetainedPayloadPathAudit, error) {
	checked := normalizeColumnRetainedPayloadAuditPaths(paths)
	for _, path := range checked {
		present := columnJSONPathExists(obj, path)
		audit.Paths = append(audit.Paths, ColumnRetainedPayloadPath{Path: path, Present: present})
		if present {
			audit.Violations = append(audit.Violations, path)
		}
	}
	if len(audit.Violations) > 0 {
		return audit, fmt.Errorf("collections: retained payload contains declared typed paths: %s", strings.Join(audit.Violations, ", "))
	}
	return audit, nil
}

// AuditRetainedPayloadDeclaredPathsAbsent scans persisted retained primary-row
// bodies and verifies that declared typed-column paths were removed. It is
// read-only: it does not flush buffered writes, publish roots, compact, or
// mutate storage.
func (c *Collection) AuditRetainedPayloadDeclaredPathsAbsent(opts ColumnRetainedPayloadCollectionAuditOptions) (result ColumnRetainedPayloadCollectionAuditResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result, err = retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained payload audit panic: %v", recovered))
		}
	}()
	if c == nil {
		return retainedPayloadCollectionAuditError(result, errCollectionNil)
	}
	result.Collection = c.collectionName()
	if c.db == nil {
		return retainedPayloadCollectionAuditError(result, errCollectionDBNil)
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return retainedPayloadCollectionAuditError(result, backenddb.ErrClosed)
	}
	defer func() { _ = snap.Close() }()

	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return retainedPayloadCollectionAuditError(result, err)
	}
	cfgPtr := catalog.meta.Options.ColumnStore
	if cfgPtr == nil || !cfgPtr.Enabled {
		result.Status = "not_configured"
		result.RetainedPayloadPolicy = columnRetainedPayloadAuditPolicy(cfgPtr)
		result.RetainedPayloadEncoding, result.RetainedPayloadEncodingStatus = ColumnRetainedPayloadEncodingStatus(cfgPtr)
		result.RetainedPayloadCompression, result.RetainedPayloadCompressionPolicy, result.RetainedPayloadCompressionStatus = ColumnRetainedPayloadCompressionStatus(cfgPtr)
		return result, nil
	}
	cfg := cfgPtr.copy()
	result.RetainedPayloadPolicy = columnRetainedPayloadAuditPolicy(&cfg)
	result.RetainedPayloadEncoding, result.RetainedPayloadEncodingStatus = ColumnRetainedPayloadEncodingStatus(&cfg)
	result.RetainedPayloadCompression, result.RetainedPayloadCompressionPolicy, result.RetainedPayloadCompressionStatus = ColumnRetainedPayloadCompressionStatus(&cfg)
	result.DeclaredPaths = normalizeColumnRetainedPayloadAuditPaths(opts.Paths)
	if len(result.DeclaredPaths) == 0 {
		result.DeclaredPaths = columnRetainedPayloadAuditDeclaredPaths(cfg)
	}
	if result.RetainedPayloadPolicy == ColumnRetainedPayloadNone {
		result.Status = "inactive_no_retained_payload"
		return result, nil
	}
	if len(result.DeclaredPaths) == 0 && !opts.IncludeSemanticStreamBlockLayout {
		result.Status = "no_declared_paths"
		return result, nil
	}
	if opts.IncludeSemanticStreamBlockLayout && !columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) {
		return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: semantic-stream-v1 block layout audit requires retained encoding %q", ColumnRetainedPayloadEncodingSemanticStreamV1))
	}

	it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil {
		return retainedPayloadCollectionAuditError(result, err)
	}
	if it == nil {
		result.Status = "passed"
		return result, nil
	}
	defer func() { _ = it.Close() }()

	resolver := columnRetainedPayloadTemplateResolver(snap, catalog)
	includeDecodedStats := opts.IncludeShapeStats || opts.IncludeValueFamilyStats || opts.IncludeSemanticStreams
	semanticBlockLayoutOnly := opts.IncludeSemanticStreamBlockLayout &&
		columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) &&
		!includeDecodedStats
	var shape *columnRetainedPayloadShapeCollector
	if opts.IncludeShapeStats {
		shape = newColumnRetainedPayloadShapeCollector(opts.ShapeMaxDepth)
	}
	var valueFamilies *columnRetainedPayloadValueFamilyCollector
	if opts.IncludeValueFamilyStats {
		valueFamilies = newColumnRetainedPayloadValueFamilyCollector(opts.ValueFamilyMaxDepth, opts.ValueFamilyMaxUnique)
	}
	var semanticStreams *columnRetainedPayloadSemanticStreamCollector
	if opts.IncludeSemanticStreams {
		semanticStreams = newColumnRetainedPayloadSemanticStreamCollector(opts.SemanticStreamMaxDepth)
	}
	var semanticBlockLayoutLocatorRows map[string]uint64
	var semanticBlockLayoutPrimaryLocatorBytes int64
	if opts.IncludeSemanticStreamBlockLayout {
		semanticBlockLayoutLocatorRows = make(map[string]uint64)
	}
	for it.Valid() {
		if opts.MaxDocuments > 0 && result.CheckedRows >= opts.MaxDocuments {
			result.Truncated = true
			break
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		documentID := string(it.UnsafeKey())
		retained := it.ValueCopy(nil)
		if err := it.Error(); err != nil {
			return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained payload audit read %q: %w", documentID, err))
		}
		if !it.Valid() {
			return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained payload audit iterator invalid after reading %q", documentID))
		}
		result.CheckedRows++
		result.RetainedPayloadBytes += int64(len(retained))
		semanticBlockLayoutLocator := false
		if opts.IncludeSemanticStreamBlockLayout {
			if ok, err := validateColumnRetainedSemanticStreamV1LocatorAtSnapshot(snap, catalog, retained, semanticBlockLayoutLocatorRows); err != nil {
				return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained payload audit %q: %w", documentID, err))
			} else if ok {
				semanticBlockLayoutLocator = true
				semanticBlockLayoutPrimaryLocatorBytes += int64(len(retained))
			}
		}
		if semanticBlockLayoutOnly {
			if !semanticBlockLayoutLocator {
				payloadAudit, auditErr := auditColumnRetainedPayloadPathsAbsentWithResolver(cfg, retained, result.DeclaredPaths, resolver)
				if auditErr != nil {
					for _, path := range payloadAudit.Violations {
						result.Violations = append(result.Violations, ColumnRetainedPayloadCollectionPathViolation{
							DocumentID: documentID,
							Path:       path,
						})
					}
					return retainedPayloadCollectionAuditError(result, auditErr)
				}
			}
			it.Next()
			continue
		}
		decodedRetained, err := resolveColumnRetainedPayloadAtSnapshot(snap, catalog, cfg, retained)
		if err != nil {
			return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained payload audit %q: %w", documentID, err))
		}
		var payloadAudit ColumnRetainedPayloadPathAudit
		var auditErr error
		if includeDecodedStats {
			var obj map[string]any
			obj, auditErr = decodeColumnRetainedPayloadObject(cfg, decodedRetained, resolver)
			if auditErr == nil {
				payloadAudit = ColumnRetainedPayloadPathAudit{
					RetainedPayloadPolicy:         result.RetainedPayloadPolicy,
					RetainedPayloadEncoding:       result.RetainedPayloadEncoding,
					RetainedPayloadEncodingStatus: result.RetainedPayloadEncodingStatus,
					RetainedPayloadBytes:          len(retained),
				}
				payloadAudit, auditErr = auditColumnRetainedPayloadObjectPathsAbsent(payloadAudit, obj, result.DeclaredPaths)
			}
			if auditErr == nil && shape != nil {
				auditErr = shape.addDocumentObject(obj)
			}
			if auditErr == nil && valueFamilies != nil {
				auditErr = valueFamilies.addDocumentObject(obj)
			}
			if auditErr == nil && semanticStreams != nil {
				auditErr = semanticStreams.addDocumentObject(obj)
			}
			if auditErr != nil {
				auditErr = fmt.Errorf("collections: retained payload audit %q: %w", documentID, auditErr)
			}
		} else {
			payloadAudit, auditErr = auditColumnRetainedPayloadPathsAbsentWithResolver(cfg, decodedRetained, result.DeclaredPaths, resolver)
		}
		if auditErr != nil {
			for _, path := range payloadAudit.Violations {
				result.Violations = append(result.Violations, ColumnRetainedPayloadCollectionPathViolation{
					DocumentID: documentID,
					Path:       path,
				})
			}
			return retainedPayloadCollectionAuditError(result, auditErr)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return retainedPayloadCollectionAuditError(result, err)
	}
	if opts.IncludeSemanticStreamBlockLayout && result.Truncated {
		blockPaths, err := c.auditRetainedSemanticStreamV1BlockLayoutPathsAtSnapshot(snap, catalog, semanticBlockLayoutLocatorRows)
		if err != nil {
			return retainedPayloadCollectionAuditError(result, err)
		}
		violatedPaths := appendColumnRetainedPayloadBlockLayoutViolations(&result, "semantic-stream-v1-sampled-blocks", blockPaths)
		if len(violatedPaths) > 0 {
			return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained semantic-stream-v1 sampled blocks contain declared typed paths: %s", strings.Join(violatedPaths, ", ")))
		}
	}
	if opts.IncludeSemanticStreamBlockLayout && !result.Truncated {
		blockLayout, blockPaths, err := c.auditRetainedSemanticStreamV1BlockLayoutAtSnapshot(snap, catalog, opts.SemanticStreamBlockMaxPaths)
		if err != nil {
			return retainedPayloadCollectionAuditError(result, err)
		}
		blockLayout.Rows = result.CheckedRows
		blockLayout.PrimaryLocatorBytes = semanticBlockLayoutPrimaryLocatorBytes
		result.RetainedPayloadSemanticStreamBlockLayout = &blockLayout
		violatedPaths := appendColumnRetainedPayloadBlockLayoutViolations(&result, "semantic-stream-v1-blocks", blockPaths)
		if len(violatedPaths) > 0 {
			return retainedPayloadCollectionAuditError(result, fmt.Errorf("collections: retained semantic-stream-v1 blocks contain declared typed paths: %s", strings.Join(violatedPaths, ", ")))
		}
	}
	if opts.IncludeShapeStats {
		result.RetainedPayloadShape, result.RetainedPayloadShapeTruncated = shape.result(opts.ShapeMaxPaths)
	}
	if opts.IncludeValueFamilyStats {
		valueFamilyStats, truncated, err := valueFamilies.result(opts.ValueFamilyMaxPaths)
		if err != nil {
			return retainedPayloadCollectionAuditError(result, err)
		}
		result.RetainedPayloadValueFamilies = valueFamilyStats
		result.RetainedPayloadValueFamiliesTruncated = truncated
	}
	if opts.IncludeSemanticStreams {
		semanticStreamStats, truncated, inputBytes, zstdBytes, err := semanticStreams.result(opts.SemanticStreamMaxPaths)
		if err != nil {
			return retainedPayloadCollectionAuditError(result, err)
		}
		result.RetainedPayloadSemanticStreams = semanticStreamStats
		result.RetainedPayloadSemanticStreamsTruncated = truncated
		result.RetainedPayloadSemanticStreamInputBytes = inputBytes
		result.RetainedPayloadSemanticStreamZSTDBytes = zstdBytes
	}
	if result.Truncated {
		result.Status = "passed_sampled"
	} else {
		result.Status = "passed"
	}
	return result, nil
}

func appendColumnRetainedPayloadBlockLayoutViolations(result *ColumnRetainedPayloadCollectionAuditResult, source string, blockPaths map[string]struct{}) []string {
	if result == nil || len(blockPaths) == 0 {
		return nil
	}
	violatedPaths := make([]string, 0)
	for _, path := range result.DeclaredPaths {
		pathKey := columnRetainedSemanticStreamPathKey(strings.Split(path, "."))
		if _, ok := blockPaths[pathKey]; ok {
			result.Violations = append(result.Violations, ColumnRetainedPayloadCollectionPathViolation{
				DocumentID: source,
				Path:       path,
			})
			violatedPaths = append(violatedPaths, path)
		}
	}
	return violatedPaths
}

func retainedPayloadCollectionAuditError(result ColumnRetainedPayloadCollectionAuditResult, err error) (ColumnRetainedPayloadCollectionAuditResult, error) {
	result.Status = "failed"
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	return result, err
}

func columnRetainedPayloadAuditDeclaredPaths(cfg ColumnStoreConfig) []string {
	paths := make([]string, 0, len(cfg.Columns))
	for _, col := range cfg.Columns {
		paths = append(paths, col.Path)
	}
	return normalizeColumnRetainedPayloadAuditPaths(paths)
}

func normalizeColumnRetainedPayloadAuditPaths(paths []string) []string {
	checked := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		checked = append(checked, path)
	}
	sort.Strings(checked)
	return checked
}

func columnRetainedPayloadAuditPolicy(cfg *ColumnStoreConfig) ColumnRetainedPayloadPolicy {
	if cfg == nil || !cfg.Enabled {
		return ""
	}
	if cfg.RetainedPayload == "" {
		return ColumnRetainedPayloadFull
	}
	return cfg.RetainedPayload
}

func columnJSONPathExists(obj map[string]any, path string) bool {
	if obj == nil || path == "" {
		return false
	}
	parts := strings.Split(path, ".")
	var current any = obj
	for i, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return false
		}
		next, ok := m[part]
		if !ok {
			return false
		}
		if i == len(parts)-1 {
			return true
		}
		current = next
	}
	return false
}

type columnRetainedPayloadShapeKey struct {
	path string
	kind string
}

type columnRetainedPayloadShapeCollector struct {
	byKey    map[columnRetainedPayloadShapeKey]*ColumnRetainedPayloadShapePathStat
	maxDepth int
}

func newColumnRetainedPayloadShapeCollector(maxDepth int) *columnRetainedPayloadShapeCollector {
	return &columnRetainedPayloadShapeCollector{
		byKey:    make(map[columnRetainedPayloadShapeKey]*ColumnRetainedPayloadShapePathStat),
		maxDepth: maxDepth,
	}
}

func (c *columnRetainedPayloadShapeCollector) addDocumentObject(obj map[string]any) error {
	if c == nil || obj == nil {
		return nil
	}
	seen := make(map[columnRetainedPayloadShapeKey]struct{})
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := c.addValue(key, obj[key], 1, seen); err != nil {
			return err
		}
	}
	for key := range seen {
		c.byKey[key].Documents++
	}
	return nil
}

func (c *columnRetainedPayloadShapeCollector) addValue(path string, value any, depth int, seen map[columnRetainedPayloadShapeKey]struct{}) error {
	kind := columnRetainedPayloadShapeValueKind(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal path %q %s value: %w", path, kind, err)
	}
	key := columnRetainedPayloadShapeKey{path: path, kind: kind}
	stat := c.byKey[key]
	if stat == nil {
		stat = &ColumnRetainedPayloadShapePathStat{Path: path, ValueKind: kind}
		c.byKey[key] = stat
	}
	jsonBytes := len(encoded)
	stat.Occurrences++
	stat.JSONBytes += int64(jsonBytes)
	if jsonBytes > stat.MaxJSONBytes {
		stat.MaxJSONBytes = jsonBytes
	}
	if s, ok := value.(string); ok {
		stat.StringBytes += int64(len(s))
	}
	seen[key] = struct{}{}

	if c.maxDepth > 0 && depth >= c.maxDepth {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := c.addValue(path+"."+key, typed[key], depth+1, seen); err != nil {
				return err
			}
		}
	case []any:
		childPath := path + "[]"
		for _, child := range typed {
			if err := c.addValue(childPath, child, depth+1, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *columnRetainedPayloadShapeCollector) result(maxPaths int) ([]ColumnRetainedPayloadShapePathStat, bool) {
	if c == nil || len(c.byKey) == 0 {
		return nil, false
	}
	out := make([]ColumnRetainedPayloadShapePathStat, 0, len(c.byKey))
	for _, stat := range c.byKey {
		out = append(out, *stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].JSONBytes != out[j].JSONBytes {
			return out[i].JSONBytes > out[j].JSONBytes
		}
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].ValueKind < out[j].ValueKind
	})
	if maxPaths > 0 && len(out) > maxPaths {
		return out[:maxPaths], true
	}
	return out, false
}

type columnRetainedPayloadSemanticStreamKey struct {
	path string
	kind string
}

type columnRetainedPayloadSemanticStreamCollector struct {
	byKey    map[columnRetainedPayloadSemanticStreamKey]*columnRetainedPayloadSemanticStreamPath
	maxDepth int
}

type columnRetainedPayloadSemanticStreamPath struct {
	stat        ColumnRetainedPayloadSemanticStreamStat
	zstdCounter columnRetainedPayloadCountingWriter
	zstdWriter  *zstd.Encoder
}

func newColumnRetainedPayloadSemanticStreamCollector(maxDepth int) *columnRetainedPayloadSemanticStreamCollector {
	return &columnRetainedPayloadSemanticStreamCollector{
		byKey:    make(map[columnRetainedPayloadSemanticStreamKey]*columnRetainedPayloadSemanticStreamPath),
		maxDepth: maxDepth,
	}
}

func (c *columnRetainedPayloadSemanticStreamCollector) addDocumentObject(obj map[string]any) error {
	if c == nil || obj == nil {
		return nil
	}
	seen := make(map[columnRetainedPayloadSemanticStreamKey]struct{})
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := c.addValue(key, obj[key], 1, seen); err != nil {
			return err
		}
	}
	for key := range seen {
		c.byKey[key].stat.Documents++
	}
	return nil
}

func (c *columnRetainedPayloadSemanticStreamCollector) addValue(path string, value any, depth int, seen map[columnRetainedPayloadSemanticStreamKey]struct{}) error {
	if kind, ok := columnRetainedPayloadScalarStreamValueKind(value); ok {
		if err := c.addScalar(path, kind, value); err != nil {
			return err
		}
		seen[columnRetainedPayloadSemanticStreamKey{path: path, kind: kind}] = struct{}{}
	}
	if c.maxDepth > 0 && depth >= c.maxDepth {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := c.addValue(path+"."+key, typed[key], depth+1, seen); err != nil {
				return err
			}
		}
	case []any:
		childPath := path + "[]"
		for _, child := range typed {
			if err := c.addValue(childPath, child, depth+1, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *columnRetainedPayloadSemanticStreamCollector) addScalar(path, kind string, value any) error {
	key := columnRetainedPayloadSemanticStreamKey{path: path, kind: kind}
	stream := c.byKey[key]
	if stream == nil {
		stream = &columnRetainedPayloadSemanticStreamPath{
			stat: ColumnRetainedPayloadSemanticStreamStat{Path: path, ValueKind: kind},
		}
		zstdWriter, err := zstd.NewWriter(&stream.zstdCounter,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderCRC(false),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			return fmt.Errorf("create semantic-stream zstd oracle for path %q kind %q: %w", path, kind, err)
		}
		stream.zstdWriter = zstdWriter
		c.byKey[key] = stream
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal semantic-stream path %q kind %q: %w", path, kind, err)
	}
	streamBytes := len(encoded) + 1
	stream.stat.Occurrences++
	stream.stat.JSONBytes += int64(len(encoded))
	stream.stat.StreamInputBytes += int64(streamBytes)
	if stream.stat.Occurrences == 1 || streamBytes < stream.stat.MinStreamBytes {
		stream.stat.MinStreamBytes = streamBytes
	}
	if streamBytes > stream.stat.MaxStreamBytes {
		stream.stat.MaxStreamBytes = streamBytes
	}
	if s, ok := value.(string); ok {
		stream.stat.StringBytes += int64(len(s))
	}
	if _, err := stream.zstdWriter.Write(encoded); err != nil {
		return fmt.Errorf("write semantic-stream zstd oracle for path %q kind %q: %w", path, kind, err)
	}
	if _, err := stream.zstdWriter.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("write semantic-stream zstd oracle separator for path %q kind %q: %w", path, kind, err)
	}
	return nil
}

func (c *columnRetainedPayloadSemanticStreamCollector) result(maxPaths int) ([]ColumnRetainedPayloadSemanticStreamStat, bool, int64, int64, error) {
	if c == nil || len(c.byKey) == 0 {
		return nil, false, 0, 0, nil
	}
	out := make([]ColumnRetainedPayloadSemanticStreamStat, 0, len(c.byKey))
	var totalInputBytes int64
	var totalZSTDBytes int64
	for key, stream := range c.byKey {
		if stream.zstdWriter != nil {
			if err := stream.zstdWriter.Close(); err != nil {
				return nil, false, 0, 0, fmt.Errorf("close semantic-stream zstd oracle for path %q kind %q: %w", key.path, key.kind, err)
			}
			stream.zstdWriter = nil
		}
		stat := stream.stat
		stat.ZSTDBytes = stream.zstdCounter.n
		stat.ZSTDToInputRatio = columnRetainedPayloadAuditRatio(stat.ZSTDBytes, stat.StreamInputBytes)
		totalInputBytes += stat.StreamInputBytes
		totalZSTDBytes += stat.ZSTDBytes
		out = append(out, stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StreamInputBytes != out[j].StreamInputBytes {
			return out[i].StreamInputBytes > out[j].StreamInputBytes
		}
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].ValueKind < out[j].ValueKind
	})
	if maxPaths > 0 && len(out) > maxPaths {
		return out[:maxPaths], true, totalInputBytes, totalZSTDBytes, nil
	}
	return out, false, totalInputBytes, totalZSTDBytes, nil
}

type columnRetainedPayloadValueFamilyCollector struct {
	byPath    map[string]*columnRetainedPayloadValueFamilyPath
	maxDepth  int
	maxUnique int
}

type columnRetainedPayloadValueFamilyPath struct {
	stat         ColumnRetainedPayloadValueFamilyStat
	unique       map[string]struct{}
	commonPrefix string
	commonSuffix string
	initialized  bool
	buckets      [7]int64
	gzipCounter  columnRetainedPayloadCountingWriter
	gzipWriter   *gzip.Writer
	zstdCounter  columnRetainedPayloadCountingWriter
	zstdWriter   *zstd.Encoder
}

type columnRetainedPayloadCountingWriter struct {
	n int64
}

func (w *columnRetainedPayloadCountingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

func newColumnRetainedPayloadValueFamilyCollector(maxDepth, maxUnique int) *columnRetainedPayloadValueFamilyCollector {
	return &columnRetainedPayloadValueFamilyCollector{
		byPath:    make(map[string]*columnRetainedPayloadValueFamilyPath),
		maxDepth:  maxDepth,
		maxUnique: maxUnique,
	}
}

func (c *columnRetainedPayloadValueFamilyCollector) addDocumentObject(obj map[string]any) error {
	if c == nil || obj == nil {
		return nil
	}
	seen := make(map[string]struct{})
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := c.addValue(key, obj[key], 1, seen); err != nil {
			return err
		}
	}
	for path := range seen {
		c.byPath[path].stat.Documents++
	}
	return nil
}

func (c *columnRetainedPayloadValueFamilyCollector) addValue(path string, value any, depth int, seen map[string]struct{}) error {
	if s, ok := value.(string); ok {
		if err := c.addString(path, s); err != nil {
			return err
		}
		seen[path] = struct{}{}
	}
	if c.maxDepth > 0 && depth >= c.maxDepth {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := c.addValue(path+"."+key, typed[key], depth+1, seen); err != nil {
				return err
			}
		}
	case []any:
		childPath := path + "[]"
		for _, child := range typed {
			if err := c.addValue(childPath, child, depth+1, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *columnRetainedPayloadValueFamilyCollector) addString(path, value string) error {
	stat := c.byPath[path]
	if stat == nil {
		stat = &columnRetainedPayloadValueFamilyPath{
			stat: ColumnRetainedPayloadValueFamilyStat{Path: path},
		}
		stat.gzipWriter = gzip.NewWriter(&stat.gzipCounter)
		zstdWriter, err := zstd.NewWriter(&stat.zstdCounter,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderCRC(false),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			return fmt.Errorf("create zstd oracle for path %q: %w", path, err)
		}
		stat.zstdWriter = zstdWriter
		stat.unique = make(map[string]struct{})
		c.byPath[path] = stat
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal string path %q: %w", path, err)
	}
	stat.stat.Occurrences++
	stat.stat.JSONBytes += int64(len(encoded))
	stat.stat.StringBytes += int64(len(value))
	stat.stat.OracleInputBytes += int64(len(encoded) + 1)
	length := len(value)
	if stat.stat.Occurrences == 1 || length < stat.stat.MinLength {
		stat.stat.MinLength = length
	}
	if length > stat.stat.MaxLength {
		stat.stat.MaxLength = length
	}
	stat.buckets[columnRetainedPayloadLengthBucketIndex(length)]++

	if _, ok := stat.unique[value]; !ok && !stat.stat.UniqueValuesTruncated {
		if c.maxUnique <= 0 || len(stat.unique) < c.maxUnique {
			stat.unique[value] = struct{}{}
		} else {
			stat.stat.UniqueValuesTruncated = true
		}
	}
	if !stat.initialized {
		stat.commonPrefix = value
		stat.commonSuffix = value
		stat.initialized = true
	} else {
		stat.commonPrefix = columnRetainedPayloadCommonPrefix(stat.commonPrefix, value)
		stat.commonSuffix = columnRetainedPayloadCommonSuffix(stat.commonSuffix, value)
	}

	if _, err := stat.gzipWriter.Write(encoded); err != nil {
		return fmt.Errorf("write gzip oracle for path %q: %w", path, err)
	}
	if _, err := stat.gzipWriter.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("write gzip oracle separator for path %q: %w", path, err)
	}
	if _, err := stat.zstdWriter.Write(encoded); err != nil {
		return fmt.Errorf("write zstd oracle for path %q: %w", path, err)
	}
	if _, err := stat.zstdWriter.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("write zstd oracle separator for path %q: %w", path, err)
	}
	return nil
}

func (c *columnRetainedPayloadValueFamilyCollector) result(maxPaths int) ([]ColumnRetainedPayloadValueFamilyStat, bool, error) {
	if c == nil || len(c.byPath) == 0 {
		return nil, false, nil
	}
	out := make([]ColumnRetainedPayloadValueFamilyStat, 0, len(c.byPath))
	for path, internal := range c.byPath {
		if internal.gzipWriter != nil {
			if err := internal.gzipWriter.Close(); err != nil {
				return nil, false, fmt.Errorf("close gzip oracle for path %q: %w", path, err)
			}
			internal.gzipWriter = nil
		}
		if internal.zstdWriter != nil {
			if err := internal.zstdWriter.Close(); err != nil {
				return nil, false, fmt.Errorf("close zstd oracle for path %q: %w", path, err)
			}
			internal.zstdWriter = nil
		}
		stat := internal.stat
		if stat.Occurrences > 0 {
			stat.MeanLength = float64(stat.StringBytes) / float64(stat.Occurrences)
		}
		stat.TrackedUniqueValues = int64(len(internal.unique))
		if !stat.UniqueValuesTruncated {
			stat.RepeatedValues = stat.Occurrences - stat.TrackedUniqueValues
		}
		stat.CommonPrefixBytes = len(internal.commonPrefix)
		stat.CommonPrefix, stat.CommonPrefixTruncated = columnRetainedPayloadAuditClipString(internal.commonPrefix, 128)
		stat.CommonSuffixBytes = len(internal.commonSuffix)
		stat.CommonSuffix, stat.CommonSuffixTruncated = columnRetainedPayloadAuditClipString(internal.commonSuffix, 128)
		stat.LengthBuckets = columnRetainedPayloadLengthBuckets(internal.buckets)
		stat.GzipBytes = internal.gzipCounter.n
		stat.GzipToInputRatio = columnRetainedPayloadAuditRatio(stat.GzipBytes, stat.OracleInputBytes)
		stat.ZSTDBytes = internal.zstdCounter.n
		stat.ZSTDToInputRatio = columnRetainedPayloadAuditRatio(stat.ZSTDBytes, stat.OracleInputBytes)
		out = append(out, stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].JSONBytes != out[j].JSONBytes {
			return out[i].JSONBytes > out[j].JSONBytes
		}
		if out[i].Occurrences != out[j].Occurrences {
			return out[i].Occurrences > out[j].Occurrences
		}
		return out[i].Path < out[j].Path
	})
	if maxPaths > 0 && len(out) > maxPaths {
		return out[:maxPaths], true, nil
	}
	return out, false, nil
}

func columnRetainedPayloadLengthBucketIndex(length int) int {
	switch {
	case length <= 8:
		return 0
	case length <= 16:
		return 1
	case length <= 32:
		return 2
	case length <= 64:
		return 3
	case length <= 128:
		return 4
	case length <= 256:
		return 5
	default:
		return 6
	}
}

func columnRetainedPayloadLengthBuckets(counts [7]int64) []ColumnRetainedPayloadLengthBucket {
	names := [...]string{"le_8", "le_16", "le_32", "le_64", "le_128", "le_256", "gt_256"}
	out := make([]ColumnRetainedPayloadLengthBucket, 0, len(counts))
	for i, count := range counts {
		if count == 0 {
			continue
		}
		out = append(out, ColumnRetainedPayloadLengthBucket{Bucket: names[i], Count: count})
	}
	return out
}

func columnRetainedPayloadCommonPrefix(a, b string) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return a[:i]
}

func columnRetainedPayloadCommonSuffix(a, b string) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	return a[len(a)-i:]
}

func columnRetainedPayloadAuditClipString(value string, maxBytes int) (string, bool) {
	if maxBytes <= 0 {
		return value, false
	}
	limit := len(value)
	truncated := false
	if limit > maxBytes {
		limit = maxBytes
		truncated = true
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
		truncated = true
	}
	return value[:limit], truncated
}

func columnRetainedPayloadAuditRatio(num, denom int64) float64 {
	if denom <= 0 {
		return 0
	}
	return float64(num) / float64(denom)
}

func columnRetainedPayloadShapeValueKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case string:
		return "string"
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return "number"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return "unknown"
	}
}

func columnRetainedPayloadScalarStreamValueKind(value any) (string, bool) {
	switch value.(type) {
	case nil:
		return "null", true
	case bool:
		return "bool", true
	case string:
		return "string", true
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return "number", true
	default:
		return "", false
	}
}
