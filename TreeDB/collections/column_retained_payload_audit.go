package collections

import (
	"fmt"
	"sort"
	"strings"

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
		switch cfg.RetainedPayloadEncoding {
		case ColumnRetainedPayloadEncodingTemplateV1, "":
			return string(ColumnRetainedPayloadEncodingTemplateV1), "active_template_v1_non_column_retained_payload"
		case ColumnRetainedPayloadEncodingJSON:
			return string(ColumnRetainedPayloadEncodingJSON), "active_legacy_json_non_column_retained_payload"
		default:
			return string(ColumnRetainedPayloadEncodingUnavailable), fmt.Sprintf("unavailable_unsupported_retained_payload_encoding_%s", cfg.RetainedPayloadEncoding)
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
	Paths        []string
	MaxDocuments int
}

type ColumnRetainedPayloadCollectionPathViolation struct {
	DocumentID string `json:"document_id"`
	Path       string `json:"path"`
}

type ColumnRetainedPayloadCollectionAuditResult struct {
	Collection                       string                                         `json:"collection"`
	Status                           string                                         `json:"status"`
	RetainedPayloadPolicy            ColumnRetainedPayloadPolicy                    `json:"retained_payload_policy"`
	RetainedPayloadEncoding          string                                         `json:"retained_payload_encoding"`
	RetainedPayloadEncodingStatus    string                                         `json:"retained_payload_encoding_status"`
	RetainedPayloadCompression       string                                         `json:"retained_payload_compression"`
	RetainedPayloadCompressionPolicy string                                         `json:"retained_payload_compression_policy"`
	RetainedPayloadCompressionStatus string                                         `json:"retained_payload_compression_status"`
	DeclaredPaths                    []string                                       `json:"declared_paths"`
	CheckedRows                      int                                            `json:"checked_rows"`
	RetainedPayloadBytes             int64                                          `json:"retained_payload_bytes"`
	Truncated                        bool                                           `json:"truncated,omitempty"`
	Violations                       []ColumnRetainedPayloadCollectionPathViolation `json:"violations,omitempty"`
	Errors                           []string                                       `json:"errors,omitempty"`
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
func (c *Collection) AuditRetainedPayloadDeclaredPathsAbsent(opts ColumnRetainedPayloadCollectionAuditOptions) (ColumnRetainedPayloadCollectionAuditResult, error) {
	var result ColumnRetainedPayloadCollectionAuditResult
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
	if len(result.DeclaredPaths) == 0 {
		result.Status = "no_declared_paths"
		return result, nil
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
		result.CheckedRows++
		result.RetainedPayloadBytes += int64(len(retained))
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
		it.Next()
	}
	if err := it.Error(); err != nil {
		return retainedPayloadCollectionAuditError(result, err)
	}
	if result.Truncated {
		result.Status = "passed_sampled"
	} else {
		result.Status = "passed"
	}
	return result, nil
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
