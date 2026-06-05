package collections

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

const (
	// ColumnRetainedPayloadEncodingJSON records the current production retained
	// payload body encoding: retained payloads are stored as JSON objects after
	// applying the retained-payload policy.
	ColumnRetainedPayloadEncodingJSON = "json"
	// ColumnRetainedPayloadEncodingNone records that no retained body is active.
	ColumnRetainedPayloadEncodingNone = "none"
)

// ColumnRetainedPayloadEncodingStatus reports the retained payload body encoding
// currently implied by column-store metadata. It is status/reporting metadata;
// Template-v1 retained bodies are intentionally deferred to the Template-v1
// storage implementation issue.
func ColumnRetainedPayloadEncodingStatus(cfg *ColumnStoreConfig) (encoding, status string) {
	if cfg == nil || !cfg.Enabled {
		return ColumnRetainedPayloadEncodingNone, "not_configured"
	}
	switch columnRetainedPayloadAuditPolicy(cfg) {
	case ColumnRetainedPayloadNone:
		return ColumnRetainedPayloadEncodingNone, "inactive_no_retained_payload"
	case ColumnRetainedPayloadNonColumn:
		return ColumnRetainedPayloadEncodingJSON, "active_json_non_column_retained_payload"
	case ColumnRetainedPayloadFull, "":
		return ColumnRetainedPayloadEncodingJSON, "active_json_full_retained_payload"
	default:
		return ColumnRetainedPayloadEncodingJSON, fmt.Sprintf("unknown_retained_payload_policy_%s", cfg.RetainedPayload)
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

// AuditColumnRetainedPayloadPathsAbsent verifies that declared typed JSON paths
// are absent from a retained payload body. It fails closed on malformed retained
// JSON and on any declared path that remains present.
func AuditColumnRetainedPayloadPathsAbsent(cfg ColumnStoreConfig, retained []byte, paths []string) (ColumnRetainedPayloadPathAudit, error) {
	policy := columnRetainedPayloadAuditPolicy(&cfg)
	encoding, status := ColumnRetainedPayloadEncodingStatus(&cfg)
	audit := ColumnRetainedPayloadPathAudit{
		RetainedPayloadPolicy:         policy,
		RetainedPayloadEncoding:       encoding,
		RetainedPayloadEncodingStatus: status,
		RetainedPayloadBytes:          len(retained),
	}
	if encoding != ColumnRetainedPayloadEncodingJSON && encoding != ColumnRetainedPayloadEncodingNone {
		return audit, fmt.Errorf("collections: retained payload path audit unsupported encoding %q", encoding)
	}

	trimmed := bytes.TrimSpace(retained)
	if len(trimmed) == 0 {
		trimmed = []byte("{}")
	}
	obj, err := decodeColumnJSONObject(trimmed)
	if err != nil {
		return audit, fmt.Errorf("collections: retained payload path audit: %w", err)
	}

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
